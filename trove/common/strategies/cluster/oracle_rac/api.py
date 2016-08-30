# flake8: noqa

# Copyright (c) 2016 Tesora, Inc.
#
# This file is part of the Tesora DBaas Platform Enterprise Edition.
#
# Tesora DBaaS Platform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# According to sec. 7 of the GNU Affero General Public License, version 3, the
# terms of the AGPL are supplemented with the following terms:
#
# "Tesora", "Tesora DBaaS Platform", and the Tesora logo are trademarks
#  of Tesora, Inc.,
#
# The licensing of the Program under the AGPL does not imply a trademark
# license. Therefore any rights, title and interest in our trademarks remain
# entirely with us.
#
# However, if you propagate an unmodified version of the Program you are
# allowed to use the term "Tesora" solely to indicate that you distribute the
# Program. Furthermore you may use our trademarks where it is necessary to
# indicate the intended purpose of a product or service provided you use it in
# accordance with honest practices in industrial or commercial matters.
#
# If you want to propagate modified versions of the Program under the name
# "Tesora" or "Tesora DBaaS Platform", you may only do so if you have a written
# permission by Tesora, Inc. (to acquire a permission please contact
# Tesora, Inc at trademark@tesora.com).
#
# The interactive user interface of the software displays an attribution notice
# containing the term "Tesora" and/or the logo of Tesora.  Interactive user
# interfaces of unmodified and modified versions must display Appropriate Legal
# Notices according to sec. 5 of the GNU Affero General Public License,
# version 3, when you propagate unmodified or modified versions of  the
# Program. In accordance with sec. 7 b) of the GNU Affero General Public
# License, version 3, these Appropriate Legal Notices must retain the logo of
# Tesora or display the words "Initial Development by Tesora" if the display of
# the logo is not reasonably feasible for technical reasons.

import re

import netaddr
from novaclient import exceptions as nova_exceptions
from oslo_log import log as logging
from oslo_utils import importutils

from trove.cluster import models
from trove.cluster import tasks
from trove.cluster import views
from trove.common import cfg
from trove.common import crypto_utils
from trove.common import exception
from trove.common.i18n import _
from trove.common import remote
from trove.common.strategies.cluster import base
from trove.common.strategies.cluster.oracle_rac import utils as rac_utils
from trove.common import utils
from trove.extensions.mgmt.clusters import views as mgmt_views
from trove.instance import models as inst_models
from trove.quota import quota
from trove.taskmanager import api as task_api

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class OracleRACAPIStrategy(base.BaseAPIStrategy):

    @property
    def cluster_class(self):
        return OracleRACCluster

    @property
    def cluster_controller_actions(self):
        return {
            'grow': self._action_grow_cluster,
            'shrink': self._action_shrink_cluster
        }

    def _action_grow_cluster(self, cluster, body):
        nodes = body['grow']
        instances = []
        for node in nodes:
            instance = {
                'flavor_id': utils.get_id_from_href(node['flavorRef'])
            }
            if 'name' in node:
                instance['name'] = node['name']
            instances.append(instance)
        return cluster.grow(instances)

    def _action_shrink_cluster(self, cluster, body):
        nodes = body['shrink']
        instance_ids = [node['id'] for node in nodes]
        return cluster.shrink(instance_ids)

    @property
    def cluster_view_class(self):
        return OracleRACClusterView

    @property
    def mgmt_cluster_view_class(self):
        return OracleRACMgmtClusterView


class OracleRACCluster(models.Cluster):

    @classmethod
    def create(cls, context, name, datastore, datastore_version,
               instances, extended_properties, locality):
        nova_client = remote.create_nova_client(context)
        network_driver = (importutils.import_class(
            CONF.network_driver))(context, None)
        ds_conf = CONF.get(datastore_version.manager)
        num_instances = len(instances)

        # Run checks first
        if not network_driver.subnet_support:
            raise exception.TroveError(_(
                "The configured network driver does not support subnet "
                "management. This is required for Oracle RAC clusters."))

        quota.check_quotas(context.tenant, {'instances': num_instances})
        for instance in instances:
            if not instance.get('flavor_id'):
                raise exception.BadRequest(_("Missing required flavor_id."))
            try:
                nova_client.flavors.get(instance['flavor_id'])
            except nova_exceptions.NotFound:
                raise exception.FlavorNotFound(uuid=instance['flavor_id'])
            if instance.get('volume_size'):
                raise exception.VolumeNotSupported()
            if instance.get('region_name'):
                raise exception.BadRequest(_("Instance region_name option not "
                                             "supported."))

        database = extended_properties.get('database')
        if not database:
            raise exception.BadRequest(_("Missing database name."))
        if len(database) > 8:
            raise exception.BadValue(_("Database name greater than 8 chars."))
        storage_info = check_storage_info(extended_properties)
        subnet, subnetpool, network = check_public_network_info(
            ds_conf, network_driver, num_instances, extended_properties)

        ssh_pem, ssh_pub = crypto_utils.generate_ssh_keys()

        sys_password = utils.generate_random_password(
            datastore=datastore.name)
        admin_password = utils.generate_random_password(
            datastore=datastore.name)

        # Create the cluster
        db_info = models.DBCluster.create(
            name=name, tenant_id=context.tenant,
            datastore_version_id=datastore_version.id,
            task_status=tasks.ClusterTasks.BUILDING_INITIAL)

        if not subnet:
            LOG.debug("Creating RAC public subnet on network {net} from "
                      "pool {pool}".format(net=network['id'],
                                           pool=subnetpool['id']))
            subnet = create_public_subnet_from_pool(
                ds_conf, network_driver, db_info.id, subnetpool, network,
                extended_properties.get('router'),
                extended_properties.get('prefixlen'))
            LOG.debug("Created subnet {sub} with CIDR {cidr}".format(
                sub=subnet['id'], cidr=subnet['cidr']))

        interconnect_network, interconnect_subnet = create_interconnect(
            ds_conf, network_driver, db_info.id)
        LOG.debug("Created interconnect network {net} with subnet "
                  "{sub}".format(net=interconnect_network['id'],
                                 sub=interconnect_subnet['id']))

        public_subnet_manager = rac_utils.RACPublicSubnetManager(
            subnet['cidr'])
        interconnect_subnet_manager = rac_utils.CommonSubnetManager(
            interconnect_subnet['cidr'])

        subnet = configure_public_subnet(
            ds_conf, network_driver, db_info.id, subnet,
            public_subnet_manager.allocation_pool)
        LOG.debug("RAC public subnet ({sub_id}) info: name='{name}', scans="
                  "{scans}".format(sub_id=subnet['id'], name=subnet['name'],
                                   scans=public_subnet_manager.scan_list))

        cluster_config = {
            'id': db_info.id,
            'instance_type': 'node',
            'storage': storage_info,
            'ssh_pem': ssh_pem,
            'ssh_pub': ssh_pub,
            'database': database,
            'sys_password': sys_password,
            'admin_password': admin_password}

        vips = (public_subnet_manager.scan_list +
                [public_subnet_manager.instance_vip(i)
                 for i in range(len(instances))])

        for i, instance in enumerate(instances):
            instance_name = rac_utils.make_instance_hostname(name, i)
            nics = instance.get('nics') or []
            public_port_name = rac_utils.make_object_name(
                ds_conf, ['public', 'port', str(i + 1)], db_info.id)
            public_port = create_port(
                network_driver, public_port_name, i,
                subnet, public_subnet_manager, vips=vips)
            interconnect_port_name = rac_utils.make_object_name(
                ds_conf, ['interconnect', 'port', str(i + 1)], db_info.id)
            interconnect_port = create_port(
                network_driver, interconnect_port_name, i,
                interconnect_subnet, interconnect_subnet_manager)
            nics.append({'port-id': public_port['id']})
            nics.append({'port-id': interconnect_port['id']})
            LOG.debug("Creating instance {name} with public ip {pub} and "
                      "interconnect ip {int}".format(
                        name=instance_name,
                        pub=public_port['fixed_ips'][0]['ip_address'],
                        int=interconnect_port['fixed_ips'][0]['ip_address']))
            inst_models.Instance.create(
                context,
                instance_name,
                instance['flavor_id'],
                datastore_version.image_id,
                [], [], datastore,
                datastore_version,
                None, None,
                availability_zone=instance.get('availability_zone'),
                nics=nics,
                cluster_config=cluster_config,
                modules=instance.get('modules'),
                locality=locality)

        task_api.load(context, datastore_version.manager).create_cluster(
            db_info.id)

        return OracleRACCluster(context, db_info, datastore, datastore_version)


def check_storage_info(properties):
    """Check the storage information provided by the user."""
    storage_type = properties.get('storage_type')
    if not storage_type:
        raise exception.BadRequest(_("Missing storage type."))
    storage = {'type': storage_type, 'data': dict()}
    if storage_type == 'nfs':
        def _check_mount(name):
            value = properties.get(name)
            if not value or not re.match(r'\S+:\S+', str(value)):
                raise exception.BadRequest(_(
                    "Invalid or missing mount {name}. "
                    "Specify mount values for votedisk_mount, "
                    "registry_mount, and database_mount in the format "
                    "'host:path'.".format(name=name)))
            storage['data'][name] = value

        for mount in ['votedisk_mount', 'registry_mount', 'database_mount']:
            _check_mount(mount)
    else:
        raise exception.BadValue(_("Unsupported cluster storage "
                                   "type ").format(t=storage_type))
    return storage


def check_public_network_info(conf, driver, num_instances, properties):
    """Check the network information provided by the user."""
    subnet_id = properties.get('subnet')
    subnetpool_id = properties.get('subnetpool')
    subnet = None
    subnetpool = None
    network = None
    if subnet_id:
        subnet = check_subnet(conf, driver, num_instances, subnet_id)
    elif subnetpool_id:
        prefixlen = properties.get('prefixlen')
        network_id = properties.get('network')
        router_id = properties.get('router')
        subnetpool = check_subnetpool(
            conf, driver, num_instances, subnetpool_id, prefixlen)
        network = check_network(driver, network_id)
        check_router(driver, router_id)
    else:
        raise exception.BadRequest(_("Network input missing subnet or "
                                     "subnetpool information."))
    return subnet, subnetpool, network


def check_subnet(conf, driver, num_instances, subnet_id):
    try:
        subnet = driver.get_subnet_by_id(subnet_id)
    except exception.TroveError:
        raise exception.BadValue(_("Public subnet is not valid."))
    check_subnet_ip_count(conf, subnet['cidr'], num_instances)
    return subnet


def check_subnetpool(conf, driver, num_instances, subnetpool_id, prefixlen):
    try:
        subnetpool = driver.get_subnetpool_by_id(subnetpool_id)
    except exception.TroveError:
        raise exception.BadValue(_("Public subnet pool is not valid."))
    prefix_len = determine_subnet_prefix_len(conf, subnetpool, prefixlen)
    pmin = int(subnetpool['min_prefixlen'])
    pmax = int(subnetpool['max_prefixlen'])
    if prefix_len < pmin or prefix_len > pmax:
        raise exception.BadValue(_(
            "Subnet prefix length {plen} is outside of pool's "
            "range {pmin}-{pmax}.").format(
            plen=prefix_len, pmin=pmin, pmax=pmax))
    ipnet = netaddr.IPNetwork(subnetpool['prefixes'][0])
    ipnet.prefixlen = prefix_len
    check_subnet_ip_count(conf, str(ipnet.cidr), num_instances)
    return subnetpool


def check_network(driver, network_id):
    if not network_id:
        raise exception.BadRequest(_(
            "Network information missing 'id'. To use a subnet pool a network "
            "id must be given."))
    try:
        network = driver.get_network_by_id(network_id)
    except exception.TroveError:
        raise exception.BadValue(_("Public network is not valid."))
    return network


def check_router(driver, router_id):
    if not router_id:
        raise exception.BadRequest(_(
            "Network information missing 'router'. To use a subnet pool a "
            "router id must be given."))
    try:
        driver.get_router_by_id(router_id)
    except exception.TroveError:
        raise exception.BadValue(_("Router is not valid."))


def check_subnet_ip_count(conf, cidr, num_instances):
    """Check the given subnet and configured rac subnet have enough IPs."""
    subnet_manager = rac_utils.RACPublicSubnetManager(cidr)
    if num_instances > subnet_manager.max_instances:
        raise exception.BadValue(_(
            "Not enough IPs available in the specified public subnet."))
    interconnect_manager = rac_utils.CommonSubnetManager(
        conf.interconnect_subnet_cidr)
    if num_instances > interconnect_manager.max_instances:
        raise exception.BadValue(_("Not enough IPs available in the configured "
                                   "private RAC interconnect subnet."))


def determine_subnet_prefix_len(conf, subnetpool, prefixlen):
    return int(prefixlen or conf.default_prefixlen or
               subnetpool['default_prefixlen'])


def create_public_subnet_from_pool(conf, driver, cluster_id, subnetpool,
                                   network, router_id, prefixlen=None):
    prefixlen = determine_subnet_prefix_len(
        conf, subnetpool, prefixlen)
    subnet = driver.create_subnet(network['id'],
                                  ip_version=4,
                                  subnetpool_id=subnetpool['id'],
                                  prefixlen=prefixlen)
    driver.connect_subnet_to_router(router_id, subnet['id'])
    return subnet


def create_interconnect(conf, driver, cluster_id):
    network_name = rac_utils.make_interconnect_network_name(conf, cluster_id)
    subnet_name = rac_utils.make_object_name(
        conf, ['interconnect', 'subnet'], cluster_id)
    cidr = conf.interconnect_subnet_cidr
    network = driver.create_network(network_name)
    subnet = driver.create_subnet(network['id'],
                                  name=subnet_name,
                                  cidr=cidr,
                                  gateway_ip=None,
                                  ip_version=4)
    return network, subnet


def configure_public_subnet(conf, driver, cluster_id, subnet, ip_range):
    allocation_pools = [{'start': ip_range[0], 'end': ip_range[-1]}]
    subnet_name = rac_utils.make_object_name(
        conf, ['public', 'subnet'], cluster_id)
    subnet = driver.update_subnet(subnet['id'],
                                  name=subnet_name,
                                  enable_dhcp=True,
                                  allocation_pools=allocation_pools)
    # find and rename the subnet's interface to the router
    ports = driver.list_ports()
    for port in ports:
        if port.get('device_owner') == 'network:router_interface':
            if port.get('fixed_ips')[0].get('subnet_id') == subnet['id']:
                interface_name = rac_utils.make_object_name(
                    conf, ['public', 'interface', 'port'], cluster_id)
                driver.update_port(port['id'], name=interface_name)
    return subnet


def create_port(driver, port_name, instance_number, subnet,
                subnet_manager, vips=[]):
    fixed_ips = [{'subnet_id': subnet['id'],
                  'ip_address': subnet_manager.instance_ip(instance_number)}]
    kwargs = {'name': port_name,
              'fixed_ips': fixed_ips,
              "admin_state_up": True}

    if vips:
        allowed_address_pairs = [{'ip_address': vip} for vip in vips]
        kwargs.update({'allowed_address_pairs': allowed_address_pairs})

    return driver.create_port(subnet['network_id'], **kwargs)


def get_subnet_by_name(driver, subnet_name):
    subnets = driver.list_subnets()
    matches = [item for item in subnets if item['name'] == subnet_name]
    if len(matches) == 0:
        return None
    if len(matches) > 1:
        raise exception.TroveError("Multiple subnets found matching "
                                   "name " + subnet_name)
    return driver.get_subnet_by_id(matches[0]['id'])


class OracleRACClusterView(views.ClusterView):

    def build_instances(self):
        instances, instance_ips = self._build_instances(['node'], ['node'])
        ds_conf = CONF.get(self.cluster.ds_version.manager)
        network_driver = (importutils.import_class(CONF.network_driver)
                          )(self.cluster.context, None)
        subnet_name = rac_utils.make_object_name(
            ds_conf, ['public', 'subnet'], self.cluster.id)
        subnet = get_subnet_by_name(network_driver, subnet_name)
        if not subnet:
            LOG.exception(
                "Oracle RAC subnet {name} does not "
                "exist.".format(name=subnet_name))
            return instances, []
        subnet_manager = rac_utils.RACPublicSubnetManager(
            subnet['cidr'])
        return instances, subnet_manager.scan_list


class OracleRACMgmtClusterView(mgmt_views.MgmtClusterView):

    def build_instances(self):
        return self._build_instances(['node'], ['node'])

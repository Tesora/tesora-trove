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

from eventlet.timeout import Timeout
from oslo_log import log as logging
from oslo_utils import importutils

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster import base
from trove.common.strategies.cluster.oracle_rac import utils as rac_utils
from trove.common import utils
from trove.extensions.security_group import models as secgroup_models
from trove.instance import models as instance_models
from trove.taskmanager import api as task_api
from trove.taskmanager import models as task_models

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
USAGE_SLEEP_TIME = CONF.usage_sleep_time  # seconds.


class OracleRACTaskManagerStrategy(base.BaseTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return OracleRACTaskManagerAPI

    @property
    def task_manager_cluster_tasks_class(self):
        return OracleRACClusterTasks


class OracleRACClusterTasks(task_models.ClusterTasks):

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s." % cluster_id)
        self.ds_conf = CONF.get(self.datastore_version.manager)
        self.network_driver = (importutils.import_class(
            CONF.network_driver))(context, None)

        def _create_cluster():
            db_instances = instance_models.DBInstance.find_all(
                cluster_id=cluster_id).all()
            instance_ids = [db_instance.id for db_instance in db_instances]
            LOG.debug("instances in cluster %s: %s" % (cluster_id,
                                                       instance_ids))
            if not self._all_instances_ready(instance_ids, cluster_id):
                return
            LOG.debug("all instances in cluster %s ready." % cluster_id)
            instances = [instance_models.Instance.load(
                context, instance_id) for instance_id in instance_ids]

            self._open_firewall_between_nodes(instance_ids)
            self._get_subnets()

            instance_guests = [self.get_guest(instance)
                               for instance in instances]

            for instance_guest in instance_guests:
                instance_guest.configure_hosts(
                    self.name,
                    self.pub_subnet['cidr'],
                    self.int_subnet['cidr'])
            self._get_hostnames(instances)
            for instance_guest in instance_guests:
                instance_guest.establish_ssh_user_equivalency(
                    self.host_ip_pairs)

            instance_guests[0].configure_grid(
                self.id, self.name, self._make_nodes_string(include_vips=True),
                self.pub_subnet['cidr'], self.int_subnet['cidr'])
            for instance_guest in instance_guests:
                instance_guest.run_grid_root()

            instance_guests[0].install_oracle_database(
                self._make_nodes_string())
            for instance_guest in instance_guests:
                instance_guest.run_oracle_root()

            self._allow_interconnect_vips(instances, instance_guests)

            instance_guests[0].create_rac_database(self._make_nodes_string())

            # for instance_guest in instance_guests[1:]:
            #     instance_guest.determine_sid()

            for instance in instances:
                self.get_guest(instance).cluster_complete()

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _create_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception(_("Timeout for building cluster."))
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End create_cluster for id: %s." % cluster_id)

    def _open_firewall_between_nodes(self, instance_ids):
        LOG.debug("Opening firewall between cluster instances.")
        security_groups = [
            secgroup_models.SecurityGroup.
                get_security_group_by_id_or_instance_id(instance_id,
                                                        self.tenant_id)
            for instance_id in instance_ids]
        for security_group in security_groups:
            for friendly_group in security_groups:
                if security_group == friendly_group.id:
                    # same group
                    continue
                self.network_driver.create_security_group_rule(
                    security_group.id, ethertype='IPv4',
                    direction='ingress', remote_group_id=friendly_group.id)

    def _get_subnets(self):
        public_subnet_name = rac_utils.make_object_name(
            self.ds_conf, ['public', 'subnet'], self.id)
        interconnect_subnet_name = rac_utils.make_object_name(
            self.ds_conf, ['interconnect', 'subnet'], self.id)
        subnets = self.network_driver.list_subnets()
        self.pub_subnet = None
        self.int_subnet = None
        for subnet in subnets:
            if subnet['name'] == public_subnet_name:
                self.pub_subnet = subnet
            elif subnet['name'] == interconnect_subnet_name:
                self.int_subnet = subnet
        if not self.pub_subnet and self.int_subnet:
            raise exception.TroveError(_(
                "Could not find cluster's public and interconnect subnets."))
        LOG.debug("Cluster {clu} is using public subnet {pubsub} and "
                  "interconnect subnet {intsub}".format(
                      clu=self.id, pubsub=self.pub_subnet['cidr'],
                      intsub=self.int_subnet['cidr']))

    def _get_hostnames(self, instances):
        self.host_ip_pairs = []
        pub_subnet_manager = rac_utils.RACPublicSubnetManager(
            self.pub_subnet['cidr'])
        network_name = self.network_driver.get_network_by_id(
            self.pub_subnet['network_id'])['name']
        for instance in instances:
            public_ip = instance.addresses[network_name][0]['addr']
            i = pub_subnet_manager.instance_index_from_ip(public_ip)
            hostname = rac_utils.make_instance_hostname(self.name, i)
            # append a tuple
            self.host_ip_pairs.append((hostname, public_ip))

    def _make_nodes_string(self, include_vips=False):
        nodes = []
        for pair in self.host_ip_pairs:
            hostname = pair[0]
            if include_vips:
                nodes.append(':'.join([hostname, hostname + '-vip']))
            else:
                nodes.append(hostname)
        return ','.join(nodes)

    def _allow_interconnect_vips(self, instances, instance_guests):
        network_name = rac_utils.make_interconnect_network_name(self.ds_conf,
                                                                self.id)
        # get the interconnect subnet's ports
        ports = [port for port in self.network_driver.list_ports()
                 if port['fixed_ips'][0]['subnet_id'] == self.int_subnet['id']]
        for i in range(len(instances)):
            ip = instances[i].addresses[network_name][0]['addr']
            vip = instance_guests[i].get_private_vip(ip)
            port_id = [port for port in ports
                       if port['fixed_ips'][0]['ip_address'] == ip][0]['id']
            self.network_driver.update_port(
                port_id, allowed_address_pairs=[{'ip_address': vip}])


class OracleRACTaskManagerAPI(task_api.API):
    pass

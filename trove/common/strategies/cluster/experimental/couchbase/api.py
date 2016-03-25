# Copyright 2015 Tesora Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_log import log as logging

from trove.cluster import models
from trove.cluster.tasks import ClusterTasks
from trove.cluster.views import ClusterView
from trove.common import cfg
from trove.common import server_group as srv_grp
from trove.common.strategies.cluster import base
from trove.common.strategies.cluster.experimental.couchbase.taskmanager import(
    CouchbaseClusterTasks)
from trove.common import utils
from trove.extensions.mgmt.clusters.views import MgmtClusterView
from trove.instance import models as inst_models
from trove.quota.quota import check_quotas
from trove.taskmanager import api as task_api


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class CouchbaseAPIStrategy(base.BaseAPIStrategy):

    @property
    def cluster_class(self):
        return CouchbaseCluster

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
            if 'volume' in node:
                instance['volume_size'] = int(node['volume']['size'])
            instances.append(instance)
        return cluster.grow(instances)

    def _action_shrink_cluster(self, cluster, body):
        nodes = body['shrink']
        instance_ids = [node['id'] for node in nodes]
        return cluster.shrink(instance_ids)

    @property
    def cluster_view_class(self):
        return CouchbaseClusterView

    @property
    def mgmt_cluster_view_class(self):
        return CouchbaseMgmtClusterView


class CouchbaseCluster(models.Cluster):

    DEFAULT_SERVICES = "data"
    MAX_PASSWORD_LEN = 24

    @classmethod
    def create(cls, context, name, datastore, datastore_version,
               instances, extended_properties, locality):
        LOG.debug("Processing a request for creating a new cluster.")

        # Updating Cluster Task.
        db_info = models.DBCluster.create(
            name=name, tenant_id=context.tenant,
            datastore_version_id=datastore_version.id,
            task_status=ClusterTasks.BUILDING_INITIAL)

        cls._create_cluster_instances(
            context, db_info.id, db_info.name,
            datastore, datastore_version, instances, extended_properties,
            locality)

        # Calling taskmanager to further proceed for cluster-configuration.
        task_api.load(context, datastore_version.manager).create_cluster(
            db_info.id)

        return CouchbaseCluster(context, db_info, datastore, datastore_version)

    @classmethod
    def _create_cluster_instances(
            cls, context, cluster_id, cluster_name,
            datastore, datastore_version, instances, extended_properties,
            locality):
        LOG.debug("Processing a request for new cluster instances.")

        cluster_node_ids = CouchbaseClusterTasks.find_cluster_node_ids(
            cluster_id)

        cluster_password = None

        # Couchbase imposes cluster wide quota on the memory that get
        # evenly distributed between node services.
        # All nodes (including future nodes) need to be able to accommodate
        # this quota.
        # We therefore require the cluster to be homogeneous.

        # Load the flavor and volume information from the existing instances
        # if any.
        # Generate the administrative password for a new cluster or reuse the
        # one from an existing cluster.
        required_instance_flavor = None
        required_volume_size = None
        if cluster_node_ids:
            cluster_nodes = CouchbaseClusterTasks.load_cluster_nodes(
                context,
                cluster_node_ids)
            coordinator = cluster_nodes[0]
            required_instance_flavor = coordinator['instance'].flavor_id
            required_volume_size = coordinator['instance'].volume_size

            cluster_password = coordinator['guest'].get_cluster_password()
        else:
            pwd_len = min(cls.MAX_PASSWORD_LEN, CONF.default_password_length)
            cluster_password = utils.generate_random_password(pwd_len)

        models.assert_homogeneous_cluster(
            instances,
            required_flavor=required_instance_flavor,
            required_volume_size=required_volume_size)

        couchbase_conf = CONF.get(datastore_version.manager)
        eph_enabled = couchbase_conf.device_path
        vol_enabled = couchbase_conf.volume_support

        # Validate instance flavors.
        models.get_flavors_from_instance_defs(context, instances,
                                              vol_enabled, eph_enabled)

        # Compute the total volume allocation.
        req_volume_size = models.get_required_volume_size(instances,
                                                          vol_enabled)

        # Check requirements against quota.
        num_new_instances = len(instances)
        deltas = {'instances': num_new_instances, 'volumes': req_volume_size}
        check_quotas(context.tenant, deltas)

        # Creating member instances.
        num_instances = len(cluster_node_ids)
        new_instances = []
        for instance_idx, instance in enumerate(instances, num_instances + 1):
            instance_az = instance.get('availability_zone', None)

            member_config = {"id": cluster_id,
                             "instance_type": "member",
                             "cluster_password": cluster_password}

            instance_name = instance.get('name')
            if not instance_name:
                instance_name = cls._build_instance_name(
                    cluster_name, cls.DEFAULT_SERVICES, instance_az,
                    instance_idx)

            new_instance = inst_models.Instance.create(
                context, instance_name,
                instance['flavor_id'],
                datastore_version.image_id,
                [], [],
                datastore, datastore_version,
                instance['volume_size'], None,
                nics=instance.get('nics', None),
                availability_zone=instance_az,
                configuration_id=None,
                cluster_config=member_config,
                locality=locality)

            new_instances.append(new_instance)

        return new_instances

    @classmethod
    def _build_instance_name(cls, cluster_name, service, group, instance_idx):
        name_components = [cluster_name, service, 'services']
        if group:
            name_components.append(group)
        name_components.append(str(instance_idx))

        return '-'.join(name_components)

    def grow(self, instances):
        LOG.debug("Processing a request for growing cluster: %s" % self.id)

        self.validate_cluster_available()

        context = self.context
        db_info = self.db_info
        datastore = self.ds
        datastore_version = self.ds_version

        db_info.update(task_status=ClusterTasks.GROWING_CLUSTER)

        locality = srv_grp.ServerGroup.convert_to_hint(self.server_group)
        new_instances = self._create_cluster_instances(
            context, db_info.id, db_info.name, datastore, datastore_version,
            instances, None, locality)

        task_api.load(context, datastore_version.manager).grow_cluster(
            db_info.id, [instance.id for instance in new_instances])

        return CouchbaseCluster(context, db_info, datastore, datastore_version)

    def shrink(self, removal_ids):
        LOG.debug("Processing a request for shrinking cluster: %s" % self.id)

        self.validate_cluster_available()

        context = self.context
        db_info = self.db_info
        datastore = self.ds
        datastore_version = self.ds_version

        db_info.update(task_status=ClusterTasks.SHRINKING_CLUSTER)

        task_api.load(context, datastore_version.manager).shrink_cluster(
            db_info.id, removal_ids)

        return CouchbaseCluster(context, db_info, datastore, datastore_version)


class CouchbaseClusterView(ClusterView):

    def build_instances(self):
        return self._build_instances(['member'], ['member'])


class CouchbaseMgmtClusterView(MgmtClusterView):

    def build_instances(self):
        return self._build_instances(['member'], ['member'])

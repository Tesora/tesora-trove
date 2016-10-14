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

from eventlet.timeout import Timeout
import six

import netaddr
from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common.notification import EndNotification
from trove.common.strategies.cluster import base
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.taskmanager import api as task_api
import trove.taskmanager.models as task_models


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
USAGE_SLEEP_TIME = CONF.usage_sleep_time  # seconds.


class CouchbaseTaskManagerStrategy(base.BaseTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return CouchbaseTaskManagerAPI

    @property
    def task_manager_cluster_tasks_class(self):
        return CouchbaseClusterTasks


class CouchbaseClusterTasks(task_models.ClusterTasks):

    # These define the validation rules for the different managers
    VALID_SERVICES = {
        "couchbase": [""],
        "couchbase_4": ["data", "index", "query"],
        "couchbase_ee": ["data", "index", "query"],
    }
    REQUIRED_SERVICES = {
        "couchbase": [],
        "couchbase_4": ["data", "index", "query"],
        "couchbase_ee": ["data"],
    }
    REQUIRE_ALL_SERVICES = {
        "couchbase": True,
        "couchbase_4": True,
        "couchbase_ee": False,
    }
    # This defines which instances which are displayed with cluster-instances
    # command.  By default, all valid services are displayed as defined
    # in all the managers.
    EXPOSED_SERVICES = set(
        item for sublist in
        [VALID_SERVICES[key] for key in VALID_SERVICES.keys()]
        for item in sublist)

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s." % cluster_id)

        def _create_cluster():
            cluster_node_ids = self.find_cluster_node_ids(cluster_id)

            # Wait for cluster nodes to get to cluster-ready status.
            LOG.debug("Waiting for all nodes to become ready.")
            if not self._all_instances_ready(cluster_node_ids, cluster_id):
                return

            cluster_nodes = self.load_cluster_nodes(context, cluster_node_ids)
            coordinator = self._get_coordinator_node(cluster_nodes)

            LOG.debug("Initializing the cluster on node '%s'."
                      % coordinator['ip'])

            # start with the coordinator as it will have all the required
            # services.
            guest_node_info = self.build_guest_node_info([coordinator])
            # now add all the other nodes so we can get a list of all services
            # needed to calculate the memory allocation properly.
            add_node_info = [node for node in cluster_nodes
                             if node != coordinator]
            guest_node_info.extend(self.build_guest_node_info(add_node_info))
            coordinator['guest'].initialize_cluster(guest_node_info)

            self._add_nodes(coordinator, add_node_info)
            coordinator['guest'].cluster_complete()
            LOG.debug("Cluster create finished successfully.")

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            with EndNotification(context, cluster_id=cluster_id):
                _create_cluster()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception(_("Timeout for building cluster."))
            self.update_statuses_on_failure(cluster_id)
        except Exception:
            LOG.exception(_("Error creating cluster."))
            self.update_statuses_on_failure(cluster_id)
            raise
        finally:
            self.reset_task()
            timeout.cancel()

        LOG.debug("End create_cluster for id: %s." % cluster_id)

    def _get_coordinator_node(self, node_info):
        # The coordinator node must be one with all the required services.
        # If we can't find one, then we can't continue - this would
        # likely indicate a hole in the validation process.
        for node in node_info:
            manager = node['instance'].datastore_version.manager
            guest_node_info = self.build_guest_node_info([node])
            if set(self.REQUIRED_SERVICES[manager]).issubset(
                    guest_node_info[0]['services']):
                return node
        raise exception.TroveError(
            _("Could not find instance with all required types (%s).") %
            ','.join(self.REQUIRED_SERVICES[manager]))

    def _add_nodes(self, coordinator, add_node_info):
        LOG.debug("Adding nodes and rebalancing the cluster.")
        guest_node_info = self.build_guest_node_info(add_node_info)
        result = coordinator['guest'].add_nodes(guest_node_info)
        if not result or len(result) < 2:
            raise exception.TroveError(
                _("No status returned from adding nodes to cluster."))

        if result[0]:
            LOG.debug("Marking added nodes active.")
            for node in add_node_info:
                node['guest'].cluster_complete()
        else:
            raise exception.TroveError(
                _("Could not add nodes to cluster: %s") % result[1])

    @classmethod
    def find_cluster_node_ids(cls, cluster_id):
        db_instances = DBInstance.find_all(cluster_id=cluster_id).all()
        return [db_instance.id for db_instance in db_instances]

    @classmethod
    def load_cluster_nodes(cls, context, node_ids):
        return [cls.build_node_info(Instance.load(context, node_id))
                for node_id in node_ids]

    @classmethod
    def build_node_info(cls, instance):
        guest = cls.get_guest(instance)
        ip = None
        ips = instance.get_visible_ip_addresses()
        if ips:
            ipv4s = [i for i in ips if netaddr.valid_ipv4(i)]
            if ipv4s:
                ip = ipv4s[0]
        return {'instance': instance,
                'guest': guest,
                'id': instance.id,
                'ip': ip}

    @classmethod
    def build_guest_node_info(cls, node_info):
        # This is the node_info that will be sent down to the guest.
        guest_node_info = []
        for node in node_info:
            services = node['instance'].type
            if isinstance(services, six.string_types):
                services = services.split(',')
            guest_node = {
                'host': node['ip'],
                'services': services
            }
            guest_node_info.append(guest_node)

        return guest_node_info

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        LOG.debug("Begin grow_cluster for id: %s." % cluster_id)

        def _grow_cluster():
            # Wait for new nodes to get to cluster-ready status.
            LOG.debug("Waiting for new nodes to become ready.")
            if not self._all_instances_ready(new_instance_ids, cluster_id):
                return

            new_instances = [Instance.load(context, instance_id)
                             for instance_id in new_instance_ids]
            add_node_info = [self.build_node_info(instance)
                             for instance in new_instances]

            LOG.debug("All nodes ready, proceeding with cluster setup.")

            cluster_node_ids = self.find_cluster_node_ids(cluster_id)
            cluster_nodes = self.load_cluster_nodes(context, cluster_node_ids)

            old_node_info = [node for node in cluster_nodes
                             if node['id'] not in new_instance_ids]

            # Rebalance the cluster via one of the existing nodes.
            # Clients can continue to store and retrieve information and
            # do not need to be aware that a rebalance operation is taking
            # place.
            coordinator = old_node_info[0]
            self._add_nodes(coordinator, add_node_info)
            LOG.debug("Cluster grow finished successfully.")

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            with EndNotification(context, cluster_id=cluster_id):
                _grow_cluster()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception(_("Timeout for growing cluster."))
        except Exception:
            LOG.exception(_("Error growing cluster."))
            raise
        finally:
            self.reset_task()
            timeout.cancel()

        LOG.debug("End grow_cluster for id: %s." % cluster_id)

    def shrink_cluster(self, context, cluster_id, removal_ids):
        LOG.debug("Begin shrink_cluster for id: %s." % cluster_id)

        def _shrink_cluster():
            cluster_node_ids = self.find_cluster_node_ids(cluster_id)
            cluster_nodes = self.load_cluster_nodes(context, cluster_node_ids)

            remove_node_info = CouchbaseClusterTasks.load_cluster_nodes(
                context, removal_ids)

            remaining_node_info = [node for node in cluster_nodes
                                   if node['id'] not in removal_ids]

            LOG.debug("All nodes ready, proceeding with cluster setup.")

            # Rebalance the cluster via one of the remaining nodes.
            coordinator = remaining_node_info[0]
            self._remove_nodes(coordinator, remove_node_info)
            LOG.debug("Cluster shrink finished successfully.")

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            with EndNotification(context, cluster_id=cluster_id):
                _shrink_cluster()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception(_("Timeout for shrinking cluster."))
        except Exception:
            LOG.exception(_("Error shrinking cluster."))
            raise
        finally:
            self.reset_task()
            timeout.cancel()

        LOG.debug("End shrink_cluster for id: %s." % cluster_id)

    def _remove_nodes(self, coordinator, removed_nodes):
        LOG.debug("Decommissioning nodes and rebalancing the cluster.")
        guest_node_info = self.build_guest_node_info(removed_nodes)
        result = coordinator['guest'].remove_nodes(guest_node_info)
        if not result or len(result) < 2:
            raise exception.TroveError(
                _("No status returned from removing nodes from cluster."))

        if result[0]:
            for node in removed_nodes:
                instance = node['instance']
                LOG.debug("Deleting decommissioned instance %s." %
                          instance.id)
                instance.update_db(cluster_id=None)
                Instance.delete(instance)
        else:
            raise exception.TroveError(
                _("Could not remove nodes from cluster: %s") % result[1])

    def upgrade_cluster(self, context, cluster_id, datastore_version):
        self.rolling_upgrade_cluster(context, cluster_id, datastore_version)


class CouchbaseTaskManagerAPI(task_api.API):
    pass

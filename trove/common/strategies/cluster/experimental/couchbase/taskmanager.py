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
from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster import base
from trove.common import utils
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

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s." % cluster_id)

        def _create_cluster():
            cluster_node_ids = self.find_cluster_node_ids(cluster_id)

            # Wait for cluster nodes to get to cluster-ready status.
            LOG.debug("Waiting for all nodes to become ready.")
            if not self._all_instances_ready(cluster_node_ids, cluster_id):
                return

            cluster_nodes = self.load_cluster_nodes(context, cluster_node_ids)
            coordinator = cluster_nodes[0]

            LOG.debug("Initializing the cluster on node '%s'."
                      % coordinator['ip'])
            coordinator['guest'].initialize_cluster()

            added_nodes = [node for node in cluster_nodes
                           if node != coordinator]

            self._add_nodes(coordinator, added_nodes)

            coordinator['guest'].cluster_complete()

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

    def _add_nodes(self, coordinator, added_nodes):
        LOG.debug("Adding nodes and rebalacing the cluster.")
        coordinator['guest'].add_nodes({node['ip'] for node in added_nodes})

        LOG.debug("Waiting for the rebalancing process to finish.")
        self._wait_for_rebalance_to_finish(coordinator)

        LOG.debug("Marking added nodes active.")
        for node in added_nodes:
            node['guest'].cluster_complete()

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
        return {'instance': instance,
                'guest': guest,
                'id': instance.id,
                'ip': cls.get_ip(instance)}

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        LOG.debug("Begin grow_cluster for id: %s." % cluster_id)

        def _grow_cluster():
            # Wait for new nodes to get to cluster-ready status.
            LOG.debug("Waiting for new nodes to become ready.")
            if not self._all_instances_ready(new_instance_ids, cluster_id):
                return

            new_instances = [Instance.load(context, instance_id)
                             for instance_id in new_instance_ids]
            added_nodes = [self.build_node_info(instance)
                           for instance in new_instances]

            LOG.debug("All nodes ready, proceeding with cluster setup.")

            cluster_node_ids = self.find_cluster_node_ids(cluster_id)
            cluster_nodes = self.load_cluster_nodes(context, cluster_node_ids)

            old_nodes = [node for node in cluster_nodes
                         if node['id'] not in new_instance_ids]

            # Rebalance the cluster via one of the existing nodes.
            # Clients can continue to store and retrieve information and
            # do not need to be aware that a rebalance operation is taking
            # place.
            # The new nodes are marked active only if the rebalancing
            # completes.
            try:
                coordinator = old_nodes[0]
                self._add_nodes(coordinator, added_nodes)
                LOG.debug("Cluster configuration finished successfully.")
            except Exception:
                LOG.exception(_("Error growing cluster."))
                self.update_statuses_on_failure(cluster_id)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _grow_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception(_("Timeout for growing cluster."))
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End grow_cluster for id: %s." % cluster_id)

    def _wait_for_rebalance_to_finish(self, coordinator):
        try:
            utils.poll_until(
                lambda: coordinator['guest'],
                lambda node: not node.get_cluster_rebalance_status(),
                sleep_time=USAGE_SLEEP_TIME,
                time_out=CONF.cluster_usage_timeout)
        except exception.PollTimeOut as e:
            LOG.exception(e)
            raise exception.TroveError(_("Timed out while waiting for the "
                                         "rebalancing process to finish."))

    def shrink_cluster(self, context, cluster_id, removal_ids):
        LOG.debug("Begin shrink_cluster for id: %s." % cluster_id)

        def _shrink_cluster():
            cluster_node_ids = self.find_cluster_node_ids(cluster_id)
            cluster_nodes = self.load_cluster_nodes(context, cluster_node_ids)

            removed_nodes = CouchbaseClusterTasks.load_cluster_nodes(
                context, removal_ids)

            remaining_nodes = [node for node in cluster_nodes
                               if node['id'] not in removal_ids]

            LOG.debug("All nodes ready, proceeding with cluster setup.")

            # Rebalance the cluster via one of the remaining nodes.
            try:
                coordinator = remaining_nodes[0]
                self._remove_nodes(coordinator, removed_nodes)
                LOG.debug("Cluster configuration finished successfully.")
            except Exception:
                LOG.exception(_("Error shrinking cluster."))
                self.update_statuses_on_failure(cluster_id)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _shrink_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception(_("Timeout for shrinking cluster."))
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End shrink_cluster for id: %s." % cluster_id)

    def _remove_nodes(self, coordinator, removed_nodes):
        LOG.debug("Decommissioning nodes and rebalacing the cluster.")
        coordinator['guest'].remove_nodes({node['ip']
                                           for node in removed_nodes})

        # Always remove decommissioned instances from the cluster,
        # irrespective of the result of rebalancing.
        for node in removed_nodes:
            node['instance'].update_db(cluster_id=None)

        LOG.debug("Waiting for the rebalancing process to finish.")
        self._wait_for_rebalance_to_finish(coordinator)

        # Delete decommissioned instances only when the cluster is in a
        # consistent state.
        LOG.debug("Deleting decommissioned instances.")
        for node in removed_nodes:
            Instance.delete(node['instance'])


class CouchbaseTaskManagerAPI(task_api.API):
    pass

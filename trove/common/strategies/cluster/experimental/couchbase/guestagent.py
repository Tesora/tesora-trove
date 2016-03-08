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

from trove.common import cfg
from trove.common.strategies.cluster import base
from trove.guestagent import api as guest_api


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class CouchbaseGuestAgentStrategy(base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return CouchbaseGuestAgentAPI


class CouchbaseGuestAgentAPI(guest_api.API):

    def initialize_cluster(self):
        LOG.debug("Configuring cluster parameters via node: %s" % self.id)
        self._call("initialize_cluster",
                   guest_api.AGENT_HIGH_TIMEOUT, self.version_cap)

    def get_cluster_password(self):
        LOG.debug("Retrieving cluster password from node: %s" % self.id)
        return self._call("get_cluster_password",
                          guest_api.AGENT_LOW_TIMEOUT, self.version_cap)

    def get_cluster_rebalance_status(self):
        LOG.debug("Retrieving status of current cluster rebalance via node: %s"
                  % self.id)
        return self._call("get_cluster_rebalance_status",
                          guest_api.AGENT_LOW_TIMEOUT, self.version_cap)

    def add_nodes(self, nodes):
        LOG.debug("Adding nodes to the cluster: %s" % self.id)
        self._cast('add_nodes', self.version_cap, nodes=nodes)

    def remove_nodes(self, nodes):
        LOG.debug("Removing nodes from the cluster: %s" % self.id)
        self._cast('remove_nodes', self.version_cap, nodes=nodes)

    def cluster_complete(self):
        LOG.debug("Sending a setup completion notification for node: %s"
                  % self.id)
        return self._call("cluster_complete", guest_api.AGENT_HIGH_TIMEOUT,
                          self.version_cap)

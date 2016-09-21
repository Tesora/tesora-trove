# Copyright 2016 Tesora Inc.
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

import collections

from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.guestagent.datastore.experimental.couchbase import (
    service as community_service
)


LOG = logging.getLogger(__name__)


class Couchbase4App(community_service.CouchbaseApp):

    def initialize_cluster(self, enabled_services=None):
        enabled_services = (enabled_services or
                            cfg.get_configuration_property('default_services'))
        LOG.info(_("Enabling Couchbase services: %s") % enabled_services)
        self.build_admin().run_cluster_init(self.ramsize_quota_mb,
                                            enabled_services)

    def rebalance_cluster(self, added_nodes=None, removed_nodes=None,
                          enabled_services=None):
        enabled_services = (enabled_services or
                            cfg.get_configuration_property('default_services'))
        LOG.info(_("Enabling Couchbase services: %s") % enabled_services)
        self.build_admin().run_rebalance(added_nodes, removed_nodes,
                                         enabled_services)

    def build_admin(self):
        return Couchbase4Admin(self.get_cluster_admin())


class Couchbase4Admin(community_service.CouchbaseAdmin):

    # How much of the total cluster memory quota will be allocated to the
    # indexing service.
    INDEX_MEM_RATIO = 0.25

    def run_cluster_init(self, ramsize_quota_mb, enabled_services):
        LOG.debug("Configuring cluster parameters.")
        data_quota_mb, index_quota_mb = self._compute_mem_allocations_mb(
            ramsize_quota_mb, enabled_services)

        options = {'cluster-init-username': self._user.name,
                   'cluster-init-password': self._user.password,
                   'cluster-init-port': self._http_client_port,
                   'cluster-ramsize': data_quota_mb,
                   'services': ','.join(enabled_services)}

        if index_quota_mb > 0:
            options.update({'cluster-index-ramsize': index_quota_mb})

        self._run_couchbase_command('cluster-init', options)

    def _compute_mem_allocations_mb(self, ramsize_quota_mb, enabled_services):
        """Couchbase 4.x and higher split the available memory quota between
        data and index services.
        If the indexing service is turned on the quota value must be at least
        256MB.

        Compute the index quota as 25% of the total and use the rest for data
        services. Return '0' quota if the service is not enabled.
        """
        if 'index' in enabled_services:
            index_quota_mb = max(int(self.INDEX_MEM_RATIO * ramsize_quota_mb),
                                 Couchbase4App.MIN_RAMSIZE_QUOTA_MB)
        else:
            index_quota_mb = 0

        data_quota_mb = ramsize_quota_mb - index_quota_mb

        if data_quota_mb < Couchbase4App.MIN_RAMSIZE_QUOTA_MB:
            required = Couchbase4App.MIN_RAMSIZE_QUOTA_MB - data_quota_mb
            raise TroveError(_("Not enough memory for Couchbase services. "
                               "Additional %dMB is required.") % required)

        return data_quota_mb, index_quota_mb

    def run_rebalance(self, added_nodes, removed_nodes, enabled_services):
        LOG.debug("Rebalancing the cluster.")
        options = []
        if added_nodes:
            for node_ip in added_nodes:
                options.append(
                    collections.OrderedDict([
                        ('server-add', node_ip),
                        ('server-add-username', self._user.name),
                        ('server-add-password', self._user.password)]))
            options.append({'services': ','.join(enabled_services)})

        if removed_nodes:
            options.append({'server-remove': removed_nodes})

        if options:
            self._run_couchbase_command('rebalance', options)
        else:
            LOG.info(_("No changes to the topology, skipping rebalance."))

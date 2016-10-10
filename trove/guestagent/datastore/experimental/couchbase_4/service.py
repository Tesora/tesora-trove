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

from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.guestagent.datastore.experimental.couchbase import (
    service as community_service
)


LOG = logging.getLogger(__name__)


class Couchbase4App(community_service.CouchbaseApp):

    def build_admin(self):
        return Couchbase4Admin(self.get_cluster_admin())


class Couchbase4Admin(community_service.CouchbaseAdmin):

    # How much of the total cluster memory quota will be allocated to the
    # indexing service.
    INDEX_MEM_RATIO = 0.25

    def get_cluster_init_options(self, node_info, ramsize_quota_mb):
        init_options = super(Couchbase4Admin, self).get_cluster_init_options(
            node_info, ramsize_quota_mb)
        if node_info:
            services = node_info[0].get('services')
            # get all services
            service_lists = [node.get('services') for node in node_info]
            all_services = set(
                [item for subtypes in service_lists for item in subtypes])
        else:
            # Use datastore defaults if no node_info is provided
            # (i.e. during single instance provisioning).
            services = cfg.get_configuration_property('default_services')
            all_services = services

        data_quota_mb, index_quota_mb = self._compute_mem_allocations_mb(
            ramsize_quota_mb, all_services)
        init_options['cluster-ramsize'] = data_quota_mb
        init_options['cluster-index-ramsize'] = index_quota_mb
        if services:
            if isinstance(services, list):
                services = ','.join(services)
            init_options['service'] = services

        return init_options

    def _compute_mem_allocations_mb(self, ramsize_quota_mb, enabled_services):
        """Couchbase 4.x and higher split the available memory quota between
        data and index services.
        If the indexing service is turned on the quota value must be at least
        256MB.

        Compute the index quota as 25% of the total and use the rest for data
        services. Return '256' quota (the min) if the service is not enabled.
        """
        if 'index' in enabled_services:
            index_quota_mb = max(int(self.INDEX_MEM_RATIO * ramsize_quota_mb),
                                 Couchbase4App.MIN_RAMSIZE_QUOTA_MB)
        else:
            index_quota_mb = Couchbase4App.MIN_RAMSIZE_QUOTA_MB

        data_quota_mb = ramsize_quota_mb - index_quota_mb

        if data_quota_mb < Couchbase4App.MIN_RAMSIZE_QUOTA_MB:
            required = Couchbase4App.MIN_RAMSIZE_QUOTA_MB - data_quota_mb
            raise TroveError(_("Not enough memory for Couchbase services. "
                               "Additional %dMB is required.") % required)

        return data_quota_mb, index_quota_mb

    def get_cluster_add_options(self, node_info):
        add_options = super(Couchbase4Admin, self).get_cluster_add_options(
            node_info)
        for index, node in enumerate(node_info):
            services = node.get('services')
            if services:
                options = add_options[index]
                if isinstance(services, list):
                    services = ','.join(services)
                options['services'] = services

        return add_options

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

    def build_admin(self):
        return Couchbase4Admin(self.get_cluster_admin())


class Couchbase4Admin(community_service.CouchbaseAdmin):

    def run_cluster_init(self, ramsize_quota_mb, enabled_services):
        LOG.debug("Configuring cluster parameters.")
        self._run_couchbase_command(
            'cluster-init', {'cluster-init-username': self._user.name,
                             'cluster-init-password': self._user.password,
                             'cluster-init-port': self._http_client_port,
                             'cluster-ramsize': ramsize_quota_mb,
                             'services': ','.join(enabled_services)})

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

from trove.guestagent.datastore.experimental.cassandra_3 import (
    service as community_service
)


LOG = logging.getLogger(__name__)


class Cassandra22App(community_service.Cassandra3App):

    def __init__(self):
        super(Cassandra22App, self).__init__()

    def apply_initial_guestagent_configuration(self, cluster_name=None):
        super(Cassandra22App, self).apply_initial_guestagent_configuration(
            cluster_name=cluster_name)

        # As of Cassandra 2.2, there is no security manager or
        # anything else in place to prevent insecure operations such as
        # opening a socket or writing to the filesystem.
        #
        # We disable UDFs explicitly to protect the guest.
        updates = {'enable_user_defined_functions': False}
        self.configuration_manager.apply_system_override(updates)

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

from trove.common.db.cassandra import models
from trove.guestagent.datastore.experimental.cassandra import (
    service as community_service
)


LOG = logging.getLogger(__name__)


class Cassandra3App(community_service.CassandraApp):

    def __init__(self):
        super(Cassandra3App, self).__init__()

    def apply_initial_guestagent_configuration(self, cluster_name=None):
        super(Cassandra3App, self).apply_initial_guestagent_configuration(
            cluster_name=cluster_name)

        updates = {'role_manager': 'CassandraRoleManager',
                   'listen_on_broadcast_address': False}
        self.configuration_manager.apply_system_override(updates)

    def _reset_user_password_to_default(self, username):
        LOG.debug("Resetting the password of user '%s' to '%s'."
                  % (username, self.default_superuser_password))

        user = models.CassandraUser(username, self.default_superuser_password)
        with community_service.CassandraLocalhostConnection(user) as client:
            client.execute(
                "UPDATE system_auth.roles SET salted_hash=%s "
                "WHERE role='{}';", (user.name,),
                (self.default_superuser_pwd_hash,))

            return user

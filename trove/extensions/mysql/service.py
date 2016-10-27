# Copyright 2011 OpenStack Foundation
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

from urllib import unquote

from oslo_log import log as logging
from oslo_utils import importutils

from trove.common import cfg
from trove.extensions.common.service import DatastoreDatabaseController
from trove.extensions.common.service import DatastoreUserAccessController
from trove.extensions.common.service import DatastoreUserController
from trove.extensions.common.service import DefaultRootController

from trove.extensions.mysql import views as mysql_views
from trove.guestagent.db import models as guest_models


LOG = logging.getLogger(__name__)
import_class = importutils.import_class
CONF = cfg.CONF


class MySQLUserController(DatastoreUserController):

    def is_reserved_id(self, user_id):
        user_id = self._to_canonical(user_id)
        return user_id in cfg.get_ignored_users(manager='mysql')

    def _to_canonical(self, user_id):
        username, hostname = self.parse_user_id(user_id)
        hostname = hostname or '%'
        return '%s@%s' % (username, hostname)

    def build_model_view(self, user_model):
        return mysql_views.UserView(user_model)

    def build_model_collection_view(self, user_models):
        return mysql_views.UsersView(user_models)

    def parse_user_from_response(self, user_data):
        user_model = guest_models.MySQLUser()
        user_model.deserialize(user_data)
        return user_model

    def parse_user_from_request(self, user_data):
        name = user_data['name']
        host = user_data.get('host', '%')
        password = user_data['password']
        databases = user_data.get('databases', [])
        user_model = guest_models.MySQLUser()
        user_model.name = name
        user_model.host = host
        user_model.password = password
        for db in databases:
            user_model.databases = db['name']
        return user_model

    def get_user_id(self, user_model):
        return '%s@%s' % (user_model.name, user_model.host)

    def parse_user_id(self, user_id):
        unquoted = unquote(user_id)
        if '@' not in unquoted:
            return unquoted, '%'
        if unquoted.endswith('@'):
            return unquoted, '%'
        splitup = unquoted.split('@')
        host = splitup[-1]
        user = '@'.join(splitup[:-1])
        return user, host

    def apply_user_updates(self, user_model, updates):
        id_changed = False
        updated_name = updates.get('name')
        if updated_name is not None:
            user_model.name = updated_name
            id_changed = True
        updated_host = updates.get('host')
        if updated_host is not None:
            user_model.host = updated_host
            id_changed = True
        updated_password = updates.get('password')
        if updated_password is not None:
            user_model.password = updated_password

        return self.get_user_id(user_model) if id_changed else None

    def change_passwords(self, client, user_models):
        change_users = []
        for user in user_models:
            change_user = {'name': user.name,
                           'host': user.host,
                           'password': user.password,
                           }
            change_users.append(change_user)
        return client.change_passwords(users=change_users)


class MySQLDatabaseController(DatastoreDatabaseController):

    def is_reserved_id(self, database_id):
        return database_id in cfg.get_ignored_dbs(manager='mysql')

    def build_model_view(self, database_model):
        return mysql_views.DatabaseView(database_model)

    def build_model_collection_view(self, database_models):
        return mysql_views.DatabasesView(database_models)

    def parse_database_from_response(self, database_data):
        database_model = guest_models.MySQLDatabase()
        database_model.deserialize(database_data)
        return database_model

    def parse_database_from_request(self, database_data):
        name = database_data['name']
        database_model = guest_models.ValidatedMySQLDatabase()
        database_model.name = name
        return database_model


class MySQLUserAccessController(DatastoreUserAccessController):

    @property
    def user_controller(self):
        return MySQLUserController()

    @property
    def database_controller(self):
        return MySQLDatabaseController()


class MySQLRootController(DefaultRootController):
    pass

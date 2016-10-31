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


from trove.extensions.common import service
from trove.extensions.mongodb import views
from trove.guestagent.db import models as guest_models


class MongoDBUserController(service.DatastoreUserController):

    def build_model_view(self, user_model):
        return views.UserView(user_model)

    def build_model_collection_view(self, user_models):
        return views.UsersView(user_models)

    def parse_user_from_request(self, user_data):
        name = user_data['name']
        password = user_data['password']
        databases = user_data.get('databases', [])
        user_model = guest_models.MongoDBUser(name, password)
        for db in databases:
            user_model.databases = db['name']
        return user_model

    def parse_user_from_response(self, user_data):
        return guest_models.MongoDBUser.deserialize_user(user_data)


class MongoDBDatabaseController(service.DatastoreDatabaseController):

    def build_model_view(self, database_model):
        return views.DatabaseView(database_model)

    def build_model_collection_view(self, database_models):
        return views.DatabasesView(database_models)

    def parse_database_from_response(self, database_data):
        return guest_models.MongoDBSchema.deserialize_schema(database_data)

    def parse_database_from_request(self, database_data):
        return guest_models.MongoDBSchema(database_data['name'])


class MongoDBUserAccessController(service.DatastoreUserAccessController):

    def assert_database_show(self, req, tenant_id, instance_id, database_id):
        pass

    @property
    def user_controller(self):
        return MongoDBUserController()

    @property
    def database_controller(self):
        return MongoDBDatabaseController()

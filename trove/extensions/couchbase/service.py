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


from trove.extensions.common.service import DatastoreUserController
from trove.extensions.couchbase import views as couchbase_views
from trove.guestagent.db import models as guest_models


class CouchbaseUserController(DatastoreUserController):

    def build_model_view(self, user_model):
        return couchbase_views.UserView(user_model)

    def build_model_collection_view(self, user_models):
        return couchbase_views.UsersView(user_models)

    def parse_user_from_response(self, user_data):
        return guest_models.CouchbaseUser.deserialize_user(user_data)

    def parse_user_from_request(self, user_data):
        name = user_data['name']
        password = user_data['password']
        return guest_models.CouchbaseUser(name, password)

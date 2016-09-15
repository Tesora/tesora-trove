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


from trove.common import exception
from trove.common.i18n import _LE
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
        roles = user_data.get('roles')
        bucket_ramsize = user_data.get('bucket_ramsize')
        bucket_replica = user_data.get('bucket_replica')
        enable_index_replica = user_data.get('enable_index_replica')
        bucket_eviction_policy = user_data.get('bucket_eviction_policy')
        bucket_priority = user_data.get('bucket_priority')
        return guest_models.CouchbaseUser(
            name, password,
            roles=roles,
            bucket_ramsize_mb=bucket_ramsize,
            bucket_replica_count=bucket_replica,
            enable_index_replica=enable_index_replica,
            bucket_eviction_policy=bucket_eviction_policy,
            bucket_priority=bucket_priority)

    def apply_user_updates(self, user_model, updates):

        # When editing buckets, be sure to always specify all properties.
        # Couchbase Server may otherwise reset the property value to default.

        def get_attribute(name):
            value = updates.get(name)
            if value is None:
                raise exception.MissingKey(
                    _LE("Specify all user properties."))

            return value

        if 'name' in updates:
            raise exception.BadRequest(
                _LE("Couchbase users cannot be renamed."))

        user_model.password = get_attribute('password')
        user_model.bucket_ramsize_mb = get_attribute('bucket_ramsize')
        user_model.bucket_replica_count = get_attribute('bucket_replica')
        user_model.enable_index_replica = get_attribute('enable_index_replica')
        user_model.bucket_eviction_policy = get_attribute(
            'bucket_eviction_policy')
        user_model.bucket_priority = get_attribute('bucket_priority')

        # Couchbase buckets cannot be renamed, the ID hence never changes.
        return None

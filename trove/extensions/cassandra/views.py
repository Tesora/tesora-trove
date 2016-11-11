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

from trove.extensions.common import views as common_views
from trove.guestagent.db import models as guest_models


class DatabaseView(common_views.SingleModelView):

    def __init__(self, schema):
        super(DatabaseView, self).__init__('database', schema)

    @classmethod
    def deserialize_model(cls, schema):
        return {'name': schema.name}


class DatabasesView(common_views.ModelCollectionView):

    def __init__(self, schemas):
        super(DatabasesView, self).__init__('databases', schemas)

    @classmethod
    def deserialize_model(cls, schema):
        return DatabaseView.deserialize_model(schema)


class UserView(common_views.SingleModelView):

    def __init__(self, user):
        super(UserView, self).__init__('user', user)

    @classmethod
    def deserialize_model(cls, user):
        item = {
            "name": user.name,
        }
        # Database models are stored in serialized form.
        schema_models = [guest_models.CassandraSchema.deserialize_schema(db)
                         for db in user.databases]
        item.update(DatabasesView(schema_models).data())
        return item


class UsersView(common_views.ModelCollectionView):

    def __init__(self, users):
        super(UsersView, self).__init__('users', users)

    @classmethod
    def deserialize_model(cls, user):
        return UserView.deserialize_model(user)

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


class UserView(common_views.SingleModelView):

    def __init__(self, user):
        super(UserView, self).__init__('user', user)

    @classmethod
    def deserialize_model(cls, user):
        return {
            "name": user.name,
            "bucket_ramsize": user.bucket_ramsize_mb,
            "bucket_replica": user.bucket_replica_count,
            "used_ram": user.used_ram_mb
        }


class UsersView(common_views.ModelCollectionView):

    def __init__(self, users):
        super(UsersView, self).__init__('users', users)

    @classmethod
    def deserialize_model(cls, user):
        return UserView.deserialize_model(user)

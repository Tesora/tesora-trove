# Copyright [2015] Hewlett-Packard Development Company, L.P.
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

from trove.common import pagination


class View(object):

    def __init__(self, name):
        self.name = name

    def data(self):
        pass

    @classmethod
    def deserialize_model(cls, model_object):
        pass


class SingleModelView(View):

    def __init__(self, name, model_object):
        super(SingleModelView, self).__init__(name)
        self.model_object = model_object

    def data(self):
        return {self.name: self.deserialize_model(self.model_object)}


class ModelCollectionView(View):

    def __init__(self, name, model_collection):
        super(ModelCollectionView, self).__init__(name)
        self.model_collection = model_collection

    def data(self):
        items = [self.deserialize_model(model_object)
                 for model_object in self.model_collection]
        return {self.name: items}

    def paginated(self, url, next_marker):
        return pagination.SimplePaginatedDataView(
            url, self.name, self, next_marker)


class RootCreatedView(SingleModelView):

    def __init__(self, user):
        super(RootCreatedView, self).__init__('user', user)

    @classmethod
    def deserialize_model(cls, user):
        return {
            "name": user.name,
            "password": user.password
        }


class RootEnabledView(View):

    def __init__(self, is_root_enabled):
        super(RootEnabledView, self).__init__('rootEnabled')
        self.is_root_enabled = is_root_enabled

    def data(self):
        return {self.name: self.is_root_enabled}

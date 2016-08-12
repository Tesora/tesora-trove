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

from trove.guestagent.datastore.experimental.couchbase import (
    manager as community_manager
)
from trove.guestagent.datastore.experimental.couchbase_4 import service


class Manager(community_manager.Manager):

    def __init__(self):
        super(Manager, self).__init__(manager_name='couchbase_4')

    def build_app(self):
        return service.Couchbase4App()

    def initialize_cluster(self, context, enabled_services=None):
        self.app.initialize_cluster(enabled_services=enabled_services)

    def add_nodes(self, context, nodes, enabled_services=None):
        self.app.rebalance_cluster(
            added_nodes=nodes, enabled_services=enabled_services)

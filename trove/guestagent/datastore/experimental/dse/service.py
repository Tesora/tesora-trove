# Copyright 2015 Tesora Inc.
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

from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.cassandra import (
    service as community_service
)


class DSEApp(community_service.CassandraApp):

    def __init__(self):
        super(DSEApp, self).__init__()

    @property
    def service_candidates(self):
        return ['dse']

    @property
    def cassandra_conf_dir(self):
        return {
            operating_system.REDHAT: "/etc/dse/cassandra/",
            operating_system.DEBIAN: "/etc/dse/cassandra/",
            operating_system.SUSE: "/etc/dse/cassandra/"
        }[operating_system.get_os()]

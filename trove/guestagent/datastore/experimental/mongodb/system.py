#   Copyright (c) 2014 Mirantis, Inc.
#   All Rights Reserved.
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

from os import path

from trove.guestagent.common import operating_system
from trove.guestagent import pkg

OS_NAME = operating_system.get_os()

CONFIG_DIR = "/etc"
MONGOD_CONFIG_CANDIDATES = [path.join(CONFIG_DIR, name) for name in
                            ['mongodb.conf', 'mongod.conf']]
MONGOD_CONFIG_FILE = operating_system.file_discovery(MONGOD_CONFIG_CANDIDATES)
MONGOD_CONFIG_OVERRIDES_DIR = path.join(CONFIG_DIR, 'mongod_overrides')

MONGOS_CONFIG_CANDIDATES = [path.join(CONFIG_DIR, name) for name in
                            ['mongodb.conf', 'mongos.conf']]
MONGOS_CONFIG_FILE = operating_system.file_discovery(MONGOS_CONFIG_CANDIDATES)
MONGOS_CONFIG_OVERRIDES_DIR = path.join(CONFIG_DIR, 'mongos_overrides')

MONGO_ADMIN_NAME = 'os_admin'
MONGO_ADMIN_ROLES = [{'db': 'admin', 'role': 'userAdminAnyDatabase'},
                     {'db': 'admin', 'role': 'dbAdminAnyDatabase'},
                     {'db': 'admin', 'role': 'clusterAdmin'},
                     {'db': 'admin', 'role': 'readWriteAnyDatabase'}]
MONGO_ADMIN_CREDS_FILENAME = '.os_mongo_admin_creds.json'
MONGO_ADMIN_CREDS_FILE = path.join(path.expanduser('~'),
                                   MONGO_ADMIN_CREDS_FILENAME)
MONGO_KEY_FILE = '/etc/mongo_key'
MONGOS_SERVICE_CANDIDATES = ["mongos"]
MONGOD_SERVICE_CANDIDATES = ["mongodb", "mongod"]
MONGODB_KILL = "sudo kill %s"
FIND_PID = "ps xaco pid,cmd | awk '/mongo(d|db|s)/ {print $1}'"
TIME_OUT = 1000

MONGO_USER = "mongodb"

PACKAGER = pkg.Package()

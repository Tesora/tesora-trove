#  Copyright 2013 Mirantis Inc.
#  All Rights Reserved.
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

from trove.common import cfg
from trove.guestagent.common import operating_system

CONF = cfg.CONF

CASSANDRA_OWNER = 'cassandra'
CASSANDRA_DATA_DIR = "/var/lib/cassandra/data"
CASSANDRA_SYSTEM_KEYSPACE = 'system'

CASSANDRA_CONF = {
    operating_system.REDHAT: "/etc/cassandra/default.conf/cassandra.yaml",
    operating_system.DEBIAN: "/etc/cassandra/cassandra.yaml",
    operating_system.SUSE: "/etc/cassandra/default.conf/cassandra.yaml"
}
CASSANDRA_CONF_BACKUP = {key: value + '.old'
                         for key, value in CASSANDRA_CONF.items()}

CASSANDRA_TEMP_CONF = "/tmp/cassandra.yaml"
CASSANDRA_TEMP_DIR = "/tmp/cassandra"

INIT_FS = "sudo mkdir -p %s"
ENABLE_CASSANDRA_ON_BOOT = "sudo update-rc.d cassandra enable"
DISABLE_CASSANDRA_ON_BOOT = "sudo update-rc.d cassandra disable"

# Use service command to start and stop cassandra
START_CASSANDRA = "sudo service cassandra start"
STOP_CASSANDRA = "sudo service cassandra stop"

CASSANDRA_KILL = "sudo killall java  || true"
SERVICE_STOP_TIMEOUT = 60
INSTALL_TIMEOUT = 10000

DEFAULT_SUPERUSER_NAME = "cassandra"
DEFAULT_SUPERUSER_PASSWORD = "cassandra"

# Default 'salted_hash' value for 'cassandra' user on Cassandra 2.1.
DEFAULT_SUPERUSER_PWD_HASH = (
    "$2a$10$wPEVuXBU7WE2Uwzqq3t19ObRJyoKztzC/Doyfr0VtDmVXC4GDAV3e"
)

CQLSH_CONF_PATH = "~/.cassandra/cqlshrc"

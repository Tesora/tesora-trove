# Copyright 2014 Tesora, Inc.
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
#

from trove.common import cfg
from trove.guestagent.backup.backupagent import BackupAgent
from trove.guestagent.strategies.replication import mysql_base
from trove.openstack.common import log as logging

AGENT = BackupAgent()
CONF = cfg.CONF

MASTER_CONFIG = """
[mysqld]
log_bin = /var/lib/mysql/mysql-bin.log
binlog_format = MIXED
enforce_gtid_consistency = ON
gtid_mode = ON
"""

PERCONA_CONFIG = """
enforce_storage_engine = Inno
"""

SLAVE_CONFIG = """
[mysqld]
log_bin = /var/lib/mysql/mysql-bin.log
relay_log = /var/lib/mysql/mysql-relay-bin.log
relay_log_info_repository = TABLE
relay_log_recovery = 1
relay_log_purge = 1
gtid_mode = ON
read_only = true
"""

LOG = logging.getLogger(__name__)


class MysqlGTIDReplication(mysql_base.MysqlReplicationBase):
    """MySql Replication coordinated by GTIDs."""

    def _get_master_config(self):
        config = MASTER_CONFIG
        if (CONF.datastore_manager == "percona"):
            config = config + PERCONA_CONFIG
        return MASTER_CONFIG

    def _get_slave_config(self):
        return SLAVE_CONFIG

    def connect_to_master(self, service, snapshot):
        logging_config = snapshot['log_position']
        LOG.debug("connect_to_master %s" % logging_config['replication_user'])
        change_master_cmd = (
            "CHANGE MASTER TO MASTER_HOST='%(host)s', "
            "MASTER_PORT=%(port)s, "
            "MASTER_USER='%(user)s', "
            "MASTER_PASSWORD='%(password)s', "
            "MASTER_AUTO_POSITION=1 " %
            {
                'host': snapshot['master']['host'],
                'port': snapshot['master']['port'],
                'user': logging_config['replication_user']['name'],
                'password': logging_config['replication_user']['password']
            })
        service.execute_on_client(change_master_cmd)
        service.start_slave()

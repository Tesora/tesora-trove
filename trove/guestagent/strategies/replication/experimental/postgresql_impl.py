# Copyright 2014 Tesora, Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
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

from oslo_log import log as logging
from oslo_utils import netutils
from trove.common import cfg
from trove.common import stream_codecs
from trove.guestagent.backup.backupagent import BackupAgent
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.experimental.postgresql import pgutil
from trove.guestagent.datastore.experimental.postgresql\
    .service.config import PgSqlConfig
from trove.guestagent.datastore.experimental.postgresql\
    .service.database import PgSqlDatabase
from trove.guestagent.datastore.experimental.postgresql\
    .service.install import PgSqlInstall
from trove.guestagent.datastore.experimental.postgresql \
    .service.process import PgSqlProcess
from trove.guestagent.datastore.experimental.postgresql\
    .service.root import PgSqlRoot
from trove.guestagent.strategies import backup
from trove.guestagent.strategies.replication import base

AGENT = BackupAgent()
CONF = cfg.CONF

REPL_BACKUP_NAMESPACE = 'trove.guestagent.strategies.backup.experimental' \
                        '.postgresql_impl'
REPL_BACKUP_STRATEGY = 'PgBaseBackup'
REPL_BACKUP_INCREMENTAL_STRATEGY = 'PgBaseBackupIncremental'
REPL_BACKUP_RUNNER = backup.get_backup_strategy(
    REPL_BACKUP_STRATEGY, REPL_BACKUP_NAMESPACE)
REPL_BACKUP_INCREMENTAL_RUNNER = backup.get_backup_strategy(
    REPL_BACKUP_INCREMENTAL_STRATEGY, REPL_BACKUP_NAMESPACE)
REPL_EXTRA_OPTS = CONF.backup_runner_options.get(REPL_BACKUP_STRATEGY, '')

LOG = logging.getLogger(__name__)

TRIGGER_FILE = '/tmp/postgresql.trigger'
REPL_USER = 'replicator'
# FIXME(atomic77) No further comment necessary
REPL_PW = 'insecure'
SLAVE_STANDBY_OVERRIDE = 'SlaveStandbyOverride'


class PostgresqlReplicationStreaming(
    base.Replication,
    PgSqlConfig,
    PgSqlDatabase,
    PgSqlRoot,
    PgSqlInstall,
):

    def __init__(self, *args, **kwargs):
        super(PostgresqlReplicationStreaming, self).__init__(*args, **kwargs)

    def get_master_ref(self, service, snapshot_info):
        master_ref = {
            'host': netutils.get_my_ipv4(),
            'port': CONF.postgresql.postgresql_port
        }
        return master_ref

    def backup_required_for_replication(self):
        """Indicates whether a backup is required for replication."""
        return True

    def snapshot_for_replication(self, context, service,
                                 location, snapshot_info):

        snapshot_id = snapshot_info['id']
        replica_number = snapshot_info.get('replica_number', 1)

        LOG.debug("Acquiring backup for replica number %d." % replica_number)
        # Only create a backup if it's the first replica
        if replica_number == 1:
            AGENT.execute_backup(
                context, snapshot_info, runner=REPL_BACKUP_RUNNER,
                extra_opts=REPL_EXTRA_OPTS,
                incremental_runner=REPL_BACKUP_INCREMENTAL_RUNNER)
        else:
            LOG.debug("Using existing backup created for previous replica.")
        LOG.debug("Replication snapshot %s used for replica number %d."
                  % (snapshot_id, replica_number))

        repl_user_info = {
            'name': REPL_USER,
            'password': REPL_PW
        }

        log_position = {
            'replication_user': repl_user_info
        }

        return snapshot_id, log_position

    def enable_as_master(self, service, master_config):
        """For a server to be a master in postgres, we need to enable
        the replication user in pg_hba and ensure that WAL logging is
        at the appropriate level (use the same settings as backups)
        """

        if not self.get_user(None, REPL_USER, None):
            pgutil.psql("CREATE USER %s REPLICATION LOGIN ENCRYPTED "
                        "PASSWORD '%s';" % (REPL_USER, REPL_PW))

        self.set_as_master(master_config)

        pgutil.psql("SELECT pg_reload_conf()")

    def enable_as_slave(self, service, snapshot, slave_config):
        """Adds appropriate config options to postgresql.conf, and writes out
        the recovery.conf file used to set up replication
        """

        self.write_standby_recovery_file(snapshot, sslmode='prefer')
        self.enable_hot_standby(service)
        # Ensure the WAL arch is empty before restoring
        PgSqlProcess.recreate_wal_archive_dir()

    def detach_slave(self, service, for_failover):
        """Remove recovery file and bounce server"""
        LOG.info("Detaching slave, use trigger file to disable recovery mode")
        operating_system.write_file(TRIGGER_FILE, '')
        operating_system.chown(TRIGGER_FILE, user=self.PGSQL_OWNER,
                               group=self.PGSQL_OWNER, as_root=True)
        replica_info = None

        return replica_info

    def cleanup_source_on_replica_detach(self, admin_service, replica_info):
        pass

    def demote_master(self, service):
        service.disable_backups()

    def connect_to_master(self, service, snapshot):
        """All that is required in postgresql to connect to a slave is to
        restart, which is called in enable_as_slave
        """
        self.restart(context=None)

    def remove_recovery_file(self):
        operating_system.remove(self.PGSQL_RECOVERY_CONFIG, as_root=True)

    def write_standby_recovery_file(self, snapshot, sslmode='prefer'):
        LOG.info("Snapshot data received:" + str(snapshot))

        logging_config = snapshot['log_position']
        conninfo_params = \
            {'host': snapshot['master']['host'],
             'port': snapshot['master']['port'],
             'repl_user': logging_config['replication_user']['name'],
             'password': logging_config['replication_user']['password'],
             'sslmode': sslmode}

        conninfo = 'host=%(host)s ' \
                   'port=%(port)s ' \
                   'user=%(repl_user)s ' \
                   'password=%(password)s ' \
                   'sslmode=%(sslmode)s ' % conninfo_params

        recovery_conf = "standby_mode = 'on'\n"
        recovery_conf += "primary_conninfo = '" + conninfo + "'\n"
        recovery_conf += "trigger_file = '/tmp/postgresql.trigger'\n"
        recovery_conf += "recovery_target_timeline='latest'\n"

        operating_system.write_file(self.PGSQL_RECOVERY_CONFIG, recovery_conf,
                                    codec=stream_codecs.IdentityCodec(),
                                    as_root=True)
        operating_system.chown(self.PGSQL_RECOVERY_CONFIG, user="postgres",
                               group="postgres", as_root=True)

    def enable_hot_standby(self, service):
        opts = {'hot_standby': 'on'}
        service.configuration_manager.\
            apply_system_override(opts, SLAVE_STANDBY_OVERRIDE)

    def set_as_master(self, master_config):
        LOG.info("Setting pgsql host as master")

        hba_entry = "host   replication   replicator    0.0.0.0/0   md5 \n"

        # TODO(atomic77) Remove this hack after adding cfg manager for pg_hba
        tmp_hba = '/tmp/pg_hba'
        operating_system.copy(self.PGSQL_HBA_CONFIG, tmp_hba,
                              force=True, as_root=True)
        operating_system.chmod(tmp_hba, FileMode.OCTAL_MODE("0777"),
                               as_root=True)
        with open(tmp_hba, 'a+') as hba_file:
            hba_file.write(hba_entry)

        operating_system.copy(tmp_hba, self.PGSQL_HBA_CONFIG,
                              force=True, as_root=True)
        operating_system.chmod(self.PGSQL_HBA_CONFIG,
                               FileMode.OCTAL_MODE("0600"),
                               as_root=True)
        operating_system.remove(tmp_hba, as_root=True)

    def get_replica_context(self, service):
        repl_user_info = {
            'name': REPL_USER,
            'password': REPL_PW
        }

        log_position = {
            'replication_user': repl_user_info
        }
        return {
            'master': self.get_master_ref(None, None),
            'log_position': log_position
        }

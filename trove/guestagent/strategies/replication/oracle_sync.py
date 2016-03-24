# flake8: noqa

# Copyright (c) 2016 Tesora, Inc.
#
# This file is part of the Tesora DBaas Platform Enterprise Edition.
#
# Tesora DBaaS Platform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# According to sec. 7 of the GNU Affero General Public License, version 3, the
# terms of the AGPL are supplemented with the following terms:
#
# "Tesora", "Tesora DBaaS Platform", and the Tesora logo are trademarks
#  of Tesora, Inc.,
#
# The licensing of the Program under the AGPL does not imply a trademark
# license. Therefore any rights, title and interest in our trademarks remain
# entirely with us.
#
# However, if you propagate an unmodified version of the Program you are
# allowed to use the term "Tesora" solely to indicate that you distribute the
# Program. Furthermore you may use our trademarks where it is necessary to
# indicate the intended purpose of a product or service provided you use it in
# accordance with honest practices in industrial or commercial matters.
#
# If you want to propagate modified versions of the Program under the name
# "Tesora" or "Tesora DBaaS Platform", you may only do so if you have a written
# permission by Tesora, Inc. (to acquire a permission please contact
# Tesora, Inc at trademark@tesora.com).
#
# The interactive user interface of the software displays an attribution notice
# containing the term "Tesora" and/or the logo of Tesora.  Interactive user
# interfaces of unmodified and modified versions must display Appropriate Legal
# Notices according to sec. 5 of the GNU Affero General Public License,
# version 3, when you propagate unmodified or modified versions of  the
# Program. In accordance with sec. 7 b) of the GNU Affero General Public
# License, version 3, these Appropriate Legal Notices must retain the logo of
# Tesora or display the words "Initial Development by Tesora" if the display of
# the logo is not reasonably feasible for technical reasons.

import getpass
from os import path
from oslo_log import log as logging
from oslo_utils import netutils
import socket

import cx_Oracle

from trove.common import cfg
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent.common import configuration
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle import service as ora_service
from trove.guestagent.datastore.oracle import sql_query
from trove.guestagent.datastore.oracle import system
from trove.guestagent.strategies.replication import base

CONF = cfg.CONF
MANAGER = 'oracle'
LOG = logging.getLogger(__name__)
ORANET_DIR = '%s/network/admin' % CONF.get(MANAGER).oracle_home
TNS_FILE_NAME = 'tnsnames.ora'
TNS_PATH = path.join(ORANET_DIR, TNS_FILE_NAME)
LSNR_FILE_NAME = 'listener.ora'
LSNR_PATH = path.join(ORANET_DIR, LSNR_FILE_NAME)
OVERRIDES_PATH = path.join(ORANET_DIR, 'overrides')


class OracleSyncReplication(base.Replication):
    """Oracle Replication strategy."""

    __strategy_name__ = 'OracleSyncReplication'

    def __init__(self):
        super(OracleSyncReplication, self).__init__()
        self._oracle_config = None

    def _get_config(self):
        if not self._oracle_config:
            self._oracle_config = ora_service.OracleConfig()
        return self._oracle_config

    def get_replication_detail(self, service):
        replication_detail = {
            'db_name': self._get_config().db_name,
            'db_unique_name': self._get_config().db_unique_name,
            'host': netutils.get_my_ipv4()
        }
        return replication_detail

    def get_master_ref(self, service, snapshot_info):
        """Capture information from a master node"""
        pfile = '/tmp/init%s_stby.ora' % self._get_config().db_name
        pwfile = ('%(ora_home)s/dbs/orapw%(db_name)s' %
                  {'ora_home': CONF.get(MANAGER).oracle_home,
                   'db_name': self._get_config().db_name})
        ctlfile = '/tmp/%s_stby.ctl' % self._get_config().db_name
        oratabfile = '/etc/oratab'
        oracnffile = CONF.get(MANAGER).conf_file
        datafile = '/tmp/oradata.tar.gz'

        def _cleanup_tmp_files():
            operating_system.remove(ctlfile, force=True, as_root=True)
            operating_system.remove(pfile, force=True, as_root=True)
            operating_system.remove(datafile, force=True, as_root=True)

        _cleanup_tmp_files()

        with ora_service.LocalOracleClient(self._get_config().db_name,
                                           service=True) as client:
            client.execute("ALTER DATABASE CREATE STANDBY CONTROLFILE AS "
                           "'%s'" % ctlfile)
            ora_service.OracleAdmin().create_parameter_file(target=pfile,
                                                            client=client)
            q = sql_query.Query()
            q.columns = ["value"]
            q.tables = ["v$parameter"]
            q.where = ["name = 'fal_server'"]
            client.execute(str(q))
            row = client.fetchone()
            db_list = []
            if row is not None and row[0] is not None:
                db_list = str(row[0]).split(",")
            db_list.insert(0, self._get_config().db_name)

        # Create a tar file containing files needed for slave creation
        utils.execute_with_timeout('tar', '-Pczvf', datafile, ctlfile,
                                   pwfile, pfile, oratabfile, oracnffile,
                                   run_as_root=True, root_helper='sudo')
        oradata_encoded = operating_system.read_file(
            datafile, codec=stream_codecs.Base64Codec(), as_root=True)
        _cleanup_tmp_files()
        master_ref = {
            'host': netutils.get_my_ipv4(),
            'db_name': self._get_config().db_name,
            'db_list': db_list,
            'oradata': oradata_encoded,
        }
        return master_ref

    def backup_required_for_replication(self):
        LOG.debug('Request for replication backup: no backup required')
        return False

    def post_processing_required_for_replication(self):
        """"Post processing required for replication"""
        return True

    def snapshot_for_replication(self, context, service,
                                 location, snapshot_info):
        return None, None

    def _log_apply_is_running(self):
        with ora_service.LocalOracleClient(
                self._get_config().db_name) as client:
            client.execute("select count(*) from v$managed_standby "
                           "where process like 'MRP%'")
            row = client.fetchone()
            return int(row[0]) > 0

    def wait_for_txn(self):
        # Turn this slave node into master when switching over
        # (promote-to-replica-source)
        if self._log_apply_is_running():
            # Switchover from slave to master only if the current
            # instance is already a slave
            with ora_service.OracleConnection(
                    self._get_config().db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("ALTER DATABASE COMMIT TO SWITCHOVER TO "
                               "PRIMARY WITH SESSION SHUTDOWN")
                conn.shutdown(mode=cx_Oracle.DBSHUTDOWN_IMMEDIATE)
                cursor.execute("alter database dismount")
                conn.shutdown(mode=cx_Oracle.DBSHUTDOWN_FINAL)

            # The DB has been shut down at this point, need to establish a
            # new connection in PRELIM_AUTH mode in order to start it up.
            with ora_service.OracleConnection(
                    self._get_config().db_name,
                    mode=(cx_Oracle.SYSDBA |
                          cx_Oracle.PRELIM_AUTH)) as conn:
                conn.startup()

            # DB is now up but not open, re-connect to the DB in SYSDBA
            # mode to open it.
            with ora_service.OracleConnection(
                    self._get_config().db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("alter database mount")
                cursor.execute("alter database open")
                cursor.execute("ALTER SYSTEM SWITCH LOGFILE")
            ora_status = ora_service.OracleAppStatus()
            ora_status.update()

    def enable_as_master(self, service, master_config):
        # Turn this slave node into master when failing over
        # (eject-replica-source)
        if self._log_apply_is_running():
            with ora_service.LocalOracleClient(
                    self._get_config().db_name) as client:
                client.execute("ALTER DATABASE RECOVER MANAGED STANDBY "
                               "DATABASE FINISH")
                client.execute("ALTER DATABASE ACTIVATE STANDBY DATABASE")
                client.execute("ALTER DATABASE OPEN")
                client.execute("ALTER SYSTEM SWITCH LOGFILE")

    def _create_tns_entry(self, dbname, host, service_name):
        return ('(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)'
                '(HOST=%(host)s)(PORT=%(port)s))'
                '(CONNECT_DATA=(SERVICE_NAME=%(service_name)s)))' %
                {'dbname': dbname, 'host': host,
                 'port': CONF.get(MANAGER).listener_port,
                 'service_name': service_name})

    def _create_tns_file(self, dbs):
        if not path.isfile(TNS_PATH):
            operating_system.write_file(TNS_PATH, '', as_root=True)
        conf = configuration.ConfigurationManager(
            TNS_PATH, system.ORACLE_INSTANCE_OWNER, system.ORACLE_GROUP_OWNER,
            stream_codecs.PropertiesCodec('='), requires_root=True)
        conf.save_configuration({})
        for db in dbs:
            tns_entry = self._create_tns_entry(db['db_unique_name'],
                                               db['host'],
                                               self._get_config().db_name)
            conf.apply_system_override({db['db_unique_name']: tns_entry},
                                       db['db_unique_name'])

    def _create_lsnr_file(self):
        """Create the listener.ora file"""
        listener_file_name = 'listener.ora'
        listener_path = path.join(ORANET_DIR, listener_file_name)
        content = ('SID_LIST_LISTENER=(SID_LIST=(SID_DESC='
                   '(GLOBAL_DBNAME=%(db_name)s)'
                   '(ORACLE_HOME=%(ora_home)s)'
                   '(SID_NAME=%(db_name)s)))\n' %
                   {'db_name': self._get_config().db_name,
                    'ora_home': CONF.get(MANAGER).oracle_home})
        content += ('LISTENER=(DESCRIPTION_LIST=(DESCRIPTION=(ADDRESS='
                    '(PROTOCOL=TCP)(HOST=%(host)s)(PORT=%(port)s))'
                    '(ADDRESS=(PROTOCOL=IPC)(KEY=EXTPROC1521))))\n' %
                    {'host': socket.gethostname(),
                     'port': CONF.get(MANAGER).listener_port})
        content += ('ADR_BASE_LISTENER=%s\n' %
                    CONF.get(MANAGER).oracle_base)
        operating_system.write_file(listener_path, content, as_root=True)
        operating_system.chown(listener_path, system.ORACLE_INSTANCE_OWNER,
                               system.ORACLE_GROUP_OWNER, as_root=True)

    def _create_static_params(self, oracle_client):
        """Create replication system parameters that only needs to be
        setup once.
        """
        oracle_client.execute('ALTER DATABASE FORCE LOGGING')
        oracle_client.execute("ALTER SYSTEM SET LOG_ARCHIVE_FORMAT="
                              "'%t_%s_%r.arc' SCOPE=SPFILE")
        oracle_client.execute("ALTER SYSTEM SET "
                              "LOG_ARCHIVE_MAX_PROCESSES=%s" %
                              CONF.get(MANAGER).log_archive_max_process)
        oracle_client.execute("ALTER SYSTEM SET REMOTE_LOGIN_PASSWORDFILE="
                              "EXCLUSIVE SCOPE=SPFILE")
        oracle_client.execute("ALTER SYSTEM SET STANDBY_FILE_MANAGEMENT=AUTO")
        oracle_client.execute("ALTER SYSTEM SET REDO_TRANSPORT_USER=%s "
                              "SCOPE=BOTH" % ora_service.ADMIN_USER_NAME)

    def _update_dynamic_params(self, oracle_client, dbs):
        """Update replication system parameters that changes according to
        the current topology.
        """
        db_list = [db['db_unique_name'] for db in dbs]
        dg_config_list = ",".join(db_list)
        fal_server_list = ",".join("'%s'" % db for db in db_list)
        oracle_client.execute("ALTER SYSTEM SET LOG_ARCHIVE_CONFIG="
                              "'DG_CONFIG=(%s)'" % dg_config_list)
        oracle_client.execute("ALTER SYSTEM SET FAL_SERVER=%s" %
                              fal_server_list)
        for index in range(2, 31):
            oracle_client.execute("ALTER SYSTEM SET LOG_ARCHIVE_DEST_%s=''" %
                                  index)
        log_index = 2
        for db in dbs:
            if db['db_unique_name'] != self._get_config().db_unique_name:
                oracle_client.execute(
                    "ALTER SYSTEM SET "
                    "LOG_ARCHIVE_DEST_%(log_index)s='SERVICE="
                    "%(db)s NOAFFIRM ASYNC VALID_FOR="
                    "(ONLINE_LOGFILES,PRIMARY_ROLE) "
                    "DB_UNIQUE_NAME=%(db)s'" %
                    {'log_index': log_index, 'db': db['db_unique_name']})
                oracle_client.execute("ALTER SYSTEM SET "
                                      "LOG_ARCHIVE_DEST_STATE_%s=ENABLE" %
                                      log_index)
                log_index += 1

    def _create_standby_log_files(self, oracle_client):
        for i in range(1, CONF.get(MANAGER).standby_log_count + 1):
            standby_log_file = path.join(
                CONF.get(MANAGER).oracle_base, 'oradata',
                self._get_config().db_name, 'standby_redo%s.log' % i)
            oracle_client.execute(
                "ALTER DATABASE ADD STANDBY LOGFILE "
                "('%(log_file)s') SIZE %(log_size)sM" %
                {'log_file': standby_log_file,
                 'log_size': CONF.get(MANAGER).standby_log_size})

    def _is_new_replication_node(self):
        return not (path.isfile(LSNR_PATH) and path.isfile(TNS_PATH))

    def complete_master_setup(self, context, slave_detail):
        """Finalize master setup and start the master Oracle processes."""
        dbs = [self.get_replication_detail(None)]
        dbs.extend(slave_detail)
        with ora_service.LocalOracleClient(self._get_config().db_name,
                                           service=True) as ora_client:
            if self._is_new_replication_node():
                self._create_lsnr_file()
                self._create_standby_log_files(ora_client)
                self._create_static_params(ora_client)
            self._create_tns_file(dbs)
            self._update_dynamic_params(ora_client, dbs)
            ora_client.execute("ALTER SYSTEM SWITCH LOGFILE")

    def _complete_new_slave_setup(self, master_host, dbs):
        ora_conf = ora_service.OracleConfig()
        sys_password = ora_conf.sys_password
        with ora_service.OracleConnection(
                self._get_config().db_name,
                mode=(cx_Oracle.SYSDBA | cx_Oracle.PRELIM_AUTH)) as conn:
            conn.startup()
        db_list = [db['db_unique_name'] for db in dbs]
        fal_server_list = ",".join("'%s'" % db for db in db_list)
        # The RMAN DUPLICATE command requires connecting to target with the
        # 'sys' user. If we use any other user, such as 'os_admin', even with
        # the sysdba and sysoper roles assigned, it will still fail with:
        # ORA-01017: invalid username/password; logon denied
        cmd = ("""\"\
rman target %(admin_user)s/%(admin_pswd)s@%(host)s/%(db_name)s \
auxiliary %(admin_user)s/%(admin_pswd)s@%(db_unique_name)s <<EOF
run {
DUPLICATE TARGET DATABASE FOR STANDBY
FROM ACTIVE DATABASE DORECOVER SPFILE
SET db_unique_name='%(db_unique_name)s' COMMENT 'Is standby'
SET FAL_SERVER=%(fal_server_list)s COMMENT 'Is primary'
NOFILENAMECHECK;
}
EXIT;
EOF\"
""")
        duplicate_cmd = (cmd % {'admin_user': 'sys',
                                'admin_pswd': sys_password,
                                'host': master_host,
                                'db_name': self._get_config().db_name,
                                'db_unique_name':
                                    self._get_config().db_unique_name,
                                'fal_server_list': fal_server_list})
        utils.execute_with_timeout("su - oracle -c " + duplicate_cmd,
                                   run_as_root=True, root_helper='sudo',
                                   timeout=CONF.restore_usage_timeout,
                                   shell=True, log_output_on_error=True)
        with ora_service.LocalOracleClient(
                self._get_config().db_name) as client:
            client.execute("ALTER SYSTEM SET REDO_TRANSPORT_USER = %s "
                           "SCOPE = BOTH" % ora_service.ADMIN_USER_NAME)
            client.execute("ALTER DATABASE OPEN READ ONLY")
            client.execute("ALTER DATABASE RECOVER MANAGED STANDBY DATABASE "
                           "USING CURRENT LOGFILE DISCONNECT FROM SESSION")

    def complete_slave_setup(self, context, master_detail, slave_detail):
        """Finalize slave setup and start the slave Oracle processes."""
        if self._is_new_replication_node():
            dbs = [master_detail]
            dbs.extend(slave_detail)
            self._create_tns_file(dbs)
            self._complete_new_slave_setup(master_detail['host'], dbs)

    def sync_data_to_slaves(self, context):
        """Trigger an archive log switch and flush transactions down to the
        slaves.
        """
        LOG.debug("sync_data_to_slaves - switching log file")
        with ora_service.LocalOracleClient(
                self._get_config().db_name) as client:
            client.execute("ALTER SYSTEM SWITCH LOGFILE")

    def prepare_slave(self, snapshot):
        """Prepare the environment needed for starting the slave Oracle
        processes.
        """
        master_info = snapshot['master']
        db_name = master_info['db_name']

        tmp_dir = '/tmp'
        tmp_data_path = path.join(tmp_dir, 'oradata.tar.gz')
        orabase_path = CONF.get(MANAGER).oracle_base
        orahome_path = CONF.get(MANAGER).oracle_home
        db_data_path = path.join(orabase_path, 'oradata', db_name)
        fast_recovery_path = path.join(orabase_path, 'fast_recovery_area')
        db_fast_recovery_path = path.join(fast_recovery_path, db_name)
        audit_path = path.join(orabase_path, 'admin', db_name, 'adump')
        admin_path = path.join(orabase_path, 'admin')

        # Create necessary directories and set permissions
        directories = [db_data_path, db_fast_recovery_path, audit_path]
        for directory in directories:
            operating_system.create_directory(directory,
                                              system.ORACLE_INSTANCE_OWNER,
                                              system.ORACLE_GROUP_OWNER,
                                              as_root=True)
        operating_system.chown(fast_recovery_path,
                               system.ORACLE_INSTANCE_OWNER,
                               system.ORACLE_GROUP_OWNER, as_root=True)
        operating_system.chown(admin_path, system.ORACLE_INSTANCE_OWNER,
                               system.ORACLE_GROUP_OWNER, as_root=True)

        # Install on the slave files extracted from the master
        # (e.g. the control, pfile, password, oracle.cnf file ... etc)
        oradata = master_info['oradata']
        operating_system.write_file(tmp_data_path, oradata,
                                    codec=stream_codecs.Base64Codec())
        utils.execute_with_timeout('tar', '-Pxzvf', tmp_data_path,
                                   run_as_root=True, root_helper='sudo')

        # Put the control file in place
        tmp_ctlfile_path = path.join(tmp_dir, '%s_stby.ctl' % db_name)
        ctlfile1_path = path.join(db_data_path, 'control01.ctl')
        ctlfile2_path = path.join(db_fast_recovery_path, 'control02.ctl')
        operating_system.move(tmp_ctlfile_path, ctlfile1_path, as_root=True)
        operating_system.copy(ctlfile1_path, ctlfile2_path, preserve=True,
                              as_root=True)

        db_unique_name = ('%(db_name)s_%(replica_label)s' %
                          {'db_name': db_name,
                           'replica_label': utils.generate_random_string(6)})

        # Customize the pfile for slave and put it in the right place.
        # The pfile that came from master is owned by the 'oracle' user,
        # so we need to change ownership first before editing it.
        tmp_pfile_path = path.join(tmp_dir, 'init%s_stby.ora' % db_name)
        pfile_path = path.join(orahome_path, 'dbs', 'init%s.ora' % db_name)
        operating_system.chown(tmp_pfile_path, getpass.getuser(), None,
                               as_root=True)
        with open(tmp_pfile_path, 'a') as pfile:
            pfile.write("*.db_unique_name='%s'\n" % db_unique_name)

        # Finished editing pfile, put it in the proper directory and chown
        # back to oracle user and group
        operating_system.move(tmp_pfile_path, pfile_path, force=True,
                              as_root=True)
        operating_system.chown(pfile_path, system.ORACLE_INSTANCE_OWNER,
                               system.ORACLE_GROUP_OWNER, as_root=True)

        self._get_config().db_name = db_name
        self._get_config().db_unique_name = db_unique_name

        # Set proper permissions on the oratab file
        operating_system.chown('/etc/oratab', system.ORACLE_INSTANCE_OWNER,
                               system.ORACLE_GROUP_OWNER, as_root=True)

        # Create the listener.ora file
        self._create_lsnr_file()

        # Restart the listener
        utils.execute_with_timeout("sudo", "su", "-", "oracle", "-c",
                                   "lsnrctl reload",
                                   timeout=CONF.usage_timeout)

    def enable_as_slave(self, service, snapshot, slave_config):
        """Turn this node into slave by enabling the log apply process."""
        with ora_service.LocalOracleClient(
                self._get_config().db_name) as client:
            client.execute("select count(*) from v$managed_standby "
                           "where process like 'MRP%'")
            row = client.fetchone()
            if int(row[0]) == 0:
                # Only attempt to enable log apply if it is not already
                # running
                LOG.debug('Slave processes does not exist in '
                          'v$managed_standy, switching on LOG APPLY')
                client.execute("ALTER DATABASE RECOVER MANAGED STANDBY "
                               "DATABASE USING CURRENT LOGFILE DISCONNECT "
                               "FROM SESSION")
        utils.execute_with_timeout("sudo", "su", "-", "oracle", "-c",
                                   "lsnrctl reload",
                                   timeout=CONF.usage_timeout)

    def detach_slave(self, service, for_failover=False):
        """Detach this slave by disabling the log apply process,
        setting it to read/write.
        """
        if not for_failover:
            with ora_service.LocalOracleClient(
                    self._get_config().db_name) as client:
                client.execute("ALTER DATABASE RECOVER MANAGED STANDBY "
                               "DATABASE CANCEL")
                client.execute("ALTER DATABASE ACTIVATE STANDBY DATABASE")
                client.execute("ALTER DATABASE OPEN")
                for index in range(2, 31):
                    client.execute("ALTER SYSTEM SET LOG_ARCHIVE_DEST_%s=''" %
                                   index)
                client.execute("ALTER SYSTEM SET LOG_ARCHIVE_CONFIG=''")
                client.execute("ALTER SYSTEM SET FAL_SERVER=''")

    def cleanup_source_on_replica_detach(self, service, replica_info):
        # Nothing needs to be done to the master when a replica goes away.
        pass

    def get_replica_context(self, service):
        return {
            'is_master': True,
        }

    def demote_master(self, service):
        pass

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

from os import path
import socket

import cx_Oracle
from oslo_log import log as logging
from oslo_utils import netutils

from trove.common import cfg
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle_common import sql_query
from trove.guestagent.strategies.replication import base

CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'
LOG = logging.getLogger(__name__)

TMP_DIR = '/tmp'


class OracleSyncReplication(base.Replication):
    """Oracle Replication strategy."""

    __strategy_name__ = 'OracleSyncReplication'

    def _update_parameters(self, service, cursor, parameters, deferred=False):
        """Set system parameters and update the configuration manager."""
        for k, v in parameters.items():
            cursor.execute(str(sql_query.AlterSystem.set_parameter(
                k, v, deferred=deferred)))
        service.configuration_manager.apply_system_override(parameters)
        service.generate_spfile()

    def get_replication_detail(self, service):
        replication_detail = {
            'db_name': service.admin.database_name,
            'db_unique_name': service.admin.ora_config.db_unique_name,
            'host': netutils.get_my_ipv4()}
        return replication_detail

    def get_master_ref(self, service, snapshot_info):
        """Capture information from a master node"""
        ctlfile = path.join(TMP_DIR,
                            '%s_stby.ctl' % service.admin.database_name)
        datafile = path.join(TMP_DIR, 'oradata.tar.gz')

        def _cleanup_tmp_files():
            operating_system.remove(ctlfile, force=True, as_root=True)
            operating_system.remove(datafile, force=True, as_root=True)

        _cleanup_tmp_files()

        with service.cursor(service.admin.database_name) as cursor:
            cursor.execute(str(sql_query.AlterDatabase(
                "CREATE STANDBY CONTROLFILE AS '%s'" % ctlfile)))
            cursor.execute(str(sql_query.Query(
                columns=['VALUE'],
                tables=['V$PARAMETER'],
                where=["NAME = 'fal_server'"])))
            row = cursor.fetchone()
            db_list = []
            if row is not None and row[0] is not None:
                db_list = str(row[0]).split(",")
            db_list.insert(0, service.admin.database_name)

        # Create a tar file containing files needed for slave creation
        utils.execute_with_timeout('tar', '-Pczvf', datafile, ctlfile,
                                   service.paths.orapw_file,
                                   service.paths.oratab_file,
                                   CONF.get(MANAGER).conf_file,
                                   run_as_root=True, root_helper='sudo')
        oradata_encoded = operating_system.read_file(
            datafile,
            codec=stream_codecs.Base64Codec(),
            as_root=True,
            decode=False)
        _cleanup_tmp_files()
        master_ref = {
            'host': netutils.get_my_ipv4(),
            'db_name': service.admin.database_name,
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

    def snapshot_for_replication(self, context, service, location,
                                 snapshot_info):
        return None, None

    def _log_apply_is_running(self, cursor):
        cursor.execute(str(sql_query.Query(
            columns=['COUNT(*)'],
            tables=['V$MANAGED_STANDBY'],
            where=["PROCESS LIKE 'MRP%'"])))
        row = cursor.fetchone()
        return int(row[0]) > 0

    def wait_for_txn(self, service):
        # Turn this slave node into master when switching over
        # (promote-to-replica-source)
        with service.cursor(service.admin.database_name) as cursor:
            if not self._log_apply_is_running(cursor):
                # Switchover from slave to master only if the current
                # instance is already a slave
                return
            cursor.execute(str(sql_query.AlterDatabase(
                "COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN")))
        service.restart()
        with service.cursor(service.admin.database_name) as cursor:
            cursor.execute(str(sql_query.AlterSystem('SWITCH LOGFILE')))
        service.status.update()

    def enable_as_master(self, service, master_config):
        # Turn this slave node into master when failing over
        # (eject-replica-source)
        with service.cursor(service.admin.database_name) as cursor:
            if self._log_apply_is_running(cursor):
                cursor.execute(str(sql_query.AlterDatabase(
                    'RECOVER MANAGED STANDBY DATABASE FINISH')))
                cursor.execute(str(sql_query.AlterDatabase(
                    'ACTIVATE STANDBY DATABASE')))
                cursor.execute(str(sql_query.AlterDatabase('OPEN')))
                cursor.execute(str(sql_query.AlterSystem('SWITCH LOGFILE')))

    def _create_tns_file(self, service, dbs):
        tns_file = service.paths.tns_file
        tns_entries = {}
        for db in dbs:
            tns_entry = (
                '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)'
                '(HOST=%(host)s)(PORT=%(port)s))'
                '(CONNECT_DATA=(SERVICE_NAME=%(service_name)s)))' %
                {'dbname': db['db_unique_name'],
                 'host': db['host'],
                 'port': CONF.get(MANAGER).listener_port,
                 'service_name': service.admin.database_name})
            tns_entries[db['db_unique_name']] = tns_entry
        operating_system.write_file(tns_file, tns_entries,
                                    codec=stream_codecs.KeyValueCodec(),
                                    as_root=True)
        operating_system.chown(tns_file,
                               service.instance_owner,
                               service.instance_owner_group,
                               force=True, as_root=True)

    def _force_logging_enabled(self, cursor):
        """Checking whether the database is in force logging mode.
        """
        cursor.execute(str(sql_query.Query(
            columns=['FORCE_LOGGING'], tables=['V$DATABASE'])))
        row = cursor.fetchone()
        return (row[0] == 'YES')

    def _create_static_params(self, service, cursor):
        """Create replication system parameters that only needs to be
        setup once.
        """
        if not self._force_logging_enabled(cursor):
            cursor.execute(str(sql_query.AlterDatabase('FORCE LOGGING')))
        max_processes = CONF.get(MANAGER).log_archive_max_process
        settings = {
            'LOG_ARCHIVE_MAX_PROCESSES': max_processes,
            'STANDBY_FILE_MANAGEMENT': 'AUTO',
            'REDO_TRANSPORT_USER': service.admin_user_name.upper()}
        self._update_parameters(service, cursor, settings)
        settings = {
            'LOG_ARCHIVE_FORMAT': '%t_%s_%r.arc',
            'REMOTE_LOGIN_PASSWORDFILE': 'EXCLUSIVE'}
        self._update_parameters(service, cursor, settings, deferred=True)

    def _clear_log_archive_dests(self, service, cursor):
        settings = dict()
        for index in range(2, 31):
            settings['LOG_ARCHIVE_DEST_%s' % index] = "''"
        self._update_parameters(service, cursor, settings)

    def _update_dynamic_params(self, service, cursor, dbs):
        """Update replication system parameters that changes according to
        the current topology.
        """
        db_list = [db['db_unique_name'] for db in dbs]
        settings = {
            'LOG_ARCHIVE_CONFIG': 'DG_CONFIG=(%s)' % ','.join(db_list),
            'FAL_SERVER': ','.join("'%s'" % db for db in db_list)}
        self._update_parameters(service, cursor, settings)
        # Set up a log destination for each slave
        self._clear_log_archive_dests(service, cursor)
        log_index = 2
        settings = dict()
        for db in dbs:
            if db['db_unique_name'] != service.admin.ora_config.db_unique_name:
                dest = ('SERVICE=%(db)s NOAFFIRM ASYNC VALID_FOR='
                        '(ONLINE_LOGFILES,PRIMARY_ROLE) '
                        'DB_UNIQUE_NAME=%(db)s' %
                        {'db': db['db_unique_name']})
                settings['LOG_ARCHIVE_DEST_%s' % log_index] = dest
                settings['LOG_ARCHIVE_DEST_STATE_%s' % log_index] = 'ENABLE'
                log_index += 1
        self._update_parameters(service, cursor, settings)

    def _create_standby_log_files(self, service, cursor):
        for i in range(1, CONF.get(MANAGER).standby_log_count + 1):
            standby_log_file = service.paths.standby_log_file % i
            cursor.execute(str(sql_query.AlterDatabase(
                "ADD STANDBY LOGFILE ('%(log_file)s') SIZE %(log_size)sM" %
                {'log_file': standby_log_file,
                 'log_size': CONF.get(MANAGER).standby_log_size})))

    def _is_new_slave_node(self, service):
        return not path.isfile(service.paths.tns_file)

    def _is_new_master_node(self, service):
        standby_log_file = service.paths.standby_log_file % "1"
        return not operating_system.exists(standby_log_file, as_root=True)

    def complete_master_setup(self, service, slave_detail):
        """Finalize master setup and start the master Oracle processes."""
        dbs = [self.get_replication_detail(service)]
        dbs.extend(slave_detail)
        with service.cursor(service.admin.database_name) as cursor:
            if self._is_new_master_node(service):
                self._create_standby_log_files(service, cursor)
                self._create_static_params(service, cursor)
            self._create_tns_file(service, dbs)
            self._update_dynamic_params(service, cursor, dbs)
            cursor.execute(str(sql_query.AlterSystem(
                'SWITCH LOGFILE')))

    def _complete_new_slave_setup(self, service, master_host, dbs):
        sys_password = service.admin.ora_config.root_password
        with service.client(service.admin.database_name,
                            mode=(cx_Oracle.SYSDBA |
                                  cx_Oracle.PRELIM_AUTH)) as client:
            client.startup()
        db_list = [db['db_unique_name'] for db in dbs]
        fal_server_list = ",".join("'%s'" % db for db in db_list)
        # The RMAN DUPLICATE command requires connecting to target with the
        # 'sys' user. If we use any other user, such as 'os_admin', even with
        # the sysdba and sysoper roles assigned, it will still fail with:
        # ORA-01017: invalid username/password; logon denied
        duplicate_cmd = (
            "DUPLICATE TARGET DATABASE FOR STANDBY FROM ACTIVE DATABASE "
            "DORECOVER SPFILE SET db_unique_name='%(db_unique_name)s' COMMENT "
            "'Is standby' SET FAL_SERVER=%(fal_server_list)s COMMENT "
            "'Is primary' NOFILENAMECHECK"
            % {'db_unique_name': service.admin.ora_config.db_unique_name,
               'fal_server_list': fal_server_list})
        script = service.rman_scripter(
            commands=duplicate_cmd,
            t_user=service.root_user_name, t_pswd=sys_password,
            t_host=master_host, t_db=service.admin.database_name,
            a_user=service.root_user_name, a_pswd=sys_password,
            a_db=service.admin.ora_config.db_unique_name)
        script.run(timeout=CONF.restore_usage_timeout)
        with service.cursor(service.admin.database_name) as cursor:
            # This is set before the configuration manager is initialized so
            # set the parameter directly
            cursor.execute(str(sql_query.AlterSystem.set_parameter(
                'REDO_TRANSPORT_USER', service.admin_user_name.upper())))
            cursor.execute(str(sql_query.AlterDatabase('OPEN READ ONLY')))
            cursor.execute(str(sql_query.AlterDatabase(
                'RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE '
                'DISCONNECT FROM SESSION')))
        service.prep_pfile_management()

    def complete_slave_setup(self, service, master_detail, slave_detail):
        """Finalize slave setup and start the slave Oracle processes."""
        if self._is_new_slave_node(service):
            dbs = [master_detail]
            dbs.extend(slave_detail)
            self._create_tns_file(service, dbs)
            self._complete_new_slave_setup(service, master_detail['host'], dbs)

    def sync_data_to_slaves(self, service):
        """Trigger an archive log switch and flush transactions down to the
        slaves.
        """
        LOG.debug("sync_data_to_slaves - switching log file")
        with service.cursor(service.admin.database_name) as cursor:
            cursor.execute(str(sql_query.AlterSystem('SWITCH LOGFILE')))

    def prepare_slave(self, service, snapshot):
        """Prepare the environment needed for starting the slave Oracle
        processes.
        """
        master_info = snapshot['master']
        db_name = master_info['db_name']
        db_unique_name = ('%(db_name)s_%(replica_label)s' %
                          {'db_name': db_name,
                           'replica_label': utils.generate_random_string(6)})
        service.paths.update_db_name(db_name)

        # Create necessary directories and set necessary permissions
        new_dirs = [service.paths.db_data_dir,
                    service.paths.db_fast_recovery_logs_dir,
                    service.paths.db_fast_recovery_dir,
                    service.paths.audit_dir]
        for directory in new_dirs:
            operating_system.create_directory(directory,
                                              service.instance_owner,
                                              service.instance_owner_group,
                                              as_root=True)

        chown_dirs = [service.paths.fast_recovery_area,
                      service.paths.admin_dir]
        for directory in chown_dirs:
            operating_system.chown(directory,
                                   service.instance_owner,
                                   service.instance_owner_group,
                                   as_root=True)

        # Install on the slave files extracted from the master
        # (e.g. the control, pfile, password, oracle.cnf file ... etc)
        oradata_encoded = master_info['oradata']
        tmp_data_path = path.join(TMP_DIR, 'oradata.tar.gz')
        operating_system.write_file(tmp_data_path, oradata_encoded,
                                    codec=stream_codecs.Base64Codec(),
                                    encode=False)
        utils.execute_with_timeout('tar', '-Pxzvf', tmp_data_path,
                                   run_as_root=True, root_helper='sudo')

        # Put the control file in place
        tmp_ctlfile_path = path.join(TMP_DIR, '%s_stby.ctl' % db_name)
        operating_system.move(tmp_ctlfile_path,
                              service.paths.ctlfile1_file,
                              as_root=True)
        operating_system.copy(service.paths.ctlfile1_file,
                              service.paths.ctlfile2_file,
                              preserve=True, as_root=True)

        # Set the db_name and db_unique_name via the PFILE which will be
        # removed later
        operating_system.write_file(service.paths.pfile,
                                    "*.db_unique_name='%s'\n"
                                    "*.db_name='%s'\n"
                                    % (db_unique_name, db_name),
                                    as_root=True)
        operating_system.chown(service.paths.pfile,
                               service.instance_owner,
                               service.instance_owner_group,
                               as_root=True, force=True)

        service.admin.delete_conf_cache()
        service.admin.ora_config.db_name = db_name
        service.admin.ora_config.db_unique_name = db_unique_name

        # Set proper permissions on the oratab file
        operating_system.chown(service.paths.oratab_file,
                               service.instance_owner,
                               service.instance_owner_group,
                               as_root=True, force=True)

        # Create the listener.ora file and restart
        service.configure_listener()

    def _restart_listener(self, service):
        service.run_oracle_sys_command('lsnrctl reload')

    def enable_as_slave(self, service, snapshot, slave_config):
        """Turn this node into slave by enabling the log apply process."""
        with service.cursor(service.admin.database_name) as cursor:
            if not self._log_apply_is_running(cursor):
                # Only attempt to enable log apply if it is not already
                # running
                LOG.debug('Slave processes does not exist in '
                          'v$managed_standy, switching on LOG APPLY')
                cursor.execute(str(sql_query.AlterDatabase(
                    'RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE '
                    'DISCONNECT FROM SESSION')))
        self._restart_listener(service)

    def detach_slave(self, service, for_failover=False):
        """Detach this slave by disabling the log apply process,
        setting it to read/write.
        """
        if not for_failover:
            settings = {
                'LOG_ARCHIVE_CONFIG': "''",
                'FAL_SERVER': "''"}
            with service.cursor(service.admin.database_name) as cursor:
                self._clear_log_archive_dests(service, cursor)
                cursor.execute(str(sql_query.AlterDatabase(
                    'RECOVER MANAGED STANDBY DATABASE CANCEL')))
                cursor.execute(str(sql_query.AlterDatabase(
                    'ACTIVATE STANDBY DATABASE')))
                cursor.execute(str(sql_query.AlterDatabase('OPEN')))
                self._update_parameters(service, cursor, settings)

    def cleanup_source_on_replica_detach(self, service, replica_info):
        # Nothing needs to be done to the master when a replica goes away.
        pass

    def get_replica_context(self, service):
        return {'is_master': True}

    def demote_master(self, service):
        pass

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

import glob

import mock
from oslo_utils import netutils

from trove.common import cfg
import trove.common.context as context
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent import backup
from trove.guestagent.common import operating_system
import trove.guestagent.db.models as models
from trove.tests.unittests import trove_testtools


class GuestAgentOracleManagerTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentOracleManagerTest, self).setUp()
        self.context = context.TroveContext()

        # Mock out the Oracle driver cx_Oracle before importing
        # the guestagent functionality
        self.oracle_patch = mock.patch.dict('sys.modules',
                                            {'cx_Oracle': mock.Mock()})
        self.addCleanup(self.oracle_patch.stop)
        self.oracle_patch.start()
        self.patch_datastore_manager('oracle')
        import trove.guestagent.datastore.oracle.manager as manager_module
        self.manager_module = manager_module
        self.manager = self.manager_module.Manager()
        self.manager._oracle_app = mock.MagicMock()

        self.mount_point = '/u01/app/oracle/oradata'
        self.test_instance_name = 'ora1'
        self.patch_conf_property('guest_name', self.test_instance_name)

    def tearDown(self):
        super(GuestAgentOracleManagerTest, self).tearDown()

    def test_prepare(self):
        self.manager.refresh_guest_log_defs = mock.MagicMock()
        self.manager.attach_replica = mock.MagicMock()
        self.manager._perform_restore = mock.MagicMock()

        self.manager.do_prepare(None, None, None, None, None)

        self.manager.attach_replica.assert_not_called()
        self.manager._perform_restore.assert_not_called()
        self.manager.admin.enable_root.assert_not_called()

        self.manager.app.configure_listener.assert_called_with()
        self.assertTrue(self.manager.admin.create_database.called)
        self.manager.app.set_db_start_flag_in_oratab.assert_called_with()
        self.manager.refresh_guest_log_defs.assert_called_with()
        self.manager.app.prep_pfile_management.assert_called_with()

    def test_prepare_from_backup(self):
        self.manager.refresh_guest_log_defs = mock.MagicMock()
        self.manager.attach_replica = mock.MagicMock()
        self.manager._perform_restore = mock.MagicMock()

        backup_info = mock.MagicMock()
        self.manager.do_prepare(None, None, None, None, None,
                                mount_point=self.mount_point,
                                backup_info=backup_info)

        self.manager.attach_replica.assert_not_called()
        self.manager.app.configure_listener.assert_not_called()
        self.manager.admin.create_database.assert_not_called()
        self.manager.admin.enable_root.assert_not_called()

        self.manager._perform_restore.assert_called_with(
            backup_info, None, self.mount_point, self.manager.app)
        self.manager.app.set_db_start_flag_in_oratab.assert_called_with()
        self.manager.refresh_guest_log_defs.assert_called_with()
        self.manager.app.prep_pfile_management.assert_called_with()

    def test_prepare_from_snapshot(self):
        self.manager.refresh_guest_log_defs = mock.MagicMock()
        self.manager.attach_replica = mock.MagicMock()
        self.manager._perform_restore = mock.MagicMock()

        config = mock.MagicMock()
        snapshot = {'config': config}
        self.manager.do_prepare(None, None, None, None, None,
                                snapshot=snapshot)

        self.manager._perform_restore.assert_not_called()
        self.manager.app.configure_listener.assert_not_called()
        self.manager.admin.create_database.assert_not_called()
        self.manager.app.set_db_start_flag_in_oratab.assert_not_called()
        self.manager.admin.enable_root.assert_not_called()
        self.manager.refresh_guest_log_defs.assert_not_called()
        self.manager.app.prep_pfile_management.assert_not_called()

        self.manager.attach_replica.assert_called_with(None, snapshot, config)

    def test_prepare_create_database(self):
        self.manager.refresh_guest_log_defs = mock.MagicMock()

        create_db_mock = mock.MagicMock()
        self.manager.admin.create_database = create_db_mock

        def check_test(test_tag, db_name):
            self.assertEqual(
                db_name,
                create_db_mock.call_args[0][0][0]['_name'],
                message="Test %s failed")
            create_db_mock.reset_mock()

        self.manager.do_prepare(None, None, None, None, None)
        check_test('short_instance_name', self.test_instance_name)

        self.patch_conf_property('guest_name', 'ora1toolong')
        self.manager.do_prepare(None, None, None, None, None)
        check_test('long_instance_name', 'ora1tool')

        test_db_name = 'testdb'
        database = models.ValidatedMySQLDatabase()
        database.name = test_db_name
        self.manager.do_prepare(None, None,
                                [database.serialize()],
                                None, None)
        check_test('given_db_name', test_db_name)

    def test_prepare_enable_root(self):
        self.manager.refresh_guest_log_defs = mock.MagicMock()
        self.manager._perform_restore = mock.MagicMock()

        enable_root_mock = mock.MagicMock()
        self.manager.admin.enable_root = enable_root_mock

        pwd = 'password'
        self.manager.do_prepare(None, None, None, None, None,
                                root_password=pwd)
        enable_root_mock.assert_called_with(pwd)
        enable_root_mock.reset_mock()

        self.manager.do_prepare(None, None, None, None, None,
                                root_password=pwd,
                                backup_info=mock.Mock())
        enable_root_mock.assert_called_with(pwd)

    @mock.patch.object(backup, 'restore')
    def test_perform_restore(self, mock_restore):
        db_name = 'testdb'
        backup_info = {'id': None}
        restore_location = '/some/path'

        self.manager.admin.database_name = db_name

        self.manager._perform_restore(
            backup_info, None, restore_location, None)

        mock_restore.assert_called_with(None, backup_info, restore_location)
        self.manager.admin.delete_conf_cache.assert_called_with()
        self.manager.app.paths.update_db_name.assert_called_with(db_name)


class GuestAgentOracleBackupTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentOracleBackupTest, self).setUp()
        self.context = context.TroveContext()
        self.patch_datastore_manager('oracle')

        # Mock out the Oracle driver cx_Oracle before importing
        # the guestagent functionality
        self.oracle_patch = mock.patch.dict('sys.modules',
                                            {'cx_Oracle': mock.Mock()})
        self.addCleanup(self.oracle_patch.stop)
        self.oracle_patch.start()
        import trove.guestagent.datastore.oracle.service as service_module
        self.service_module = service_module
        import trove.guestagent.strategies.backup.oracle_impl as backup_module
        self.backup_module = backup_module

        self.filename = 'backup_file'
        self.dbname = 'testdb'
        self.admin = 'os_admin'
        self.password = 'password'
        self.backup_level = 0

    def tearDown(self):
        super(GuestAgentOracleBackupTest, self).tearDown()

    @mock.patch.object(operating_system, 'exists', return_value=False)
    def _instantiate_backup(self, cls, *args):
        with mock.patch.object(self.service_module, 'OracleVMConfig'):
            inst = cls(filename=self.filename)
            inst.db_name = self.dbname
            inst.app.paths = self.service_module.OracleVMPaths(self.dbname)
            inst.app.admin.admin_user_name = self.admin
            inst.app.admin.ora_config.admin_password = self.password
            return inst

    @mock.patch.object(operating_system, 'get_bytes_free_on_fs',
                       return_value=1)
    @mock.patch.object(operating_system, 'create_directory')
    @mock.patch.object(utils, 'execute_with_timeout')
    def test_run_pre_backup(self, mock_exec, *args):
        backup = self._instantiate_backup(self.backup_module.RmanBackup)
        backup.cleanup = mock.MagicMock()
        backup.estimate_backup_size = mock.MagicMock(return_value=0)

        backup._run_pre_backup()

        mock_exec.assert_called_with(
            "su - oracle -c \"export ORACLE_SID=%(dbname)s;\n"
            "rman TARGET %(admin_user)s/%(admin_pswd)s <<EOF\n"
            "run {\n"
            "configure backup optimization on;\n"
            "configure channel device type disk format "
            "'/u01/app/oracle/oradata/backupset_files/%(dbname)s/"
            "%%I_%%u_%%s_%(filename)s.dat';\n"
            "backup incremental level=0 as compressed backupset database "
            "plus archivelog;\n"
            "backup current controlfile format "
            "'/u01/app/oracle/oradata/backupset_files/"
            "%(dbname)s/%%I_%%u_%%s_%(filename)s.ctl';\n"
            "}\n"
            "EXIT;\n"
            "EOF\"\n" %
            {'admin_user': self.admin,
             'admin_pswd': self.password,
             'dbname': self.dbname,
             'filename': self.filename},
            run_as_root=True, root_helper='sudo', shell=True,
            timeout=cfg.CONF.restore_usage_timeout)

    @mock.patch.object(utils, 'execute_with_timeout')
    def test_truncate_backup_chain(self, mock_exec):
        inc_backup = self._instantiate_backup(
            self.backup_module.RmanBackupIncremental)
        parent_id = 'parent_id_12345'
        inc_backup.parent_id = parent_id
        mock_query = mock.MagicMock()
        inc_backup.app.cursor = mock_query
        mock_query().__enter__().__iter__.return_value = [['abc'], ['123']]
        inc_backup._run_pre_backup = mock.MagicMock()

        inc_backup._truncate_backup_chain()

        mock_query().__enter__().execute.assert_called_with(
            "SELECT recid FROM v$backup_piece WHERE recid > "
            "(SELECT max(recid) FROM v$backup_piece WHERE handle like "
            "'%%%s%%')"
            % parent_id)
        mock_exec.assert_called_with(
            "su - oracle -c \"export ORACLE_SID=%(dbname)s;\n"
            "rman TARGET %(admin_user)s/%(admin_pswd)s <<EOF\n"
            "run {\n"
            "delete force noprompt backupset abc,123;\n"
            "}\n"
            "EXIT;\n"
            "EOF\"\n" %
            {'admin_user': self.admin,
             'admin_pswd': self.password,
             'dbname': self.dbname},
            run_as_root=True, root_helper='sudo', shell=True)


class GuestAgentOracleRestoreTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentOracleRestoreTest, self).setUp()
        self.context = context.TroveContext()
        self.patch_datastore_manager('oracle')

        # Mock out the Oracle driver cx_Oracle before importing
        # the guestagent functionality
        self.oracle_patch = mock.patch.dict('sys.modules',
                                            {'cx_Oracle': mock.Mock()})
        self.addCleanup(self.oracle_patch.stop)
        self.oracle_patch.start()
        import trove.guestagent.datastore.oracle.service as service_module
        self.service_module = service_module
        import trove.guestagent.strategies.restore.oracle_impl as restore_module
        self.restore_module = restore_module

        self.backup_id = 'testbackup'
        self.dbname = 'testdb'
        self.admin = 'os_admin'
        self.password = 'password'

    def tearDown(self):
        super(GuestAgentOracleRestoreTest, self).tearDown()

    @mock.patch.object(operating_system, 'exists', return_value=False)
    def _instantiate_restore(self, cls, *args):
        with mock.patch.object(self.service_module, 'OracleVMConfig'):
            inst = cls(None, location=None, checksum=None,
                       backup_id=self.backup_id)
            inst.db_name = self.dbname
            inst.app.paths = self.service_module.OracleVMPaths(self.dbname)
            inst.app.admin.admin_user_name = self.admin
            inst.app.admin.ora_config.admin_password = self.password
            return inst

    @mock.patch.object(glob, 'glob')
    @mock.patch.object(utils, 'execute_with_timeout')
    def test_perform_restore(self, mock_exec, mock_glob):
        restore = self._instantiate_restore(self.restore_module.RmanBackup)
        control_file = '/path/to/control/file'
        mock_glob.return_value = [control_file]

        restore._perform_restore()

        mock_exec.assert_called_with(
            "su - oracle -c \"export ORACLE_SID=%(dbname)s;\n"
            "rman TARGET %(admin_user)s/%(admin_pswd)s <<EOF\n"
            "run {\n"
            "startup nomount;\n"
            "restore controlfile from '%(ctl_file)s';\n"
            "startup mount;\n"
            "crosscheck backup;\n"
            "delete noprompt expired backup;\n"
            "restore database;\n"
            "}\n"
            "EXIT;\n"
            "EOF\"\n" %
            {'admin_user': self.admin,
             'admin_pswd': self.password,
             'dbname': self.dbname,
             'ctl_file': control_file},
            run_as_root=True, root_helper='sudo', shell=True,
            timeout=cfg.CONF.restore_usage_timeout)

    @mock.patch.object(utils, 'execute_with_timeout')
    def test_perform_recover(self, mock_exec):
        restore = self._instantiate_restore(self.restore_module.RmanBackup)

        restore._perform_recover()

        mock_exec.assert_called_with(
            "su - oracle -c \"export ORACLE_SID=%(dbname)s;\n"
            "rman TARGET %(admin_user)s/%(admin_pswd)s <<EOF\n"
            "run {\n"
            "recover database;\n"
            "}\n"
            "EXIT;\n"
            "EOF\"\n" %
            {'admin_user': self.admin,
             'admin_pswd': self.password,
             'dbname': self.dbname},
            run_as_root=True, root_helper='sudo', shell=True,
            timeout=cfg.CONF.restore_usage_timeout)

    @mock.patch.object(utils, 'execute_with_timeout')
    def test_open_database(self, mock_exec):
        restore = self._instantiate_restore(self.restore_module.RmanBackup)

        restore._open_database()

        mock_exec.assert_called_with(
            "su - oracle -c \"export ORACLE_SID=%(dbname)s;\n"
            "rman TARGET %(admin_user)s/%(admin_pswd)s <<EOF\n"
            "run {\n"
            "alter database open resetlogs;\n"
            "}\n"
            "EXIT;\n"
            "EOF\"\n" %
            {'admin_user': self.admin,
             'admin_pswd': self.password,
             'dbname': self.dbname},
            run_as_root=True, root_helper='sudo', shell=True,
            timeout=cfg.CONF.oracle.usage_timeout)

    @mock.patch.object(operating_system, 'read_file')
    @mock.patch.object(operating_system, 'write_file')
    @mock.patch.object(operating_system, 'chown')
    def test_create_oratab_entry(self, mock_chown, mock_write, mock_read):
        restore = self._instantiate_restore(self.restore_module.RmanBackup)
        mock_read.return_value = "TEST FILE"
        oratab_file = '/etc/oratab'
        oratab_contents = ("TEST FILE\n"
                           "%(dbname)s:/u01/app/oracle/product/dbaas:N\n"
                           % {'dbname': self.dbname})

        restore._create_oratab_entry()

        mock_read.assert_called_with(oratab_file, as_root=True)
        mock_write.assert_called_with(oratab_file, oratab_contents,
                                      as_root=True)
        mock_chown.assert_called_with(oratab_file,
                                      'oracle', 'oinstall',
                                      recursive=True, force=True, as_root=True)


class GuestAgentOracleReplicationTest(trove_testtools.TestCase):

    @mock.patch.object(operating_system, 'exists', return_value=False)
    def setUp(self, *args):
        super(GuestAgentOracleReplicationTest, self).setUp()
        self.context = context.TroveContext()
        self.patch_datastore_manager('oracle')

        # Mock out the Oracle driver cx_Oracle before importing
        # the guestagent functionality
        mock_cx_oracle = mock.MagicMock()
        mock_cx_oracle.SYSDBA = 0
        mock_cx_oracle.PRELIM_AUTH = 1
        self.oracle_patch = mock.patch.dict('sys.modules',
                                            {'cx_Oracle': mock_cx_oracle})
        self.addCleanup(self.oracle_patch.stop)
        self.oracle_patch.start()
        import trove.guestagent.datastore.oracle.service as service_module
        self.service_module = service_module
        self.dbname = 'testdb'
        with mock.patch.object(self.service_module,
                               'OracleVMConfig') as mock_conf:
            mock_conf().db_name = self.dbname
            self.service = self.service_module.OracleVMApp(
                self.service_module.OracleVMAppStatus())

        from trove.guestagent.strategies.replication import oracle_sync
        self.replication_module = oracle_sync
        self.replication = self.replication_module.OracleSyncReplication()

        self.cursor = mock.MagicMock()

    def tearDown(self):
        super(GuestAgentOracleReplicationTest, self).tearDown()

    def test_update_parameters(self):
        mock_cm = mock.MagicMock()
        self.service.configuration_manager = mock_cm
        mock_gen_spfile = mock.MagicMock()
        self.service.generate_spfile = mock_gen_spfile

        values = [{'key1': 'value1', 'key2': 'value2'},
                  {'key3': 'value3'},]

        self.replication._update_parameters(
            self.service, self.cursor, values[0])
        self.replication._update_parameters(
            self.service, self.cursor, values[1], deferred=True)

        self.cursor.execute.assert_has_calls([
            mock.call("ALTER SYSTEM SET key1 = 'value1' SCOPE = BOTH"),
            mock.call("ALTER SYSTEM SET key2 = 'value2' SCOPE = BOTH"),
            mock.call("ALTER SYSTEM SET key3 = 'value3' SCOPE = SPFILE")],
            any_order=True)
        mock_cm.apply_system_override.assert_has_calls([
            mock.call({'key1': 'value1', 'key2': 'value2'}),
            mock.call({'key3': 'value3'})])
        mock_gen_spfile.assert_has_calls([mock.call(), mock.call()])

    @mock.patch.object(operating_system, 'list_files_in_directory',
                       return_value=['/u01/oracle/controlfile/test.ctl'])
    @mock.patch.object(operating_system, 'remove')
    @mock.patch.object(operating_system, 'read_file')
    @mock.patch.object(netutils, 'get_my_ipv4')
    @mock.patch.object(utils, 'execute_with_timeout')
    def test_get_master_ref(self, mock_exec, mock_ipv4, mock_read, *args):
        mock_query = mock.MagicMock()
        self.service.cursor = mock_query
        mock_query().__enter__().fetchone.return_value = ['db1,db2']

        ref = self.replication.get_master_ref(self.service, None)

        mock_query().__enter__().execute.assert_has_calls([
            mock.call("ALTER DATABASE CREATE STANDBY CONTROLFILE "
                      "AS '/tmp/test.ctl.bak'"),
            mock.call("SELECT VALUE FROM V$PARAMETER WHERE "
                      "NAME = 'fal_server'")])
        mock_exec.assert_called_with(
            'tar', '-Pczvf', '/tmp/oradata.tar.gz',
            '/tmp/test.ctl.bak',
            '/u01/app/oracle/product/dbaas/dbs/orapw%s' % self.dbname,
            '/etc/oratab', '/etc/oracle/oracle.cnf',
            run_as_root=True, root_helper='sudo')
        self.assertEqual(
            {'host': mock_ipv4(),
             'db_name': self.dbname,
             'db_list': [self.dbname] + ['db1', 'db2'],
             'oradata': mock_read()},
            ref)

    def test_log_apply_is_running(self):
        mock_query = mock.MagicMock()
        self.cursor.execute = mock_query

        self.cursor.fetchone.return_value = [1]
        self.assertTrue(self.replication._log_apply_is_running(self.cursor))

        self.cursor.fetchone.return_value = [0]
        self.assertFalse(self.replication._log_apply_is_running(self.cursor))

        mock_query.assert_called_with(
            "SELECT COUNT(*) FROM V$MANAGED_STANDBY WHERE PROCESS LIKE 'MRP%'")

    def test_enable_as_master(self):
        self.service.cursor = self.cursor
        self.replication._log_apply_is_running = mock.MagicMock(
            return_value=True)

        self.replication.enable_as_master(self.service, None)

        self.cursor().__enter__().execute.assert_has_calls([
            mock.call("ALTER DATABASE RECOVER MANAGED STANDBY DATABASE FINISH"),
            mock.call("ALTER DATABASE ACTIVATE STANDBY DATABASE"),
            mock.call("ALTER DATABASE OPEN"),
            mock.call("ALTER SYSTEM SWITCH LOGFILE")])

    @mock.patch.object(stream_codecs, 'KeyValueCodec')
    @mock.patch.object(operating_system, 'chown')
    @mock.patch.object(operating_system, 'write_file')
    def test_create_tns_file(self, mock_write, mock_chown, mock_codec):
        tns_file = '/u01/app/oracle/product/dbaas/network/admin/tnsnames.ora'
        dbs = [{'db_unique_name': 'db1', 'host': '10.0.0.1'},
               {'db_unique_name': 'db2', 'host': '10.0.0.2'}]
        tns_data = {
            dbs[0]['db_unique_name']:
                '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)'
                '(HOST=%(host1)s)(PORT=1521))'
                '(CONNECT_DATA=(SERVICE_NAME=%(dbname)s)))'
                % {'dbname': self.dbname,
                   'host1': dbs[0]['host']},
            dbs[1]['db_unique_name']:
                '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)'
                '(HOST=%(host2)s)(PORT=1521))'
                '(CONNECT_DATA=(SERVICE_NAME=%(dbname)s)))'
                % {'dbname': self.dbname,
                   'host2': dbs[1]['host']}}

        self.replication._create_tns_file(self.service, dbs)

        mock_write.assert_called_with(tns_file, tns_data,
                                      codec=mock_codec(),
                                      as_root=True)
        mock_chown.assert_called_with(tns_file, 'oracle', 'oinstall',
                                      force=True, as_root=True)

    def test_force_logging_enabled(self):
        self.cursor.fetchone.return_value = ['YES']
        self.assertTrue(self.replication._force_logging_enabled(self.cursor))
        self.cursor.fetchone.return_value = ['NO']
        self.assertFalse(self.replication._force_logging_enabled(self.cursor))
        self.cursor.execute.assert_called_with(
            'SELECT FORCE_LOGGING FROM V$DATABASE')

    def test_create_static_params(self):
        procs = 100
        self.patch_conf_property('log_archive_max_process', procs,
                                 section='oracle')
        mock_force = mock.MagicMock()
        self.replication._force_logging_enabled = mock_force
        mock_update = mock.MagicMock()
        self.replication._update_parameters = mock_update

        mock_force.return_value = True
        self.replication._create_static_params(self.service, self.cursor)
        self.cursor.execute.assert_not_called()
        mock_update.assert_has_calls([
            mock.call(
                self.service, self.cursor,
                {'LOG_ARCHIVE_MAX_PROCESSES': procs,
                 'STANDBY_FILE_MANAGEMENT': 'AUTO',
                 'REDO_TRANSPORT_USER': 'OS_ADMIN'}),
            mock.call(
                self.service, self.cursor,
                {'LOG_ARCHIVE_FORMAT': '%t_%s_%r.arc',
                 'REMOTE_LOGIN_PASSWORDFILE': 'EXCLUSIVE'},
                deferred=True)])

        mock_force.return_value = False
        self.replication._create_static_params(self.service, self.cursor)
        self.cursor.execute.assert_called_with('ALTER DATABASE FORCE LOGGING')

    def test_clear_log_archive_dests(self):
        mock_update = mock.MagicMock()
        self.replication._update_parameters = mock_update

        self.replication._clear_log_archive_dests(self.service, self.cursor)
        mock_update.assert_called_with(
            self.service, self.cursor,
            {"LOG_ARCHIVE_DEST_9": "''",
             "LOG_ARCHIVE_DEST_8": "''",
             "LOG_ARCHIVE_DEST_3": "''",
             "LOG_ARCHIVE_DEST_2": "''",
             "LOG_ARCHIVE_DEST_7": "''",
             "LOG_ARCHIVE_DEST_6": "''",
             "LOG_ARCHIVE_DEST_5": "''",
             "LOG_ARCHIVE_DEST_4": "''",
             "LOG_ARCHIVE_DEST_22": "''",
             "LOG_ARCHIVE_DEST_23": "''",
             "LOG_ARCHIVE_DEST_20": "''",
             "LOG_ARCHIVE_DEST_21": "''",
             "LOG_ARCHIVE_DEST_26": "''",
             "LOG_ARCHIVE_DEST_27": "''",
             "LOG_ARCHIVE_DEST_24": "''",
             "LOG_ARCHIVE_DEST_25": "''",
             "LOG_ARCHIVE_DEST_28": "''",
             "LOG_ARCHIVE_DEST_29": "''",
             "LOG_ARCHIVE_DEST_19": "''",
             "LOG_ARCHIVE_DEST_18": "''",
             "LOG_ARCHIVE_DEST_13": "''",
             "LOG_ARCHIVE_DEST_12": "''",
             "LOG_ARCHIVE_DEST_11": "''",
             "LOG_ARCHIVE_DEST_10": "''",
             "LOG_ARCHIVE_DEST_17": "''",
             "LOG_ARCHIVE_DEST_16": "''",
             "LOG_ARCHIVE_DEST_15": "''",
             "LOG_ARCHIVE_DEST_14": "''",
             "LOG_ARCHIVE_DEST_30": "''"})

    def test_update_dynamic_params(self):
        mock_update = mock.MagicMock()
        self.replication._update_parameters = mock_update
        mock_clear = mock.MagicMock()
        self.replication._clear_log_archive_dests = mock_clear
        self.service.admin.ora_config.db_unique_name = 'db1'

        self.replication._update_dynamic_params(
            self.service, self.cursor,
            [{'db_unique_name': 'db1'},
             {'db_unique_name': 'db2'}])

        mock_clear.assert_called_with(self.service, self.cursor)
        mock_update.assert_has_calls([
            mock.call(
                self.service, self.cursor,
                {'LOG_ARCHIVE_CONFIG': 'DG_CONFIG=(db1,db2)',
                 'FAL_SERVER': "'db1','db2'"}),
            mock.call(
                self.service, self.cursor,
                {'LOG_ARCHIVE_DEST_2': 'SERVICE=db2 NOAFFIRM ASYNC VALID_FOR='
                                       '(ONLINE_LOGFILES,PRIMARY_ROLE) '
                                       'DB_UNIQUE_NAME=db2',
                 'LOG_ARCHIVE_DEST_STATE_2': 'ENABLE'})])

    def test_create_standby_log_files(self):
        self.patch_conf_property('mount_point', '/u01/app/oracle/oradata',
                                 section='oracle')
        dir = '/u01/app/oracle/oradata/testdb'
        self.patch_conf_property('standby_log_count', 2,
                                 section='oracle')
        self.patch_conf_property('standby_log_size', 10,
                                 section='oracle')

        self.replication._create_standby_log_files(self.service, self.cursor)

        self.cursor.execute.assert_has_calls([
            mock.call(
                "ALTER DATABASE ADD STANDBY LOGFILE "
                "('%s/standby_redo1.log') SIZE 10M" % dir),
            mock.call(
                "ALTER DATABASE ADD STANDBY LOGFILE "
                "('%s/standby_redo2.log') SIZE 10M" % dir)])

    def test_complete_master_setup(self):
        self.service.cursor = self.cursor
        cursor = self.cursor().__enter__()

        slave_detail = [mock.MagicMock()]
        mock_detail = mock.MagicMock()
        self.replication.get_replication_detail = mock_detail
        dbs = [mock_detail()] + slave_detail

        mock_is_new = mock.MagicMock()
        self.replication._is_new_master_node = mock_is_new
        self.replication._create_standby_log_files = mock.MagicMock()
        self.replication._create_static_params = mock.MagicMock()
        self.replication._create_tns_file = mock.MagicMock()
        self.replication._update_dynamic_params = mock.MagicMock()


        mock_is_new.return_value = False
        self.replication.complete_master_setup(self.service, slave_detail)

        self.replication._create_standby_log_files.assert_not_called()
        self.replication._create_static_params.assert_not_called()
        self.replication._create_tns_file.assert_called_with(
            self.service, dbs)
        self.replication._update_dynamic_params.assert_called_with(
            self.service, cursor, dbs)
        cursor.execute.assert_called_with(
            'ALTER SYSTEM SWITCH LOGFILE')

        mock_is_new.return_value = True
        self.replication.complete_master_setup(self.service, slave_detail)
        self.replication._create_standby_log_files.assert_called_with(
            self.service, cursor)
        self.replication._create_static_params.assert_called_with(
            self.service, cursor)

    @mock.patch.object(utils, 'execute_with_timeout')
    def test_complete_new_slave_setup(self, mock_exec):
        self.service.admin.ora_config.root_password = 'syspassword'
        self.service.admin.ora_config.db_unique_name = 'db0'
        self.service.prep_pfile_management = mock.MagicMock()
        self.service.client = mock.MagicMock()
        self.service.cursor = self.cursor
        cursor = self.cursor().__enter__()

        dbs = [{'db_unique_name': 'db1'},
               {'db_unique_name': 'db2'}]
        self.replication._complete_new_slave_setup(
            self.service, 'masterhost', dbs)

        self.assertTrue(self.service.client().__enter__().startup.called)
        mock_exec.assert_called_with(
            "su - oracle -c \"rman "
            "TARGET sys/syspassword@masterhost/testdb "
            "AUXILIARY sys/syspassword@db0 <<EOF\n"
            "run {\n"
            "DUPLICATE TARGET DATABASE FOR STANDBY FROM ACTIVE DATABASE "
            "DORECOVER SPFILE SET db_unique_name='db0' COMMENT "
            "'Is standby' SET FAL_SERVER='db1','db2' COMMENT "
            "'Is primary' NOFILENAMECHECK;\n"
            "}\n"
            "EXIT;\n"
            "EOF\"\n",
            run_as_root=True, root_helper='sudo', shell=True,
            timeout=cfg.CONF.restore_usage_timeout)
        cursor.execute.assert_has_calls([
            mock.call("ALTER SYSTEM SET REDO_TRANSPORT_USER = 'OS_ADMIN' "
                      "SCOPE = BOTH"),
            mock.call('ALTER DATABASE OPEN READ ONLY'),
            mock.call('ALTER DATABASE RECOVER MANAGED STANDBY DATABASE '
                      'USING CURRENT LOGFILE DISCONNECT FROM SESSION')])
        self.service.prep_pfile_management.assert_called_with()

    def test_complete_slave_setup(self):
        self.replication._is_new_slave_node = mock.MagicMock(return_value=False)
        self.replication._create_tns_file = mock.MagicMock()
        self.replication._complete_new_slave_setup = mock.MagicMock()

        master = {'host': 'masterhost'}
        slaves = [mock.MagicMock()]
        dbs = [master] + slaves

        self.replication.complete_slave_setup(self.service, master, slaves)
        self.replication._create_tns_file.assert_not_called()
        self.replication._complete_new_slave_setup.assert_not_called()

        self.replication._is_new_slave_node.return_value = True
        self.replication.complete_slave_setup(self.service, master, slaves)
        self.replication._create_tns_file.assert_called_with(self.service, dbs)
        self.replication._complete_new_slave_setup.assert_called_with(
            self.service, 'masterhost', dbs)

    def test_sync_data_to_slaves(self):
        self.service.cursor = self.cursor
        self.replication.sync_data_to_slaves(self.service)
        self.cursor().__enter__().execute.assert_called_with(
            'ALTER SYSTEM SWITCH LOGFILE')

    def test_restart_listener(self):
        self.service.run_oracle_sys_command = mock.MagicMock()
        self.replication._restart_listener(self.service)
        self.service.run_oracle_sys_command.assert_called_with(
            'lsnrctl reload')

    def test_enable_as_slave(self):
        self.service.cursor = self.cursor
        cursor = self.cursor().__enter__()
        self.replication._restart_listener = mock.MagicMock()
        self.replication._log_apply_is_running = mock.MagicMock(
            return_value=True)

        self.replication.enable_as_slave(self.service, None, None)
        cursor.execute.assert_not_called()
        self.replication._restart_listener.assert_called_with(self.service)

        self.replication._log_apply_is_running.return_value = False
        self.replication.enable_as_slave(self.service, None, None)
        cursor.execute.assert_called_with(
            'ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT '
            'LOGFILE DISCONNECT FROM SESSION')

    def test_detach_slave(self):
        self.service.cursor = self.cursor
        cursor = self.cursor().__enter__()
        self.replication._clear_log_archive_dests = mock.MagicMock()
        self.replication._update_parameters = mock.MagicMock()

        self.replication.detach_slave(self.service, for_failover=True)
        cursor.execute.assert_not_called()

        self.replication.detach_slave(self.service)
        self.replication._clear_log_archive_dests.assert_called_with(
            self.service, cursor)
        cursor.execute.assert_has_calls([
            mock.call('ALTER DATABASE RECOVER MANAGED STANDBY '
                      'DATABASE CANCEL'),
            mock.call('ALTER DATABASE ACTIVATE STANDBY DATABASE'),
            mock.call('ALTER DATABASE OPEN')])
        self.replication._update_parameters.assert_called_with(
            self.service, cursor,
            {'LOG_ARCHIVE_CONFIG': "''", 'FAL_SERVER': "''"})


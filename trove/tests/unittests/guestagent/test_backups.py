#    Copyright 2012 OpenStack Foundation
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
import mock
from mock import ANY, DEFAULT, patch
from oslo_utils import netutils
from testtools.testcase import ExpectedException
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.strategies.backup import base as backupBase
from trove.guestagent.strategies.backup import mysql_impl
from trove.guestagent.strategies.restore import base as restoreBase
from trove.guestagent.strategies.restore.mysql_impl import MySQLRestoreMixin
from trove.tests.unittests import trove_testtools

from trove.guestagent.datastore.experimental.cassandra import (
    service as cass_service
)

BACKUP_XTRA_CLS = ("trove.guestagent.strategies.backup."
                   "mysql_impl.InnoBackupEx")
RESTORE_XTRA_CLS = ("trove.guestagent.strategies.restore."
                    "mysql_impl.InnoBackupEx")
BACKUP_XTRA_INCR_CLS = ("trove.guestagent.strategies.backup."
                        "mysql_impl.InnoBackupExIncremental")
RESTORE_XTRA_INCR_CLS = ("trove.guestagent.strategies.restore."
                         "mysql_impl.InnoBackupExIncremental")
BACKUP_SQLDUMP_CLS = ("trove.guestagent.strategies.backup."
                      "mysql_impl.MySQLDump")
RESTORE_SQLDUMP_CLS = ("trove.guestagent.strategies.restore."
                       "mysql_impl.MySQLDump")
BACKUP_CBBACKUP_CLS = ("trove.guestagent.strategies.backup."
                       "experimental.couchbase_impl.CbBackup")
RESTORE_CBBACKUP_CLS = ("trove.guestagent.strategies.restore."
                        "experimental.couchbase_impl.CbBackup")
BACKUP_NODETOOLSNAPSHOT_CLS = ("trove.guestagent.strategies.backup."
                               "experimental.cassandra_impl.NodetoolSnapshot")
RESTORE_NODETOOLSNAPSHOT_CLS = ("trove.guestagent.strategies.restore."
                                "experimental.cassandra_impl.NodetoolSnapshot")
BACKUP_MONGODUMP_CLS = ("trove.guestagent.strategies.backup."
                        "experimental.mongo_impl.MongoDump")
RESTORE_MONGODUMP_CLS = ("trove.guestagent.strategies.restore."
                         "experimental.mongo_impl.MongoDump")

PIPE = " | "
ZIP = "gzip"
UNZIP = "gzip -d -c"
ENCRYPT = "openssl enc -aes-256-cbc -salt -pass pass:default_aes_cbc_key"
DECRYPT = "openssl enc -d -aes-256-cbc -salt -pass pass:default_aes_cbc_key"
XTRA_BACKUP_RAW = ("sudo innobackupex --stream=xbstream %(extra_opts)s"
                   " /var/lib/mysql/data 2>/tmp/innobackupex.log")
XTRA_BACKUP = XTRA_BACKUP_RAW % {'extra_opts': ''}
XTRA_BACKUP_EXTRA_OPTS = XTRA_BACKUP_RAW % {'extra_opts': '--no-lock'}
XTRA_BACKUP_INCR = ('sudo innobackupex --stream=xbstream'
                    ' --incremental --incremental-lsn=%(lsn)s'
                    ' %(extra_opts)s /var/lib/mysql/data'
                    ' 2>/tmp/innobackupex.log')
SQLDUMP_BACKUP_RAW = ("mysqldump --all-databases %(extra_opts)s "
                      "--opt --password=password -u os_admin"
                      " 2>/tmp/mysqldump.log")
SQLDUMP_BACKUP = SQLDUMP_BACKUP_RAW % {'extra_opts': ''}
SQLDUMP_BACKUP_EXTRA_OPTS = (SQLDUMP_BACKUP_RAW %
                             {'extra_opts': '--events --routines --triggers'})
XTRA_RESTORE_RAW = "sudo xbstream -x -C %(restore_location)s"
XTRA_RESTORE = XTRA_RESTORE_RAW % {'restore_location': '/var/lib/mysql/data'}
XTRA_INCR_PREPARE = ("sudo innobackupex --apply-log"
                     " --redo-only /var/lib/mysql/data"
                     " --defaults-file=/var/lib/mysql/data/backup-my.cnf"
                     " --ibbackup xtrabackup %(incr)s"
                     " 2>/tmp/innoprepare.log")
SQLDUMP_RESTORE = "sudo mysql"
PREPARE = ("sudo innobackupex --apply-log /var/lib/mysql/data "
           "--defaults-file=/var/lib/mysql/data/backup-my.cnf "
           "--ibbackup xtrabackup 2>/tmp/innoprepare.log")
CRYPTO_KEY = "default_aes_cbc_key"

CBBACKUP_CMD = "tar cpPf - /tmp/backups"

CBBACKUP_RESTORE = "sudo tar xpPf -"

MONGODUMP_CMD = "sudo tar cPf - /var/lib/mongodb/dump"

MONGODUMP_RESTORE = "sudo tar xPf -"


class GuestAgentBackupTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentBackupTest, self).setUp()
        self.orig = mysql_impl.get_auth_password
        mysql_impl.get_auth_password = mock.Mock(
            return_value='password')
        self.orig_exec_with_to = utils.execute_with_timeout

    def tearDown(self):
        super(GuestAgentBackupTest, self).tearDown()
        mysql_impl.get_auth_password = self.orig
        utils.execute_with_timeout = self.orig_exec_with_to

    def test_backup_decrypted_xtrabackup_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = False
        RunnerClass = utils.import_class(BACKUP_XTRA_CLS)
        bkup = RunnerClass(12345, extra_opts="")
        self.assertEqual(XTRA_BACKUP + PIPE + ZIP, bkup.command)
        self.assertEqual("12345.xbstream.gz", bkup.manifest)

    def test_backup_decrypted_xtrabackup_with_extra_opts_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = False
        RunnerClass = utils.import_class(BACKUP_XTRA_CLS)
        bkup = RunnerClass(12345, extra_opts="--no-lock")
        self.assertEqual(XTRA_BACKUP_EXTRA_OPTS + PIPE + ZIP, bkup.command)
        self.assertEqual("12345.xbstream.gz", bkup.manifest)

    def test_backup_encrypted_xtrabackup_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = True
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_XTRA_CLS)
        bkup = RunnerClass(12345, extra_opts="")
        self.assertEqual(XTRA_BACKUP + PIPE + ZIP + PIPE + ENCRYPT,
                         bkup.command)
        self.assertEqual("12345.xbstream.gz.enc", bkup.manifest)

    def test_backup_xtrabackup_incremental(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = False
        RunnerClass = utils.import_class(BACKUP_XTRA_INCR_CLS)
        opts = {'lsn': '54321', 'extra_opts': ''}
        expected = (XTRA_BACKUP_INCR % opts) + PIPE + ZIP
        bkup = RunnerClass(12345, extra_opts="", lsn="54321")
        self.assertEqual(expected, bkup.command)
        self.assertEqual("12345.xbstream.gz", bkup.manifest)

    def test_backup_xtrabackup_incremental_with_extra_opts_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = False
        RunnerClass = utils.import_class(BACKUP_XTRA_INCR_CLS)
        opts = {'lsn': '54321', 'extra_opts': '--no-lock'}
        expected = (XTRA_BACKUP_INCR % opts) + PIPE + ZIP
        bkup = RunnerClass(12345, extra_opts="--no-lock", lsn="54321")
        self.assertEqual(expected, bkup.command)
        self.assertEqual("12345.xbstream.gz", bkup.manifest)

    def test_backup_xtrabackup_incremental_encrypted(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = True
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_XTRA_INCR_CLS)
        opts = {'lsn': '54321', 'extra_opts': ''}
        expected = (XTRA_BACKUP_INCR % opts) + PIPE + ZIP + PIPE + ENCRYPT
        bkup = RunnerClass(12345, extra_opts="", lsn="54321")
        self.assertEqual(expected, bkup.command)
        self.assertEqual("12345.xbstream.gz.enc", bkup.manifest)

    def test_backup_decrypted_mysqldump_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = False
        RunnerClass = utils.import_class(BACKUP_SQLDUMP_CLS)
        bkup = RunnerClass(12345, extra_opts="")
        self.assertEqual(SQLDUMP_BACKUP + PIPE + ZIP, bkup.command)
        self.assertEqual("12345.gz", bkup.manifest)

    def test_backup_decrypted_mysqldump_with_extra_opts_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = False
        RunnerClass = utils.import_class(BACKUP_SQLDUMP_CLS)
        bkup = RunnerClass(12345, extra_opts="--events --routines --triggers")
        self.assertEqual(SQLDUMP_BACKUP_EXTRA_OPTS + PIPE + ZIP, bkup.command)
        self.assertEqual("12345.gz", bkup.manifest)

    def test_backup_encrypted_mysqldump_command(self):
        backupBase.BackupRunner.is_zipped = True
        backupBase.BackupRunner.is_encrypted = True
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_SQLDUMP_CLS)
        bkup = RunnerClass(12345, user="user",
                           password="password", extra_opts="")
        self.assertEqual(SQLDUMP_BACKUP + PIPE + ZIP + PIPE + ENCRYPT,
                         bkup.command)
        self.assertEqual("12345.gz.enc", bkup.manifest)

    def test_restore_decrypted_xtrabackup_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = False
        RunnerClass = utils.import_class(RESTORE_XTRA_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="md5")
        self.assertEqual(UNZIP + PIPE + XTRA_RESTORE, restr.restore_cmd)
        self.assertEqual(PREPARE, restr.prepare_cmd)

    def test_restore_encrypted_xtrabackup_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = True
        restoreBase.RestoreRunner.decrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(RESTORE_XTRA_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="md5")
        self.assertEqual(DECRYPT + PIPE + UNZIP + PIPE + XTRA_RESTORE,
                         restr.restore_cmd)
        self.assertEqual(PREPARE, restr.prepare_cmd)

    def test_restore_xtrabackup_incremental_prepare_command(self):
        RunnerClass = utils.import_class(RESTORE_XTRA_INCR_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="m5d")
        # Final prepare command (same as normal xtrabackup)
        self.assertEqual(PREPARE, restr.prepare_cmd)
        # Incremental backup prepare command
        expected = XTRA_INCR_PREPARE % {'incr': '--incremental-dir=/foo/bar/'}
        observed = restr._incremental_prepare_cmd('/foo/bar/')
        self.assertEqual(expected, observed)
        # Full backup prepare command
        expected = XTRA_INCR_PREPARE % {'incr': ''}
        observed = restr._incremental_prepare_cmd(None)
        self.assertEqual(expected, observed)

    def test_restore_decrypted_xtrabackup_incremental_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = False
        RunnerClass = utils.import_class(RESTORE_XTRA_INCR_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="m5d")
        # Full restore command
        expected = UNZIP + PIPE + XTRA_RESTORE
        self.assertEqual(expected, restr.restore_cmd)
        # Incremental backup restore command
        opts = {'restore_location': '/foo/bar/'}
        expected = UNZIP + PIPE + (XTRA_RESTORE_RAW % opts)
        observed = restr._incremental_restore_cmd('/foo/bar/')
        self.assertEqual(expected, observed)

    def test_restore_encrypted_xtrabackup_incremental_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = True
        restoreBase.RestoreRunner.decrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(RESTORE_XTRA_INCR_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="md5")
        # Full restore command
        expected = DECRYPT + PIPE + UNZIP + PIPE + XTRA_RESTORE
        self.assertEqual(expected, restr.restore_cmd)
        # Incremental backup restore command
        opts = {'restore_location': '/foo/bar/'}
        expected = DECRYPT + PIPE + UNZIP + PIPE + (XTRA_RESTORE_RAW % opts)
        observed = restr._incremental_restore_cmd('/foo/bar/')
        self.assertEqual(expected, observed)

    def test_restore_decrypted_mysqldump_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = False
        RunnerClass = utils.import_class(RESTORE_SQLDUMP_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="md5")
        self.assertEqual(UNZIP + PIPE + SQLDUMP_RESTORE, restr.restore_cmd)

    def test_restore_encrypted_mysqldump_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = True
        restoreBase.RestoreRunner.decrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(RESTORE_SQLDUMP_CLS)
        restr = RunnerClass(None, restore_location="/var/lib/mysql/data",
                            location="filename", checksum="md5")
        self.assertEqual(DECRYPT + PIPE + UNZIP + PIPE + SQLDUMP_RESTORE,
                         restr.restore_cmd)

    def test_backup_encrypted_cbbackup_command(self):
        backupBase.BackupRunner.is_encrypted = True
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_CBBACKUP_CLS)
        utils.execute_with_timeout = mock.Mock(return_value=None)
        bkp = RunnerClass(12345)
        self.assertIsNotNone(bkp)
        self.assertEqual(
            CBBACKUP_CMD + PIPE + ZIP + PIPE + ENCRYPT, bkp.command)
        self.assertIn("gz.enc", bkp.manifest)

    def test_backup_not_encrypted_cbbackup_command(self):
        backupBase.BackupRunner.is_encrypted = False
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_CBBACKUP_CLS)
        utils.execute_with_timeout = mock.Mock(return_value=None)
        bkp = RunnerClass(12345)
        self.assertIsNotNone(bkp)
        self.assertEqual(CBBACKUP_CMD + PIPE + ZIP, bkp.command)
        self.assertIn("gz", bkp.manifest)

    def test_restore_decrypted_cbbackup_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = False
        RunnerClass = utils.import_class(RESTORE_CBBACKUP_CLS)
        restr = RunnerClass(None, restore_location="/tmp",
                            location="filename", checksum="md5")
        self.assertEqual(UNZIP + PIPE + CBBACKUP_RESTORE, restr.restore_cmd)

    def test_restore_encrypted_cbbackup_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = True
        restoreBase.RestoreRunner.decrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(RESTORE_CBBACKUP_CLS)
        restr = RunnerClass(None, restore_location="/tmp",
                            location="filename", checksum="md5")
        self.assertEqual(DECRYPT + PIPE + UNZIP + PIPE + CBBACKUP_RESTORE,
                         restr.restore_cmd)

    @patch.multiple('trove.guestagent.common.operating_system',
                    chmod=DEFAULT, remove=DEFAULT)
    def test_reset_root_password_on_mysql_restore(self, chmod, remove):
        with patch.object(MySQLRestoreMixin,
                          '_start_mysqld_safe_with_init_file',
                          return_value=True):
            inst = MySQLRestoreMixin()
            inst.reset_root_password()

            chmod.assert_called_once_with(ANY, FileMode.ADD_READ_ALL,
                                          as_root=True)

            # Make sure the temporary error log got deleted as root
            # (see bug/1423759).
            remove.assert_called_once_with(ANY, force=True, as_root=True)

    def test_backup_encrypted_mongodump_command(self):
        backupBase.BackupRunner.is_encrypted = True
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_MONGODUMP_CLS)
        utils.execute_with_timeout = mock.Mock(return_value=None)
        bkp = RunnerClass(12345)
        self.assertIsNotNone(bkp)
        self.assertEqual(
            MONGODUMP_CMD + PIPE + ZIP + PIPE + ENCRYPT, bkp.command)
        self.assertIn("gz.enc", bkp.manifest)

    def test_backup_not_encrypted_mongodump_command(self):
        backupBase.BackupRunner.is_encrypted = False
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(BACKUP_MONGODUMP_CLS)
        utils.execute_with_timeout = mock.Mock(return_value=None)
        bkp = RunnerClass(12345)
        self.assertIsNotNone(bkp)
        self.assertEqual(MONGODUMP_CMD + PIPE + ZIP, bkp.command)
        self.assertIn("gz", bkp.manifest)

    def test_restore_decrypted_mongodump_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = False
        RunnerClass = utils.import_class(RESTORE_MONGODUMP_CLS)
        restr = RunnerClass(None, restore_location="/tmp",
                            location="filename", checksum="md5")
        self.assertEqual(restr.restore_cmd, UNZIP + PIPE + MONGODUMP_RESTORE)

    def test_restore_encrypted_mongodump_command(self):
        restoreBase.RestoreRunner.is_zipped = True
        restoreBase.RestoreRunner.is_encrypted = True
        restoreBase.RestoreRunner.decrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(RESTORE_MONGODUMP_CLS)
        restr = RunnerClass(None, restore_location="/tmp",
                            location="filename", checksum="md5")
        self.assertEqual(restr.restore_cmd,
                         DECRYPT + PIPE + UNZIP + PIPE + MONGODUMP_RESTORE)


class CassandraBackupTest(trove_testtools.TestCase):

    _BASE_BACKUP_CMD = ('tar --transform="s#snapshots/%s/##" -cpPf - -C "%s" '
                        '"%s"')
    _BASE_RESTORE_CMD = 'sudo tar -xpPf - -C "%(restore_location)s"'
    _DATA_DIR = 'data_dir'
    _SNAPSHOT_NAME = 'snapshot_name'
    _SNAPSHOT_FILES = {'foo.db', 'bar.db'}
    _MOCK_IP = "1.1.1.1"
    _RESTORE_LOCATION = {'restore_location': '/var/lib/cassandra'}

    def setUp(self):
        super(CassandraBackupTest, self).setUp()
        self.orig_1 = cass_service.CassandraAppStatus
        self.orig_2 = operating_system.read_yaml_file
        self.orig_3 = netutils.get_my_ipv4
        cass_service.CassandraAppStatus = mock.MagicMock()
        operating_system.read_yaml_file = mock.MagicMock(return_value={
            'data_file_directories': [self._DATA_DIR]
        })

    def tearDown(self):
        super(CassandraBackupTest, self).tearDown()
        netutils.get_my_ipv4 = self.orig_3
        operating_system.read_yaml_file = self.orig_2
        cass_service.CassandraAppStatus = self.orig_1

    def test_backup_encrypted_zipped_nodetoolsnapshot_command(self):
        bkp = self._build_backup_runner(True, True)
        bkp._run_pre_backup()
        self.assertIsNotNone(bkp)
        self.assertEqual(self._BASE_BACKUP_CMD % (
            self._SNAPSHOT_NAME,
            self._DATA_DIR,
            '" "'.join(self._SNAPSHOT_FILES)
        ) + PIPE + ZIP + PIPE + ENCRYPT, bkp.command)
        self.assertIn(".gz.enc", bkp.manifest)

    def test_backup_not_encrypted_not_zipped_nodetoolsnapshot_command(self):
        bkp = self._build_backup_runner(False, False)
        bkp._run_pre_backup()
        self.assertIsNotNone(bkp)
        self.assertEqual(self._BASE_BACKUP_CMD % (
            self._SNAPSHOT_NAME,
            self._DATA_DIR,
            '" "'.join(self._SNAPSHOT_FILES)
        ), bkp.command)
        self.assertNotIn(".gz.enc", bkp.manifest)

    def test_backup_not_encrypted_but_zipped_nodetoolsnapshot_command(self):
        bkp = self._build_backup_runner(False, True)
        bkp._run_pre_backup()
        self.assertIsNotNone(bkp)
        self.assertEqual(self._BASE_BACKUP_CMD % (
            self._SNAPSHOT_NAME,
            self._DATA_DIR,
            '" "'.join(self._SNAPSHOT_FILES)
        ) + PIPE + ZIP, bkp.command)
        self.assertIn(".gz", bkp.manifest)
        self.assertNotIn(".enc", bkp.manifest)

    def test_backup_encrypted_but_not_zipped_nodetoolsnapshot_command(self):
        bkp = self._build_backup_runner(True, False)
        bkp._run_pre_backup()
        self.assertIsNotNone(bkp)
        self.assertEqual(self._BASE_BACKUP_CMD % (
            self._SNAPSHOT_NAME,
            self._DATA_DIR,
            '" "'.join(self._SNAPSHOT_FILES)
        ) + PIPE + ENCRYPT, bkp.command)
        self.assertIn(".enc", bkp.manifest)
        self.assertNotIn(".gz", bkp.manifest)

    def test_restore_encrypted_but_not_zipped_nodetoolsnapshot_command(self):
        restoreBase.RestoreRunner.is_zipped = False
        restoreBase.RestoreRunner.is_encrypted = True
        restoreBase.RestoreRunner.decrypt_key = CRYPTO_KEY
        RunnerClass = utils.import_class(RESTORE_NODETOOLSNAPSHOT_CLS)
        rstr = RunnerClass(None, restore_location=self._RESTORE_LOCATION,
                           location="filename", checksum="md5")
        self.assertIsNotNone(rstr)
        self.assertEqual(self._BASE_RESTORE_CMD % self._RESTORE_LOCATION,
                         rstr.base_restore_cmd % self._RESTORE_LOCATION)

    def _build_backup_runner(self, is_encrypted, is_zipped):
        backupBase.BackupRunner.is_zipped = is_zipped
        backupBase.BackupRunner.is_encrypted = is_encrypted
        backupBase.BackupRunner.encrypt_key = CRYPTO_KEY
        netutils.get_my_ipv4 = mock.Mock(return_value=self._MOCK_IP)
        RunnerClass = utils.import_class(BACKUP_NODETOOLSNAPSHOT_CLS)
        runner = RunnerClass(self._SNAPSHOT_NAME)
        runner._remove_snapshot = mock.MagicMock()
        runner._snapshot_all_keyspaces = mock.MagicMock()
        runner._find_in_subdirectories = mock.MagicMock(
            return_value=self._SNAPSHOT_FILES
        )

        return runner


class CouchbaseBackupTests(trove_testtools.TestCase):

    def setUp(self):
        super(CouchbaseBackupTests, self).setUp()
        self.exec_timeout_patch = patch.object(utils, 'execute_with_timeout')
        self.exec_timeout_patch.start()
        self.backup_runner = utils.import_class(BACKUP_CBBACKUP_CLS)
        self.backup_runner_patch = patch.multiple(
            self.backup_runner, _run=DEFAULT,
            _run_pre_backup=DEFAULT, _run_post_backup=DEFAULT)

    def tearDown(self):
        super(CouchbaseBackupTests, self).tearDown()
        self.backup_runner_patch.stop()
        self.exec_timeout_patch.stop()

    def test_backup_success(self):
        backup_runner_mocks = self.backup_runner_patch.start()
        with self.backup_runner(12345):
            pass

        backup_runner_mocks['_run_pre_backup'].assert_called_once_with()
        backup_runner_mocks['_run'].assert_called_once_with()
        backup_runner_mocks['_run_post_backup'].assert_called_once_with()

    def test_backup_failed_due_to_run_backup(self):
        backup_runner_mocks = self.backup_runner_patch.start()
        backup_runner_mocks['_run'].configure_mock(
            side_effect=exception.TroveError('test')
        )
        with ExpectedException(exception.TroveError, 'test'):
            with self.backup_runner(12345):
                pass

        backup_runner_mocks['_run_pre_backup'].assert_called_once_with()
        backup_runner_mocks['_run'].assert_called_once_with()
        self.assertEqual(0, backup_runner_mocks['_run_post_backup'].call_count)


class CouchbaseRestoreTests(trove_testtools.TestCase):

    def setUp(self):
        super(CouchbaseRestoreTests, self).setUp()

        self.restore_runner = utils.import_class(
            RESTORE_CBBACKUP_CLS)(
                'swift', location='http://some.where',
                checksum='True_checksum',
                restore_location='/tmp/somewhere')

    def tearDown(self):
        super(CouchbaseRestoreTests, self).tearDown()

    def test_restore_success(self):
        expected_content_length = 123
        self.restore_runner._run_restore = mock.Mock(
            return_value=expected_content_length)
        self.restore_runner.pre_restore = mock.Mock()
        self.restore_runner.post_restore = mock.Mock()
        actual_content_length = self.restore_runner.restore()
        self.assertEqual(
            expected_content_length, actual_content_length)

    def test_restore_failed_due_to_pre_restore(self):
        self.restore_runner.post_restore = mock.Mock()
        self.restore_runner.pre_restore = mock.Mock(
            side_effect=exception.ProcessExecutionError('Error'))
        self.restore_runner._run_restore = mock.Mock()
        self.assertRaises(exception.ProcessExecutionError,
                          self.restore_runner.restore)

    def test_restore_failed_due_to_run_restore(self):
        self.restore_runner.pre_restore = mock.Mock()
        self.restore_runner._run_restore = mock.Mock(
            side_effect=exception.ProcessExecutionError('Error'))
        self.restore_runner.post_restore = mock.Mock()
        self.assertRaises(exception.ProcessExecutionError,
                          self.restore_runner.restore)


class MongodbBackupTests(trove_testtools.TestCase):

    def setUp(self):
        super(MongodbBackupTests, self).setUp()
        self.exec_timeout_patch = patch.object(utils, 'execute_with_timeout')
        self.exec_timeout_patch.start()
        self.backup_runner = utils.import_class(
            BACKUP_MONGODUMP_CLS)
        self.backup_runner_patch = patch.multiple(
            self.backup_runner, _run=DEFAULT,
            _run_pre_backup=DEFAULT, _run_post_backup=DEFAULT)

    def tearDown(self):
        super(MongodbBackupTests, self).tearDown()
        self.backup_runner_patch.stop()
        self.exec_timeout_patch.stop()

    def test_backup_success(self):
        backup_runner_mocks = self.backup_runner_patch.start()
        with self.backup_runner(12345):
            pass

        backup_runner_mocks['_run_pre_backup'].assert_called_once_with()
        backup_runner_mocks['_run'].assert_called_once_with()
        backup_runner_mocks['_run_post_backup'].assert_called_once_with()

    def test_backup_failed_due_to_run_backup(self):
        backup_runner_mocks = self.backup_runner_patch.start()
        backup_runner_mocks['_run'].configure_mock(
            side_effect=exception.TroveError('test')
        )
        with ExpectedException(exception.TroveError, 'test'):
            with self.backup_runner(12345):
                pass
        backup_runner_mocks['_run_pre_backup'].assert_called_once_with()
        backup_runner_mocks['_run'].assert_called_once_with()
        self.assertEqual(0, backup_runner_mocks['_run_post_backup'].call_count)


class MongodbRestoreTests(trove_testtools.TestCase):

    def setUp(self):
        super(MongodbRestoreTests, self).setUp()

        self.restore_runner = utils.import_class(
            RESTORE_MONGODUMP_CLS)('swift', location='http://some.where',
                                   checksum='True_checksum',
                                   restore_location='/var/lib/somewhere')

    def tearDown(self):
        super(MongodbRestoreTests, self).tearDown()

    def test_restore_success(self):
        expected_content_length = 123
        self.restore_runner._run_restore = mock.Mock(
            return_value=expected_content_length)
        self.restore_runner.pre_restore = mock.Mock()
        self.restore_runner.post_restore = mock.Mock()
        actual_content_length = self.restore_runner.restore()
        self.assertEqual(
            expected_content_length, actual_content_length)

    def test_restore_failed_due_to_pre_restore(self):
        self.restore_runner.post_restore = mock.Mock()
        self.restore_runner.pre_restore = mock.Mock(
            side_effect=exception.ProcessExecutionError('Error'))
        self.restore_runner._run_restore = mock.Mock()
        self.assertRaises(exception.ProcessExecutionError,
                          self.restore_runner.restore)

    def test_restore_failed_due_to_run_restore(self):
        self.restore_runner.pre_restore = mock.Mock()
        self.restore_runner._run_restore = mock.Mock(
            side_effect=exception.ProcessExecutionError('Error'))
        self.restore_runner.post_restore = mock.Mock()
        self.assertRaises(exception.ProcessExecutionError,
                          self.restore_runner.restore)

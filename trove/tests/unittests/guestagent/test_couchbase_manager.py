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

import os
import stat
import tempfile

import mock
from mock import DEFAULT
from mock import MagicMock
from mock import Mock
from mock import patch
from mock import PropertyMock
from oslo_utils import netutils

from trove.common import utils
from trove.guestagent import backup
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.couchbase import (
    manager as couch_manager)
from trove.guestagent.datastore.experimental.couchbase import (
    service as couch_service)
from trove.guestagent import volume
from trove.tests.unittests import trove_testtools


class GuestAgentCouchbaseManagerTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentCouchbaseManagerTest, self).setUp()
        self.context = trove_testtools.TroveTestContext(self)
        self.manager = couch_manager.Manager()
        self.packages = 'couchbase-server'
        app_patcher = patch.multiple(
            couch_service.CouchbaseApp,
            stop_db=DEFAULT, start_db=DEFAULT, restart=DEFAULT)
        self.addCleanup(app_patcher.stop)
        app_patcher.start()

        netutils_patcher = patch.object(netutils, 'get_my_ipv4')
        self.addCleanup(netutils_patcher.stop)
        netutils_patcher.start()

    def tearDown(self):
        super(GuestAgentCouchbaseManagerTest, self).tearDown()

    def test_update_status(self):
        mock_status = MagicMock()
        self.manager.app.status = mock_status
        self.manager.update_status(self.context)
        mock_status.update.assert_any_call()

    def test_prepare_device_path_true(self):
        self._prepare_dynamic()

    def test_prepare_from_backup(self):
        self._prepare_dynamic(backup_id='backup_id_123abc')

    @patch.multiple(couch_service.CouchbaseApp,
                    install_if_needed=DEFAULT,
                    start_db_with_conf_changes=DEFAULT,
                    init_storage_structure=DEFAULT,
                    apply_initial_guestagent_configuration=DEFAULT,
                    apply_post_restore_updates=DEFAULT,
                    secure=DEFAULT)
    @patch.multiple(volume.VolumeDevice,
                    format=DEFAULT,
                    mount=DEFAULT,
                    mount_points=Mock(return_value=[]))
    @patch.object(backup, 'restore')
    def _prepare_dynamic(self, device_path='/dev/vdb', backup_id=None,
                         *mocks, **kwmocks):

        # covering all outcomes is starting to cause trouble here
        backup_info = {'id': backup_id,
                       'location': 'fake-location',
                       'type': 'CbBackup',
                       'checksum': 'fake-checksum'} if backup_id else None

        mock_status = MagicMock()
        mock_status.begin_install = MagicMock(return_value=None)
        self.manager.app.status = mock_status

        instance_ram = 2048
        mount_point = '/var/lib/couchbase'

        with patch.object(couch_service.CouchbaseApp, 'available_ram_mb',
                          new_callable=PropertyMock) as available_ram_mock:
            available_ram_mock.return_value = instance_ram

            self.manager.prepare(self.context, self.packages, None,
                                 instance_ram, None, device_path=device_path,
                                 mount_point=mount_point,
                                 backup_info=backup_info,
                                 overrides=None,
                                 cluster_config=None)

            available_ram_mock.assert_called_once_with(instance_ram)

        # verification/assertion
        mock_status.begin_install.assert_any_call()

        storage_mock = kwmocks['init_storage_structure']
        init_mock = kwmocks['apply_initial_guestagent_configuration']
        init_mock.assert_called_once_with(None)
        storage_mock.assert_called_once_with(mount_point)
        kwmocks['install_if_needed'].assert_any_call(self.packages)

        if backup_info:
            backup.restore.assert_called_once_with(self.context,
                                                   backup_info,
                                                   mount_point)
            kwmocks['apply_post_restore_updates'].assert_called_once_with(
                backup_info)
        kwmocks['secure'].assert_called_once_with()

    def test_restart(self):
        mock_status = MagicMock()
        self.manager.app.status = mock_status
        couch_service.CouchbaseApp.restart = MagicMock(return_value=None)
        # invocation
        self.manager.restart(self.context)
        # verification/assertion
        couch_service.CouchbaseApp.restart.assert_any_call()

    def test_stop_db(self):
        mock_status = MagicMock()
        self.manager.app.status = mock_status
        couch_service.CouchbaseApp.stop_db = MagicMock(return_value=None)
        # invocation
        self.manager.stop_db(self.context)
        # verification/assertion
        couch_service.CouchbaseApp.stop_db.assert_any_call(
            do_not_start_on_reboot=False)

    def __fake_mkstemp(self):
        self.tempfd, self.tempname = self.original_mkstemp()
        return self.tempfd, self.tempname

    def __fake_mkstemp_raise(self):
        raise OSError(11, 'Resource temporarily unavailable')

    def __cleanup_tempfile(self):
        if self.tempname:
            os.unlink(self.tempname)

    @mock.patch.object(utils, 'execute_with_timeout',
                       Mock(return_value=('0', '')))
    def test_write_password_to_file1(self):
        self.original_mkstemp = tempfile.mkstemp
        self.tempname = None

        with mock.patch.object(tempfile,
                               'mkstemp',
                               self.__fake_mkstemp):
            self.addCleanup(self.__cleanup_tempfile)

            app = couch_service.CouchbaseApp()
            app.write_password_to_file('mypassword')

            filepermissions = os.stat(self.tempname).st_mode
            self.assertEqual(stat.S_IRUSR, filepermissions & 0o777)

    @mock.patch.object(utils, 'execute_with_timeout',
                       Mock(return_value=('0', '')))
    @mock.patch(
        'trove.guestagent.datastore.experimental.couchbase.service.LOG')
    def test_write_password_to_file2(self, mock_logging):
        self.original_mkstemp = tempfile.mkstemp
        self.tempname = None

        with mock.patch.object(tempfile,
                               'mkstemp',
                               self.__fake_mkstemp_raise):

            app = couch_service.CouchbaseApp()

            self.assertRaises(RuntimeError,
                              app.write_password_to_file,
                              'mypassword')

    @mock.patch.object(operating_system, 'create_directory')
    def test_init_storage_structure(self, mkdir_mock):
        mount_point = Mock()
        app = couch_service.CouchbaseApp(Mock())
        app.init_storage_structure(mount_point)
        mkdir_mock.assert_called_once_with(
            mount_point, user=app.couchbase_owner, group=app.couchbase_owner,
            as_root=True)

    def test_build_command_options(self):
        app = couch_service.CouchbaseApp(Mock())
        opts = app.build_admin()._build_command_options({'bucket': 'bucket1',
                                                         'bucket-replica': 0,
                                                         'wait': None})
        self.assertEqual(
            set(['--bucket=bucket1', '--bucket-replica=0', '--wait']),
            set(opts))

    def test_parse_bucket_list(self):
        app = couch_service.CouchbaseApp(Mock())
        bucket_list = app.build_admin()._parse_bucket_list(
            "bucket1\n saslPassword: password1\n ramQuota: 268435456\n"
            "bucket2\n saslPassword: password2\n ramQuota: 134217728")
        self.assertEqual({'bucket1': {'saslPassword': 'password1',
                                      'ramQuota': '268435456'},
                          'bucket2': {'saslPassword': 'password2',
                                      'ramQuota': '134217728'}}, bucket_list)

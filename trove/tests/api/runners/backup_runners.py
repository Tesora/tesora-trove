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

from proboscis import asserts

from troveclient.compat import exceptions

from trove.common.utils import generate_uuid
from trove.tests.api.runners.base_test_runners import BaseTestRunner


BACKUP_NAME = 'backup_test'
BACKUP_DESC = 'test description'

backup_info = None
backup_count_prior_to_create = 0
backup_count_for_instance_prior_to_create = 0


class BackupRunner(BaseTestRunner):

    def run_backup_create_instance_invalid(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        """Test create backup with invalid instance id."""
        invalid_inst_id = 'invalid-inst-id'
        self.assert_raises(
            expected_exception, expected_http_code,
            self.instance_info.dbaas.backups.create,
            BACKUP_NAME, invalid_inst_id, BACKUP_DESC)

    def run_backup_create_instance_not_found(
            self, expected_exception=exceptions.NotFound,
            expected_http_code=404):
        """Test create backup with unknown instance."""
        self.assert_raises(
            expected_exception, expected_http_code,
            self.instance_info.dbaas.backups.create,
            BACKUP_NAME, generate_uuid(), BACKUP_DESC)

    def run_backup_create(self):
        """Test create backup for a given instance."""
        # Necessary to test that the count increases.
        global backup_count_prior_to_create
        backup_count_prior_to_create = len(
            self.instance_info.dbaas.backups.list())
        global backup_count_for_instance_prior_to_create
        backup_count_for_instance_prior_to_create = len(
            self.instance_info.dbaas.instances.backups(self.instance_info.id))

        result = self.instance_info.dbaas.backups.create(
            BACKUP_NAME, self.instance_info.id, BACKUP_DESC)
        global backup_info
        backup_info = result
        self.assert_equal(BACKUP_NAME, result.name,
                          'Unexpected backup name')
        self.assert_equal(BACKUP_DESC, result.description,
                          'Unexpected backup description')
        self.assert_equal(self.instance_info.id, result.instance_id,
                          'Unexpected instance ID for backup')
        self.assert_equal('NEW', result.status,
                          'Unexpected status for backup')
        instance = self.instance_info.dbaas.instances.get(
            self.instance_info.id)

        datastore_version = self.instance_info.dbaas.datastore_versions.get(
            self.instance_info.dbaas_datastore,
            self.instance_info.dbaas_datastore_version)

        self.assert_equal('BACKUP', instance.status,
                          'Unexpected instance status')
        self.assert_equal(self.instance_info.dbaas_datastore,
                          result.datastore['type'],
                          'Unexpected datastore')
        self.assert_equal(self.instance_info.dbaas_datastore_version,
                          result.datastore['version'],
                          'Unexpected datastore version')
        self.assert_equal(datastore_version.id, result.datastore['version_id'],
                          'Unexpected datastore version id')

    def run_instance_action_right_after_backup_create(self):
        """Test any instance action while backup is running."""
        asserts.assert_unprocessable(
            self.instance_info.dbaas.instances.resize_instance,
            self.instance_info.id, 1)

    def test_backup_create_another_backup_running(self):
        """Test create backup when another backup is running."""
        asserts.assert_unprocessable(
            self.instance_info.dbaas.backups.create,
            'backup_test2', self.instance_info.id, 'test description2')

    def test_backup_delete_still_running(self):
        """Test delete backup when it is running."""
        result = self.instance_info.dbaas.backups.list()
        backup = result[0]
        asserts.assert_unprocessable(
            self.instance_info.dbaas.backups.delete, backup.id)

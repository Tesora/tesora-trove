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

from proboscis import test

from trove import tests
from trove.tests.api.groups.abstract_test_group import AbstractTestGroup
from trove.tests.api.instances import WaitForGuestInstallationToFinish


GROUP = "dbaas.api.backup_group"


@test(depends_on_classes=[WaitForGuestInstallationToFinish],
      groups=[GROUP, tests.INSTANCES])
class BackupGroup(AbstractTestGroup):

    def __init__(self):
        super(BackupGroup, self).__init__(
            'backup_runners', 'BackupRunner')

    @test
    def backup_create_instance_invalid(self):
        self.test_runner.run_backup_create_instance_invalid()

    @test(runs_after=['backup_create_instance_invalid'])
    def backup_create_instance_not_found(self):
        self.test_runner.run_backup_create_instance_not_found()

    @test(runs_after=['backup_create_instance_not_found'])
    def backup_create(self):
        self.test_runner.run_backup_create()

    @test(depends_on=[backup_create],
          runs_after=['backup_create'])
    def instance_action_right_after_backup_create(self):
        self.test_runner.run_instance_action_right_after_backup_create()

    @test(depends_on=[backup_create],
          runs_after=['instance_action_right_after_backup_create'])
    def backup_create_another_backup_running(self):
        self.test_runner.run_backup_create_another_backup_running()

    @test(depends_on=[backup_create],
          runs_after=['backup_create_another_backup_running'])
    def backup_delete_still_running(self):
        self.test_runner.run_backup_delete_still_running()

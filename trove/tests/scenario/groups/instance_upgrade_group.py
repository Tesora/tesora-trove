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

from trove.tests.scenario.groups import instance_create_group
from trove.tests.scenario.groups.test_group import TestGroup


GROUP = "scenario.instance_upgrade_group"


@test(depends_on_groups=[instance_create_group.GROUP], groups=[GROUP])
class InstanceUpgradeGroup(TestGroup):

    def __init__(self):
        super(InstanceUpgradeGroup, self).__init__(
            'instance_upgrade_runners', 'InstanceUpgradeRunner')
        self.database_actions_runner = self.get_runner(
            'database_actions_runners', 'DatabaseActionsRunner')
        self.user_actions_runner = self.get_runner(
            'user_actions_runners', 'UserActionsRunner')

    @test
    def create_user_databases(self):
        """Create user databases on an existing instance."""
        # These databases may be referenced by the users (below) so we need to
        # create them first.
        self.database_actions_runner.run_databases_create()

    @test(runs_after=[create_user_databases])
    def create_users(self):
        """Create users on an existing instance."""
        self.user_actions_runner.run_users_create()

    @test(runs_after=[create_users])
    def instance_upgrade(self):
        """Upgrade an existing instance."""
        self.test_runner.run_instance_upgrade()

    @test(depends_on=[instance_upgrade])
    def show_user(self):
        """Show created users."""
        self.user_actions_runner.run_user_show()

    @test(depends_on=[create_users],
          runs_after=[show_user])
    def list_users(self):
        """List the created users."""
        self.user_actions_runner.run_users_list()

    @test(depends_on=[create_users],
          runs_after=[list_users])
    def delete_user(self):
        """Delete the created users."""
        self.user_actions_runner.run_user_delete()

    @test(depends_on=[create_user_databases], runs_after=[delete_user])
    def delete_user_databases(self):
        """Delete the user databases."""
        self.database_actions_runner.run_database_delete()

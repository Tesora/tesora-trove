# Copyright 2016 Tesora, Inc.
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

import copy
from mock import Mock, patch

from trove.module import models
from trove.taskmanager import api as task_api
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util


class CreateModuleTest(trove_testtools.TestCase):

    @patch.object(task_api.API, 'get_client', Mock(return_value=Mock()))
    def setUp(self):
        util.init_db()
        self.context = Mock()
        self.name = "name"
        self.module_type = 'ping'
        self.contents = 'my_contents\n'

        super(CreateModuleTest, self).setUp()

    @patch.object(task_api.API, 'get_client', Mock(return_value=Mock()))
    def tearDown(self):
        super(CreateModuleTest, self).tearDown()

    def test_can_create_update_module(self):
        module = models.Module.create(
            self.context,
            self.name, self.module_type, self.contents,
            'my desc', 'my_tenant', None, None, False, True, False,
            False, 5, True)
        self.assertIsNotNone(module)
        new_module = copy.copy(module)
        models.Module.update(self.context, new_module, module, False)
        module.delete()

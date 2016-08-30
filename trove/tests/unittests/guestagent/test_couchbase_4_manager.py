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

from mock import DEFAULT
from mock import Mock
from mock import patch
from oslo_utils import netutils

from trove.common.exception import TroveError
from trove.guestagent.datastore.experimental.couchbase_4 import (
    manager as couch_manager)
from trove.guestagent.datastore.experimental.couchbase_4 import (
    service as couch_service)
from trove.tests.unittests import trove_testtools


class GuestAgentCouchbase4ManagerTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentCouchbase4ManagerTest, self).setUp()
        self.context = trove_testtools.TroveTestContext(self)
        self.manager = couch_manager.Manager()
        self.packages = 'couchbase-server'
        app_patcher = patch.multiple(
            couch_service.Couchbase4App,
            stop_db=DEFAULT, start_db=DEFAULT, restart=DEFAULT)
        self.addCleanup(app_patcher.stop)
        app_patcher.start()

        netutils_patcher = patch.object(netutils, 'get_my_ipv4')
        self.addCleanup(netutils_patcher.stop)
        netutils_patcher.start()

    def tearDown(self):
        super(GuestAgentCouchbase4ManagerTest, self).tearDown()

    def test_compute_mem_allocations_mb(self):
        def assert_mem_values(expected, observed):
            self.assertEqual(expected, [str(item) for item in observed])

        admin = couch_service.Couchbase4Admin(Mock())

        assert_mem_values(['1536', '512'],
                          admin._compute_mem_allocations_mb(2048,
                                                            ['data', 'index']))
        assert_mem_values(['256', '256'],
                          admin._compute_mem_allocations_mb(512,
                                                            ['data', 'index']))

        assert_mem_values(['2048', '0'],
                          admin._compute_mem_allocations_mb(2048, ['data']))
        assert_mem_values(['512', '0'],
                          admin._compute_mem_allocations_mb(512, ['data']))
        assert_mem_values(['256', '0'],
                          admin._compute_mem_allocations_mb(256, ['data']))

        self.assertRaisesRegexp(
            TroveError,
            "Not enough memory for Couchbase services. "
            "Additional 128MB is required.",
            admin._compute_mem_allocations_mb, 384, ['data', 'index']
        )
        self.assertRaisesRegexp(
            TroveError,
            "Not enough memory for Couchbase services. "
            "Additional 384MB is required.",
            admin._compute_mem_allocations_mb, 128, ['data', 'index']
        )
        self.assertRaisesRegexp(
            TroveError,
            "Not enough memory for Couchbase services. "
            "Additional 128MB is required.",
            admin._compute_mem_allocations_mb, 128, ['data']
        )

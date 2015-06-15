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

from trove.guestagent.common import guestagent_utils
from trove.tests.unittests import trove_testtools


class TestGuestagentUtils(trove_testtools.TestCase):

    def test_update_dict(self):
        data = [{
            'dict': {}, 'update': {}, 'expected': {}, 'remove': False,
        }, {
            'dict': {}, 'update': {}, 'expected': {}, 'remove': True,
        }, {
            'dict': None, 'update': {}, 'expected': {}, 'remove': False,
        }, {
            'dict': None, 'update': {}, 'expected': {}, 'remove': True,
        }, {
            'dict': {}, 'update': None, 'expected': {}, 'remove': False,
        }, {
            'dict': {}, 'update': None, 'expected': {}, 'remove': True,
        }, {
            'dict': {}, 'update': None, 'expected': {}, 'remove': False,
        }, {
            'dict': {}, 'update': None, 'expected': {}, 'remove': True,
        }, {
            'dict': None, 'update': {'name': 'Tom'},
            'expected': {'name': 'Tom'}, 'remove': False,
        }, {
            'dict': None, 'update': {'name': None},
            'expected': {}, 'remove': True,
        }, {
            'dict': {}, 'update': {'name': 'Tom'},
            'expected': {'name': 'Tom'}, 'remove': False,
        }, {
            'dict': {}, 'update': {'name': None},
            'expected': {}, 'remove': True,
        }, {
            'dict': {'name': 'Tom'}, 'update': {},
            'expected': {'name': 'Tom'}, 'remove': False,
        }, {
            'dict': {'name': 'Tom'}, 'update': {'name': None},
            'expected': {}, 'remove': True,
        }, {
            'dict': {'key1': 'value1',
                     'dict1': {'key1': 'value1', 'key2': 'value2'}},
            'update': {'key1': 'value1+',
                       'key2': 'value2',
                       'dict1': {'key3': 'value3'}},
            'expected': {'key1': 'value1+',
                         'key2': 'value2',
                         'dict1': {'key1': 'value1', 'key2': 'value2',
                                   'key3': 'value3'}},
            'remove': False,
        }, {
            'dict': {'key1': 'value1',
                     'key2': 'value2',
                     'dict1': {'key1': 'value1', 'key2': 'value2'}},
            'update': {'key1': 'garbage',
                       'dict1': {'key1': 'not relevant', 'key2': None}},
            'expected': {'key2': 'value2',
                         'dict1': {}},
            'remove': True,
        }, {
            'dict': {'d1': {'d2': {'d3': {'k1': 'v1'}}}},
            'update': {'d1': {'d2': {'d3': {'k2': 'v2'}}}},
            'expected': {'d1': {'d2': {'d3': {'k1': 'v1', 'k2': 'v2'}}}},
            'remove': False,
        }, {
            'dict': {'d1': {'d2': {'d3': {'k1': 'v1'}}}},
            'update': {'d1': {'d2': {'d3': None}}},
            'expected': {'d1': {'d2': {}}},
            'remove': True,
        }, {
            'dict': {'timeout': 0, 'save': [[900, 1], [300, 10]]},
            'update': {'save': [[300, 20], [60, 10000]]},
            'expected': {'timeout': 0,
                         'save': [[300, 20], [60, 10000]]},
            'remove': False,
        }, {
            'dict': {'timeout': 0, 'save': [[900, 1], [300, 10]]},
            'update': {'save': None},
            'expected': {'timeout': 0},
            'remove': True,
        }, {
            'dict': {'rpc_address': '0.0.0.0',
                     'broadcast_rpc_address': '0.0.0.0',
                     'listen_address': '0.0.0.0',
                     'seed_provider': [{
                         'class_name':
                         'org.apache.cassandra.locator.SimpleSeedProvider',
                         'parameters': [{'seeds': '0.0.0.0'}]}]
                     },
            'update': {'rpc_address': '127.0.0.1',
                       'seed_provider': {'parameters': {
                           'seeds': '127.0.0.1'}}
                       },
            'expected': {'rpc_address': '127.0.0.1',
                         'broadcast_rpc_address': '0.0.0.0',
                         'listen_address': '0.0.0.0',
                         'seed_provider': [{
                             'class_name':
                             'org.apache.cassandra.locator.SimpleSeedProvider',
                             'parameters': [{'seeds': '127.0.0.1'}]}]
                         },
            'remove': False,
        }, {
            'dict': {'rpc_address': '0.0.0.0',
                     'broadcast_rpc_address': '0.0.0.0',
                     'listen_address': '0.0.0.0',
                     'seed_provider': [{
                         'class_name':
                         'org.apache.cassandra.locator.SimpleSeedProvider',
                         'parameters': [{'seeds': '0.0.0.0'}]}]
                     },
            'update': {'broadcast_rpc_address': None,
                       'seed_provider': {'class_name': None}
                       },
            'expected': {'rpc_address': '0.0.0.0',
                         'listen_address': '0.0.0.0',
                         'seed_provider': [{
                             'parameters': [{'seeds': '0.0.0.0'}]}]
                         },
            'remove': True,
        }, {
            'dict': {'rpc_address': '127.0.0.1',
                     'broadcast_rpc_address': '0.0.0.0',
                     'listen_address': '0.0.0.0',
                     'seed_provider': [{
                         'class_name':
                         'org.apache.cassandra.locator.SimpleSeedProvider',
                         'parameters': [{'seeds': '0.0.0.0'}]}]
                     },
            'update': {'seed_provider':
                       [{'class_name':
                         'org.apache.cassandra.locator.SimpleSeedProvider'}]
                       },
            'expected': {'rpc_address': '127.0.0.1',
                         'broadcast_rpc_address': '0.0.0.0',
                         'listen_address': '0.0.0.0',
                         'seed_provider': [{
                             'class_name':
                             'org.apache.cassandra.locator.SimpleSeedProvider'
                         }]},
            'remove': False,
        }, {
            'dict': {'rpc_address': '0.0.0.0',
                     'broadcast_rpc_address': '0.0.0.0',
                     'listen_address': '0.0.0.0',
                     'seed_provider': [{
                         'class_name':
                         'org.apache.cassandra.locator.SimpleSeedProvider',
                         'parameters': [{'seeds': '0.0.0.0'}]}]
                     },
            'update': {'listen_address': None,
                       'seed_provider': None
                       },
            'expected': {'rpc_address': '0.0.0.0',
                         'broadcast_rpc_address': '0.0.0.0',
                         },
            'remove': True,
        }]
        count = 0
        for record in data:
            count += 1
            target = record['dict']
            update = record['update']
            expected = record['expected']
            remove = record['remove']
            result = guestagent_utils.update_dict(update, target,
                                                  remove=remove)
            msg = 'Unexpected result for test %s' % str(count)
            self.assertEqual(expected, result, msg)

    def test_build_file_path(self):
        self.assertEqual(
            'base_dir/base_name',
            guestagent_utils.build_file_path('base_dir', 'base_name'))

        self.assertEqual(
            'base_dir/base_name.ext1',
            guestagent_utils.build_file_path('base_dir', 'base_name', 'ext1'))

        self.assertEqual(
            'base_dir/base_name.ext1.ext2',
            guestagent_utils.build_file_path(
                'base_dir', 'base_name', 'ext1', 'ext2'))

    def test_flatten_expand_dict(self):
        self._assert_flatten_expand_dict({}, {})
        self._assert_flatten_expand_dict({'ns1': 1}, {'ns1': 1})
        self._assert_flatten_expand_dict(
            {'ns1': {'ns2a': {'ns3a': True, 'ns3b': False}, 'ns2b': 10}},
            {'ns1.ns2a.ns3a': True, 'ns1.ns2a.ns3b': False, 'ns1.ns2b': 10})

    def _assert_flatten_expand_dict(self, nested_dict, flattened_dict):
        self.assertEqual(
            flattened_dict, guestagent_utils.flatten_dict(nested_dict))
        self.assertEqual(
            nested_dict, guestagent_utils.expand_dict(flattened_dict))

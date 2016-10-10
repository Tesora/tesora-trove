# Copyright 2016 Tesora Inc.
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

import mock

from trove.common import exception
from trove.extensions.couchbase import service
from trove.guestagent.db import models as guest_models
from trove.tests.unittests import trove_testtools


class CouchbaseUserControllerTest(trove_testtools.TestCase):

    def setUp(self):
        trove_testtools.TestCase.setUp(self)
        self.controller = service.CouchbaseUserController()

    def test_parse_user_from_response(self):
        with mock.patch.object(
                guest_models.CouchbaseUser, 'deserialize_user') as des:
            user_data = mock.NonCallableMock()
            self.controller.parse_user_from_response(user_data)
            des.assert_called_once_with(user_data)

    def test_parse_user_from_request(self):
        user_data = {'name': 'mock_name',
                     'password': 'mock_password',
                     'bucket_ramsize': 256,
                     'bucket_replica': 1,
                     'enable_index_replica': 0,
                     'bucket_eviction_policy': 'fullEviction',
                     'bucket_priority': 'high'}

        expected = guest_models.CouchbaseUser(
            user_data['name'],
            user_data['password'],
            bucket_ramsize_mb=user_data['bucket_ramsize'],
            bucket_replica_count=user_data['bucket_replica'],
            enable_index_replica=user_data['enable_index_replica'],
            bucket_eviction_policy=user_data['bucket_eviction_policy'],
            bucket_priority=user_data['bucket_priority'])

        observed = self.controller.parse_user_from_request(user_data)

        self.assertEqual(expected.serialize(), observed.serialize())

    def test_apply_user_updates(self):
        updates = {'password': 'mock_password',
                   'bucket_ramsize': 256,
                   'bucket_replica': 1,
                   'enable_index_replica': 0,
                   'bucket_eviction_policy': 'fullEviction',
                   'bucket_priority': 'high'}

        user = guest_models.CouchbaseUser('test_user')

        expected = guest_models.CouchbaseUser(
            'test_user',
            updates['password'],
            bucket_ramsize_mb=updates['bucket_ramsize'],
            bucket_replica_count=updates['bucket_replica'],
            enable_index_replica=updates['enable_index_replica'],
            bucket_eviction_policy=updates['bucket_eviction_policy'],
            bucket_priority=updates['bucket_priority'])

        self.assertIsNone(self.controller.apply_user_updates(user, updates))
        self.assertEqual(expected.serialize(), user.serialize())

        updates_with_name = dict(updates)
        updates_with_name.update({'name': 'mock_name'})
        self.assertRaises(
            exception.BadRequest,
            self.controller.apply_user_updates, user, updates_with_name)

        for key in updates:
            updates_with_missing = dict(updates)
            del updates_with_missing[key]
            self.assertRaises(
                exception.MissingKey,
                self.controller.apply_user_updates, user, updates_with_missing)

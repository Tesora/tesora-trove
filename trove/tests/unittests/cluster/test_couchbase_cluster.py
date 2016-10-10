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

import uuid

from mock import Mock
from mock import patch
from novaclient import exceptions as nova_exceptions
from trove.cluster.models import Cluster
from trove.cluster.models import ClusterTasks
from trove.cluster.models import DBCluster
from trove.common import cfg
from trove.common import exception
from trove.common import remote
from trove.common.strategies.cluster.experimental.couchbase import (
    api as couchbase_api)
from trove.common.strategies.cluster.experimental.couchbase import (
    taskmanager as couchbase_tm)
from trove.instance import models as inst_models
from trove.quota.quota import QUOTAS
from trove.taskmanager import api as task_api
from trove.tests.unittests import trove_testtools


CONF = cfg.CONF


class ClusterTest(trove_testtools.TestCase):
    def setUp(self):
        super(ClusterTest, self).setUp()

        self.cluster_id = str(uuid.uuid4())
        self.cluster_name = "Cluster" + self.cluster_id
        self.tenant_id = "23423432"
        self.dv_id = "1"
        self.db_info = DBCluster(ClusterTasks.NONE,
                                 id=self.cluster_id,
                                 name=self.cluster_name,
                                 tenant_id=self.tenant_id,
                                 datastore_version_id=self.dv_id,
                                 task_id=ClusterTasks.NONE._code)

        self.get_client_patch = patch.object(task_api.API, 'get_client')
        self.get_client_mock = self.get_client_patch.start()
        self.addCleanup(self.get_client_patch.stop)
        self.dbcreate_patch = patch.object(DBCluster, 'create',
                                           return_value=self.db_info)
        self.dbcreate_mock = self.dbcreate_patch.start()
        self.addCleanup(self.dbcreate_patch.stop)

        self.context = trove_testtools.TroveTestContext(self)
        self.datastore = Mock()
        self.dv = Mock()
        self.dv.manager = "couchbase_4"
        self.datastore_version = self.dv
        self.cluster = couchbase_api.CouchbaseCluster(
            self.context, self.db_info, self.datastore, self.datastore_version)
        self.cluster._server_group_loaded = True
        self.instances_w_volumes = [{'volume_size': 1,
                                     'flavor_id': '1234'}] * 3
        self.instances_no_volumes = [{'flavor_id': '1234'}] * 3

    def tearDown(self):
        super(ClusterTest, self).tearDown()

    def patch_conf(self, manager=None,
                   volume_support=True, device_path='/dev/vdb'):
        manager = manager or 'couchbase_4'
        self.patch_conf_property(
            'volume_support', volume_support, section=manager)
        self.patch_conf_property(
            'device_path', device_path, section=manager)

    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_create_invalid_flavor_specified(self,
                                             mock_client):
        (mock_client.return_value.flavors.get) = Mock(
            side_effect=nova_exceptions.NotFound(
                404, "Flavor id not found %s" % id))

        self.assertRaises(exception.FlavorNotFound,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_w_volumes,
                          {}, None)

    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_create_volume_no_specified(self, mock_client):
        self.patch_conf(volume_support=True)
        self.assertRaises(exception.ClusterVolumeSizeRequired,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_no_volumes,
                          {}, None)

    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_create_storage_specified_with_no_volume_support(self,
                                                             mock_client):
        self.patch_conf(volume_support=False)
        mock_client.return_value.flavors = Mock()
        self.assertRaises(exception.VolumeNotSupported,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_w_volumes,
                          {}, None)

    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_create_storage_not_specified_and_no_ephemeral_flavor(self,
                                                                  mock_client):
        class FakeFlavor:
            def __init__(self, flavor_id):
                self.flavor_id = flavor_id

            @property
            def id(self):
                return self.flavor.id

            @property
            def ephemeral(self):
                return 0
        self.patch_conf(volume_support=False)
        (mock_client.return_value.
         flavors.get.return_value) = FakeFlavor('1234')
        self.assertRaises(exception.LocalStorageNotSpecified,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_no_volumes,
                          {}, None)

    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_create(self, mock_client, mock_check_quotas,
                    mock_task_api, mock_ins_create):
        self.patch_conf(volume_support=True)
        mock_client.return_value.flavors = Mock(return_value=1234)
        self.cluster.create(Mock(),
                            self.cluster_name,
                            self.datastore,
                            self.datastore_version,
                            self.instances_w_volumes, {}, None)
        mock_task_api.return_value.create_cluster.assert_called_with(
            self.dbcreate_mock.return_value.id)
        self.assertEqual(3, mock_ins_create.call_count)

    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_create_with_ephemeral_flavor(self, mock_client, mock_check_quotas,
                                          mock_task_api, mock_ins_create):
        class FakeFlavor:
            def __init__(self, flavor_id):
                self.flavor_id = flavor_id

            @property
            def id(self):
                return self.flavor.id

            @property
            def ephemeral(self):
                return 1
        self.patch_conf(volume_support=False)
        (mock_client.return_value.
         flavors.get.return_value) = FakeFlavor('1234')
        self.cluster.create(Mock(),
                            self.cluster_name,
                            self.datastore,
                            self.datastore_version,
                            self.instances_no_volumes, {}, None)
        mock_task_api.return_value.create_cluster.assert_called_with(
            self.dbcreate_mock.return_value.id)
        self.assertEqual(3, mock_ins_create.call_count)

    @patch.object(DBCluster, 'update')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(remote, 'create_nova_client')
    @patch.multiple(couchbase_tm.CouchbaseClusterTasks,
                    find_cluster_node_ids=Mock(return_value=[]))
    def test_grow(self, mock_client, mock_check_quotas, mock_task_api,
                  mock_ins_create, mock_update):
        self.patch_conf(volume_support=True)
        mock_client.return_value.flavors = Mock()
        self.cluster.grow(self.instances_w_volumes)
        mock_task_api.return_value.grow_cluster.assert_called_with(
            self.dbcreate_mock.return_value.id,
            [mock_ins_create.return_value.id] * 3)
        self.assertEqual(3, mock_ins_create.call_count)

    @patch('trove.cluster.models.LOG')
    def test_delete_bad_task_status(self, mock_logging):
        self.cluster.db_info.task_status = ClusterTasks.BUILDING_INITIAL
        self.assertRaises(exception.UnprocessableEntity,
                          self.cluster.delete)

    @patch.object(task_api.API, 'delete_cluster')
    @patch.object(Cluster, 'update_db')
    @patch.object(inst_models.DBInstance, 'find_all')
    def test_delete_task_status_none(self,
                                     mock_find_all,
                                     mock_update_db,
                                     mock_delete_cluster):
        self.cluster.db_info.task_status = ClusterTasks.NONE
        self.cluster.delete()
        mock_update_db.assert_called_with(task_status=ClusterTasks.DELETING)

    @patch.object(task_api.API, 'delete_cluster')
    @patch.object(Cluster, 'update_db')
    @patch.object(inst_models.DBInstance, 'find_all')
    def test_delete_task_status_deleting(self,
                                         mock_find_all,
                                         mock_update_db,
                                         mock_delete_cluster):
        self.cluster.db_info.task_status = ClusterTasks.DELETING
        self.cluster.delete()
        mock_update_db.assert_called_with(task_status=ClusterTasks.DELETING)

    def test_get_instance_types(self):
        expected_default_types = ['data', 'index', 'query']
        couchbase_manager = 'couchbase'
        couchbase_4_manager = 'couchbase_4'
        couchbase_ee_manager = 'couchbase_ee'
        # data format:
        #     instances,
        #     manager, for_grow,
        #     expected_exception,
        #     expected_types or expected exception message
        data = [
            # line 01
            [
                [{}, {}, {}],
                couchbase_manager, False,
                None,
                [[], [], []]
            ],
            [
                [{'instance_type': ['data']},
                 {'instance_type': ['index', 'query']},
                 {},
                 {'instance_type': ''},
                 {'instance_type': None}],
                couchbase_manager, False,
                exception.TroveError,
                "Unknown type 'data,index,query' specified. "
                "Allowed values are ''."
            ],
            [
                [{'instance_type': ['query']}],
                couchbase_manager, False,
                exception.TroveError,
                "Unknown type 'query' specified. Allowed values are ''."
            ],
            [
                [{'instance_type': ['bad']}],
                couchbase_manager, False,
                exception.TroveError,
                "Unknown type 'bad' specified. Allowed values are ''."
            ],
            # line 05
            [
                [{'volume_size': 1, 'instance_type': ['index', 'data'],
                  'flavor_id': u'25', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None},
                 {'volume_size': 1, 'instance_type': ['query', 'bad'],
                  'flavor_id': u'17', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None}],
                couchbase_manager, False,
                exception.TroveError,
                "Unknown type 'bad,data,index,query' specified. "
                "Allowed values are ''."
            ],
            [
                [{"volume": {"size": "1"}, "flavorRef": "25"},
                 {"volume": {"size": "1"}, "flavorRef": "25",
                  "instance_type": ["index"]}],
                couchbase_manager, False,
                exception.TroveError,
                "Unknown type 'index' specified. Allowed values are ''."
            ],
            [
                [{}, {}, {}],
                couchbase_4_manager, False,
                None,
                [expected_default_types, expected_default_types,
                 expected_default_types]
            ],
            [
                [{'instance_type': ['data']},
                 {'instance_type': ['index', 'query']},
                 {},
                 {'instance_type': ''},
                 {'instance_type': None}],
                couchbase_4_manager, False,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'data' specified but 'data,index,query' is required "
                "\(per instance: True\)."
            ],
            [
                [{'instance_type': ['query']}],
                couchbase_4_manager, False,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'query' specified but 'data,index,query' is required "
                "\(per instance: True\)."
            ],
            # line 10
            [
                [{'instance_type': ['bad']}],
                couchbase_4_manager, False,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."
            ],
            [
                [{'volume_size': 1, 'instance_type': ['index', 'data'],
                  'flavor_id': u'25', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None},
                 {'volume_size': 1, 'instance_type': ['query', 'bad'],
                  'flavor_id': u'17', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None}],
                couchbase_4_manager, False,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."
            ],
            [
                [{"volume": {"size": "1"}, "flavorRef": "25"},
                 {"volume": {"size": "1"}, "flavorRef": "25",
                  "instance_type": ["index"]}],
                couchbase_4_manager, False,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'index' specified but 'data,index,query' is required "
                "\(per instance: True\)."
            ],
            [
                [{}, {}, {}],
                couchbase_ee_manager, False,
                None,
                [expected_default_types, expected_default_types,
                 expected_default_types]
            ],
            [
                [{'instance_type': ['data']},
                 {'instance_type': ['index', 'query']},
                 {},
                 {'instance_type': ''},
                 {'instance_type': None}],
                couchbase_ee_manager, False,
                None,
                [['data'], ['index', 'query'],
                 expected_default_types, expected_default_types,
                 expected_default_types]
            ],
            # line 15
            [
                [{'instance_type': ['query']}],
                couchbase_ee_manager, False,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'query' specified but 'data' is required "
                "\(per instance: False\)."
            ],
            [
                [{'instance_type': ['bad']}],
                couchbase_ee_manager, False,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."
            ],
            [
                [{'volume_size': 1, 'instance_type': ['index', 'data'],
                  'flavor_id': u'25', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None},
                 {'volume_size': 1, 'instance_type': ['query', 'bad'],
                  'flavor_id': u'17', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None}],
                couchbase_ee_manager, False,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."

            ],
            [
                [{"volume": {"size": "1"}, "flavorRef": "25"},
                 {"volume": {"size": "1"}, "flavorRef": "25",
                  "instance_type": ["index"]}],
                couchbase_ee_manager, False,
                None,
                [expected_default_types, ['index']]
            ],
            [
                [{}, {}, {}],
                couchbase_manager, True,
                None,
                [[], [], []]
            ],
            # line 20
            [
                [{'instance_type': ['data']},
                 {'instance_type': ['index', 'query']},
                 {},
                 {'instance_type': ''},
                 {'instance_type': None}],
                couchbase_manager, True,
                exception.TroveError,
                "Unknown type 'data,index,query' specified. "
                "Allowed values are ''."
            ],
            [
                [{'instance_type': ['query']}],
                couchbase_manager, True,
                exception.TroveError,
                "Unknown type 'query' specified. Allowed values are ''."
            ],
            [
                [{'instance_type': ['bad']}],
                couchbase_manager, True,
                exception.TroveError,
                "Unknown type 'bad' specified. Allowed values are ''."
            ],
            [
                [{'volume_size': 1, 'instance_type': ['index', 'data'],
                  'flavor_id': u'25', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None},
                 {'volume_size': 1, 'instance_type': ['query', 'bad'],
                  'flavor_id': u'17', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None}],
                couchbase_manager, True,
                exception.TroveError,
                "Unknown type 'bad,data,index,query' specified. "
                "Allowed values are ''."
            ],
            [
                [{"volume": {"size": "1"}, "flavorRef": "25"},
                 {"volume": {"size": "1"}, "flavorRef": "25",
                  "instance_type": ["index"]}],
                couchbase_manager, True,
                exception.TroveError,
                "Unknown type 'index' specified. Allowed values are ''."
            ],
            # line 25
            [
                [{}, {}, {}],
                couchbase_4_manager, True,
                None,
                [expected_default_types, expected_default_types,
                 expected_default_types]
            ],
            [
                [{'instance_type': ['data']},
                 {'instance_type': ['index', 'query']},
                 {},
                 {'instance_type': ''},
                 {'instance_type': None}],
                couchbase_4_manager, True,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'data' specified but 'data,index,query' is required "
                "\(per instance: True\)."
            ],
            [
                [{'instance_type': ['query']}],
                couchbase_4_manager, True,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'query' specified but 'data,index,query' is required "
                "\(per instance: True\)."
            ],
            [
                [{'instance_type': ['bad']}],
                couchbase_4_manager, True,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."
            ],
            [
                [{'volume_size': 1, 'instance_type': ['index', 'data'],
                  'flavor_id': u'25', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None},
                 {'volume_size': 1, 'instance_type': ['query', 'bad'],
                  'flavor_id': u'17', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None}],
                couchbase_4_manager, True,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."
            ],
            # line 30
            [
                [{"volume": {"size": "1"}, "flavorRef": "25"},
                 {"volume": {"size": "1"}, "flavorRef": "25",
                  "instance_type": ["index"]}],
                couchbase_4_manager, True,
                exception.ClusterInstanceTypeMissing,
                "Instance\(s\) missing one or more required types. "
                "'index' specified but 'data,index,query' is required "
                "\(per instance: True\)."
            ],
            [
                [{}, {}, {}],
                couchbase_ee_manager, True,
                None,
                [expected_default_types, expected_default_types,
                 expected_default_types]
            ],
            [
                [{'instance_type': ['data']},
                 {'instance_type': ['index', 'query']},
                 {},
                 {'instance_type': ''},
                 {'instance_type': None}],
                couchbase_ee_manager, True,
                None,
                [['data'], ['index', 'query'],
                 expected_default_types, expected_default_types,
                 expected_default_types]
            ],
            [
                [{'instance_type': ['query']}],
                couchbase_ee_manager, True,
                None,
                [['query']]
            ],
            [
                [{'instance_type': ['bad']}],
                couchbase_ee_manager, True,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."
            ],
            # line 35
            [
                [{'volume_size': 1, 'instance_type': ['index', 'data'],
                  'flavor_id': u'25', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None},
                 {'volume_size': 1, 'instance_type': ['query', 'bad'],
                  'flavor_id': u'17', 'availability_zone': None,
                  'nics': None, 'modules': None, 'volume_type': None,
                  'region_name': None}],
                couchbase_ee_manager, True,
                exception.TroveError,
                "Unknown type 'bad' specified. "
                "Allowed values are 'data,index,query'."

            ],
            [
                [{"volume": {"size": "1"}, "flavorRef": "25"},
                 {"volume": {"size": "1"}, "flavorRef": "25",
                  "instance_type": ["index"]}],
                couchbase_ee_manager, True,
                None,
                [expected_default_types, ['index']]
            ],
        ]

        for data_line, datum in enumerate(data, 1):
            instances = datum[0]
            manager = datum[1]
            for_grow = datum[2]
            expected_exception = datum[3]
            expected_result = datum[4]
            line_msg = ' (using data from line %d)' % data_line

            try:
                self.patch_conf_property('default_services',
                                         expected_default_types,
                                         section=manager)
            except AttributeError:
                pass
            if expected_exception:
                # We wrap this so we can print out the line_msg to know
                # which data caused the error.
                try:
                    self.assertRaisesRegex(
                        expected_exception, expected_result,
                        couchbase_api.CouchbaseCluster.get_instance_types,
                        instances, manager, for_grow)
                except Exception as ex:
                    self.fail("(%s) %s %s" % (
                        ex.__class__.__name__, ex, line_msg))
            else:
                types = couchbase_api.CouchbaseCluster.get_instance_types(
                    instances, manager, for_grow)
                self.assertEqual(
                    expected_result, types, "Wrong types found" + line_msg)

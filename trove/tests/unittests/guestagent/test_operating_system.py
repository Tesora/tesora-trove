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

import itertools
import os
import tempfile
import testtools
from testtools import ExpectedException
from ConfigParser import ParsingError
from mock import DEFAULT, call, MagicMock, patch
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import (IdentityCodec,
                                                      IniCodec, YamlCodec)


class TestOperatingSystem(testtools.TestCase):

    def test_identity_file_codec(self):
        data = ("Lorem Ipsum, Lorem Ipsum\n"
                "Lorem Ipsum, Lorem Ipsum\n"
                "Lorem Ipsum, Lorem Ipsum\n")

        self._test_file_codec(data, IdentityCodec())

    def test_ini_file_codec(self):
        data_no_none = {"Section1": {"s1k1": 's1v1',
                                     "s1k2": '3.1415926535'},
                        "Section2": {"s2k1": '1',
                                     "s2k2": 'True'}}

        self._test_file_codec(data_no_none, IniCodec())

        data_with_none = {"Section1": {"s1k1": 's1v1',
                                       "s1k2": '3.1415926535'},
                          "Section2": {"s2k1": '1',
                                       "s2k2": 'True',
                                       "s2k3": None}}

        self._test_file_codec(data_no_none, IniCodec(allow_no_value=True))
        self._test_file_codec(data_with_none, IniCodec(allow_no_value=True))

        self._test_file_codec(
            data_with_none,
            read_codec=IniCodec(allow_no_value=True),
            write_codec=IniCodec(allow_no_value=False),
            expected_exception=ExpectedException(TypeError))

        self._test_file_codec(
            data_with_none,
            read_codec=IniCodec(allow_no_value=False),
            write_codec=IniCodec(allow_no_value=True),
            expected_exception=ExpectedException(ParsingError))

    def test_yaml_file_codec(self):
        data = {"Section1": 's1v1',
                "Section2": {"s2k1": '1',
                             "s2k2": 'True'},
                "Section3": {"Section4": {"s4k1": '3.1415926535',
                                          "s4k2": None}}
                }

        self._test_file_codec(data, YamlCodec())
        self._test_file_codec(data, YamlCodec(default_flow_style=True))

    def _test_file_codec(self, data, read_codec, write_codec=None,
                         expected_exception=None):
        write_codec = write_codec or read_codec

        with tempfile.NamedTemporaryFile() as test_file:
            if expected_exception:
                with expected_exception:
                    operating_system.write_file(test_file.name, data,
                                                codec=write_codec)
                    operating_system.read_file(test_file.name,
                                               codec=read_codec)
            else:
                operating_system.write_file(test_file.name, data,
                                            codec=write_codec)
                read = operating_system.read_file(test_file.name,
                                                  codec=read_codec)
                self.assertEqual(data, read)

    def test_read_write_file_input_validation(self):
        with ExpectedException(exception.UnprocessableEntity,
                               "File does not exist: None"):
            operating_system.read_file(None)

        with ExpectedException(exception.UnprocessableEntity,
                               "File does not exist: /__DOES_NOT_EXIST__"):
            operating_system.read_file('/__DOES_NOT_EXIST__')

        with ExpectedException(exception.UnprocessableEntity,
                               "Invalid path: None"):
            operating_system.write_file(None, {})

    def test_write_file_as_root(self):
        target_file = tempfile.NamedTemporaryFile()
        temp_file = tempfile.NamedTemporaryFile()

        with patch('tempfile.NamedTemporaryFile', return_value=temp_file):
            self._assert_execute_call(
                [['cp', '-f', temp_file.name, target_file.name]],
                [{'run_as_root': True, 'root_helper': 'sudo'}],
                operating_system.write_file, None,
                target_file.name, "Lorem Ipsum", as_root=True
            )
        self.assertFalse(os.path.exists(temp_file.name))

    @patch('trove.common.utils.execute_with_timeout',
           side_effect=Exception("Error while executing 'cp'."))
    def test_write_file_as_root_with_error(self, execute):
        target_file = tempfile.NamedTemporaryFile()
        temp_file = tempfile.NamedTemporaryFile()
        with patch('tempfile.NamedTemporaryFile', return_value=temp_file):
            with ExpectedException(Exception, "Error while executing 'cp'."):
                operating_system.write_file(target_file.name,
                                            "Lorem Ipsum", as_root=True)
        self.assertFalse(os.path.exists(temp_file.name))

    def test_read_write_ini_file(self):
        self._test_read_write_file(
            operating_system.read_config_file,
            operating_system.write_config_file,
            'trove.guestagent.common.operating_system.IniCodec')

    def test_read_write_yaml_file(self):
        self._test_read_write_file(
            operating_system.read_yaml_file,
            operating_system.write_yaml_file,
            'trove.guestagent.common.operating_system.SafeYamlCodec')

    def _test_read_write_file(self, read_func, write_func, expected_codec_cls):
        with tempfile.NamedTemporaryFile() as test_file:
            with patch.multiple(
                    'trove.guestagent.common.operating_system',
                    write_file=DEFAULT, read_file=DEFAULT) as exp_calls:
                with patch(expected_codec_cls) as codec_cls:
                    sample_data = MagicMock()
                    write_func(test_file.name, sample_data)
                    read_func(test_file.name)
                    exp_calls['write_file'].assert_called_once_with(
                        test_file.name, sample_data,
                        codec=codec_cls.return_value, as_root=False)
                    exp_calls['read_file'].assert_called_once_with(
                        test_file.name, codec=codec_cls.return_value)

    def test_start_service(self):
        self._assert_service_call(operating_system.start_service,
                                  'cmd_start')

    def test_stop_service(self):
        self._assert_service_call(operating_system.stop_service,
                                  'cmd_stop')

    def test_enable_service_on_boot(self):
        self._assert_service_call(operating_system.enable_service_on_boot,
                                  'cmd_enable')

    def test_disable_service_on_boot(self):
        self._assert_service_call(operating_system.disable_service_on_boot,
                                  'cmd_disable')

    @patch.object(operating_system, '_execute_service_command')
    def _assert_service_call(self, fun, expected_cmd_key,
                             exec_service_cmd_mock):
        test_candidate_names = ['test_service_1', 'test_service_2']
        fun(test_candidate_names)
        exec_service_cmd_mock.assert_called_once_with(test_candidate_names,
                                                      expected_cmd_key)

    @patch.object(operating_system, 'service_discovery',
                  return_value={'cmd_start': 'start',
                                'cmd_stop': 'stop',
                                'cmd_enable': 'enable',
                                'cmd_disable': 'disable'})
    def test_execute_service_command(self, discovery_mock):
        test_service_candidates = ['service_name']
        self._assert_execute_call([['start']], [{'shell': True}],
                                  operating_system._execute_service_command,
                                  None, test_service_candidates, 'cmd_start')
        discovery_mock.assert_called_once_with(test_service_candidates)

        with ExpectedException(exception.UnprocessableEntity,
                               "Candidate service names not specified."):
            operating_system._execute_service_command([], 'cmd_disable')

        with ExpectedException(exception.UnprocessableEntity,
                               "Candidate service names not specified."):
            operating_system._execute_service_command(None, 'cmd_start')

        with ExpectedException(RuntimeError, "Service control command not "
                               "available: unknown"):
            operating_system._execute_service_command(test_service_candidates,
                                                      'unknown')

    def test_modes(self):
        self._assert_modes(None, None, None, operating_system.FileMode())
        self._assert_modes(None, None, None,
                           operating_system.FileMode([], [], []))
        self._assert_modes(0o770, 0o4, 0o3, operating_system.FileMode(
            [stat.S_IRWXU, stat.S_IRWXG],
            [stat.S_IROTH],
            [stat.S_IWOTH | stat.S_IXOTH])
        )
        self._assert_modes(0o777, None, None, operating_system.FileMode(
            [stat.S_IRWXU, stat.S_IRWXG, stat.S_IRWXO])
        )
        self._assert_modes(0o777, None, None, operating_system.FileMode(
            reset=[stat.S_IRWXU, stat.S_IRWXG, stat.S_IRWXO])
        )
        self._assert_modes(None, 0o777, None, operating_system.FileMode(
            add=[stat.S_IRWXU, stat.S_IRWXG, stat.S_IRWXO])
        )
        self._assert_modes(None, None, 0o777, operating_system.FileMode(
            remove=[stat.S_IRWXU, stat.S_IRWXG, stat.S_IRWXO])
        )

        self.assertEqual(
            operating_system.FileMode(add=[stat.S_IRUSR, stat.S_IWUSR]),
            operating_system.FileMode(add=[stat.S_IWUSR, stat.S_IRUSR]))

        self.assertEqual(
            hash(operating_system.FileMode(add=[stat.S_IRUSR, stat.S_IWUSR])),
            hash(operating_system.FileMode(add=[stat.S_IWUSR, stat.S_IRUSR])))

        self.assertNotEqual(
            operating_system.FileMode(add=[stat.S_IRUSR, stat.S_IWUSR]),
            operating_system.FileMode(reset=[stat.S_IRUSR, stat.S_IWUSR]))

        self.assertNotEqual(
            hash(operating_system.FileMode(add=[stat.S_IRUSR, stat.S_IWUSR])),
            hash(operating_system.FileMode(reset=[stat.S_IRUSR, stat.S_IWUSR]))
        )

    def _assert_modes(self, ex_reset, ex_add, ex_remove, actual):
        self.assertEqual(bool(ex_reset or ex_add or ex_remove),
                         actual.has_any())
        self.assertEqual(ex_reset, actual.get_reset_mode())
        self.assertEqual(ex_add, actual.get_add_mode())
        self.assertEqual(ex_remove, actual.get_remove_mode())

    def test_chmod(self):
        self._assert_execute_call(
            [['chmod', '-R', '=064', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chmod, None,
            'path', FileMode.SET_GRP_RW_OTH_R,
            as_root=True)
        self._assert_execute_call(
            [['chmod', '-R', '+444', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chmod, None,
            'path', FileMode.ADD_READ_ALL,
            as_root=True)

        self._assert_execute_call(
            [['chmod', '-R', '+060', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chmod, None,
            'path', FileMode.ADD_GRP_RW,
            as_root=True)

        self._assert_execute_call(
            [['chmod', '-R', '=777', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chmod, None,
            'path', FileMode.SET_FULL,
            as_root=True)

        self._assert_execute_call(
            [['chmod', '-f', '=777', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chmod, None,
            'path', FileMode.SET_FULL,
            as_root=True, recursive=False, force=True)

        self._assert_execute_call(
            [['chmod', '-R', '=777', 'path']],
            [{'timeout': 100}],
            operating_system.chmod, None,
            'path', FileMode.SET_FULL,
            timeout=100)

        self._assert_execute_call(
            [['chmod', '-R', '=777', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo', 'timeout': None}],
            operating_system.chmod, None,
            'path', FileMode.SET_FULL,
            as_root=True, timeout=None)

        self._assert_execute_call(
            None, None,
            operating_system.chmod,
            ExpectedException(exception.UnprocessableEntity,
                              "No file mode specified."),
            'path', FileMode())

        self._assert_execute_call(
            None, None,
            operating_system.chmod,
            ExpectedException(exception.UnprocessableEntity,
                              "No file mode specified."),
            'path', None)

        self._assert_execute_call(
            None, None,
            operating_system.chmod,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot change mode of a blank file."),
            '', FileMode.SET_FULL)

        self._assert_execute_call(
            None, None,
            operating_system.chmod,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot change mode of a blank file."),
            None, FileMode.SET_FULL)

        self._assert_execute_call(
            None, None,
            operating_system.chmod,
            ExpectedException(UnknownArgumentError,
                              "Got unknown keyword args: {'_unknown_kw': 0}"),
            'path', FileMode.SET_FULL, _unknown_kw=0)

    def test_remove(self):
        self._assert_execute_call(
            [['rm', '-R', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.remove, None, 'path', as_root=True)

        self._assert_execute_call(
            [['rm', '-f', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.remove, None, 'path', recursive=False, force=True,
            as_root=True)

        self._assert_execute_call(
            [['rm', '-R', 'path']],
            [{'timeout': 100}],
            operating_system.remove, None,
            'path', timeout=100)

        self._assert_execute_call(
            [['rm', '-R', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo', 'timeout': None}],
            operating_system.remove, None, 'path', timeout=None, as_root=True)

        self._assert_execute_call(
            None, None,
            operating_system.remove,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot remove a blank file."), '')

        self._assert_execute_call(
            None, None,
            operating_system.remove,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot remove a blank file."), None)

        self._assert_execute_call(
            None, None,
            operating_system.remove,
            ExpectedException(UnknownArgumentError,
                              "Got unknown keyword args: {'_unknown_kw': 0}"),
            'path', _unknown_kw=0)

    def test_move(self):
        self._assert_execute_call(
            [['mv', 'source', 'destination']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.move, None, 'source', 'destination', as_root=True)

        self._assert_execute_call(
            [['mv', '-f', 'source', 'destination']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.move, None, 'source', 'destination', force=True,
            as_root=True)

        self._assert_execute_call(
            [['mv', 'source', 'destination']],
            [{'timeout': 100}],
            operating_system.move, None, 'source', 'destination',
            timeout=100)

        self._assert_execute_call(
            [['mv', 'source', 'destination']],
            [{'run_as_root': True, 'root_helper': 'sudo', 'timeout': None}],
            operating_system.move, None, 'source', 'destination', timeout=None,
            as_root=True)

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), '', 'destination')

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), None, 'destination')

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing destination path."), 'source', '')

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing destination path."), 'source', None)

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), '', '')

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), None, None)

        self._assert_execute_call(
            None, None,
            operating_system.move,
            ExpectedException(UnknownArgumentError,
                              "Got unknown keyword args: {'_unknown_kw': 0}"),
            'source', 'destination', _unknown_kw=0)

    def test_copy(self):
        self._assert_execute_call(
            [['cp', '-R', 'source', 'destination']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.copy, None, 'source', 'destination', as_root=True)

        self._assert_execute_call(
            [['cp', '-f', '-p', 'source', 'destination']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.copy, None, 'source', 'destination', force=True,
            preserve=True, recursive=False, as_root=True)

        self._assert_execute_call(
            [['cp', '-R', 'source', 'destination']],
            [{'timeout': 100}],
            operating_system.copy, None, 'source', 'destination',
            timeout=100)

        self._assert_execute_call(
            [['cp', '-R', 'source', 'destination']],
            [{'run_as_root': True, 'root_helper': "sudo", 'timeout': None}],
            operating_system.copy, None, 'source', 'destination', timeout=None,
            as_root=True)

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), '', 'destination')

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), None, 'destination')

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing destination path."), 'source', '')

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing destination path."), 'source', None)

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), '', '')

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(exception.UnprocessableEntity,
                              "Missing source path."), None, None)

        self._assert_execute_call(
            None, None,
            operating_system.copy,
            ExpectedException(UnknownArgumentError,
                              "Got unknown keyword args: {'_unknown_kw': 0}"),
            'source', 'destination', _unknown_kw=0)

    def test_chown(self):
        self._assert_execute_call(
            [['chown', '-R', 'usr:grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None, 'path', 'usr', 'grp', as_root=True)

        self._assert_execute_call(
            [['chown', 'usr:grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None,
            'path', 'usr', 'grp', recursive=False, as_root=True)

        self._assert_execute_call(
            [['chown', '-f', '-R', 'usr:grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None,
            'path', 'usr', 'grp', force=True, as_root=True)

        self._assert_execute_call(
            [['chown', '-R', ':grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None, 'path', '', 'grp', as_root=True)

        self._assert_execute_call(
            [['chown', '-R', 'usr:', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None, 'path', 'usr', '', as_root=True)

        self._assert_execute_call(
            [['chown', '-R', ':grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None, 'path', None, 'grp', as_root=True)

        self._assert_execute_call(
            [['chown', '-R', 'usr:', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.chown, None, 'path', 'usr', None, as_root=True)

        self._assert_execute_call(
            [['chown', '-R', 'usr:', 'path']],
            [{'timeout': 100}],
            operating_system.chown, None,
            'path', 'usr', None, timeout=100)

        self._assert_execute_call(
            [['chown', '-R', 'usr:', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo',
              'timeout': None}],
            operating_system.chown, None,
            'path', 'usr', None, timeout=None, as_root=True)

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot change ownership of a blank file."),
            '', 'usr', 'grp')

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot change ownership of a blank file."),
            None, 'usr', 'grp')

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(exception.UnprocessableEntity,
                              "Please specify owner or group, or both."),
            'path', '', '')

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(exception.UnprocessableEntity,
                              "Please specify owner or group, or both."),
            'path', None, None)

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot change ownership of a blank file."),
            None, None, None)

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot change ownership of a blank file."),
            '', '', '')

        self._assert_execute_call(
            None, None,
            operating_system.chown,
            ExpectedException(UnknownArgumentError,
                              "Got unknown keyword args: {'_unknown_kw': 0}"),
            'path', 'usr', None, _unknown_kw=0)

    def test_create_directory(self):
        self._assert_execute_call(
            [['mkdir', '-p', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None, 'path', as_root=True)

        self._assert_execute_call(
            [['mkdir', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None, 'path', force=False,
            as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', 'usr:grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'},
             {'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None,
            'path', user='usr', group='grp', as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', ':grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'},
             {'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None, 'path', group='grp',
            as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', 'usr:', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'},
             {'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None, 'path', user='usr',
            as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', 'usr:', 'path']],
            [{'timeout': 100}, {'timeout': 100}],
            operating_system.create_directory, None,
            'path', user='usr', timeout=100)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', 'usr:', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo', 'timeout': None},
             {'run_as_root': True, 'root_helper': 'sudo', 'timeout': None}],
            operating_system.create_directory, None,
            'path', user='usr', timeout=None, as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', 'usr:', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'},
             {'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None,
            'path', user='usr', group='', as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path'], ['chown', '-R', ':grp', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'},
             {'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None,
            'path', user='', group='grp', as_root=True)

        self._assert_execute_call(
            [['mkdir', '-p', 'path']],
            [{'run_as_root': True, 'root_helper': 'sudo'}],
            operating_system.create_directory, None, 'path', user='', group='',
            as_root=True)

        self._assert_execute_call(
            None, None,
            operating_system.create_directory,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot create a blank directory."),
            '', user='usr', group='grp')

        self._assert_execute_call(
            None, None,
            operating_system.create_directory,
            ExpectedException(exception.UnprocessableEntity,
                              "Cannot create a blank directory."), None)

        self._assert_execute_call(
            None, None,
            operating_system.create_directory,
            ExpectedException(UnknownArgumentError,
                              "Got unknown keyword args: {'_unknown_kw': 0}"),
            'path', _unknown_kw=0)

    def _assert_execute_call(self, exec_args, exec_kwargs,
                             fun, return_value, *args, **kwargs):
        """
        Execute a function with given arguments.
        Assert a return value and appropriate sequence of calls to the
        'utils.execute_with_timeout' interface as the result.

        :param exec_args:         Expected arguments to the execute calls.
                                  This is a list-of-list where each sub-list
                                  represent a single call to
                                  'utils.execute_with_timeout'.
        :type exec_args:          list-of-lists

        :param exec_kwargs:       Expected keywords to the execute call.
                                  This is a list-of-dicts where each dict
                                  represent a single call to
                                  'utils.execute_with_timeout'.
        :type exec_kwargs:        list-of-dicts

        :param fun:               Tested function call.
        :type fun:                callable

        :param return_value:      Expected return value or exception
                                  from the tested call if any.
        :type return_value:       object

        :param args:              Arguments passed to the tested call.
        :type args:               list

        :param kwargs:            Keywords passed to the tested call.
        :type kwargs:             dict
        """

        with patch.object(utils, 'execute_with_timeout') as exec_call:
            if isinstance(return_value, ExpectedException):
                with return_value:
                    fun(*args, **kwargs)
            else:
                actual_value = fun(*args, **kwargs)
                if return_value is not None:
                    self.assertEqual(return_value, actual_value,
                                     "Return value mismatch.")
                expected_calls = []
                for arg, kw in itertools.izip(exec_args, exec_kwargs):
                    expected_calls.append(call(*arg, **kw))

                self.assertEqual(expected_calls, exec_call.mock_calls,
                                 "Mismatch in calls to "
                                 "'execute_with_timeout'.")

    def test_get_os_redhat(self):
        with patch.object(os.path, 'isfile', side_effect=[True]):
            find_os = operating_system.get_os()
        self.assertEqual('redhat', find_os)

    def test_get_os_suse(self):
        with patch.object(os.path, 'isfile', side_effect=[False, True]):
            find_os = operating_system.get_os()
        self.assertEqual('suse', find_os)

    def test_get_os_debian(self):
        with patch.object(os.path, 'isfile', side_effect=[False, False]):
            find_os = operating_system.get_os()
        self.assertEqual('debian', find_os)

    def test_upstart_type_service_discovery(self):
        with patch.object(os.path, 'isfile', side_effect=[True]):
            mysql_service = operating_system.service_discovery(["mysql"])
        self.assertIsNotNone(mysql_service['cmd_start'])
        self.assertIsNotNone(mysql_service['cmd_enable'])

    def test_sysvinit_type_service_discovery(self):
        with patch.object(os.path, 'isfile', side_effect=[False, True, True]):
            mysql_service = operating_system.service_discovery(["mysql"])
        self.assertIsNotNone(mysql_service['cmd_start'])
        self.assertIsNotNone(mysql_service['cmd_enable'])

    def test_sysvinit_chkconfig_type_service_discovery(self):
        with patch.object(os.path, 'isfile',
                          side_effect=[False, True, False, True]):
            mysql_service = operating_system.service_discovery(["mysql"])
        self.assertIsNotNone(mysql_service['cmd_start'])
        self.assertIsNotNone(mysql_service['cmd_enable'])

    @patch.object(os.path, 'islink', return_value=True)
    @patch.object(os.path, 'realpath')
    @patch.object(os.path, 'basename')
    def test_systemd_symlinked_type_service_discovery(self, mock_base,
                                                      mock_path, mock_islink):
        with patch.object(os.path, 'isfile', side_effect=[False, False, True]):
            mysql_service = operating_system.service_discovery(["mysql"])
        self.assertIsNotNone(mysql_service['cmd_start'])
        self.assertIsNotNone(mysql_service['cmd_enable'])

    def test_systemd_not_symlinked_type_service_discovery(self):
        with patch.object(os.path, 'isfile', side_effect=[False, False, True]):
            with patch.object(os.path, 'islink', return_value=False):
                mysql_service = operating_system.service_discovery(["mysql"])
        self.assertIsNotNone(mysql_service['cmd_start'])
        self.assertIsNotNone(mysql_service['cmd_enable'])

    def test_file_discovery(self):
        with patch.object(os.path, 'isfile', side_effect=[False, True]):
                config_file = operating_system.file_discovery(
                    ["/etc/mongodb.conf", "/etc/mongod.conf"])
        self.assertEqual('/etc/mongod.conf', config_file)

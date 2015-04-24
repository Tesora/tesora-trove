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

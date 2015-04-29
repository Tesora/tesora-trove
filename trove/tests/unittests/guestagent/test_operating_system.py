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
import testtools
from testtools import ExpectedException
from mock import call, patch
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import operating_system


class TestOperatingSystem(testtools.TestCase):

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

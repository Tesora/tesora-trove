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

import tempfile
import testtools
from testtools import ExpectedException
from trove.common import exception
from trove.guestagent.common import operating_system
from ConfigParser import ParsingError


class Test(testtools.TestCase):

    def test_read_write_config(self):
        with tempfile.NamedTemporaryFile() as test_file:
            contents = {"Section1": {"s1k1": 's1v1',
                                     "s1k2": '3.1415926535'},
                        "Section2": {"s2k1": '1',
                                     "s2k2": 'True'}}

            operating_system.write_config_file(test_file.name, contents)
            read = operating_system.read_config_file(test_file.name)
            self.assertEqual(contents, read)

        with tempfile.NamedTemporaryFile() as test_file:
            contents = {"Section1": {"s1k1": 's1v1',
                                     "s1k2": '3.1415926535'},
                        "Section2": {"s2k1": '1',
                                     "s2k2": 'True',
                                     "s2k3": None}}

            with ExpectedException(TypeError):
                operating_system.write_config_file(test_file.name, contents)

            operating_system.write_config_file(test_file.name, contents,
                                               allow_no_value=True)
            with ExpectedException(ParsingError):
                read = operating_system.read_config_file(test_file.name)

            read = operating_system.read_config_file(test_file.name,
                                                     allow_no_value=True)
            self.assertEqual(contents, read)

        with ExpectedException(exception.UnprocessableEntity,
                               "File does not exist: None"):
            read = operating_system.read_config_file(None)

        with ExpectedException(exception.UnprocessableEntity,
                               "File does not exist: /__DOES_NOT_EXIST__"):
            read = operating_system.read_config_file('/__DOES_NOT_EXIST__')

        with ExpectedException(exception.UnprocessableEntity,
                               "Invalid path: None"):
            operating_system.write_config_file(None, {})

    def test_read_write_yaml_file(self):
        contents = {"Section1": 's1v1',
                    "Section2": {"s2k1": '1',
                                 "s2k2": 'True'},
                    "Section3": {"Section4": {"s4k1": '3.1415926535',
                                              "s4k2": None}}
                    }

        with tempfile.NamedTemporaryFile() as test_file:
            operating_system.write_yaml_file(test_file.name, contents)
            read = operating_system.read_yaml_file(test_file.name)
            self.assertEqual(contents, read)

        with tempfile.NamedTemporaryFile() as test_file:
            operating_system.write_yaml_file(test_file.name, contents,
                                             default_flow_style=True)
            read = operating_system.read_yaml_file(test_file.name)
            self.assertEqual(contents, read)

        with ExpectedException(exception.UnprocessableEntity,
                               "File does not exist: None"):
            read = operating_system.read_yaml_file(None)

        with ExpectedException(exception.UnprocessableEntity,
                               "File does not exist: /__DOES_NOT_EXIST__"):
            read = operating_system.read_yaml_file('/__DOES_NOT_EXIST__')

        with ExpectedException(exception.UnprocessableEntity,
                               "Invalid path: None"):
            operating_system.write_yaml_file(None, {})

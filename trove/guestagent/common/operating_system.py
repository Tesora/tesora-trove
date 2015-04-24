# Copyright (c) 2011 OpenStack Foundation
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

import abc
import os
import StringIO
import tempfile
import yaml
from ConfigParser import SafeConfigParser
from trove.common import exception
from trove.common import utils
from trove.common.i18n import _

REDHAT = 'redhat'
DEBIAN = 'debian'
SUSE = 'suse'

# A newline character for writing into text files (default).
# Do not use 'os.linesep' when writing files in the text mode.
# Python does automatic conversion to platform-specific representation.
# '\n' would get converted twice and you would end up with '\r\r\n'.
NEWLINE = '\n'


class StreamCodec(object):

    @abc.abstractmethod
    def serialize(self, data):
        """Serialize a Python object into a stream.
        """

    @abc.abstractmethod
    def deserialize(self, stream):
        """Deserialize stream data into a Python structure.
        """


class IdentityCodec(StreamCodec):
    """
    A basic passthrough codec.
    Does not modify the data in any way.
    """

    def serialize(self, data):
        return data

    def deserialize(self, stream):
        return stream


class YamlCodec(StreamCodec):
    """
    Read/write data from/into a YAML config file.

    a: 1
    b: {c: 3, d: 4}
    ...

    The above file content (flow-style) would be represented as:
    {'a': 1,
     'b': {'c': 3, 'd': 4,}
     ...
    }
    """

    def __init__(self, default_flow_style=False):
        """
        :param default_flow_style:  Use flow-style (inline) formatting of
                                    nested collections.
        :type default_flow_style:   boolean
        """
        self.__default_flow_style = default_flow_style

    def serialize(self, dict_data):
        return yaml.dump(dict_data,
                         default_flow_style=self.__default_flow_style)

    def deserialize(self, stream):
        return yaml.load(stream)


class IniCodec(StreamCodec):
    """
    Read/write data from/into an ini-style config file.

    [section_1]
    key = value
    key = value
    ...

    [section_2]
    key = value
    key = value
    ...

    The above file content would be represented as:
    {'section_1': {'key': value, 'key': value, ...},
     'section_2': {'key': value, key': value, ...}
     ...
    }
    """

    def __init__(self, allow_no_value=False):
        """
        :param allow_no_value:  Allow keys without values
                                (one line each, written without trailing '=').
        :type allow_no_value:   boolean
        """
        self.__allow_no_value = allow_no_value

    def serialize(self, dict_data):
        parser = self.__build_config_parser(dict_data)
        output = StringIO.StringIO()
        parser.write(output)

        return output.getvalue()

    def deserialize(self, stream):
        parser = self.__build_config_parser()
        parser.readfp(StringIO.StringIO(stream))

        return {s: {k: v for k, v in parser.items(s, raw=True)}
                for s in parser.sections()}

    def __build_config_parser(self, sections=None):
        parser = SafeConfigParser(allow_no_value=self.__allow_no_value)
        if sections:
            for section in sections:
                    parser.add_section(section)
                    for key, value in sections[section].items():
                        parser.set(section, key, value)

        return parser


def read_config_file(path, allow_no_value=False):
    return read_file(path, codec=IniCodec(allow_no_value=allow_no_value))


def write_config_file(path, data, allow_no_value=False, as_root=False):
    codec = IniCodec(allow_no_value=allow_no_value)
    write_file(path, data, codec=codec, as_root=as_root)


def read_yaml_file(path):
    return read_file(path, codec=YamlCodec())


def write_yaml_file(path, data, default_flow_style=False, as_root=False):
    codec = YamlCodec(default_flow_style=default_flow_style)
    write_file(path, data, codec=codec, as_root=as_root)


def read_file(path, codec=IdentityCodec()):
    """
    Read a file into a Python data structure
    digestible by 'write_file'.

    :param path             Path to the read config file.
    :type path              string

    :param codec:           A codec used to deserialize the data.
    :type codec:            StreamCodec

    :returns:               A dictionary of key-value pairs.

    :raises:                :class:`UnprocessableEntity` if file doesn't exist.
    :raises:                :class:`UnprocessableEntity` if codec not given.
    """
    if path and os.path.exists(path):
        with open(path, 'r') as fp:
            return codec.deserialize(fp.read())

    raise exception.UnprocessableEntity(_("File does not exist: %s") % path)


def write_file(path, data, codec=IdentityCodec(), as_root=False):
    """Write data into file using a given codec.
    Overwrite any existing contents.
    The written file can be read back into its original
    form by 'read_file'.

    :param path                Path to the written config file.
    :type path                 string

    :param data:               An object representing the file contents.
    :type data:                object

    :param codec:              A codec used to serialize the data.
    :type codec:               StreamCodec

    :param codec:              Execute as root.
    :type codec:               boolean

    :raises:                   :class:`UnprocessableEntity` if path not given.
    """
    if path:
        if as_root:
            _write_file_as_root(path, data, codec)
        else:
            with open(path, 'w', 0) as fp:
                fp.write(codec.serialize(data))
    else:
        raise exception.UnprocessableEntity(_("Invalid path: %s") % path)


def _write_file_as_root(path, data, codec=IdentityCodec):
    """Write a file as root. Overwrite any existing contents.

    :param path                Path to the written file.
    :type path                 string

    :param data:               An object representing the file contents.
    :type data:                StreamCodec

    :param codec:              A codec used to serialize the data.
    :type codec:               StreamCodec
    """
    # The files gets removed automatically once the managing object goes
    # out of scope.
    with tempfile.NamedTemporaryFile('w', 0, delete=False) as fp:
        fp.write(codec.serialize(data))
        fp.close()  # Release the resource before proceeding.
        utils.execute_with_timeout("cp", "-f", fp.name, path,
                                   run_as_root=True, root_helper="sudo")


def get_os():
    if os.path.isfile("/etc/redhat-release"):
        return REDHAT
    elif os.path.isfile("/etc/SuSE-release"):
        return SUSE
    else:
        return DEBIAN


def file_discovery(file_candidates):
    for file in file_candidates:
        if os.path.isfile(file):
            return file


def service_discovery(service_candidates):
    """
    This function discovering how to start, stop, enable, disable service
    in current environment. "service_candidates" is array with possible
    system service names. Works for upstart, systemd, sysvinit.
    """
    result = {}
    for service in service_candidates:
        # check upstart
        if os.path.isfile("/etc/init/%s.conf" % service):
            # upstart returns error code when service already started/stopped
            result['cmd_start'] = "sudo start %s || true" % service
            result['cmd_stop'] = "sudo stop %s || true" % service
            result['cmd_enable'] = ("sudo sed -i '/^manual$/d' "
                                    "/etc/init/%s.conf" % service)
            result['cmd_disable'] = ("sudo sh -c 'echo manual >> "
                                     "/etc/init/%s.conf'" % service)
            break
        # check sysvinit
        if os.path.isfile("/etc/init.d/%s" % service):
            result['cmd_start'] = "sudo service %s start" % service
            result['cmd_stop'] = "sudo service %s stop" % service
            if os.path.isfile("/usr/sbin/update-rc.d"):
                result['cmd_enable'] = "sudo update-rc.d %s defaults; sudo " \
                                       "update-rc.d %s enable" % (service,
                                                                  service)
                result['cmd_disable'] = "sudo update-rc.d %s defaults; sudo " \
                                        "update-rc.d %s disable" % (service,
                                                                    service)
            elif os.path.isfile("/sbin/chkconfig"):
                result['cmd_enable'] = "sudo chkconfig %s on" % service
                result['cmd_disable'] = "sudo chkconfig %s off" % service
            break
        # check systemd
        service_path = "/lib/systemd/system/%s.service" % service
        if os.path.isfile(service_path):
            result['cmd_start'] = "sudo systemctl start %s" % service
            result['cmd_stop'] = "sudo systemctl stop %s" % service

            # currently "systemctl enable" doesn't work for symlinked units
            # as described in https://bugzilla.redhat.com/1014311, therefore
            # replacing a symlink with its real path
            if os.path.islink(service_path):
                real_path = os.path.realpath(service_path)
                unit_file_name = os.path.basename(real_path)
                result['cmd_enable'] = ("sudo systemctl enable %s" %
                                        unit_file_name)
                result['cmd_disable'] = ("sudo systemctl disable %s" %
                                         unit_file_name)
            else:
                result['cmd_enable'] = "sudo systemctl enable %s" % service
                result['cmd_disable'] = "sudo systemctl disable %s" % service
            break
    return result


def update_owner(user, group, path):
    """
       Changes the owner and group for the path (recursively)
    """
    utils.execute_with_timeout("chown", "-R", "%s:%s" % (user, group), path,
                               run_as_root=True, root_helper="sudo")

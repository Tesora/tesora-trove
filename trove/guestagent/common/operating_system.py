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

import os

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


def read_config_file(path, allow_no_value=False):
    """
    Read a given ini-style config file into a nested dictionary
    digestible by 'write_config_file'.

    :param path             Path to the read config file.
    :type path              string

    :param allow_no_value:  Allow keys without values.
    :type allow_no_value:   boolean

    :returns:               A nested dictionary where the outer level
                            represents whole sections of inner key-value pairs.

    :raises:                :class:`UnprocessableEntity` if file doesn't exist.
    """
    if path and os.path.exists(path):
        config = __create_config_parser(allow_no_value)
        with open(path, 'r') as fp:
            config.readfp(fp)

        return {s: {k: v for k, v in config.items(s, raw=True)}
                for s in config.sections()}

    raise exception.UnprocessableEntity(_("File does not exist: %s") % path)


def write_config_file(path, sections, allow_no_value=False):
    """
    Write given sections into an ini-style config file.
    The written file can be read back into its original dictionary
    form by 'read_config_file'.

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

    :param path             Path to the written config file.
    :type path              string

    :param sections:        A nested dictionary where the outer level
                            represents whole sections of inner key-value pairs.
    :type sections:         dict

    :param allow_no_value:  Allow keys without values
                            (one line each, written without trailing '=').
    :type allow_no_value:   boolean

    :raises:                :class:`UnprocessableEntity` if path not given.
    """
    if path:
        config = __build_config_parser(sections, allow_no_value)
        with open(path, 'w', 0) as fp:
            config.write(fp)
    else:
        raise exception.UnprocessableEntity(_("Invalid path: %s") % path)


def read_yaml_file(path):
    """
    Read a given YAML config file into a dictionary
    digestible by 'write_yaml_file'.

    :param path             Path to the read config file.
    :type path              string

    :returns:               A dictionary of key-value pairs.

    :raises:                :class:`UnprocessableEntity` if file doesn't exist.
    """
    if path and os.path.exists(path):
        with open(path, 'r') as fp:
            return yaml.load(fp.read())

    raise exception.UnprocessableEntity(_("File does not exist: %s") % path)


def write_yaml_file(path, sections, default_flow_style=False):
    """Write given sections into an YAML config file.
    The written file can be read back into its original dictionary
    form by 'read_yaml_file'.

    a: 1
    b: {c: 3, d: 4}
    ...

    The above file content (flow-style) would be represented as:
    {'a': 1,
     'b': {'c': 3, 'd': 4,}
     ...
    }

    :param path                 Path to the written config file.
    :type path                  string

    :param sections:            A dictionary representing the file contents
                                (see above).
    :type sections:             dict

    :param default_flow_style:  Use flow-style (inline) formatting of nested
                                collections.
    :type default_flow_style:   boolean

    :raises:                    :class:`UnprocessableEntity` if path not given.
    """
    if path:
        with open(path, 'w', 0) as fp:
            fp.write(yaml.dump(sections,
                               default_flow_style=default_flow_style))
    else:
        raise exception.UnprocessableEntity(_("Invalid path: %s") % path)


def __build_config_parser(sections, allow_no_value=False):
    config = __create_config_parser(allow_no_value)
    for section in sections:
            config.add_section(section)
            for key, value in sections[section].items():
                config.set(section, key, value)

    return config


def __create_config_parser(allow_no_value=False):
    return SafeConfigParser(allow_no_value=allow_no_value)


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

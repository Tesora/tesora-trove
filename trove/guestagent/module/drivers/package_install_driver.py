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

from datetime import date

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.module.drivers import module_driver


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class PackageInstallDriver(module_driver.ModuleDriver):
    """Module to install a package."""

    def get_type(self):
        return 'package_install'

    def get_description(self):
        return "Package Install Module Driver"

    def get_updated(self):
        return date(2016, 4, 12)

    @module_driver.output(
        log_message=_("Installing Package '%(pkg_name)s'"),
        success_message=_("Package '%(pkg_name)s' installed"),
        fail_message=_("Package '%(pkg_name)s' not installed"))
    def apply(self, name, datastore, ds_version, data_file, admin_module):
        if not admin_module:
            raise exception.ModuleInvalid(
                reason='Module not created with admin options')
        pkg_cmd, install_opts, uninstall_opts = (
            operating_system.get_package_command())
        if pkg_cmd:
            self._run_pkg_cmd(pkg_cmd, install_opts, data_file)

    def _run_pkg_cmd(self, pkg_cmd, opts, target):
        cmd = [pkg_cmd]
        cmd.extend(opts)
        cmd.append(target)
        exec_args = {'timeout': 120,
                     'run_as_root': True,
                     'root_helper': 'sudo',
                     'log_output_on_error': True}
        return utils.execute_with_timeout(*cmd, **exec_args)

    @module_driver.output(
        log_message=_("Removing package '%(pkg_name)s'"),
        success_message=_("Package '%(pkg_name)s' removed"),
        fail_message=_("Package '%(pkg_name)s' not removed"))
    def remove(self, name, datastore, ds_version, datafile):
        pkg_cmd, install_opts, uninstall_opts = (
            operating_system.get_package_command())
        if pkg_cmd:
            self._run_pkg_cmd(
                pkg_cmd, uninstall_opts, self.message_args['pkg_name'])

    def configure(self, name, datastore, ds_version, datafile):
        pkg_name = "unknown"
        try:
            pkg_name = operating_system.get_package_name(datafile)
        except Exception:
            LOG.exception(
                _("Could not get package name for '%' module") % name)
        self.message_args = {'pkg_name': pkg_name}

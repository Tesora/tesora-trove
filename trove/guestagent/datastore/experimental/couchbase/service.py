# Copyright (c) 2013 eBay Software Foundation
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

import json
import os
import stat
import subprocess
import tempfile

from oslo_log import log as logging
from oslo_utils import netutils
import pexpect

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common import utils as utils
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.couchbase import system
from trove.guestagent.datastore import service
from trove.guestagent.db import models
from trove.guestagent import pkg


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
packager = pkg.Package()


class CouchbaseApp(object):
    """
    Handles installation and configuration of couchbase
    on a trove instance.
    """

    MIN_RAMSIZE_QUOTA_MB = 256
    _ADMIN_USER = 'root'  # TODO(pmalik): Should be 'os_admin'.

    @property
    def couchbase_owner(self):
        return 'couchbase'

    @property
    def http_client_port(self):
        return 8091

    def __init__(self, status, state_change_wait_time=None):
        """
        Sets default status and state_change_wait_time
        """
        if state_change_wait_time:
            self.state_change_wait_time = state_change_wait_time
        else:
            self.state_change_wait_time = CONF.state_change_wait_time
        self.status = status

    def install_if_needed(self, packages):
        """
        Install couchbase if needed, do nothing if it is already installed.
        """
        LOG.info(_('Preparing Guest as Couchbase Server.'))
        if not packager.pkg_is_installed(packages):
            LOG.debug('Installing Couchbase.')
            self._install_couchbase(packages)

    def apply_initial_guestagent_configuration(self, available_ram_mb):
        self.ip_address = netutils.get_my_ipv4()
        mount_point = CONF.couchbase.mount_point
        try:
            LOG.info(_('Configuring node-specific parameters.'))
            self.run_node_init(mount_point, mount_point, self.ip_address)

            LOG.info(_('Configuring cluster parameters.'))
            cluster_admin_password = CouchbaseRootAccess.get_password()

            ramsize_quota_pc = CONF.couchbase.cluster_ramsize_pc / 100.0
            ramsize_quota_mb = min(round(ramsize_quota_pc * available_ram_mb),
                                   self.MIN_RAMSIZE_QUOTA_MB)

            self.run_cluster_init(self._ADMIN_USER,
                                  cluster_admin_password,
                                  self.http_client_port, ramsize_quota_mb)
            LOG.info(_('Couchbase Server initial setup finished.'))
        except exception.ProcessExecutionError:
            LOG.exception(_('Error performing initial Couchbase setup.'))
            raise RuntimeError("Couchbase Server initial setup failed")

    def init_storage_structure(self, mount_point):
        try:
            operating_system.create_directory(
                mount_point, user=self.couchbase_owner,
                group=self.couchbase_owner, as_root=True)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error while initiating storage structure."))

    def run_node_init(self, data_path, index_path, hostname):
        self._run_couchbase_command(
            'node-init', {'node-init-data-path': data_path,
                          'node-init-index-path': index_path,
                          'node-init-hostname': hostname})

    def run_cluster_init(self, cluster_admin, cluster_admin_password,
                         cluster_http_port, ramsize_quota_mb):
        self._run_couchbase_command(
            'cluster-init', {'cluster-init-username': cluster_admin,
                             'cluster-init-password': cluster_admin_password,
                             'cluster-init-port': cluster_http_port,
                             'cluster-ramsize': ramsize_quota_mb})

    def _run_couchbase_command(self, cmd, options, **kwargs):
        """Execute a couchbase-cli command on this node.
        """
        # couchbase-cli COMMAND -c [host]:[port] -u user -p password [options]

        host_and_port = 'localhost:%d' % self.http_client_port
        password = CouchbaseRootAccess.get_password()
        args = self._build_command_options(options)
        cmd_tokens = [self.couchbase_cli_bin, cmd,
                      '-c', host_and_port,
                      '-u', self._ADMIN_USER,
                      '-p', password] + args
        return utils.execute(' '.join(cmd_tokens), shell=True, **kwargs)

    def _build_command_options(self, options):
        return ['--%s=%s' % (name, value) for name, value in options.items()]

    @property
    def couchbase_cli_bin(self):
        return guestagent_utils.build_file_path(self.couchbase_bin_dir,
                                                'couchbase-cli')

    @property
    def couchbase_bin_dir(self):
        return '/opt/couchbase/bin'

    def _install_couchbase(self, packages):
        """
        Install the Couchbase Server.
        """
        LOG.debug('Installing Couchbase Server. Creating %s' %
                  system.COUCHBASE_CONF_DIR)
        operating_system.create_directory(system.COUCHBASE_CONF_DIR,
                                          as_root=True)
        pkg_opts = {}
        packager.pkg_install(packages, pkg_opts, 1200)
        self.start_db()
        LOG.debug('Finished installing Couchbase Server.')

    def _enable_db_on_boot(self):
        """
        Enables Couchbase Server on boot.
        """
        LOG.info(_('Enabling Couchbase Server on boot.'))
        try:
            couchbase_service = operating_system.service_discovery(
                system.SERVICE_CANDIDATES)
            utils.execute_with_timeout(
                couchbase_service['cmd_enable'], shell=True)
        except KeyError:
            raise RuntimeError(_(
                "Command to enable Couchbase Server on boot not found."))

    def _disable_db_on_boot(self):
        LOG.debug("Disabling Couchbase Server on boot.")
        try:
            couchbase_service = operating_system.service_discovery(
                system.SERVICE_CANDIDATES)
            utils.execute_with_timeout(
                couchbase_service['cmd_disable'], shell=True)
        except KeyError:
            raise RuntimeError(
                "Command to disable Couchbase Server on boot not found.")

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        """
        Stops Couchbase Server on the trove instance.
        """
        LOG.debug('Stopping Couchbase Server.')
        if do_not_start_on_reboot:
            self._disable_db_on_boot()

        try:
            couchbase_service = operating_system.service_discovery(
                system.SERVICE_CANDIDATES)
            utils.execute_with_timeout(
                couchbase_service['cmd_stop'], shell=True)
        except KeyError:
            raise RuntimeError("Command to stop Couchbase Server not found.")

        if not self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.SHUTDOWN,
                self.state_change_wait_time, update_db):
            LOG.error(_('Could not stop Couchbase Server.'))
            self.status.end_restart()
            raise RuntimeError(_("Could not stop Couchbase Server."))

    def restart(self):
        LOG.info(_("Restarting Couchbase Server."))
        try:
            self.status.begin_restart()
            self.stop_db()
            self.start_db()
        finally:
            self.status.end_restart()

    def start_db(self, update_db=False):
        """
        Start the Couchbase Server.
        """
        LOG.info(_("Starting Couchbase Server."))

        self._enable_db_on_boot()
        try:
            couchbase_service = operating_system.service_discovery(
                system.SERVICE_CANDIDATES)
            utils.execute_with_timeout(
                couchbase_service['cmd_start'], shell=True)
        except exception.ProcessExecutionError:
            pass
        except KeyError:
            raise RuntimeError("Command to start Couchbase Server not found.")

        if not self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.RUNNING,
                self.state_change_wait_time, update_db):
            LOG.error(_("Start up of Couchbase Server failed."))
            try:
                utils.execute_with_timeout(system.cmd_kill)
            except exception.ProcessExecutionError:
                LOG.exception(_('Error killing Couchbase start command.'))
            self.status.end_restart()
            raise RuntimeError("Could not start Couchbase Server")

    def enable_root(self, root_password=None):
        return CouchbaseRootAccess.enable_root(root_password)

    def start_db_with_conf_changes(self, config_contents):
        self.start_db(update_db=True)

    def reset_configuration(self, configuration):
        pass


class CouchbaseAppStatus(service.BaseDbStatus):
    """
    Handles all of the status updating for the couchbase guest agent.
    """

    def _get_actual_db_status(self):
        self.ip_address = netutils.get_my_ipv4()
        pwd = None
        try:
            pwd = CouchbaseRootAccess.get_password()
            return self._get_status_from_couchbase(pwd)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error getting the Couchbase status."))

        return rd_instance.ServiceStatuses.SHUTDOWN

    def _get_status_from_couchbase(self, pwd):
        out, err = utils.execute_with_timeout(
            (system.cmd_couchbase_status %
             {'IP': self.ip_address, 'PWD': pwd}),
            shell=True)
        server_stats = json.loads(out)
        if not err and server_stats["clusterMembership"] == "active":
            return rd_instance.ServiceStatuses.RUNNING
        else:
            return rd_instance.ServiceStatuses.SHUTDOWN


class CouchbaseRootAccess(object):

    # TODO(pmalik): This should be obtained from the CouchbaseRootUser model.
    DEFAULT_ADMIN_NAME = 'root'
    DEFAULT_ADMIN_PASSWORD = 'password'

    @classmethod
    def enable_root(cls, root_password=None):
        admin = models.CouchbaseRootUser()
        if root_password:
            CouchbaseRootAccess().write_password_to_file(root_password)
        else:
            CouchbaseRootAccess().set_password(admin.password)
        return admin.serialize()

    def set_password(self, root_password):
        self.ip_address = netutils.get_my_ipv4()
        child = pexpect.spawn(system.cmd_reset_pwd % {'IP': self.ip_address})
        try:
            child.expect('.*password.*')
            child.sendline(root_password)
            child.expect('.*(yes/no).*')
            child.sendline('yes')
            child.expect('.*successfully.*')
        except pexpect.TIMEOUT:
            child.delayafterclose = 1
            child.delayafterterminate = 1
            try:
                child.close(force=True)
            except pexpect.ExceptionPexpect:
                # Close fails to terminate a sudo process on some OSes.
                subprocess.call(['sudo', 'kill', str(child.pid)])

        self.write_password_to_file(root_password)

    def write_password_to_file(self, root_password):
        operating_system.create_directory(system.COUCHBASE_CONF_DIR,
                                          as_root=True)
        try:
            tempfd, tempname = tempfile.mkstemp()
            os.fchmod(tempfd, stat.S_IRUSR | stat.S_IWUSR)
            os.write(tempfd, root_password)
            os.fchmod(tempfd, stat.S_IRUSR)
            os.close(tempfd)
        except OSError as err:
            message = _("An error occurred in saving password "
                        "(%(errno)s). %(strerror)s.") % {
                            "errno": err.errno,
                            "strerror": err.strerror}
            LOG.exception(message)
            raise RuntimeError(message)

        operating_system.move(tempname, system.pwd_file, as_root=True)

    @classmethod
    def get_password(cls):
        pwd = cls.DEFAULT_ADMIN_PASSWORD
        if os.path.exists(system.pwd_file):
            with open(system.pwd_file) as file:
                pwd = file.readline().strip()
        return pwd

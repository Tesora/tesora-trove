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
import psutil
import re
import stat
import tempfile

from oslo_log import log as logging
from oslo_utils import netutils
import pexpect

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common import pagination
from trove.common.stream_codecs import StringConverter
from trove.common import utils as utils
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
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

    # TODO(pmalik): This should be obtained from the CouchbaseRootUser model.
    DEFAULT_ADMIN_NAME = 'root'
    DEFAULT_ADMIN_PASSWORD = 'password'

    MIN_RAMSIZE_QUOTA_MB = 256
    _ADMIN_USER = 'root'  # TODO(pmalik): Should be 'os_admin'.

    COUCHBASE_KILL_CMD = 'pkill -u couchbaso tee -a'

    SECRET_KEY_FILE = 'secret_key'

    @property
    def couchbase_owner(self):
        return 'couchbase'

    @property
    def service_candidates(self):
        return ["couchbase-server"]

    @property
    def couchbase_pwd_file(self):
        return guestagent_utils.build_file_path(self.couchbase_conf_dir,
                                                self.SECRET_KEY_FILE)

    @property
    def couchbase_conf_dir(self):
        return '/etc/couchbase'

    @property
    def couchbase_opt_etc_dir(self):
        return '/opt/couchbase/etc'

    def __init__(self, state_change_wait_time=None):
        """
        Sets default status and state_change_wait_time
        """
        if state_change_wait_time:
            self.state_change_wait_time = state_change_wait_time
        else:
            self.state_change_wait_time = CONF.state_change_wait_time
        self.status = CouchbaseAppStatus(self.build_admin())

    @property
    def available_ram_mb(self):
        return int(psutil.virtual_memory().total / 1048576)

    def build_admin(self):
        return CouchbaseAdmin(self.get_cluster_admin())

    def install_if_needed(self, packages):
        """
        Install couchbase if needed, do nothing if it is already installed.
        """
        LOG.info(_('Preparing Guest as Couchbase Server.'))
        if not packager.pkg_is_installed(packages):
            LOG.debug('Installing Couchbase.')
            self._install_couchbase(packages)

    def initialize_node(self):
        ip_address = netutils.get_my_ipv4()
        mount_point = CONF.couchbase.mount_point
        self.build_admin().run_node_init(mount_point, mount_point, ip_address)

    def apply_post_restore_updates(self, backup_info):
        self.status = CouchbaseAppStatus(self.build_admin())

    def initialize_cluster(self):
        """Initialize this node as cluster.
        """
        self.build_admin().run_cluster_init(self.ramsize_quota_mb)

    def get_cluster_admin(self):
        cluster_password = self.get_password()
        return models.CouchbaseUser(self._ADMIN_USER, cluster_password)

    def secure(self, password=None, initialize=True):
        self.store_admin_credentials(password=password)
        if initialize:
            self.initialize_cluster()
        # Update the internal status with the new user.
        self.status = CouchbaseAppStatus(self.build_admin())

    @property
    def ramsize_quota_mb(self):
        ramsize_quota_pc = CONF.couchbase.cluster_ramsize_pc / 100.0
        return max(int(ramsize_quota_pc * self.available_ram_mb),
                   self.MIN_RAMSIZE_QUOTA_MB)

    def init_storage_structure(self, mount_point):
        try:
            operating_system.create_directory(
                mount_point, user=self.couchbase_owner,
                group=self.couchbase_owner, as_root=True)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error while initiating storage structure."))

    def _install_couchbase(self, packages):
        """
        Install the Couchbase Server.
        """
        LOG.debug('Installing Couchbase Server. Creating %s' %
                  self.couchbase_conf_dir)
        operating_system.create_directory(self.couchbase_conf_dir,
                                          as_root=True)
        pkg_opts = {}
        packager.pkg_install(packages, pkg_opts, 1200)
        self.start_db()
        LOG.debug('Finished installing Couchbase Server.')

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        self.status.stop_db_service(
            self.service_candidates, self.state_change_wait_time,
            disable_on_boot=do_not_start_on_reboot, update_db=update_db)

    def restart(self):
        self.status.restart_db_service(
            self.service_candidates, self.state_change_wait_time)

    def start_db(self, update_db=False):
        self.status.start_db_service(
            self.service_candidates, self.state_change_wait_time,
            enable_on_boot=True, update_db=update_db)

    def enable_root(self, root_password=None):
        self.status.begin_restart()
        try:
            admin = self.reset_admin_credentials(password=root_password)
            # Update the internal status with the new user.
            self.status = CouchbaseAppStatus(self.build_admin())
            return admin.serialize()
        finally:
            self.status.end_restart()

    def start_db_with_conf_changes(self, config_contents):
        self.start_db(update_db=True)

    def reset_configuration(self, configuration):
        pass

    def rebalance_cluster(self, added_nodes=None, removed_nodes=None):
        self.build_admin().run_rebalance(added_nodes, removed_nodes)

    def get_cluster_rebalance_status(self):
        """Return whether rebalancing is currently running.
        """
        return self.build_admin().get_cluster_rebalance_status()

    def store_admin_credentials(self, password=None):
        admin = models.CouchbaseRootUser(password=password)
        self._write_password_to_file(admin.password)
        return admin

    def reset_admin_credentials(self, password=None):
        admin = models.CouchbaseRootUser(password=password)
        self._set_password(admin.password)
        return admin

    def _set_password(self, root_password):
        self.build_admin().reset_root_password(root_password)
        self._write_password_to_file(root_password)

    def _write_password_to_file(self, root_password):
        operating_system.create_directory(self.couchbase_conf_dir,
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

        operating_system.move(tempname, self.couchbase_pwd_file, as_root=True)

    def get_password(self):
        pwd = self.DEFAULT_ADMIN_PASSWORD
        if os.path.exists(self.couchbase_pwd_file):
            with open(self.couchbase_pwd_file) as file:
                pwd = file.readline().strip()
        return pwd

    def save_files_pre_upgrade(self, mount_point):
        LOG.debug('Saving files pre-upgrade.')
        mnt_opt_etc = os.path.join(mount_point, 'save_opt_etc')
        mnt_etc = os.path.join(mount_point, 'save_etc')
        for save_dir in [mnt_opt_etc, mnt_etc]:
            operating_system.remove(save_dir, force=True, as_root=True)
        operating_system.copy(self.couchbase_opt_etc_dir, mnt_opt_etc,
                              preserve=True, recursive=True, as_root=True)
        operating_system.copy(self.couchbase_conf_dir,
                              mnt_etc, preserve=True, recursive=True,
                              as_root=True)
        return {'save_opt_etc': mnt_opt_etc,
                'save_etc': mnt_etc}

    def restore_files_post_upgrade(self, upgrade_info):
        LOG.debug('Restoring files post-upgrade.')
        operating_system.copy('%s/.' % upgrade_info['save_opt_etc'],
                              self.couchbase_opt_etc_dir,
                              preserve=True, recursive=True,
                              force=True, as_root=True)
        operating_system.copy(upgrade_info['save_etc'],
                              self.couchbase_conf_dir,
                              preserve=True, force=True, as_root=True)
        for save_dir in [upgrade_info['save_opt_etc'],
                         upgrade_info['save_etc']]:
            operating_system.remove(save_dir, force=True, as_root=True)


class CouchbaseAdmin(object):

    def __init__(self, user):
        self._user = user
        self._http_client_port = CONF.couchbase.couchbase_port

    def run_node_init(self, data_path, index_path, hostname):
        LOG.debug("Configuring node-specific parameters.")
        self._run_couchbase_command(
            'node-init', {'node-init-data-path': data_path,
                          'node-init-index-path': index_path,
                          'node-init-hostname': hostname})

    def run_cluster_init(self, ramsize_quota_mb):
        LOG.debug("Configuring cluster parameters.")
        self._run_couchbase_command(
            'cluster-init', {'cluster-init-username': self._user.name,
                             'cluster-init-password': self._user.password,
                             'cluster-init-port': self._http_client_port,
                             'cluster-ramsize': ramsize_quota_mb})

    def run_rebalance(self, added_nodes, removed_nodes):
        """Rebalance the cluster by adding and/or removing nodes.
        Rebalance moves the data around the cluster so that the data is
        distributed across the entire cluster.
        The rebalancing process can take place while the cluster is running and
        servicing requests.
        Clients using the cluster read and write to the existing structure
        with the data being moved in the background among nodes.
        """

        LOG.debug("Rebalancing the cluster.")
        options = {}
        if added_nodes:
            for ip in added_nodes:
                options.update({'server-add': ip,
                                'server-add-username': self._user.name,
                                'server-add-password': self._user.password})
        if removed_nodes:
            options.update({'server-remove': ip for ip in removed_nodes})

        if options:
            self._run_couchbase_command('rebalance', options)
        else:
            LOG.info(_("No changes to the topology, skipping rebalance."))

    def get_cluster_rebalance_status(self):
        """Return whether rebalancing is currently running.
        """
        # Status message: "(u'<status name>', <status message>)\n"
        status, _ = self._run_couchbase_command('rebalance-status')
        status_tokens = StringConverter({}).to_objects(status)
        LOG.debug("Current rebalance status: %s (%s)" % status_tokens)
        return status_tokens[0] not in ('none', 'notRunning')

    def run_bucket_create(self, bucket_name, bucket_password, bucket_port,
                          bucket_type, bucket_ramsize_quota_mb,
                          enable_index_replica, eviction_policy,
                          replica_count):
        LOG.debug("Creating a new bucket: %s" % bucket_name)
        self._run_couchbase_command(
            'bucket-create', {'bucket': bucket_name,
                              'bucket-type': bucket_type,
                              'bucket-password': bucket_password,
                              'bucket-port': bucket_port,
                              'bucket-ramsize': bucket_ramsize_quota_mb,
                              'enable-flush': 0,
                              'enable-index-replica': enable_index_replica,
                              'bucket-eviction-policy': eviction_policy,
                              'bucket-replica': replica_count,
                              'wait': None,
                              'force': None})

    def run_bucket_edit(self, bucket_name, bucket_password, bucket_port,
                        bucket_type, bucket_ramsize_quota_mb,
                        enable_index_replica, eviction_policy, replica_count):
        LOG.debug("Modifying bucket: %s" % bucket_name)
        self._run_couchbase_command(
            'bucket-edit', {'bucket': bucket_name,
                            'bucket-type': bucket_type,
                            'bucket-password': bucket_password,
                            'bucket-port': bucket_port,
                            'bucket-ramsize': bucket_ramsize_quota_mb,
                            'enable-flush': 0,
                            'enable-index-replica': enable_index_replica,
                            'bucket-eviction-policy': eviction_policy,
                            'bucket-replica': replica_count,
                            'wait': None,
                            'force': None})

    def run_bucket_list(self):
        LOG.debug("Retrieving the list of buckets.")
        bucket_list, _ = self._run_couchbase_command('bucket-list')
        return bucket_list

    def run_bucket_delete(self, bucket_name):
        LOG.debug("Deleting bucket: %s" % bucket_name)
        self._run_couchbase_command('bucket-delete', {'bucket': bucket_name})

    def run_server_info(self):
        out, _ = self._run_couchbase_command('server-info')
        return json.loads(out)

    def run_server_list(self):
        out, _ = self._run_couchbase_command('server-list')
        return out.splitlines()

    def _run_couchbase_command(self, cmd, options=None, **kwargs):
        """Execute a couchbase-cli command on this node.
        """
        # couchbase-cli COMMAND -c [host]:[port] -u user -p password [options]

        host_and_port = 'localhost:%d' % self._http_client_port
        args = self._build_command_options(options)
        cmd_tokens = [self.couchbase_cli_bin, cmd,
                      '-c', host_and_port,
                      '-u', self._user.name,
                      '-p', self._user.password] + args
        return utils.execute(' '.join(cmd_tokens), shell=True, **kwargs)

    def _build_command_options(self, options):
        cmd_opts = []
        if options:
            for name, value in options.items():
                tokens = [name]
                if value is not None:
                    tokens.append(str(value))
                cmd_opts.append('--%s' % '='.join(tokens))

        return cmd_opts

    @property
    def couchbase_cli_bin(self):
        return guestagent_utils.build_file_path(self.couchbase_bin_dir,
                                                'couchbase-cli')

    @property
    def couchbase_bin_dir(self):
        return '/opt/couchbase/bin'

    def create_user(self, context, users):
        if len(users) > 1:
            raise exception.UnprocessableEntity(
                _("Only a single user can be created on the instance."))

        user_list = self._list_buckets()
        if user_list:
            raise exception.UnprocessableEntity(
                _("There already is a user on this instance: %s")
                % user_list[0].name)

        self._create_bucket(models.CouchbaseUser.deserialize_user(users[0]))

    def _create_bucket(self, user):
        bucket_ramsize_quota_mb = self.get_memory_quota_mb()
        num_cluster_nodes = self.get_num_cluster_nodes()
        replica_count = min(CONF.couchbase.default_replica_count,
                            num_cluster_nodes - 1)
        self.run_bucket_create(
            user.name,
            user.password,
            CONF.couchbase.bucket_port,
            CONF.couchbase.bucket_type,
            bucket_ramsize_quota_mb,
            int(CONF.couchbase.enable_index_replica),
            CONF.couchbase.eviction_policy,
            replica_count)

    def get_memory_quota_mb(self):
        server_info = self.run_server_info()
        return server_info['memoryQuota']

    def get_num_cluster_nodes(self):
        server_list = self.run_server_list()
        return len(server_list)

    def delete_user(self, context, user):
        self._delete_bucket(models.CassandraUser.deserialize_user(user))

    def _delete_bucket(self, user):
        self.run_bucket_delete(user.name)

    def get_user(self, context, username, hostname):
        user = self._find_bucket(username)
        return user.serialize() if user is not None else None

    def _find_bucket(self, username):
        return next((user for user in self._list_buckets()
                     if user.name == username), None)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        users = [user.serialize() for user in self._list_buckets()]
        return pagination.paginate_list(users, limit, marker, include_marker)

    def _list_buckets(self):
        bucket_list = self.run_bucket_list()
        return [models.CouchbaseUser(item)
                for item in self._parse_bucket_list(bucket_list)]

    def _parse_bucket_list(self, bucket_list):
        buckets = dict()
        if bucket_list:
            bucket_info = dict()
            for item in bucket_list.splitlines():
                if not re.match('^\s.*$', item):
                    bucket_info = dict()
                    buckets.update({item.strip(): bucket_info})
                else:
                    key, value = item.split(':', 1)
                    bucket_info.update({key.strip(): value.lstrip()})

        return buckets

    def change_passwords(self, context, users):
        if len(users) > 1:
            raise exception.UnprocessableEntity(
                _("There is only one user on the instance."))

        self._edit_bucket(models.CouchbaseUser.deserialize_user(users[0]))

    def _edit_bucket(self, user):
        # When changing the active bucket configuration,
        # specify all existing configuration parameters to avoid having them
        # reset to defaults.
        buckets = self._parse_bucket_list(self.run_bucket_list())
        bucket = buckets[user.name]

        bucket_ramsize_quota_mb = str(int(bucket['ramQuota']) / 1048576)
        replica_count = int(bucket['numReplicas'])
        bucket_type = bucket['bucketType']
        self.run_bucket_edit(
            user.name,
            user.password,
            CONF.couchbase.bucket_port,
            bucket_type,
            bucket_ramsize_quota_mb,
            int(CONF.couchbase.enable_index_replica),
            CONF.couchbase.eviction_policy,
            replica_count)

    def update_attributes(self, context, username, hostname, user_attrs):
        new_name = user_attrs.get('name')
        if new_name:
            raise exception.UnprocessableEntity(
                _("Users cannot be renamed."))

        new_password = user_attrs.get('password')
        if new_password:
            user = models.CouchbaseUser(username, password=new_password)
            self._edit_bucket(user)

    def reset_root_password(self, new_password):
        host_and_port = 'localhost:%d' % self._http_client_port
        cmd = guestagent_utils.build_file_path(self.couchbase_bin_dir,
                                               'cbreset_password')
        cmd_tokens = ['sudo', cmd, host_and_port]

        child = pexpect.spawn(' '.join(cmd_tokens))
        try:
            child.expect('.*password.*')
            child.sendline(new_password)
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
                utils.execute_with_timeout(
                    'kill', str(child.pid),
                    run_as_root=True, root_helper='sudo')


class CouchbaseAppStatus(service.BaseDbStatus):
    """
    Handles all of the status updating for the couchbase guest agent.
    """

    def __init__(self, admin):
        super(CouchbaseAppStatus, self).__init__()
        self._admin = admin

    def _get_actual_db_status(self):
        try:
            server_info = self._admin.run_server_info()
            if server_info["clusterMembership"] == "active":
                return rd_instance.ServiceStatuses.RUNNING
        except Exception:
            LOG.exception(_("Error getting Couchbase status."))

        return rd_instance.ServiceStatuses.SHUTDOWN

    def cleanup_stalled_db_services(self):
        utils.execute_with_timeout(CouchbaseApp.COUCHBASE_KILL_CMD,
                                   run_as_root=True, root_helper='sudo')

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

import collections
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
from trove.guestagent import dbaas as dbaas_utils
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
    def upgrade_copy_info(self):
        return {
            'save_etc': {
                'is_dir': True,
                'path': self.couchbase_conf_dir},
            'save_opt_etc': {
                'is_dir': True,
                'path': '/opt/couchbase/etc'},
            'save_opt_config_dat': {
                'is_dir': False,
                'path': '/opt/couchbase/var/lib/couchbase/config/config.dat'},
            'save_opt_ip_start': {
                'is_dir': False,
                'path': '/opt/couchbase/var/lib/couchbase/ip_start'},
        }

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

    def initialize_cluster(self, node_info=None):
        """Initialize this node as cluster.
        """
        self.build_admin().run_cluster_init(node_info, self.ramsize_quota_mb)

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

    def rebalance_cluster(self, add_node_info=None, remove_node_info=None):
        return self.build_admin().run_rebalance(
            add_node_info, remove_node_info)

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
        for save_dir, save_dir_info in self.upgrade_copy_info.items():
            is_dir = save_dir_info['is_dir']
            from_path = save_dir_info['path']
            to_path = os.path.join(mount_point, save_dir)
            operating_system.remove(to_path, force=True, as_root=True)
            operating_system.copy(from_path, to_path,
                                  preserve=True, recursive=is_dir,
                                  as_root=True)
        return {'copy_info': self.upgrade_copy_info}

    def restore_files_post_upgrade(self, upgrade_info):
        LOG.debug('Restoring files post-upgrade.')
        mount_point = upgrade_info['mount_point']
        upgrade_copy_info = upgrade_info['copy_info']
        for save_dir, save_dir_info in upgrade_copy_info.items():
            is_dir = save_dir_info['is_dir']
            from_path = os.path.join(mount_point, save_dir)
            files = os.path.join(from_path, '.') if is_dir else from_path
            operating_system.copy(files, save_dir_info['path'],
                                  preserve=True, recursive=is_dir,
                                  force=True, as_root=True)
            operating_system.remove(from_path, force=True, as_root=True)
        self.status.set_ready()


class CouchbaseAdmin(object):

    def __init__(self, user):
        self._user = user
        self._http_client_port = CONF.couchbase.couchbase_port

    def run_node_init(self, data_path, index_path, hostname):
        LOG.debug("Configuring node-specific parameters.")
        self._run_couchbase_command(
            'node-init', self.get_node_init_options(
                data_path, index_path, hostname))

    def get_node_init_options(self, data_path, index_path, hostname):
        return {
            'node-init-data-path': data_path,
            'node-init-index-path': index_path,
            'node-init-hostname': hostname
        }

    def run_cluster_init(self, node_info, ramsize_quota_mb):
        LOG.debug("Configuring cluster parameters.")
        self._run_couchbase_command(
            'cluster-init', self.get_cluster_init_options(
                node_info, ramsize_quota_mb))

    def get_cluster_init_options(self, node_info, ramsize_quota_mb):
        return {
            'cluster-init-username': self._user.name,
            'cluster-init-password': self._user.password,
            'cluster-init-port': self._http_client_port,
            'cluster-ramsize': ramsize_quota_mb,
        }

    def run_rebalance(self, add_node_info, remove_node_info):
        """Rebalance the cluster by adding and/or removing nodes.
        Rebalance moves the data around the cluster so that the data is
        distributed across the entire cluster.
        The rebalancing process can take place while the cluster is running and
        servicing requests.
        Clients using the cluster read and write to the existing structure
        with the data being moved in the background among nodes.
        """

        LOG.debug("Rebalancing the cluster.")
        success = True
        message = None
        options = []
        if add_node_info:
            options.extend(self.get_cluster_add_options(add_node_info))
        if remove_node_info:
            options.extend(self.get_cluster_remove_options(remove_node_info))

        if options:
            try:
                self._run_couchbase_command('rebalance', options)
            except exception.ProcessExecutionError as ex:
                success = False
                message = str(ex.stdout)
                if ex.stderr:
                    message += ": %s" % str(ex.stderr)
        else:
            LOG.info(_("No changes to the topology, skipping rebalance."))

        return success, message

    def get_cluster_add_options(self, node_info):

        add_options = []
        for node in node_info:
            options = collections.OrderedDict()
            options['server-add'] = node['host']
            options['server-add-username'] = self._user.name
            options['server-add-password'] = self._user.password
            add_options.append(options)

        return add_options

    def get_cluster_remove_options(self, node_info):

        return [{'server-remove': [ni['host'] for ni in node_info]}]

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
                          replica_count, bucket_priority):
        LOG.debug("Creating a new bucket: %s" % bucket_name)
        options = {'bucket': bucket_name,
                   'bucket-type': bucket_type,
                   'bucket-port': bucket_port,
                   'bucket-ramsize': bucket_ramsize_quota_mb,
                   'enable-flush': 0,
                   'enable-index-replica': enable_index_replica,
                   'bucket-eviction-policy': eviction_policy,
                   'bucket-replica': replica_count,
                   'bucket-priority': bucket_priority,
                   'wait': None,
                   'force': None}
        if bucket_password is not None:
            options.update({'bucket-password': bucket_password})
        self._run_couchbase_command('bucket-create', options)

    def run_bucket_edit(self, bucket_name, bucket_password, bucket_port,
                        bucket_type, bucket_ramsize_quota_mb,
                        enable_index_replica, eviction_policy, replica_count,
                        bucket_priority):
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
                            'bucket-priority': bucket_priority,
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
        """Build options for a Couchbase CLI command.
        Couchbase options take form of '--name=value' or '--name'.

        The options must be provided in a Python dict where the key
        is the 'name'.
        The value is the actual option value or None in case of simple flags.
        The value may also be a list in which case the option gets repeated
        for each value.

        The options argument may be a single dict or a list of dicts
        in which the options from each dict get appended iteratively.

        :param options:          The option name-value pairs.
        :type options:           dict or list-of-dicts
        """

        def append_option_group(opt_group, cmd_opts):
            for name, value in opt_group.items():
                if utils.is_collection(value):
                    for item in value:
                        append_option(name, item, cmd_opts)
                else:
                    append_option(name, value, cmd_opts)

        def append_option(name, value, cmd_opts):
            tokens = [name]
            if value is not None:
                tokens.append(str(value))
            cmd_opts.append('--%s' % '='.join(tokens))

        cmd_opts = []
        if options:
            if isinstance(options, (list, tuple)):
                for opt_group in options:
                    append_option_group(opt_group, cmd_opts)
            else:
                append_option_group(options, cmd_opts)

        return cmd_opts

    @property
    def couchbase_cli_bin(self):
        return guestagent_utils.build_file_path(self.couchbase_bin_dir,
                                                'couchbase-cli')

    @property
    def couchbase_bin_dir(self):
        return '/opt/couchbase/bin'

    def create_user(self, context, users):
        read_only_users = []
        buckets = []
        user_models = [models.CouchbaseUser.deserialize_user(user)
                       for user in users]
        for item in user_models:
            if item.roles and item.roles[0].get('name') == 'read-only':
                read_only_users.append(item)
            else:
                buckets.append(item)

        if read_only_users:
            self._set_read_only_user(read_only_users[0])

        self.create_buckets(buckets)

    def create_buckets(self, users):
        self._compute_bucket_mem_allocations(users)
        for user in users:
            self._create_bucket(user)

    def _compute_bucket_mem_allocations(self, users):
        """Compute memory allocation for Couchbase users.

        Trove will use whatever is set for every given user.
        It will fail if the total exceeds the available memory.
        Users without memory quota will evenly split the remaining available
        memory.
        """

        users_without_quota = set()
        user_alloc_mb = 0
        for user in users:
            if user.bucket_ramsize_mb is not None:
                user_alloc_mb += user.bucket_ramsize_mb
            else:
                users_without_quota.add(user)

        total_quota_mb = self.get_memory_quota_mb()
        used_quota_mb = self.get_used_quota_mb()
        available_quota_mb = total_quota_mb - used_quota_mb
        available_for_auto_alloc_mb = available_quota_mb - user_alloc_mb
        num_users_without_quota = len(users_without_quota)
        auto_alloc_per_bucket_mb = (
            available_for_auto_alloc_mb / max(num_users_without_quota, 1))

        if ((available_for_auto_alloc_mb < 0) or
            (num_users_without_quota > 0 and
             auto_alloc_per_bucket_mb <
             models.CouchbaseUser.MIN_BUCKET_RAMSIZE_MB)):
            required = user_alloc_mb - available_quota_mb
            raise exception.TroveError(
                _("Not enough memory for Couchbase buckets. "
                  "Additional %dMB is required.") % required)

        for user in users_without_quota:
            user.bucket_ramsize_mb = auto_alloc_per_bucket_mb

        return users

    def _set_read_only_user(self, user):
        LOG.debug("Setting the read-only user: %s" % user.name)
        options = {'set': None,
                   'ro-username': user.name}
        if user.password:
            options['ro-password'] = user.password
        self._run_couchbase_command('user-manage', options)

    def _get_read_only_user(self):
        LOG.debug("Getting the read-only user.")
        options = {'list': None}
        name = self._run_couchbase_command('user-manage', options)[0]
        if 'Object Not Found' not in name:
            return models.CouchbaseUser(name=name.strip(),
                                        roles={'name': 'read-only'})

        return None

    def _delete_read_only_user(self):
        LOG.debug("Deleting the read-only user.")
        options = {'delete': None}
        self._run_couchbase_command('user-manage', options)

    def _create_bucket(self, user):
        bucket_ramsize_quota_mb = (user.bucket_ramsize_mb or
                                   self.get_memory_quota_mb())
        num_cluster_nodes = self.get_num_cluster_nodes()
        replica_count = (user.bucket_replica_count or
                         min(CONF.couchbase.default_replica_count,
                             num_cluster_nodes - 1))
        eviction_policy = (user.bucket_eviction_policy or
                           CONF.couchbase.eviction_policy)
        enable_index_replica = (user.enable_index_replica or
                                int(CONF.couchbase.enable_index_replica))
        bucket_priority = (user.bucket_priority or
                           CONF.couchbase.bucket_priority)
        bucket_port = (user.bucket_port or
                       CONF.couchbase.bucket_port)

        self.run_bucket_create(
            user.name,
            user.password,
            bucket_port,
            CONF.couchbase.bucket_type,
            bucket_ramsize_quota_mb,
            enable_index_replica,
            eviction_policy,
            replica_count,
            bucket_priority)

    def get_memory_quota_mb(self):
        server_info = self.run_server_info()
        return server_info['memoryQuota']

    def get_used_quota_mb(self):
        server_info = self.run_server_info()
        ram_info = server_info['storageTotals']['ram']
        return ram_info['quotaUsedPerNode'] / 1048576

    def get_num_cluster_nodes(self):
        server_list = self.run_server_list()
        return len(server_list)

    def delete_user(self, context, user):
        couchbase_user = models.CouchbaseUser.deserialize_user(user)
        ro_user = self._get_read_only_user()
        if ro_user and ro_user.name == couchbase_user.name:
            self._delete_read_only_user()
        else:
            self._delete_bucket(couchbase_user)

    def _delete_bucket(self, user):
        self.run_bucket_delete(user.name)

    def get_user(self, context, username, hostname):
        ro_user = self._get_read_only_user()
        if ro_user and ro_user.name == username:
            return ro_user.serialize()
        user = self._find_bucket(username)
        return user.serialize() if user is not None else None

    def _find_bucket(self, username):
        return next((user for user in self._list_buckets()
                     if user.name == username), None)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        users = [user.serialize() for user in self._list_buckets()]
        ro_user = self._get_read_only_user()
        if ro_user:
            users.append(ro_user.serialize())
        return pagination.paginate_list(users, limit, marker, include_marker)

    def _list_buckets(self):
        bucket_list = self.run_bucket_list()
        buckets = []
        for name, info in self._parse_bucket_list(bucket_list).items():
            bucket_ramsize_quota_mb = int(info['ramQuota']) / 1048576
            bucket_replica_count = int(info['numReplicas'])
            used_ram_mb = dbaas_utils.to_mb(float(info['ramUsed']))
            buckets.append(models.CouchbaseUser(
                name,
                bucket_ramsize_mb=bucket_ramsize_quota_mb,
                bucket_replica_count=bucket_replica_count,
                used_ram_mb=used_ram_mb))
        return buckets

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

    def _block_read_only_user_edit(self, username):
        ro_user = self._get_read_only_user()
        if ro_user and ro_user.name == username:
            raise exception.BadRequest(_(
                "Cannot edit the read-only user. Delete it before creating "
                "a new one."))

    def change_passwords(self, context, users):
        user_models = [models.CouchbaseUser.deserialize_user(users)
                       for user in users]

        for item in user_models:
            self._block_read_only_user_edit(item.name)

        for item in user_models:
            self._edit_bucket(item)

    def _edit_bucket(self, user):
        # When changing the active bucket configuration,
        # specify all existing configuration parameters to avoid having them
        # reset to defaults.

        current = self._find_bucket(user.name)

        bucket_ramsize_quota_mb = (user.bucket_ramsize_mb or
                                   current.bucket_ramsize_mb)
        replica_count = (user.bucket_replica_count or
                         current.bucket_replica_count)
        eviction_policy = (user.bucket_eviction_policy or
                           CONF.couchbase.eviction_policy)
        enable_index_replica = (user.enable_index_replica or
                                int(CONF.couchbase.enable_index_replica))
        bucket_priority = (user.bucket_priority or
                           CONF.couchbase.bucket_priority)
        bucket_port = (user.bucket_port or
                       CONF.couchbase.bucket_port)

        self.run_bucket_edit(
            user.name,
            user.password,
            bucket_port,
            CONF.couchbase.bucket_type,
            bucket_ramsize_quota_mb,
            enable_index_replica,
            eviction_policy,
            replica_count,
            bucket_priority)

    def update_attributes(self, context, username, hostname, user_attrs):
        self._block_read_only_user_edit(username)

        new_name = user_attrs.get('name')
        if new_name:
            raise exception.UnprocessableEntity(
                _("Users cannot be renamed."))

        user = models.CouchbaseUser(
            username,
            password=user_attrs.get('password'),
            bucket_ramsize_mb=user_attrs.get('bucket_ramsize'),
            bucket_replica_count=user_attrs.get('bucket_replica'),
            enable_index_replica=user_attrs.get('enable_index_replica'),
            bucket_eviction_policy=user_attrs.get('bucket_eviction_policy'),
            bucket_priority=user_attrs.get('bucket_priority'))

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

#  Copyright 2013 Mirantis Inc.
#  All Rights Reserved.
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
import stat

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import OperationTimedOut

from oslo_log import log as logging
from oslo_utils import netutils

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common import pagination
from trove.common.stream_codecs import IniCodec
from trove.common.stream_codecs import SafeYamlCodec
from trove.common import utils
from trove.guestagent.common.configuration import ConfigurationManager
from trove.guestagent.common.configuration import OneFileOverrideStrategy
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore import service
from trove.guestagent.db import models
from trove.guestagent import pkg


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

packager = pkg.Package()


class CassandraApp(object):
    """Prepares DBaaS on a Guest container."""

    _ADMIN_USER = 'os_admin'

    _CONF_AUTH_SEC = 'authentication'
    _CONF_USR_KEY = 'username'
    _CONF_PWD_KEY = 'password'
    _CONF_DIR_MODS = stat.S_IRWXU
    _CONF_FILE_MODS = stat.S_IRUSR

    CASSANDRA_KILL_CMD = "sudo killall java  || true"

    def __init__(self):
        self.state_change_wait_time = CONF.state_change_wait_time
        self.status = CassandraAppStatus(self.get_current_superuser())

        revision_dir = self._init_overrides_dir()
        self.configuration_manager = ConfigurationManager(
            self.cassandra_conf,
            self.cassandra_owner, self.cassandra_owner,
            SafeYamlCodec(default_flow_style=False), requires_root=True,
            override_strategy=OneFileOverrideStrategy(revision_dir))

    def _init_overrides_dir(self):
        """Initialize a directory for configuration overrides.
        """
        revision_dir = guestagent_utils.build_file_path(
            os.path.dirname(self.cassandra_conf),
            ConfigurationManager.DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)

        if not os.path.exists(revision_dir):
            operating_system.create_directory(
                revision_dir,
                user=self.cassandra_owner, group=self.cassandra_owner,
                force=True, as_root=True)

        return revision_dir

    @property
    def service_candidates(self):
        return ['cassandra']

    @property
    def cassandra_conf(self):
        return {
            operating_system.REDHAT:
                "/etc/cassandra/default.conf/cassandra.yaml",
            operating_system.DEBIAN:
                "/etc/cassandra/cassandra.yaml",
            operating_system.SUSE:
                "/etc/cassandra/default.conf/cassandra.yaml"
        }[operating_system.get_os()]

    @property
    def cassandra_owner(self):
        return 'cassandra'

    @property
    def cassandra_data_dir(self):
        return guestagent_utils.build_file_path(
            self.cassandra_working_dir, 'data')

    @property
    def cassandra_working_dir(self):
        return "/var/lib/cassandra"

    @property
    def default_superuser_name(self):
        return "cassandra"

    @property
    def default_superuser_password(self):
        return "cassandra"

    @property
    def default_superuser_pwd_hash(self):
        # Default 'salted_hash' value for 'cassandra' user on Cassandra 2.1.
        return "$2a$10$wPEVuXBU7WE2Uwzqq3t19ObRJyoKztzC/Doyfr0VtDmVXC4GDAV3e"

    @property
    def cqlsh_conf_path(self):
        return "~/.cassandra/cqlshrc"

    def install_if_needed(self, packages):
        """Prepare the guest machine with a Cassandra server installation."""
        LOG.info(_("Preparing Guest as a Cassandra Server"))
        if not packager.pkg_is_installed(packages):
            self._install_db(packages)
        LOG.debug("Cassandra install_if_needed complete")

    def _enable_db_on_boot(self):
        operating_system.enable_service_on_boot(self.service_candidates)

    def _disable_db_on_boot(self):
        operating_system.disable_service_on_boot(self.service_candidates)

    def init_storage_structure(self, mount_point):
        try:
            operating_system.create_directory(mount_point, as_root=True)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error while initiating storage structure."))

    def start_db(self, update_db=False):
        LOG.info(_("Starting Cassandra server."))
        self._enable_db_on_boot()
        try:
            operating_system.start_service(self.service_candidates)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error starting Cassandra"))
            pass

        if not (self.status.
                wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.RUNNING,
                self.state_change_wait_time,
                update_db)):
            try:
                utils.execute_with_timeout(self.CASSANDRA_KILL_CMD,
                                           shell=True)
            except exception.ProcessExecutionError:
                LOG.exception(_("Error killing Cassandra start command."))
            self.status.end_restart()
            raise RuntimeError(_("Could not start Cassandra"))

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        if do_not_start_on_reboot:
            self._disable_db_on_boot()
        operating_system.stop_service(self.service_candidates)

        if not (self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.SHUTDOWN,
                self.state_change_wait_time, update_db)):
            LOG.error(_("Could not stop Cassandra."))
            self.status.end_restart()
            raise RuntimeError(_("Could not stop Cassandra."))

    def restart(self):
        try:
            self.status.begin_restart()
            LOG.info(_("Restarting Cassandra server."))
            self.stop_db()
            self.start_db()
        finally:
            self.status.end_restart()

    def _install_db(self, packages):
        """Install Cassandra server"""
        LOG.debug("Installing Cassandra server.")
        packager.pkg_install(packages, None, 10000)
        LOG.debug("Finished installing Cassandra server")

    def _remove_system_tables(self):
        """
        Clean up the system keyspace.

        System tables are initialized on the first boot.
        They store certain properties, such as 'cluster_name',
        that cannot be easily changed once afterwards.
        The system keyspace needs to be cleaned up first. The
        tables will be regenerated on the next startup.
        Make sure to also cleanup the commitlog and caches to avoid
        startup errors due to inconsistencies.

        The service should not be running at this point.
        """
        if self.status.is_running:
            raise RuntimeError(_("Cannot remove system tables. "
                                 "The service is still running."))

        LOG.info(_('Removing existing system tables.'))
        system_keyspace_dir = guestagent_utils.build_file_path(
            self.cassandra_data_dir, 'system')
        commitlog_file = guestagent_utils.build_file_path(
            self.cassandra_working_dir, 'commitlog')
        chaches_dir = guestagent_utils.build_file_path(
            self.cassandra_working_dir, 'saved_caches')

        operating_system.remove(system_keyspace_dir,
                                force=True, recursive=True, as_root=True)
        operating_system.remove(commitlog_file,
                                force=True, recursive=True, as_root=True)
        operating_system.remove(chaches_dir,
                                force=True, recursive=True, as_root=True)

        operating_system.create_directory(
            system_keyspace_dir,
            user=self.cassandra_owner, group=self.cassandra_owner,
            force=True, as_root=True)
        operating_system.create_directory(
            commitlog_file,
            user=self.cassandra_owner, group=self.cassandra_owner,
            force=True, as_root=True)
        operating_system.create_directory(
            chaches_dir,
            user=self.cassandra_owner, group=self.cassandra_owner,
            force=True, as_root=True)

    def _apply_post_restore_updates(self, backup_info):
        """The service should not be running at this point.

        The restored database files carry some properties over from the
        original instance that need to be updated with appropriate
        values for the new instance.
        These include:

            - Reset the 'cluster_name' property to match the new unique
              ID of this instance.
              This is to ensure that the restored instance is a part of a new
              single-node cluster rather than forming a one with the
              original node.
            - Reset the administrator's password.
              The original password from the parent instance may be
              compromised or long lost.

        A general procedure is:
            - update the configuration property with the current value
              so that the service can start up
            - reset the superuser password
            - restart the service
            - change the cluster name
            - restart the service

        :seealso: _reset_admin_password
        :seealso: change_cluster_name
        """

        if self.status.is_running:
            raise RuntimeError(_("Cannot reset the cluster name. "
                                 "The service is still running."))

        LOG.debug("Applying post-restore updates to the database.")

        try:
            # Change the 'cluster_name' property to the current in-database
            # value so that the database can start up.
            self._update_cluster_name_property(backup_info['instance_id'])

            # Reset the superuser password so that we can log-in.
            self._reset_admin_password()

            # Start the database and update the 'cluster_name' to the
            # new value.
            self.start_db(update_db=False)
            self.change_cluster_name(CONF.guest_id)
        finally:
            self.stop_db()  # Always restore the initial state of the service.

    def secure(self, update_user=None):
        """Configure the Trove administrative user.
        Update an existing user if given.
        Create a new one using the default database credentials
        otherwise and drop the built-in user when finished.
        """
        LOG.info(_('Configuring Trove superuser.'))

        current_superuser = update_user or models.CassandraUser(
            self.default_superuser_name,
            self.default_superuser_password)

        if update_user:
            os_admin = models.CassandraUser(update_user.name,
                                            utils.generate_random_password())
            CassandraAdmin(current_superuser).alter_user_password(os_admin)
        else:
            os_admin = models.CassandraUser(self._ADMIN_USER,
                                            utils.generate_random_password())
            CassandraAdmin(current_superuser)._create_superuser(os_admin)
            CassandraAdmin(os_admin).drop_user(current_superuser)

        self.__create_cqlsh_config({self._CONF_AUTH_SEC:
                                    {self._CONF_USR_KEY: os_admin.name,
                                     self._CONF_PWD_KEY: os_admin.password}})

        # Update the internal status with the new user.
        self.status = CassandraAppStatus(os_admin)

        return os_admin

    def _reset_admin_password(self):
        """
        Reset the password of the Trove's administrative superuser.

        The service should not be running at this point.

        A general password reset procedure is:
            - disable user authentication and remote access
            - restart the service
            - update the password in the 'system_auth.credentials' table
            - re-enable authentication and make the host reachable
            - restart the service
        """
        if self.status.is_running:
            raise RuntimeError(_("Cannot reset the administrative password. "
                                 "The service is still running."))

        try:
            # Disable automatic startup in case the node goes down before
            # we have the superuser secured.
            self._disable_db_on_boot()

            self.__disable_remote_access()
            self.__disable_authentication()

            # We now start up the service and immediately re-enable
            # authentication in the configuration file (takes effect after
            # restart).
            # Then we reset the superuser password to its default value
            # and restart the service to get user functions back.
            self.start_db(update_db=False)
            self.__enable_authentication()
            os_admin = self.__reset_user_password_to_default(self._ADMIN_USER)
            self.status = CassandraAppStatus(os_admin)
            self.restart()

            # Now change the administrative password to a new secret value.
            self.secure(update_user=os_admin)
        finally:
            self.stop_db()  # Always restore the initial state of the service.

        # At this point, we should have a secured database with new Trove-only
        # superuser password.
        # Proceed to re-enable remote access and automatic startup.
        self.__enable_remote_access()
        self._enable_db_on_boot()

    def __reset_user_password_to_default(self, username):
        LOG.debug("Resetting the password of user '%s' to '%s'."
                  % (username, self.default_superuser_password))

        user = models.CassandraUser(username, self.default_superuser_password)
        with CassandraLocalhostConnection(user) as client:
            client.execute(
                "UPDATE system_auth.credentials SET salted_hash=%s "
                "WHERE username='{}';", (user.name,),
                (self.default_superuser_pwd_hash,))

            return user

    def change_cluster_name(self, cluster_name):
        """Change the 'cluster_name' property of an exesting running instance.
        Cluster name is stored in the database and is required to match the
        configuration value. Cassandra fails to start otherwise.
        """

        if not self.status.is_running:
            raise RuntimeError(_("Cannot change the cluster name. "
                                 "The service is not running."))

        LOG.debug("Changing the cluster name to '%s'." % cluster_name)

        # Update the in-database value.
        self.__reset_cluster_name(cluster_name)

        # Update the configuration property.
        self._update_cluster_name_property(cluster_name)

        self.restart()

    def __reset_cluster_name(self, cluster_name):
        # Reset the in-database value stored locally on this node.
        current_superuser = self.get_current_superuser()
        with CassandraLocalhostConnection(current_superuser) as client:
            client.execute(
                "UPDATE system.local SET cluster_name = '{}' "
                "WHERE key='local';", (cluster_name,))

        # Newer version of Cassandra require a flush to ensure the changes
        # to the local system keyspace persist.
        self.flush_tables('system', 'local')

    def __create_cqlsh_config(self, sections):
        config_path = self._get_cqlsh_conf_path()
        config_dir = os.path.dirname(config_path)
        if not os.path.exists(config_dir):
            os.mkdir(config_dir, self._CONF_DIR_MODS)
        else:
            os.chmod(config_dir, self._CONF_DIR_MODS)
        operating_system.write_file(config_path, sections, codec=IniCodec())
        os.chmod(config_path, self._CONF_FILE_MODS)

    def get_current_superuser(self):
        """
        Build the Trove superuser.
        Use the stored credentials.
        If not available fall back to the defaults.
        """
        if self.has_user_config():
            return self.__load_current_superuser()

        LOG.warn(_("Trove administrative user has not been configured yet. "
                   "Using the built-in default: %s")
                 % self.default_superuser_name)
        return models.CassandraUser(self.default_superuser_name,
                                    self.default_superuser_password)

    def has_user_config(self):
        """
        Return TRUE if there is a client configuration file available
        on the guest.
        """
        return os.path.exists(self._get_cqlsh_conf_path())

    def __load_current_superuser(self):
        config = operating_system.read_file(self._get_cqlsh_conf_path(),
                                            codec=IniCodec())
        return models.CassandraUser(
            config[self._CONF_AUTH_SEC][self._CONF_USR_KEY],
            config[self._CONF_AUTH_SEC][self._CONF_PWD_KEY]
        )

    def apply_initial_guestagent_configuration(self, cluster_name=None):
        """Update guestagent-controlled configuration properties.
        These changes to the default template are necessary in order to make
        the database service bootable and accessible in the guestagent context.

        :param cluster_name:  The 'cluster_name' configuration property.
                              Use the unique guest id by default.
        :type cluster_name:   string
        """
        self.configuration_manager.apply_system_override(
            {'data_file_directories': [self.cassandra_data_dir]})
        self._make_host_reachable()
        self._update_cluster_name_property(cluster_name or CONF.guest_id)

    def _make_host_reachable(self):
        """
        Some of these settings may be overriden by user defined
        configuration groups.

        authenticator and authorizer
            - Necessary to enable users and permissions.
        rpc_address - Enable remote connections on all interfaces.
        broadcast_rpc_address - RPC address to broadcast to drivers and
                                other clients. Must be set if
                                rpc_address = 0.0.0.0 and can never be
                                0.0.0.0 itself.
        listen_address - The address on which the node communicates with
                         other nodes. Can never be 0.0.0.0.
        seed_provider - A list of discovery contact points.
        """
        self.__enable_authentication()
        self.__enable_remote_access()

    def __enable_remote_access(self):
        updates = {
            'rpc_address': "0.0.0.0",
            'broadcast_rpc_address': netutils.get_my_ipv4(),
            'listen_address': netutils.get_my_ipv4(),
            'seed_provider': {'parameters':
                              [{'seeds': netutils.get_my_ipv4()}]
                              }
        }

        self.configuration_manager.apply_system_override(updates)

    def __disable_remote_access(self):
        updates = {
            'rpc_address': "127.0.0.1",
            'listen_address': '127.0.0.1',
            'seed_provider': {'parameters':
                              [{'seeds': '127.0.0.1'}]
                              }
        }

        self.configuration_manager.apply_system_override(updates)

    def __enable_authentication(self):
        updates = {
            'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
            'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'
        }

        self.configuration_manager.apply_system_override(updates)

    def __disable_authentication(self):
        updates = {
            'authenticator': 'org.apache.cassandra.auth.AllowAllAuthenticator',
            'authorizer': 'org.apache.cassandra.auth.AllowAllAuthorizer'
        }

        self.configuration_manager.apply_system_override(updates)

    def _update_cluster_name_property(self, name):
        """This 'cluster_name' property prevents nodes from one
        logical cluster from talking to another.
        All nodes in a cluster must have the same value.
        """
        self.configuration_manager.apply_system_override({'cluster_name':
                                                          name})

    def update_overrides(self, context, overrides, remove=False):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def remove_overrides(self):
        self.configuration_manager.remove_user_override()

    def start_db_with_conf_changes(self, config_contents):
        LOG.debug("Starting database with configuration changes.")
        if self.status.is_running:
            raise RuntimeError(_("The service is still running."))

        self.configuration_manager.save_configuration(config_contents)
        # The configuration template has to be updated with
        # guestagent-controlled settings.
        self.apply_initial_guestagent_configuration()
        self.start_db(True)

    def reset_configuration(self, configuration):
        LOG.debug("Resetting configuration.")
        config_contents = configuration['config_contents']
        self.configuration_manager.save_configuration(config_contents)

    def _get_cqlsh_conf_path(self):
        return os.path.expanduser(self.cqlsh_conf_path)

    def flush_tables(self, keyspace, *tables):
        """Flushes one or more tables from the memtable.
        """
        LOG.debug("Flushing tables.")
        # nodetool -h <HOST> -p <PORT> -u <USER> -pw <PASSWORD> flush --
        # <keyspace> ( <table> ... )
        self._run_nodetool_command('flush', keyspace, *tables)

    def _run_nodetool_command(self, cmd, *args, **kwargs):
        """Execute a nodetool command on this node.
        """
        cassandra = self.get_current_superuser()
        return utils.execute('nodetool',
                             '-h', 'localhost',
                             '-u', cassandra.name,
                             '-pw', cassandra.password, cmd, *args, **kwargs)


class CassandraAppStatus(service.BaseDbStatus):

    def __init__(self, superuser):
        """
        :param superuser:        User account the Status uses for connecting
                                 to the database.
        :type superuser:         CassandraUser
        """
        super(CassandraAppStatus, self).__init__()
        self.__user = superuser

    def _get_actual_db_status(self):
        try:
            with CassandraLocalhostConnection(self.__user):
                return rd_instance.ServiceStatuses.RUNNING
        except NoHostAvailable:
            return rd_instance.ServiceStatuses.SHUTDOWN
        except Exception:
            LOG.exception(_("Error getting Cassandra status."))

        return rd_instance.ServiceStatuses.SHUTDOWN


class CassandraAdmin(object):
    """Handles administrative tasks on the Cassandra database.

    In Cassandra only SUPERUSERS can create other users and grant permissions
    to database resources. Trove uses the 'cassandra' superuser to perform its
    administrative tasks.

    The users it creates are all 'normal' (NOSUPERUSER) accounts.
    The permissions it can grant are also limited to non-superuser operations.
    This is to prevent anybody from creating a new superuser via the Trove API.
    Similarly, all list operations include only non-superuser accounts.
    """

    # Non-superuser grant modifiers.
    __NO_SUPERUSER_MODIFIERS = ('ALTER', 'CREATE', 'DROP', 'MODIFY', 'SELECT')

    def __init__(self, user):
        self.__admin_user = user

    def create_user(self, context, users):
        """
        Create new non-superuser accounts.
        New users are by default granted full access to all database resources.
        """
        with CassandraLocalhostConnection(self.__admin_user) as client:
            for item in users:
                self._create_user_and_grant(client,
                                            self._deserialize_user(item))

    def _create_user_and_grant(self, client, user):
        """
        Create new non-superuser account and grant it full access to its
        databases.
        """
        self._create_user(client, user)
        for db in user.databases:
            self._grant_full_access_on_keyspace(
                client, self._deserialize_keyspace(db), user)

    def _create_user(self, client, user):
        # Create only NOSUPERUSER accounts here.
        LOG.debug("Creating a new user '%s'." % user.name)
        client.execute("CREATE USER '{}' WITH PASSWORD %s NOSUPERUSER;",
                       (user.name,), (user.password,))

    def _create_superuser(self, user):
        """Create a new superuser account and grant it full superuser-level
        access to all keyspaces.
        """
        LOG.debug("Creating a new superuser '%s'." % user.name)
        with CassandraLocalhostConnection(self.__admin_user) as client:
            client.execute("CREATE USER '{}' WITH PASSWORD %s SUPERUSER;",
                           (user.name,), (user.password,))
            client.execute("GRANT ALL PERMISSIONS ON ALL KEYSPACES TO '{}';",
                           (user.name,))

    def delete_user(self, context, user):
        self.drop_user(self._deserialize_user(user))

    def drop_user(self, user):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            self._drop_user(client, user)

    def _drop_user(self, client, user):
        LOG.debug("Deleting user '%s'." % user.name)
        client.execute("DROP USER '{}';", (user.name, ))

    def get_user(self, context, username, hostname):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            user = self._find_user(client, username)
            return user.serialize() if user is not None else None

    def _find_user(self, client, username):
        """
        Lookup a user with a given username.
        Search only in non-superuser accounts.
        Return a new Cassandra user instance or None if no match is found.
        """
        return next((user for user in self._get_non_system_users(client)
                     if user.name == username), None)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        """
        List all non-superuser accounts.
        Return an empty set if None.
        """
        with CassandraLocalhostConnection(self.__admin_user) as client:
            users = [user.serialize() for user in
                     self._get_non_system_users(client)]
            return pagination.paginate_list(users, limit, marker,
                                            include_marker)

    def _get_non_system_users(self, client):
        """
        Return a set of unique user instances.
        Return only non-superuser accounts. Omit user names on the ignore list.
        """
        return {self._build_user(client, user.name)
                for user in client.execute("LIST USERS;")
                if not user.super and user.name not in self.ignore_users}

    def _build_user(self, client, username, check_reserved=True):
        if check_reserved:
            self._check_reserved_user_name(username)

        user = models.CassandraUser(username)
        for keyspace in self._get_available_keyspaces(client):
            found = self._get_permissions_on_keyspace(client, keyspace, user)
            if found:
                user.databases.append(keyspace.serialize())

        return user

    def _get_permissions_on_keyspace(self, client, keyspace, user):
        return {item.permission for item in
                client.execute("LIST ALL PERMISSIONS ON KEYSPACE \"{}\" "
                               "OF '{}' NORECURSIVE;",
                               (keyspace.name, user.name))}

    def grant_access(self, context, username, hostname, databases):
        """
        Grant full access on keyspaces to a given username.
        """
        user = models.CassandraUser(username)
        with CassandraLocalhostConnection(self.__admin_user) as client:
            for db in databases:
                self._grant_full_access_on_keyspace(
                    client, models.CassandraSchema(db), user)

    def revoke_access(self, context, username, hostname, database):
        """
        Revoke all permissions on any database resources from a given username.
        """
        user = models.CassandraUser(username)
        with CassandraLocalhostConnection(self.__admin_user) as client:
            self._revoke_all_access_on_keyspace(
                client, models.CassandraSchema(database), user)

    def _grant_full_access_on_keyspace(self, client, keyspace, user,
                                       check_reserved=True):
        """
        Grant all non-superuser permissions on a keyspace to a given user.
        """
        if check_reserved:
            self._check_reserved_user_name(user.name)
            self._check_reserved_keyspace_name(keyspace.name)

        for access in self.__NO_SUPERUSER_MODIFIERS:
            self._grant_permission_on_keyspace(client, access, keyspace, user)

    def _grant_permission_on_keyspace(self, client, modifier, keyspace, user):
        """
        Grant a non-superuser permission on a keyspace to a given user.
        Raise an exception if the caller attempts to grant a superuser access.
        """
        LOG.debug("Granting '%s' access on '%s' to user '%s'."
                  % (modifier, keyspace.name, user.name))
        if modifier in self.__NO_SUPERUSER_MODIFIERS:
            client.execute("GRANT {} ON KEYSPACE \"{}\" TO '{}';",
                           (modifier, keyspace.name, user.name))
        else:
            raise exception.UnprocessableEntity(
                "Invalid permission modifier (%s). Allowed values are: '%s'"
                % (modifier, ', '.join(self.__NO_SUPERUSER_MODIFIERS)))

    def _revoke_all_access_on_keyspace(self, client, keyspace, user,
                                       check_reserved=True):
        if check_reserved:
            self._check_reserved_user_name(user.name)
            self._check_reserved_keyspace_name(keyspace.name)

        LOG.debug("Revoking all permissions on '%s' from user '%s'."
                  % (keyspace.name, user.name))
        client.execute("REVOKE ALL PERMISSIONS ON KEYSPACE \"{}\" FROM '{}';",
                       (keyspace.name, user.name))

    def update_attributes(self, context, username, hostname, user_attrs):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            user = self._build_user(client, username)
            new_name = user_attrs.get('name')
            new_password = user_attrs.get('password')
            self._update_user(client, user, new_name, new_password)

    def _update_user(self, client, user, new_username, new_password):
        """
        Update a user of a given username.
        Updatable attributes include username and password.
        If a new username and password are given a new user with those
        attributes is created and all permissions from the original
        user get transfered to it. The original user is then dropped
        therefore revoking its permissions.
        If only new password is specified the existing user gets altered
        with that password.
        """
        if new_username is not None and user.name != new_username:
            if new_password is not None:
                self._rename_user(client, user, new_username, new_password)
            else:
                raise exception.UnprocessableEntity(
                    _("Updating username requires specifying a password "
                      "as well."))
        elif new_password is not None and user.password != new_password:
            user.password = new_password
            self._alter_user_password(client, user)

    def _rename_user(self, client, user, new_username, new_password):
        """
        Rename a given user also updating its password.
        Transfer the current permissions to the new username.
        Drop the old username therefore revoking its permissions.
        """
        LOG.debug("Renaming user '%s' to '%s'" % (user.name, new_username))
        new_user = models.CassandraUser(new_username, new_password)
        new_user.databases.extend(user.databases)
        self._create_user_and_grant(client, new_user)
        self._drop_user(client, user)

    def alter_user_password(self, user):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            self._alter_user_password(client, user)

    def change_passwords(self, context, users):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            for user in users:
                self._alter_user_password(client, self._deserialize_user(user))

    def _alter_user_password(self, client, user):
        LOG.debug("Changing password of user '%s'." % user.name)
        client.execute("ALTER USER '{}' "
                       "WITH PASSWORD %s;", (user.name,), (user.password,))

    def create_database(self, context, databases):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            for item in databases:
                self._create_single_node_keyspace(
                    client, self._deserialize_keyspace(item))

    def _create_single_node_keyspace(self, client, keyspace):
        """
        Create a single-replica keyspace.

        Cassandra stores replicas on multiple nodes to ensure reliability and
        fault tolerance. All replicas are equally important;
        there is no primary or master.
        A replication strategy determines the nodes where
        replicas are placed. SimpleStrategy is for a single data center only.
        The total number of replicas across the cluster is referred to as the
        replication factor.

        Replication Strategy:
        'SimpleStrategy' is not optimized for multiple data centers.
        'replication_factor' The number of replicas of data on multiple nodes.
                             Required for SimpleStrategy; otherwise, not used.

        Keyspace names are case-insensitive by default.
        To make a name case-sensitive, enclose it in double quotation marks.
        """
        client.execute("CREATE KEYSPACE \"{}\" WITH REPLICATION = "
                       "{{ 'class' : 'SimpleStrategy', "
                       "'replication_factor' : 1 }};", (keyspace.name,))

    def delete_database(self, context, database):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            self._drop_keyspace(client, self._deserialize_keyspace(database))

    def _drop_keyspace(self, client, keyspace):
        LOG.debug("Dropping keyspace '%s'." % keyspace.name)
        client.execute("DROP KEYSPACE \"{}\";", (keyspace.name,))

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            databases = [keyspace.serialize() for keyspace
                         in self._get_available_keyspaces(client)]
            return pagination.paginate_list(databases, limit, marker,
                                            include_marker)

    def _get_available_keyspaces(self, client):
        """
        Return a set of unique keyspace instances.
        Omit keyspace names on the ignore list.
        """
        return {models.CassandraSchema(db.keyspace_name)
                for db in client.execute("SELECT * FROM "
                                         "system.schema_keyspaces;")
                if db.keyspace_name not in self.ignore_dbs}

    def list_access(self, context, username, hostname):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            user = self._find_user(client, username)
            if user:
                return user.databases

        raise exception.UserNotFound(username)

    def _deserialize_keyspace(self, keyspace_dict, check_reserved=True):
        if keyspace_dict:
            db = models.CassandraSchema.deserialize_schema(keyspace_dict)
            if check_reserved:
                self._check_reserved_keyspace_name(db.name)

            return db

        return None

    def _check_reserved_keyspace_name(self, name):
        if name in self.ignore_dbs:
            raise ValueError(_("This keyspace-name is reserved: %s") % name)

    def _deserialize_user(self, user_dict, check_reserved=True):
        if user_dict:
            user = models.CassandraUser.deserialize_user(user_dict)
            if check_reserved:
                self._check_reserved_user_name(user.name)

            return user

        return None

    def _check_reserved_user_name(self, name):
        if name in self.ignore_users:
            raise ValueError(_("This user-name is reserved: %s") % name)

    @property
    def ignore_users(self):
        return self.get_configuration_property('ignore_users')

    @property
    def ignore_dbs(self):
        return self.get_configuration_property('ignore_dbs')

    @staticmethod
    def get_configuration_property(property_name):
        manager = CONF.datastore_manager or "cassandra"
        return CONF.get(manager).get(property_name)


class CassandraConnection(object):
    """A wrapper to manage a Cassandra connection."""

    # Cassandra 2.1 only supports protocol versions 3 and lower.
    NATIVE_PROTOCOL_VERSION = 3

    def __init__(self, contact_points, user):
        self.__user = user
        # A Cluster is initialized with a set of initial contact points.
        # After the driver connects to one of the nodes it will automatically
        # discover the rest.
        # Will connect to '127.0.0.1' if None contact points are given.
        self._cluster = Cluster(
            contact_points=contact_points,
            auth_provider=PlainTextAuthProvider(user.name, user.password),
            protocol_version=self.NATIVE_PROTOCOL_VERSION)
        self.__session = None

    def __enter__(self):
        self.__connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__disconnect()

    def execute(self, query, identifiers=None, data_values=None, timeout=None):
        """
        Execute a query with a given sequence or dict of data values to bind.
        If a sequence is used, '%s' should be used the placeholder for each
        argument. If a dict is used, '%(name)s' style placeholders must
        be used.
        Only data values should be supplied this way. Other items,
        such as keyspaces, table names, and column names should be set
        ahead of time. Use the '{}' style placeholders and
        'identifiers' parameter for those.
        Raise an exception if the operation exceeds the given timeout (sec).
        There is no timeout if set to None.
        Return a set of rows or an empty list if None.
        """
        if self.__is_active():
            try:
                rows = self.__session.execute(self.__bind(query, identifiers),
                                              data_values, timeout)
                return rows or []
            except OperationTimedOut:
                LOG.error(_("Query execution timed out."))
                raise

        LOG.debug("Cannot perform this operation on a closed connection.")
        raise exception.UnprocessableEntity()

    def __bind(self, query, identifiers):
        if identifiers:
            return query.format(*identifiers)
        return query

    def __connect(self):
        if not self._cluster.is_shutdown:
            LOG.debug("Connecting to a Cassandra cluster as '%s'."
                      % self.__user.name)
            if not self.__is_active():
                self.__session = self._cluster.connect()
            else:
                LOG.debug("Connection already open.")
            LOG.debug("Connected to cluster: '%s'"
                      % self._cluster.metadata.cluster_name)
            for host in self._cluster.metadata.all_hosts():
                LOG.debug("Connected to node: '%s' in rack '%s' at datacenter "
                          "'%s'" % (host.address, host.rack, host.datacenter))
        else:
            LOG.debug("Cannot perform this operation on a terminated cluster.")
            raise exception.UnprocessableEntity()

    def __disconnect(self):
        if self.__is_active():
            try:
                LOG.debug("Disconnecting from cluster: '%s'"
                          % self._cluster.metadata.cluster_name)
                self._cluster.shutdown()
                self.__session.shutdown()
            except Exception:
                LOG.debug("Failed to disconnect from a Cassandra cluster.")

    def __is_active(self):
        return self.__session and not self.__session.is_shutdown


class CassandraLocalhostConnection(CassandraConnection):
    """
    A connection to the localhost Cassandra server.
    """

    def __init__(self, user):
        super(CassandraLocalhostConnection, self).__init__(None, user)

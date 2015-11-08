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

import collections
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
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.experimental.cassandra import system
from trove.guestagent.datastore import service
from trove.guestagent.db import models
from trove.guestagent import pkg


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

packager = pkg.Package()


class CassandraApp(object):
    """Prepares DBaaS on a Guest container."""

    _CONF_AUTH_SEC = 'authentication'
    _CONF_USR_KEY = 'username'
    _CONF_PWD_KEY = 'password'
    _CONF_DIR_MODS = stat.S_IRWXU
    _CONF_FILE_MODS = stat.S_IRUSR
    _CASSANDRA_CONF = system.CASSANDRA_CONF[operating_system.get_os()]
    _CASSANDRA_CONF_BACKUP = system.CASSANDRA_CONF_BACKUP[
        operating_system.get_os()]

    def __init__(self, status):
        """By default login with root no password for initial setup."""
        self.state_change_wait_time = CONF.state_change_wait_time
        self.status = status

    def install_if_needed(self, packages):
        """Prepare the guest machine with a cassandra server installation."""
        LOG.info(_("Preparing Guest as a Cassandra Server"))
        if not packager.pkg_is_installed(packages):
            self._install_db(packages)
        LOG.debug("Cassandra install_if_needed complete")

    def _enable_db_on_boot(self):
        operating_system.enable_service_on_boot(system.SERVICE_CANDIDATES)

    def _disable_db_on_boot(self):
        operating_system.disable_service_on_boot(system.SERVICE_CANDIDATES)

    def init_storage_structure(self, mount_point):
        try:
            operating_system.create_directory(mount_point, as_root=True)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error while initiating storage structure."))

    def start_db(self, update_db=False):
        LOG.info(_("Starting Cassandra server."))
        self._enable_db_on_boot()
        try:
            operating_system.start_service(system.SERVICE_CANDIDATES)
        except exception.ProcessExecutionError:
            LOG.exception(_("Error starting Cassandra"))
            pass

        if not (self.status.
                wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.RUNNING,
                self.state_change_wait_time,
                update_db)):
            try:
                utils.execute_with_timeout(system.CASSANDRA_KILL,
                                           shell=True)
            except exception.ProcessExecutionError:
                LOG.exception(_("Error killing Cassandra start command."))
            self.status.end_restart()
            raise RuntimeError(_("Could not start Cassandra"))

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        if do_not_start_on_reboot:
            self._disable_db_on_boot()
        operating_system.stop_service(system.SERVICE_CANDIDATES)

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
        """Install cassandra server"""
        LOG.debug("Installing Cassandra server.")
        packager.pkg_install(packages, None, system.INSTALL_TIMEOUT)
        LOG.debug("Finished installing Cassandra server")

    def _remove_system_tables(self):
        """
        Clean up the system keyspace.

        System tables are initialized on the first boot.
        They store certain properties, such as 'cluster_name',
        that cannot be easily changed once afterwards.
        The system keyspace needs to be cleaned up first. The
        tables will be regenerated on the next startup.

        The service should not be running at this point.
        """
        if self.status.is_running:
            raise RuntimeError(_("Cannot remove system tables. "
                                 "The service is still running."))

        LOG.info(_('Removing existing system tables.'))
        system_keyspace_dir = '%s/%s/' % (system.CASSANDRA_DATA_DIR,
                                          system.CASSANDRA_SYSTEM_KEYSPACE)
        utils.execute_with_timeout('rm', '-r', '-f', system_keyspace_dir,
                                   run_as_root=True, root_helper='sudo')

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

        :seealso: _reset_superuser_password
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
            self._reset_superuser_password()

            # Start the database and update the 'cluster_name' to the
            # new value.
            self.start_db(update_db=False)
            self.change_cluster_name(CONF.guest_id)
        finally:
            self.stop_db()  # Always restore the initial state of the service.

    def configure_superuser_access(self):
        LOG.info(_('Configuring Cassandra superuser.'))
        current_superuser = CassandraApp.get_current_superuser()
        cassandra = models.CassandraUser(system.DEFAULT_SUPERUSER_NAME,
                                         utils.generate_random_password())
        self.__create_cqlsh_config({self._CONF_AUTH_SEC:
                                    {self._CONF_USR_KEY: cassandra.name,
                                     self._CONF_PWD_KEY: cassandra.password}})
        CassandraAdmin(current_superuser).alter_user_password(cassandra)
        self.status.set_superuser(cassandra)

        return cassandra

    def _reset_superuser_password(self):
        """
        The service should not be running at this point.

        A general password reset procedure is:
            - disable user authentication and remote access
            - restart the service
            - update the password in the 'system_auth.credentials' table
            - re-enable authentication and make the host reachable
            - restart the service
        """
        if self.status.is_running:
            raise RuntimeError(_("Cannot reset the superuser password. "
                                 "The service is still running."))

        LOG.debug("Resetting the superuser password to '%s'."
                  % system.DEFAULT_SUPERUSER_PASSWORD)

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
            self.__reset_superuser_password()
            self.restart()

            # Now we configure the superuser access the same way as during
            # normal provisioning and restart to apply the changes.
            self.configure_superuser_access()
            self.restart()
        finally:
            self.stop_db()  # Always restore the initial state of the service.

        # At this point, we should have a secured database with new Trove-only
        # superuser password.
        # Proceed to re-enable remote access and automatic startup.
        self.__enable_remote_access()
        self._enable_db_on_boot()

    def __reset_superuser_password(self):
        current_superuser = CassandraApp.get_current_superuser()
        with CassandraLocalhostConnection(current_superuser) as client:
            client.execute(
                "UPDATE system_auth.credentials SET salted_hash=%s "
                "WHERE username='{}';", (current_superuser.name,),
                (system.DEFAULT_SUPERUSER_PWD_HASH,))

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
        current_superuser = CassandraApp.get_current_superuser()
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
        operating_system.write_config_file(config_path, sections)
        os.chmod(config_path, self._CONF_FILE_MODS)

    @classmethod
    def get_current_superuser(self):
        """
        Build the Trove superuser.
        Use the stored credentials.
        If not available fall back to the defaults.
        """
        if CassandraApp.has_user_config():
            return CassandraApp.__load_current_superuser()

        return models.CassandraUser(system.DEFAULT_SUPERUSER_NAME,
                                    system.DEFAULT_SUPERUSER_PASSWORD)

    @classmethod
    def has_user_config(self):
        """
        Return TRUE if there is a client configuration file available
        on the guest.
        """
        return os.path.exists(self._get_cqlsh_conf_path())

    @classmethod
    def __load_current_superuser(self):
        config = operating_system.read_config_file(self._get_cqlsh_conf_path())
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
        self.make_host_reachable()
        self._update_cluster_name_property(cluster_name or CONF.guest_id)

    def make_host_reachable(self):
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

        self._update_config(updates)

    def __disable_remote_access(self):
        updates = {
            'rpc_address': "127.0.0.1",
            'listen_address': '127.0.0.1',
            'seed_provider': {'parameters':
                              [{'seeds': '127.0.0.1'}]
                              }
        }

        self._update_config(updates)

    def __enable_authentication(self):
        updates = {
            'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
            'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'
        }

        self._update_config(updates)

    def __disable_authentication(self):
        updates = {
            'authenticator': 'org.apache.cassandra.auth.AllowAllAuthenticator',
            'authorizer': 'org.apache.cassandra.auth.AllowAllAuthorizer'
        }

        self._update_config(updates)

    def _update_cluster_name_property(self, name):
        """This 'cluster_name' property prevents nodes from one
        logical cluster from talking to another.
        All nodes in a cluster must have the same value.
        """
        self._update_config({'cluster_name': name})

    def update_overrides(self, context, overrides, remove=False):
        if overrides:
            if not os.path.exists(self._CASSANDRA_CONF_BACKUP):
                utils.execute_with_timeout("cp", "-f", "-p",
                                           self._CASSANDRA_CONF,
                                           self._CASSANDRA_CONF_BACKUP,
                                           run_as_root=True,
                                           root_helper="sudo")
                LOG.info(_("The old configuration has been saved to '%s'.")
                         % self._CASSANDRA_CONF_BACKUP)
                self._update_config(overrides)
            else:
                raise exception.TroveError(
                    _("This instance already has a "
                      "Configuration Group attached."))

    def remove_overrides(self):
        if os.path.exists(self._CASSANDRA_CONF_BACKUP):
            LOG.info(_("Restoring previous configuration from '%s'.")
                     % self._CASSANDRA_CONF_BACKUP)
            utils.execute_with_timeout("mv", "-f",
                                       self._CASSANDRA_CONF_BACKUP,
                                       self._CASSANDRA_CONF,
                                       run_as_root=True, root_helper="sudo")
        else:
            raise exception.TroveError(
                _("This instance does not have a "
                  "Configuration Group attached."))

    def _update_config(self, options):
        config = operating_system.read_yaml_file(self._CASSANDRA_CONF)
        self.write_config(CassandraApp._update_dict(options, config))

    @staticmethod
    def _update_dict(updates, target):
        """Recursively update a target dictionary with given updates.

        Updates are provided as a dictionary of key-value pairs
        where a value can also be a nested dictionary in which case
        its key is treated as a sub-section of the outer key.
        If a list value is encountered the update is applied
        iteratively on all its items.
        """
        if isinstance(target, list):
            for index, item in enumerate(target):
                target[index] = CassandraApp._update_dict(updates, item)
            return target

        for k, v in updates.iteritems():
            if isinstance(v, collections.Mapping):
                target[k] = CassandraApp._update_dict(v, target.get(k, {}))
            else:
                target[k] = updates[k]
        return target

    def write_config(self, config, is_raw=False):
        LOG.info(_('Saving Cassandra configuration.'))

        if is_raw:
            operating_system.write_file(self._CASSANDRA_CONF, config,
                                        as_root=True)
        else:
            operating_system.write_yaml_file(self._CASSANDRA_CONF, config,
                                             as_root=True)

        operating_system.chown(self._CASSANDRA_CONF,
                               system.CASSANDRA_OWNER, system.CASSANDRA_OWNER,
                               as_root=True)
        operating_system.chmod(self._CASSANDRA_CONF, FileMode.ADD_READ_ALL,
                               as_root=True)

    def start_db_with_conf_changes(self, config_contents):
        LOG.debug("Starting database with configuration changes.")
        if self.status.is_running:
            raise RuntimeError(_("The service is still running."))

        self.write_config(config_contents, is_raw=True)
        # The configuration template has to be updated with
        # guestagent-controlled settings.
        self.apply_initial_guestagent_configuration()
        self.start_db(True)

    def reset_configuration(self, configuration):
        LOG.debug("Resetting configuration.")
        self.write_config(configuration['config_contents'], is_raw=True)

    @classmethod
    def _get_cqlsh_conf_path(self):
        return os.path.expanduser(system.CQLSH_CONF_PATH)

    def get_data_directory(self):
        """Return current data directory.
        """
        config = operating_system.read_yaml_file(self._CASSANDRA_CONF)
        return config['data_file_directories'][0]

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

    def set_superuser(self, user):
        self.__user = user

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

    def delete_user(self, context, user):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            self._drop_user(client, self._deserialize_user(user))

    def _drop_user(self, client, user):
        LOG.debug("Deleting user '%s'." % user.name)
        client.execute("DROP USER '{}';", (user.name, ))

    def get_user(self, context, username, hostname):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            return self._find_user(client, username).serialize()

    def _find_user(self, client, username):
        """
        Lookup a user with a given username.
        Search only in non-superuser accounts.
        Return a new Cassandra user instance or raise if no match is found.
        """
        found = next((user for user in self._get_non_system_users(client)
                      if user.name == username), None)
        if found:
            return found

        raise exception.UserNotFound()

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
                if not user.super and user.name not in CONF.ignore_users}

    def _build_user(self, client, username):
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

    def _grant_full_access_on_keyspace(self, client, keyspace, user):
        """
        Grant all non-superuser permissions on a keyspace to a given user.
        """
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

    def _revoke_all_access_on_keyspace(self, client, keyspace, user):
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
                if db.keyspace_name not in CONF.ignore_dbs}

    def list_access(self, context, username, hostname):
        with CassandraLocalhostConnection(self.__admin_user) as client:
            return self._find_user(client, username).databases

    def _deserialize_keyspace(self, keyspace_dict):
        if keyspace_dict:
            return models.CassandraSchema.deserialize_schema(keyspace_dict)

        return None

    def _deserialize_user(self, user_dict):
        if user_dict:
            return models.CassandraUser.deserialize_user(user_dict)

        return None


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

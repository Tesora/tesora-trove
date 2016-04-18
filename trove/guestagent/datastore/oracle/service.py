# flake8: noqa

# Copyright (c) 2015 Tesora, Inc.
#
# This file is part of the Tesora DBaas Platform Enterprise Edition.
#
# Tesora DBaaS Platform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# According to sec. 7 of the GNU Affero General Public License, version 3, the
# terms of the AGPL are supplemented with the following terms:
#
# "Tesora", "Tesora DBaaS Platform", and the Tesora logo are trademarks
#  of Tesora, Inc.,
#
# The licensing of the Program under the AGPL does not imply a trademark
# license. Therefore any rights, title and interest in our trademarks remain
# entirely with us.
#
# However, if you propagate an unmodified version of the Program you are
# allowed to use the term "Tesora" solely to indicate that you distribute the
# Program. Furthermore you may use our trademarks where it is necessary to
# indicate the intended purpose of a product or service provided you use it in
# accordance with honest practices in industrial or commercial matters.
#
# If you want to propagate modified versions of the Program under the name
# "Tesora" or "Tesora DBaaS Platform", you may only do so if you have a written
# permission by Tesora, Inc. (to acquire a permission please contact
# Tesora, Inc at trademark@tesora.com).
#
# The interactive user interface of the software displays an attribution notice
# containing the term "Tesora" and/or the logo of Tesora.  Interactive user
# interfaces of unmodified and modified versions must display Appropriate Legal
# Notices according to sec. 5 of the GNU Affero General Public License,
# version 3, when you propagate unmodified or modified versions of  the
# Program. In accordance with sec. 7 b) of the GNU Affero General Public
# License, version 3, these Appropriate Legal Notices must retain the logo of
# Tesora or display the words "Initial Development by Tesora" if the display of
# the logo is not reasonably feasible for technical reasons.

import os

import cx_Oracle

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common import pagination
from trove.common import stream_codecs
from trove.common import utils as utils
from trove.guestagent.common import configuration
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle import sql_query
from trove.guestagent.datastore.oracle import system
from trove.guestagent.datastore import service
from trove.guestagent.db import models

ADMIN_USER_NAME = "os_admin"
CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'
LOG = logging.getLogger(__name__)


class OracleApp(object):
    """
    Handles Oracle installation and configuration
    on a Trove instance.
    """
    @classmethod
    def _init_overrides_dir(cls):
        """Initialize a directory for configuration overrides.
        """
        revision_dir = cls._param_file_path(configuration.ConfigurationManager.
                                            DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)
        if not os.path.exists(revision_dir):
            operating_system.create_directory(
                revision_dir,
                user=system.ORACLE_INSTANCE_OWNER,
                group=system.ORACLE_GROUP_OWNER,
                force=True, as_root=True)
        return revision_dir

    def _init_configuration_manager(self):
        revision_dir = self._init_overrides_dir()
        self.configuration_manager = configuration.ConfigurationManager(
            self.pfile,
            system.ORACLE_INSTANCE_OWNER,
            system.ORACLE_GROUP_OWNER,
            stream_codecs.PropertiesCodec(delimiter='=',
                                          comment_markers=('#')),
            requires_root=True,
            override_strategy=configuration.OneFileOverrideStrategy(
                revision_dir)
        )

    @classmethod
    def _param_file_path(cls, name):
        param_dir = guestagent_utils.build_file_path(
            CONF.get(MANAGER).oracle_home, 'dbs')
        return guestagent_utils.build_file_path(param_dir, name)

    def __init__(self, status, state_change_wait_time=None):
        LOG.debug("Initialize OracleApp.")
        if state_change_wait_time:
            self.state_change_wait_time = state_change_wait_time
        else:
            self.state_change_wait_time = CONF.state_change_wait_time
        LOG.debug("state_change_wait_time = %s." % self.state_change_wait_time)

        self.pfile = self._param_file_path(system.PFILE_NAME)
        self.new_spfile = self._param_file_path(system.NEW_SPFILE_NAME)
        self.configuration_manager = None
        if operating_system.exists(self.pfile):
            self._init_configuration_manager()
        self.status = status

    def change_ownership(self, mount_point):
        LOG.debug("Changing ownership of the Oracle data directory.")
        operating_system.chown(mount_point,
                               system.ORACLE_INSTANCE_OWNER,
                               system.ORACLE_GROUP_OWNER,
                               force=True,
                               as_root=True)

    def start_db_with_conf_changes(self, config_contents):
        LOG.debug("Inside the guest - Status is_running = (%s)."
                  % self.status.is_running)
        if not self.status.is_running:
            self.start_db()

    def start_db(self, update_db=False):
        LOG.debug("Start the Oracle databases.")
        ora_conf = OracleConfig()
        db_name = ora_conf.db_name
        self.update_spfile()
        self.status.start_db_service()
        with LocalOracleClient(db_name) as client:
            client.execute('select database_role, open_mode from v$database')
            row = client.fetchone()
            if row[0] == 'PHYSICAL STANDBY' and row[1] == 'READ ONLY':
                # Start up log apply if this database is supposed to be
                # a standby.
                client.execute("ALTER DATABASE RECOVER MANAGED STANDBY "
                               "DATABASE USING CURRENT LOGFILE DISCONNECT "
                               "FROM SESSION")

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        LOG.debug("Stop the Oracle databases.")
        self.status.stop_db_service()

    def restart(self):
        LOG.debug("Restarting Oracle server instance.")
        try:
            self.status.begin_restart()
            self.stop_db()
            self.start_db()
        finally:
            self.status.end_restart()

    def prep_pfile_management(self):
        """Generate the base PFILE from the original SPFILE and
        initialize the configuration manager to use it.
        """
        ora_admin = OracleAdmin()
        ora_admin.create_parameter_file(target=self.pfile)
        self._init_configuration_manager()

    def generate_spfile(self):
        LOG.debug("Generating a new SPFILE.")
        ora_admin = OracleAdmin()
        ora_admin.create_parameter_file(
            source=self.pfile,
            target=self.new_spfile,
            create_system=True
        )

    def remove_overrides(self):
        LOG.debug("Removing overrides.")
        self.configuration_manager.remove_user_override()
        self.generate_spfile()

    def update_overrides(self, overrides):
        if overrides:
            LOG.debug("Updating PFILE.")
            self.configuration_manager.apply_user_override(overrides)
            self.generate_spfile()

    def apply_overrides(self, overrides):
        ora_admin = OracleAdmin()
        ora_admin.set_initialization_parameters(overrides)

    def update_spfile(self):
        """Checks if there is a new SPFILE and replaces the old.
        The database must be shutdown before running this.
        """
        spfile = self._param_file_path(
            system.SPFILE_NAME % {'db_name': OracleAdmin().database_name})
        if operating_system.exists(self.new_spfile, as_root=True):
            LOG.debug('Found a new SPFILE.')
            operating_system.move(
                self.new_spfile,
                spfile,
                as_root=True,
                force=True
            )

    def make_read_only(self, read_only):
        if read_only:
            ora_conf = OracleConfig()
            db_name = ora_conf.db_name
            admin_pswd = ora_conf.admin_password
            os.environ["ORACLE_SID"] = db_name
            os.environ["ORACLE_HOME"] = CONF.get(MANAGER).oracle_home
            connection = cx_Oracle.connect(user=ADMIN_USER_NAME,
                                           password=admin_pswd,
                                           mode=cx_Oracle.SYSDBA)
            cursor = connection.cursor()
            cursor.execute('select open_mode from v$database')
            row = cursor.fetchone()
            if not row[0].startswith('READ ONLY'):
                # This command has the side effect of shutting down the
                # database and close the connection. Therefore the
                # database needs to be restarted.
                # Due to the behavior of this command, we
                # are not using the LocalOracleClient here to manage
                # the connection.
                cursor.execute("ALTER DATABASE COMMIT TO SWITCHOVER TO "
                               "STANDBY")
                self.restart()
            del os.environ["ORACLE_SID"]

    def set_db_start_flag_in_oratab(self, db_start_flag='Y'):
        """
        Set the database start flag of all entries in the oratab file to the
        specified value.
        """
        oratab_path = '/etc/oratab'
        oratab = operating_system.read_file(oratab_path,
                                            stream_codecs.PropertiesCodec(
                                                delimiter=':'))
        for key in oratab.keys():
            oratab[key][1] = db_start_flag
        operating_system.write_file(
            oratab_path, oratab,
            stream_codecs.PropertiesCodec(delimiter=':',
                                          line_terminator='\n'),
            as_root=True)
        operating_system.chown(oratab_path, 'oracle', 'oinstall',
                               as_root=True)


class OracleAppStatus(service.BaseDbStatus):
    """
    Handles all of the status updating for the Oracle guest agent.
    """
    def _get_actual_db_status(self):
        try:
            out, err = utils.execute_with_timeout(
                system.ORACLE_STATUS, shell=True)
            if out != '0\n':
                # If the number of 'ora' process is not zero, it means an
                # Oracle instance is running.
                LOG.debug("Setting state to "
                          "rd_instance.ServiceStatuses.RUNNING")
                return rd_instance.ServiceStatuses.RUNNING
            else:
                LOG.debug("Setting state to "
                          "rd_instance.ServiceStatuses.SHUTDOWN")
                return rd_instance.ServiceStatuses.SHUTDOWN
        except exception.ProcessExecutionError:
            LOG.exception(_("Error getting the Oracle server status."))
            return rd_instance.ServiceStatuses.CRASHED

    def _run_dbora_service(self, mode):
        cmd = 'sudo systemctl %s dbora.service' % mode
        utils.execute_with_timeout(
            cmd, timeout=CONF.get(MANAGER).service_start_timeout,
            shell=True, log_output_on_error=True)

    def start_db_service(self, service_candidates=None, timeout=None,
                         enable_on_boot=True, update_db=False):
        self._run_dbora_service(mode='start')

    def stop_db_service(self, service_candidates=None, timeout=None,
                        disable_on_boot=False, update_db=False):
        self._run_dbora_service(mode='stop')

def run_command(command, superuser=system.ORACLE_INSTANCE_OWNER,
                timeout=system.TIMEOUT):
    return utils.execute_with_timeout("sudo", "su", "-", superuser, "-c",
                                      command, timeout=timeout)


class OracleConfig(object):

    _CONF_FILE = CONF.get(MANAGER).conf_file
    _CONF_ORA_SEC = 'ORACLE'
    _CONF_SYS_KEY = 'sys_pwd'
    _CONF_ADMIN_KEY = 'os_admin_pwd'
    _CONF_ROOT_ENABLED = 'root_enabled'
    _CONF_DB_NAME = 'db_name'
    _CONF_DB_UNIQUE_NAME = 'db_unique_name'

    def __init__(self):
        self._admin_pwd = None
        self._sys_pwd = None
        self._db_name = None
        self._db_unique_name = None
        self.codec = stream_codecs.IniCodec()
        if not os.path.isfile(self._CONF_FILE):
            operating_system.create_directory(os.path.dirname(self._CONF_FILE),
                                              as_root=True)
            section = {self._CONF_ORA_SEC: {}}
            operating_system.write_file(self._CONF_FILE, section,
                                        codec=self.codec,
                                        as_root=True)
        else:
            config = operating_system.read_file(self._CONF_FILE,
                                                codec=self.codec,
                                                as_root=True)
            try:
                if self._CONF_SYS_KEY in config[self._CONF_ORA_SEC]:
                    self._sys_pwd = config[
                        self._CONF_ORA_SEC][self._CONF_SYS_KEY]
                if self._CONF_ADMIN_KEY in config[self._CONF_ORA_SEC]:
                    self._admin_pwd = config[
                        self._CONF_ORA_SEC][self._CONF_ADMIN_KEY]
                if self._CONF_ROOT_ENABLED in config[self._CONF_ORA_SEC]:
                    self._root_enabled = config[
                        self._CONF_ORA_SEC][self._CONF_ROOT_ENABLED]
                if self._CONF_DB_NAME in config[self._CONF_ORA_SEC]:
                    self._db_name = config[
                        self._CONF_ORA_SEC][self._CONF_DB_NAME]
                if self._CONF_DB_UNIQUE_NAME in config[self._CONF_ORA_SEC]:
                    self._db_unique_name = config[
                        self._CONF_ORA_SEC][self._CONF_DB_UNIQUE_NAME]
            except KeyError:
                # the ORACLE section does not exist, stop parsing
                pass

    def _save_value_in_file(self, param, value):
        config = operating_system.read_file(self._CONF_FILE, codec=self.codec,
                                            as_root=True)
        config[self._CONF_ORA_SEC][param] = value
        operating_system.write_file(self._CONF_FILE, config, codec=self.codec,
                                    as_root=True)

    @property
    def sys_password(self):
        return self._sys_pwd

    @sys_password.setter
    def sys_password(self, value):
        self._save_value_in_file(self._CONF_SYS_KEY, value)
        self._sys_pwd = value

    @property
    def admin_password(self):
        return self._admin_pwd

    @admin_password.setter
    def admin_password(self, value):
        self._save_value_in_file(self._CONF_ADMIN_KEY, value)
        self._admin_pwd = value

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, value):
        self._save_value_in_file(self._CONF_DB_NAME, value)
        self._db_name = value

    @property
    def db_unique_name(self):
        return self._db_unique_name or self._db_name

    @db_unique_name.setter
    def db_unique_name(self, value):
        self._save_value_in_file(self._CONF_DB_UNIQUE_NAME, value)
        self._db_unique_name = value

    def is_root_enabled(self):
        return bool(self._root_enabled)

    def enable_root(self):
        self._save_value_in_file(self._CONF_ROOT_ENABLED, 'true')
        self._root_enabled = 'true'

    def disable_root(self):
        self._save_value_in_file(self._CONF_ROOT_ENABLED, 'false')
        self._root_enabled = 'false'


class LocalOracleClient(object):
    """A wrapper to manage Oracle connection."""

    def __init__(self, sid, service=False, user_id=None, password=None):
        os.environ["ORACLE_HOME"] = CONF.get(MANAGER).oracle_home
        self.sid = sid
        self.service = service
        if user_id:
            self.user_id = user_id
            self.password = password
        else:
            self.user_id = ADMIN_USER_NAME
            self.password = OracleConfig().admin_password

    def __enter__(self):
        if self.service:
            ora_dsn = cx_Oracle.makedsn('localhost',
                                        CONF.get(MANAGER).listener_port,
                                        service_name=self.sid)
        else:
            ora_dsn = cx_Oracle.makedsn('localhost',
                                        CONF.get(MANAGER).listener_port,
                                        self.sid)
        self.conn = cx_Oracle.connect(user=self.user_id,
                                      password=self.password,
                                      dsn=ora_dsn,
                                      mode=cx_Oracle.SYSDBA)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.close()


class OracleConnection(object):
    """A wrapper to manage Oracle connection."""

    def __init__(self, sid, service=False, user_id=None, password=None,
                 mode=cx_Oracle.SYSDBA):
        os.environ["ORACLE_HOME"] = CONF.get(MANAGER).oracle_home
        self.sid = sid
        self.service = service
        self.mode = mode
        if user_id:
            self.user_id = user_id
            self.password = password
        else:
            self.user_id = ADMIN_USER_NAME
            self.password = OracleConfig().admin_password

    def __enter__(self):
        if self.service:
            ora_dsn = cx_Oracle.makedsn('localhost',
                                        CONF.get(MANAGER).listener_port,
                                        service_name=self.sid)
        else:
            ora_dsn = cx_Oracle.makedsn('localhost',
                                        CONF.get(MANAGER).listener_port,
                                        self.sid)
        self.conn = cx_Oracle.connect(user=self.user_id,
                                      password=self.password,
                                      dsn=ora_dsn, mode=self.mode)
        return self.conn

    def __exit__(self, type, value, traceback):
        try:
            self.conn.close()
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if (error.code == 1012):
                # ORA-01012: not logged on, connection already closed
                pass
            else:
                raise e


class OracleAdmin(object):
    """
    Handles administrative tasks on the Oracle instance.
    """

    def create_database(self, databases):
        """Create the given database(s)."""
        dbName = None
        db_create_failed = []
        LOG.debug("Creating Oracle databases.")
        ora_conf = OracleConfig()
        for database in databases:
            oradb = models.OracleSchema.deserialize_schema(database)
            dbName = oradb.name
            ora_conf.db_name = dbName
            LOG.debug("Creating Oracle database: %s." % dbName)
            try:
                sys_pwd = utils.generate_random_password(password_length=30)
                ora_conf.sys_password = sys_pwd
                run_command(system.CREATE_DB_COMMAND %
                            {'gdbname': dbName, 'sid': dbName,
                             'pswd': sys_pwd,
                             'db_ram': CONF.get(MANAGER).db_ram_size,
                             'template': CONF.get(MANAGER).template})
                client = LocalOracleClient(sid=dbName, service=True,
                                           user_id='sys', password=sys_pwd)
                self._create_admin_user(client)
                self.create_cloud_user_role(database)
            except exception.ProcessExecutionError:
                LOG.exception(_(
                    "There was an error creating database: %s.") % dbName)
                db_create_failed.append(dbName)
                pass
        if len(db_create_failed) > 0:
            LOG.exception(_("Creating the following databases failed: %s.") %
                          db_create_failed)

    def delete_database(self, database):
        """Delete the specified database."""
        dbName = None
        try:
            oradb = models.OracleSchema.deserialize_schema(database)
            dbName = oradb.name
            LOG.debug("Deleting Oracle database: %s." % dbName)
            run_command(system.DELETE_DB_COMMAND %
                        {'db': dbName, 'sys_user': ADMIN_USER_NAME,
                         'sys_pswd': OracleConfig().admin_password})
        except exception.ProcessExecutionError:
            LOG.exception(_(
                "There was an error while deleting database:%s.") % dbName)
            raise exception.GuestError(_("Unable to delete database: %s.") %
                                       dbName)

    def _create_admin_user(self, ora_sys_client):
        """
        Create a os_admin user with a random password
        with all privileges similar to the root user.
        """
        with ora_sys_client as client:
            oracnf = OracleConfig()
            if not oracnf.admin_password:
                oracnf.admin_password = utils.generate_random_password(
                    password_length=30)
            q = sql_query.CreateUser(ADMIN_USER_NAME, oracnf.admin_password)
            client.execute(str(q))
            q = ('grant sysdba, sysoper to %s' % ADMIN_USER_NAME)
            client.execute(str(q))

    def is_root_enabled(self):
        """Return True if root access is enabled; False otherwise."""
        return OracleRootAccess.is_root_enabled()

    def enable_root(self, root_password=None):
        """Enable the sys user global access and/or
           reset the sys password.
        """
        return OracleRootAccess.enable_root(root_password)

    def disable_root(self):
        """Disable reset the sys password."""
        return OracleRootAccess.disable_root()

    def _database_is_up(self, dbname):
        try:
            with LocalOracleClient(dbname, service=True) as client:
                q = sql_query.Query()
                q.columns = ["ROLE"]
                q.tables = ["DBA_ROLES"]
                q.where = ["ROLE = '%s'" %
                           CONF.get(MANAGER).cloud_user_role.upper()]
                client.execute(str(q))
                client.fetchall()
                if client.rowcount == 1:
                    return True
                else:
                    return False
        except cx_Oracle.DatabaseError:
            return False

    def _get_database_names(self):
        oratab_items = operating_system.read_file(
            '/etc/oratab', stream_codecs.PropertiesCodec(delimiter=':')
        )
        return oratab_items.keys()

    def list_databases(self, limit=None, marker=None, include_marker=False):
        dblist_page, next_marker = pagination.paginate_list(
            self._get_database_names(), limit, marker, include_marker)
        result = [models.OracleSchema(name).serialize()
                  for name in dblist_page]
        return result, next_marker

    @property
    def database_name(self):
        databases = self._get_database_names()
        if len(databases) == 0:
            raise exception.GuestError('No database found on guest.')
        elif len(databases) > 1:
            raise exception.GuestError('Multiple databases found on guest.')
        return databases[0]

    def create_cloud_user_role(self, database):
        LOG.debug("Creating database cloud user role")
        oradb = models.OracleSchema.deserialize_schema(database)
        with LocalOracleClient(oradb.name, service=True) as client:
            q = sql_query.CreateRole(CONF.get(MANAGER).cloud_user_role)
            client.execute(str(q))
            # TO-DO: Refactor GRANT query into the sql_query module
            grant_sql = ('grant create session, create table, '
                         'select any table, update any table, '
                         'insert any table, drop any table '
                         'to cloud_user_role')
            client.execute(grant_sql)
        LOG.debug("Finished creating database cloud user role")

    def create_user(self, users):
        LOG.debug("Creating database users")
        for item in users:
            user = models.OracleUser.deserialize_user(item)
            if self._database_is_up(self.database_name):
                with LocalOracleClient(self.database_name,
                                       service=True) as client:
                    q = sql_query.CreateUser(user.name, user.password)
                    client.execute(str(q))
                    # TO-DO: Refactor GRANT query into the sql_query module
                    client.execute('GRANT cloud_user_role to %s' % user.name)
                    client.execute('GRANT UNLIMITED TABLESPACE to %s' %
                                   user.name)
                LOG.debug(_("Created user %(user)s on %(db)s") %
                          {'user': user.name, 'db': self.database_name})
            else:
                LOG.debug(_("Failed to create user %(user)s on %(db)s") %
                          {'user': user.name, 'db': self.database_name})
        LOG.debug("Finished creating database users")

    def delete_user(self, user):
        LOG.debug("Delete a given user.")
        oracle_user = models.OracleUser.deserialize_user(user)
        userName = oracle_user.name
        user_dbs = oracle_user.databases
        LOG.debug("For user %s, databases to be deleted = %r." % (
            userName, user_dbs))

        if len(user_dbs) == 0:
            databases = self.list_access(oracle_user.name, None)
        else:
            databases = user_dbs

        LOG.debug("databases for user = %r." % databases)
        for database in databases:
            oradb = models.OracleSchema.deserialize_schema(database)
            with LocalOracleClient(oradb.name, service=True) as client:
                q = sql_query.DropUser(oracle_user.name, cascade=True)
                client.execute(str(q))

    def list_users(self, limit=None, marker=None, include_marker=False):
        LOG.debug(
            "List all users for all the databases in an Oracle server "
            "instance.")
        user_list = {}

        databases, marker = self.list_databases()
        for database in databases:
            oracle_db = models.OracleSchema.deserialize_schema(database)
            with LocalOracleClient(oracle_db.name, service=True) as client:
                q = sql_query.Query()
                q.columns = ["grantee"]
                q.tables = ["dba_role_privs"]
                q.where = ["granted_role = '%s'" %
                           CONF.get(MANAGER).cloud_user_role,
                           "grantee != 'SYS'"]
                client.execute(str(q))
                for row in client:
                    user_name = row[0]
                    if user_name in user_list:
                        user = models.OracleUser.deserialize_user(
                            user_list.get(user_name))
                    else:
                        user = models.OracleUser(user_name)
                    user.databases = oracle_db.name
                    user_list.update({user_name: user.serialize()})

        return user_list.values(), marker

    def get_user(self, username, hostname):
        LOG.debug("Get details of a given database user.")
        user = self._get_user(username, hostname)
        if not user:
            return None
        return user.serialize()

    def _get_user(self, username, hostname):
        LOG.debug("Get details of a given database user %s." % username)
        user = models.OracleUser(username)
        databases, marker = self.list_databases()
        for database in databases:
            oracle_db = models.OracleSchema.deserialize_schema(database)
            with LocalOracleClient(oracle_db.name, service=True) as client:
                q = sql_query.Query()
                q.columns = ["username"]
                q.tables = ["all_users"]
                q.where = ["username = '%s'" % username.upper()]
                client.execute(str(q))
                client.fetchall()
                if client.rowcount == 1:
                    user.databases.append(database)

        return user

    def list_access(self, username, hostname):
        """
           Show all the databases to which the user has more than
           USAGE granted.
        """
        LOG.debug("Listing databases that user: %s has access to." % username)
        user = self._get_user(username, hostname)
        return user.databases

    def change_passwords(self, users):
        """Change the passwords of one or more existing users."""
        LOG.debug("Changing the password of some users.")
        with LocalOracleClient(self.database_name, service=True) as client:
            for item in users:
                LOG.debug("Changing password for user %s." % item)
                user = models.OracleUser(item['name'],
                                         password=item['password'])
                q = sql_query.AlterUser(user.name, password=user.password)
                client.execute(str(q))

    def update_attributes(self, username, hostname, user_attrs):
        """Change the attributes of an existing user."""
        LOG.debug("Changing user attributes for user %s." % username)
        user = self._get_user(username, hostname)
        if user:
            password = user_attrs.get('password')
            if password:
                self.change_passwords([{'name': username,
                                        'password': password}])

    def create_parameter_file(self, source=None, target=None,
                              create_system=False, client=None):
        """Create a parameter file.
        :param source:          source file path, if None then default
        :param target:          target file path, if None then default
        :param create_system:   if True, creates an SPFILE, else a PFILE
        :param client:          cx_Oracle connection to use, else created
        """
        source_type = 'SPFILE'
        source_clause = ''
        source_log_msg = 'default location'
        if source:
            source_clause = "='%s'" % source
            source_log_msg = source
        target_type = 'PFILE'
        target_clause = ''
        target_log_msg = 'default location'
        if target:
            target_clause = "='%s'" % target
            target_log_msg = target
        if create_system:
            source_type = 'PFILE'
            target_type = 'SPFILE'
        LOG.debug("Creating %(target_type)s at %(target)s from "
                  "%(source_type)s at %(source)s."
                  % {'target_type': target_type,
                     'target': target_log_msg,
                     'source_type': source_type,
                     'source': source_log_msg})
        q = ("CREATE %(target_type)s%(target)s FROM "
             "%(source_type)s%(source)s"
             % {'target_type': target_type,
                'target': target_clause,
                'source_type': source_type,
                'source': source_clause})

        if client:
            client.execute(q)
        else:
            with LocalOracleClient(self.database_name, service=True) as client:
                client.execute(q)

    def set_initialization_parameters(self, set_parameters):
        LOG.debug("Setting initialization parameters.")
        with LocalOracleClient(self.database_name, service=True) as client:
            for k, v in set_parameters.items():
                try:
                    LOG.debug("Setting initialization parameter %s = %s"
                              % (k, v))
                    q = sql_query.AlterSystem.set_parameter(k, v)
                    client.execute(str(q))
                except cx_Oracle.DatabaseError:
                    LOG.exception(_("Error setting initialization parameter "
                                    "%(k)s = %(v)s") % {'k': k, 'v': v})

    def get_parameter(self, parameter):
        """Get the value of the given intialization parameter."""
        parameter = parameter.lower()
        LOG.debug('Getting current value of initialization parameter %s'
                  % parameter)
        q = sql_query.Query(columns=['value'],
                            tables=['v$parameter'],
                            where=["name = '%s'" % parameter])
        with LocalOracleClient(self.database_name, service=True) as client:
            client.execute(str(q))
            value = client.fetchone()[0]
        LOG.debug('Found parameter %s = %s' % (parameter, value))
        return value


class OracleRootAccess(object):
    @classmethod
    def is_root_enabled(cls):
        """Return True if root access is enabled; False otherwise."""
        return OracleConfig().is_root_enabled()

    @classmethod
    def enable_root(cls, root_password=None):
        """Enable access with the sys user and/or
           reset the sys password.
        """
        if root_password:
            sys_pwd = root_password
        else:
            sys_pwd = utils.generate_random_password(password_length=30)

        ora_admin = OracleAdmin()
        databases, marker = ora_admin.list_databases()
        for database in databases:
            oradb = models.OracleSchema.deserialize_schema(database)
            with LocalOracleClient(oradb.name, service=True) as client:
                client.execute('alter user sys identified by "%s"' %
                               sys_pwd)

        oracnf = OracleConfig()
        oracnf.enable_root()
        oracnf.sys_password = sys_pwd
        LOG.debug('enable_root completed')

        user = models.RootUser()
        user.name = "sys"
        user.host = "%"
        user.password = sys_pwd
        return user.serialize()

    @classmethod
    def disable_root(cls):
        """Disable reset the sys password."""
        sys_pwd = utils.generate_random_password(password_length=30)
        ora_admin = OracleAdmin()
        databases, marker = ora_admin.list_databases()
        for database in databases:
            oradb = models.OracleSchema.deserialize_schema(database)
            with LocalOracleClient(oradb.name, service=True) as client:
                client.execute('alter user sys identified by "%s"' %
                               sys_pwd)
        oracnf = OracleConfig()
        oracnf.disable_root()
        oracnf.sys_password = sys_pwd
        LOG.debug('disable_root completed')

# flake8: noqa

# Copyright (c) 2016 Tesora, Inc.
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
from os import path

import cx_Oracle
from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import pagination
from trove.common import stream_codecs
from trove.common import utils as utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle_common import sql_query
from trove.guestagent.db import models

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

ORACLE_TIMEOUT = 1200


def run_sys_command(command, user, timeout=ORACLE_TIMEOUT, shell=False):
    return utils.execute_with_timeout('su', '-', user, '-c', command,
                                      run_as_root=True,
                                      root_helper='sudo',
                                      timeout=timeout,
                                      shell=shell,
                                      log_output_on_error=True)


def new_oracle_password(password_length=30):
    return utils.generate_random_password(password_length=password_length)


class OracleConfig(object):
    """A wrapper to manage the Trove Oracle guestagent configuration file."""
    codec_class = stream_codecs.IniCodec
    section_name = 'ORACLE'
    tag_db_name = 'db_name'
    tag_admin_password = 'admin_password'
    tag_root_enabled = 'root_enabled'
    tag_root_password = 'root_password'
    key_names = {
        tag_db_name: 'db_name',
        tag_admin_password: 'os_admin_pwd',
        tag_root_enabled: 'root_enabled',
        tag_root_password: 'root_pswd'}

    def __init__(self, file_path):
        self.file_path = file_path
        self._codec = self.codec_class()
        self._values = dict.fromkeys(self.key_names)
        if not path.isfile(self.file_path):
            operating_system.create_directory(
                path.dirname(self.file_path), as_root=True)
            # create a new blank section
            section = {self.section_name: {}}
            operating_system.write_file(
                self.file_path, section, codec=self._codec, as_root=True)
        else:
            config = operating_system.read_file(
                self.file_path, codec=self._codec, as_root=True)
            self._parse_ora_config(config[self.section_name])

    def _parse_ora_config(self, ora_config):
        for option, name in self.key_names.iteritems():
            if name in ora_config:
                self._values[option] = ora_config.get(name)

    def _save_value_in_file(self, option, value):
        config = operating_system.read_file(
            self.file_path, codec=self._codec, as_root=True)
        name = self.key_names[option]
        config[self.section_name][name] = value
        operating_system.write_file(
            self.file_path, config, codec=self._codec, as_root=True)

    def _set_option(self, option, value):
        self._save_value_in_file(option, value)
        self._values[option] = value

    @property
    def db_name(self):
        return self._values[self.tag_db_name]

    @db_name.setter
    def db_name(self, value):
        self._set_option(self.tag_db_name, value)

    @property
    def root_password(self):
        return self._values[self.tag_root_password]

    @root_password.setter
    def root_password(self, value):
        self._set_option(self.tag_root_password, value)

    @property
    def admin_password(self):
        return self._values[self.tag_admin_password]

    @admin_password.setter
    def admin_password(self, value):
        self._set_option(self.tag_admin_password, value)

    def is_root_enabled(self):
        return bool(self._values[self.tag_root_enabled])

    def enable_root(self):
        self._set_option(self.tag_root_enabled, 'true')

    def disable_root(self):
        self._set_option(self.tag_root_enabled, 'false')


class OracleClient(object):
    """A wrapper to manage Oracle connections."""

    def __init__(self, sid, oracle_home,
                 hostname, port, user_id, password, use_service, mode):
        self.sid = sid
        self.hostname = hostname
        self.port = port
        self.user_id = user_id
        self.password = password
        self.use_service = use_service
        self.mode = mode
        self.oracle_home = oracle_home

    def __enter__(self):
        os.environ['ORACLE_HOME'] = self.oracle_home
        os.environ['ORACLE_SID'] = self.sid
        if self.use_service:
            ora_dsn = cx_Oracle.makedsn(self.hostname,
                                        self.port,
                                        service_name=self.sid)
        else:
            ora_dsn = cx_Oracle.makedsn(self.hostname,
                                        self.port,
                                        self.sid)
        LOG.debug("Connecting to Oracle with DSN: %s" % ora_dsn)
        self.conn = cx_Oracle.connect(user=self.user_id,
                                      password=self.password,
                                      dsn=ora_dsn,
                                      mode=self.mode)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.conn.close()
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1012:
                # ORA-01012: not logged on, connection already closed
                pass
            else:
                raise e
        finally:
            if 'ORACLE_HOME' in os.environ:
                del os.environ['ORACLE_HOME']
            if 'ORACLE_SID' in os.environ:
                del os.environ['ORACLE_SID']


class OracleCursor(OracleClient):
    """A wrapper to manage Oracle connection cursors."""

    def __enter__(self):
        super(OracleCursor, self).__enter__()
        return self.conn.cursor()


class OracleAdmin(object):
    """Handles administrative tasks on the Oracle database."""

    def __init__(self, oracle_config, oracle_client, oracle_cursor,
                 root_user_name, cloud_role_name):
        self.client = oracle_client
        self.cursor = oracle_cursor
        self.root_user_name = root_user_name
        self.cloud_role_name = cloud_role_name
        self._config_class = oracle_config
        self._config = None

    @property
    def ora_config(self):
        if not self._config:
            self._config = self._config_class()
        return self._config

    def delete_conf_cache(self):
        self._config = None

    def _create_database(self, db_name):
        pass

    def create_database(self, databases):
        db_name = models.OracleSchema.deserialize_schema(databases[0]).name
        self.ora_config.db_name = db_name
        self._create_database(db_name)
        # Create the cloud user role for identifying dbaas-managed users
        with self.cursor(db_name) as cursor:
            cursor.execute(str(sql_query.CreateRole(self.cloud_role_name)))

    def _delete_database(self, db_name):
        pass

    def delete_database(self, database):
        db_name = models.OracleSchema.deserialize_schema(database).name
        self._delete_database(db_name)

    def is_root_enabled(self):
        LOG.debug("Checking if root is enabled.")
        return self._config_class().is_root_enabled()

    def enable_root(self, root_password=None):
        """Enable the sys user global access and/or
           reset the sys password.
        """
        LOG.debug("Enabling root.")
        if self.database_open_mode.startswith('READ ONLY'):
            raise exception.TroveError(
                _("Cannot root enable a read only database."))
        if not root_password:
            root_password = new_oracle_password()
        with self.cursor(self.database_name) as cursor:
            cursor.execute(str(sql_query.AlterUser.change_password(
                self.root_user_name, root_password)))
        self.ora_config.enable_root()
        self.ora_config.root_password = root_password
        user = models.RootUser()
        user.name = self.root_user_name
        user.host = '%'
        user.password = root_password
        LOG.debug("Successfully enabled root.")
        return user.serialize()

    def disable_root(self):
        """Reset the sys password."""
        sys_pwd = new_oracle_password()
        with self.cursor(self.database_name) as cursor:
            cursor.execute(str(
                sql_query.AlterUser.change_password('SYS', sys_pwd)))
        self.ora_config.disable_root()
        self.ora_config.root_password = sys_pwd
        LOG.debug("Successfully disabled root.")

    def _list_databases(self, limit, marker, include_marker):
        pass

    def list_databases(self, limit=None, marker=None, include_marker=False):
        return self._list_databases(limit, marker, include_marker)

    @property
    def database_name(self):
        return self.ora_config.db_name

    @property
    def database_open_mode(self):
        with self.cursor(self.database_name) as cursor:
            cursor.execute(str(sql_query.Query(
                columns=['OPEN_MODE'], tables=['V$DATABASE'])))
            row = cursor.fetchone()
        return row[0]

    def create_user(self, users):
        for item in users:
            user = models.OracleUser.deserialize_user(item)
            LOG.debug("Creating user %s." % user.name)
            with self.cursor(self.database_name) as cursor:
                cursor.execute(str(
                    sql_query.CreateUser(user.name, user.password)))
                roles = ['CREATE SESSION', 'CREATE TABLE', 'SELECT ANY TABLE',
                         'UPDATE ANY TABLE', 'INSERT ANY TABLE',
                         'DROP ANY TABLE']
                cursor.execute(str(sql_query.Grant(user.name, roles)))
                cursor.execute(str(
                    sql_query.Grant(user.name, 'UNLIMITED TABLESPACE')))
                cursor.execute(str(
                    sql_query.Grant(user.name, self.cloud_role_name)))
            LOG.debug("Successfully created user.")

    def delete_user(self, user):
        oracle_user = models.OracleUser.deserialize_user(user)
        LOG.debug("Deleting user %s." % oracle_user.name)
        with self.cursor(self.database_name) as cursor:
            cursor.execute(str(
                sql_query.DropUser(oracle_user.name, cascade=True)))

    def list_users(self, limit=None, marker=None, include_marker=False):
        LOG.debug("Listing users (limit of %s, marker %s, "
                  "%s marker)." %
                  (limit, marker,
                   ('including' if include_marker else 'not including')))
        user_names = []
        with self.cursor(self.database_name) as cursor:
            q = sql_query.Query(
                columns=['GRANTEE'],
                tables=['DBA_ROLE_PRIVS'],
                where=["GRANTED_ROLE = '%s'" % self.cloud_role_name,
                       "GRANTEE != 'SYS'"]
            )
            cursor.execute(str(q))
            for row in cursor:
                user_names.append(row[0])
        user_list_page, next_marker = pagination.paginate_list(
            list(set(user_names)), limit, marker, include_marker)
        users_page = []
        for user_name in user_list_page:
            user = models.OracleUser(user_name)
            user.databases = self.database_name
            users_page.append(user.serialize())
        LOG.debug("Successfully listed users. "
                  "Users: %s (next marker %s)." %
                  (user_list_page, next_marker))
        return users_page, marker

    def get_user(self, username, hostname):
        LOG.debug("Getting user %s." % username)
        user = self._get_user(username)
        if not user:
            LOG.debug("User does not exist.")
            return None
        return user.serialize()

    def _get_user(self, username):
        with self.cursor(self.database_name) as cursor:
            q = sql_query.Query(
                columns=['USERNAME'],
                tables=['ALL_USERS'],
                where=["USERNAME = '%s'" % username.upper()]
            )
            # Check that the user exists
            cursor.execute(str(q))
            if not cursor.fetchone():
                return None
        user = models.OracleUser(username)
        user.databases = self.database_name
        return user

    def list_access(self, username, hostname):
        """Show all the databases to which the user has more than
           USAGE granted.
        """
        LOG.debug("Listing access for user %s." % username)
        user = self._get_user(username)
        return user.databases

    def change_passwords(self, users):
        """Change the passwords of one or more existing users."""
        LOG.debug("Changing the password of some user(s).")
        with self.cursor(self.database_name) as cursor:
            for item in users:
                LOG.debug("Changing password for user %s." % item)
                user = models.OracleUser(item['name'],
                                         password=item['password'])
                cursor.execute(str(
                    sql_query.AlterUser.change_password(user.name,
                                                        user.password)))

    def update_attributes(self, username, hostname, user_attrs):
        """Change the attributes of an existing user."""
        LOG.debug("Changing user attributes for user %s." % username)
        user = self._get_user(username)
        if user:
            password = user_attrs.get('password')
            if password:
                self.change_passwords([{'name': username,
                                        'password': password}])

    def set_initialization_parameters(self, set_parameters):
        LOG.debug("Setting initialization parameters.")
        with self.cursor(self.database_name) as cursor:
            for k, v in set_parameters.items():
                try:
                    LOG.debug("Setting initialization parameter %s = %s."
                              % (k, v))
                    cursor.execute(str(
                        sql_query.AlterSystem.set_parameter(k, v)))
                except cx_Oracle.DatabaseError as e:
                    LOG.exception(_("Error setting initialization parameter "
                                    "%(k)s = %(v)s.") % {'k': k, 'v': v})

    def get_parameter(self, parameter):
        parameter = parameter.lower()
        LOG.debug("Getting current value of initialization parameter %s."
                  % parameter)
        with self.cursor(self.database_name) as cursor:
            cursor.execute(str(
                sql_query.Query(columns=['VALUE'],
                                tables=['V$PARAMETER'],
                                where=["NAME = '%s'" % parameter])))
            value = cursor.fetchone()[0]
        LOG.debug('Found parameter %s = %s.' % (parameter, value))
        return value


class OracleApp(object):
    """Manages the Oracle software."""

    def __init__(self, status,
                 oracle_client, oracle_cursor, oracle_admin,
                 state_change_wait_time=None):
        LOG.debug("Initialize OracleApp.")
        self._init_state_change_wait_time(state_change_wait_time)
        self.status = status
        self.client = oracle_client
        self.cursor = oracle_cursor
        self.admin = oracle_admin()

    def _init_state_change_wait_time(self, state_change_wait_time=None):
        if state_change_wait_time:
            self.state_change_wait_time = state_change_wait_time
        else:
            self.state_change_wait_time = CONF.state_change_wait_time
        LOG.debug("state_change_wait_time = %s." % self.state_change_wait_time)

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
import re

import cx_Oracle

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common import instance as rd_instance
from trove.common import utils
from trove.common.i18n import _
from trove.guestagent.db import models
from trove.guestagent.datastore import service

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle_ra'

ORACLE_RA_CONFIG_FILE = "/etc/oracle/oracle-ra.cnf"
ORACLE_RA_CONFIG_FILE_TEMP = "/tmp/oracle-ra.cnf.tmp"
GUEST_INFO_FILE = "/etc/guest_info"

ROOT_USERNAME = 'ROOT'
ADMIN_USERNAME = 'DFT_ADMIN_USER'
PASSWORD_MAX_LEN = 30


class OracleAppStatus(service.BaseDbStatus):
    @classmethod
    def get(cls):
        if not cls._instance:
            cls._instance = OracleAppStatus()
        return cls._instance

    def _get_actual_db_status(self):
        if os.path.exists(CONF.get(MANAGER).oracle_ra_status_file):
            with open(CONF.get(MANAGER).oracle_ra_status_file, 'r') as ra_file:
                status = ra_file.readline()
            if status.startswith('OK'):
                return rd_instance.ServiceStatuses.RUNNING
            elif status.startswith('ERROR'):
                return rd_instance.ServiceStatuses.UNKNOWN


class LocalOracleClient(object):
    """A wrapper to manage Oracle connection."""

    def __init__(self, sid, service=False):
        self.sid = sid
        self.service = service

    def __enter__(self):
        if self.service:
            ora_dsn = cx_Oracle.makedsn(CONF.get(MANAGER).oracle_host,
                                        CONF.get(MANAGER).oracle_port,
                                        service_name=self.sid)
        else:
            ora_dsn = cx_Oracle.makedsn(CONF.get(MANAGER).oracle_host,
                                        CONF.get(MANAGER).oracle_port,
                                        self.sid)

        self.conn = cx_Oracle.connect("%s/%s" %
                                      (CONF.get(MANAGER).oracle_sys_usr,
                                       CONF.get(MANAGER).oracle_sys_pswd),
                                      dsn=ora_dsn,
                                      mode=cx_Oracle.SYSDBA)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.close()


class OracleAdmin(object):
    """Handles administrative tasks on the Oracle database."""

    def create_database(self):
        """Create the list of specified databases."""
        LOG.debug("Creating pluggable database")
        pdb_name = CONF.guest_name
        if not re.match(r'[a-zA-Z0-9]\w{,63}$', pdb_name):
            raise exception.BadRequest(
                _('Database name %(name)s is not valid. Oracle pluggable '
                  'database names restrictions: limit of 64 characters, use '
                  'only alphanumerics and underscores, cannot start with an '
                  'underscore.') % {'name': pdb_name})
        admin_password = utils.generate_random_password(PASSWORD_MAX_LEN)
        with LocalOracleClient(CONF.get(MANAGER).oracle_cdb_name) as client:
            statement = ("CREATE PLUGGABLE DATABASE %(pdb_name)s "
                         "ADMIN USER %(username)s "
                         "IDENTIFIED BY %(password)s" %
                         {'pdb_name': pdb_name,
                          'username': ROOT_USERNAME,
                          'password': admin_password})
            LOG.debug("DEBUG_SQL: %s" % statement)
            client.execute(statement)
            client.execute("ALTER PLUGGABLE DATABASE %s OPEN" %
                           CONF.guest_name)
        LOG.debug("Finished creating pluggable database")

    def create_user(self, users):
        """Create users and grant them privileges for the
           specified databases.
        """
        LOG.debug("Creating database user")
        user_id = users[0]['_name']
        password = users[0]['_password']
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            client.execute('CREATE USER %(user_id)s IDENTIFIED BY %(password)s'
                           % {'user_id': user_id, 'password': password})
            client.execute('GRANT CREATE SESSION to %s' % user_id)
            client.execute('GRANT CREATE TABLE to %s' % user_id)
            client.execute('GRANT UNLIMITED TABLESPACE to %s' % user_id)
            client.execute('GRANT SELECT ANY TABLE to %s' % user_id)
            client.execute('GRANT UPDATE ANY TABLE to %s' % user_id)
            client.execute('GRANT INSERT ANY TABLE to %s' % user_id)
            client.execute('GRANT DROP ANY TABLE to %s' % user_id)
        LOG.debug("Finished creating database user")

    def delete_database(self):
        """Delete the specified database."""
        LOG.debug("Deleting pluggable database %s" % CONF.guest_name)
        with LocalOracleClient(CONF.get(MANAGER).oracle_cdb_name) as client:
            try:
                client.execute("ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE" %
                               CONF.guest_name)
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                if error.code == 65011:
                    # ORA-65011: Pluggable database (x) does not exist.
                    # No need to issue drop pluggable database call.
                    LOG.debug("Pluggable database does not exist.")
                    return True
                elif error.code == 65020:
                    # ORA-65020: Pluggable database (x) already closed.
                    # Still need to issue drop pluggable database call.
                    pass
                else:
                    # Some other unknown issue, exit now.
                    raise e

            client.execute("DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES" %
                           CONF.guest_name)

        LOG.debug("Finished deleting pluggable database")
        return True

    def delete_user(self, user):
        """Delete the specified user."""
        oracle_user = models.OracleUser.deserialize_user(user)
        self.delete_user_by_name(oracle_user.name)

    def delete_user_by_name(self, name):
        LOG.debug("Deleting user %s" % name)
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            client.execute("DROP USER %s" % name)
        LOG.debug("Deleted user %s" % name)

    def get_user(self, username, hostname):
        user = self._get_user(username, hostname)
        if not user:
            return None
        return user.serialize()

    def _get_user(self, username, hostname):
        """Return a single user matching the criteria."""
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            client.execute("SELECT USERNAME FROM ALL_USERS "
                           "WHERE USERNAME = '%s'" % username.upper())
            users = client.fetchall()
        if client.rowcount != 1:
            return None
        try:
            user = models.OracleUser(users[0][0])
            return user
        except exception.ValueError as ve:
            LOG.exception(_("Error Getting user information"))
            raise exception.BadRequest(_("Username %(user)s is not valid"
                                         ": %(reason)s") %
                                       {'user': username, 'reason': ve.message}
                                       )

    def list_users(self, limit=None, marker=None, include_marker=False):
        """List users that have access to the database."""
        LOG.debug("---Listing Users---")
        users = []
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            # filter out Oracle system users by id
            # Oracle docs say that new users are given id's between
            # 100 and 60000
            client.execute("SELECT USERNAME FROM ALL_USERS "
                           "WHERE (USER_ID BETWEEN 100 AND 60000) "
                           "AND USERNAME != '%s'" % ROOT_USERNAME.upper())
            for row in client:
                oracle_user = models.OracleUser(row[0])
                users.append(oracle_user.serialize())
        return users, None

    def change_passwords(self, users):
        """Change the passwords of one or more users."""
        LOG.debug("Changing the passwords of some users.")
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            for item in users:
                LOG.debug("Changing password for user %s." % item.name)
                client.execute('ALTER USER %(username)s '
                               'IDENTIFIED BY %(password)s'
                               % {'username': item.name,
                                  'password': item.password})

    def update_attributes(self, username, hostname, user_attrs):
        """Change the attributes of an existing user."""
        LOG.debug("Changing user attributes for user %s." % username)
        if user_attrs.get('host'):
            raise exception.DatastoreOperationNotSupported(
                operation='update_attributes:new_host', datastore=MANAGER)
        if user_attrs.get('name'):
            raise exception.DatastoreOperationNotSupported(
                operation='update_attributes:new_name', datastore=MANAGER)
        new_password = user_attrs.get('password')
        if new_password:
            user = models.OracleUser(username, new_password)
            self.change_passwords([user])

    def enable_root(self, root_password=None):
        """Create user 'root' with the dba_user role and/or reset the root
           user password.
        """
        LOG.debug("---Enabling root user---")
        if not root_password:
            root_password = utils.generate_random_password(PASSWORD_MAX_LEN)
        root_user = models.OracleUser(ROOT_USERNAME, root_password)
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            client.execute("SELECT USERNAME FROM ALL_USERS "
                           "WHERE USERNAME = upper('%s')"
                           % root_user.name.upper())
            if client.rowcount == 0:
                client.execute("CREATE USER %(username)s "
                               "IDENTIFIED BY %(password)s"
                               % {'username': root_user.name,
                                  'password': root_user.password})
            else:
                client.execute("ALTER USER %(username)s "
                               "IDENTIFIED BY %(password)s"
                               % {'username': root_user.name,
                                  'password': root_user.password})
            client.execute("GRANT PDB_DBA TO %s" % ROOT_USERNAME)
        return root_user.serialize()

    def is_root_enabled(self):
        """Return True if root access is enabled; False otherwise."""
        LOG.debug("---Checking if root is enabled---")
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            client.execute("SELECT USERNAME FROM ALL_USERS "
                           "WHERE USERNAME = upper('%s')"
                           % ROOT_USERNAME.upper())
            return client.rowcount != 0


class OracleApp(object):
    """Prepares DBaaS on a Guest container."""

    def __init__(self, status):
        """By default login with root no password for initial setup."""
        self.state_change_wait_time = CONF.state_change_wait_time
        self.status = status

    def _needs_pdb_cleanup(self):
        if os.path.exists(CONF.get(MANAGER).oracle_ra_status_file):
            with open(CONF.get(MANAGER).oracle_ra_status_file, 'r') as ra_file:
                status = ra_file.readline()
            if status.startswith('ERROR-CONN'):
                return False
            else:
                return True
        else:
            return False

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        LOG.info(_("Deleting Oracle PDB."))
        try:
            if self._needs_pdb_cleanup():
                OracleAdmin().delete_database()
            return None
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            err = {
                'error-code': error.code,
                'error-message': error.message
            }
            return err



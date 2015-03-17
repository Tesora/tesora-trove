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

from trove.common import cfg
from trove.common import exception
from trove.common import instance as rd_instance
from trove.guestagent.db import models
from trove.guestagent.datastore import service
from trove.openstack.common import log as logging
from trove.openstack.common.gettextutils import _

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'

ORACLE_CONFIG_FILE = "/etc/oracle/oracle-proxy.cnf"
ORACLE_CONFIG_FILE_TEMP = "/tmp/oracle-proxy.cnf.tmp"
GUEST_INFO_FILE = "/etc/guest_info"

PDB_ADMIN_ID = "pdbadmin"
PDB_ADMIN_PSWD = "pdbpassword"


class OracleAppStatus(service.BaseDbStatus):
    @classmethod
    def get(cls):
        if not cls._instance:
            cls._instance = OracleAppStatus()
        return cls._instance

    def _get_actual_db_status(self):
        if os.path.exists(CONF.get(MANAGER).proxy_status_file):
            with open(CONF.get(MANAGER).proxy_status_file, 'r') as proxy_file:
                status = proxy_file.readline()
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
        with LocalOracleClient(CONF.get(MANAGER).oracle_cdb_name) as client:
            client.execute("CREATE PLUGGABLE DATABASE %(pdb_name)s "
                           "ADMIN USER %(admin_id)s "
                           "IDENTIFIED BY %(admin_pswd)s" %
                           {'pdb_name': pdb_name,
                            'admin_id': PDB_ADMIN_ID,
                            'admin_pswd': PDB_ADMIN_PSWD})
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
        oracle_user = models.OracleUser()
        oracle_user.deserialize(user)
        self.delete_user_by_name(oracle_user.name, oracle_user.host)

    def delete_user_by_name(self, name, host='%'):
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
        user = models.OracleUser()
        try:
            user.name = users[0][0]  # Could possibly throw a BadRequest here.
        except exception.ValueError as ve:
            LOG.exception(_("Error Getting user information"))
            raise exception.BadRequest(_("Username %(user)s is not valid"
                                         ": %(reason)s") %
                                       {'user': username, 'reason': ve.message}
                                       )
        return user

    def list_users(self, limit=None, marker=None, include_marker=False):
        """List users that have access to the database."""
        LOG.debug("---Listing Users---")
        users = []
        with LocalOracleClient(CONF.guest_name, service=True) as client:
            # filter out Oracle system users by id
            # Oracle docs say that new users are given id's between
            # 100 and 60000
            client.execute('SELECT USERNAME FROM ALL_USERS '
                           'WHERE (USER_ID BETWEEN 100 AND 60000) '
                           'AND USERNAME <> "%s"' % PDB_ADMIN_ID.upper())
            for row in client:
                oracle_user = models.OracleUser()
                oracle_user.name = row[0]
                # mysql_user.host = row['Host']
                # self._associate_dbs(mysql_user)
                users.append(oracle_user.serialize())
        return users, None


class OracleApp(object):
    """Prepares DBaaS on a Guest container."""

    def __init__(self, status):
        """By default login with root no password for initial setup."""
        self.state_change_wait_time = CONF.state_change_wait_time
        self.status = status

    def _needs_pdb_cleanup(self):
        if os.path.exists(CONF.get(MANAGER).proxy_status_file):
            with open(CONF.get(MANAGER).proxy_status_file, 'r') as proxy_file:
                status = proxy_file.readline()
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



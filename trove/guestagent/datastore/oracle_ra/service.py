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
from trove.common.i18n import _
from trove.guestagent.common import operating_system
from trove.guestagent.db import models
from trove.guestagent.datastore.oracle_common import service
from trove.guestagent.datastore.oracle_common import sql_query
from trove.guestagent.datastore.service import BaseDbStatus

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle_ra'

ROOT_USER_NAME = 'ROOT'


class OracleRAAppStatus(BaseDbStatus):

    def _get_actual_db_status(self):
        if os.path.exists(CONF.get(MANAGER).oracle_ra_status_file):
            status = operating_system.read_file(
                CONF.get(MANAGER).oracle_ra_status_file, as_root=True)
            if status.startswith('OK'):
                return rd_instance.ServiceStatuses.RUNNING
            elif status.startswith('ERROR'):
                return rd_instance.ServiceStatuses.UNKNOWN


class OracleRAConfig(service.OracleConfig):
    tag_host = 'host'
    tag_port = 'port'
    tag_admin_user = 'admin_user'
    tag_cdb_name = 'cdb_name'

    def __init__(self):
        self.key_names.update({
            self.tag_admin_password: 'oracle_sys_pswd',
            self.tag_host: 'oracle_host',
            self.tag_port: 'oracle_port',
            self.tag_admin_user: 'oracle_sys_usr',
            self.tag_cdb_name: 'oracle_cdb_name'})
        super(OracleRAConfig, self).__init__(CONF.get(MANAGER).conf_file)

    def store_ra_config(self, config_contents):
        base_config = operating_system.read_file(self.file_path,
                                                 codec=self._codec,
                                                 as_root=True)

        def _set(opt, val):
            name = self.key_names[opt]
            ra_config[name] = val
            self._values[opt] = val

        ra_config = base_config[self.section_name]
        for option, override in [(self.tag_host, 'oracle_host'),
                                 (self.tag_port, 'oracle_port'),
                                 (self.tag_admin_user, 'sys_usr'),
                                 (self.tag_admin_password, 'sys_pswd'),
                                 (self.tag_cdb_name, 'cdb_name')]:
            value = config_contents.get(override)
            _set(option, value)
        base_config[self.section_name] = ra_config
        operating_system.write_file(self.file_path,
                                    base_config,
                                    codec=self._codec,
                                    as_root=True)

    @property
    def host(self):
        return self._values[self.tag_host]

    @property
    def port(self):
        return self._values[self.tag_port]

    @property
    def admin_user(self):
        return self._values[self.tag_admin_user]

    @property
    def cdb_name(self):
        return self._values[self.tag_cdb_name]


class OracleRAClient(service.OracleClient):

    def __init__(self, sid,
                 hostname=None,
                 port=None,
                 user_id=None,
                 password=None,
                 use_service=True,
                 mode=cx_Oracle.SYSDBA):
        config = OracleRAConfig()
        hostname = hostname if hostname else config.host
        port = port if port else config.port
        user_id = user_id if user_id else config.admin_user
        password = password if password else config.admin_password
        super(OracleRAClient, self).__init__(
            sid, CONF.get(MANAGER).oracle_home,
            hostname, port, user_id, password, use_service, mode)


class OracleRACursor(service.OracleCursor, OracleRAClient):
    pass


class OracleRAAdmin(service.OracleAdmin):

    def __init__(self):
        super(OracleRAAdmin, self).__init__(
            OracleRAConfig, OracleRAClient, OracleRACursor,
            ROOT_USER_NAME, CONF.get(MANAGER).cloud_user_role.upper())

    def _create_database(self, db_name):
        LOG.debug("Creating pluggable database %s." % db_name)
        if not re.match(r'[a-zA-Z0-9]\w{,63}$', db_name):
            raise exception.BadRequest(
                _('Database name %(name)s is not valid. Oracle pluggable '
                  'database names restrictions: limit of 64 characters, use '
                  'only alphanumerics and underscores, cannot start with an '
                  'underscore.') % {'name': db_name})
        with self.cursor(self.ora_config.cdb_name) as cursor:
            cursor.execute(str(
                sql_query.CreatePDB(
                    db_name, self.root_user_name, service.new_oracle_password())))
            cursor.execute(str(sql_query.AlterPDB(db_name, 'OPEN')))
        LOG.debug("Successfully created pluggable database")

    def _delete_database(self, db_name):
        LOG.debug("Deleting pluggable database %s." % db_name)
        with self.cursor(self.ora_config.cdb_name) as cursor:
            try:
                cursor.execute(str(
                    sql_query.AlterPDB(db_name, 'CLOSE IMMEDIATE')))
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
            cursor.execute(str(sql_query.DropPDB(db_name)))
        LOG.debug("Finished deleting pluggable database")
        return True

    def create_pdb(self, db_name):
        self.create_database([models.OracleSchema(db_name).serialize()])

    def delete_pdb(self, db_name):
        self.delete_database(models.OracleSchema(db_name).serialize())


class OracleRAApp(service.OracleApp):

    def __init__(self, status, state_change_wait_time=None):
        super(OracleRAApp, self).__init__(
            status, OracleRAClient, OracleRACursor, OracleRAAdmin,
            state_change_wait_time)

    def _needs_pdb_cleanup(self):
        if os.path.exists(CONF.get(MANAGER).oracle_ra_status_file):
            status = operating_system.read_file(
                CONF.get(MANAGER).oracle_ra_status_file, as_root=True)
            if status.startswith('ERROR-CONN'):
                return False
            else:
                return True
        else:
            return False

    def stop_db(self, do_not_start_on_reboot=False):
        LOG.info(_("Deleting Oracle PDB."))
        try:
            if self._needs_pdb_cleanup():
                self.admin.delete_pdb(self.admin.database_name)
            return None
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            err = {
                'error-code': error.code,
                'error-message': error.message
            }
            return err

    def create_ra_status_file(self, status):
        operating_system.write_file(CONF.get(MANAGER).oracle_ra_status_file,
                                    status, as_root=True)

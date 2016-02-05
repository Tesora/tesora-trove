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

import ConfigParser
import os

import cx_Oracle

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common import utils as utils
from trove.common.i18n import _
from trove.guestagent.datastore import manager
from trove.guestagent.datastore.oracle_ra.service import OracleAppStatus
from trove.guestagent.datastore.oracle_ra.service import OracleAdmin
from trove.guestagent.datastore.oracle_ra.service import OracleApp


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle_ra'


class Manager(manager.Manager):

    def __init__(self):
        super(Manager, self).__init__()

    @property
    def status(self):
        return OracleAppStatus.get()

    def change_passwords(self, context, users):
        OracleAdmin().change_passwords(users)

    def update_attributes(self, context, username, hostname, user_attrs):
        OracleAdmin().update_attributes(username, hostname, user_attrs)

    def reset_configuration(self, context, configuration):
        raise exception.DatastoreOperationNotSupported(
            operation='reset_configuration', datastore=MANAGER)

    def create_database(self, context):
        return OracleAdmin().create_database()

    def create_user(self, context, users):
        OracleAdmin().create_user(users)

    def delete_database(self, context, database):
        return OracleAdmin().delete_database(database)

    def delete_user(self, context, user):
        OracleAdmin().delete_user(user)

    def get_user(self, context, username, hostname):
        return OracleAdmin().get_user(username, hostname)

    def grant_access(self, context, username, hostname, databases):
        raise exception.DatastoreOperationNotSupported(
            operation='grant_access', datastore=MANAGER)

    def revoke_access(self, context, username, hostname, database):
        raise exception.DatastoreOperationNotSupported(
            operation='revoke_access', datastore=MANAGER)

    def list_access(self, context, username, hostname):
        raise exception.DatastoreOperationNotSupported(
            operation='list_access', datastore=MANAGER)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        raise exception.DatastoreOperationNotSupported(
            operation='list_databases', datastore=MANAGER)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        return OracleAdmin().list_users(limit, marker,
                                        include_marker)

    def enable_root(self, context):
        return OracleAdmin().enable_root()

    def disable_root(self, context):
        LOG.debug("Disabling root.")
        raise exception.DatastoreOperationNotSupported(
            operation='disable_root', datastore=MANAGER)

    def is_root_enabled(self, context):
        return OracleAdmin().is_root_enabled()

    def _load_oracle_config(self, config_contents):
        """
        Persist the Oracle config group param values to
        /etc/trove/trove-guestagent.conf, so that the instance can
        recover the Oracle connectivity after rebooting.
        """
        TROVEGUEST_CONFIG_FILE = '/etc/trove/trove-guestagent.conf'
        TROVEGUEST_CONFIG_FILE_TEMP = '/tmp/trove-guestagent.conf.tmp'

        oracle_host = config_contents.get('oracle_host')
        oracle_port = config_contents.get('oracle_port')
        oracle_sys_usr = config_contents.get('sys_usr')
        oracle_sys_pswd = config_contents.get('sys_pswd')
        oracle_cdb_name = config_contents.get('cdb_name')

        CONF.set_override(name='oracle_host',
                          override=oracle_host,
                          group=MANAGER)
        CONF.set_override(name='oracle_port',
                          override=oracle_port,
                          group=MANAGER)
        CONF.set_override(name='oracle_sys_usr',
                          override=oracle_sys_usr,
                          group=MANAGER)
        CONF.set_override(name='oracle_sys_pswd',
                          override=oracle_sys_pswd,
                          group=MANAGER)
        CONF.set_override(name='oracle_cdb_name',
                          override=oracle_cdb_name,
                          group=MANAGER)

        config = ConfigParser.RawConfigParser()
        config.read(TROVEGUEST_CONFIG_FILE)
        config.add_section(MANAGER)
        config.set(MANAGER, 'oracle_host', oracle_host)
        config.set(MANAGER, 'oracle_port', oracle_port)
        config.set(MANAGER, 'oracle_sys_usr', oracle_sys_usr)
        config.set(MANAGER, 'oracle_sys_pswd', oracle_sys_pswd)
        config.set(MANAGER, 'oracle_cdb_name', oracle_cdb_name)

        with open(TROVEGUEST_CONFIG_FILE_TEMP, 'w') as configfile:
            config.write(configfile)

        utils.execute_with_timeout("sudo", "mv", "-f",
                                   TROVEGUEST_CONFIG_FILE_TEMP,
                                   TROVEGUEST_CONFIG_FILE)

    def _create_ra_status_file(self, status):
        RA_STATUS_FILE_TEMP = '/tmp/oracle-ra-status'
        with open(RA_STATUS_FILE_TEMP, 'w') as statusfile:
            statusfile.write(status)

        utils.execute_with_timeout("sudo", "mv", "-f",
                                   RA_STATUS_FILE_TEMP,
                                   CONF.get(MANAGER).oracle_ra_status_file)

    def do_prepare(self, context, packages, databases, memory_mb, users,
                device_path=None, mount_point=None, backup_info=None,
                config_contents=None, root_password=None, overrides=None,
                cluster_config=None, snapshot=None):
        """This is called from prepare in the base class."""
        ERROR_MSG = 'Failed to create Oracle database instance.'

        try:
            self._load_oracle_config(overrides)
        except Exception as e:
            LOG.exception(_('%s Invalid configuration detail.'
                            % ERROR_MSG))
            self._create_ra_status_file('ERROR-CONN')
            raise e

        try:
            self.create_database(context)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            LOG.exception(_(ERROR_MSG))
            if (error.code == 1017 or
               12500 <= error.code <= 12629 or
               65006 <= error.code <= 65025):
                # ORA-01017: invalid username/password; logon denied
                # ORA-12500 - 12629: TNS issues, most likely related to
                # Oracle connectivity
                # ORA-65006 - 65022: Oracle CDB or PDB issues
                # This branch distinguish Oracle issues that occurs
                # at the init stage, so that the user can later on
                # delete the resultant ERROR trove instances.
                self._create_ra_status_file('ERROR-CONN')
            else:
                self._create_ra_status_file('ERROR')
            raise e
        except Exception as e:
            LOG.exception(_(ERROR_MSG))
            self._create_ra_status_file('ERROR')
            raise e

        try:
            if users:
                self.create_user(context, users)
        except Exception as e:
            self._create_ra_status_file('ERROR')
            LOG.exception(_(ERROR_MSG))
            raise e

        self._create_ra_status_file('OK')

    def restart(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='restart', datastore=MANAGER)

    def start_db_with_conf_changes(self, context, config_contents):
        raise exception.DatastoreOperationNotSupported(
            operation='start_db_with_conf_changes', datastore=MANAGER)

    def stop_db(self, context, do_not_start_on_reboot=False):
        app = OracleApp(OracleAppStatus.get())
        return app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        raise exception.DatastoreOperationNotSupported(
            operation='get_filesystem_stats', datastore=MANAGER)

    def create_backup(self, context, backup_info):
        raise exception.DatastoreOperationNotSupported(
            operation='create_backup', datastore=MANAGER)

    def mount_volume(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='mount_volume', datastore=MANAGER)

    def unmount_volume(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='unmount_volume', datastore=MANAGER)

    def resize_fs(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='resize_fs', datastore=MANAGER)

    def update_overrides(self, context, overrides, remove=False):
        raise exception.DatastoreOperationNotSupported(
            operation='update_overrides', datastore=MANAGER)

    def apply_overrides(self, context, overrides):
        raise exception.DatastoreOperationNotSupported(
            operation='apply_overrides', datastore=MANAGER)

    def get_replication_snapshot(self, context, snapshot_info,
                                 replica_source_config=None):
        raise exception.DatastoreOperationNotSupported(
            operation='get_replication_snapshot', datastore=MANAGER)

    def attach_replication_slave(self, context, snapshot, slave_config):
        raise exception.DatastoreOperationNotSupported(
            operation='attach_replication_slave', datastore=MANAGER)

    def detach_replica(self, context, for_failover=False):
        raise exception.DatastoreOperationNotSupported(
            operation='detach_replica', datastore=MANAGER)

    def cleanup_source_on_replica_detach(self, context, replica_info):
        raise exception.DatastoreOperationNotSupported(
            operation='cleanup_source_on_replica_detach', datastore=MANAGER)

    def demote_replication_master(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='demote_replication_master', datastore=MANAGER)

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

import cx_Oracle
from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.guestagent.datastore.oracle_common import manager
from trove.guestagent.datastore.oracle_ra import service

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.OracleManager):

    def __init__(self):
        super(Manager, self).__init__(service.OracleRAApp,
                                      service.OracleRAAppStatus,
                                      manager_name='oracle_ra')

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path=None, mount_point=None, backup_info=None,
                   config_contents=None, root_password=None, overrides=None,
                   cluster_config=None, snapshot=None):
        """This is called from prepare in the base class."""
        error_msg = 'Failed to create Oracle database instance.'

        try:
            config = service.OracleRAConfig()
            config.store_ra_config(overrides)
        except Exception as e:
            LOG.exception(_('%s Invalid configuration detail.'
                            % error_msg))
            self.app.create_ra_status_file('ERROR-CONN')
            raise e

        app = self.app
        admin = self.admin

        try:
            admin.create_pdb(CONF.guest_name)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            LOG.exception(_(error_msg))
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
                app.create_ra_status_file('ERROR-CONN')
            else:
                app.create_ra_status_file('ERROR')
            raise e
        except Exception as e:
            LOG.exception(_(error_msg))
            app.create_ra_status_file('ERROR')
            raise e

        app.create_ra_status_file('OK')

    def apply_overrides_on_prepare(self, context, overrides):
        LOG.debug("Ignoring configuration for %s" % self.manager_name)
        pass

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        # Oracle remote agent don't have any filesystem info to report
        return {}

    def mount_volume(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='mount_volume', datastore=self.manager)

    def unmount_volume(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='unmount_volume', datastore=self.manager)

    def resize_fs(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='resize_fs', datastore=self.manager)

    def reset_configuration(self, context, configuration):
        raise exception.DatastoreOperationNotSupported(
            operation='reset_configuration', datastore=self.manager)

    def create_database(self, context, databases):
        raise exception.DatastoreOperationNotSupported(
            operation='create_database', datastore=self.manager)

    def delete_database(self, context, database):
        raise exception.DatastoreOperationNotSupported(
            operation='delete_database', datastore=self.manager)

    def grant_access(self, context, username, hostname, databases):
        raise exception.DatastoreOperationNotSupported(
            operation='grant_access', datastore=self.manager)

    def revoke_access(self, context, username, hostname, database):
        raise exception.DatastoreOperationNotSupported(
            operation='revoke_access', datastore=self.manager)

    def list_access(self, context, username, hostname):
        raise exception.DatastoreOperationNotSupported(
            operation='list_access', datastore=self.manager)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        raise exception.DatastoreOperationNotSupported(
            operation='list_databases', datastore=self.manager)

    def disable_root(self, context):
        LOG.debug("Disabling root.")
        raise exception.DatastoreOperationNotSupported(
            operation='disable_root', datastore=self.manager)

    def restart(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='restart', datastore=self.manager)

    def start_db_with_conf_changes(self, context, config_contents):
        raise exception.DatastoreOperationNotSupported(
            operation='start_db_with_conf_changes', datastore=self.manager)

    def update_overrides(self, context, overrides, remove=False):
        raise exception.DatastoreOperationNotSupported(
            operation='update_overrides', datastore=self.manager)

    def apply_overrides(self, context, overrides):
        raise exception.DatastoreOperationNotSupported(
            operation='apply_overrides', datastore=self.manager)

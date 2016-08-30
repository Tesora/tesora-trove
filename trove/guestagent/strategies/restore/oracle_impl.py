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

import glob
from os import path

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle import service
from trove.guestagent.strategies.restore import base

CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'
LOG = logging.getLogger(__name__)


class RmanBackup(base.RestoreRunner):
    __strategy_name__ = 'rmanbackup'
    base_restore_cmd = 'sudo tar xPf -'

    def __init__(self, *args, **kwargs):
        super(RmanBackup, self).__init__(*args, **kwargs)
        self.app = service.OracleVMApp(service.OracleVMAppStatus())
        self.content_length = 0
        self.backup_id = kwargs.get('backup_id')
        self.db_name = None

    def _perform_restore(self):
        control_file = glob.glob(path.join(self.app.paths.db_backup_dir,
                                           '*%s.ctl' % self.backup_id))[0]
        cmds = [
            'startup nomount',
            "restore controlfile from '%s'" % control_file,
            'startup mount',
            'crosscheck backup',
            'delete noprompt expired backup',
            'restore database']
        script = self.app.rman_scripter(
            commands=cmds, sid=self.db_name,
            t_user=self.app.admin_user_name,
            t_pswd=self.app.admin.ora_config.admin_password)
        script.run(timeout=CONF.restore_usage_timeout)

    def _perform_recover(self):
        script = self.app.rman_scripter(
            commands='recover database',
            sid=self.db_name,
            t_user=self.app.admin_user_name,
            t_pswd=self.app.admin.ora_config.admin_password)
        try:
            script.run(timeout=CONF.restore_usage_timeout)
        except exception.ProcessExecutionError as p:
            # Ignore the "media recovery requesting unknown archived log" error
            # because RMAN would throw this error even when recovery is
            # successful.
            # If there are in fact errors when recovering the database, the
            # database open step following will fail anyway.
            if str(p).find('media recovery requesting '
                           'unknown archived log') != -1:
                pass
            else:
                raise p

    def _open_database(self):
        script = self.app.rman_scripter(
            commands='alter database open resetlogs',
            sid=self.db_name,
            t_user=self.app.admin_user_name,
            t_pswd=self.app.admin.ora_config.admin_password)
        script.run(timeout=CONF.get(MANAGER).usage_timeout)

    def _unpack_backup_files(self, location, checksum):
        LOG.debug("Restoring full backup files.")
        self.content_length = self._unpack(location, checksum, self.restore_cmd)

    def _run_restore(self):
        metadata = self.storage.load_metadata(self.location, self.checksum)
        self.db_name = metadata['db_name']
        self.app.paths.update_db_name(self.db_name)

        new_dirs = [self.app.paths.audit_dir,
                    self.app.paths.db_fast_recovery_logs_dir,
                    self.app.paths.db_fast_recovery_dir,
                    self.app.paths.db_data_dir]
        for new_dir in new_dirs:
            operating_system.create_directory(
                new_dir,
                user=self.app.instance_owner,
                group=self.app.instance_owner_group,
                force=True, as_root=True)

        # the backup set will restore directly to ORADATA/backupset_files
        self._unpack_backup_files(self.location, self.checksum)

        if operating_system.exists(self.app.paths.base_spfile, as_root=True):
            operating_system.copy(self.app.paths.base_spfile,
                                  self.app.paths.spfile,
                                  preserve=True, as_root=True)

        # the conf file was just restored by the unpack so sync now
        self.app.admin.delete_conf_cache()
        self.app.admin.ora_config.db_name = self.db_name

        chown_dirs = [self.app.paths.backup_dir,
                      self.app.paths.fast_recovery_area]
        for chown_dir in chown_dirs:
            operating_system.chown(
                chown_dir,
                self.app.instance_owner, self.app.instance_owner_group,
                recursive=True, force=True, as_root=True)

        self._perform_restore()
        self._perform_recover()
        self._open_database()

    def _create_oratab_entry(self):
        oratab = self.app.paths.oratab_file
        file_content = operating_system.read_file(oratab, as_root=True)
        file_content += ("\n%(db_name)s:%(ora_home)s:N\n" %
                         {'db_name': self.db_name,
                          'ora_home': self.app.paths.oracle_home})
        operating_system.write_file(oratab, file_content, as_root=True)
        operating_system.chown(oratab,
                               self.app.instance_owner,
                               self.app.instance_owner_group,
                               recursive=True, force=True, as_root=True)

    def post_restore(self):
        self._create_oratab_entry()
        operating_system.remove(self.app.paths.backup_dir,
                                force=True, as_root=True, recursive=True)


class RmanBackupIncremental(RmanBackup):

    def _unpack_backup_files(self, location, checksum):
        LOG.debug("Restoring incremental backup files.")
        metadata = self.storage.load_metadata(location, checksum)
        if 'parent_location' in metadata:
            LOG.info(_("Restoring parent: %(parent_location)s "
                       "checksum: %(parent_checksum)s.") % metadata)
            parent_location = metadata['parent_location']
            parent_checksum = metadata['parent_checksum']
            self._unpack_backup_files(parent_location, parent_checksum)

        self.content_length += self._unpack(location, checksum,
                                            self.restore_cmd)

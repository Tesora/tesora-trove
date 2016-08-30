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

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle import service
from trove.guestagent.datastore.oracle_common import sql_query
from trove.guestagent.strategies.backup import base

CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'
LOG = logging.getLogger(__name__)


class RmanBackup(base.BackupRunner):
    """Implementation of Backup Strategy for RMAN."""
    __strategy_name__ = 'rmanbackup'

    def __init__(self, *args, **kwargs):
        self.app = service.OracleVMApp(service.OracleVMAppStatus())
        self.db_name = self.app.admin.database_name
        self.backup_id = kwargs.get('filename')
        self.backup_level = 0
        super(RmanBackup, self).__init__(*args, **kwargs)

    def _run_pre_backup(self):
        """Create backupset in backup dir"""
        self.cleanup()
        try:
            est_backup_size = self.estimate_backup_size()
            avail = operating_system.get_bytes_free_on_fs(
                self.app.paths.data_dir)
            if est_backup_size > avail:
                # TODO(schang): BackupRunner will leave the trove instance
                # in a BACKUP state
                raise OSError(_("Need more free space to run RMAN backup, "
                                "estimated %(est_backup_size)s "
                                "and found %(avail)s bytes free.") %
                              {'est_backup_size': est_backup_size,
                               'avail': avail})
            bkp_dir = self.app.paths.db_backup_dir
            operating_system.create_directory(
                bkp_dir,
                user=self.app.instance_owner,
                group=self.app.instance_owner_group,
                force=True,
                as_root=True)
            cmds = [
                "configure backup optimization on",
                ("backup incremental level=%s as compressed backupset "
                 "database format '%s/%%I_%%u_%%s_%s.dat' plus archivelog"
                 % (self.backup_level, bkp_dir, self.backup_id)),
                ("backup current controlfile format '%s/%%I_%%u_%%s_%s.ctl'"
                 % (bkp_dir, self.backup_id))]
            script = self.app.rman_scripter(
                commands=cmds, sid=self.db_name,
                t_user=self.app.admin_user_name,
                t_pswd=self.app.admin.ora_config.admin_password)
            script.run(timeout=CONF.restore_usage_timeout)

        except exception.ProcessExecutionError as e:
            LOG.debug("Caught exception when creating backup files")
            self.cleanup()
            raise e

    @property
    def cmd(self):
        """Tars and streams the backup data to the stdout"""
        cmd = 'sudo tar cPf - %s %s %s %s %s' % (
            self.app.paths.backup_dir,
            self.app.paths.redo_logs_backup_dir,
            self.app.paths.orapw_file,
            self.app.paths.base_spfile,
            CONF.get(MANAGER).conf_file)
        return cmd + self.zip_cmd + self.encrypt_cmd

    def cleanup(self):
        operating_system.remove(self.app.paths.backup_dir,
                                force=True, as_root=True, recursive=True)

    def _run_post_backup(self):
        self.cleanup()

    def metadata(self):
        LOG.debug("Getting metadata from backup.")
        meta = {'db_name': self.db_name}
        LOG.info(_("Metadata for backup: %s.") % str(meta))
        return meta

    def estimate_backup_size(self):
        """Estimate the backup size. The estimation is 1/3 the total size
        of datafiles, which is a conservative figure derived from the
        Oracle RMAN backupset compression ratio.
        """
        with self.app.cursor(self.db_name) as cursor:
            cursor.execute(str(sql_query.Query(
                columns=['sum(bytes)'],
                tables=['dba_data_files'])))
            result = cursor.fetchall()
            return result[0][0] / 3


class RmanBackupIncremental(RmanBackup):
    """RMAN incremental backup."""

    def __init__(self, *args, **kwargs):
        super(RmanBackupIncremental, self).__init__(*args, **kwargs)
        self.parent_id = kwargs.get('parent_id')
        self.parent_location = kwargs.get('parent_location')
        self.parent_checksum = kwargs.get('parent_checksum')
        self.backup_level = 1

    def _truncate_backup_chain(self):
        """Truncate all backups in the backup chain after the
        specified parent backup."""

        max_recid = sql_query.Query(
            columns=['max(recid)'],
            tables=['v$backup_piece'],
            where=["handle like '%%%s%%'" % self.parent_id])
        q = sql_query.Query(
            columns=['recid'],
            tables=['v$backup_piece'],
            where=['recid > (%s)' % str(max_recid)])
        with self.app.cursor(self.db_name) as cursor:
            cursor.execute(str(q))
            delete_list = [str(row[0]) for row in cursor]

        if delete_list:
            self.app.rman_scripter(
                commands='delete force noprompt backupset %s'
                         % ','.join(delete_list),
                sid=self.db_name,
                t_user=self.app.admin_user_name,
                t_pswd=self.app.admin.ora_config.admin_password).run()

    def _run_pre_backup(self):
        # Delete from the control file backups that are no longer valid in Trove
        self._truncate_backup_chain()
        # Perform incremental backup
        super(RmanBackupIncremental, self)._run_pre_backup()

    def metadata(self):
        _meta = super(RmanBackupIncremental, self).metadata()
        _meta.update({
            'parent_location': self.parent_location,
            'parent_checksum': self.parent_checksum,
        })
        return _meta

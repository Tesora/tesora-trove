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
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle import (
    service as oracle_service)
from trove.guestagent.datastore.oracle import sql_query
from trove.guestagent.datastore.oracle.service import LocalOracleClient
from trove.guestagent.db import models
from trove.guestagent.strategies.backup import base

CONF = cfg.CONF

LOG = logging.getLogger(__name__)
LARGE_TIMEOUT = 1200
BACKUP_DIR = CONF.get('oracle').mount_point + '/backupset_files'
REDO_LOGS_BKUP_DIR = (CONF.get('oracle').fast_recovery_area +
                      '/%(db_name)s/backupset')
ORACLE_HOME = CONF.get('oracle').oracle_home
CONF_FILE = CONF.get('oracle').conf_file
ADMIN_USER = 'os_admin'

class RmanBackup(base.BackupRunner):
    """Implementation of Backup Strategy for RMAN."""
    __strategy_name__ = 'rmanbackup'

    def __init__(self, *args, **kwargs):
        self.status = oracle_service.OracleAppStatus()
        self.ora_admin = oracle_service.OracleAdmin()
        self.oracnf = oracle_service.OracleConfig()
        self.db_name = self._get_db_name()
        self.backup_id = kwargs.get('filename')
        self.backup_level = 0
        super(RmanBackup, self).__init__(*args, **kwargs)

    def _get_db_name(self):
        dbs, marker = self.ora_admin.list_databases()
        # There will be only one Oracle database per trove instance
        oradb = models.OracleSchema.deserialize_schema(dbs[0])
        return oradb.name

    def _run_pre_backup(self):
        """Create backupset in backup dir"""
        self.cleanup()
        operating_system.create_directory(BACKUP_DIR,
                                          user='oracle', group='oinstall',
                                          force=True, as_root=True)
        try:
            est_backup_size = self.estimate_backup_size()
            avail = operating_system.get_bytes_free_on_fs(CONF.get('oracle').
                                                          mount_point)
            if est_backup_size > avail:
                # TODO(schang): BackupRunner will leave the trove instance
                # in a BACKUP state
                raise OSError(_("Need more free space to run RMAN backup, "
                                "estimated %(est_backup_size)s"
                                " and found %(avail)s bytes free ") %
                              {'est_backup_size': est_backup_size,
                               'avail': avail})
            backup_dir = (BACKUP_DIR + '/%s') % self.db_name
            operating_system.create_directory(backup_dir,
                                              user='oracle',
                                              group='oinstall',
                                              force=True,
                                              as_root=True)
            backup_cmd = ("""\"\
rman target %(admin_user)s/%(admin_pswd)s@localhost/%(db_name)s <<EOF
run {
configure backup optimization on;
backup incremental level=%(backup_level)s as compressed backupset database format '%(backup_dir)s/%%I_%%u_%%s_%(backup_id)s.dat' plus archivelog;
backup current controlfile format '%(backup_dir)s/%%I_%%u_%%s_%(backup_id)s.ctl';
}
EXIT;
EOF\"
""" % {'admin_user': ADMIN_USER, 'admin_pswd': self.oracnf.admin_password,
       'db_name': self.db_name, 'backup_dir': backup_dir,
       'backup_id': self.backup_id, 'backup_level': self.backup_level})
            utils.execute_with_timeout("su - oracle -c " + backup_cmd,
                                       run_as_root=True,
                                       root_helper='sudo',
                                       timeout=LARGE_TIMEOUT,
                                       shell=True,
                                       log_output_on_error=True)

        except exception.ProcessExecutionError as e:
            LOG.debug("Caught exception when creating backup files")
            self.cleanup()
            raise e

    def _get_sp_pw_files(self):
        """Create a list of sp and password files to be backed up"""
        result = ['%(ora_home)s/dbs/orapw%(db_name)s' %
                  {'ora_home': ORACLE_HOME, 'db_name': self.db_name},
                  '%(ora_home)s/dbs/spfile%(db_name)s.ora' %
                  {'ora_home': ORACLE_HOME, 'db_name': self.db_name}]
        return result;

    @property
    def cmd(self):
        """Tars and streams the backup data to the stdout"""
        cmd = ('sudo tar cPf - %(backup_dir)s %(sp_pw_files)s %(conf_file)s '
               '%(redo_logs_backup)s' %
               {'backup_dir': BACKUP_DIR,
                'redo_logs_backup': REDO_LOGS_BKUP_DIR %
                {'db_name': self.db_name.upper()},
                'sp_pw_files': ' '.join(self._get_sp_pw_files()),
                'conf_file': CONF_FILE})

        return cmd + self.zip_cmd + self.encrypt_cmd

    def cleanup(self):
        operating_system.remove(BACKUP_DIR, force=True, as_root=True,
                                recursive=True)
        operating_system.remove(REDO_LOGS_BKUP_DIR %
                                {'db_name': self.db_name.upper()}, force=True,
                                as_root=True, recursive=True)

    def _run_post_backup(self):
        self.cleanup()

    def metadata(self):
        LOG.debug('Getting metadata from backup.')
        meta = {'db_name': self.db_name}
        LOG.info(_("Metadata for backup: %s.") % str(meta))
        return meta

    def estimate_backup_size(self):
        """Estimate the backup size. The estimation is 1/3 the total size
        of datafiles, which is a conservative figure derived from the
        Oracle RMAN backupset compression ratio."""
        with oracle_service.LocalOracleClient(self.db_name,
                                              service=True) as client:
            q = sql_query.Query()
            q.columns = ["sum(bytes)"]
            q.tables = ["dba_data_files"]
            client.execute(str(q))
            result = client.fetchall()
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

        with LocalOracleClient(self.db_name, service=True) as client:
            max_recid = sql_query.Query()
            max_recid.columns = ["max(recid)"]
            max_recid.tables = ["v$backup_piece"]
            max_recid.where = ["handle like '%%%s%%'" % self.parent_id]

            q = sql_query.Query()
            q.columns = ["recid"]
            q.tables = ["v$backup_piece"]
            q.where = ["recid > (%s)" % str(max_recid)]
            client.execute(str(q))
            delete_list = [ str(row[0]) for row in client ]

        if delete_list:
            cmd = ("""\"\
rman target %(admin_user)s/%(admin_pswd)s@localhost/%(db_name)s <<EOF
run {
delete force noprompt backupset %(delete_list)s;
}
EXIT;
EOF\"
""" % {'admin_user': ADMIN_USER, 'admin_pswd': self.oracnf.admin_password,
       'db_name': self.db_name, 'delete_list': ",".join(delete_list)})
            utils.execute_with_timeout("su - oracle -c " + cmd,
                                       run_as_root=True,
                                       root_helper='sudo',
                                       timeout=LARGE_TIMEOUT,
                                       shell=True,
                                       log_output_on_error=True)

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
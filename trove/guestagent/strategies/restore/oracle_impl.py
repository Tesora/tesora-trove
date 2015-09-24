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

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle import (
    service as oracle_service)
from trove.guestagent.strategies.restore import base

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
LARGE_TIMEOUT = 1200
ORA_PATH = '/u01/app/oracle'
ORA_DATA_PATH = CONF.get('oracle').mount_point
ORA_FAST_RECOVERY_PATH =  ORA_PATH + '/fast_recovery_area'
ORA_AUDIT_PATH = ORA_PATH + '/admin/%(db)s/adump'
ORA_BACKUP_PATH = ORA_DATA_PATH + '/backupset_files'
ORATAB_PATH = '/etc/oratab'
ORACLE_HOME = CONF.get('oracle').oracle_home
ADMIN_USER = 'os_admin'
ADMIN_PSWD = oracle_service.OracleConfig().admin_password

class RmanBackup(base.RestoreRunner):
    __strategy_name__ = 'rmanbackup'
    base_restore_cmd = 'sudo tar xPf -'

    def __init__(self, *args, **kwargs):
        super(RmanBackup, self).__init__(*args, **kwargs)
        self.status = oracle_service.OracleAppStatus()
        self.app = oracle_service.OracleApp(self.status)
        self.content_length = 0
        self.backup_id = kwargs.get('backup_id')
        self.db_name = ''

    def _perform_restore(self):
        control_file = glob.glob(ORA_BACKUP_PATH  + '/' +
                                 self.db_name +
                                 '/*' + self.backup_id + '.ctl')[0]
        restore_cmd = ("""\"export ORACLE_SID=%(db_name)s
rman target %(admin_user)s/%(admin_pswd)s <<EOF
run {
startup nomount;
restore controlfile from '%(ctl_file)s';
startup mount;
crosscheck backup;
delete noprompt expired backup;
restore database;
}
EXIT;
EOF\"
""" % {'admin_user': ADMIN_USER, 'admin_pswd': ADMIN_PSWD,
       'db_name': self.db_name, 'ctl_file': control_file})
        cmd = "su - oracle -c " + restore_cmd
        utils.execute_with_timeout(cmd,
                                   run_as_root=True, root_helper='sudo',
                                   timeout=LARGE_TIMEOUT,
                                   shell=True, log_output_on_error=True)

    def _perform_recover(self):
        recover_cmd = ("""\"export ORACLE_SID=%(db_name)s
rman target %(admin_user)s/%(admin_pswd)s <<EOF
run {
recover database;
}
EXIT;
EOF\"
""" % {'admin_user': ADMIN_USER, 'admin_pswd': ADMIN_PSWD,
       'db_name': self.db_name})
        cmd = "su - oracle -c " + recover_cmd
        try:
            utils.execute_with_timeout(cmd,
                                       run_as_root=True, root_helper='sudo',
                                       timeout=LARGE_TIMEOUT,
                                       shell=True, log_output_on_error=True)
        except exception.ProcessExecutionError as p:
            # Ignore the "media recovery requesting unknown archived log" error
            # because RMAN would throw this error even when recovery is
            # successful.
            # If there are in fact errors when recovering the database, the
            # database open step following will fail anyway.
            if str(p).find('media recovery requesting unknown archived log') != -1:
                pass
            else:
                raise(p)

    def _open_database(self):
        open_cmd = ("""\"export ORACLE_SID=%(db_name)s
rman target %(admin_user)s/%(admin_pswd)s <<EOF
run {
alter database open resetlogs;
}
EXIT;
EOF\"
""" % {'admin_user': ADMIN_USER, 'admin_pswd': ADMIN_PSWD,
       'db_name': self.db_name})
        cmd = "su - oracle -c " + open_cmd
        utils.execute_with_timeout(cmd,
                                   run_as_root=True, root_helper='sudo',
                                   timeout=LARGE_TIMEOUT,
                                   shell=True, log_output_on_error=True)

    def _unpack_backup_files(self, location, checksum):
        LOG.debug("Restoring full backup files")
        self.content_length = self._unpack(location, checksum, self.restore_cmd)

    def _run_restore(self):
        metadata = self.storage.load_metadata(self.location, self.checksum)
        self.db_name = metadata['db_name']
        operating_system.create_directory(ORA_FAST_RECOVERY_PATH,
                                          user='oracle', group='oinstall', force=True,
                                          as_root=True)
        operating_system.create_directory(ORA_AUDIT_PATH % {'db': self.db_name},
                                          user='oracle', group='oinstall',
                                          force=True, as_root=True)
        operating_system.create_directory(ORA_FAST_RECOVERY_PATH + '/' + self.db_name,
                                          user='oracle', group='oinstall',
                                          force=True, as_root=True)
        operating_system.create_directory(ORA_DATA_PATH + '/' + self.db_name,
                                          user='oracle', group='oinstall',
                                          force=True, as_root=True)
        # the backup set will restore directly to ORADATA/backupset_files
        self._unpack_backup_files(self.location, self.checksum)

        operating_system.chown(ORA_BACKUP_PATH, 'oracle', 'oinstall',
                               recursive=True, force=True, as_root=True)

        self._perform_restore()
        self._perform_recover()
        self._open_database()

    def _create_oratab_entry(self):
        """Create in the /etc/oratab file entries for the databases being
        restored"""
        file_content = operating_system.read_file(ORATAB_PATH)
        file_content += ("\n%(db_name)s:%(ora_home)s:N\n" %
                         {'db_name': self.db_name, 'ora_home': ORACLE_HOME})
        operating_system.write_file(ORATAB_PATH, file_content, as_root=True)
        operating_system.chown(ORATAB_PATH, 'oracle', 'oinstall',
                               recursive=True, force=True, as_root=True)

    def post_restore(self):
        self._create_oratab_entry();
        operating_system.remove(ORA_BACKUP_PATH, force=True, as_root=True,
                                recursive=True)


class RmanBackupIncremental(RmanBackup):

    def _unpack_backup_files(self, location, checksum):
        LOG.debug("Restoring incremental backup files")
        metadata = self.storage.load_metadata(location, checksum)
        if 'parent_location' in metadata:
            LOG.info(_("Restoring parent: %(parent_location)s"
                       " checksum: %(parent_checksum)s.") % metadata)
            parent_location = metadata['parent_location']
            parent_checksum = metadata['parent_checksum']
            self._unpack_backup_files(parent_location, parent_checksum)

        self.content_length += self._unpack(location, checksum, self.restore_cmd)

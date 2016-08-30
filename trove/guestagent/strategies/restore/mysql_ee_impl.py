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

import hashlib
import re

from oslo_log import log as logging

from trove.common import cfg
from trove.common.i18n import _
from trove.guestagent.common import operating_system
import trove.guestagent.strategies.restore.mysql_impl as mysql_restore

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
MANAGER = 'mysql_ee'
MYSQL_BACKUP_DIR = CONF.get(MANAGER).backup_dir
MYSQL_DATA_DIR = '%s/data' % CONF.get(MANAGER).mount_point
RESTORE_LOG = '/tmp/mysqlrestore.log'
BACKUP_KEY = hashlib.sha256(CONF.backup_aes_cbc_key).hexdigest()


class MySqlBackup(mysql_restore.InnoBackupEx):
    """Implementation of Restore Strategy for MySqlBackup."""
    __strategy_name__ = 'MySqlBackup'

    def __init__(self, *args, **kwargs):
        self._app = None
        super(MySqlBackup, self).__init__(*args, **kwargs)
        self.decrypt_param = (' --decrypt --key=%s' % BACKUP_KEY
                              if self.is_encrypted else '')
        self.uncompress_param = (' --uncompress' if self.is_zipped else '')
        self.restore_cmd = (('sudo mysqlbackup --backup-image=-'
                             ' --backup-dir=%(bkp_dir)s'
                             ' --datadir=%(data_dir)s' +
                             self.uncompress_param +
                             self.decrypt_param +
                             ' copy-back-and-apply-log'
                             ' 2>%(restore_log)s') %
                            {'bkp_dir': MYSQL_BACKUP_DIR,
                             'data_dir': MYSQL_DATA_DIR,
                             'restore_log': RESTORE_LOG})

    def check_process(self):
        """Check the output from mysqlbackup for 'completed OK!'."""
        LOG.debug('Checking mysqlbackup restore process output.')
        with open(RESTORE_LOG, 'r') as restore_log:
            output = restore_log.read()
            LOG.info(output)
            if not output:
                LOG.error(_("mysqlbackup restore log file empty."))
                return False
            last_line = output.splitlines()[-1].strip()
            if not re.search('completed OK!', last_line):
                LOG.error(_("mysqlbackup restore did not complete "
                            "successfully."))
                return False
        return True

    def pre_restore(self):
        super(MySqlBackup, self).pre_restore()
        if operating_system.exists(MYSQL_BACKUP_DIR, is_directory=True,
                                   as_root=True):
            operating_system.create_directory(MYSQL_BACKUP_DIR,
                                              as_root=True)

    def _run_prepare(self):
        pass


class MySqlBackupIncremental(mysql_restore.InnoBackupExIncremental,
                             MySqlBackup):

    def __init__(self, *args, **kwargs):
        super(MySqlBackupIncremental, self).__init__(*args, **kwargs)

    def _incremental_restore_cmd(self, incremental_dir):
        """Return a command for a restore with a incremental location."""
        cmd = (('sudo mysqlbackup --backup-image=-'
                ' --incremental --incremental-backup-dir=%(bkp_dir)s'
                ' --datadir=%(data_dir)s' +
                self.decrypt_param +
                ' copy-back-and-apply-log'
                ' 2>%(restore_log)s') %
               {'bkp_dir': incremental_dir,
                'data_dir': MYSQL_DATA_DIR,
                'restore_log': RESTORE_LOG})
        return self.unzip_cmd + cmd

    def _incremental_prepare(self, incremental_dir):
        pass

    def _run_prepare(self):
        pass

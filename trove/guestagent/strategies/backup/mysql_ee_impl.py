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
import trove.guestagent.strategies.backup.mysql_impl as mysql_backup

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
MANAGER = 'mysql_ee'
MYSQL_BACKUP_DIR = CONF.get(MANAGER).backup_dir


class MySqlBackup(mysql_backup.InnoBackupEx):
    """Implementation of full backup strategy for mysqlbackup."""
    __strategy_name__ = 'mysqlbackup'
    log_file_path = '/tmp/mysqlbackup.log'
    encrypt_param = ' --encrypt --key=%s' % hashlib.sha256(
        CONF.backup_aes_cbc_key).hexdigest()
    compress_param = ' --compress'

    @property
    def cmd(self):
        args = {'bkp_dir': MYSQL_BACKUP_DIR,
                'bkp_key': hashlib.sha256(
                    CONF.backup_aes_cbc_key).hexdigest(),
                'log_path': self.log_file_path,
                'extra_opts': '%(extra_opts)s'}
        cmd = ("sudo mysqlbackup"
               " --with-timestamp"
               " --backup-image=-"
               " --backup_dir=%(bkp_dir)s" +
               (self.compress_param if self.is_zipped else '') +
               (self.encrypt_param if self.is_encrypted else '') +
               " %(extra_opts)s" +
               self.user_and_pass +
               " backup-to-image"
               " 2>%(log_path)s") % args
        return cmd

    @property
    def lsn_regex(self):
        return "\s+Was able to parse the log up to lsn (\d+)."

    def _run_pre_backup(self):
        if operating_system.exists(MYSQL_BACKUP_DIR, is_directory=True,
                                   as_root=True):
            operating_system.create_directory(MYSQL_BACKUP_DIR,
                                              as_root=True)

    def check_process(self):
        """Check the output from mysqlbackup for 'completed OK!'."""
        LOG.debug('Checking mysqlbackup process output.')
        with open(self.log_file_path, 'r') as backup_log:
            output = backup_log.read()
            LOG.info(output)
            if not output:
                LOG.error(_("mysqlbackup log file empty."))
                return False
            last_line = output.splitlines()[-1].strip()
            if not re.search('completed OK!', last_line):
                LOG.error(_("mysqlbackup did not complete successfully."))
                return False
        return True

    @property
    def filename(self):
        return self.base_filename


class MySqlBackupIncremental(MySqlBackup):
    """Implementation of incremental backup strategy for mysqlbackup."""

    def __init__(self, *args, **kwargs):
        if not kwargs.get('lsn'):
            raise AttributeError('lsn attribute missing, bad parent?')
        super(MySqlBackupIncremental, self).__init__(*args, **kwargs)
        self.parent_location = kwargs.get('parent_location')
        self.parent_checksum = kwargs.get('parent_checksum')

    @property
    def cmd(self):
        args = {'bkp_dir': MYSQL_BACKUP_DIR,
                'bkp_key': hashlib.sha256(
                    CONF.backup_aes_cbc_key).hexdigest(),
                'log_path': self.log_file_path,
                'lsn': '%(lsn)s',
                'extra_opts': '%(extra_opts)s'}
        cmd = ("sudo mysqlbackup"
               " --incremental --start-lsn=%(lsn)s"
               " --with-timestamp"
               " --backup-image=-"
               " --backup_dir=%(bkp_dir)s" +
               (self.encrypt_param if self.is_encrypted else '') +
               " %(extra_opts)s" +
               self.user_and_pass +
               " backup-to-image"
               " 2>%(log_path)s") % args
        # MySql Enterprise Backup do not natively support compression of
        # incremental backups
        return cmd + self.zip_cmd

    def metadata(self):
        _meta = super(MySqlBackupIncremental, self).metadata()
        _meta.update({
            'parent_location': self.parent_location,
            'parent_checksum': self.parent_checksum,
        })
        return _meta

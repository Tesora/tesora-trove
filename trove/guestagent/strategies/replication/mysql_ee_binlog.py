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

from oslo_log import log as logging

from trove.common import cfg
from trove.common.i18n import _
from trove.common import stream_codecs
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.mysql.service import MySqlApp
from trove.guestagent.strategies import backup
from trove.guestagent.strategies.replication import mysql_binlog

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
BACKUP_STRATEGY = 'MySqlBackup'
BACKUP_INCR_STRATEGY = 'MySqlBackupIncremental'
BACKUP_NAMESPACE = 'trove.guestagent.strategies.backup.mysql_ee_impl'


class MysqlEEBinLogReplication(mysql_binlog.MysqlBinlogReplication):
    """MySqlEE Replication coordinated by GTIDs."""

    @property
    def repl_backup_runner(self):
        return backup.get_backup_strategy(BACKUP_STRATEGY,
                                          BACKUP_NAMESPACE)

    @property
    def repl_incr_backup_runner(self):
        return backup.get_backup_strategy(BACKUP_INCR_STRATEGY,
                                          BACKUP_NAMESPACE)

    @property
    def repl_backup_extra_opts(self):
        return CONF.backup_runner_options.get(BACKUP_STRATEGY, '')

    def _read_log_position(self):
        backup_var_file = ('%s/backup_variables.txt' %
                           MySqlApp.get_data_dir())
        if operating_system.exists(backup_var_file):
            try:
                LOG.info(_("Reading log position from %s") % backup_var_file)
                backup_vars = operating_system.read_file(
                    backup_var_file,
                    stream_codecs.PropertiesCodec(delimiter='='),
                    as_root=True)
                binlog_position = backup_vars['binlog_position']
                binlog_file, binlog_pos = binlog_position.split(':')
                return {
                    'log_file': binlog_file,
                    'log_position': int(binlog_pos)
                }
            except Exception as ex:
                LOG.exception(ex)
                raise self.UnableToDetermineBinlogPosition(
                    {'binlog_file': backup_var_file})
        else:
            LOG.info(_("Log position detail not available. "
                       "Using default values."))
            return {'log_file': '',
                    'log_position': 4}

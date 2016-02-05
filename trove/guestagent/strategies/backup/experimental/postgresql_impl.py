# Copyright (c) 2013 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import re
import stat

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.experimental.postgresql import pgutil
from trove.guestagent.datastore.experimental.postgresql.service.config \
    import PgSqlConfig
from trove.guestagent.datastore.experimental.postgresql.service.users \
    import PgSqlUsers
from trove.guestagent.strategies.backup import base

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
WAL_ARCHIVE_DIR = CONF.postgresql.wal_archive_location


class PgDump(base.BackupRunner):
    """Implementation of Backup Strategy for pg_dump."""
    __strategy_name__ = 'pg_dump'

    @property
    def cmd(self):
        cmd = 'sudo -u postgres pg_dumpall '
        return cmd + self.zip_cmd + self.encrypt_cmd


class PgBaseBackupUtil(object):
    def most_recent_backup_wal(self, pos=0):
        """
        Return the WAL file for the most recent backup
        """
        mrb = self.most_recent_backup_file(pos=pos)
        return mrb.split(".")[0]

    def most_recent_backup_file(self, pos=0):
        """
        Look for the most recent .backup file that basebackup creates
        :return: a string like 000000010000000000000006.00000168.backup
        """
        walre = re.compile("[0-9A-F]{24}.*.backup")
        b = [f for f in os.listdir(WAL_ARCHIVE_DIR)
             if walre.search(f)]
        b = sorted(b, reverse=True)
        if not b:
            return None
        return b[pos]

    def log_files_since_last_backup(self, pos=0):
        """Return the WAL files since the provided last backup
        pg_archivebackup depends on alphanumeric sorting to decide wal order,
        so we'll do so too:
        https://github.com/postgres/postgres/blob/REL9_4_STABLE/contrib
           /pg_archivecleanup/pg_archivecleanup.c#L122
        """
        last_wal = self.most_recent_backup_wal(pos=pos)
        d = os.listdir(WAL_ARCHIVE_DIR)
        LOG.info("Using %s for most recent wal file" % last_wal)
        LOG.info("wal archive dir contents" + str(d))
        walre = re.compile("^[0-9A-F]{24}$")
        wal = [f for f in d
               if walre.search(f)
               and f >= last_wal]
        return wal


class PgBaseBackup(base.BackupRunner, PgSqlConfig, PgBaseBackupUtil,
                   PgSqlUsers):
    """Base backups are taken with the pg_basebackup filesystem-level backup
     tool pg_basebackup creates a copy of the binary files in the PostgreSQL
     cluster data directory and enough WAL segments to allow the database to
     be brought back to a consistent state. Associated with each backup is a
     log location, normally indicated by the WAL file name and the position
     inside the file.
     """
    __strategy_name__ = 'pg_basebackup'

    def __init__(self, *args, **kwargs):
        super(PgBaseBackup, self).__init__(*args, **kwargs)
        self.label = None
        self.stop_segment = None
        self.start_segment = None
        self.start_wal_file = None
        self.stop_wal_file = None
        self.checkpoint_location = None
        self.mrb = None

    @property
    def cmd(self):
        cmd = "pg_basebackup -h %s -U %s --pgdata=-" \
              " --label=%s --format=tar --xlog " % \
              (self.UNIX_SOCKET_DIR, self.ADMIN_USER, self.base_filename)

        return cmd + self.zip_cmd + self.encrypt_cmd

    def base_backup_metadata(self, f):
        """Parse the contents of the .backup file"""
        meta = {}
        operating_system.chmod(f, FileMode(add=[stat.S_IROTH]), as_root=True)

        start_re = re.compile("START WAL LOCATION: (.*) \(file (.*)\)")
        stop_re = re.compile("STOP WAL LOCATION: (.*) \(file (.*)\)")
        checkpt_re = re.compile("CHECKPOINT LOCATION: (.*)")
        label_re = re.compile("LABEL: (.*)")

        with open(f, 'r') as base_metadata:
            lines = "\n".join(base_metadata.readlines())

            match = start_re.search(lines)
            if match:
                self.start_segment = meta['start-segment'] = match.group(1)
                self.start_wal_file = meta['start-wal-file'] = match.group(2)

            match = stop_re.search(lines)
            if match:
                self.stop_segment = meta['stop-segment'] = match.group(1)
                self.stop_wal_file = meta['stop-wal-file'] = match.group(2)

            match = checkpt_re.search(lines)
            if match:
                self.checkpoint_location \
                    = meta['checkpoint-location'] = match.group(1)

            match = label_re.search(lines)
            if match:
                self.label = meta['label'] = match.group(1)
        return meta

    def check_process(self):
        """If any of the below variables were not set by either metadata()
           or direct retrieval from the pgsql backup commands, then something
           has gone wrong
        """
        if not self.start_segment or not self.start_wal_file:
            LOG.info("Unable to determine starting WAL file/segment")
            return False
        if not self.stop_segment or not self.stop_wal_file:
            LOG.info("Unable to determine ending WAL file/segment")
            return False
        if not self.label:
            LOG.info("No backup label found")
            return False
        return True

    def metadata(self):
        """pg_basebackup may complete, and we arrive here before the
        history file is written to the wal archive. So we need to
        handle two possibilities:
        - this is the first backup, and no history file exists yet
        - this isn't the first backup, and so the history file we retrieve
        isn't the one we just ran!
         """
        def _metadata_found():
            LOG.debug("Polling for backup metadata... ")
            self.mrb = self.most_recent_backup_file()
            if not self.mrb:
                LOG.debug("No history files found!")
                return False
            meta = self.base_backup_metadata(
                os.path.join(WAL_ARCHIVE_DIR, self.mrb))
            LOG.debug("Label to pg_basebackup: %s label found: %s" %
                      (self.base_filename, meta['label']))
            LOG.info(_("Metadata for backup: %s.") % str(meta))
            return meta['label'] == self.base_filename

        try:
            utils.poll_until(_metadata_found, sleep_time=5, time_out=60)
        except exception.PollTimeOut:
            raise RuntimeError(_("Timeout waiting for backup metadata for"
                                 " backup %s") % self.base_filename)

        return self.base_backup_metadata(
            os.path.join(WAL_ARCHIVE_DIR, self.mrb))

    def _run_post_backup(self):
        """Get rid of WAL data we don't need any longer"""
        arch_cleanup_bin = os.path.join(self.pgsql_extra_bin_dir,
                                        "pg_archivecleanup")
        f = os.path.basename(self.most_recent_backup_file())
        cmd_full = " ".join((arch_cleanup_bin, WAL_ARCHIVE_DIR, f))
        out, err = utils.execute("sudo", "su", "-", self.PGSQL_OWNER, "-c",
                                 "%s" % cmd_full)


class PgBaseBackupIncremental(PgBaseBackup):
    """To restore an incremental backup from a previous backup, in PostgreSQL,
       is effectively to replay the WAL entries to a designated point in time.
       All that is required is the most recent base backup, and all WAL files
     """

    def __init__(self, *args, **kwargs):
        LOG.info("Incr instantiated with args/kwargs %s %s " %
                 (str(args), str(kwargs)))
        if not kwargs.get('parent_location'):
            raise AttributeError('Parent missing!')

        super(PgBaseBackupIncremental, self).__init__(*args, **kwargs)
        self.parent_location = kwargs.get('parent_location')
        self.parent_checksum = kwargs.get('parent_checksum')

    def _run_pre_backup(self):
        self.backup_label = self.base_filename
        r = pgutil.query("SELECT pg_start_backup('%s', true)" %
                         self.backup_label)
        self.start_segment = r[0][0]

        r = pgutil.query("SELECT pg_xlogfile_name('%s')" % self.start_segment)
        self.start_wal_file = r[0][0]

        r = pgutil.query("SELECT pg_stop_backup()")
        self.stop_segment = r[0][0]

        # We have to hack this because self.command is
        # initialized in the base class before we get here, which is
        # when we will know exactly what WAL files we want to archive
        self.command = self._cmd()

    def _cmd(self):
        # TODO(atomic77) Store the list of files in a var but this should
        # be written to a file to ensure we don't get into cmd line
        # overflow issues when this file list gets bigger
        wal_file_list = self.log_files_since_last_backup(pos=1)
        LOG.info("Got wal file list: " + str(wal_file_list))
        c = 'sudo tar -cf - -C {wal_dir} {wal_list} '.format(
            wal_dir=WAL_ARCHIVE_DIR,
            wal_list=" ".join(wal_file_list))
        return c + self.zip_cmd + self.encrypt_cmd

    def metadata(self):
        _meta = super(PgBaseBackupIncremental, self).metadata()
        LOG.info(_("Metadata grabbed from super class: %s.") % str(_meta))
        _meta.update({
            'parent_location': self.parent_location,
            'parent_checksum': self.parent_checksum,
        })
        LOG.info("Returning metadata for incr: " + str(_meta))
        return _meta

#    Copyright 2014 Mirantis Inc.
#    All Rights Reserved.
#    Copyright 2015 Tesora Inc.
#    All Rights Reserved.s
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

from oslo_log import log as logging

from trove.common import exception
from trove.common import utils
from trove.guestagent.datastore.experimental.cassandra import service
from trove.guestagent.strategies.backup import base

LOG = logging.getLogger(__name__)


class NodetoolSnapshot(base.BackupRunner):
    """Implementation of backup using the Nodetool (http://goo.gl/QtXVsM)
    utility.
    """

    # It is recommended to include the system keyspace in the backup.
    # Keeping the system keyspace will reduce the restore time
    # by avoiding need to rebuilding indexes.

    __strategy_name__ = 'nodetoolsnapshot'
    _SNAPSHOT_EXTENSION = 'db'

    def _run_pre_backup(self):
        """Take snapshot(s) for all keyspaces.
        Remove existing ones first if any.
        Snapshot(s) will be stored in the data directory tree:
        <data dir>/<keyspace>/<table>/snapshots/<snapshot name>
        """

        self._remove_snapshot(self.filename)
        self._snapshot_all_keyspaces(self.filename)

        # Commonly 'self.command' gets resolved in the base constructor,
        # but we can build the full command only after having taken the
        # keyspace snapshot(s).
        self.command = self._backup_cmd + self.command

    def _run_post_backup(self):
        """Remove the created snapshot(s).
        """

        self._remove_snapshot(self.filename)

    def _remove_snapshot(self, snapshot_name):
        LOG.debug('Clearing snapshot(s) for all keyspaces with snapshot name '
                  '"%s".' % snapshot_name)
        utils.execute('nodetool', 'clearsnapshot', '-t %s' % snapshot_name)

    def _snapshot_all_keyspaces(self, snapshot_name):
        LOG.debug('Creating snapshot(s) for all keyspaces with snapshot name '
                  '"%s".' % snapshot_name)
        utils.execute('nodetool', 'snapshot', '-t %s' % snapshot_name)

    @property
    def cmd(self):
        return self.zip_cmd + self.encrypt_cmd

    @property
    def _backup_cmd(self):
        """Command to collect and package keyspace snapshot(s).
        """

        data_dir = service.CassandraApp(None).get_data_directory()
        return self._build_snapshot_package_cmd(data_dir, self.filename)

    def _build_snapshot_package_cmd(self, data_dir, snapshot_name):
        """Collect all files for a given snapshot and build a package
        command for them.
        Transform the paths such that the backup can be restored simply by
        extracting the archive right to an existing data directory
        (i.e. place the root into the <data dir> and
        remove the 'snapshots/<snapshot name>' portion of the path).
        Attempt to preserve access modifiers on the archived files.
        Assert the backup is not empty as there should always be
        at least the system keyspace. Fail if there is nothing to backup.
        """

        LOG.debug('Searching for all snapshot(s) with name "%s".'
                  % snapshot_name)
        snapshot_files = self._find_in_subdirectories(data_dir, snapshot_name,
                                                      self._SNAPSHOT_EXTENSION)
        num_snapshot_files = len(snapshot_files)
        LOG.debug('Found %d snapshot (*.%s) files.'
                  % (num_snapshot_files, self._SNAPSHOT_EXTENSION))
        if num_snapshot_files > 0:
            return ('tar --transform="s#snapshots/%s/##" -cpPf - -C "%s" "%s"'
                    % (snapshot_name, data_dir, '" "'.join(snapshot_files)))

        # There should always be at least the system keyspace snapshot.
        raise exception.BackupCreationError()

    def _find_in_subdirectories(self, root_dir, subdir_name, file_extension):
        """Find all files with a given extension that are contained
        within all sub-directories of a given name below the root path
        (i.e. '<root dir>/.../<sub dir>/.../*.ext').
        """

        return {os.path.relpath(os.path.join(root, name), root_dir)
                for (root, _, files) in os.walk(root_dir, topdown=True)
                if (os.path.split(root)[1] == subdir_name)
                for name in files
                if (os.path.splitext(name)[1][1:] == file_extension)}

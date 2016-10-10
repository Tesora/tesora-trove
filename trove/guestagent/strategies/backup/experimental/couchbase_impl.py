# Copyright (c) 2014 eBay Software Foundation
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
#

import json

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import utils
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.couchbase import service
from trove.guestagent.datastore.experimental.couchbase import system
from trove.guestagent.strategies.backup import base


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
OUTFILE = '/tmp' + system.BUCKETS_JSON


class CbBackup(base.BackupRunner):
    """
    Implementation of Backup Strategy for Couchbase.
    """
    __strategy_name__ = 'cbbackup'

    BUCKET_PATH_TRANSFORM = (
        's#([0-9]){4}-([0-9]){2}-([0-9]){2}T([0-9]){6}Z(-full)*/##gx')

    def __init__(self, filename, **kwargs):
        self.app = service.CouchbaseApp()
        super(CbBackup, self).__init__(filename, **kwargs)

    @property
    def cmd(self):
        """
        Creates backup dump dir, tars it up, and encrypts it.

        Empty buckets will be backed up in:
        '<backup_root>/bucket-<name>'

        Buckets with data will be stored in:
        '<backup_root>/<timestamp>/<timestamp>-full/bucket-<name>'

        'cbrestore' requires full path to the bucket backup directory.

        We transform the names such that they all extract into the backup root,
        where 'cbrestore' can find them.
        """
        cmd = ('tar --transform="%s" -cpPf - %s' % (self.BUCKET_PATH_TRANSFORM,
                                                    system.COUCHBASE_DUMP_DIR))
        return cmd + self.zip_cmd + self.encrypt_cmd

    def _save_buckets_config(self, password):
        url = system.COUCHBASE_REST_API + '/pools/default/buckets'
        utils.execute_with_timeout('curl -u root:' + password +
                                   ' ' + url + ' > ' + OUTFILE,
                                   shell=True, timeout=300)

    def _backup(self):
        self.run_cbbackup()

    def _run_pre_backup(self):
        try:
            operating_system.remove(system.COUCHBASE_DUMP_DIR,
                                    force=True, recursive=True)
            operating_system.create_directory(system.COUCHBASE_DUMP_DIR)
            pw = self.app.get_password()
            self._save_buckets_config(pw)
            with open(OUTFILE, "r") as f:
                out = f.read()
                if out != "[]":
                    d = json.loads(out)
                    all_memcached = True
                    for i in range(len(d)):
                        bucket_type = d[i]["bucketType"]
                        if bucket_type != "memcached":
                            all_memcached = False
                            break
                    if not all_memcached:
                        self._backup()
                    else:
                        LOG.info(_("All buckets are memcached.  "
                                   "Skipping backup."))
            operating_system.move(OUTFILE, system.COUCHBASE_DUMP_DIR)
            if pw != "password":
                # Not default password, backup generated root password
                operating_system.copy(self.app.couchbase_pwd_file,
                                      system.COUCHBASE_DUMP_DIR,
                                      preserve=True, as_root=True)
        except exception.ProcessExecutionError as p:
            LOG.error(p)
            raise p

    def _run_post_backup(self):
        operating_system.remove(system.COUCHBASE_DUMP_DIR,
                                force=True, recursive=True)

    def run_cbbackup(self):
        host_and_port = 'localhost:%d' % CONF.couchbase.couchbase_port
        admin_user = self.app.get_cluster_admin()
        cmd_tokens = [self.cbbackup_bin,
                      'http://' + host_and_port,
                      system.COUCHBASE_DUMP_DIR,
                      '-u', admin_user.name,
                      '-p', admin_user.password]
        return utils.execute(' '.join(cmd_tokens), shell=True)

    @property
    def cbbackup_bin(self):
        admin = self.app.build_admin()
        return guestagent_utils.build_file_path(admin.couchbase_bin_dir,
                                                'cbbackup')

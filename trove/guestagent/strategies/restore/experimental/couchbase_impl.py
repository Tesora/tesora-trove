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
import os.path

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.couchbase import service
from trove.guestagent.datastore.experimental.couchbase import system
from trove.guestagent.db import models as guest_models
from trove.guestagent import dbaas
from trove.guestagent.strategies.restore import base


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class CbBackup(base.RestoreRunner):
    """
    Implementation of Restore Strategy for Couchbase.
    """
    __strategy_name__ = 'cbbackup'
    base_restore_cmd = 'sudo tar xpPf -'

    def __init__(self, *args, **kwargs):
        self.app = service.CouchbaseApp()
        super(CbBackup, self).__init__(*args, **kwargs)

    def pre_restore(self):
        try:
            operating_system.remove(system.COUCHBASE_DUMP_DIR, force=True)
        except exception.ProcessExecutionError as p:
            LOG.error(p)
            raise p

    def post_restore(self):
        try:
            # Root enabled for the backup
            pwd_file = guestagent_utils.build_file_path(
                system.COUCHBASE_DUMP_DIR, self.app.SECRET_KEY_FILE)
            if os.path.exists(pwd_file):
                with open(pwd_file, "r") as f:
                    pw = f.read().rstrip("\n")
                    self.app.reset_admin_credentials(password=pw)

            buckets_json = system.COUCHBASE_DUMP_DIR + system.BUCKETS_JSON
            buckets = self._parse_buckets(buckets_json)
            admin = self.app.build_admin()
            max_num_replicas = admin.get_num_cluster_nodes() - 1
            for bucket in buckets:
                bucket.bucket_replica_count = min(bucket.bucket_replica_count,
                                                  max_num_replicas)

            admin.create_buckets(buckets)
            for bucket in buckets:
                self.run_cbrestore(bucket.name)

        except exception.ProcessExecutionError as p:
            LOG.error(p)
            raise base.RestoreError("Couchbase restore failed.")

    def _parse_buckets(self, buckets_json):
        with open(buckets_json, "r") as f:
            out = f.read()
            if out == "[]":
                # No buckets or data to restore. Done.
                return []

            return [self._parse_bucket_metadata(item)
                    for item in json.loads(out)]

    def _parse_bucket_metadata(self, meta):
        bucket_name = meta["name"]
        bucket_type = meta["bucketType"]
        if bucket_type == "membase":
            bucket_type = "couchbase"
        if meta["authType"] != "none":
            bucket_password = meta["saslPassword"]
            # SASL buckets can be only on this port.
            bucket_port = "11211"
        else:
            bucket_password = None
            bucket_port = meta["proxyPort"]
        replica_count = meta["replicaNumber"]
        bucket_ramsize_mb = int(dbaas.to_mb(meta["quota"]["ram"]))
        enable_index_replica = 1 if meta["replicaIndex"] else 0

        return guest_models.CouchbaseUser(
            bucket_name,
            password=bucket_password,
            bucket_ramsize_mb=bucket_ramsize_mb,
            bucket_replica_count=replica_count,
            enable_index_replica=enable_index_replica,
            bucket_eviction_policy=None,
            bucket_priority=None,
            bucket_port=bucket_port)

    def run_cbrestore(self, bucket_name):
        host_and_port = 'localhost:%d' % CONF.couchbase.couchbase_port
        admin_user = self.app.get_cluster_admin()
        cmd_tokens = [self.cbrestore_bin,
                      system.COUCHBASE_DUMP_DIR,
                      'http://' + host_and_port,
                      '-u', admin_user.name,
                      '-p', admin_user.password,
                      '--bucket-source=' + bucket_name]
        return utils.execute(' '.join(cmd_tokens), shell=True)

    @property
    def cbrestore_bin(self):
        admin = self.app.build_admin()
        return guestagent_utils.build_file_path(admin.couchbase_bin_dir,
                                                'cbrestore')

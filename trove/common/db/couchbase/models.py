# Copyright 2016 Tesora, Inc.
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

import re

from six import u

from trove.common.db import models
from trove.common.i18n import _


class CouchbaseSchema(models.DatastoreSchema):
    """Represents a Couchbase bucket and its associated properties.

    The bucket name can only contain characters in range A-Z, a-z, 0-9
    as well as underscore, period, dash and percent symbols and
    can be a maximum of 100 characters in length.
    """

    name_regex = re.compile(u(r'^[a-zA-Z0-9_\.\-%]+$'))

    @property
    def _max_schema_name_length(self):
        return 100

    def _is_valid_schema_name(self, value):
        return self.name_regex.match(value) is not None


class CouchbaseUser(models.DatastoreUser):
    """Represents a Couchbase user and its associated properties."""

    MAX_PASSWORD_LEN = 24
    MAX_REPLICA_COUNT = 3
    MIN_BUCKET_RAMSIZE_MB = 100
    VALID_BUCKET_PRIORITY = ['low', 'high']
    VALID_BUCKET_EVICTION_POLICY = ['valueOnly', 'fullEviction']

    def __init__(self, name='root', password=None, roles=None,
                 bucket_ramsize_mb=None,
                 bucket_replica_count=None,
                 enable_index_replica=None,
                 bucket_eviction_policy=None,
                 bucket_priority=None,
                 used_ram_mb=None,
                 bucket_port=None,
                 *args, **kwargs):
        super(CouchbaseUser, self).__init__(name, password, roles=roles,
                                            *args, **kwargs)

        self._bucket_ramsize_mb = None
        self._bucket_replica_count = None
        self._enable_index_replica = None
        self._bucket_eviction_policy = None
        self._bucket_priority = None
        self._used_ram_mb = used_ram_mb
        self._bucket_port = bucket_port

        if bucket_ramsize_mb is not None:
            self.bucket_ramsize_mb = bucket_ramsize_mb
        if bucket_replica_count is not None:
            self.bucket_replica_count = bucket_replica_count
        if enable_index_replica is not None:
            self.enable_index_replica = enable_index_replica
        if bucket_eviction_policy is not None:
            self.bucket_eviction_policy = bucket_eviction_policy
        if bucket_priority is not None:
            self.bucket_priority = bucket_priority

    def _build_database_schema(self, name):
        return CouchbaseSchema(name)

    @property
    def _max_username_length(self):
        return 24

    def _is_valid_password(self, value):
        length = len(value)
        return length > 5 and length <= self.MAX_PASSWORD_LEN

    @property
    def bucket_ramsize_mb(self):
        return self._bucket_ramsize_mb

    @bucket_ramsize_mb.setter
    def bucket_ramsize_mb(self, value):
        if not self._is_integer(value, self.MIN_BUCKET_RAMSIZE_MB, None):
            raise ValueError(
                _("Bucket RAM quota cannot be less than 100MB."))
        self._bucket_ramsize_mb = value

    def _is_non_negative_int(self, value):
        return self._is_integer(value, 0)

    def _is_integer(self, value, lower_bound=None, upper_bound=None):
        try:
            if str(value).isdigit():
                int_value = int(value)
                return ((lower_bound is None or
                         int_value >= lower_bound) and
                        (upper_bound is None or
                         int_value <= upper_bound))
        except (ValueError, TypeError):
            pass

        return False

    @property
    def bucket_replica_count(self):
        return self._bucket_replica_count

    @bucket_replica_count.setter
    def bucket_replica_count(self, value):
        if not self._is_integer(value, 0, self.MAX_REPLICA_COUNT):
            raise ValueError(
                _("Replica count must be an integer between 0 and 3."))
        self._bucket_replica_count = value

    @property
    def enable_index_replica(self):
        return self._enable_index_replica

    @enable_index_replica.setter
    def enable_index_replica(self, value):
        if not self._is_non_negative_int(value):
            raise ValueError(
                _("Index replica value must be '1' (yes) or '0' (no)."))
        self._enable_index_replica = value

    @property
    def bucket_eviction_policy(self):
        return self._bucket_eviction_policy

    @bucket_eviction_policy.setter
    def bucket_eviction_policy(self, value):
        if value not in self.VALID_BUCKET_EVICTION_POLICY:
            raise ValueError(_("Bucket eviction policy must be one of: '%s'")
                             % "', '".join(self.VALID_BUCKET_EVICTION_POLICY))
        self._bucket_eviction_policy = value

    @property
    def bucket_priority(self):
        return self._bucket_priority

    @bucket_priority.setter
    def bucket_priority(self, value):
        if value not in self.VALID_BUCKET_PRIORITY:
            raise ValueError(_("Bucket priority must be one of: '%s'")
                             % "', '".join(self.VALID_BUCKET_PRIORITY))
        self._bucket_priority = value

    @property
    def used_ram_mb(self):
        return self._used_ram_mb

    @property
    def bucket_port(self):
        return self._bucket_port

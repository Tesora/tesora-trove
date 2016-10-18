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

    def __init__(self, name, password=None, *args, **kwargs):
        super(CouchbaseUser, self).__init__(name, password, *args, **kwargs)

    def _build_database_schema(self, name):
        return CouchbaseSchema(name)

    @property
    def _max_username_length(self):
        return 24

    def _is_valid_password(self, value):
        length = len(value)
        return length > 5 and length <= self.MAX_PASSWORD_LEN

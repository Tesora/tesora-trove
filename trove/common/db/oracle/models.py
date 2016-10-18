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

from trove.common.db import models


class OracleSchema(models.DatastoreSchema):
    """Represents a Oracle schema and its associated properties.

    Oracle database names need to be alphanumeric and the length cannot exceed
    8 characters.
    """
    name_regex = re.compile(r'[a-zA-Z0-9]\w+$')

    @property
    def _max_schema_name_length(self):
        return 8

    def _is_valid_schema_name(self, value):
        return self.name_regex.match(value) is not None


class OracleUser(models.DatastoreUser):
    """Represents an Oracle user and its associated properties."""

    @property
    def _max_username_length(self):
        return 30

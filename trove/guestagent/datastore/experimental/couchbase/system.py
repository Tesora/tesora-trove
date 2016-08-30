# Copyright (c) 2013 eBay Software Foundation
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


# FIXME(pmalik): These properties are used by Couchbase backup/restore
# strategies. This file should eventually go away and the strategies should
# be using the existing CouchbaseApp interface to execute database commands.
COUCHBASE_DUMP_DIR = '/tmp/backups'
COUCHBASE_WEBADMIN_PORT = '8091'
COUCHBASE_REST_API = 'http://localhost:' + COUCHBASE_WEBADMIN_PORT
BUCKETS_JSON = '/buckets.json'

# Do not extend this file any more. Use the CouchbaseApp instead.

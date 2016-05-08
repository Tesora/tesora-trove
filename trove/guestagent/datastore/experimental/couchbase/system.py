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


COUCHBASE_DUMP_DIR = '/tmp/backups'
COUCHBASE_CONF_DIR = '/etc/couchbase'
COUCHBASE_OPT_ETC_DIR = '/opt/couchbase/etc'
COUCHBASE_WEBADMIN_PORT = '8091'
COUCHBASE_REST_API = 'http://localhost:' + COUCHBASE_WEBADMIN_PORT
BUCKETS_JSON = '/buckets.json'
SECRET_KEY = '/secret_key'
SERVICE_CANDIDATES = ["couchbase-server"]
cmd_kill = 'sudo pkill -u couchbaso tee -a '
cmd_reset_pwd = 'sudo /opt/couchbase/bin/cbreset_password %(IP)s:8091'
pwd_file = COUCHBASE_CONF_DIR + SECRET_KEY

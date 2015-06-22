# Copyright 2015 Tesora Inc.
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

from trove.common import cfg
from trove.guestagent.guest_log import GuestLog

CONF = cfg.CONF


class MySQLGuestLog(GuestLog):
    """
    classdocs
    """

    datastore_logs = {
        'slow_query': ('USER', None,
                       '/var/lib/mysql/my-slow.log'),
        'general': ('USER', None,
                    '/var/lib/mysql/my.log'),
        'error': ('SYS', 'mysql',
                  '/var/log/mysqld.log'),
        'guest': ('SYS', None,
                  '/var/log/trove/trove-guestagent.log')
    }

    @classmethod
    def list(cls, context, app):
        return super(MySQLGuestLog, cls).list(context, app, cls.datastore_logs)

    @classmethod
    def publish(cls, context, app, log, disable):
        return cls.instance(context, log).publish_log(app, disable)

    def __init__(self, context, log):
        super(MySQLGuestLog, self).__init__(context, log)

    def _datastore_logs(self):
        return MySQLGuestLog.datastore_logs

    def _enable_log(self, app, log, log_filename, disable):
        self.logging_enabled = True
        if disable:
            self._disable_container()

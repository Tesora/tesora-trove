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
#

from datetime import date

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import stream_codecs
from trove.guestagent.common import operating_system
from trove.guestagent.module.drivers import module_driver


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class DBCommandExecutorDriver(module_driver.ModuleDriver):
    """Module to execute Oracle database commands."""

    def get_type(self):
        return "db_command_executor"

    def get_description(self):
        return "Database Command Executor Driver"

    def get_updated(self):
        return date(2016, 5, 30)

    @module_driver.output(
        log_message=_("Modifying database"),
        success_message=_('Database has been modified'),
        fail_message=_('Database modifications were unsuccessful'))
    def apply(self, name, datastore, ds_version, data_file, admin_module):
        if not admin_module:
            raise exception.ModuleInvalid(
                reason='Module not created with admin options')

        if operating_system.owned_by_root(data_file):
            data = operating_system.read_file(data_file, as_root=True,
                                              codec=stream_codecs.IniCodec(
                                                  strip_spaces=False))

            for section, queries in data.items():
                for key, query in data[section].items():
                    self._execute_query(query, datastore)
        else:
            LOG.exception(_("Data file not owned by root. File may have "
                            "been modified"))
            raise

    def _execute_query(self, query, datastore):
        # To make it generic we will need to refactor the manager
        try:
            if datastore == "oracle":
                from trove.guestagent.datastore.oracle import service
                with service.OracleVMCursor(service.OracleVMConfig().db_name)\
                        as cursor:
                    cursor.execute("""%s""" % query)
            else:
                LOG.exception(_("Invalid datastore (%s) for query.") %
                              datastore)
                raise exception.DatastoreNotFound(message="Invalid datastore "
                                                          "(%s) for query." %
                                                          datastore)
        except Exception:
            LOG.exception(_("Error when executing query '%s'") % query)
            raise

    @module_driver.output(
        log_message=_('No database modifications were made.'),
        success_message=_('Changes were reverted'),
        fail_message=_('Changes could not be reverted'))
    def remove(self, name, datastore, ds_version, data_file):
        return True, "Module removed, no modifications were made to the " \
                     "database"

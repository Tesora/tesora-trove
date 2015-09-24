# Copyright (c) 2013 OpenStack Foundation
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

from collections import OrderedDict
import os

from distutils.version import LooseVersion
from oslo_log import log as logging

from trove.common import cfg
from trove.common.i18n import _
from trove.common.stream_codecs import PropertiesCodec
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.experimental.postgresql.service.process import(
    PgSqlProcess)
from trove.guestagent.datastore.experimental.postgresql.service.status import(
    PgSqlAppStatus)

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class PgSqlConfig(PgSqlProcess):
    """Mixin that implements the config API.

    This mixin has a dependency on the PgSqlProcess mixin.
    """

    @property
    def PGSQL_OWNER(self):
        return 'postgres'

    @property
    def PGSQL_CONFIG(self):
        return guestagent_utils.build_file_path(
            self.current_config_dir, 'main/postgresql.conf')

    @property
    def PGSQL_HBA_CONFIG(self):
        return guestagent_utils.build_file_path(
            self.current_config_dir, 'main/pg_hba.conf')

    @property
    def current_config_dir(self):
        """Get the most current of the existing Postgres installation
        configuration directories.
        """
        installations = operating_system.list_files_in_directory(
            '/etc/postgresql/', recursive=False, include_dirs=True)
        return sorted(
            installations,
            key=lambda item: LooseVersion(os.path.basename(item)))[-1]

    def reset_configuration(self, context, configuration):
        """Reset the PgSql configuration file to the one given.

        The configuration parameter is a string containing the full
        configuration file that should be used.
        """
        operating_system.write_file(self.PGSQL_CONFIG, configuration,
                                    as_root=True)
        operating_system.chown(self.PGSQL_CONFIG,
                               self.PGSQL_OWNER, self.PGSQL_OWNER,
                               recursive=False, as_root=True)

    def set_db_to_listen(self, context):
        """Update guestagent-controlled configuration properties.
        """

        LOG.debug("Applying initial guestagent configuration.")
        # Local access from administrative users is implicitly trusted.
        #
        # Remote access from the Trove's account is always rejected as
        # it is not needed and could be used by malicious users to hijack the
        # instance.
        #
        # Remote connections require the client to supply a double-MD5-hashed
        # password.
        #
        # Make the rules readable only by the Postgres service.
        #
        # NOTE: The order of entries is important.
        # The first failure to authenticate stops the lookup.
        # That is why the 'local' connections validate first.
        # The OrderedDict is necessary to guarantee the iteration order.
        access_rules = OrderedDict(
            [('local', [['all', 'postgres,os_admin', None, 'trust'],
                        ['all', 'all', None, 'md5']]),
             ('host', [['all', 'postgres,os_admin', '127.0.0.1/32', 'trust'],
                       ['all', 'postgres,os_admin', '::1/128', 'trust'],
                       ['all', 'postgres,os_admin', 'localhost', 'trust'],
                       ['all', 'os_admin', '0.0.0.0/0', 'reject'],
                       ['all', 'os_admin', '::/0', 'reject'],
                       ['all', 'all', '0.0.0.0/0', 'md5'],
                       ['all', 'all', '::/0', 'md5']])
             ])
        operating_system.write_file(self.PGSQL_HBA_CONFIG, access_rules,
                                    PropertiesCodec(
                                        string_mappings={'\t': None}),
                                    as_root=True)
        operating_system.chown(self.PGSQL_HBA_CONFIG,
                               self.PGSQL_OWNER, self.PGSQL_OWNER,
                               as_root=True)
        operating_system.chmod(self.PGSQL_HBA_CONFIG, FileMode.SET_USR_RO,
                               as_root=True)

    def start_db_with_conf_changes(self, context, config_contents):
        """Restarts the PgSql instance with a new configuration."""
        LOG.info(
            _("{guest_id}: Going into restart mode for config file changes.")
            .format(
                guest_id=CONF.guest_id,
            )
        )
        PgSqlAppStatus.get().begin_restart()
        self.stop_db(context)
        self.reset_configuration(context, config_contents)
        self.start_db(context)
        LOG.info(
            _("{guest_id}: Ending restart mode for config file changes.")
            .format(
                guest_id=CONF.guest_id,
            )
        )
        PgSqlAppStatus.get().end_restart()

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

from oslo_log import log as logging

from trove.common import cfg
from trove.common.i18n import _
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.postgresql import pgutil
from trove.guestagent.datastore.experimental.postgresql.service.status import(
    PgSqlAppStatus)

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class PgSqlConfig(object):
    """Mixin that implements the config API.
    """
    def reset_configuration(self, context, configuration):
        """Reset the PgSql configuration file to the one given.

        The configuration parameter is a string containing the full
        configuration file that should be used. It is rendered for
        a second time on the guest for the OS-specific elements
        """
        config_location = pgutil.get_pgsql_conf_location()

        LOG.debug(
            "{guest_id}: Writing configuration file to /tmp/pgsql_config."
            .format(
                guest_id=CONF.guest_id,
            )
        )

        config_second_pass = pgutil.render_config(configuration)

        with open('/tmp/pgsql_config', 'w+') as config_file:
            config_file.write(config_second_pass)
        operating_system.chown('/tmp/pgsql_config', 'postgres', None,
                               recursive=False, as_root=True)
        operating_system.move('/tmp/pgsql_config', config_location, timeout=30,
                              as_root=True)

    def set_db_to_listen(self, context):
        """Allow remote connections with encrypted passwords."""
        # Using cat to read file due to read permissions issues.
        hba_location = pgutil.get_pgsql_hba_location()
        out, err = utils.execute_with_timeout(
            'sudo', 'cat', hba_location, timeout=30,
        )
        LOG.debug(
            "{guest_id}: Writing hba file to /tmp/pgsql_hba_config.".format(
                guest_id=CONF.guest_id,
            )
        )
        with open('/tmp/pgsql_hba_config', 'w+') as config_file:
            config_file.write(out)
            config_file.write("host    all     all     0.0.0.0/0   md5\n")

        operating_system.chown('/tmp/pgsql_hba_config',
                               'postgres', None, recursive=False, as_root=True)
        operating_system.move('/tmp/pgsql_hba_config', hba_location,
                              timeout=30, as_root=True)

    def start_db_with_conf_changes(self, context, config_contents):
        """Restarts the PgSql instance with a new configuration."""
        LOG.info(
            _("{guest_id}: Going into restart mode for config file changes.")
            .format(
                guest_id=CONF.guest_id,
            )
        )
        config_second_pass = pgutil.render_config(config_contents)
        PgSqlAppStatus.get().begin_restart()
        self.stop_db(context)
        self.reset_configuration(context, config_second_pass)
        self.start_db(context)
        LOG.info(
            _("{guest_id}: Ending restart mode for config file changes.")
            .format(
                guest_id=CONF.guest_id,
            )
        )
        PgSqlAppStatus.get().end_install()

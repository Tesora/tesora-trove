#    Copyright 2014 Mirantis Inc.
#    All Rights Reserved.
#    Copyright 2015 Tesora Inc.
#    All Rights Reserved.
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

from trove.common import utils
from trove.guestagent.common import operating_system as os_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.cassandra import system
from trove.guestagent.strategies.restore import base
from trove.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class NodetoolSnapshot(base.RestoreRunner):
    """Implementation of restore using the Nodetool (http://goo.gl/QtXVsM)
    utility.
    """

    __strategy_name__ = 'nodetoolsnapshot'

    def __init__(self, storage, **kwargs):
        conf = operating_system.read_yaml_file(
            system.CASSANDRA_CONF[operating_system.get_os()])
        kwargs.update({'restore_location': conf['data_file_directories'][0]})
        super(NodetoolSnapshot, self).__init__(storage, **kwargs)

    def pre_restore(self):
        """Prepare the data directory for restored files.
        The directory itself is not included in the backup archive
        (i.e. the archive is rooted inside the data directory).
        This is to make sure we can always restore an old backup
        even if the standard guest agent data directory changes.
        """

        LOG.debug('Initializing a data directory.')
        NodetoolSnapshot._create_directory_as_user(self.restore_location,
                                                   system.CASSANDRA_OWNER,
                                                   system.CASSANDRA_OWNER)

    def post_restore(self):
        """Updated ownership on the restored files.
        """

        LOG.debug('Updating ownership of the restored files.')
        os_utils.update_owner(system.CASSANDRA_OWNER, system.CASSANDRA_OWNER,
                              self.restore_location)

    @classmethod
    def _create_directory_as_user(self, dir_path, user, group, force=True):
        """Create a given directory as root and update its ownership.

        :param dir_path:        Path to the created directory.
        :type dir_path:         string

        :param user:            Owner.
        :type user:             string

        :param group:           Group.
        :type group:            string

        :param force:           No error if existing, make parent directories
                                as needed.
        :type force:            boolean
        """

        NodetoolSnapshot._create_directory_as_root(dir_path, force)
        os_utils.update_owner(user, group, dir_path)

    @classmethod
    def _create_directory_as_root(self, dir_path, force=True):
        """Create a given directory as the root user.

        :param dir_path:        Path to the created directory.
        :type dir_path:         string

        :param force:           No error if existing, make parent directories
                                as needed.
        :type force:            boolean
        """

        utils.execute_with_timeout('mkdir', '-p' if force else '', dir_path,
                                   root_helper='sudo', run_as_root=True)

    @property
    def base_restore_cmd(self):
        """Command to extract a backup archive into a given location.
        Attempt to preserve access modifiers on the archived files.
        """

        return 'sudo tar -xpPf - -C "%(restore_location)s"'

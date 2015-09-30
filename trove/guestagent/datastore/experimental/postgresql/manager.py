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


import os

from oslo_log import log as logging

from .service.config import PgSqlConfig
from .service.database import PgSqlDatabase
from .service.install import PgSqlInstall
from .service.root import PgSqlRoot
from .service.status import PgSqlAppStatus
import pgutil
from trove.common import cfg
from trove.common import utils
from trove.guestagent import backup
from trove.guestagent.datastore import manager
from trove.guestagent import dbaas
from trove.guestagent import guest_log
from trove.guestagent import volume


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(
        manager.Manager,
        PgSqlDatabase,
        PgSqlRoot,
        PgSqlConfig,
        PgSqlInstall,
):

    PG_BUILTIN_ADMIN = 'postgres'

    def __init__(self):
        super(Manager, self).__init__()

    @property
    def status(self):
        return PgSqlAppStatus.get()

    @property
    def datastore_log_defs(self):
        owner = 'postgres'
        datastore_dir = self.PGSQL_DATA_DIR()
        long_query_time = CONF.get(self.manager).get(
            'guest_log_long_query_time')
        general_log_file = self.build_log_file_name(
            self.GUEST_LOG_DEFS_GENERAL_LABEL, owner,
            datastore_dir=datastore_dir)
        general_log_dir, general_log_filename = os.path.split(general_log_file)
        return {
            self.GUEST_LOG_DEFS_GENERAL_LABEL: {
                self.GUEST_LOG_TYPE_LABEL: guest_log.LogType.USER,
                self.GUEST_LOG_USER_LABEL: owner,
                self.GUEST_LOG_FILE_LABEL: general_log_file,
                self.GUEST_LOG_ENABLE_LABEL: {
                    'log_destination': 'stderr',
                    'logging_collector': 'on',
                    'log_directory': general_log_dir,
                    'log_filename': general_log_filename,
                    'log_output': 'file',
                    'log_statement': 'all',
                    'debug_print_plan': 'on',
                    'log_min_duration_statement': long_query_time,
                },
                self.GUEST_LOG_DISABLE_LABEL: {
                    'general_log': 'off',
                },
            },
        }

    def rpc_ping(self, context):
        LOG.debug("Responding to RPC ping.")
        return True

    def do_prepare(
            self,
            context,
            packages,
            databases,
            memory_mb,
            users,
            device_path=None,
            mount_point=None,
            backup_info=None,
            config_contents=None,
            root_password=None,
            overrides=None,
            cluster_config=None,
            snapshot=None
    ):
        pgutil.PG_ADMIN = self.PG_BUILTIN_ADMIN
        self.install(context, packages)
        self.stop_db(context)
        if device_path:
            device = volume.VolumeDevice(device_path)
            device.format()
            if os.path.exists(mount_point):
                device.migrate_data(mount_point)
            device.mount(mount_point)
        self.configuration_manager.save_configuration(config_contents)
        self.apply_initial_guestagent_configuration()

        if backup_info:
            pgutil.PG_ADMIN = self.ADMIN_USER
            backup.restore(context, backup_info, '/tmp')

        self.start_db(context)

        if not backup_info:
            self._secure(context)

        if root_password and not backup_info:
            self.enable_root(context, root_password)

        if databases:
            self.create_database(context, databases)

        if users:
            self.create_user(context, users)

    def _secure(self, context):
        # Create a new administrative user for Trove and also
        # disable the built-in superuser.
        self.create_database(context, [{'_name': self.ADMIN_USER}])
        self._create_admin_user(context)
        pgutil.PG_ADMIN = self.ADMIN_USER
        postgres = {'_name': self.PG_BUILTIN_ADMIN,
                    '_password': utils.generate_random_password()}
        self.alter_user(context, postgres, 'NOSUPERUSER', 'NOLOGIN')

    def get_filesystem_stats(self, context, fs_path):
        mount_point = CONF.get(CONF.datastore_manager).mount_point
        return dbaas.get_filesystem_volume_stats(mount_point)

    def create_backup(self, context, backup_info):
        self.enable_backups()
        backup.backup(context, backup_info)

    def mount_volume(self, context, device_path=None, mount_point=None):
        """Mount the volume as specified by device_path to mount_point."""
        device = volume.VolumeDevice(device_path)
        device.mount(mount_point, write_to_fstab=False)
        LOG.debug(
            "Mounted device {device} at mount point {mount}.".format(
                device=device_path, mount=mount_point))

    def unmount_volume(self, context, device_path=None, mount_point=None):
        """Unmount the volume as specified by device_path from mount_point."""
        device = volume.VolumeDevice(device_path)
        device.unmount(mount_point)
        LOG.debug(
            "Unmounted device {device} from mount point {mount}.".format(
                device=device_path, mount=mount_point))

    def resize_fs(self, context, device_path=None, mount_point=None):
        """Resize the filesystem as specified by device_path at mount_point."""
        device = volume.VolumeDevice(device_path)
        device.resize_fs(mount_point)
        LOG.debug(
            "Resized the filesystem at {mount}.".format(
                mount=mount_point))

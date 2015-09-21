# Copyright 2015 IBM Corp
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.notification import EndNotification
from trove.guestagent.datastore.experimental.db2 import service
from trove.guestagent.datastore import manager
from trove.guestagent import dbaas
from trove.guestagent import volume

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager


class Manager(manager.Manager):
    """
    This is DB2 Manager class. It is dynamically loaded
    based off of the datastore of the Trove instance.
    """
    def __init__(self):
        self.appStatus = service.DB2AppStatus()
        self.app = service.DB2App(self.appStatus)
        self.admin = service.DB2Admin()
        super(Manager, self).__init__()

    @property
    def status(self):
        return self.appStatus

    def rpc_ping(self, context):
        LOG.debug("Responding to RPC ping.")
        return True

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path=None, mount_point=None, backup_info=None,
                   config_contents=None, root_password=None, overrides=None,
                   cluster_config=None, snapshot=None):
        """This is called from prepare in the base class."""
        if device_path:
            device = volume.VolumeDevice(device_path)
            device.unmount_device(device_path)
            device.format()
            device.mount(mount_point)
            LOG.debug('Mounted the volume.')
        self.app.change_ownership(mount_point)
        self.app.start_db()

        if databases:
            self.create_database(context, databases)

        if users:
            self.create_user(context, users)

    def restart(self, context):
        """
        Restart this DB2 instance.
        This method is called when the guest agent
        gets a restart message from the taskmanager.
        """
        LOG.debug("Restart a DB2 server instance.")
        self.app.restart()

    def stop_db(self, context, do_not_start_on_reboot=False):
        """
        Stop this DB2 instance.
        This method is called when the guest agent
        gets a stop message from the taskmanager.
        """
        LOG.debug("Stop a given DB2 server instance.")
        self.app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        LOG.debug("Get the filesystem stats.")
        mount_point = CONF.get(
            'db2' if not MANAGER else MANAGER).mount_point
        return dbaas.get_filesystem_volume_stats(mount_point)

    def create_database(self, context, databases):
        LOG.debug("Creating database(s)." % databases)
        with EndNotification(context):
            self.admin.create_database(databases)

    def delete_database(self, context, database):
        with EndNotification(context):
            LOG.debug("Deleting database %s." % database)
            return self.admin.delete_database(database)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        LOG.debug("Listing all databases.")
        return self.admin.list_databases(limit, marker, include_marker)

    def create_user(self, context, users):
        LOG.debug("Create user(s).")
        with EndNotification(context):
            self.admin.create_user(users)

    def delete_user(self, context, user):
        LOG.debug("Delete a user %s." % user)
        with EndNotification(context):
            self.admin.delete_user(user)

    def get_user(self, context, username, hostname):
        LOG.debug("Show details of user %s." % username)
        return self.admin.get_user(username, hostname)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        LOG.debug("List all users.")
        return self.admin.list_users(limit, marker, include_marker)

    def list_access(self, context, username, hostname):
        LOG.debug("List all the databases the user has access to.")
        return self.admin.list_access(username, hostname)

    def mount_volume(self, context, device_path=None, mount_point=None):
        device = volume.VolumeDevice(device_path)
        device.mount(mount_point, write_to_fstab=False)
        LOG.debug("Mounted the device %s at the mount point %s." %
                  (device_path, mount_point))

    def unmount_volume(self, context, device_path=None, mount_point=None):
        device = volume.VolumeDevice(device_path)
        device.unmount(mount_point)
        LOG.debug("Unmounted the device %s from the mount point %s." %
                  (device_path, mount_point))

    def resize_fs(self, context, device_path=None, mount_point=None):
        device = volume.VolumeDevice(device_path)
        device.resize_fs(mount_point)
        LOG.debug("Resized the filesystem %s." % mount_point)

    def start_db_with_conf_changes(self, context, config_contents):
        LOG.debug("Starting DB2 with configuration changes.")
        self.app.start_db_with_conf_changes(config_contents)

    def grant_access(self, context, username, hostname, databases):
        LOG.debug("Granting acccess.")
        raise exception.DatastoreOperationNotSupported(
            operation='grant_access', datastore=MANAGER)

    def revoke_access(self, context, username, hostname, database):
        LOG.debug("Revoking access.")
        raise exception.DatastoreOperationNotSupported(
            operation='revoke_access', datastore=MANAGER)

    def reset_configuration(self, context, configuration):
        """
         Currently this method does nothing. This method needs to be
         implemented to enable rollback of flavor-resize on guestagent side.
        """
        LOG.debug("Resetting DB2 configuration.")
        pass

    def change_passwords(self, context, users):
        LOG.debug("Changing password.")
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='change_passwords', datastore=MANAGER)

    def update_attributes(self, context, username, hostname, user_attrs):
        LOG.debug("Updating database attributes.")
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='update_attributes', datastore=MANAGER)

    def enable_root(self, context):
        LOG.debug("Enabling root.")
        raise exception.DatastoreOperationNotSupported(
            operation='enable_root', datastore=MANAGER)

    def enable_root_with_password(self, context, root_password=None):
        LOG.debug("Enabling root with password.")
        raise exception.DatastoreOperationNotSupported(
            operation='enable_root_with_password', datastore=MANAGER)

    def disable_root(self, context):
        LOG.debug("Disabling root.")
        raise exception.DatastoreOperationNotSupported(
            operation='disable_root', datastore=MANAGER)

    def is_root_enabled(self, context):
        LOG.debug("Checking if root is enabled.")
        raise exception.DatastoreOperationNotSupported(
            operation='is_root_enabled', datastore=MANAGER)

    def _perform_restore(self, backup_info, context, restore_location, app):
        raise exception.DatastoreOperationNotSupported(
            operation='_perform_restore', datastore=MANAGER)

    def create_backup(self, context, backup_info):
        LOG.debug("Creating backup.")
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='create_backup', datastore=MANAGER)

    def get_config_changes(self, cluster_config, mount_point=None):
        LOG.debug("Get configuration changes")
        raise exception.DatastoreOperationNotSupported(
            operation='get_configuration_changes', datastore=MANAGER)

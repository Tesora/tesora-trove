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

import os

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common.notification import EndNotification
from trove.guestagent import backup
from trove.guestagent.datastore.experimental.couchbase import service
from trove.guestagent.datastore.experimental.couchbase import system
from trove.guestagent.datastore import manager
from trove.guestagent import dbaas
from trove.guestagent import volume


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager


class Manager(manager.Manager):
    """
    This is Couchbase Manager class. It is dynamically loaded
    based off of the datastore of the trove instance
    """

    def __init__(self):
        self.appStatus = service.CouchbaseAppStatus()
        self.app = service.CouchbaseApp(self.appStatus)
        super(Manager, self).__init__()

    @property
    def status(self):
        return self.appStatus

    def rpc_ping(self, context):
        LOG.debug("Responding to RPC ping.")
        return True

    def change_passwords(self, context, users):
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='change_passwords', datastore=MANAGER)

    def reset_configuration(self, context, configuration):
        self.app.reset_configuration(configuration)

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path=None, mount_point=None, backup_info=None,
                   config_contents=None, root_password=None, overrides=None,
                   cluster_config=None, snapshot=None):
        """This is called from prepare in the base class."""
        self.app.install_if_needed(packages)
        self.app.available_ram_mb = memory_mb

        if device_path:
            device = volume.VolumeDevice(device_path)
            # unmount if device is already mounted
            device.unmount_device(device_path)
            device.format()
            device.mount(mount_point)
            self.app.init_storage_structure(mount_point)
            LOG.debug('Mounted the volume (%s).' % device_path)

        self.app.start_db(update_db=False)
        self.app.apply_initial_guestagent_configuration(cluster_config)

        if root_password:
            LOG.debug('Enabling root user (with password).')
            self.app.enable_root(root_password)

        if backup_info:
            LOG.debug('Now going to perform restore.')
            self._perform_restore(backup_info,
                                  context,
                                  mount_point)

        if not cluster_config:
            if self.is_root_enabled(context):
                self.status.report_root(
                    context, service.CouchbaseRootAccess.DEFAULT_ADMIN_NAME)

    def restart(self, context):
        """
        Restart this couchbase instance.
        This method is called when the guest agent
        gets a restart message from the taskmanager.
        """
        self.app.restart()

    def start_db_with_conf_changes(self, context, config_contents):
        self.app.start_db_with_conf_changes(config_contents)

    def stop_db(self, context, do_not_start_on_reboot=False):
        """
        Stop this couchbase instance.
        This method is called when the guest agent
        gets a stop message from the taskmanager.
        """
        self.app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        mount_point = CONF.get(
            'mysql' if not MANAGER else MANAGER).mount_point
        return dbaas.get_filesystem_volume_stats(mount_point)

    def update_attributes(self, context, username, hostname, user_attrs):
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='update_attributes', datastore=MANAGER)

    def create_database(self, context, databases):
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='create_database', datastore=MANAGER)

    def create_user(self, context, users):
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='create_user', datastore=MANAGER)

    def delete_database(self, context, database):
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='delete_database', datastore=MANAGER)

    def delete_user(self, context, user):
        with EndNotification(context):
            raise exception.DatastoreOperationNotSupported(
                operation='delete_user', datastore=MANAGER)

    def get_user(self, context, username, hostname):
        raise exception.DatastoreOperationNotSupported(
            operation='get_user', datastore=MANAGER)

    def grant_access(self, context, username, hostname, databases):
        raise exception.DatastoreOperationNotSupported(
            operation='grant_access', datastore=MANAGER)

    def revoke_access(self, context, username, hostname, database):
        raise exception.DatastoreOperationNotSupported(
            operation='revoke_access', datastore=MANAGER)

    def list_access(self, context, username, hostname):
        raise exception.DatastoreOperationNotSupported(
            operation='list_access', datastore=MANAGER)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        raise exception.DatastoreOperationNotSupported(
            operation='list_databases', datastore=MANAGER)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        raise exception.DatastoreOperationNotSupported(
            operation='list_users', datastore=MANAGER)

    def enable_root(self, context):
        LOG.debug("Enabling root.")
        return self.app.enable_root()

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
        return os.path.exists(system.pwd_file)

    def _perform_restore(self, backup_info, context, restore_location):
        """
        Restores all couchbase buckets and their documents from the
        backup.
        """
        LOG.info(_("Restoring database from backup %s") %
                 backup_info['id'])
        try:
            backup.restore(context, backup_info, restore_location)
        except Exception as e:
            LOG.error(_("Error performing restore from backup %s") %
                      backup_info['id'])
            LOG.error(e)
            self.status.set_status(rd_instance.ServiceStatuses.FAILED)
            raise
        LOG.info(_("Restored database successfully"))

    def create_backup(self, context, backup_info):
        """
        Backup all couchbase buckets and their documents.
        """
        with EndNotification(context):
            backup.backup(context, backup_info)

    def mount_volume(self, context, device_path=None, mount_point=None):
        device = volume.VolumeDevice(device_path)
        device.mount(mount_point, write_to_fstab=False)
        LOG.debug("Mounted the device %s at the mount_point %s." %
                  (device_path, mount_point))

    def unmount_volume(self, context, device_path=None, mount_point=None):
        device = volume.VolumeDevice(device_path)
        device.unmount(mount_point)
        LOG.debug("Unmounted the device %s from the mount point %s." %
                  (device_path, mount_point))

    def resize_fs(self, context, device_path=None, mount_point=None):
        device = volume.VolumeDevice(device_path)
        device.resize_fs(mount_point)
        LOG.debug("Resized the filesystem at %s." % mount_point)

    def update_overrides(self, context, overrides, remove=False):
        LOG.debug("Updating overrides.")
        raise exception.DatastoreOperationNotSupported(
            operation='update_overrides', datastore=MANAGER)

    def apply_overrides(self, context, overrides):
        LOG.debug("Applying overrides.")
        raise exception.DatastoreOperationNotSupported(
            operation='apply_overrides', datastore=MANAGER)

    def get_replication_snapshot(self, context, snapshot_info,
                                 replica_source_config=None):
        raise exception.DatastoreOperationNotSupported(
            operation='get_replication_snapshot', datastore=MANAGER)

    def attach_replication_slave(self, context, snapshot, slave_config):
        LOG.debug("Attaching replication slave.")
        raise exception.DatastoreOperationNotSupported(
            operation='attach_replication_slave', datastore=MANAGER)

    def detach_replica(self, context, for_failover=False):
        raise exception.DatastoreOperationNotSupported(
            operation='detach_replica', datastore=MANAGER)

    def get_replica_context(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='get_replica_context', datastore=MANAGER)

    def make_read_only(self, context, read_only):
        raise exception.DatastoreOperationNotSupported(
            operation='make_read_only', datastore=MANAGER)

    def enable_as_master_2(self, context, replica_source_config,
                           for_failover=False):
        raise exception.DatastoreOperationNotSupported(
            operation='enable_as_master', datastore=MANAGER)

    def get_txn_count(self):
        raise exception.DatastoreOperationNotSupported(
            operation='get_txn_count', datastore=MANAGER)

    def get_latest_txn_id(self):
        raise exception.DatastoreOperationNotSupported(
            operation='get_latest_txn_id', datastore=MANAGER)

    def wait_for_txn(self, txn):
        raise exception.DatastoreOperationNotSupported(
            operation='wait_for_txn', datastore=MANAGER)

    def demote_replication_master(self, context):
        LOG.debug("Demoting replication slave.")
        raise exception.DatastoreOperationNotSupported(
            operation='demote_replication_master', datastore=MANAGER)

    def initialize_cluster(self, context):
        self.app.initialize_cluster()

    def get_cluster_password(self, context):
        return self.app.get_cluster_admin().password

    def get_cluster_rebalance_status(self, context):
        return self.app.get_cluster_rebalance_status()

    def add_nodes(self, context, nodes):
        self.app.rebalance_cluster(added_nodes=nodes)

    def remove_nodes(self, context, nodes):
        self.app.rebalance_cluster(removed_nodes=nodes)

#  Copyright 2013 Mirantis Inc.
#  All Rights Reserved.
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

import os
from trove.common import cfg
from trove.common import exception
from trove.guestagent import volume
from trove.guestagent.datastore.experimental.cassandra import service
from trove.openstack.common import periodic_task
from trove.openstack.common import log as logging
from trove.common.i18n import _
from trove.guestagent import dbaas
from trove.guestagent.datastore.experimental.cassandra.service import (
    CassandraAdmin
)

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
MANAGER = CONF.datastore_manager


class Manager(periodic_task.PeriodicTasks):

    def __init__(self):
        self.appStatus = service.CassandraAppStatus(
            service.CassandraApp.get_current_superuser())
        self.app = service.CassandraApp(self.appStatus)
        self.__admin = CassandraAdmin(self.app.get_current_superuser())

    @periodic_task.periodic_task(ticks_between_runs=3)
    def update_status(self, context):
        """Update the status of the Cassandra service."""
        self.appStatus.update()

    def rpc_ping(self, context):
        LOG.debug("Responding to RPC ping.")
        return True

    def restart(self, context):
        self.app.restart()

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        mount_point = CONF.get(
            'mysql' if not MANAGER else MANAGER).mount_point
        return dbaas.get_filesystem_volume_stats(mount_point)

    def start_db_with_conf_changes(self, context, config_contents):
        self.app.start_db_with_conf_changes(config_contents)

    def stop_db(self, context, do_not_start_on_reboot=False):
        self.app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def reset_configuration(self, context, configuration):
        self.app.reset_configuration(configuration)

    def prepare(self, context, packages, databases, memory_mb, users,
                device_path=None, mount_point=None, backup_info=None,
                config_contents=None, root_password=None, overrides=None,
                cluster_config=None, snapshot=None):
        LOG.info(_("Preparing Cassandra guest."))
        self.appStatus.begin_install()
        LOG.debug("Installing cassandra.")
        self.app.install_if_needed(packages)
        self.app.init_storage_structure(mount_point)

        if config_contents or device_path:
            # Stop the db while we configure
            # FIXME(amrith) Once the cassandra bug
            # https://issues.apache.org/jira/browse/CASSANDRA-2356
            # is fixed, this code may have to be revisited.
            LOG.debug("Stopping database prior to initial configuration.")
            self.app.stop_db()

            if config_contents:
                LOG.debug("Applying configuration.")
                self.app.write_config(config_contents, is_raw=True)
                self.app.make_host_reachable()

            if device_path:
                LOG.debug("Preparing data volume.")
                device = volume.VolumeDevice(device_path)
                # unmount if device is already mounted
                device.unmount_device(device_path)
                device.format()
                if os.path.exists(mount_point):
                    #rsync exiting data
                    LOG.debug("Migrating existing data.")
                    device.migrate_data(mount_point)
                #mount the volume
                LOG.debug("Mounting new volume.")
                device.mount(mount_point)

            LOG.debug("Starting database with configuration changes.")
            self.app.start_db(update_db=False)

            if not service.CassandraApp.has_user_config():
                LOG.debug("Securing superuser access.")
                self.app.configure_superuser_access()
                self.app.restart()

            self.__admin = CassandraAdmin(self.app.get_current_superuser())

        self.appStatus.end_install_or_restart()

        if databases:
            self.create_database(context, databases)

        if users:
            self.create_user(context, users)

        LOG.info(_("Completed setup of Cassandra database instance."))

    def change_passwords(self, context, users):
        self.__admin.change_passwords(context, users)

    def update_attributes(self, context, username, hostname, user_attrs):
        self.__admin.update_attributes(context, username, hostname, user_attrs)

    def create_database(self, context, databases):
        self.__admin.create_database(context, databases)

    def create_user(self, context, users):
        self.__admin.create_user(context, users)

    def delete_database(self, context, database):
        self.__admin.delete_database(context, database)

    def delete_user(self, context, user):
        self.__admin.delete_user(context, user)

    def get_user(self, context, username, hostname):
        return self.__admin.get_user(context, username, hostname)

    def grant_access(self, context, username, hostname, databases):
        self.__admin.grant_access(context, username, hostname, databases)

    def revoke_access(self, context, username, hostname, database):
        self.__admin.revoke_access(context, username, hostname, database)

    def list_access(self, context, username, hostname):
        return self.__admin.list_access(context, username, hostname)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        return self.__admin.list_databases(context, limit, marker,
                                           include_marker)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        return self.__admin.list_users(context, limit, marker, include_marker)

    def enable_root(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='enable_root', datastore=MANAGER)

    def is_root_enabled(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='is_root_enabled', datastore=MANAGER)

    def _perform_restore(self, backup_info, context, restore_location, app):
        raise exception.DatastoreOperationNotSupported(
            operation='_perform_restore', datastore=MANAGER)

    def create_backup(self, context, backup_info):
        raise exception.DatastoreOperationNotSupported(
            operation='create_backup', datastore=MANAGER)

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
        LOG.debug("Resized the filesystem at %s." % mount_point)

    def update_overrides(self, context, overrides, remove=False):
        LOG.debug("Updating overrides.")
        if remove:
            self.app.remove_overrides()
        self.app.update_overrides(context, overrides, remove)

    def apply_overrides(self, context, overrides):
        """Configuration changes are made in the config YAML file and
        require restart, so this is a no-op.
        """
        pass

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

    def enable_as_master(self, context, replica_source_config):
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
        LOG.debug("Demoting replication master.")
        raise exception.DatastoreOperationNotSupported(
            operation='demote_replication_master', datastore=MANAGER)

# flake8: noqa

# Copyright (c) 2015 Tesora, Inc.
#
# This file is part of the Tesora DBaas Platform Enterprise Edition.
#
# Tesora DBaaS Platform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# According to sec. 7 of the GNU Affero General Public License, version 3, the
# terms of the AGPL are supplemented with the following terms:
#
# "Tesora", "Tesora DBaaS Platform", and the Tesora logo are trademarks
#  of Tesora, Inc.,
#
# The licensing of the Program under the AGPL does not imply a trademark
# license. Therefore any rights, title and interest in our trademarks remain
# entirely with us.
#
# However, if you propagate an unmodified version of the Program you are
# allowed to use the term "Tesora" solely to indicate that you distribute the
# Program. Furthermore you may use our trademarks where it is necessary to
# indicate the intended purpose of a product or service provided you use it in
# accordance with honest practices in industrial or commercial matters.
#
# If you want to propagate modified versions of the Program under the name
# "Tesora" or "Tesora DBaaS Platform", you may only do so if you have a written
# permission by Tesora, Inc. (to acquire a permission please contact
# Tesora, Inc at trademark@tesora.com).
#
# The interactive user interface of the software displays an attribution notice
# containing the term "Tesora" and/or the logo of Tesora.  Interactive user
# interfaces of unmodified and modified versions must display Appropriate Legal
# Notices according to sec. 5 of the GNU Affero General Public License,
# version 3, when you propagate unmodified or modified versions of  the
# Program. In accordance with sec. 7 b) of the GNU Affero General Public
# License, version 3, these Appropriate Legal Notices must retain the logo of
# Tesora or display the words "Initial Development by Tesora" if the display of
# the logo is not reasonably feasible for technical reasons.

import ConfigParser
from trove.common import cfg
from trove.common import exception
from trove.common import instance as rd_instance
from trove.guestagent import dbaas
from trove.guestagent import volume
from trove.guestagent.datastore.oracle.service import OracleAppStatus
from trove.guestagent.datastore.oracle.service import OracleAdmin
from trove.guestagent.datastore.oracle.service import OracleApp
from trove.openstack.common import log as logging
from trove.openstack.common.gettextutils import _
from trove.openstack.common import periodic_task


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'
#REPLICATION_STRATEGY = CONF.get(MANAGER).replication_strategy
#REPLICATION_NAMESPACE = CONF.get(MANAGER).replication_namespace
#EPLICATION_STRATEGY_CLASS = get_replication_strategy(REPLICATION_STRATEGY,
#                                                      REPLICATION_NAMESPACE)


class Manager(periodic_task.PeriodicTasks):

    @periodic_task.periodic_task(ticks_between_runs=1)
    def update_status(self, context):
        """Update the status of the Oracle service."""
        OracleAppStatus.get().update()

    def change_passwords(self, context, users):
        return OracleAdmin().change_passwords(users)

    def update_attributes(self, context, username, hostname, user_attrs):
        return OracleAdmin().update_attributes(username, hostname, user_attrs)

    def reset_configuration(self, context, configuration):
        app = OracleApp(OracleAppStatus.get())
        app.reset_configuration(configuration)

    def create_database(self, context, databases):
        return OracleAdmin().create_database(databases)

    def create_user(self, context, users):
        OracleAdmin().create_user(users)

    def delete_database(self, context, database):
        return OracleAdmin().delete_database(database)

    def delete_user(self, context, user):
        OracleAdmin().delete_user(user)

    def get_user(self, context, username, hostname):
        return OracleAdmin().get_user(username, hostname)

    def grant_access(self, context, username, hostname, databases):
        return OracleAdmin().grant_access(username, hostname, databases)

    def revoke_access(self, context, username, hostname, database):
        return OracleAdmin().revoke_access(username, hostname, database)

    def list_access(self, context, username, hostname):
        return OracleAdmin().list_access(username, hostname)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        return OracleAdmin().list_databases(limit, marker,
                                            include_marker)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        return OracleAdmin().list_users(limit, marker,
                                        include_marker)

    def enable_root(self, context):
        return OracleAdmin().enable_root()

    def is_root_enabled(self, context):
        return OracleAdmin().is_root_enabled()

    def _perform_restore(self, backup_info, context, restore_location, app):
        LOG.info(_("Restoring database from backup %s.") % backup_info['id'])
        try:
            backup.restore(context, backup_info, restore_location)
        except Exception:
            LOG.exception(_("Error performing restore from backup %s.") %
                          backup_info['id'])
            app.status.set_status(rd_instance.ServiceStatuses.FAILED)
            raise
        LOG.info(_("Restored database successfully."))

    def prepare(self, context, packages, databases, memory_mb, users,
                device_path=None, mount_point=None, backup_info=None,
                config_contents=None, root_password=None, overrides=None,
                cluster_config=None):

        if overrides is not None:
            app = OracleApp(OracleAppStatus.get())
            app.configure(overrides)

        """Makes ready DBAAS on a Guest container."""
        # Read the instance name from guest_info, and use it as
        # the Oracle pluggabel database name.
        config = ConfigParser.ConfigParser()
        config.read('/etc/guest_info')
        instance_name = config.get('DEFAULT', 'guest_name')
        self.create_database(context, [instance_name])

        LOG.debug("Before creating user")
        if users:
            LOG.debug("About to create user %s" % users)
            self.create_user(context, users)
            LOG.debug("After creating user")

        LOG.info(_('Completed setup of Oracle database instance.'))

    def restart(self, context):
        app = OracleApp(OracleAppStatus.get())
        app.restart()

    def start_db_with_conf_changes(self, context, config_contents):
        app = OracleApp(OracleAppStatus.get())
        app.start_db_with_conf_changes(config_contents)

    def stop_db(self, context, do_not_start_on_reboot=False):
        app = OracleApp(OracleAppStatus.get())
        app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        mount_point = CONF.get(MANAGER).mount_point
        return dbaas.get_filesystem_volume_stats(mount_point)

    def create_backup(self, context, backup_info):
        """
        Entry point for initiating a backup for this guest agents db instance.
        The call currently blocks until the backup is complete or errors. If
        device_path is specified, it will be mounted based to a point specified
        in configuration.

        :param backup_info: a dictionary containing the db instance id of the
                            backup task, location, type, and other data.
        """
        backup.backup(context, backup_info)

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

    def update_overrides(self, context, overrides, remove=False):
        LOG.debug("Updating overrides (%s)." % overrides)
        app = OracleApp(OracleAppStatus.get())
        app.update_overrides(overrides, remove=remove)

    def apply_overrides(self, context, overrides):
        LOG.debug("Applying overrides (%s)." % overrides)
        app = OracleApp(OracleAppStatus.get())
        app.apply_overrides(overrides)

    def get_replication_snapshot(self, context, snapshot_info,
                                 replica_source_config=None):
        LOG.debug("Getting replication snapshot.")
        app = OracleApp(OracleAppStatus.get())

        replication = REPLICATION_STRATEGY_CLASS(context)
        replication.enable_as_master(app, snapshot_info,
                                     replica_source_config)

        snapshot_id, log_position = (
            replication.snapshot_for_replication(context, app, None,
                                                 snapshot_info))

        mount_point = CONF.get(MANAGER).mount_point
        volume_stats = dbaas.get_filesystem_volume_stats(mount_point)

        replication_snapshot = {
            'dataset': {
                'datastore_manager': MANAGER,
                'dataset_size': volume_stats.get('used', 0.0),
                'volume_size': volume_stats.get('total', 0.0),
                'snapshot_id': snapshot_id
            },
            'replication_strategy': REPLICATION_STRATEGY,
            'master': replication.get_master_ref(app, snapshot_info),
            'log_position': log_position
        }

        return replication_snapshot

    def _validate_slave_for_replication(self, context, snapshot):
        if (snapshot['replication_strategy'] != REPLICATION_STRATEGY):
            raise exception.IncompatibleReplicationStrategy(
                snapshot.update({
                    'guest_strategy': REPLICATION_STRATEGY
                }))

        mount_point = CONF.get(MANAGER).mount_point
        volume_stats = dbaas.get_filesystem_volume_stats(mount_point)
        if (volume_stats.get('total', 0.0) <
                snapshot['dataset']['dataset_size']):
            raise exception.InsufficientSpaceForReplica(
                snapshot.update({
                    'slave_volume_size': volume_stats.get('total', 0.0)
                }))

    def attach_replication_slave(self, context, snapshot, slave_config):
        LOG.debug("Attaching replication snapshot.")
        app = OracleApp(OracleAppStatus.get())
        try:
            self._validate_slave_for_replication(context, snapshot)
            replication = REPLICATION_STRATEGY_CLASS(context)
            replication.enable_as_slave(app, snapshot, slave_config)
        except Exception:
            LOG.exception("Error enabling replication.")
            app.status.set_status(rd_instance.ServiceStatuses.FAILED)
            raise

    def detach_replica(self, context):
        LOG.debug("Detaching replica.")
        app = OracleApp(OracleAppStatus.get())
        replication = REPLICATION_STRATEGY_CLASS(context)
        replica_info = replication.detach_slave(app)
        return replica_info

    def cleanup_source_on_replica_detach(self, context, replica_info):
        LOG.debug("Cleaning up the source on the detach of a replica.")
        replication = REPLICATION_STRATEGY_CLASS(context)
        replication.cleanup_source_on_replica_detach(OracleAdmin(),
                                                     replica_info)

    def demote_replication_master(self, context):
        LOG.debug("Demoting replication master.")
        app = OracleApp(OracleAppStatus.get())
        replication = REPLICATION_STRATEGY_CLASS(context)
        replication.demote_master(app)

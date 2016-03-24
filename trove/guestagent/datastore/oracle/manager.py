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

from os import path
import re

from oslo_log import log as logging
from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as ds_instance
from trove.guestagent import backup
from trove.guestagent.common import guestagent_utils
from trove.guestagent.datastore import manager
from trove.guestagent.datastore.oracle import service
from trove.guestagent.datastore.oracle import system
from trove.guestagent import dbaas
from trove.guestagent.db import models
from trove.guestagent import guest_log
from trove.guestagent import volume

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.Manager):
    """
    This is the Oracle Manager class. It is dynamically loaded
    based off of the datastore of the Trove instance.
    """
    def __init__(self):
        super(Manager, self).__init__('oracle')
        self.appStatus = service.OracleAppStatus()
        self.app = service.OracleApp(self.appStatus)
        self.admin = service.OracleAdmin()

    @property
    def status(self):
        return self.appStatus

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

        if snapshot:
            self.attach_replica(context, snapshot, snapshot['config'])
        else:
            if backup_info:
                self._perform_restore(backup_info, context,
                                      mount_point, self.app)
            else:
                if databases:
                    # only create 1 database
                    self.admin.create_database(databases[:1])
                else:
                    # using ValidatedMySQLDatabase here for to simulate the
                    # object that would normally be passed in via --databases,
                    # and to bookmark this for when per-datastore validation is
                    # added
                    db = models.ValidatedMySQLDatabase()
                    # no database name provided so default to first 8 valid
                    # characters of instance name (alphanumeric, no '_')
                    db.name = re.sub(r'[\W_]', '', CONF.guest_name[:8])
                    self.admin.create_database([db.serialize()])

            self.refresh_guest_log_defs()

            self.app.prep_pfile_management()

            if root_password:
                self.admin.enable_root(root_password)


    def restart(self, context):
        LOG.debug("Restart an Oracle server instance.")
        self.app.restart()

    def stop_db(self, context, do_not_start_on_reboot=False):
        LOG.debug("Stop a given Oracle server instance.")
        self.app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        LOG.debug("Listing all databases.")
        return self.admin.list_databases(limit, marker, include_marker)

    def create_user(self, context, users):
        LOG.debug("Create user(s).")
        self.admin.create_user(users)

    def delete_user(self, context, user):
        LOG.debug("Delete a user %s." % user)
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
        LOG.debug("Starting Oracle with configuration changes.")
        self.app.start_db_with_conf_changes(config_contents)

    def reset_configuration(self, context, configuration):
        """
         Currently this method does nothing. This method needs to be
         implemented to enable rollback of flavor-resize on guestagent side.
        """
        LOG.debug("Resetting Oracle configuration.")
        pass

    @property
    def datastore_log_defs(self):
        if not self.appStatus.is_running:
            # do nothing if Oracle is not running
            return {}
        owner = system.ORACLE_INSTANCE_OWNER
        group = system.ORACLE_GROUP_OWNER
        sid = self.admin.database_name
        diag_dest = self.admin.get_parameter('diagnostic_dest')
        dbname = sid.lower()
        # alert log path:
        # <diagnostic_dest>/diag/rdbms/<dbname>/<instname>/alert/log.xml
        alert_log_file = self.validate_log_file(
            guestagent_utils.build_file_path(
                path.join(diag_dest, 'diag', 'rdbms', dbname, sid, 'alert'),
                'log', 'xml'
            ), owner, group=group
        )

        return {
            'alert': {
                self.GUEST_LOG_TYPE_LABEL: guest_log.LogType.SYS,
                self.GUEST_LOG_USER_LABEL: owner,
                self.GUEST_LOG_FILE_LABEL: alert_log_file,
            },
        }

    def change_passwords(self, context, users):
        LOG.debug("Changing password.")
        return self.admin.change_passwords(users)

    def update_attributes(self, context, username, hostname, user_attrs):
        LOG.debug("Updating database attributes.")
        return self.admin.update_attributes(
            username, hostname, user_attrs)

    def enable_root(self, context):
        LOG.debug("Enabling root.")
        return self.admin.enable_root()

    def disable_root(self, context):
        LOG.debug("Disabling root.")
        return self.admin.disable_root()

    def is_root_enabled(self, context):
        LOG.debug("Checking if root is enabled.")
        return self.admin.is_root_enabled()

    def _perform_restore(self, backup_info, context, restore_location, app):
        LOG.info(_("Restoring database from backup %s.") % backup_info['id'])
        try:
            backup.restore(context, backup_info, restore_location)
        except Exception:
            LOG.exception(_("Error performing restore from backup %s.") %
                          backup_info['id'])
            app.status.set_status(ds_instance.ServiceStatuses.FAILED)
            raise
        LOG.info(_("Restored database successfully."))

    def create_backup(self, context, backup_info):
        LOG.debug("Creating backup.")
        backup.backup(context, backup_info)

    def update_overrides(self, context, overrides, remove=False):
        LOG.debug("Update overrides request accepted.")
        if remove:
            self.app.remove_overrides()
        else:
            LOG.debug("Updating overrides: %s" % overrides)
            self.app.update_overrides(overrides)

    def apply_overrides(self, context, overrides):
        LOG.debug("Apply overrides request accepted.")
        if overrides:
            LOG.debug("Applying overrides: %s" % overrides)
            self.app.apply_overrides(overrides)

    def backup_required_for_replication(self, context):
        return self.replication.backup_required_for_replication()

    def post_processing_required_for_replication(self, context):
        return self.replication.post_processing_required_for_replication()

    def get_replication_snapshot(self, context, snapshot_info,
                                 replica_source_config=None):
        LOG.debug("Getting replication snapshot.")
        snapshot_id, log_position = (
            self.replication.snapshot_for_replication(context, self.app, None,
                                                      snapshot_info))
        mount_point = CONF.get(self.manager).mount_point
        volume_stats = dbaas.get_filesystem_volume_stats(mount_point)

        replication_snapshot = {
            'dataset': {
                'datastore_manager': self.manager,
                'dataset_size': volume_stats.get('used', 0.0),
                'volume_size': volume_stats.get('total', 0.0),
                'snapshot_id': snapshot_id
            },
            'replication_strategy': self.replication_strategy,
            'master': self.replication.get_master_ref(self.app, snapshot_info),
            'log_position': log_position,
            'replica_number': snapshot_info['replica_number']
        }

        return replication_snapshot

    def enable_as_master(self, context, replica_source_config):
        LOG.debug("Calling enable_as_master.")
        self.replication.enable_as_master(self.app, replica_source_config)

    def get_replication_detail(self, context):
        LOG.debug("Calling get_replication_detail.")
        return self.replication.get_replication_detail(self.app)

    def complete_master_setup(self, context, dbs):
        LOG.debug("Calling complete_master_setup.")
        self.replication.complete_master_setup(self.app, dbs)

    def complete_slave_setup(self, context, master_detail, slave_detail):
        LOG.debug("Calling complete_slave_setup.")
        self.replication.complete_slave_setup(
            self.app, master_detail, slave_detail)

    def sync_data_to_slaves(self, context):
        LOG.debug("Calling sync_data_to_slaves.")
        self.replication.sync_data_to_slaves(self.app)

    def detach_replica(self, context, for_failover=False):
        LOG.debug("Detaching replica.")
        replica_info = self.replication.detach_slave(self.app, for_failover)
        return replica_info

    def get_replica_context(self, context):
        LOG.debug("Getting replica context.")
        replica_info = self.replication.get_replica_context(self.app)
        return replica_info

    def _validate_slave_for_replication(self, context, replica_info):
        if (replica_info['replication_strategy'] != self.replication_strategy):
            raise exception.IncompatibleReplicationStrategy(
                replica_info.update({
                    'guest_strategy': self.replication_strategy
                }))

    def attach_replica(self, context, replica_info, slave_config):
        LOG.debug("Attaching replica.")
        try:
            if 'replication_strategy' in replica_info:
                self._validate_slave_for_replication(context, replica_info)
            if 'is_master' in replica_info and replica_info['is_master']:
                self.replication.enable_as_slave(self.app, replica_info,
                                                 slave_config)
            else:
                self.replication.prepare_slave(replica_info)
        except Exception:
            LOG.exception("Error enabling replication.")
            self.app.status.set_status(ds_instance.ServiceStatuses.FAILED)
            raise

    def make_read_only(self, context, read_only):
        LOG.debug("Executing make_read_only(%s)" % read_only)
        self.app.make_read_only(read_only)

    def _get_repl_info(self):
        return self.app.admin.get_info('replication')

    def _get_master_host(self):
        slave_info = self._get_repl_info()
        return slave_info and slave_info['master_host'] or None

    def _get_repl_offset(self):
        repl_info = self._get_repl_info()
        LOG.debug("Got repl info: %s" % repl_info)
        offset_key = '%s_repl_offset' % repl_info['role']
        offset = repl_info[offset_key]
        LOG.debug("Found offset %s for key %s." % (offset, offset_key))
        return int(offset)

    def get_last_txn(self, context):
        #master_host = self._get_master_host()
        #repl_offset = self._get_repl_offset()
        return None, None

    def get_latest_txn_id(self, context):
        LOG.info(_("Retrieving latest repl offset."))
        #return self._get_repl_offset()
        return None

    def wait_for_txn(self, context, txn):
        self.replication.wait_for_txn()

    def cleanup_source_on_replica_detach(self, context, replica_info):
        LOG.debug("Cleaning up the source on the detach of a replica.")
        self.replication.cleanup_source_on_replica_detach(
            self.app, replica_info)

    def demote_replication_master(self, context):
        LOG.debug("Demoting replica source.")
        self.replication.demote_master(self.app)

    def get_node_ip(self, context):
        LOG.debug("Retrieving cluster node ip address.")
        return self.app.get_node_ip()

    def get_node_id_for_removal(self, context):
        LOG.debug("Validating removal of node from cluster.")
        return self.app.get_node_id_for_removal()

    def remove_nodes(self, context, node_ids):
        LOG.debug("Removing nodes from cluster.")
        self.app.remove_nodes(node_ids)

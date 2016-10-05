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
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common.notification import EndNotification
from trove.guestagent import backup
from trove.guestagent.datastore.experimental.couchbase import service
from trove.guestagent.datastore import manager
from trove.guestagent import volume


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.Manager):
    """
    This is Couchbase Manager class. It is dynamically loaded
    based off of the datastore of the trove instance
    """

    def __init__(self, manager_name='couchbase'):
        super(Manager, self).__init__(manager_name)
        self._app = None
        self._admin = None

    @property
    def app(self):
        if self._app is None:
            self._app = self.build_app()
        return self._app

    def build_app(self):
        return service.CouchbaseApp()

    @property
    def admin(self):
        if self._admin is None:
            self._admin = self.app.build_admin()
        return self._admin

    @property
    def status(self):
        return self.app.status

    def reset_configuration(self, context, configuration):
        self.app.reset_configuration(configuration)

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot):
        """This is called from prepare in the base class."""
        self.app.install_if_needed(packages)

        if device_path:
            device = volume.VolumeDevice(device_path)
            # unmount if device is already mounted
            device.unmount_device(device_path)
            device.format()
            device.mount(mount_point)
            self.app.init_storage_structure(mount_point)
            LOG.debug('Mounted the volume (%s).' % device_path)

        self.app.start_db(update_db=False)

        self.app.initialize_node()

        if cluster_config:
            # If cluster configuration is provided retrieve the cluster
            # password and store it on the filesystem. Skip the cluster
            # initialization as it will be performed later from the
            # task manager.
            self.app.secure(password=cluster_config['cluster_password'],
                            initialize=False)
        else:
            self.app.secure(password=root_password, initialize=True)

        if backup_info:
            LOG.debug('Now going to perform restore.')
            self._perform_restore(backup_info,
                                  context,
                                  mount_point)
            self.app.apply_post_restore_updates(backup_info)

        self._admin = self.app.build_admin()

        if not cluster_config:
            if backup_info and self.is_root_enabled(context):
                self.status.report_root(context, self.app.DEFAULT_ADMIN_NAME)

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

    def create_user(self, context, users):
        with EndNotification(context):
            self.admin.create_user(context, users)

    def delete_user(self, context, user):
        with EndNotification(context):
            self.admin.delete_user(context, user)

    def get_user(self, context, username, hostname):
        return self.admin.get_user(context, username, hostname)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        return self.admin.list_users(context, limit, marker, include_marker)

    def change_passwords(self, context, users):
        with EndNotification(context):
            self.admin.change_passwords(context, users)

    def update_attributes(self, context, username, hostname, user_attrs):
        with EndNotification(context):
            self.admin.update_attributes(context, username, hostname,
                                         user_attrs)

    def enable_root(self, context):
        LOG.debug("Enabling root.")
        root = self.app.enable_root()
        self._admin = self.app.build_admin()
        return root

    def enable_root_with_password(self, context, root_password=None):
        return self.app.enable_root(root_password)

    def is_root_enabled(self, context):
        LOG.debug("Checking if root is enabled.")
        return os.path.exists(self.app.couchbase_pwd_file)

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

    def initialize_cluster(self, context, node_info):
        self.app.initialize_cluster(node_info=node_info)

    def get_cluster_password(self, context):
        return self.app.get_cluster_admin().password

    def get_cluster_rebalance_status(self, context):
        return self.app.get_cluster_rebalance_status()

    def add_nodes(self, context, node_info):
        self.app.rebalance_cluster(add_node_info=node_info)

    def remove_nodes(self, context, node_info):
        self.app.rebalance_cluster(remove_node_info=node_info)

    def pre_upgrade(self, context):
        LOG.debug('Preparing Couchbase for upgrade.')
        self.app.status.begin_restart()
        self.app.stop_db()
        mount_point = CONF.couchbase.mount_point
        upgrade_info = self.app.save_files_pre_upgrade(mount_point)
        upgrade_info['mount_point'] = mount_point
        return upgrade_info

    def post_upgrade(self, context, upgrade_info):
        LOG.debug('Finalizing Couchbase upgrade.')
        self.app.stop_db()
        if 'device' in upgrade_info:
            self.mount_volume(context, mount_point=upgrade_info['mount_point'],
                              device_path=upgrade_info['device'])
        self.app.restore_files_post_upgrade(upgrade_info)
        # password file has been restored at this point, need to refresh the
        # credentials stored in the app and admin by resetting them.
        self._app = None
        self._admin = None
        self.app.start_db()

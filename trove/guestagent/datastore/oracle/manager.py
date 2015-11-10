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

from oslo_log import log as logging
from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as ds_instance
from trove.guestagent import backup
from trove.guestagent.datastore import manager
from trove.guestagent.datastore.oracle import service
from trove.guestagent import dbaas
from trove.guestagent.db import models
from trove.guestagent import volume

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = 'oracle'


class Manager(manager.Manager):
    """
    This is the Oracle Manager class. It is dynamically loaded
    based off of the datastore of the Trove instance.
    """
    def __init__(self):
        super(Manager, self).__init__()
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

        if backup_info:
            self._perform_restore(backup_info, context,
                                  mount_point, self.app)
        else:
            # using ValidatedMySQLDatabase here for to simulate the object
            # that would normally be passed in via --databases, and to bookmark
            # this for when per-datastore validation is added
            db = models.ValidatedMySQLDatabase()
            db.name = CONF.guest_name
            self.admin.create_database([db.serialize()])

        if users:
            self.create_user(context, users)

        if root_password:
            self.admin.enable_root(root_password)

    def restart(self, context):
        LOG.debug("Restart an Oracle server instance.")
        self.app.restart()

    def stop_db(self, context, do_not_start_on_reboot=False):
        LOG.debug("Stop a given Oracle server instance.")
        self.app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        LOG.debug("Get the filesystem stats.")
        mount_point = CONF.get(MANAGER).mount_point
        return dbaas.get_filesystem_volume_stats(mount_point)

    def create_database(self, context, databases):
        LOG.debug("Creating database(s)." % databases)
        raise exception.DatastoreOperationNotSupported(
            operation='create_database', datastore=MANAGER)

    def delete_database(self, context, database):
        LOG.debug("Deleting database %s." % database)
        raise exception.DatastoreOperationNotSupported(
            operation='delete_database', datastore=MANAGER)

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
        LOG.debug("Resetting Oracle configuration.")
        pass

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

    def get_config_changes(self, cluster_config, mount_point=None):
        LOG.debug("Get configuration changes")
        raise exception.DatastoreOperationNotSupported(
            operation='get_configuration_changes', datastore=MANAGER)

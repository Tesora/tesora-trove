# flake8: noqa

# Copyright (c) 2016 Tesora, Inc.
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
from trove.guestagent.datastore.oracle_common import manager
from trove.guestagent.datastore.oracle_rac import service

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.OracleManager):

    def __init__(self):
        super(Manager, self).__init__(service.OracleRACApp,
                                      service.OracleRACAppStatus,
                                      manager_name='oracle_rac')

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path=None, mount_point=None, backup_info=None,
                   config_contents=None, root_password=None, overrides=None,
                   cluster_config=None, snapshot=None):

        LOG.debug("Oracle RAC preparing!")
        if not cluster_config:
            raise exception.DatastoreOperationNotSupported(
                "Oracle RAC does not support single instance configuration.")
        app = self.app
        app.mount_storage(cluster_config['storage'])
        app.configure_ssh(cluster_config['ssh_pem'], cluster_config['ssh_pub'])
        app.store_cluster_info(cluster_config['database'],
                               cluster_config['sys_password'],
                               cluster_config['admin_password'])

    def configure_hosts(self, context, cluster_name, public_cidr, private_cidr):
        LOG.debug("Configuring hosts for {name} using public {pub} "
                  "and private {pri}".format(
                      name=cluster_name, pub=public_cidr, pri=private_cidr))
        self.app.configure_hosts(cluster_name, public_cidr, private_cidr)

    def establish_ssh_user_equivalency(self, context, host_ip_pairs):
        LOG.debug("Establishing SSH user equivalency with hosts "
                  "{pairs}".format(pairs=str(host_ip_pairs)))
        self.app.establish_ssh_user_equivalency(host_ip_pairs)

    def configure_grid(self, context, cluster_id, cluster_name, nodes_string,
                       public_cidr, private_cidr):
        LOG.debug("Configuring GRID for cluster {name}.".format(
            name=cluster_name))
        self.app.configure_grid(cluster_id, cluster_name, nodes_string,
                                public_cidr, private_cidr)

    def run_grid_root(self, context):
        LOG.debug("Running GRID root script.")
        self.app.run_grid_root()

    def install_oracle_database(self, context, nodes_string):
        LOG.debug("Installing Oracle database software.")
        self.app.install_oracle_database(nodes_string)

    def run_oracle_root(self, context):
        LOG.debug("Running Oracle database root script.")
        self.app.run_oracle_root()

    def get_private_vip(self, context, ip):
        LOG.debug("Getting private interconnect virtual IP.")
        return self.app.get_private_vip(ip)

    def create_rac_database(self, context, nodes_string):
        LOG.debug("Creating the RAC database.")
        return self.app.create_rac_database(nodes_string)

    def determine_sid(self, context):
        LOG.debug("Determing node's SID.")
        self.app.determine_sid()

    def apply_overrides_on_prepare(self, context, overrides):
        LOG.debug("Ignoring configuration for %s" % self.manager_name)
        pass

    def get_filesystem_stats(self, context, fs_path):
        """Gets the filesystem stats for the path given."""
        # Oracle RAC don't have any filesystem info to report
        return {}

    def mount_volume(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='mount_volume', datastore=self.manager)

    def unmount_volume(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='unmount_volume', datastore=self.manager)

    def resize_fs(self, context, device_path=None, mount_point=None):
        raise exception.DatastoreOperationNotSupported(
            operation='resize_fs', datastore=self.manager)

    def reset_configuration(self, context, configuration):
        raise exception.DatastoreOperationNotSupported(
            operation='reset_configuration', datastore=self.manager)

    def create_database(self, context, databases):
        raise exception.DatastoreOperationNotSupported(
            operation='create_database', datastore=self.manager)

    def delete_database(self, context, database):
        raise exception.DatastoreOperationNotSupported(
            operation='delete_database', datastore=self.manager)

    def grant_access(self, context, username, hostname, databases):
        raise exception.DatastoreOperationNotSupported(
            operation='grant_access', datastore=self.manager)

    def revoke_access(self, context, username, hostname, database):
        raise exception.DatastoreOperationNotSupported(
            operation='revoke_access', datastore=self.manager)

    def list_access(self, context, username, hostname):
        raise exception.DatastoreOperationNotSupported(
            operation='list_access', datastore=self.manager)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        raise exception.DatastoreOperationNotSupported(
            operation='list_databases', datastore=self.manager)

    def disable_root(self, context):
        LOG.debug("Disabling root.")
        raise exception.DatastoreOperationNotSupported(
            operation='disable_root', datastore=self.manager)

    def restart(self, context):
        raise exception.DatastoreOperationNotSupported(
            operation='restart', datastore=self.manager)

    def start_db_with_conf_changes(self, context, config_contents):
        raise exception.DatastoreOperationNotSupported(
            operation='start_db_with_conf_changes', datastore=self.manager)

    def update_overrides(self, context, overrides, remove=False):
        raise exception.DatastoreOperationNotSupported(
            operation='update_overrides', datastore=self.manager)

    def apply_overrides(self, context, overrides):
        raise exception.DatastoreOperationNotSupported(
            operation='apply_overrides', datastore=self.manager)

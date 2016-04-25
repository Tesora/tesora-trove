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

import abc

from oslo_log import log as logging
from trove.common import cfg
from trove.common.notification import EndNotification
from trove.guestagent.datastore import manager

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class OracleManager(manager.Manager):

    def __init__(self, oracle_app, oracle_app_status, manager_name):
        super(OracleManager, self).__init__(manager_name)
        self._oracle_app = oracle_app
        self._oracle_app_status = oracle_app_status

    @property
    def status(self):
        return self._oracle_app_status()

    @property
    def app(self):
        return self._oracle_app(self.status)

    @property
    def admin(self):
        return self.app.admin

    @abc.abstractmethod
    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info, config_contents,
                   root_password, overrides, cluster_config, snapshot):
        pass

    def change_passwords(self, context, users):
        with EndNotification(context):
            self.admin.change_passwords(users)

    def update_attributes(self, context, username, hostname, user_attrs):
        with EndNotification(context):
            self.admin.update_attributes(username, hostname, user_attrs)

    def reset_configuration(self, context, configuration):
        self.app.reset_configuration(configuration)

    def create_database(self, context, databases):
        with EndNotification(context):
            return self.admin.create_database(databases)

    def create_user(self, context, users):
        with EndNotification(context):
            self.admin.create_user(users)

    def delete_database(self, context, database):
        with EndNotification(context):
            return self.admin.delete_database(database)

    def delete_user(self, context, user):
        with EndNotification(context):
            self.admin.delete_user(user)

    def get_user(self, context, username, hostname):
        return self.admin.get_user(username, hostname)

    def grant_access(self, context, username, hostname, databases):
        return self.admin.grant_access(username, hostname, databases)

    def revoke_access(self, context, username, hostname, database):
        return self.admin.revoke_access(username, hostname, database)

    def list_access(self, context, username, hostname):
        return self.admin.list_access(username, hostname)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        return self.admin.list_databases(limit, marker, include_marker)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        return self.admin.list_users(limit, marker, include_marker)

    def enable_root(self, context):
        return self.admin.enable_root()

    def enable_root_with_password(self, context, root_password=None):
        return self.admin.enable_root(root_password)

    def is_root_enabled(self, context):
        return self.admin.is_root_enabled()

    def disable_root(self, context):
        return self.admin.disable_root()

    def restart(self, context):
        self.app.restart()

    def start_db_with_conf_changes(self, context, config_contents):
        self.app.start_db_with_conf_changes(config_contents)

    def stop_db(self, context, do_not_start_on_reboot=False):
        self.app.stop_db(do_not_start_on_reboot=do_not_start_on_reboot)

    def update_overrides(self, context, overrides, remove=False):
        app = self.app
        if remove:
            app.remove_overrides()
        app.update_overrides(overrides)

    def apply_overrides(self, context, overrides):
        self.app.apply_overrides(overrides)

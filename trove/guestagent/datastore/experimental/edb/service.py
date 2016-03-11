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

from trove.common import cfg
from trove.common import utils

from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.postgresql import (
    service as community_service
)
from trove.guestagent.db import models

CONF = cfg.CONF

MANAGER = CONF.datastore_manager or 'edb'
IGNORE_DBS_LIST = CONF.get(MANAGER).ignore_dbs
IGNORE_USERS_LIST = CONF.get(MANAGER).ignore_users


class EDBApp(community_service.PgSqlApp):

    def __init__(self):
        super(EDBApp, self).__init__()

    @property
    def service_candidates(self):
        return ['ppas-%s' % self.pg_version[1]]

    @property
    def pgsql_owner(self):
        return 'enterprisedb'

    @property
    def default_superuser_name(self):
        return "enterprisedb"

    @property
    def pgsql_base_data_dir(self):
        return '/var/lib/ppas/'

    @property
    def pgsql_config_dir(self):
        return {
            operating_system.DEBIAN: '/etc/postgresql/',
            operating_system.REDHAT: '/var/lib/ppas/',
            operating_system.SUSE: '/var/lib/pgsql/'
        }[self.OS]

    @property
    def pgsql_extra_bin_dir(self):
        return {
            operating_system.DEBIAN: '/usr/lib/postgresql/%s/bin/',
            operating_system.REDHAT: '/usr/ppas-%s/bin/',
            operating_system.SUSE: '/usr/bin/'
        }[self.OS] % self.pg_version[1]

    def secure(self, context):
        # EDB's 'enterprisedb' user does not have the home database pre-created
        # out of the box. We need to create it here.
        utils.execute_with_timeout(
            "createdb", "-h", "localhost", "-U", "enterprisedb",
            "enterprisedb", log_output_on_error=True)
        super(EDBApp, self).secure(context)

    def build_root_user(self, password=None):
        return models.EnterpriseDBRootUser(password=password)

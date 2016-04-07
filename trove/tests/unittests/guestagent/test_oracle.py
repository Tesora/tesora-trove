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

import mock
from mock import DEFAULT

import trove.common.cfg as cfg
import trove.common.context as context
import trove.guestagent.db.models as models
from trove.tests.unittests import trove_testtools


CONF = cfg.CONF


class GuestAgentManagerTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentManagerTest, self).setUp()
        self.context = context.TroveContext()

        # Mock out the Oracle driver cx_Oracle before importing
        # the guestagent functionality
        # self.oracle_patch = mock.patch.dict('sys.modules', {'cx_Oracle': mock.Mock()})
        # self.addCleanup(self.oracle_patch.stop)
        # self.oracle_patch.start()
        # import trove.guestagent.datastore.oracle.manager as manager
        # import trove.guestagent.datastore.oracle.service as dbaas
        # self.manager = manager.Manager()
        # self.dbaas = dbaas

        # self.LocalOracleClient = self.dbaas.LocalOracleClient
        # self.dbaas.LocalOracleClient = mock.MagicMock()

    def tearDown(self):
        # self.dbaas.LocalOracleClient = self.LocalOracleClient
        # self.oracle_patch.stop()
        super(GuestAgentManagerTest, self).tearDown()

    """
    def test_prepare(self):
        CONF.guest_name = 'testdb'
        schema = models.ValidatedMySQLDatabase()
        schema.name = 'testdb'
        with mock.patch.multiple(self.manager,
                                 admin=DEFAULT,
                                 app=DEFAULT,
                                 refresh_guest_log_defs=DEFAULT):
            self.manager.do_prepare(
                self.context, None, None, None, None
            )
            self.manager.refresh_guest_log_defs.assert_any_call()
            self.manager.app.prep_pfile_management.assert_any_call()
            self.manager.admin.create_database.assert_called_with([schema.serialize()])
    """

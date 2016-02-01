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

import sys
import testtools
import mock

import testtools.content as content
import trove.common.cfg as cfg
import trove.common.context as context
import trove.common.exception as exception
from trove.tests.unittests import trove_testtools


CONF = cfg.CONF


class GuestAgentManagerTest(trove_testtools.TestCase):

    def setUp(self):
        super(GuestAgentManagerTest, self).setUp()
        self.context = context.TroveContext()

        # Mock out the Oracle driver cx_Oracle before importing
        # the guestagent functionality
        self.oracle_patch = mock.patch.dict('sys.modules', {'cx_Oracle': mock.Mock()})
        self.addCleanup(self.oracle_patch.stop)
        self.oracle_patch.start()
        import trove.guestagent.datastore.oracle_ra.manager as manager
        import trove.guestagent.datastore.oracle_ra.service as dbaas
        self.manager = manager.Manager()
        self.dbaas = dbaas

        self.LocalOracleClient = self.dbaas.LocalOracleClient
        self.dbaas.LocalOracleClient = mock.MagicMock()

    def tearDown(self):
        self.dbaas.LocalOracleClient = self.LocalOracleClient
        self.oracle_patch.stop()
        super(GuestAgentManagerTest, self).tearDown()

    def test_update_state(self):
        mock_status = mock.MagicMock()
        with mock.patch.object(self.dbaas.OracleAppStatus, 'get', return_value=mock_status):
            self.manager.update_status(self.context)
            self.dbaas.OracleAppStatus.get.assert_any_call()
            mock_status.update.assert_any_call()

    def test_create_database(self):
        # codepath to create
        with mock.patch.object(self.dbaas.OracleAdmin, 'create_database'):
            self.manager.create_database(self.context)
            self.dbaas.OracleAdmin.create_database.assert_any_call()

        # verify names
        data = [{'name': 'valid', 'result': True},
                {'name': 'underscore_allowed', 'result': True},
                {'name': '_underscores_allowed_EXCEPT_at_the_start', 'result': False},
                {'name': 'CAPS_are_OK', 'result': True},
                {'name': '123_numbers_allowed_890', 'result': True},
                {'name': 'dashes-not-allowed', 'result': False},
                {'name': 'this_IS_valid_but_is_the_max_size_1234567890_fyi_max_len_is_64__',
                 'result': True},
                {'name': 'this_IS_NOT_valid_as_it_is_greater_than_the_max_size_of_64_chars_',
                 'result': False}]

        def _test_name(test):
            CONF.guest_name = test['name']
            # this will cause the name to be dumped if the test fails
            self.addDetail('dbname', content.text_content(test['name']))
            if test['result']:
                self.manager.create_database(self.context)
            else:
                self.assertRaises(exception.BadRequest,
                                  self.manager.create_database,
                                  self.context)

        for set in data:
            _test_name(set)

    def test_update_attributes(self):
        # update hostname
        self.assertRaises(exception.DatastoreOperationNotSupported,
                          self.manager.update_attributes,
                          self.context, 'old_user', 'old_host',
                          {'host': 'a_new_hostname'})
        # update username
        self.assertRaises(exception.DatastoreOperationNotSupported,
                          self.manager.update_attributes,
                          self.context, 'old_user', 'old_host',
                          {'name': 'a_new_username'})
#        # update password
#        with mock.patch.object(models.OracleUser, '_is_valid_user_name', return_value=True):
#            with mock.patch.object(self.dbaas.OracleAdmin, 'change_passwords') as change_passwords:
#                user_attr = {'host': None, 'name': None,
#                             'password': 'a_new_password'}
#                self.manager.update_attributes(self.context,
#                                               'old_user', 'old_host',
#                                               user_attr)
#                # do a manual 'is called' assert as the passed in object is
#                # not mocked, then check the type of the arg
#                self.assertEqual(change_passwords.called, True,
#                                 'OracleAdmin.change_passwords was not called.')
#                self.assertEqual(type(change_passwords.call_args[0][0][0]),
#                                 models.OracleUser)

#    def test_delete_user(self):
#        with mock.patch.object(models.OracleUser, '_is_valid_user_name', return_value=True):
#        user = models.OracleUser('username')
#        with mock.patch.object(self.dbaas.OracleAdmin, 'delete_user_by_name') as delete_user_by_name:
#            self.manager.delete_user(self.context, user.serialize())
#            delete_user_by_name.assert_any_call(user.name)

#    def test_root_enable(self):
#        with mock.patch.object(models.OracleUser, '_is_valid_user_name', return_value=True):
#            root_user = models.OracleUser(None)
#            root_user.deserialize(self.manager.enable_root(self.context))
#            self.assertEqual(self.dbaas.ROOT_USERNAME, root_user.name,
#                             'Username does not match.')
#            self.assertEqual(self.dbaas.PASSWORD_MAX_LEN, len(root_user.password),
#                             'Password length does not match.')

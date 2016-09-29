# Copyright 2015 Tesora Inc.
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

from six.moves.urllib import parse as urllib_parse

from proboscis import SkipTest

from trove.common import exception
from trove.common.utils import poll_until
from trove.tests.scenario import runners
from trove.tests.scenario.runners.test_runners import SkipKnownBug
from trove.tests.scenario.runners.test_runners import TestRunner
from troveclient.compat import exceptions


class UserActionsRunner(TestRunner):

    # TODO(pmalik): I believe the 202 (Accepted) should be replaced by
    # 200 (OK) as the actions are generally very fast and their results
    # available immediately upon execution of the request. This would
    # likely require replacing GA casts with calls which I believe are
    # more appropriate anyways.

    def __init__(self):
        super(UserActionsRunner, self).__init__()
        self.user_defs = []
        self.renamed_user_orig_def = None

    @property
    def first_user_def(self):
        if self.user_defs:
            # Try to use the first user with databases if any.
            for user_def in self.user_defs:
                if 'databases' in user_def and user_def['databases']:
                    return user_def
            return self.user_defs[0]
        raise SkipTest("No valid user definitions provided.")

    @property
    def non_existing_user_def(self):
        user_def = self.test_helper.get_non_existing_user_definition()
        if user_def:
            return user_def
        raise SkipTest("No valid user definitions provided.")

    def run_users_create(self, expected_http_code=202):
        users = self.test_helper.get_valid_user_definitions()
        if users:
            self.user_defs = self.assert_users_create(
                self.instance_info.id, users, expected_http_code)
        else:
            raise SkipTest("No valid user definitions provided.")

    def assert_users_create(self, instance_id, serial_users_def,
                            expected_http_code):
        self.auth_client.users.create(instance_id, serial_users_def)
        self.assert_client_code(expected_http_code)
        self.wait_for_user_create(instance_id, serial_users_def)
        return serial_users_def

    def run_user_show(self, expected_http_code=200):
        for user_def in self.user_defs:
            self.assert_user_show(
                self.instance_info.id, user_def, expected_http_code)

    def assert_user_show(self, instance_id, expected_user_def,
                         expected_http_code):
        user_name = expected_user_def['name']
        user_host = expected_user_def.get('host')

        queried_user = self.auth_client.users.get(
            instance_id, user_name, user_host)
        self.assert_client_code(expected_http_code)
        self._assert_user_matches(queried_user, expected_user_def)

    def _assert_user_matches(self, user, expected_user_def):
        user_name = expected_user_def['name']
        for key, expected in expected_user_def.items():
            if key not in self.ignored_user_attributes:
                self.assert_true(
                    hasattr(user, key),
                    "Returned user '%s' does not have attribute '%s'."
                    % (user_name, key))
                actual = getattr(user, key)
                if isinstance(expected, (list, tuple)):
                    self.assert_list_elements_equal(
                        expected, actual,
                        "Mismatch element in list attribute '%s' of user: %s" %
                        (user_name, key))
                else:
                    self.assert_equal(expected, actual,
                                      "Mismatch in attribute '%s' of user: %s"
                                      % (user_name, key))

    def run_users_list(self, expected_http_code=200):
        self.assert_users_list(
            self.instance_info.id, self.user_defs, expected_http_code)

    def assert_users_list(self, instance_id, expected_user_defs,
                          expected_http_code, limit=2):
        full_list = self.auth_client.users.list(instance_id)
        self.assert_client_code(expected_http_code)
        listed_users = {user.name: user for user in full_list}
        self.assert_is_none(full_list.next,
                            "Unexpected pagination in the list.")

        for user_def in expected_user_defs:
            user_name = user_def['name']
            self.assert_true(
                user_name in listed_users,
                "User not included in the 'user-list' output: %s" %
                user_name)
            self._assert_user_matches(listed_users[user_name], user_def)

        # Check that the system (ignored) users are not included in the output.
        system_users = self.get_system_users()
        self.assert_false(
            any(name in listed_users for name in system_users),
            "System users should not be included in the 'user-list' output.")

        # Test list pagination.
        list_page = self.auth_client.users.list(instance_id, limit=limit)
        self.assert_client_code(expected_http_code)

        self.assert_true(len(list_page) <= limit)
        if len(full_list) > limit:
            self.assert_is_not_none(list_page.next, "List page is missing.")
        else:
            self.assert_is_none(list_page.next, "An extra page in the list.")
        marker = list_page.next

        self.assert_pagination_match(list_page, full_list, 0, limit,
                                     comp=self.users_equal)
        if marker:
            last_user = list_page[-1]
            expected_marker = self.as_pagination_marker(last_user)
            self.assert_equal(expected_marker, marker,
                              "Pagination marker should be the last element "
                              "in the page.")
            list_page = self.auth_client.users.list(instance_id, marker=marker)
            self.assert_client_code(expected_http_code)
            self.assert_pagination_match(
                list_page, full_list, limit, len(full_list),
                comp=self.users_equal)

    def as_pagination_marker(self, user):
        return urllib_parse.quote(user.name)

    def users_equal(self, a, b):
        return self._users_equal(
            a, b, ignored_attributes=self.ignored_user_attributes)

    @property
    def ignored_user_attributes(self):
        """Any user properties that are either not returned from the server or
        should not be take into account in comparisons.
        """
        return ['password']

    def _users_equal(self, a, b, ignored_attributes=None):
        a_dict = self.copy_dict(a.__dict__, ignored_keys=ignored_attributes)
        b_dict = self.copy_dict(b.__dict__, ignored_keys=ignored_attributes)
        return a_dict == b_dict

    def run_user_access_show(self, expected_http_code=200):
        for user_def in self.user_defs:
            self.assert_user_access_show(
                self.instance_info.id, user_def, expected_http_code)

    def assert_user_access_show(self, instance_id, user_def,
                                expected_http_code):
        user_name, user_host = self._get_user_name_host_pair(user_def)
        user_dbs = self.auth_client.users.list_access(instance_id, user_name,
                                                      hostname=user_host)
        self.assert_client_code(expected_http_code)

        expected_dbs = {db_def['name'] for db_def in user_def['databases']}
        listed_dbs = [db.name for db in user_dbs]

        self.assert_equal(len(expected_dbs), len(listed_dbs),
                          "Unexpected number of databases on the user access "
                          "list.")

        for database in expected_dbs:
            self.assert_true(
                database in listed_dbs,
                "Database not found in the user access list: %s" % database)

    def run_user_access_revoke(self, expected_http_code=202):
        self._apply_on_all_databases(
            self.instance_info.id, self.assert_user_access_revoke,
            expected_http_code)

    def _apply_on_all_databases(self, instance_id, action, expected_http_code):
        if any(user_def['databases'] for user_def in self.user_defs):
            for user_def in self.user_defs:
                user_name, user_host = self._get_user_name_host_pair(user_def)
                db_defs = user_def['databases']
                for db_def in db_defs:
                    db_name = db_def['name']
                    action(instance_id, user_name, user_host,
                           db_name, expected_http_code)
        else:
            raise SkipTest("No user databases defined.")

    def assert_user_access_revoke(self, instance_id, user_name, user_host,
                                  database, expected_http_code):
        self.auth_client.users.revoke(
            instance_id, user_name, database, hostname=user_host)
        self.assert_client_code(expected_http_code)
        user_dbs = self.auth_client.users.list_access(
            instance_id, user_name, hostname=user_host)
        self.assert_false(any(db.name == database for db in user_dbs),
                          "Database should no longer be included in the user "
                          "access list after revoke: %s" % database)

    def run_user_access_grant(self, expected_http_code=202):
        self._apply_on_all_databases(
            self.instance_info.id, self.assert_user_access_grant,
            expected_http_code)

    def assert_user_access_grant(self, instance_id, user_name, user_host,
                                 database, expected_http_code):
        self.auth_client.users.grant(
            instance_id, user_name, [database], hostname=user_host)
        self.assert_client_code(expected_http_code)
        user_dbs = self.auth_client.users.list_access(
            instance_id, user_name, hostname=user_host)
        self.assert_true(any(db.name == database for db in user_dbs),
                         "Database should be included in the user "
                         "access list after granting access: %s" % database)

    def run_user_create_with_no_attributes(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        self.assert_users_create_failure(
            self.instance_info.id, {}, expected_exception, expected_http_code)

    def run_user_create_with_blank_name(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        # Test with missing user name attribute.
        no_name_usr_def = self.copy_dict(self.non_existing_user_def,
                                         ignored_keys=['name'])
        self.assert_users_create_failure(
            self.instance_info.id, no_name_usr_def,
            expected_exception, expected_http_code)

        # Test with empty user name attribute.
        blank_name_usr_def = self.copy_dict(self.non_existing_user_def)
        blank_name_usr_def.update({'name': ''})
        self.assert_users_create_failure(
            self.instance_info.id, blank_name_usr_def,
            expected_exception, expected_http_code)

    def run_user_create_with_blank_password(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        # Test with missing password attribute.
        no_pass_usr_def = self.copy_dict(self.non_existing_user_def,
                                         ignored_keys=['password'])
        self.assert_users_create_failure(
            self.instance_info.id, no_pass_usr_def,
            expected_exception, expected_http_code)

        # Test with missing databases attribute.
        no_db_usr_def = self.copy_dict(self.non_existing_user_def,
                                       ignored_keys=['databases'])
        self.assert_users_create_failure(
            self.instance_info.id, no_db_usr_def,
            expected_exception, expected_http_code)

    def run_existing_user_create(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        self.assert_users_create_failure(
            self.instance_info.id, self.first_user_def,
            expected_exception, expected_http_code)

    def run_system_user_create(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        # TODO(pmalik): Actions on system users and databases should probably
        # return Forbidden 403 instead. The current error messages are
        # confusing (talking about a malformed request).
        system_users = self.get_system_users()
        if system_users:
            user_defs = [{'name': name, 'password': 'password1',
                          'databases': []} for name in system_users]
            self.assert_users_create_failure(
                self.instance_info.id, user_defs,
                expected_exception, expected_http_code)

    def assert_users_create_failure(
            self, instance_id, serial_users_def,
            expected_exception, expected_http_code):
        self.assert_raises(
            expected_exception, expected_http_code,
            self.auth_client.users.create, instance_id, serial_users_def)

    def run_user_update_with_blank_name(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        self.assert_user_attribute_update_failure(
            self.instance_info.id, self.first_user_def, {'name': ''},
            expected_exception, expected_http_code)

    def run_user_update_with_existing_name(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        self.assert_user_attribute_update_failure(
            self.instance_info.id, self.first_user_def,
            {'name': self.first_user_def['name']},
            expected_exception, expected_http_code)

    def assert_user_attribute_update_failure(
            self, instance_id, user_def, update_attribites,
            expected_exception, expected_http_code):
        user_name, user_host = self._get_user_name_host_pair(user_def)

        self.assert_raises(
            expected_exception, expected_http_code,
            self.auth_client.users.update_attributes, instance_id,
            user_name, update_attribites, user_host)

    def _get_user_name_host_pair(self, user_def):
        return user_def['name'], user_def.get('host')

    def run_system_user_attribute_update(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        # TODO(pmalik): Actions on system users and databases should probably
        # return Forbidden 403 instead. The current error messages are
        # confusing (talking about a malformed request).
        system_users = self.get_system_users()
        if system_users:
            for name in system_users:
                user_def = {'name': name, 'password': 'password2'}
                self.assert_user_attribute_update_failure(
                    self.instance_info.id, user_def, user_def,
                    expected_exception, expected_http_code)

    def run_user_attribute_update(self, expected_http_code=202):
        updated_def = self.first_user_def
        # Update the name by appending a random string to it.
        updated_name = ''.join([updated_def['name'], 'upd'])
        update_attribites = {'name': updated_name,
                             'password': 'password2'}
        self.assert_user_attribute_update(
            self.instance_info.id, updated_def,
            update_attribites, expected_http_code)

    def assert_user_attribute_update(self, instance_id, user_def,
                                     update_attribites, expected_http_code):
        user_name, user_host = self._get_user_name_host_pair(user_def)

        self.auth_client.users.update_attributes(
            instance_id, user_name, update_attribites, user_host)
        self.assert_client_code(expected_http_code)

        # Update the stored definitions with the new value.
        expected_def = None
        for user_def in self.user_defs:
            if user_def['name'] == user_name:
                self.renamed_user_orig_def = dict(user_def)
                user_def.update(update_attribites)
                expected_def = user_def

        self.wait_for_user_create(instance_id, self.user_defs)

        # Verify using 'user-show' and 'user-list'.
        self.assert_user_show(instance_id, expected_def, 200)
        self.assert_users_list(instance_id, self.user_defs, 200)

    def run_user_recreate_with_no_access(self, expected_http_code=202):
        if (self.renamed_user_orig_def and
                self.renamed_user_orig_def['databases']):
            self.assert_user_recreate_with_no_access(
                self.instance_info.id, self.renamed_user_orig_def,
                expected_http_code)
        else:
            raise SkipTest("No renamed users with databases.")

    def assert_user_recreate_with_no_access(self, instance_id, original_def,
                                            expected_http_code=202):
        # Recreate a previously renamed user without assigning any access
        # rights to it.
        recreated_user_def = dict(original_def)
        recreated_user_def.update({'databases': []})
        user_def = self.assert_users_create(
            instance_id, [recreated_user_def], expected_http_code)

        # Append the new user to defs for cleanup.
        self.user_defs.extend(user_def)

        # Assert empty user access.
        self.assert_user_access_show(instance_id, recreated_user_def, 200)

    def run_user_delete(self, expected_http_code=202):
        for user_def in self.user_defs:
            self.assert_user_delete(
                self.instance_info.id, user_def, expected_http_code)

    def assert_user_delete(self, instance_id, user_def, expected_http_code):
        user_name, user_host = self._get_user_name_host_pair(user_def)

        self.auth_client.users.delete(instance_id, user_name, user_host)
        self.assert_client_code(expected_http_code)
        self._wait_for_user_delete(instance_id, user_name)

    def _wait_for_user_delete(self, instance_id, deleted_user_name):
        self.report.log("Waiting for deleted user to disappear from the "
                        "listing: %s" % deleted_user_name)

        def _db_is_gone():
            all_users = self.get_user_names(instance_id)
            return deleted_user_name not in all_users

        try:
            poll_until(_db_is_gone, time_out=self.GUEST_CAST_WAIT_TIMEOUT_SEC)
            self.report.log("User is now gone from the instance.")
        except exception.PollTimeOut:
            self.fail("User still listed after the poll timeout: %ds" %
                      self.GUEST_CAST_WAIT_TIMEOUT_SEC)

    def run_nonexisting_user_show(
            self, expected_exception=exceptions.NotFound,
            expected_http_code=404):
        self.assert_user_show_failure(
            self.instance_info.id,
            {'name': self.non_existing_user_def['name']},
            expected_exception, expected_http_code)

    def assert_user_show_failure(self, instance_id, user_def,
                                 expected_exception, expected_http_code):
        user_name, user_host = self._get_user_name_host_pair(user_def)

        self.assert_raises(
            expected_exception, expected_http_code,
            self.auth_client.users.get, instance_id, user_name, user_host)

    def run_system_user_show(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        # TODO(pmalik): Actions on system users and databases should probably
        # return Forbidden 403 instead. The current error messages are
        # confusing (talking about a malformed request).
        system_users = self.get_system_users()
        if system_users:
            for name in system_users:
                self.assert_user_show_failure(
                    self.instance_info.id, {'name': name},
                    expected_exception, expected_http_code)

    def run_nonexisting_user_update(self, expected_http_code=404):
        # Test valid update on a non-existing user.
        update_def = {'name': self.non_existing_user_def['name']}
        self.assert_user_attribute_update_failure(
            self.instance_info.id, update_def, update_def,
            exceptions.NotFound, expected_http_code)

    def run_nonexisting_user_delete(
            self, expected_exception=exceptions.NotFound,
            expected_http_code=404):
        self.assert_user_delete_failure(
            self.instance_info.id,
            {'name': self.non_existing_user_def['name']},
            expected_exception, expected_http_code)

    def assert_user_delete_failure(
            self, instance_id, user_def,
            expected_exception, expected_http_code):
        user_name, user_host = self._get_user_name_host_pair(user_def)

        self.assert_raises(expected_exception, expected_http_code,
                           self.auth_client.users.delete,
                           instance_id, user_name, user_host)

    def run_system_user_delete(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        # TODO(pmalik): Actions on system users and databases should probably
        # return Forbidden 403 instead. The current error messages are
        # confusing (talking about a malformed request).
        system_users = self.get_system_users()
        if system_users:
            for name in system_users:
                self.assert_user_delete_failure(
                    self.instance_info.id, {'name': name},
                    expected_exception, expected_http_code)

    def get_system_users(self):
        return self.get_datastore_config_property('ignore_users')


class MysqlUserActionsRunner(UserActionsRunner):

    def as_pagination_marker(self, user):
        return urllib_parse.quote('%s@%s' % (user.name, user.host))


class MariadbUserActionsRunner(MysqlUserActionsRunner):

    def __init__(self):
        super(MariadbUserActionsRunner, self).__init__()


class PerconaUserActionsRunner(MysqlUserActionsRunner):

    def __init__(self):
        super(PerconaUserActionsRunner, self).__init__()


class PxcUserActionsRunner(MysqlUserActionsRunner):

    def __init__(self):
        super(PxcUserActionsRunner, self).__init__()


class PostgresqlUserActionsRunner(UserActionsRunner):

    def run_user_update_with_existing_name(self):
        raise SkipKnownBug(runners.BUG_WRONG_API_VALIDATION)

    def run_system_user_show(self):
        raise SkipKnownBug(runners.BUG_WRONG_API_VALIDATION)

    def run_system_user_attribute_update(self):
        raise SkipKnownBug(runners.BUG_WRONG_API_VALIDATION)

    def run_system_user_delete(self):
        raise SkipKnownBug(runners.BUG_WRONG_API_VALIDATION)


class CouchbaseUserActionsRunner(UserActionsRunner):

    def run_user_attribute_update(self, expected_http_code=202):
        updated_def = self.first_user_def
        update_attribites = {'password': 'password2',
                             'bucket_ramsize': 512,
                             'bucket_replica': 1,
                             'enable_index_replica': 1,
                             'bucket_eviction_policy': 'fullEviction',
                             'bucket_priority': 'high'}
        self.assert_user_attribute_update(
            self.instance_info.id, updated_def,
            update_attribites, expected_http_code)

    def run_user_update_with_existing_name(
            self, expected_exception=exceptions.BadRequest,
            expected_http_code=400):
        raise SkipTest("Couchbase users cannot be renamed.")

    def run_user_access_show(self):
        raise SkipTest("Operation is currently not supported.")

    def run_user_access_revoke(self):
        raise SkipTest("Operation is currently not supported.")

    def run_user_access_grant(self):
        raise SkipTest("Operation is currently not supported.")

    def run_user_recreate_with_no_access(self):
        raise SkipTest("Couchbase users cannot be renamed.")

    def get_system_users(self):
        # Couchbase does not define 'ignore_users' property.
        return []

    @property
    def ignored_user_attributes(self):
        return ['password', 'used_ram', 'bucket_priority',
                'enable_index_replica', 'bucket_eviction_policy']


class Couchbase_4UserActionsRunner(CouchbaseUserActionsRunner):
    pass

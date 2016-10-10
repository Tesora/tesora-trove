#    Copyright 2012 OpenStack Foundation
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

from trove.common import exception
from trove.guestagent.db import models as guest_models
from urllib import unquote


def populate_validated_databases(dbs):
    """
    Create a serializable request with user provided data
    for creating new databases.
    """
    try:
        databases = []
        unique_identities = set()
        for database in dbs:
            mydb = guest_models.ValidatedMySQLDatabase()
            mydb.name = database.get('name', '')
            if mydb.name in unique_identities:
                raise exception.DatabaseInitialDatabaseDuplicateError()
            unique_identities.add(mydb.name)
            mydb.character_set = database.get('character_set', '')
            mydb.collate = database.get('collate', '')
            databases.append(mydb.serialize())
        return databases
    except ValueError as ve:
        # str(ve) contains user input and may include '%' which can cause a
        # format str vulnerability. Escape the '%' to avoid this. This is
        # okay to do since we're not using dict args here in any case.
        safe_string = str(ve).replace('%', '%%')
        raise exception.BadRequest(safe_string)


def parse_users(users):
    user_models = []
    for user in users:
        user_model = guest_models.MySQLUser()
        user_model.name = user.get('name', '')
        user_model.host = user.get('host', '%')
        user_model.password = user.get('password', '')
        user_dbs = user.get('databases', '')
        user_db_names = [user_db.get('name', '') for user_db in user_dbs]
        for user_db_name in user_db_names:
            user_model.databases = user_db_name
        user_models.append(user_model)
        user_model.roles = user.get('roles', [])

    return user_models


def get_user_identity(user_model):
    return '%s@%s' % (user_model.name, user_model.host)


def populate_users(users):
    """Create a serializable request containing users."""
    user_models = parse_users(users)
    unique_identities = set()
    for model in user_models:
        user_identity = get_user_identity(model)
        if user_identity in unique_identities:
            raise exception.DatabaseInitialUserDuplicateError()
        unique_identities.add(user_identity)

    return [model.serialize() for model in user_models]


def unquote_user_host(user_hostname):
    unquoted = unquote(user_hostname)
    if '@' not in unquoted:
        return unquoted, '%'
    if unquoted.endswith('@'):
        return unquoted, '%'
    splitup = unquoted.split('@')
    host = splitup[-1]
    user = '@'.join(splitup[:-1])
    return user, host

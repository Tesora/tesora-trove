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

import re

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class OracleSql(object):
    """The base class of Oracle SQL statement classes.
       Child classes must implement the statement property.
    """
    def __repr__(self):
        return str(self)

    def __str__(self):
        LOG.debug('SQL statement: ' + self._cleansed_statement)
        return self.statement

    @property
    def statement(self):
        return ""

    @property
    def _cleansed_statement(self):
        return re.sub(r'IDENTIFIED BY ".*"',
                      'IDENTIFIED BY "***"',
                      self.statement)


class Query(OracleSql):

    def __init__(self, columns=None, tables=None, where=None, order=None,
                 group=None, limit=None):
        self.columns = columns or []
        self.tables = tables or []
        self.where = where or []
        self.order = order or []
        self.group = group or []
        self.limit = limit

    @property
    def _columns(self):
        if not self.columns:
            return "SELECT *"
        return "SELECT %s" % (", ".join(self.columns))

    @property
    def _tables(self):
        if not self.tables:
            raise ValueError('SQL query requires a table.')
        return "FROM %s" % (", ".join(self.tables))

    @property
    def _where(self):
        if not self.where:
            return ""
        return "WHERE %s" % (" AND ".join(self.where))

    @property
    def _order(self):
        if not self.order:
            return ""
        return "ORDER BY %s" % (", ".join(self.order))

    @property
    def _group_by(self):
        if not self.group:
            return ""
        return "GROUP BY %s" % (", ".join(self.group))

    @property
    def _limit(self):
        if not self.limit:
            return ""
        return "LIMIT %s" % str(self.limit)

    @property
    def statement(self):
        query = [
            self._columns,
            self._tables,
            self._where,
            self._order,
            self._group_by,
            self._limit,
        ]
        query = [q for q in query if q]
        return " ".join(query)


class CreateUser(OracleSql):

    def __init__(self, user, password):
        self.user = user
        self.password = password

    @property
    def log_statement(self):
        return 'CREATE USER %s IDENTIFIED BY "***"' % (self.user,)

    @property
    def statement(self):
        return 'CREATE USER %s IDENTIFIED BY "%s"' % (self.user, self.password)


class DropUser(OracleSql):

    def __init__(self, user, cascade=False):
        self.user = user
        self.cascade = cascade

    @property
    def statement(self):
        q = "DROP USER %s" % self.user
        if self.cascade:
            q += " CASCADE"
        return q


class AlterUser(OracleSql):

    def __init__(self, user, clause):
        self.user = user
        self.clause = clause
        self._log = "ALTER USER %s %s" % (self.user, self.clause)

    @classmethod
    def change_password(cls, user, password):
        q = cls(user, 'IDENTIFIED BY "%s"' % password)
        q._log = q._log.replace(password, '***')
        return q

    @property
    def log_statement(self):
        return self._log

    @property
    def statement(self):
        return "ALTER USER %s %s" % (self.user, self.clause)


class CreateRole(OracleSql):

    def __init__(self, role):
        self.role = role

    @property
    def statement(self):
        return "CREATE ROLE %s" % self.role


class Grant(OracleSql):

    def __init__(self, user, privileges):
        self.user = user
        self.privileges = privileges

    @property
    def statement(self):
        if type(self.privileges) is list:
            privileges_str = ", ".join(self.privileges)
        else:
            privileges_str = self.privileges
        return "GRANT %s TO %s" % (privileges_str, self.user)


class AlterSystem(OracleSql):

    def __init__(self, clause):
        self.clause = None

    @classmethod
    def set_parameter(cls, k, v, deferred=False):
        scope = 'SPFILE' if deferred else 'BOTH'
        q = cls('SET %s = %s SCOPE = %s' % (k, v, scope))
        return q

    @property
    def statement(self):
        return "ALTER SYSTEM " + self.clause


class AlterDatabase(OracleSql):

    def __init__(self, clause):
        self.clause = clause

    @property
    def statement(self):
        return "ALTER DATABASE %s" % self.clause


class CreatePFile(OracleSql):
    _SOURCE_TYPE = 'SPFILE'
    _TARGET_TYPE = 'PFILE'

    def __init__(self, source=None, target=None):
        self.source = source
        self.target = target

    @property
    def statement(self):
        return ("CREATE %s%s FROM %s%s"
                % (self._TARGET_TYPE,
                   ("='%s'" % self.target if self.target else ''),
                   self._SOURCE_TYPE,
                   ("='%s'" % self.source if self.source else '')))


class CreateSPFile(CreatePFile):
    _SOURCE_TYPE = 'PFILE'
    _TARGET_TYPE = 'SPFILE'


class CreatePDB(OracleSql):

    def __init__(self, pdb, user, password):
        self.pdb = pdb
        self.user = user
        self.password = password

    @property
    def log_statement(self):
        return ('CREATE PLUGGABLE DATABASE %s '
                'ADMIN USER %s '
                'IDENTIFIED BY "***"'
                % (self.pdb, self.user,))

    @property
    def statement(self):
        return ('CREATE PLUGGABLE DATABASE %s '
                'ADMIN USER %s '
                'IDENTIFIED BY "%s"'
                % (self.pdb, self.user, self.password))


class DropPDB(OracleSql):

    def __init__(self, pdb):
        self.pdb = pdb

    @property
    def statement(self):
        return 'DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES' % self.pdb


class AlterPDB(OracleSql):

    def __init__(self, pdb, clause):
        self.pdb = pdb
        self.clause = clause

    @property
    def statement(self):
        return 'ALTER PLUGGABLE DATABASE %s %s' % (self.pdb, self.clause)

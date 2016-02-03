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
"""

Intermediary class for building SQL queries for use by the guest agent.
Do not hard-code strings into the guest agent; use this module to build
them for you.

"""


class Query(object):

    def __init__(self, columns=None, tables=None, where=None, order=None,
                 group=None, limit=None):
        self.columns = columns or []
        self.tables = tables or []
        self.where = where or []
        self.order = order or []
        self.group = group or []
        self.limit = limit

    def __repr__(self):
        return str(self)

    @property
    def _columns(self):
        if not self.columns:
            return "SELECT *"
        return "SELECT %s" % (", ".join(self.columns))

    @property
    def _tables(self):
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

    def __str__(self):
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


class CreateUser(object):

    def __init__(self, user, password=None):
        self.user = user
        self.password = password

    def __repr__(self):
        return str(self)

    @property
    def _identity(self):
        if self.password:
            return 'IDENTIFIED BY "%s"' % self.password
        return ""

    def __str__(self):
        query = ['CREATE USER %s' % self.user,
                 self._identity,
                 ]
        query = [q for q in query if q]
        return " ".join(query)

class DropUser(object):

    def __init__(self, user, cascade=False):
        self.user = user
        self.cascade = cascade

    def __repr__(self):
        return str(self)

    def __str__(self):
        q = "DROP USER %s" % self.user
        if self.cascade:
            q += " CASCADE"
        return q

class CreateRole(object):

    def __init__(self, role):
        self.role = role

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "CREATE ROLE %s" % self.role

class AlterUser(object):

    def __init__(self, user, password=None):
        self.user = user
        self.password = password

    def __repr__(self):
        return str(self)

    def __str__(self):
        q = "ALTER USER %s" % self.user
        if self.password:
            q += " IDENTIFIED BY %s" % self.password
        return q


class AlterSystem(object):

    def __init__(self):
        self.query = None

    @classmethod
    def set_parameter(cls, k, v):
        q = AlterSystem()
        q.query = "SET %s = %s" % (k, v)
        return q

    def __str__(self):
        return "ALTER SYSTEM " + self.query

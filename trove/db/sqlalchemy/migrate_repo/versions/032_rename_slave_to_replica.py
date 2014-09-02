# Copyright Tesora, Inc. 2014
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

from migrate.changeset.constraint import ForeignKeyConstraint
from sqlalchemy.schema import MetaData

from trove.db.sqlalchemy.migrate_repo.schema import Table


def upgrade(migrate_engine):
    _rename_column(migrate_engine, 'slave_of_id', 'replica_of_id')


def downgrade(migrate_engine):
    _rename_column(migrate_engine, 'replica_of_id', 'slave_of_id')


def _rename_column(migrate_engine, name, newName):
    meta = MetaData()
    meta.bind = migrate_engine
    instances = Table('instances', meta, autoload=True)
    column = instances.c[name]

    if migrate_engine.name == 'sqlite':
        # SQLite allows the column to be renamed directly and does not
        # support 'alter table drop constraint'
        column.alter(name=newName)
        return

    # find and drop the foreign key constraint
    for constraint in instances.constraints:
        try:
            if name == constraint.columns[0]:
                ForeignKeyConstraint([column], [instances.c.id],
                                     name=constraint.name).drop()
                break
        except KeyError:
            pass   # not an fk constraint

    column.alter(name=newName)

    # execute SQL directly to add back the fk constraint, because calling
    # instances.append_constraint(ForeignKeyConstraint(...)) fails.
    # SQLAlchemy is caching column info somewhere and chokes on the new name

    migrate_engine.execute('ALTER TABLE instances ADD '
                           'CONSTRAINT FOREIGN KEY (`%s`) '
                           'REFERENCES `instances` (`id`)' % newName)

# Copyright 2016 Tesora, Inc.
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

from oslo_log import log as logging
from sqlalchemy import ForeignKey
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData

from trove.common import cfg
from trove.db.sqlalchemy.migrate_repo.schema import String
from trove.db.sqlalchemy.migrate_repo.schema import Table
from trove.db.sqlalchemy import utils as db_utils


CONF = cfg.CONF
logger = logging.getLogger('trove.db.sqlalchemy.migrate_repo.schema')

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    # Load 'configurations' table to MetaData.
    Table('configurations', meta, autoload=True, autoload_with=migrate_engine)
    instances = Table('clusters', meta, autoload=True)
    instances.create_column(Column('configuration_id', String(36),
                                   ForeignKey("configurations.id")))


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    clusters = Table('clusters', meta, autoload=True)
    configurations = Table('configurations', meta, autoload=True,
                           autoload_with=migrate_engine)

    constraint_names = db_utils.get_foreign_key_constraint_names(
        engine=migrate_engine,
        table='clusters',
        columns=['configuration_id'],
        ref_table='configurations',
        ref_columns=['id'])
    db_utils.drop_foreign_key_constraints(
        constraint_names=constraint_names,
        columns=[clusters.c.configuration_id],
        ref_columns=[configurations.c.id])

    clusters.drop_column('configuration_id')

# Copyright 2018 SAP SE
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from migrate.changeset.constraint import UniqueConstraint
from sqlalchemy import MetaData, Table
from sqlalchemy import select, func
from oslo_log import log as logging

LOG = logging.getLogger(__name__)

def quota_usages_table(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    return Table('quota_usages', meta, autoload=True)


def _build_constraint(migrate_engine, quota_usages=None):
    quota_usages = quota_usages or quota_usages_table(migrate_engine)
    return UniqueConstraint(
        'project_id', 'resource', 'deleted',
        table=quota_usages,
    )


def upgrade(migrate_engine):
    quota_usages = quota_usages_table(migrate_engine)
    uniqueness = [quota_usages.c.project_id,
         quota_usages.c.resource,
         quota_usages.c.deleted
         ]

    # Get all quota_usages ranked by their id
    rankquery = select([quota_usages.c.id, func.dense_rank().over(
        partition_by=uniqueness, order_by=quota_usages.c.id
    ).label('rank')]).alias('qu')

    # Filter out the first ones (leaving the duplicates)
    filterquery = select([rankquery.c.id]).where(rankquery.c.rank > 1)

    # Delete all later duplicates
    query = quota_usages.delete().where(quota_usages.c.id.in_(filterquery))
    migrate_engine.execute(query)
    cons = _build_constraint(migrate_engine, quota_usages=quota_usages)
    cons.create()


def downgrade(migrate_engine):
    cons = _build_constraint(migrate_engine)
    cons.drop()

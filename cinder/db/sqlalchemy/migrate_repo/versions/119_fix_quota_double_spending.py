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


def _build_constraint(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    table = Table('quota_usages', meta, autoload=True)
    return UniqueConstraint(
        'project_id', 'resource', 'deleted',
        table=table,
    )


def upgrade(migrate_engine):
    cons = _build_constraint(migrate_engine)
    cons.create()


def downgrade(migrate_engine):
    cons = _build_constraint(migrate_engine)
    cons.drop()

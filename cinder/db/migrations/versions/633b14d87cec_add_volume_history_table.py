# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Add volume_history table

Revision ID: 633b14d87cec
Revises: daa98075b90d
Create Date: 2026-03-11
"""

from alembic import op
import sqlalchemy as sa

revision = '633b14d87cec'
down_revision = 'daa98075b90d'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'volume_history',
        sa.Column('created_at', sa.DateTime),
        sa.Column('updated_at', sa.DateTime),
        sa.Column('deleted_at', sa.DateTime),
        sa.Column('deleted', sa.Boolean, default=False),
        sa.Column('id', sa.String(36), primary_key=True, nullable=False),
        sa.Column('volume_id', sa.String(36), sa.ForeignKey('volumes.id'),
                  nullable=False),
        sa.Column('project_id', sa.String(255)),
        sa.Column('user_id', sa.String(255)),
        sa.Column('request_id', sa.String(255)),
        sa.Column('action', sa.String(64), nullable=False),
        sa.Column('changes', sa.Text),
        mysql_engine='InnoDB',
        mysql_charset='utf8',
    )
    op.create_index('volume_history_volume_id_idx', 'volume_history',
                    ['volume_id'])

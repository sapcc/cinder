# Copyright (C) 2015 OpenStack Foundation
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

"""Tests for db purge."""

import datetime
import uuid

from oslo_utils import timeutils

from cinder import context
from cinder import db
from cinder.db.sqlalchemy import api as db_api
from cinder import exception
from cinder import test

from oslo_db.sqlalchemy import utils as sqlalchemyutils


class PurgeDeletedTest(test.TestCase):

    def setUp(self):
        super(PurgeDeletedTest, self).setUp()
        self.context = context.get_admin_context()
        self.engine = db_api.get_engine()
        self.session = db_api.get_session()
        self.conn = self.engine.connect()
        self.volumes = sqlalchemyutils.get_table(
            self.engine, "volumes")
        # The volume_metadata table has a FK of volume_id
        self.vm = sqlalchemyutils.get_table(
            self.engine, "volume_metadata")

        self.vol_types = sqlalchemyutils.get_table(
            self.engine, "volume_types")
        # The volume_type_projects table has a FK of volume_type_id
        self.vol_type_proj = sqlalchemyutils.get_table(
            self.engine, "volume_type_projects")

        self.snapshots = sqlalchemyutils.get_table(
            self.engine, "snapshots")

        self.sm = sqlalchemyutils.get_table(
            self.engine, "snapshot_metadata")

        self.vgm = sqlalchemyutils.get_table(
            self.engine, "volume_glance_metadata")

        self.uuidstrs = []
        for unused in range(6):
            self.uuidstrs.append(uuid.uuid4().hex)
        # Add 6 rows to table
        for uuidstr in self.uuidstrs:
            ins_stmt = self.volumes.insert().values(id=uuidstr)
            self.conn.execute(ins_stmt)
            ins_stmt = self.vm.insert().values(volume_id=uuidstr)
            self.conn.execute(ins_stmt)
            ins_stmt = self.vgm.insert().values(
                volume_id=uuidstr, key='image_name', value='test')
            self.conn.execute(ins_stmt)

            ins_stmt = self.vol_types.insert().values(id=uuidstr)
            self.conn.execute(ins_stmt)
            ins_stmt = self.vol_type_proj.insert().\
                values(volume_type_id=uuidstr)
            self.conn.execute(ins_stmt)

            ins_stmt = self.snapshots.insert().values(
                id=uuidstr, volume_id=uuidstr)
            self.conn.execute(ins_stmt)
            ins_stmt = self.sm.insert().values(snapshot_id=uuidstr)
            self.conn.execute(ins_stmt)

            ins_stmt = self.vgm.insert().values(
                snapshot_id=uuidstr, key='image_name', value='test')
            self.conn.execute(ins_stmt)

        # Set 4 of them deleted, 2 are 60 days ago, 2 are 20 days ago
        old = timeutils.utcnow() - datetime.timedelta(days=20)
        older = timeutils.utcnow() - datetime.timedelta(days=60)

        make_vol_old = self.volumes.update().\
            where(self.volumes.c.id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_vol_older = self.volumes.update().\
            where(self.volumes.c.id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)
        make_vol_meta_old = self.vm.update().\
            where(self.vm.c.volume_id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_vol_meta_older = self.vm.update().\
            where(self.vm.c.volume_id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)

        make_vol_types_old = self.vol_types.update().\
            where(self.vol_types.c.id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_vol_types_older = self.vol_types.update().\
            where(self.vol_types.c.id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)
        make_vol_type_proj_old = self.vol_type_proj.update().\
            where(self.vol_type_proj.c.volume_type_id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_vol_type_proj_older = self.vol_type_proj.update().\
            where(self.vol_type_proj.c.volume_type_id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)

        make_snap_old = self.snapshots.update().\
            where(self.snapshots.c.id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_snap_older = self.snapshots.update().\
            where(self.snapshots.c.id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)
        make_snap_meta_old = self.sm.update().\
            where(self.sm.c.snapshot_id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_snap_meta_older = self.sm.update().\
            where(self.sm.c.snapshot_id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)

        make_vol_glance_meta_old = self.vgm.update().\
            where(self.vgm.c.volume_id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_vol_glance_meta_older = self.vgm.update().\
            where(self.vgm.c.volume_id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)
        make_snap_glance_meta_old = self.vgm.update().\
            where(self.vgm.c.snapshot_id.in_(self.uuidstrs[1:3]))\
            .values(deleted_at=old)
        make_snap_glance_meta_older = self.vgm.update().\
            where(self.vgm.c.snapshot_id.in_(self.uuidstrs[4:6]))\
            .values(deleted_at=older)

        self.conn.execute(make_vol_old)
        self.conn.execute(make_vol_older)
        self.conn.execute(make_vol_meta_old)
        self.conn.execute(make_vol_meta_older)

        self.conn.execute(make_vol_types_old)
        self.conn.execute(make_vol_types_older)
        self.conn.execute(make_vol_type_proj_old)
        self.conn.execute(make_vol_type_proj_older)

        self.conn.execute(make_snap_old)
        self.conn.execute(make_snap_older)
        self.conn.execute(make_snap_meta_old)
        self.conn.execute(make_snap_meta_older)

        self.conn.execute(make_vol_glance_meta_old)
        self.conn.execute(make_vol_glance_meta_older)
        self.conn.execute(make_snap_glance_meta_old)
        self.conn.execute(make_snap_glance_meta_older)

    def test_purge_deleted_rows_old(self):
        dialect = self.engine.url.get_dialect()
        if dialect == sqlite.dialect:
            # We're seeing issues with foreign key support in SQLite 3.6.20
            # SQLAlchemy doesn't support it at all with < SQLite 3.6.19
            # It works fine in SQLite 3.7.
            # Force foreign_key checking if running SQLite >= 3.7
            import sqlite3
            tup = sqlite3.sqlite_version_info
            if tup[0] > 3 or (tup[0] == 3 and tup[1] >= 7):
                self.conn.execute("PRAGMA foreign_keys = ON")
        # Purge at 30 days old, should only delete 2 rows
        db.purge_deleted_rows(self.context, age_in_days=30)

        vol_rows = self.session.query(self.volumes).count()
        vol_meta_rows = self.session.query(self.vm).count()
        vol_type_rows = self.session.query(self.vol_types).count()
        vol_type_proj_rows = self.session.query(self.vol_type_proj).count()
        snap_rows = self.session.query(self.snapshots).count()
        snap_meta_rows = self.session.query(self.sm).count()
        vol_glance_meta_rows = self.session.query(self.vgm).count()

        # Verify that we only deleted 2
        self.assertEqual(4, vol_rows)
        self.assertEqual(4, vol_meta_rows)
        self.assertEqual(4, vol_type_rows)
        self.assertEqual(4, vol_type_proj_rows)
        self.assertEqual(4, snap_rows)
        self.assertEqual(4, snap_meta_rows)
        self.assertEqual(8, vol_glance_meta_rows)

    def test_purge_deleted_rows_older(self):
        dialect = self.engine.url.get_dialect()
        if dialect == sqlite.dialect:
            # We're seeing issues with foreign key support in SQLite 3.6.20
            # SQLAlchemy doesn't support it at all with < SQLite 3.6.19
            # It works fine in SQLite 3.7.
            # Force foreign_key checking if running SQLite >= 3.7
            import sqlite3
            tup = sqlite3.sqlite_version_info
            if tup[0] > 3 or (tup[0] == 3 and tup[1] >= 7):
                self.conn.execute("PRAGMA foreign_keys = ON")
        # Purge at 10 days old now, should delete 2 more rows
        db.purge_deleted_rows(self.context, age_in_days=10)

        vol_rows = self.session.query(self.volumes).count()
        vol_meta_rows = self.session.query(self.vm).count()
        vol_type_rows = self.session.query(self.vol_types).count()
        vol_type_proj_rows = self.session.query(self.vol_type_proj).count()
        snap_rows = self.session.query(self.snapshots).count()
        snap_meta_rows = self.session.query(self.sm).count()
        vol_glance_meta_rows = self.session.query(self.vgm).count()

        # Verify that we only have 2 rows now
        self.assertEqual(2, vol_rows)
        self.assertEqual(2, vol_meta_rows)
        self.assertEqual(2, vol_type_rows)
        self.assertEqual(2, vol_type_proj_rows)
        self.assertEqual(2, snap_rows)
        self.assertEqual(2, snap_meta_rows)
        self.assertEqual(4, vol_glance_meta_rows)

    def test_purge_deleted_rows_bad_args(self):
        # Test with no age argument
        self.assertRaises(TypeError, db.purge_deleted_rows, self.context)
        # Test purge with non-integer
        self.assertRaises(exception.InvalidParameterValue,
                          db.purge_deleted_rows, self.context,
                          age_in_days='ten')
        # Test with negative value
        self.assertRaises(exception.InvalidParameterValue,
                          db.purge_deleted_rows, self.context,
                          age_in_days=-1)

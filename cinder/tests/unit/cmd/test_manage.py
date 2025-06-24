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

from io import StringIO
from pathlib import Path
import random
from unittest import mock
import uuid

from oslo_utils import timeutils
from sqlalchemy import insert

from cinder.cmd.manage import SapCommands
from cinder import context
from cinder import coordination
from cinder.db.sqlalchemy import api as db_api
from cinder.db.sqlalchemy import models
from cinder.tests.unit import test


class SapCommandsTests(test.TestCase):
    def setUp(self):
        super(SapCommandsTests, self).setUp()
        mock_coordinator = mock.Mock()
        # random directory such that directory operations do not fail
        mock_coordinator._dir = "/tmp"
        coordination.COORDINATOR.coordinator = mock_coordinator
        self.ctxt = context.get_admin_context()

    def test_parse_fix_flags(self):
        sap_commands = SapCommands()
        all_valid_options = 'orphan_attachments,error_deleting_volumes,' \
            'error_deleting_snapshots,metadata_volumes,' \
            'admin_metadata_volumes,glance_metadata_volumes,' \
            'glance_metadata_snapshots,attachment_deleted_volume,' \
            'group_volume_types,deleted_at_timestamps,undelete_services'
        result = sap_commands._parse_fix_flags(all_valid_options)
        self.assertTrue(all(result.values()))
        result = sap_commands._parse_fix_flags("")
        self.assertTrue(not any(result.values()))
        self.assertCountEqual(all_valid_options.split(','), result.keys())
        self.assertRaises(ValueError, sap_commands._parse_fix_flags,
                          "undelete_services,invalid_option")

    def test_clean_old_lock_files(self):
        sap_commands = SapCommands()
        fnames = [
            "cinder-nfs-4291c6b3-dd2a-4e9b-ad76-7572a2ce0971",
            "cinder-4291c6b3-dd2a-4e9b-ad76-7572a2ce0971",
            "cinder-attachment_update-4291c6b3-dd2a-4e9b-ad76-7572a2ce0971-"
            "no-match.txt"
        ]
        mock_files = []
        for fname in fnames:
            mock_file1 = mock.MagicMock(spec=Path)
            mock_file1.is_file.return_value = True
            mock_file1.name = fname
            mock_files.append(mock_file1)
        with mock.patch('sys.stdout', new_callable=StringIO) as mock_stdout,\
                mock.patch("pathlib.Path.iterdir", return_value=mock_files):
            sap_commands.clean_old_lock_files(dry_run=False, verbose=True,
                                              batch_size=10000)
            print_output = mock_stdout.getvalue()
        self.assertIn("Found 3 lock files", print_output)

    def test_mark_deleted_by_ids_volume(self):
        # Create a couple of volumes - some should be deleted, others are
        # already deleted
        n_volumes = 10
        mark_as_deleted_ids, ids = [], []
        already_deleted_idx = [0, 1, 2]
        should_be_deleted_idx = [3, 4, 5]
        with db_api.main_context_manager.writer.using(self.ctxt) as session:
            for i in range(n_volumes):
                id = str(uuid.uuid4())
                volume_type_id = str(uuid.uuid4())
                ids.append(id)
                if i in should_be_deleted_idx:
                    mark_as_deleted_ids.append(id)
                deleted = 1 if i in already_deleted_idx else 0
                session.execute(insert(models.Volume).values(
                    id=id, status="available", volume_type_id=volume_type_id,
                    deleted=deleted))
            # Check that the volumes were created correctly
            count_total_volumes = session.query(models.Volume).count()
            self.assertEqual(n_volumes, count_total_volumes)
            count_already_deleted_volumes = session.query(models.Volume).\
                filter_by(deleted=1).count()
            self.assertEqual(len(already_deleted_idx),
                             count_already_deleted_volumes)
            # Check if date has been updated properly for to-be-deleted volumes
            sap_commands = SapCommands()
            now = timeutils.utcnow()
            sap_commands._set_deleted_by_ids(self.ctxt,
                                             models.Volume,
                                             mark_as_deleted_ids,
                                             now = now)
            count_by_date = session.query(models.Volume).\
                filter_by(updated_at=now, deleted_at=now).\
                count()
            self.assertEqual(len(should_be_deleted_idx), count_by_date)
            # check total deleted flag count
            count_deleted_volumes = session.query(models.Volume).\
                filter_by(deleted=1).count()
            self.assertEqual(len(already_deleted_idx) +
                             len(should_be_deleted_idx),
                             count_deleted_volumes)
            # Nothing should be changed if id list is empty
            sap_commands._set_deleted_by_ids(self.ctxt,
                                             models.Volume,
                                             [],
                                             now = now)
            count_deleted_volumes = session.query(models.Volume)\
                .filter_by(deleted=1).count()
            self.assertEqual(len(already_deleted_idx) +
                             len(should_be_deleted_idx),
                             count_deleted_volumes)

    def test_mark_deleted_by_ids_volume_metadata(self):
        # create some volume_metadata, some of them should be flagged as
        # deleted, some of them should not because they are already deleted
        # almost identical to test_mark_deleted_by_ids_volume but with
        # different model
        n_volumes = 10
        mark_as_deleted_ids, ids = [], []
        already_deleted_idx = [0, 1, 2]
        should_be_deleted_idx = [3, 4, 5]
        with db_api.main_context_manager.writer.using(self.ctxt) as session:
            for i in range(n_volumes):
                id = random.randint(1, 100000)
                volume_id = str(uuid.uuid4())
                ids.append(id)
                if i in should_be_deleted_idx:
                    mark_as_deleted_ids.append(id)
                deleted = 1 if i in already_deleted_idx else 0
                session.execute(insert(models.VolumeMetadata).values(
                    id=id, volume_id=volume_id, deleted=deleted))
            count_total_volumes = session.query(models.VolumeMetadata).count()
            self.assertEqual(n_volumes, count_total_volumes)
            count_already_deleted_volumes = session.query(
                models.VolumeMetadata)\
                .filter_by(deleted=1).count()
            self.assertEqual(len(already_deleted_idx),
                             count_already_deleted_volumes)
            sap_commands = SapCommands()
            now = timeutils.utcnow()
            sap_commands._set_deleted_by_ids(self.ctxt,
                                             models.VolumeMetadata,
                                             mark_as_deleted_ids,
                                             now = now)
            count_by_date = session.query(models.VolumeMetadata)\
                .filter_by(updated_at=now, deleted_at=now)\
                .count()
            self.assertEqual(len(should_be_deleted_idx), count_by_date)
            count_deleted_volumes = session.query(models.VolumeMetadata)\
                .filter_by(deleted=1).count()
            self.assertEqual(len(already_deleted_idx) +
                             len(should_be_deleted_idx),
                             count_deleted_volumes)

    def test_get_admin_metadata(self):
        n_volumes = 10
        metadata_ids_expected = []
        volume_ids_joined_index = [1, 2, 3, 4, 5, 6]
        with db_api.main_context_manager.writer.using(self.ctxt) as session:
            for metadata_id in range(n_volumes):
                volume_id = str(uuid.uuid4())
                # mark every second volume as deleted
                deleted = 0 if metadata_id % 2 == 0 else 1
                # keep track of metadata ids that can be joined to deleted
                # volumes for test assertion
                if deleted == 1 and metadata_id in volume_ids_joined_index:
                    metadata_ids_expected.append(metadata_id)
                session.execute(insert(models.Volume).values(
                    id=volume_id, volume_type_id="fake_volume_type_id",
                    deleted=deleted))
                if metadata_id in volume_ids_joined_index:
                    volume_id_reference = volume_id
                else:
                    volume_id_reference = str(uuid.uuid4())
                # mark no volume_admin_metadata as deleted
                session.execute(insert(models.VolumeAdminMetadata).values(
                    id=metadata_id, volume_id=volume_id_reference, deleted=0))
        sap_commands = SapCommands()
        ids_result = sap_commands._get_admin_metadata_ids_of_deleted_volumes(
            self.ctxt)
        self.assertSetEqual(set(ids_result), set(metadata_ids_expected))

# Copyright 2024 SAP SE
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
"""Tests for Phased Retype Contract (Ticket #01)."""

from unittest import mock

from cinder import exception
from cinder import objects
from cinder.tests.unit import fake_constants as fake
from cinder.tests.unit import fake_volume
from cinder.tests.unit import volume as base
from cinder.volume import manager as vol_manager


class PhasedRetypeManagerTestCase(base.BaseVolumeTestCase):
    """Tests for phased retype manager methods."""

    def setUp(self):
        super(PhasedRetypeManagerTestCase, self).setUp()
        self.manager = vol_manager.VolumeManager()

    def _create_volume(self, status='available', migration_status=None,
                       **kwargs):
        vol = fake_volume.fake_volume_obj(
            self.context,
            status=status,
            migration_status=migration_status,
            **kwargs)
        vol.save = mock.Mock()
        vol.conditional_update = mock.Mock(return_value=True)
        vol.obj_as_admin = mock.MagicMock()
        vol.update = mock.Mock()
        return vol

    # --- prepare_retype tests ---

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_prepare_retype_success(self, mock_req_init):
        """Test successful prepare_retype persists model_update and state."""
        volume = self._create_volume()
        new_type = mock.MagicMock()
        new_type.id = fake.VOLUME_TYPE_ID

        with mock.patch.object(
                objects.VolumeType, 'get_by_id', return_value=new_type):
            self.manager.driver.prepare_retype = mock.Mock(
                return_value=(True, {'metadata_key': 'value'}))

            result = self.manager.prepare_retype(
                self.context, volume, fake.VOLUME_TYPE_ID,
                {'host': 'dest@backend#pool'})

        # Outcome: caller receives the driver's model_update
        self.assertEqual({'metadata_key': 'value'}, result)
        # Outcome: model_update was applied to the volume
        volume.update.assert_called_once_with({'metadata_key': 'value'})
        # Outcome: volume is in 'prepared' state
        self.assertEqual('prepared', volume.migration_status)

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_prepare_retype_driver_returns_false(self, mock_req_init):
        """Test that unsupported retype raises and sets error state."""
        volume = self._create_volume()
        new_type = mock.MagicMock()

        with mock.patch.object(
                objects.VolumeType, 'get_by_id', return_value=new_type):
            self.manager.driver.prepare_retype = mock.Mock(
                return_value=(False, None))

            self.assertRaises(
                exception.VolumeMigrationFailed,
                self.manager.prepare_retype,
                self.context, volume, fake.VOLUME_TYPE_ID,
                {'host': 'dest@backend#pool'})

        self.assertEqual('error', volume.migration_status)

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_prepare_retype_driver_exception(self, mock_req_init):
        """Test that driver exception propagates and sets error state."""
        volume = self._create_volume()
        new_type = mock.MagicMock()

        with mock.patch.object(
                objects.VolumeType, 'get_by_id', return_value=new_type):
            self.manager.driver.prepare_retype = mock.Mock(
                side_effect=Exception('driver error'))

            self.assertRaises(
                Exception,
                self.manager.prepare_retype,
                self.context, volume, fake.VOLUME_TYPE_ID,
                {'host': 'dest@backend#pool'})

        self.assertEqual('error', volume.migration_status)

    # --- finalize_retype tests ---

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_finalize_retype_updates_volume_fields(self, mock_req_init):
        """Test finalize directly updates volume fields (bypasses
        finish_volume_migration to avoid _name_id mismatch)."""
        volume = self._create_volume(migration_status='prepared')

        self.manager.driver.finalize_retype = mock.Mock(
            return_value={'provider_location': '10.0.0.1:/vol/nfs1'})

        self.manager.finalize_retype(
            self.context, volume, fake.VOLUME_TYPE_ID,
            'dest@backend#pool')

        # Outcome: volume fields are directly updated to new values
        volume.update.assert_called_once()
        update_args = volume.update.call_args[0][0]
        self.assertIsNone(update_args['_name_id'])
        self.assertEqual('dest@backend#pool', update_args['host'])
        self.assertEqual(fake.VOLUME_TYPE_ID, update_args['volume_type_id'])
        # Outcome: driver-returned fields are merged in
        self.assertEqual('10.0.0.1:/vol/nfs1',
                         update_args['provider_location'])
        # Outcome: migration is complete
        self.assertIsNone(volume.migration_status)

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_finalize_retype_idempotent(self, mock_req_init):
        """Test finalize is safe to re-call on already-finalized volumes."""
        for status in (None, 'success'):
            volume = self._create_volume(migration_status=status)
            self.manager.driver.finalize_retype = mock.Mock()

            self.manager.finalize_retype(
                self.context, volume, fake.VOLUME_TYPE_ID,
                'dest@backend#pool')

            # Outcome: no driver call, no state changes
            self.manager.driver.finalize_retype.assert_not_called()
            volume.update.assert_not_called()

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_finalize_retype_driver_exception(self, mock_req_init):
        """Test that driver failure sets error state and propagates."""
        volume = self._create_volume(migration_status='prepared')

        self.manager.driver.finalize_retype = mock.Mock(
            side_effect=Exception('rename failed'))

        self.assertRaises(
            Exception,
            self.manager.finalize_retype,
            self.context, volume, fake.VOLUME_TYPE_ID,
            'dest@backend#pool')

        self.assertEqual('error', volume.migration_status)

    # --- abort_retype tests ---

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_abort_retype_success(self, mock_req_init):
        """Test abort calls driver and clears migration_status."""
        volume = self._create_volume(migration_status='prepared')

        self.manager.driver.abort_retype = mock.Mock(return_value=None)

        self.manager.abort_retype(self.context, volume)

        self.manager.driver.abort_retype.assert_called_once_with(
            self.context, volume)
        # Outcome: volume is unblocked
        self.assertIsNone(volume.migration_status)

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_abort_retype_idempotent(self, mock_req_init):
        """Test abort is safe to re-call on non-migrating volumes."""
        for status in (None, 'success'):
            volume = self._create_volume(migration_status=status)
            self.manager.driver.abort_retype = mock.Mock()

            self.manager.abort_retype(self.context, volume)

            # Outcome: no driver call, no side effects
            self.manager.driver.abort_retype.assert_not_called()

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_abort_retype_driver_exception(self, mock_req_init):
        """Test that driver failure sets error state and propagates."""
        volume = self._create_volume(migration_status='prepared')

        self.manager.driver.abort_retype = mock.Mock(
            side_effect=Exception('abort failed'))

        self.assertRaises(
            Exception,
            self.manager.abort_retype,
            self.context, volume)

        self.assertEqual('error', volume.migration_status)

    # --- refresh_connection tests ---

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_refresh_connection_updates_attachment(self, mock_req_init):
        """Test that new connection_info is persisted to attachment."""
        volume = self._create_volume(status='in-use')
        connector = {'initiator': 'iqn.test', 'host': 'compute1'}
        expected_conn_info = {
            'driver_volume_type': 'nfs',
            'data': {'export': '10.0.0.1:/vol/nfs1',
                     'name': 'volume-%s' % volume.id}
        }

        attachment = mock.MagicMock()
        attachment.connector = connector
        attachment.save = mock.Mock()

        with mock.patch.object(
                objects.VolumeAttachment, 'get_by_id',
                return_value=attachment):
            self.manager.driver.initialize_connection = mock.Mock(
                return_value=expected_conn_info)

            result = self.manager.refresh_connection(
                self.context, volume, fake.ATTACHMENT_ID)

        # Outcome: caller receives new connection_info
        self.assertEqual(expected_conn_info, result)
        # Outcome: attachment record is updated
        self.assertEqual(expected_conn_info, attachment.connection_info)
        attachment.save.assert_called_once()

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_refresh_connection_rejects_missing_connector(self, mock_req_init):
        """Test that missing connector is a clear error."""
        volume = self._create_volume(status='in-use')

        attachment = mock.MagicMock()
        attachment.connector = None

        with mock.patch.object(
                objects.VolumeAttachment, 'get_by_id',
                return_value=attachment):

            self.assertRaises(
                exception.InvalidInput,
                self.manager.refresh_connection,
                self.context, volume, fake.ATTACHMENT_ID)

    @mock.patch('cinder.volume.volume_utils.require_driver_initialized')
    def test_refresh_connection_no_partial_update_on_failure(self,
                                                            mock_req_init):
        """Test that attachment is NOT corrupted if driver fails."""
        volume = self._create_volume(status='in-use')
        connector = {'initiator': 'iqn.test', 'host': 'compute1'}

        attachment = mock.MagicMock()
        attachment.connector = connector
        attachment.save = mock.Mock()

        with mock.patch.object(
                objects.VolumeAttachment, 'get_by_id',
                return_value=attachment):
            self.manager.driver.initialize_connection = mock.Mock(
                side_effect=Exception('driver connection failed'))

            self.assertRaises(
                Exception,
                self.manager.refresh_connection,
                self.context, volume, fake.ATTACHMENT_ID)

        # Outcome: attachment was NOT modified
        attachment.save.assert_not_called()


class PhasedRetypeApiTestCase(base.BaseVolumeTestCase):
    """Tests for phased retype volume API methods."""

    def _create_volume(self, status='available', migration_status=None,
                       **kwargs):
        vol = fake_volume.fake_volume_obj(
            self.context,
            status=status,
            migration_status=migration_status,
            **kwargs)
        vol.conditional_update = mock.Mock(return_value=True)
        return vol

    @mock.patch('cinder.volume.rpcapi.VolumeAPI.prepare_retype')
    def test_prepare_retype_blocked_when_migrating(self, mock_rpc):
        """Test that a migrating volume cannot start a new retype."""
        volume = self._create_volume(migration_status='migrating')
        volume.conditional_update = mock.Mock(return_value=False)

        self.assertRaises(
            exception.InvalidVolume,
            self.volume_api.prepare_retype,
            self.context, volume, fake.VOLUME_TYPE_ID,
            'dest@backend#pool')

        # Outcome: no RPC dispatched
        mock_rpc.assert_not_called()

    @mock.patch('cinder.volume.rpcapi.VolumeAPI.prepare_retype')
    def test_prepare_retype_dispatches_rpc(self, mock_rpc):
        """Test that prepare_retype dispatches and returns RPC result."""
        volume = self._create_volume(status='available')
        mock_rpc.return_value = {'key': 'value'}

        result = self.volume_api.prepare_retype(
            self.context, volume, fake.VOLUME_TYPE_ID,
            'dest@backend#pool')

        mock_rpc.assert_called_once_with(
            self.context, volume, fake.VOLUME_TYPE_ID, 'dest@backend#pool')
        self.assertEqual({'key': 'value'}, result)

    @mock.patch('cinder.volume.rpcapi.VolumeAPI.prepare_retype')
    def test_prepare_retype_resets_status_on_rpc_failure(self, mock_rpc):
        """Test that volume is not stuck in 'starting' if RPC fails."""
        volume = self._create_volume(status='available')
        mock_rpc.side_effect = Exception('RPC timeout')

        self.assertRaises(
            Exception,
            self.volume_api.prepare_retype,
            self.context, volume, fake.VOLUME_TYPE_ID,
            'dest@backend#pool')

        # Outcome: cleanup resets migration_status atomically
        cleanup_call = volume.conditional_update.call_args_list[1]
        self.assertEqual({'migration_status': None}, cleanup_call[0][0])
        self.assertEqual({'migration_status': 'starting'}, cleanup_call[0][1])


class MigrationStatusBlockingTestCase(base.BaseVolumeTestCase):
    """Tests that _extend and _attachment_reserve include migration_status
    in their conditional_update expected dict."""

    def _create_volume(self, status='available', migration_status=None,
                       multiattach=False, **kwargs):
        vol = fake_volume.fake_volume_obj(
            self.context,
            status=status,
            migration_status=migration_status,
            multiattach=multiattach,
            **kwargs)
        vol.conditional_update = mock.Mock(return_value=False)
        return vol

    @mock.patch('cinder.volume.volume_types.get_volume_type')
    @mock.patch('cinder.volume.volume_types.provision_filter_on_size')
    def test_extend_includes_migration_status_check(self, mock_prov,
                                                    mock_vtype):
        """Test that _extend passes migration_status to conditional_update."""
        volume = self._create_volume(
            status='available', migration_status='preparing',
            size=1, volume_type_id=fake.VOLUME_TYPE_ID)
        mock_vtype.return_value = {'id': fake.VOLUME_TYPE_ID}

        self.assertRaises(
            exception.InvalidVolume,
            self.volume_api._extend,
            self.context, volume, 2)

        # The actual assertion: migration_status is in the expected dict
        call_args = volume.conditional_update.call_args
        expected_dict = call_args[0][1]
        self.assertIn('migration_status', expected_dict)

    def test_attachment_reserve_includes_migration_status_check(self):
        """Test that _attachment_reserve passes migration_status to
        conditional_update."""
        volume = self._create_volume(
            status='available', migration_status='preparing')

        self.assertRaises(
            exception.InvalidVolume,
            self.volume_api._attachment_reserve,
            self.context, volume)

        # The actual assertion: migration_status is in the expected dict
        call_args = volume.conditional_update.call_args
        expected_dict = call_args[0][1]
        self.assertIn('migration_status', expected_dict)


class BaseDriverHooksTestCase(base.BaseVolumeTestCase):
    """Tests that base driver hooks return safe defaults matching the
    manager's expectations (opt-in contract)."""

    def test_prepare_retype_default(self):
        """Base driver returns (False, None) — manager treats as unsupported."""
        result = self.volume.driver.prepare_retype(
            self.context, mock.MagicMock(), {}, {})
        self.assertEqual((False, None), result)

    def test_finalize_retype_default(self):
        """Base driver returns None — manager proceeds without driver_update."""
        result = self.volume.driver.finalize_retype(
            self.context, mock.MagicMock())
        self.assertIsNone(result)

    def test_abort_retype_default(self):
        """Base driver returns None — no rollback needed."""
        result = self.volume.driver.abort_retype(
            self.context, mock.MagicMock())
        self.assertIsNone(result)

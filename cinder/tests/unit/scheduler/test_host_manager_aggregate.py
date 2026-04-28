# Copyright (c) 2025 SAP Corporation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#    implied. See the License for the specific language governing
#    permissions and limitations under the License.

"""Tests for the scheduler aggregate pool feature in HostManager."""

from unittest import mock

from cinder.scheduler import host_manager
from cinder import test


class PropagateAggregateStatsTestCase(test.TestCase):
    """Tests for HostManager._propagate_aggregate_stats."""

    def setUp(self):
        super(PropagateAggregateStatsTestCase, self).setUp()
        self.host_manager = host_manager.HostManager()

    def test_propagate_single_backend_pooled(self):
        """First backend reporting an aggregate_id — no other backends yet."""
        capabilities = {
            'pools': [{
                'pool_name': 'ds-nfs-01',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 100,
            }]
        }
        result = self.host_manager._propagate_aggregate_stats(
            'fcd-vc01', capabilities)

        # No other backends, so allocated_capacity_gb stays at 100
        self.assertEqual(100, result['pools'][0]['allocated_capacity_gb'])
        # Verify it was saved in aggregate_service_states
        self.assertIn('fcd-vc01',
                      self.host_manager.aggregate_service_states)
        self.assertEqual(
            100,
            self.host_manager.aggregate_service_states[
                'fcd-vc01']['shared-ds-001']['allocated_capacity_gb'])

    def test_propagate_two_fcd_backends_same_aggregate(self):
        """Two FCD backends sharing the same aggregate_id."""
        # First backend reports
        caps_vc01 = {
            'pools': [{
                'pool_name': 'ds-nfs-01',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 100,
            }]
        }
        self.host_manager._propagate_aggregate_stats('fcd-vc01', caps_vc01)

        # Second backend reports
        caps_vc02 = {
            'pools': [{
                'pool_name': 'ds-nfs-01',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 50,
            }]
        }
        result = self.host_manager._propagate_aggregate_stats(
            'fcd-vc02', caps_vc02)

        # Second backend should see 50 (own) + 100 (from vc01) = 150
        self.assertEqual(
            150, result['pools'][0]['allocated_capacity_gb'])

    def test_propagate_cross_driver_fcd_and_netapp(self):
        """FCD and NetApp backends sharing the same aggregate_id.

        This is the key scenario: a NetApp NFS pool and a VMware FCD pool
        both point to the same physical storage via aggregate_id.
        """
        # NetApp backend reports first (it now sets has_aggregate_pool)
        caps_netapp = {
            'pools': [{
                'pool_name': '10.10.10.10:/vola',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 200,
            }]
        }
        self.host_manager._propagate_aggregate_stats(
            'netapp-nfs-01', caps_netapp)

        # FCD backend reports second
        caps_fcd = {
            'pools': [{
                'pool_name': 'ds-nfs-01',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 100,
            }]
        }
        result = self.host_manager._propagate_aggregate_stats(
            'fcd-vc01', caps_fcd)

        # FCD should see 100 (own) + 200 (from NetApp) = 300
        self.assertEqual(
            300, result['pools'][0]['allocated_capacity_gb'])

    def test_propagate_bidirectional_update(self):
        """Both backends update each other across successive stats cycles."""
        # First cycle: NetApp reports
        caps_netapp_1 = {
            'pools': [{
                'pool_name': '10.10.10.10:/vola',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 200,
            }]
        }
        self.host_manager._propagate_aggregate_stats(
            'netapp-nfs-01', caps_netapp_1)

        # First cycle: FCD reports (picks up NetApp's 200)
        caps_fcd_1 = {
            'pools': [{
                'pool_name': 'ds-nfs-01',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 100,
            }]
        }
        result_fcd = self.host_manager._propagate_aggregate_stats(
            'fcd-vc01', caps_fcd_1)
        self.assertEqual(
            300, result_fcd['pools'][0]['allocated_capacity_gb'])

        # Second cycle: NetApp reports again (should pick up FCD's 100)
        caps_netapp_2 = {
            'pools': [{
                'pool_name': '10.10.10.10:/vola',
                'aggregate_id': 'shared-ds-001',
                'allocated_capacity_gb': 210,
            }]
        }
        result_netapp = self.host_manager._propagate_aggregate_stats(
            'netapp-nfs-01', caps_netapp_2)
        # NetApp should see 210 (own) + 100 (FCD's last reported) = 310
        self.assertEqual(
            310, result_netapp['pools'][0]['allocated_capacity_gb'])

    def test_propagate_different_aggregate_ids(self):
        """Backends with different aggregate_ids don't affect each other."""
        caps_a = {
            'pools': [{
                'pool_name': 'ds-nfs-01',
                'aggregate_id': 'agg-A',
                'allocated_capacity_gb': 100,
            }]
        }
        self.host_manager._propagate_aggregate_stats('backend-a', caps_a)

        caps_b = {
            'pools': [{
                'pool_name': 'ds-nfs-02',
                'aggregate_id': 'agg-B',
                'allocated_capacity_gb': 50,
            }]
        }
        result = self.host_manager._propagate_aggregate_stats(
            'backend-b', caps_b)

        # Should stay at 50 - different aggregate_id
        self.assertEqual(
            50, result['pools'][0]['allocated_capacity_gb'])

    def test_propagate_pool_without_aggregate_id_ignored(self):
        """Pools without aggregate_id should be untouched."""
        caps = {
            'pools': [
                {
                    'pool_name': 'ds-nfs-01',
                    'aggregate_id': 'shared-ds-001',
                    'allocated_capacity_gb': 100,
                },
                {
                    'pool_name': 'ds-nfs-02',
                    'allocated_capacity_gb': 50,
                },
            ]
        }
        result = self.host_manager._propagate_aggregate_stats(
            'backend-a', caps)

        # Pool without aggregate_id should be unchanged
        self.assertEqual(50, result['pools'][1]['allocated_capacity_gb'])

    def test_propagate_non_pooled_backend(self):
        """Test non-pooled backend (aggregate_id at top level)."""
        caps = {
            'aggregate_id': 'shared-ds-001',
            'allocated_capacity_gb': 100,
        }
        result = self.host_manager._propagate_aggregate_stats(
            'backend-a', caps)

        self.assertEqual(100, result['allocated_capacity_gb'])
        self.assertIn('backend-a',
                      self.host_manager.aggregate_service_states)


class ConsumeFromVolumeAggregateTestCase(test.TestCase):
    """Tests for HostManager.consume_from_volume_aggregate."""

    def setUp(self):
        super(ConsumeFromVolumeAggregateTestCase, self).setUp()
        self.host_manager = host_manager.HostManager()

    def test_consume_no_aggregate_states(self):
        """Backend not in aggregate_service_states -> no-op."""
        backend = mock.Mock()
        backend.host = 'host@backend-a#pool1'

        # Should not raise
        self.host_manager.consume_from_volume_aggregate(backend, 10)

    def test_consume_propagates_to_matching_pools(self):
        """Consuming from one pool propagates to all with same agg_id."""
        # Set up aggregate_service_states (normally done by _propagate)
        self.host_manager.aggregate_service_states = {
            'fcd-vc01': {
                'shared-ds-001': {
                    'allocated_capacity_gb': 100,
                    'pool_name': 'ds-nfs-01',
                },
            },
        }

        # Set up backend_state_map with two backends sharing aggregate_id
        fcd_caps = {
            'aggregate_id': 'shared-ds-001',
            'total_capacity_gb': 500,
            'free_capacity_gb': 400,
            'allocated_capacity_gb': 100,
            'reserved_percentage': 0,
            'max_over_subscription_ratio': 1.0,
            'thin_provisioning_support': False,
            'thick_provisioning_support': True,
            'provisioned_capacity_gb': 100,
            'timestamp': None,
            'pool_name': 'ds-nfs-01',
        }
        netapp_caps = {
            'aggregate_id': 'shared-ds-001',
            'total_capacity_gb': 500,
            'free_capacity_gb': 300,
            'allocated_capacity_gb': 200,
            'reserved_percentage': 0,
            'max_over_subscription_ratio': 1.0,
            'thin_provisioning_support': False,
            'thick_provisioning_support': True,
            'provisioned_capacity_gb': 200,
            'timestamp': None,
            'pool_name': '10.10.10.10:/vola',
        }

        fcd_state = host_manager.BackendState('host@fcd-vc01', None)
        fcd_pool = host_manager.PoolState(
            'host@fcd-vc01', fcd_caps, 'ds-nfs-01')
        fcd_state.pools['ds-nfs-01'] = fcd_pool

        netapp_state = host_manager.BackendState('host@netapp-nfs-01', None)
        netapp_pool = host_manager.PoolState(
            'host@netapp-nfs-01', netapp_caps, '10.10.10.10:/vola')
        netapp_state.pools['10.10.10.10:/vola'] = netapp_pool

        self.host_manager.backend_state_map = {
            'fcd-vc01': fcd_state,
            'netapp-nfs-01': netapp_state,
        }

        # Simulate scheduling a 10GB volume to fcd-vc01#ds-nfs-01
        backend = mock.Mock()
        backend.host = 'host@fcd-vc01#ds-nfs-01'

        self.host_manager.consume_from_volume_aggregate(backend, 10)

        # The NetApp pool should have its allocated_capacity_gb increased
        self.assertEqual(
            210,
            netapp_pool.capabilities['allocated_capacity_gb'])

    def test_consume_skips_pools_without_matching_agg_id(self):
        """Pools with different or no aggregate_id not affected."""
        self.host_manager.aggregate_service_states = {
            'backend-a': {
                'agg-A': {
                    'allocated_capacity_gb': 100,
                    'pool_name': 'pool-a',
                },
            },
        }

        caps_a = {
            'aggregate_id': 'agg-A',
            'total_capacity_gb': 500,
            'free_capacity_gb': 400,
            'allocated_capacity_gb': 100,
            'reserved_percentage': 0,
            'max_over_subscription_ratio': 1.0,
            'thin_provisioning_support': False,
            'thick_provisioning_support': True,
            'provisioned_capacity_gb': 100,
            'timestamp': None,
            'pool_name': 'pool-a',
        }
        caps_b = {
            'aggregate_id': 'agg-B',
            'total_capacity_gb': 500,
            'free_capacity_gb': 400,
            'allocated_capacity_gb': 100,
            'reserved_percentage': 0,
            'max_over_subscription_ratio': 1.0,
            'thin_provisioning_support': False,
            'thick_provisioning_support': True,
            'provisioned_capacity_gb': 100,
            'timestamp': None,
            'pool_name': 'pool-b',
        }

        state_a = host_manager.BackendState('host@backend-a', None)
        pool_a = host_manager.PoolState('host@backend-a', caps_a, 'pool-a')
        state_a.pools['pool-a'] = pool_a

        state_b = host_manager.BackendState('host@backend-b', None)
        pool_b = host_manager.PoolState('host@backend-b', caps_b, 'pool-b')
        state_b.pools['pool-b'] = pool_b

        self.host_manager.backend_state_map = {
            'backend-a': state_a,
            'backend-b': state_b,
        }

        backend = mock.Mock()
        backend.host = 'host@backend-a#pool-a'

        self.host_manager.consume_from_volume_aggregate(backend, 10)

        # backend-b should NOT be affected (different agg_id)
        self.assertEqual(
            100, pool_b.capabilities['allocated_capacity_gb'])


class UpdateServiceCapabilitiesAggregateTestCase(test.TestCase):
    """Tests for the has_aggregate_pool gate in update_service_capabilities."""

    def setUp(self):
        super(UpdateServiceCapabilitiesAggregateTestCase, self).setUp()
        self.host_manager = host_manager.HostManager()

    @mock.patch.object(host_manager.HostManager,
                       '_propagate_aggregate_stats')
    def test_aggregate_pool_triggers_propagation(self, mock_propagate):
        """has_aggregate_pool=True triggers _propagate_aggregate_stats."""
        mock_propagate.return_value = {}
        capabilities = {
            'has_aggregate_pool': True,
            'pools': [{'pool_name': 'p1', 'aggregate_id': 'agg-1',
                       'allocated_capacity_gb': 100}],
            'timestamp': None,
        }

        self.host_manager.update_service_capabilities(
            'volume', 'host@backend', capabilities, None, None)

        mock_propagate.assert_called_once()

    @mock.patch.object(host_manager.HostManager,
                       '_propagate_aggregate_stats')
    def test_no_aggregate_pool_skips_propagation(self, mock_propagate):
        """has_aggregate_pool=False does not trigger propagation."""
        capabilities = {
            'pools': [{'pool_name': 'p1',
                       'allocated_capacity_gb': 100}],
            'timestamp': None,
        }

        self.host_manager.update_service_capabilities(
            'volume', 'host@backend', capabilities, None, None)

        mock_propagate.assert_not_called()

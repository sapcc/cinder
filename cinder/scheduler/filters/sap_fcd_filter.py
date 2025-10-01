# Copyright (c) 2024 SAP SE
#
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

from cinder.scheduler import filters
from cinder.volume.volume_utils import extract_host


LOG = logging.getLogger(__name__)


class SAPFCDFilter(filters.BaseBackendFilter):
    """Filter out pools that are not the same as the original pool.

    For the FCD driver only, we want to ensure that a cross vcenter
    migration lands on the same pool as the the source vcenter.  This
    ensures that a cross vcenter migration results in no data movement.
    """

    _SHARD_PREFIX = 'vc-'

    def _is_vmware_fcd(self, backend_state):
        if backend_state.storage_protocol != 'vstorageobject':
            return False
        return True

    def _extract_shard_from_host(self, host):
        """Extract the shard from the host."""

        # get the string starting with the shard from the host
        # vc-d-X@backend#pool
        shard_plus = host[host.find(self._SHARD_PREFIX):]
        # Now get only the shard. This is the string until the next @
        return shard_plus[:shard_plus.find('@')]

    def backend_passes(self, backend_state, filter_properties):

        if not self._is_vmware_fcd(backend_state):
            LOG.debug("SAP FCD Filter ignoring backend %s isn't vmware_fcd"
                      " driver", backend_state.backend_id)
            return True

        spec = filter_properties.get('request_spec', {})
        vol = spec.get('volume_properties', {})

        valid_ops = ['migrate_volume', 'find_backend_for_connector']
        if spec.get('operation') not in valid_ops:
            LOG.debug("SAP FCD Filter ignoring operation %s",
                      spec.get('operation'))
            return True

        # We are migrating a volume.  If we are migrating to a different
        # backend, we want to ensure that the pool is the same as the
        # original backend.

        # host@backend#pool

        # This is the backend passed in to the filter.
        filter_pool = extract_host(backend_state.host, 'pool')
        filter_host = extract_host(backend_state.host, 'host')

        # This is the original host, backend and pool that the volume
        # was created on.
        orig_host = vol.get('host')
        orig_host_name = extract_host(orig_host, 'host')
        # This returns name@backend.  We only want the backend.
        orig_backend = extract_host(orig_host, 'backend').split('@')[1]
        orig_pool = extract_host(orig_host, 'pool')

        # This is the destination host, backend and pool that the volume
        # is being migrated to.
        # If the destination host provides a pool, we will ignore that
        # pool, because we want it to move to the same pool on the
        # new backend host first.  This prevents data movement.
        # You can issue a migrate command with a destination pool
        # if it's on the same host.

        # For a migrate_volume operation, the destination host can be
        # specified in the request spec. That is the destination host
        # the admin wants the volume to be migrated to.
        destination_host = spec.get('destination_host')
        if destination_host:
            dest_backend = extract_host(
                destination_host, 'backend').split('@')[1]
        else:
            # For a find_backend_for_connector operation, the destination
            # host is the host that the connector is being migrated to as
            # there is no destination_host specified in the request spec.
            destination_host = backend_state.host
            dest_backend = extract_host(
                destination_host, 'backend').split('@')[1]

        if orig_backend != dest_backend:
            LOG.debug("Allow migration to different backend %s %s %s",
                      orig_backend, dest_backend, backend_state.host)
            return True

        # we stay on the backend

        # if we move to the same pool on _any_ other host, that's fine with us
        if orig_pool == filter_pool:
            LOG.debug("Allow migration to same pool %s %s %s",
                      orig_pool, filter_pool, backend_state.host)
            return True

        # we switch pools

        tpool_mpath = backend_state.capabilities.get('mount_path', '')
        if tpool_mpath != "" and len(tpool_mpath.split('/')) == 3:
            # mount_path is qtree path
            # 192.168.8.1:/nfs_stnpca2_st051_ds03/nfs_stnpca2_st051_ds03_vc_b_2
            tpool_vol = tpool_mpath.split(':')[1].split('/')[1]
            orig_shard = self._extract_shard_from_host(orig_host)
            orig_shard_ = orig_shard.replace('-', '_')
            if orig_pool.replace("_%s" % orig_shard_, '') == tpool_vol:
                LOG.debug("Allow migration to same qtree pool %s %s %s",
                          orig_pool, filter_pool, backend_state.host)
                return True

        # if we move on the same host, it's fine if we switch pools
        if orig_host_name == filter_host:
            LOG.debug("Allow migration to same host %s %s %s",
                      orig_host_name, filter_host, backend_state.host)
            return True

        LOG.debug("Deny migration to %s", backend_state.host)
        return False

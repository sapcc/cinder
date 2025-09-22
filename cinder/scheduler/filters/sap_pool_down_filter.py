# Copyright (c) 2020 SAP SE
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
from cinder.volume import volume_utils


LOG = logging.getLogger(__name__)


class SAPPoolDownFilter(filters.BaseBackendFilter):
    """Filter out pools that are not marked 'up'."""

    def backend_passes(self, host_state, filter_properties):
        valid_ops = ['migrate_volume', 'find_backend_for_connector']
        spec = filter_properties.get('request_spec', {})
        backend = volume_utils.extract_host(
            host_state.host, 'backend').split('@')[1]

        # For FCD based volumes, we want to allow a volume migration
        # to the same pool, even if it's marked down draining.
        # As the migration is nothing more than a registration to a
        # new host.  The volume is already on the pool.

        # Just in case we are running on a host that doesn't have a pool_state,
        # we will assume it is up as that is the default for upstream cinder.
        if hasattr(host_state, 'pool_state'):
            pool_state = host_state.pool_state
        else:
            LOG.debug("Host %(host)s has no pool_state, assuming it is up",
                      {'host': host_state.host})
            pool_state = 'up'

        if pool_state == 'up':
            return True
        elif spec.get('operation') in valid_ops and backend == 'vmware_fcd':
            LOG.debug("Allow migration to a down pool for vmware_fcd only")
            return True
        else:
            LOG.debug("%(id)s pool state is not 'up'. state='%(state)s'",
                      {'id': host_state.backend_id,
                       'state': host_state.pool_state})
            return False

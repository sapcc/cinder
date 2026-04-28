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
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""The SAP Contrib API."""

from http import HTTPStatus

import jsonschema
from oslo_config import cfg
from oslo_log import log as logging
import webob

from cinder.api import common
from cinder.api import extensions
from cinder.api import microversions as mv
from cinder.api.openstack import wsgi
from cinder.api import validation
from cinder.api.validation import parameter_types
from cinder.common import constants
from cinder import exception
from cinder.i18n import _
from cinder.policies import services as policy
from cinder.policies import sap as sap_policy
from cinder import volume
from cinder.volume import rpcapi as volume_rpcapi
from cinder.volume import volume_utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


VALID_POOL_STATES = ["available", "drain"]

# Schema for SAP Contrib
schema_recount_host_stats = {
    'type': 'object',
    'properties': {
        'host': parameter_types.hostname,
    },
    'additionalProperties': False,
}

schema_set_pool_state = {
    'type': 'object',
    'properties': {
        'host': parameter_types.cinder_host,
        'status': {'type': ['string', 'null'],
                   'format': 'pool_status'},
    },
    'additionalProperties': False,
}

schema_set_aggregate_id = {
    'type': 'object',
    'properties': {
        'host': parameter_types.cinder_host,
        'aggregate_id': {'type': ['string', 'null']},
    },
    'additionalProperties': False,
}

schema_get_aggregate_id = {
    'type': 'object',
    'properties': {
        'host': parameter_types.cinder_host,
    },
    'additionalProperties': False,
}


# The validator for the pool status in the set_pool_state API
@jsonschema.FormatChecker.cls_checks('pool_status')
def _validate_pool_status(param_value):
    POOL_STATES = ["available", "drain"]
    if param_value and param_value.lower() not in POOL_STATES:
        msg = _("Pool status: %(status)s is invalid, "
                "valid statuses are: "
                "%(valid)s.") % {'status': param_value,
                                 'valid': POOL_STATES}
        raise exception.InvalidParameterValue(err=msg)
    return True


class SAPContribController(wsgi.Controller):
    def __init__(self, ext_mgr=None):
        self.ext_mgr = ext_mgr
        super(SAPContribController, self).__init__()
        self.volume_api = volume.API()
        self.rpc_apis = {
            constants.VOLUME_BINARY: volume_rpcapi.VolumeAPI(),
        }

    def _volume_api_proxy(self, fun, *args):
        try:
            return fun(*args)
        except exception.ServiceNotFound as ex:
            raise exception.InvalidInput(ex.msg)

    @volume_utils.trace
    def index(self, req):
        """List the SAP Contrib, which is none."""
        context = req.environ['cinder.context']
        context.authorize(policy.GET_ALL_POLICY)
        return webob.Response(status_int=HTTPStatus.OK)

    @volume_utils.trace
    def update(self, req, id, body):
        """Update the SAP Contrib."""
        context = req.environ['cinder.context']
        context.authorize(policy.UPDATE_POLICY)

        if id == "recount_host_stats":
            return self._recount_host_stats(req, context, body=body)
        elif id == "set_pool_state":
            return self._set_pool_state(req, context, body=body)
        elif id == "set_aggregate_id":
            return self._set_aggregate_id(req, context, body=body)
        elif id == "get_aggregate_id":
            return self._get_aggregate_id(req, context, body=body)
        else:
            raise exception.InvalidInput(reason=_("Unknown action"))

        return webob.Response(status_int=HTTPStatus.ACCEPTED)

    @validation.schema(schema_recount_host_stats)
    @volume_utils.trace
    def _recount_host_stats(self, req, context, body):
        """Ask the volume manager to recount allocated capacity for host."""
        cluster_name, host = common.get_cluster_host(req, body,
                                                     mv.REPLICATION_CLUSTER)
        self._volume_api_proxy(self.volume_api.recount_host_stats, context,
                               host)
        return webob.Response(status_int=HTTPStatus.ACCEPTED)

    @volume_utils.trace
    def _validate_set_pool_state(self, req, body):
        update = {}
        status = body.get('status', None).lower()
        host = body.get('host', None).lower()

        update['status'] = status
        update['host'] = host
        return update

    @validation.schema(schema_set_pool_state)
    @volume_utils.trace
    def _set_pool_state(self, req, context, body):
        """Set the pool state for the host."""
        cluster_name, host = common.get_cluster_host(req, body,
                                                     mv.REPLICATION_CLUSTER)
        update = self._validate_set_pool_state(req, body)

        # Make sure the status is valid
        if update['status'] not in VALID_POOL_STATES:
            msg = _("Cannot reset-state to %s"
                    % update.get('status'))
            raise webob.exc.HTTPBadRequest(explanation=msg)

        self._volume_api_proxy(self.volume_api.set_pool_state,
                               context, host, update['status'])
        return webob.Response(status_int=HTTPStatus.ACCEPTED)

    @validation.schema(schema_set_aggregate_id)
    @volume_utils.trace
    def _set_aggregate_id(self, req, context, body):
        """Set the aggregate_id for a pool."""
        cluster_name, host = common.get_cluster_host(req, body,
                                                     mv.REPLICATION_CLUSTER)
        aggregate_id = body.get('aggregate_id', None)

        self._volume_api_proxy(self.volume_api.set_aggregate_id,
                               context, host, aggregate_id)
        return webob.Response(status_int=HTTPStatus.ACCEPTED)

    @validation.schema(schema_get_aggregate_id)
    @volume_utils.trace
    def _get_aggregate_id(self, req, context, body):
        """Get the aggregate_id for a pool from the backend driver."""
        cluster_name, host = common.get_cluster_host(req, body,
                                                     mv.REPLICATION_CLUSTER)
        aggregate_id = self._volume_api_proxy(
            self.volume_api.get_aggregate_id, context, host)

        resp_body = {'host': host, 'aggregate_id': aggregate_id}
        return {'aggregate_info': resp_body}


class Sap(extensions.ExtensionDescriptor):
    """SAP Contrib support."""

    name = "SAPContrib"
    alias = "os-sap-contrib"
    updated = "2025-09-04T00:00:00-00:00"

    def get_resources(self):
        resources = []
        controller = SAPContribController(self.ext_mgr)
        resource = extensions.ResourceExtension('os-sap-contrib', controller)
        resources.append(resource)
        return resources

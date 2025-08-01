# Copyright (c) 2020 SAP SE
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

"""
RPC server and client for communicating with other netapp drivers directly.
This allows remote calls to be made to a running netapp driver to do work on
volumes.
"""
from oslo_log import log as logging
import oslo_messaging as messaging

from cinder import rpc
from cinder.volume.rpcapi import VolumeAPI
from cinder.volume import volume_utils


LOG = logging.getLogger(__name__)


class SAPNetappDriverRemoteApi(rpc.RPCAPI):
    RPC_API_VERSION = VolumeAPI.RPC_API_VERSION
    RPC_DEFAULT_VERSION = RPC_API_VERSION
    TOPIC = VolumeAPI.TOPIC
    BINARY = VolumeAPI.BINARY

    def _get_cctxt(self, host=None, version=None, **kwargs):
        kwargs['server'] = volume_utils.extract_host(host)
        return super(SAPNetappDriverRemoteApi, self)._get_cctxt(version=version,
                                                                **kwargs)

    @volume_utils.trace
    def test(self, ctxt, volume, host):
        cctxt = self._get_cctxt(host)
        LOG.warning("CAlling Netapp Remote API::test")
        cctxt = self._get_cctxt(host)
        return cctxt.call(ctxt, 'test')


class SAPNetappDriverRemoteService(object):
    RPC_API_VERSION = SAPNetappDriverRemoteApi.RPC_API_VERSION

    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, driver):
        self._driver = driver

    @volume_utils.trace
    def test(self, ctxt):
        LOG.warning("Netapp Remote Service::test")
        return True

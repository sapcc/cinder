# Copyright (c) 2026 SAP SE
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

import threading
from unittest import mock
import uuid

from oslo_config import cfg
import oslo_messaging as messaging

from cinder.tests.unit import test
from cinder.volume.drivers.netapp import remote


class NetAppRemoteTimeoutTestCase(test.TestCase):
    """Exercise RPC timeout semantics with an in-memory client and server."""

    def test_timed_out_clone_request_can_complete_later(self):
        received = threading.Event()
        release = threading.Event()
        completed = threading.Event()

        def delayed_clone(**kwargs):
            received.set()
            if not release.wait(5):
                raise RuntimeError('Test did not release the clone.')
            completed.set()

        driver = mock.Mock()
        driver.zapi_client.clone_file_rpc.side_effect = delayed_clone
        service = remote.SAPNetappDriverRemoteService(driver)
        transport = messaging.get_rpc_transport(cfg.CONF, url='fake://')
        self.addCleanup(transport.cleanup)
        target = messaging.Target(topic='netapp-test-' + uuid.uuid4().hex,
                                  server='netapp-test',
                                  version=service.RPC_API_VERSION)
        server = messaging.get_rpc_server(transport, target, [service],
                                          executor='threading')
        client = messaging.get_rpc_client(transport, target, timeout=0.1)
        server.start()
        try:
            self.assertRaises(
                messaging.MessagingTimeout, client.call, {}, 'clone_file',
                flex_vol='vol', src_path='source', dest_path='destination',
                vserver='svm', dest_exists=False, is_snapshot=False)
            self.assertTrue(received.wait(5))
            self.assertFalse(completed.is_set())
            release.set()
            self.assertTrue(completed.wait(5))
        finally:
            release.set()
            server.stop()
            server.wait()

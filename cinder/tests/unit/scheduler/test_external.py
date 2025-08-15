# Copyright 2025 SAP SE or an SAP affiliate company.
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
Tests for the external scheduler api call.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import jsonschema
import requests

from cinder import context
from cinder.scheduler.base_weight import WeighedObject
from cinder.scheduler.external import call_external_scheduler_api
from cinder.tests.unit.scheduler import fakes
from cinder.tests.unit.scheduler import test_scheduler

# The expected request schema for the external scheduler API.
# It should contain the spec, hosts, and weights.
REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "spec": {"type": "object"},
        "context": {"type": "object"},
        "hosts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                },
                "required": ["host"],
            },
        },
        "weights": {"type": "object"},
    },
    "required": ["spec", "context", "hosts", "weights"],
    "additionalProperties": False,
}


class ExternalSchedulerAPITestCase(test_scheduler.SchedulerTestCase):
    def setUp(self):
        super(ExternalSchedulerAPITestCase, self).setUp()
        self.flags(external_scheduler_api_url='http://127.0.0.1:1234')
        self.flags(external_scheduler_timeout=5)
        self.h1 = fakes.FakeBackendState('host1', {})
        self.h2 = fakes.FakeBackendState('host2', {})
        self.h3 = fakes.FakeBackendState('host3', {})
        self.example_weighed_hosts = [
            WeighedObject(self.h1, 1.0),
            WeighedObject(self.h2, 0.5),
            WeighedObject(self.h3, 0.0),
        ]
        self.example_spec = {
            'volume_properties': {'size': 1234},
        }
        self.example_ctx = context.RequestContext(
            user_id='fake_user',
            project_id='fake_project',
            is_admin=True,
            read_deleted='no',
            global_request_id='fake_global_request_id',
        )

    def _check_request(self, response=None):
        """Utility to check the request for validity."""
        def wrapped(url, json, timeout):
            self.assertEqual(timeout, 5)  # should be the default timeout
            self.assertEqual(url, 'http://127.0.0.1:1234')
            try:
                jsonschema.validate(json, REQUEST_SCHEMA)
            except jsonschema.ValidationError as e:
                msg = f"Request JSON schema validation failed: {e.message}"
                self.fail(msg)
            return response or MagicMock()
        return wrapped

    @patch('requests.post')
    def test_context_included_in_request(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'hosts': ['host1', 'host3']}
        mock_post.side_effect = self._check_request(mock_response)

        call_external_scheduler_api(
            self.example_ctx,
            self.example_weighed_hosts,
            self.example_spec,
        )

        # Check that the context is serialized and included in the request
        _, kwargs = mock_post.call_args
        self.assertIn(
            'context', kwargs['json'],
            'Context should be included in the request'
        )
        self.assertIn(
            'global_request_id', kwargs['json']['context'],
            'Global request ID should be included in the context'
        )
        # The auth_token should be excluded from the context
        self.assertNotIn(
            'auth_token', kwargs['json']['context'],
            'Auth token should not be included in the context'
        )
        expected_dict = self.example_ctx.to_dict()
        del expected_dict['auth_token']
        # Check that the context is serialized correctly
        self.assertEqual(
            expected_dict,
            kwargs['json']['context'],
            'Context should be serialized correctly'
        )

    @patch('requests.post')
    @patch('cinder.scheduler.external.LOG.debug')
    def test_enabled_api_success(self, mock_debug_log, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'hosts': ['host1', 'host3']}
        mock_post.side_effect = self._check_request(mock_response)

        log = ""

        def append_log(msg, data):
            nonlocal log
            log += msg % data
        mock_debug_log.side_effect = append_log

        hosts = call_external_scheduler_api(
            self.example_ctx,
            self.example_weighed_hosts,
            self.example_spec,
        )
        self.assertEqual(
            ['host1', 'host3'],
            [h.obj.host for h in hosts]
        )
        self.assertIn('Calling external scheduler API with ', log)

    @patch('requests.post')
    @patch('cinder.scheduler.external.LOG.warning')
    def test_enabled_api_empty_response(self, mock_warn_log, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'hosts': []}
        mock_post.side_effect = self._check_request(mock_response)

        hosts = call_external_scheduler_api(
            self.example_ctx,
            self.example_weighed_hosts,
            self.example_spec,
        )
        self.assertEqual([], hosts)
        mock_warn_log.assert_called_with(
            'External scheduler filtered out all hosts.'
        )

    @patch('requests.post')
    @patch('cinder.scheduler.external.LOG.error')
    def test_enabled_api_timeout(self, mock_err_log, mock_post):
        mock_post.side_effect = requests.exceptions.Timeout

        log = ""

        def append_log(msg, data):
            nonlocal log
            log += msg % data
        mock_err_log.side_effect = append_log

        hosts = call_external_scheduler_api(
            self.example_ctx,
            self.example_weighed_hosts,
            self.example_spec,
        )
        # Should fallback to the original host list.
        self.assertEqual(
            ['host1', 'host2', 'host3'],
            [h.obj.host for h in hosts]
        )
        self.assertIn('Failed to call external scheduler API: ', log)

    @patch('requests.post')
    @patch('cinder.scheduler.external.LOG.error')
    def test_enabled_api_invalid_response(self, mock_err_log, mock_post):
        invalid_response_dicts = [
            {},
            {"hosts": "not a list"},
            {"hosts": [1, 2, "host1"]},
            {"hosts": [{"name": "host1", "status": "up"}]},
        ]

        log = ""

        def append_log(msg, data):
            nonlocal log
            log += msg % data
        mock_err_log.side_effect = append_log

        for response_dict in invalid_response_dicts:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = response_dict
            mock_post.side_effect = self._check_request(mock_response)

            hosts = call_external_scheduler_api(
                self.example_ctx,
                self.example_weighed_hosts,
                self.example_spec,
            )
            # Should fallback to the original host list.
            self.assertEqual(
                ['host1', 'host2', 'host3'],
                [h.obj.host for h in hosts]
            )
            self.assertIn('External scheduler response is invalid: ', log)

    @patch('requests.post')
    @patch('cinder.scheduler.external.LOG.error')
    def test_enabled_api_json_decode_err(self, mock_err_log, mock_post):
        log = ""

        def append_log(msg, data):
            nonlocal log
            log += msg % data
        mock_err_log.side_effect = append_log

        mock_response = MagicMock()
        mock_response.status_code = 200
        # Note: requests.exceptions.InvalidJSONError is also a RequestException
        mock_response.json.side_effect = requests.exceptions.InvalidJSONError
        mock_post.side_effect = self._check_request(mock_response)

        hosts = call_external_scheduler_api(
            self.example_ctx,
            self.example_weighed_hosts,
            self.example_spec,
        )
        # Should fallback to the original host list.
        self.assertEqual(
            ['host1', 'host2', 'host3'],
            [h.obj.host for h in hosts]
        )
        self.assertIn('Failed to call external scheduler API: ', log)

    @patch('requests.post')
    @patch('cinder.scheduler.external.LOG.error')
    def test_enabled_api_error_reply(self, mock_err_log, mock_post):
        mock_post.side_effect = requests.exceptions.HTTPError

        log = ""

        def append_log(msg, data):
            nonlocal log
            log += msg % data
        mock_err_log.side_effect = append_log

        hosts = call_external_scheduler_api(
            self.example_ctx,
            self.example_weighed_hosts,
            self.example_spec,
        )
        # Should fallback to the original host list.
        self.assertEqual(
            ['host1', 'host2', 'host3'],
            [h.obj.host for h in hosts]
        )
        self.assertIn('Failed to call external scheduler API: ', log)

# Copyright (c) 2017 Red Hat, Inc.
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

import time
from unittest import mock

from cinder import manager
from cinder import objects
from cinder.tests.unit import test


class FakeManager(manager.CleanableManager):
    def __init__(self, service_id=None, keep_after_clean=False):
        if service_id:
            self.service_id = service_id
        self.keep_after_clean = keep_after_clean

    def _do_cleanup(self, ctxt, vo_resource):
        vo_resource.status += '_cleaned'
        vo_resource.save()
        return self.keep_after_clean


class TestManager(test.TestCase):
    @mock.patch('cinder.utils.set_log_levels')
    def test_set_log_levels(self, set_log_mock):
        service = manager.Manager()
        log_request = objects.LogLevel(prefix='sqlalchemy.', level='debug')
        service.set_log_levels(mock.sentinel.context, log_request)
        set_log_mock.assert_called_once_with(log_request.prefix,
                                             log_request.level)

    @mock.patch('cinder.utils.get_log_levels')
    def test_get_log_levels(self, get_log_mock):
        get_log_mock.return_value = {'cinder': 'DEBUG', 'cinder.api': 'ERROR'}
        service = manager.Manager()
        log_request = objects.LogLevel(prefix='sqlalchemy.')
        result = service.get_log_levels(mock.sentinel.context, log_request)
        get_log_mock.assert_called_once_with(log_request.prefix)

        expected = (objects.LogLevel(prefix='cinder', level='DEBUG'),
                    objects.LogLevel(prefix='cinder.api', level='ERROR'))

        self.assertEqual(set(str(r) for r in result.objects),
                         set(str(e) for e in expected))


class TestThreadPoolManager(test.TestCase):
    """Test cases for ThreadPoolManager greenthread pool + shutdown."""

    def test_add_to_threadpool_executes(self):
        """Test that _add_to_threadpool runs the task to completion."""
        mgr = manager.ThreadPoolManager()
        executed = []

        mgr._add_to_threadpool(lambda: executed.append(True))
        mgr.cleanup_threadpool()  # waitall ensures task completes

        self.assertEqual([True], executed)

    def test_signal_shutdown_rejects_new_tasks(self):
        """Test that new tasks are rejected after shutdown signal."""
        mgr = manager.ThreadPoolManager()
        mgr.signal_shutdown()

        executed = []
        result = mgr._add_to_threadpool(lambda: executed.append(True))

        # Should return None when shutdown is signaled
        self.assertIsNone(result)

        # Give it a moment to potentially execute
        time.sleep(0.1)

        self.assertEqual([], executed)
        mgr.cleanup_threadpool()

    def test_signal_shutdown_allows_existing_tasks(self):
        """Test that existing tasks continue after shutdown signal."""
        mgr = manager.ThreadPoolManager()
        completed = []

        def slow_task():
            time.sleep(0.1)
            completed.append(True)

        # Spawn task first
        mgr._add_to_threadpool(slow_task)
        # Then signal shutdown
        mgr.signal_shutdown()

        # Existing task should still complete on cleanup (waitall)
        mgr.cleanup_threadpool()
        self.assertEqual([True], completed)

    def test_cleanup_threadpool_allows_restart(self):
        """Test that cleanup_threadpool re-creates pool for restart.

        oslo.service ProcessLauncher with restart_method='mutate' forks
        a new child that inherits the parent's manager object. After
        stop()/wait()/cleanup(), the child calls start() -> init_host()
        which needs to submit tasks to the threadpool.
        """
        mgr = manager.ThreadPoolManager()

        # Simulate a full shutdown cycle
        mgr.signal_shutdown()
        mgr.cleanup_threadpool()

        # After cleanup, the pool should be re-created and usable
        executed = []
        result = mgr._add_to_threadpool(lambda: executed.append(True))
        self.assertIsNotNone(result)

        mgr.cleanup_threadpool()
        self.assertEqual([True], executed)

    def test_cleanup_threadpool_clears_shutdown_event(self):
        """Test cleanup resets shutdown_event so new tasks accepted."""
        mgr = manager.ThreadPoolManager()

        mgr.signal_shutdown()
        # After signal_shutdown, tasks should be rejected
        result = mgr._add_to_threadpool(lambda: None)
        self.assertIsNone(result)

        mgr.cleanup_threadpool()

        # After cleanup, tasks should be accepted again
        executed = []
        result = mgr._add_to_threadpool(lambda: executed.append(True))
        self.assertIsNotNone(result)

        mgr.cleanup_threadpool()
        self.assertEqual([True], executed)

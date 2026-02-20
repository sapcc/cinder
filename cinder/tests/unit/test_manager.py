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

from unittest import mock

import eventlet

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
    """Test cases for ThreadPoolManager graceful shutdown."""

    def test_wait_for_tasks_completes(self):
        """Test that wait_for_tasks waits for spawned tasks."""
        mgr = manager.ThreadPoolManager()
        completed = []

        def slow_task():
            eventlet.sleep(0.1)
            completed.append(True)

        mgr._add_to_threadpool(slow_task)
        result = mgr.wait_for_tasks()

        self.assertTrue(result)
        self.assertEqual([True], completed)

    def test_wait_for_tasks_with_timeout(self):
        """Test that wait_for_tasks respects timeout."""
        mgr = manager.ThreadPoolManager()
        completed = []

        def very_slow_task():
            eventlet.sleep(10)  # Longer than timeout
            completed.append(True)

        mgr._add_to_threadpool(very_slow_task)
        # Use a short timeout
        result = mgr.wait_for_tasks(timeout=0.1)

        self.assertFalse(result)
        # Task should still be running (not completed)
        self.assertEqual([], completed)

    def test_wait_for_tasks_no_tasks(self):
        """Test wait_for_tasks with no tasks returns immediately."""
        mgr = manager.ThreadPoolManager()
        result = mgr.wait_for_tasks()
        self.assertTrue(result)

    def test_wait_for_tasks_zero_timeout_waits_forever(self):
        """Test that timeout=0 means wait indefinitely."""
        mgr = manager.ThreadPoolManager()
        completed = []

        def task():
            eventlet.sleep(0.1)
            completed.append(True)

        mgr._add_to_threadpool(task)
        # timeout=0 should wait forever (not return immediately)
        result = mgr.wait_for_tasks(timeout=0)

        self.assertTrue(result)
        self.assertEqual([True], completed)

    def test_signal_shutdown_rejects_new_tasks(self):
        """Test that new tasks are rejected after shutdown signal."""
        mgr = manager.ThreadPoolManager()
        mgr.signal_shutdown()

        executed = []
        mgr._add_to_threadpool(lambda: executed.append(True))

        # Give it a moment to potentially execute
        eventlet.sleep(0.1)

        self.assertEqual([], executed)

    def test_signal_shutdown_allows_existing_tasks(self):
        """Test that existing tasks continue after shutdown signal."""
        mgr = manager.ThreadPoolManager()
        completed = []

        def slow_task():
            eventlet.sleep(0.1)
            completed.append(True)

        # Spawn task first
        mgr._add_to_threadpool(slow_task)
        # Then signal shutdown
        mgr.signal_shutdown()

        # Wait for tasks should still work
        result = mgr.wait_for_tasks()

        self.assertTrue(result)
        self.assertEqual([True], completed)

    def test_add_to_threadpool_before_shutdown(self):
        """Test tasks can be added before shutdown."""
        mgr = manager.ThreadPoolManager()
        executed = []

        mgr._add_to_threadpool(lambda: executed.append(True))
        mgr.wait_for_tasks()

        self.assertEqual([True], executed)

# Copyright 2024 OpenStack Foundation.
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

from oslo_log import log as logging

from cinder import action_track
from cinder import context as cinder_context
from cinder.tests.unit import test


class TestLogActionTrack(test.TestCase):
    """Tests for LogActionTrack class."""

    def setUp(self):
        super(TestLogActionTrack, self).setUp()
        self.context = cinder_context.get_admin_context()
        self.resource = {'type': 'volume', 'id': 'fake-volume-id'}

    @mock.patch.object(action_track, 'LOG')
    def test_track_info_level(self, mock_log):
        """Test track() logs at INFO level by default."""
        action_track.track(
            self.context,
            action_track.ACTION_VOLUME_CREATE,
            self.resource,
            "Created Volume successfully"
        )
        mock_log.info.assert_called_once()
        call_args = mock_log.info.call_args[0][0]
        self.assertIn("ACTION:'volume_create'", call_args)
        self.assertIn("MSG:'Created Volume successfully'", call_args)
        self.assertIn("RSC:", call_args)

    @mock.patch.object(action_track, 'LOG')
    def test_track_error_level(self, mock_log):
        """Test track() logs at ERROR level with FAILED marker."""
        action_track.track(
            self.context,
            action_track.ACTION_VOLUME_DELETE,
            self.resource,
            "Unable to delete busy volume.",
            loglevel=logging.ERROR
        )
        mock_log.error.assert_called_once()
        call_args = mock_log.error.call_args[0][0]
        self.assertIn("ACTION:'volume_delete'", call_args)
        self.assertIn("FAILED", call_args)
        self.assertIn("MSG:'Unable to delete busy volume.'", call_args)

    @mock.patch.object(action_track, 'LOG')
    def test_track_includes_file_info(self, mock_log):
        """Test track() includes FILE: with filename, line, function."""
        action_track.track(
            self.context,
            action_track.ACTION_VOLUME_CREATE,
            self.resource,
            "test message"
        )
        call_args = mock_log.info.call_args[0][0]
        self.assertIn("FILE:", call_args)
        # The FILE field should contain a filename path, line number,
        # and function name in the format FILE:path:lineno:funcname
        self.assertRegex(call_args, r"FILE:\S+:\d+:\S+")

    @mock.patch.object(action_track, 'LOG')
    def test_track_strips_newlines_from_message(self, mock_log):
        """Test that newlines in messages are stripped."""
        action_track.track(
            self.context,
            action_track.ACTION_VOLUME_CREATE,
            self.resource,
            "line1\nline2\nline3"
        )
        call_args = mock_log.info.call_args[0][0]
        self.assertNotIn("\n", call_args)
        self.assertIn("line1line2line3", call_args)

    @mock.patch.object(action_track, 'LOG')
    def test_track_emitter_failure_is_non_fatal(self, mock_log):
        """Test track() does not raise when the emitter fails."""
        mock_log.info.side_effect = RuntimeError("log failed")
        action_track.track(
            self.context,
            action_track.ACTION_VOLUME_CREATE,
            self.resource,
            "should not raise"
        )
        mock_log.debug.assert_called()


class TestTrackDecorator(test.TestCase):
    """Tests for the track_decorator."""

    def setUp(self):
        super(TestTrackDecorator, self).setUp()
        self.context = cinder_context.get_admin_context()

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_tracks_call(self, mock_log):
        """Test decorator logs 'called' on function entry."""
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_CREATE)
        def create_volume(self_arg, context, volume):
            return "success"

        result = create_volume(mock.Mock(), self.context, volume)
        self.assertEqual("success", result)
        calls = mock_log.info.call_args_list
        self.assertTrue(any("called" in str(c) for c in calls))

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_tracks_exception(self, mock_log):
        """Test decorator logs error on exception."""
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_DELETE)
        def delete_volume(self_arg, context, volume):
            raise ValueError("delete failed")

        self.assertRaises(
            ValueError, delete_volume, mock.Mock(), self.context, volume
        )
        mock_log.error.assert_called()
        error_call = mock_log.error.call_args[0][0]
        self.assertIn("FAILED", error_call)
        self.assertIn("delete failed", error_call)

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_reraises_exception(self, mock_log):
        """Test decorator re-raises the original exception."""
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_CREATE)
        def bad_func(self_arg, context, volume):
            raise ValueError("specific error")

        self.assertRaises(
            ValueError, bad_func, mock.Mock(), self.context, volume
        )

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_finds_context_as_ctxt(self, mock_log):
        """Test decorator finds context when named 'ctxt'."""
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_ATTACH)
        def attach(self_arg, ctxt, volume):
            return "attached"

        result = attach(mock.Mock(), self.context, volume)
        self.assertEqual("attached", result)
        mock_log.info.assert_called()

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_finds_backup_resource(self, mock_log):
        """Test decorator finds backup as a valid resource."""
        backup = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_BACKUP)
        def create_backup(self_arg, context, backup):
            return "backed up"

        result = create_backup(mock.Mock(), self.context, backup)
        self.assertEqual("backed up", result)
        mock_log.info.assert_called()

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_finds_snapshot_resource(self, mock_log):
        """Test decorator finds snapshot as a valid resource."""
        snapshot = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_SNAPSHOT_CREATE)
        def create_snapshot(self_arg, context, snapshot):
            return "snapped"

        result = create_snapshot(mock.Mock(), self.context, snapshot)
        self.assertEqual("snapped", result)
        mock_log.info.assert_called()

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_with_no_resource(self, mock_log):
        """Test decorator handles case where no resource param is found."""
        @action_track.track_decorator(action_track.ACTION_VOLUME_CREATE)
        def func_no_resource(self_arg, context, some_other_arg):
            return "ok"

        result = func_no_resource(mock.Mock(), self.context, "arg")
        self.assertEqual("ok", result)
        mock_log.info.assert_called()

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_with_no_context(self, mock_log):
        """Test decorator handles case where no context param is found."""
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_CREATE)
        def func_no_context(self_arg, other_arg, volume):
            return "ok"

        result = func_no_context(mock.Mock(), "arg", volume)
        self.assertEqual("ok", result)
        mock_log.info.assert_called()

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_preserves_return_value(self, mock_log):
        """Test decorator does not alter the return value."""
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_CREATE)
        def func(self_arg, context, volume):
            return {'key': 'value', 'number': 42}

        result = func(mock.Mock(), self.context, volume)
        self.assertEqual({'key': 'value', 'number': 42}, result)

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_file_identifies_decorated_function(self, mock_log):
        """Test FILE field identifies the decorated function.

        The function is defined outside action_track.py, so the FILE field
        in the 'called' entry must reference this test module and not the
        decorator module's generated trampoline.
        """
        volume = mock.Mock()

        @action_track.track_decorator(action_track.ACTION_VOLUME_CREATE)
        def create_volume(self_arg, context, volume):
            return "success"

        result = create_volume(mock.Mock(), self.context, volume)
        self.assertEqual("success", result)
        calls = mock_log.info.call_args_list
        entries = [str(c) for c in calls if "called" in str(c)]
        self.assertNotEqual([], entries)
        self.assertIn("test_action_track", entries[0])
        # FILE must not point at the decorator module's generated trampoline
        self.assertNotIn("decorator/__init__.py", entries[0])

    @mock.patch.object(action_track, 'LOG')
    def test_decorator_emitter_failure_does_not_mask_original_exception(
            self, mock_log):
        """Test a failing emitter does not replace the original exception.

        The tracking emitter is best-effort.  When the log call raises, the
        original exception from the decorated function must still propagate.
        """
        volume = mock.Mock()
        mock_log.error.side_effect = RuntimeError("emitter failed")

        @action_track.track_decorator(action_track.ACTION_VOLUME_DELETE)
        def delete_volume(self_arg, context, volume):
            raise ValueError("original failure")

        with self.assertRaises(ValueError) as cm:
            delete_volume(mock.Mock(), self.context, volume)
        self.assertEqual("original failure", str(cm.exception))


class TestActionConstants(test.TestCase):
    """Test that all expected action constants are defined."""

    def test_volume_action_constants(self):
        self.assertEqual("volume_create", action_track.ACTION_VOLUME_CREATE)
        self.assertEqual("volume_delete", action_track.ACTION_VOLUME_DELETE)
        self.assertEqual("volume_reserve", action_track.ACTION_VOLUME_RESERVE)
        self.assertEqual("volume_attach", action_track.ACTION_VOLUME_ATTACH)
        self.assertEqual("volume_extend", action_track.ACTION_VOLUME_EXTEND)
        self.assertEqual("volume_detach", action_track.ACTION_VOLUME_DETACH)
        self.assertEqual("volume_migrate", action_track.ACTION_VOLUME_MIGRATE)
        self.assertEqual("volume_retype", action_track.ACTION_VOLUME_RETYPE)
        self.assertEqual("volume_backup", action_track.ACTION_VOLUME_BACKUP)
        self.assertEqual("volume_restore", action_track.ACTION_BACKUP_RESTORE)
        self.assertEqual("volume_backup_delete",
                         action_track.ACTION_VOLUME_BACKUP_DELETE)
        self.assertEqual("volume_copy_to_image",
                         action_track.ACTION_VOLUME_COPY_TO_IMAGE)
        self.assertEqual("volume_backup_reset_status",
                         action_track.ACTION_VOLUME_BACKUP_RESET_STATUS)

    def test_snapshot_action_constants(self):
        self.assertEqual("snapshot_create",
                         action_track.ACTION_SNAPSHOT_CREATE)
        self.assertEqual("snapshot_delete",
                         action_track.ACTION_SNAPSHOT_DELETE)

    def test_group_action_constants(self):
        self.assertEqual("group_create", action_track.ACTION_GROUP_CREATE)
        self.assertEqual("group_delete", action_track.ACTION_GROUP_DELETE)
        self.assertEqual("group_update", action_track.ACTION_GROUP_UPDATE)
        self.assertEqual("group_snapshot_create",
                         action_track.ACTION_GROUP_SNAPSHOT_CREATE)
        self.assertEqual("group_snapshot_delete",
                         action_track.ACTION_GROUP_SNAPSHOT_DELETE)

    def test_valid_resource_names(self):
        self.assertIn('volume', action_track.VALID_RESOURCE_NAMES)
        self.assertIn('backup', action_track.VALID_RESOURCE_NAMES)
        self.assertIn('snapshot', action_track.VALID_RESOURCE_NAMES)

    def test_valid_context_names(self):
        self.assertIn('context', action_track.VALID_CONTEXT_NAMES)
        self.assertIn('ctxt', action_track.VALID_CONTEXT_NAMES)

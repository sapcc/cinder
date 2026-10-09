# Copyright 2023 Openstack Foundation.
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

import abc
import inspect
import traceback

import decorator
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils

from cinder import context as cinder_context


CONF = cfg.CONF
LOG = logging.getLogger(__name__)

ACTION_VOLUME_CREATE = "volume_create"
ACTION_VOLUME_DELETE = "volume_delete"
ACTION_VOLUME_RESERVE = "volume_reserve"
ACTION_VOLUME_ATTACH = "volume_attach"
ACTION_VOLUME_EXTEND = "volume_extend"
ACTION_VOLUME_DETACH = "volume_detach"
ACTION_VOLUME_MIGRATE = "volume_migrate"
ACTION_VOLUME_RETYPE = "volume_retype"
ACTION_VOLUME_BACKUP = "volume_backup"
ACTION_BACKUP_RESTORE = "volume_restore"
ACTION_VOLUME_BACKUP_DELETE = "volume_backup_delete"
ACTION_VOLUME_COPY_TO_IMAGE = "volume_copy_to_image"
ACTION_VOLUME_BACKUP_RESET_STATUS = "volume_backup_reset_status"
ACTION_SNAPSHOT_CREATE = "snapshot_create"
ACTION_SNAPSHOT_DELETE = "snapshot_delete"
ACTION_GROUP_CREATE = "group_create"
ACTION_GROUP_DELETE = "group_delete"
ACTION_GROUP_UPDATE = "group_update"
ACTION_GROUP_SNAPSHOT_CREATE = "group_snapshot_create"
ACTION_GROUP_SNAPSHOT_DELETE = "group_snapshot_delete"


VALID_RESOURCE_NAMES = [
    'volume', 'backup', 'snapshot'
]
VALID_CONTEXT_NAMES = [
    'context', 'ctxt'
]

log_level_map = {
    logging.CRITICAL: 'error',
    logging.ERROR: 'error',
    logging.WARNING: 'warning',
    logging.INFO: 'info',
    logging.DEBUG: 'debug',
}


class ActionTrack(object, metaclass=abc.ABCMeta):
    """Base class for the 'trace' api.

    The purpose of this trace facility is to be able to
    keep track of critical parts of operations against resources.
    This will create a standardized object/log entry for troubleshooting
    actions against resources.

    This is not for performance tracing, but for action/operation tracking.
    by default it will simply format a log entry such that the entries are
    easy to find with a standard format and information.
    """

    @staticmethod
    @abc.abstractmethod
    def track(context, action, resource, message, loglevel=logging.INFO):
        pass

    @staticmethod
    @abc.abstractmethod
    def track_with_file_info(context, action, resource, message,
                             filename, line_number, function,
                             loglevel=logging.INFO):
        pass


class LogActionTrack(ActionTrack):
    @staticmethod
    def _track_with_info(context, action, resource, message,
                         filename, line_number, function,
                         loglevel=logging.INFO):
        entry = f"ACTION:'{action}' "
        if loglevel == logging.ERROR or loglevel == logging.CRITICAL:
            # The action failed and this trace is the reason
            entry += "FAILED "

        msg = message.replace("\n", "")

        entry += (
            f"MSG:'{msg}' "
            f"FILE:{filename}:{line_number}:{function} "
            f"RSC:{resource} "
        )
        log_func = getattr(LOG, log_level_map[loglevel])
        try:
            log_func(entry, resource=resource)
        except Exception:
            # Tracking is best-effort and must never alter the operation
            # being observed.  If emitting fails for any reason, log a
            # diagnostic and carry on without raising.
            LOG.debug("action_track failed to emit entry for action '%s'",
                      action, exc_info=True)

    @staticmethod
    def track(context, action, resource, message, loglevel=logging.INFO):
        # Do not call this directly.   Call action_track.track() instead.

        # We want the frame of the actual caller of action_track.track().
        # Walk the stack to find the first frame outside of this module.
        filename = "unknown"
        lineno = 0
        funcname = "unknown"
        for frame_info, frame_lineno in traceback.walk_stack(None):
            if frame_info.f_code.co_filename != __file__:
                filename = frame_info.f_code.co_filename
                lineno = frame_lineno
                funcname = frame_info.f_code.co_name
                break
        LogActionTrack._track_with_info(context, action, resource, message,
                                        filename,
                                        lineno,
                                        funcname,
                                        loglevel=loglevel)

    @staticmethod
    def track_with_file_info(context, action, resource, message,
                             filename, line_number, function,
                             loglevel=logging.INFO):
        # Do not call this directly.
        # Call action_track.track_with_file_info() instead.
        LogActionTrack._track_with_info(
            context, action, resource, message,
            filename, line_number, function, loglevel=loglevel
        )


def track(context, action, resource, message, loglevel=logging.INFO):
    """For now we only implement LogActionTrack.

    TODO(waboring): add rabbitmq trace? to send entries to a msg queue
    or add: DBtrace to send traces to the DB instead?
    """
    LogActionTrack.track(context, action, resource, message,
                         loglevel=loglevel)


def track_with_info(context, action, resource, message, file, line_number,
                    function, loglevel=logging.INFO):
    """For now we only implement LogActionTrack.

    TODO(waboring): add rabbitmq trace? to send entries to a msg queue
    or add: DBtrace to send traces to the DB instead?
    """
    LogActionTrack.track_with_file_info(context, action, resource, message,
                                        file, line_number, function,
                                        loglevel=loglevel)


def track_decorator(action):
    """Decorator to automatically handle exceptions raised as failures.

       Place this decorator on a function that you want to mark as an
       action failure and the action_track tracing will get called
       for the action.

       @track_decorator(action_track.ACTION_VOLUME_ATTACH)
       def initialize_connection(....)
       If initialize_connection raises an exception then you will get a
       action_track.track called with action of ACTION_VOLUME_ATTACH
       and a failure.

    """
    @decorator.decorator
    def inner(func, *args, **kwargs):
        # Find the context and the volume/backup object
        resource = None
        context = None
        call_args = inspect.getcallargs(func, *args, **kwargs)
        for key in call_args:
            if key in VALID_RESOURCE_NAMES:
                resource = call_args[key]
            elif (key in VALID_CONTEXT_NAMES and
                    isinstance(call_args[key], cinder_context.RequestContext)):
                context = call_args[key]

        # Use deterministic metadata from the decorated function rather
        # than walking the stack.  When called through decorator's
        # trampoline the first frame outside action_track.py would be the
        # decorator module, not the Cinder call site.
        track_with_info(
            context, action, resource, "called",
            func.__code__.co_filename,
            func.__code__.co_firstlineno,
            func.__qualname__,
        )

        try:
            return func(*args, **kwargs)
        except Exception:
            with excutils.save_and_reraise_exception() as exc:
                # We only want the frame of the caller
                tl = traceback.extract_tb(exc.tb)
                if len(tl) > 1:
                    i = tl[1]
                else:
                    i = tl[0] if tl else None
                message = str(exc.value)
                if i:
                    track_with_info(
                        context, action, resource, message,
                        i.filename, i.lineno, i.name,
                        loglevel=logging.ERROR
                    )
                else:
                    track_with_info(
                        context, action, resource, message,
                        "unknown", 0, "unknown",
                        loglevel=logging.ERROR
                    )
    return inner

# Cinder Volume Service Graceful Shutdown Implementation Plan

## Executive Summary

This document outlines a plan to implement graceful shutdown capabilities for the Cinder Volume service, enabling it to complete in-flight operations before terminating. This is particularly critical for Kubernetes deployments where pod termination signals must be handled properly to avoid data corruption and orphaned operations.

**Key Requirements:**
1. Complete in-flight operations before shutdown
2. Stop accepting new inbound RPC requests immediately
3. **Maintain outbound RPC capability** during drain period (for scheduler notifications, volume migrations, etc.)
4. Support seamless handover to a replacement service instance with the same host identity

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Current Architecture Analysis](#current-architecture-analysis)
3. [Kubernetes Integration](#kubernetes-integration)
4. [Outbound RPC Requirements](#outbound-rpc-requirements)
5. [Host Identity and Service Handover](#host-identity-and-service-handover)
6. [Proposed Solution](#proposed-solution)
7. [Implementation Details](#implementation-details)
8. [Configuration Options](#configuration-options)
9. [Testing Strategy](#testing-strategy)
10. [Rollout Plan](#rollout-plan)

---

## Problem Statement

### Current Behavior

When the Cinder Volume service receives a termination signal (SIGTERM), it:

1. Stops accepting new RPC messages
2. **Stops coordination service immediately** (releases distributed locks)
3. Waits for the oslo.service ThreadGroup to complete
4. **Does NOT wait** for tasks spawned via `ThreadPoolManager._add_to_threadpool()`
5. **Does NOT coordinate** with long-running volume operations (create, delete, migrate, etc.)

### Impact

- Volume operations may be interrupted mid-execution
- Storage backend resources may be left in inconsistent states
- Database records may not reflect actual backend state
- Kubernetes rolling updates can cause service disruptions
- **Outbound RPC calls may fail** if TRANSPORT is cleaned up prematurely

### Goals

1. **Complete in-flight operations** before shutdown
2. **Stop accepting new requests** immediately upon receiving termination signal
3. **Maintain outbound RPC functionality** during the graceful shutdown period
4. **Respect timeout boundaries** to prevent indefinite hangs
5. **Integrate with Kubernetes** pod lifecycle management
6. **Support clean handover** to replacement service with same host identity

---

## Current Architecture Analysis

### Service Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                    cinder-volume process                     │
├─────────────────────────────────────────────────────────────┤
│  oslo.service.ProcessLauncher                               │
│    └── oslo.service.Service (cinder.service.Service)        │
│          ├── self.tg (ThreadGroup) - managed by oslo        │
│          ├── self.rpcserver (oslo.messaging RPC server)     │
│          └── self.manager (VolumeManager)                   │
│                ├── self._tp (GreenPool) - NOT managed       │
│                ├── self.scheduler_rpcapi (outbound RPC)     │
│                └── Driver operations                        │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Service Class (`cinder/service.py:124-458`)

The `Service` class extends `oslo_service.service.Service` and manages:

- RPC server lifecycle (`rpcserver`, `backend_rpcserver`, `cluster_rpcserver`)
- Periodic tasks (state reporting, capability publishing)
- Coordination service (tooz-based distributed locking)

**Current stop/wait implementation:**

```python
# cinder/service.py:431-458
def stop(self) -> None:
    try:
        if self.rpcserver is not None:
            self.rpcserver.stop()
        if self.backend_rpcserver:
            self.backend_rpcserver.stop()
        if self.cluster_rpcserver:
            self.cluster_rpcserver.stop()
    except Exception:
        pass

    if self.coordination:
        try:
            coordination.COORDINATOR.stop()  # ← PROBLEM: Stops too early
        except Exception:
            pass
    super(Service, self).stop(graceful=True)

def wait(self) -> None:
    if self.rpcserver:
        self.rpcserver.wait()
    if self.backend_rpcserver:
        self.backend_rpcserver.wait()
    if self.cluster_rpcserver:
        self.cluster_rpcserver.wait()
    super(Service, self).wait()
```

#### 2. ThreadPoolManager (`cinder/manager.py:163-169`)

```python
class ThreadPoolManager(Manager):
    def __init__(self, *args, **kwargs):
        self._tp = greenpool.GreenPool()
        super(ThreadPoolManager, self).__init__(*args, **kwargs)

    def _add_to_threadpool(self, func, *args, **kwargs):
        self._tp.spawn_n(func, *args, **kwargs)
```

**Problem:** The GreenPool (`self._tp`) is completely separate from oslo.service's ThreadGroup and is never waited on during shutdown.

#### 3. VolumeManager (`cinder/volume/manager.py:232+`)

Inherits from `ThreadPoolManager` and `CleanableManager`. Operations are:

- **Synchronous RPC handlers**: Run in oslo.messaging executor threads
- **Async threadpool tasks**: Spawned via `_add_to_threadpool()` (not tracked)
- **TaskFlow operations**: Run synchronously within RPC context

#### 4. Worker Table Tracking (`cinder/objects/cleanable.py`)

Cinder has an existing mechanism for tracking in-progress operations:

```python
@objects.Volume.set_workers
def create_volume(self, context, volume, ...):
    # Worker entry created before execution
    # Worker entry removed after completion
    ...
```

This provides **crash recovery** but is not currently used for graceful shutdown coordination.

### Signal Flow

```
SIGTERM received
    │
    ▼
oslo.service.ProcessLauncher._handle_signal()
    │
    ▼
ProcessLauncher sets graceful_shutdown_timeout alarm
    │
    ▼
ProcessLauncher.stop() → Service.stop()
    │
    ├── rpcserver.stop()      ← Stops accepting new messages
    ├── coordination.stop()    ← PROBLEM: Stops distributed locks too early
    └── oslo.Service.stop(graceful=True)
            │
            └── ThreadGroup.stop(graceful=True)
                    │
                    └── Waits for tg threads (periodic tasks only)
    │
    ▼
ProcessLauncher.wait() → Service.wait()
    │
    ├── rpcserver.wait()      ← Waits for in-flight RPC handlers
    └── oslo.Service.wait()
            │
            └── ThreadGroup.wait()
    │
    ▼
rpc.cleanup()                 ← PROBLEM: May happen before tasks complete
    │
    ▼
Process exits
    │
    ✗ ThreadPoolManager._tp tasks may still be running!
    ✗ Long-running driver operations may be interrupted!
    ✗ Outbound RPC calls during drain may fail!
```

---

## Outbound RPC Requirements

### Why Outbound RPC Must Continue During Shutdown

The Volume Manager makes outbound RPC calls during normal operations that **must complete** for proper operation. These include:

#### 1. Scheduler RPC API (`cinder/scheduler/rpcapi.py`)

Used by VolumeManager for:
- **Rescheduling failed operations**: `scheduler_rpcapi.create_volume()` when local creation fails
- **Capability updates**: `scheduler_rpcapi.update_service_capabilities()`
- **Notification**: `scheduler_rpcapi.notify_service_capabilities()`

#### 2. Volume RPC API (`cinder/volume/rpcapi.py`)

Used during migrations and multi-backend operations:
- `volume_rpcapi.initialize_connection()` - for assisted migrations
- `volume_rpcapi.terminate_connection()`
- `volume_rpcapi.delete_volume()` - cleanup on migration failure
- `volume_rpcapi.update_migrated_volume()`

#### 3. Backup RPC API (`cinder/backup/rpcapi.py`)

Used for backup operations:
- `backup_rpcapi.continue_backup()` - continuing backup on another service

### Current Outbound RPC Usage in VolumeManager

```python
# cinder/volume/manager.py - Key outbound RPC calls

# Line 973: Rescheduling on failure
self.scheduler_rpcapi.create_volume(...)

# Line 2470-2523: Migration operations
rpcapi = volume_rpcapi.VolumeAPI()
conn = rpcapi.initialize_connection(ctxt, volume, properties)
rpcapi.terminate_connection(ctxt, volume, properties, force=force)
rpcapi.remove_export(ctxt, volume)

# Line 2574-2640: Cross-backend operations
rpcapi.get_capabilities(ctxt, dest_backend, discover=True)
rpcapi.create_volume(ctxt, new_volume, ...)

# Line 2821: Migration completion
rpcapi.update_migrated_volume(ctxt, volume, new_volume, ...)

# Line 5018-5019: Failover completion
rpcapi.failover_completed(context, service, updates)

# Line 5195-5196: Backup continuation
rpcapi = backup_rpcapi.BackupAPI()
rpcapi.continue_backup(ctxt, backup, backup_device)
```

### RPC Transport Lifecycle

The RPC transport is managed globally in `cinder/rpc.py`:

```python
# cinder/rpc.py
TRANSPORT = None  # Global transport for RPC

def init(conf) -> None:
    global TRANSPORT
    TRANSPORT = messaging.get_rpc_transport(conf, ...)

def cleanup():
    global TRANSPORT
    TRANSPORT.cleanup()  # ← Must NOT happen during graceful shutdown
    TRANSPORT = None
```

**Critical Requirement:** The `TRANSPORT` must remain available throughout the graceful shutdown period so that in-flight operations can make outbound RPC calls.

### Coordination Service Considerations

The coordination service (`cinder/coordination.py`) provides distributed locking:

```python
# Current behavior - stops too early
def stop(self) -> None:
    if self.coordination:
        coordination.COORDINATOR.stop()  # ← Releases all locks immediately
```

**Issue:** If coordination stops before in-flight operations complete, any operation holding a distributed lock will have that lock released, potentially allowing another service to interfere.

**Solution:** Delay coordination shutdown until after all operations complete:

```python
# Proposed behavior
def stop(self) -> None:
    # Stop accepting new requests
    self.rpcserver.stop()
    # Do NOT stop coordination here

def wait(self) -> None:
    # Wait for all operations to complete
    self.manager.wait_for_tasks()
    self.rpcserver.wait()
    
    # NOW stop coordination (after all ops complete)
    if self.coordination:
        coordination.COORDINATOR.stop()
```

---

## Host Identity and Service Handover

### The Challenge: Same Host, New Service Instance

In Kubernetes rolling updates, a new pod is started with the **same host identity** before the old pod terminates:

```
Time ──────────────────────────────────────────────────────────►

Pod A (host=volume-backend-1)
├── Running ────────────────┤ SIGTERM │ Draining ─────────│ Exit
                            │         │                    │
Pod B (host=volume-backend-1)          │                    │
                            ├── Starting ─┤ Ready ──────────┴────►
                                          │
                                          ▼
                              New requests go to Pod B
```

### RabbitMQ Consumer Behavior

When multiple services have the same host identity and connect to the same RPC queue:

1. **Topic Queue**: `cinder-volume.volume-backend-1`
   - Both Pod A and Pod B connect as consumers
   - RabbitMQ round-robins messages between consumers
   - **Problem**: New requests might still go to draining Pod A

2. **When Pod A calls `rpcserver.stop()`**:
   - Pod A's consumer is removed from the queue
   - All new messages go to Pod B
   - In-flight messages on Pod A continue processing

### Ensuring Clean Handover

#### 1. Stop Inbound RPC First

```python
def stop(self) -> None:
    # FIRST: Stop accepting new messages
    # This removes us from the RabbitMQ consumer list
    if self.rpcserver is not None:
        self.rpcserver.stop()  # ← Pod B now gets all new messages
    if self.backend_rpcserver:
        self.backend_rpcserver.stop()
    if self.cluster_rpcserver:
        self.cluster_rpcserver.stop()
    
    # Continue with rest of shutdown...
```

#### 2. Stop Heartbeat Reporting

When the service stops sending heartbeats, the scheduler marks it as down:

```python
def report_state(self) -> None:
    if self._draining:
        LOG.info("Service is draining, skipping state report")
        return  # ← Scheduler sees us as "down" after service_down_time
```

**Configuration**: `service_down_time` (default: 60 seconds)

#### 3. Service Database State

The replacement service (Pod B) will:
1. Find existing service record in database
2. Update it with new connection info
3. Start reporting state (heartbeats)
4. Scheduler starts routing requests to Pod B

```python
# cinder/service.py:173-200 - Service initialization
try:
    service_ref = objects.Service.get_by_args(ctxt, host, binary)
    # Existing service found - update it
    service_ref.rpc_current_version = manager_class.RPC_API_VERSION
    service_ref.save()
except exception.NotFound:
    # New service - create record
    self._create_service_ref(ctxt)
```

### Worker Table and Operation Handover

The worker table tracks in-progress operations:

| Column | Description |
|--------|-------------|
| `resource_type` | Volume, Snapshot, etc. |
| `resource_id` | UUID of the resource |
| `status` | Operation status (creating, deleting, etc.) |
| `service_id` | ID of the service performing the operation |

**Scenario**: Pod A is draining with an in-progress operation

1. Pod A has worker entry: `service_id=A, status=creating, resource_id=vol-123`
2. Pod A completes operation, removes worker entry
3. If Pod A crashes before completion:
   - Worker entry remains in DB
   - Pod B runs `CleanableManager.do_cleanup()` on startup
   - Pod B can resume or cleanup the operation

**No Conflict**: The new service (Pod B) will only claim orphaned workers from services that are actually down, using the worker claiming mechanism:

```python
# cinder/manager.py:254-280 - Worker claiming
res = db.worker_claim_for_cleanup(context, service_id, worker)
# This atomically checks if the worker is still owned by original service
# and only claims it if that service hasn't updated it
```

---

## Kubernetes Integration

### Pod Termination Lifecycle

When Kubernetes deletes a pod (rolling update, scale down, node drain):

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Pod marked for termination                               │
│    └── Pod removed from Service endpoints (no new traffic)  │
├─────────────────────────────────────────────────────────────┤
│ 2. PreStop hook executed (if configured)                    │
│    └── Optional: Additional drain logic                     │
├─────────────────────────────────────────────────────────────┤
│ 3. SIGTERM sent to container                                │
│    └── Graceful shutdown period begins                      │
│    └── Inbound RPC stops, outbound RPC continues            │
├─────────────────────────────────────────────────────────────┤
│ 4. terminationGracePeriodSeconds countdown                  │
│    └── Default: 30 seconds (MUST be increased for Cinder)   │
├─────────────────────────────────────────────────────────────┤
│ 5. SIGKILL sent if still running                            │
│    └── Forceful termination - operations interrupted        │
└─────────────────────────────────────────────────────────────┘
```

### Recommended Kubernetes Configuration

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cinder-volume
spec:
  # Use RollingUpdate strategy for zero-downtime updates
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0      # Never reduce capacity during update
      maxSurge: 1            # Start new pod before killing old one
  
  template:
    spec:
      # Allow sufficient time for operations to complete
      # This MUST be longer than the longest expected operation
      terminationGracePeriodSeconds: 300  # 5 minutes
      
      containers:
      - name: cinder-volume
        image: cinder-volume:latest
        
        # Optional: PreStop hook for additional drain logic
        lifecycle:
          preStop:
            exec:
              command:
              - /bin/sh
              - -c
              - |
                # Optional: Log that we're starting to drain
                echo "Starting graceful shutdown..."
                # Small delay to ensure new pod is ready
                sleep 5
        
        # Ensure SIGTERM is sent to cinder-volume process directly
        # not to a shell wrapper
        command:
        - cinder-volume
        args:
        - --config-file=/etc/cinder/cinder.conf
        
        # Readiness probe - new pod won't receive traffic until ready
        readinessProbe:
          exec:
            command:
            - /bin/sh
            - -c
            - "cinder-manage service list --binary cinder-volume | grep -q 'up'"
          initialDelaySeconds: 10
          periodSeconds: 5
```

### Rolling Update Sequence

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Kubernetes creates Pod B (new version)                   │
│    └── Pod B initializes, connects to RabbitMQ              │
│    └── Pod B starts reporting state (heartbeats)            │
├─────────────────────────────────────────────────────────────┤
│ 2. Pod B readiness probe succeeds                           │
│    └── Pod B is now receiving new RPC requests              │
├─────────────────────────────────────────────────────────────┤
│ 3. Kubernetes sends SIGTERM to Pod A                        │
│    └── Pod A calls rpcserver.stop()                         │
│    └── Pod A removed from RabbitMQ consumer list            │
│    └── ALL new requests now go to Pod B                     │
├─────────────────────────────────────────────────────────────┤
│ 4. Pod A drains in-flight operations                        │
│    └── Outbound RPC still works (scheduler, other volumes)  │
│    └── Distributed locks held until operations complete     │
├─────────────────────────────────────────────────────────────┤
│ 5. Pod A operations complete                                │
│    └── Worker entries cleaned up                            │
│    └── Coordination service stopped                         │
│    └── Process exits cleanly                                │
└─────────────────────────────────────────────────────────────┘
```

### Ensuring No New Requests During Drain

#### Level 1: RabbitMQ Consumer Removal

When `rpcserver.stop()` is called:
- oslo.messaging stops pulling messages from the queue
- The consumer is removed from RabbitMQ
- Messages are automatically routed to other consumers (Pod B)

```python
# This happens immediately in Service.stop()
def stop(self) -> None:
    if self.rpcserver is not None:
        self.rpcserver.stop()  # ← Consumer removed from queue
```

#### Level 2: Service State (Heartbeat)

When draining, stop sending heartbeats:
- Scheduler sees service as "down" after `service_down_time`
- Scheduler won't route new operations to this service
- Provides belt-and-suspenders protection

```python
def report_state(self) -> None:
    if self._draining:
        LOG.info("Service draining, not reporting state")
        return
```

#### Level 3: Request Rejection (Defense in Depth)

Optional additional protection in the manager:

```python
def _check_not_draining(self):
    """Reject requests if we're draining."""
    if self._draining:
        raise exception.ServiceUnavailable(
            service="cinder-volume",
            reason="Service is shutting down, retry on another host")
```

---

## Proposed Solution

### Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Enhanced Shutdown Flow                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  SIGTERM ──► Service.stop()                                 │
│                  │                                          │
│                  ├── Set _draining = True                   │
│                  │   (stops heartbeat reporting)            │
│                  │                                          │
│                  ├── manager.signal_shutdown()              │
│                  │   (reject new threadpool tasks)          │
│                  │                                          │
│                  ├── rpcserver.stop() [ALL SERVERS]         │
│                  │   (removes from RabbitMQ consumer list)  │
│                  │   (NEW REQUESTS GO TO OTHER SERVICES)    │
│                  │                                          │
│                  └── oslo.Service.stop(graceful=True)       │
│                      (stops periodic tasks)                 │
│                                                             │
│          ──► Service.wait()                                 │
│                  │                                          │
│                  ├── manager.wait_for_tasks()  [NEW]        │
│                  │   (waits for GreenPool tasks)            │
│                  │   (OUTBOUND RPC STILL WORKS)             │
│                  │                                          │
│                  ├── rpcserver.wait() [ALL SERVERS]         │
│                  │   (waits for in-flight RPC handlers)     │
│                  │   (OUTBOUND RPC STILL WORKS)             │
│                  │                                          │
│                  ├── coordination.stop()  [MOVED HERE]      │
│                  │   (release locks AFTER ops complete)     │
│                  │                                          │
│                  └── oslo.Service.wait()                    │
│                                                             │
│          ──► Clean exit (RPC transport still intact)        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Key Changes

| Change | Purpose |
|--------|---------|
| Move coordination.stop() to wait() | Keep distributed locks during drain |
| Add manager.wait_for_tasks() | Wait for GreenPool tasks |
| Add _draining flag | Stop heartbeat, reject new work |
| Keep RPC TRANSPORT alive | Enable outbound RPC during drain |

---

## Implementation Details

### Phase 1: Wait for ThreadPool Tasks

#### 1.1 Modify ThreadPoolManager

```python
# cinder/manager.py

from eventlet import greenpool
import eventlet
import threading

class ThreadPoolManager(Manager):
    def __init__(self, *args, **kwargs):
        self._tp = greenpool.GreenPool()
        self._shutdown_event = threading.Event()
        super(ThreadPoolManager, self).__init__(*args, **kwargs)

    def _add_to_threadpool(self, func, *args, **kwargs):
        """Spawn a task in the threadpool.
        
        Tasks spawned here will be waited on during graceful shutdown.
        """
        if self._shutdown_event.is_set():
            LOG.warning("Rejecting threadpool task during shutdown: %s",
                        func.__name__)
            return
        self._tp.spawn_n(func, *args, **kwargs)

    def wait_for_tasks(self, timeout=None):
        """Wait for all threadpool tasks to complete.
        
        :param timeout: Maximum time to wait in seconds. None = forever.
        :returns: True if all tasks completed, False if timeout reached.
        """
        running = self._tp.running()
        if running > 0:
            LOG.info("Waiting for %d threadpool tasks to complete...", running)
        
        if timeout:
            with eventlet.Timeout(timeout, False):
                self._tp.waitall()
                LOG.info("All threadpool tasks completed.")
                return True
            LOG.warning("Timeout waiting for threadpool tasks. "
                        "%d tasks still running.", self._tp.running())
            return False
        else:
            self._tp.waitall()
            LOG.info("All threadpool tasks completed.")
            return True

    def signal_shutdown(self):
        """Signal that shutdown is in progress. Reject new threadpool tasks."""
        LOG.info("Shutdown signaled, rejecting new threadpool tasks")
        self._shutdown_event.set()
```

#### 1.2 Modify Service Class

```python
# cinder/service.py

class Service(service.Service):
    """Service object for binaries running on hosts."""
    
    service_id = None

    def __init__(self, host: str, binary: str, topic: str, ...):
        super(Service, self).__init__()
        # ... existing init code ...
        self._draining = False

    def stop(self) -> None:
        """Stop the service gracefully.
        
        This method:
        1. Sets draining state (stops heartbeat)
        2. Signals manager to reject new threadpool tasks
        3. Stops all RPC servers (removes from consumer list)
        4. Does NOT stop coordination (needed during drain)
        5. Does NOT cleanup RPC transport (needed for outbound RPC)
        """
        LOG.info("Initiating graceful shutdown for service %s on host %s",
                 self.binary, self.host)
        
        # Set draining state - stops heartbeat reporting
        self._draining = True
        
        # Signal manager to stop accepting new threadpool tasks
        if hasattr(self.manager, 'signal_shutdown'):
            self.manager.signal_shutdown()
        
        # Stop accepting new RPC messages
        # This removes us from RabbitMQ consumer list
        # New messages will go to other service instances
        try:
            if self.rpcserver is not None:
                LOG.info("Stopping RPC server (topic: %s)", self.topic)
                self.rpcserver.stop()
            if self.backend_rpcserver:
                self.backend_rpcserver.stop()
            if self.cluster_rpcserver:
                self.cluster_rpcserver.stop()
        except Exception:
            LOG.exception("Error stopping RPC servers")

        # NOTE: Do NOT stop coordination here!
        # In-flight operations may still need distributed locks.
        # Coordination will be stopped in wait() after operations complete.

        # NOTE: Do NOT cleanup RPC transport here!
        # In-flight operations may need to make outbound RPC calls.
        
        super(Service, self).stop(graceful=True)

    def wait(self) -> None:
        """Wait for all service operations to complete.
        
        This method:
        1. Waits for manager threadpool tasks
        2. Waits for in-flight RPC handlers
        3. Stops coordination (after all ops complete)
        """
        # Wait for manager's threadpool tasks
        # Outbound RPC is still available during this time
        if hasattr(self.manager, 'wait_for_tasks'):
            timeout = CONF.graceful_shutdown_timeout
            if timeout == 0:
                timeout = None  # 0 means wait forever
            LOG.info("Waiting for manager tasks (timeout=%s seconds)", timeout)
            completed = self.manager.wait_for_tasks(timeout=timeout)
            if not completed:
                LOG.warning("Some manager tasks did not complete within timeout")

        # Wait for RPC servers to finish processing in-flight requests
        # Outbound RPC is still available during this time
        LOG.info("Waiting for in-flight RPC requests to complete...")
        if self.rpcserver:
            self.rpcserver.wait()
        if self.backend_rpcserver:
            self.backend_rpcserver.wait()
        if self.cluster_rpcserver:
            self.cluster_rpcserver.wait()

        # NOW stop coordination - all operations have completed
        # and no longer need distributed locks
        if self.coordination:
            try:
                LOG.info("Stopping coordination service")
                coordination.COORDINATOR.stop()
            except Exception:
                LOG.exception("Error stopping coordination")

        super(Service, self).wait()
        LOG.info("Service %s shutdown complete", self.binary)

    def report_state(self) -> None:
        """Update the state of this service in the datastore."""
        # Don't report state if we're draining
        # This causes scheduler to see us as "down"
        if self._draining:
            LOG.debug("Service is draining, skipping state report")
            return

        if not self.manager.is_working():
            LOG.error('Manager for service %(binary)s %(host)s is '
                      'reporting problems, not sending heartbeat.',
                      {'binary': self.binary, 'host': self.host})
            return
        
        # ... rest of existing report_state code ...

    @property
    def is_draining(self) -> bool:
        """Return True if the service is shutting down."""
        return self._draining
```

### Phase 2: Manager Enhancements (Optional)

#### 2.1 Add Draining Check to VolumeManager

For defense-in-depth, reject new operations at the manager level:

```python
# cinder/volume/manager.py

class VolumeManager(manager.CleanableManager, manager.SchedulerDependentManager):
    
    def __init__(self, ...):
        # ... existing code ...
        self._draining = False

    def signal_shutdown(self):
        """Signal that shutdown is in progress."""
        self._draining = True
        super(VolumeManager, self).signal_shutdown()

    def _check_not_draining(self, operation_name):
        """Raise exception if service is draining.
        
        This is a defense-in-depth check. Normally, the RPC server
        stops accepting messages before this is needed.
        """
        if self._draining:
            LOG.warning("Rejecting %s request - service is draining",
                        operation_name)
            raise exception.ServiceUnavailable(
                service="cinder-volume",
                reason=f"Service is shutting down, cannot process {operation_name}")

    @objects.Volume.set_workers
    def create_volume(self, context, volume, ...):
        self._check_not_draining("create_volume")
        # ... existing code ...

    @objects.Volume.set_workers
    def delete_volume(self, context, volume, ...):
        self._check_not_draining("delete_volume")
        # ... existing code ...

    # Add similar checks to other entry points as needed
```

### Phase 3: Configuration Options

```python
# cinder/common/config.py (or appropriate config module)

from oslo_config import cfg

graceful_shutdown_opts = [
    cfg.IntOpt('graceful_shutdown_timeout',
               default=120,
               min=0,
               help='Maximum time in seconds to wait for operations to '
                    'complete during graceful shutdown. A value of 0 means '
                    'wait indefinitely. This should be less than the '
                    'Kubernetes terminationGracePeriodSeconds. '
                    'Default: 120 seconds.'),
    cfg.BoolOpt('graceful_shutdown_reject_new_operations',
                default=True,
                help='If True, reject new operations at the manager level '
                     'during shutdown (defense in depth). If False, rely '
                     'only on RPC server stop to prevent new work.'),
]

CONF = cfg.CONF
CONF.register_opts(graceful_shutdown_opts)
```

---

## Configuration Options

### Cinder Configuration (`cinder.conf`)

```ini
[DEFAULT]
# Maximum time to wait for operations during shutdown (seconds)
# Should be less than Kubernetes terminationGracePeriodSeconds
# 0 = wait indefinitely (not recommended in Kubernetes)
graceful_shutdown_timeout = 120

# Reject new operations at manager level during shutdown
# (defense in depth - normally RPC server stop is sufficient)
graceful_shutdown_reject_new_operations = true

# How long before a service is considered "down" (scheduler perspective)
# The draining service stops heartbeats, so it will appear down
# after this interval
service_down_time = 60
```

### oslo.service Configuration (already available)

```ini
[DEFAULT]
# oslo.service graceful shutdown timeout
# This is the overall process-level timeout (SIGALRM)
# Should be slightly less than Kubernetes terminationGracePeriodSeconds
graceful_shutdown_timeout = 180
```

### Kubernetes Deployment Configuration

```yaml
spec:
  template:
    spec:
      # Must be greater than oslo.service graceful_shutdown_timeout
      terminationGracePeriodSeconds: 300

      containers:
      - name: cinder-volume
        env:
        # Optional: Override via environment variable
        - name: OS_DEFAULT__GRACEFUL_SHUTDOWN_TIMEOUT
          value: "120"
```

### Recommended Timeout Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│  Kubernetes terminationGracePeriodSeconds: 300s             │
│    │                                                        │
│    └── oslo.service graceful_shutdown_timeout: 180s         │
│          │                                                  │
│          └── cinder graceful_shutdown_timeout: 120s         │
│                │                                            │
│                └── Individual operation timeouts            │
│                                                             │
│  SIGKILL sent if process still running after 300s           │
└─────────────────────────────────────────────────────────────┘
```

---

## Testing Strategy

### Unit Tests

```python
# cinder/tests/unit/test_graceful_shutdown.py

class TestGracefulShutdown(test.TestCase):
    
    def test_threadpool_manager_wait_for_tasks(self):
        """Test that wait_for_tasks waits for spawned tasks."""
        manager = ThreadPoolManager()
        completed = []
        
        def slow_task():
            eventlet.sleep(0.5)
            completed.append(True)
        
        manager._add_to_threadpool(slow_task)
        manager.wait_for_tasks()
        
        self.assertEqual([True], completed)

    def test_threadpool_manager_rejects_tasks_after_shutdown(self):
        """Test that new tasks are rejected during shutdown."""
        manager = ThreadPoolManager()
        manager.signal_shutdown()
        
        executed = []
        manager._add_to_threadpool(lambda: executed.append(True))
        
        self.assertEqual([], executed)

    def test_service_stop_sets_draining(self):
        """Test that service.stop() sets draining flag."""
        service = self._create_test_service()
        
        self.assertFalse(service.is_draining)
        service.stop()
        self.assertTrue(service.is_draining)

    def test_service_draining_skips_heartbeat(self):
        """Test that draining service doesn't report state."""
        service = self._create_test_service()
        service._draining = True
        
        with mock.patch.object(service, 'manager') as mock_manager:
            service.report_state()
            # Should not check is_working() since we skip early
            mock_manager.is_working.assert_not_called()

    def test_coordination_stops_after_wait(self):
        """Test that coordination stops in wait(), not stop()."""
        service = self._create_test_service()
        
        with mock.patch('cinder.coordination.COORDINATOR') as mock_coord:
            service.stop()
            mock_coord.stop.assert_not_called()
            
            service.wait()
            mock_coord.stop.assert_called_once()

    def test_outbound_rpc_works_during_drain(self):
        """Test that outbound RPC calls work during shutdown."""
        # Verify RPC transport is not cleaned up during stop/wait
        ...
```

### Integration Tests

```python
# cinder/tests/functional/test_graceful_shutdown.py

class TestGracefulShutdownFunctional(test.TestCase):
    
    def test_volume_create_completes_during_shutdown(self):
        """Test that in-flight volume create completes during shutdown."""
        # Start a volume create operation
        # Send SIGTERM to service
        # Verify volume create completed successfully
        # Verify outbound RPC (scheduler notification) succeeded
        
    def test_no_new_operations_accepted_during_drain(self):
        """Test that new operations are rejected during drain."""
        # Put service in draining state
        # Attempt to create volume
        # Verify operation is rejected or routed elsewhere

    def test_migration_outbound_rpc_during_drain(self):
        """Test that migration can make outbound RPC during drain."""
        # Start a volume migration
        # Send SIGTERM during migration
        # Verify outbound RPC to destination volume service works
        # Verify migration completes successfully

    def test_new_service_handles_orphaned_workers(self):
        """Test that replacement service handles orphaned workers."""
        # Start operation on service A
        # Kill service A without completing operation
        # Start service B with same host identity
        # Verify service B cleans up orphaned worker
```

### Manual Testing Procedure

```bash
# Terminal 1: Start cinder-volume with debug logging
cinder-volume --config-file /etc/cinder/cinder.conf --debug

# Terminal 2: Start a long-running operation
openstack volume create --size 100 --image cirros test-volume

# Terminal 3: While operation is in progress, send SIGTERM
kill -SIGTERM $(pgrep -f cinder-volume)

# Observe logs in Terminal 1:
# Expected sequence:
# 1. "Initiating graceful shutdown for service cinder-volume..."
# 2. "Stopping RPC server (topic: cinder-volume)"
# 3. "Shutdown signaled, rejecting new threadpool tasks"
# 4. "Waiting for manager tasks..."
# 5. (Volume creation continues, makes scheduler RPC calls)
# 6. "All threadpool tasks completed."
# 7. "Waiting for in-flight RPC requests to complete..."
# 8. "Stopping coordination service"
# 9. "Service cinder-volume shutdown complete"

# Terminal 2: Verify volume was created successfully
openstack volume show test-volume
# Should show status: available
```

### Kubernetes Rolling Update Test

```bash
# 1. Deploy cinder-volume
kubectl apply -f cinder-volume-deployment.yaml

# 2. Start a long-running operation
openstack volume create --size 100 test-volume

# 3. Trigger rolling update during operation
kubectl set image deployment/cinder-volume \
  cinder-volume=cinder-volume:new-version

# 4. Watch pod status
kubectl get pods -w

# 5. Verify operation completed
openstack volume show test-volume
# Should show status: available, not error
```

---

## Rollout Plan

### Phase 1: Core Implementation (Low Risk)

**Changes:**
1. Add `wait_for_tasks()` to `ThreadPoolManager`
2. Add `signal_shutdown()` to `ThreadPoolManager`
3. Modify `Service.stop()` - set draining, signal manager, stop RPC servers
4. Modify `Service.wait()` - wait for tasks, then stop coordination
5. Modify `Service.report_state()` - skip when draining
6. Add configuration options
7. Add unit tests

**Files Modified:**
- `cinder/manager.py`
- `cinder/service.py`
- `cinder/common/config.py`
- `cinder/tests/unit/test_graceful_shutdown.py` (new)

**Estimated effort:** 3-4 days

**Risk:** Low - adds waiting behavior, moves coordination stop timing

**Rollback:** Set `graceful_shutdown_timeout = 1` to minimize wait time

### Phase 2: Manager-Level Rejection (Medium Risk)

**Changes:**
1. Add `_check_not_draining()` to VolumeManager
2. Add checks to key entry points (create_volume, delete_volume, etc.)
3. Add integration tests

**Files Modified:**
- `cinder/volume/manager.py`
- `cinder/tests/functional/test_graceful_shutdown.py` (new)

**Estimated effort:** 2-3 days

**Risk:** Medium - may reject operations in edge cases

**Rollback:** Set `graceful_shutdown_reject_new_operations = false`

### Phase 3: Documentation and Kubernetes Integration

**Changes:**
1. Document recommended Kubernetes configurations
2. Create example deployment manifests
3. Add troubleshooting guide
4. Update operator documentation

**Estimated effort:** 1-2 days

**Risk:** Low - documentation only

### Rollback Plan

If issues are discovered:

1. **Quick mitigation:** Set `graceful_shutdown_timeout = 1`
2. **Phase 1 rollback:** Revert service.py changes
3. **Phase 2 rollback:** Set `graceful_shutdown_reject_new_operations = false`
4. **Kubernetes mitigation:** Reduce `terminationGracePeriodSeconds`

---

## Appendix A: Relevant Source Files

| File | Description |
|------|-------------|
| `cinder/service.py:124-458` | Service class with start/stop/wait |
| `cinder/manager.py:163-169` | ThreadPoolManager with GreenPool |
| `cinder/volume/manager.py:232+` | VolumeManager implementation |
| `cinder/objects/cleanable.py` | Worker tracking mechanism |
| `cinder/cmd/volume.py:181-209` | Volume service entry point |
| `cinder/rpc.py` | RPC transport management |
| `cinder/coordination.py` | Distributed locking |
| `cinder/scheduler/rpcapi.py` | Scheduler RPC client |
| `cinder/volume/rpcapi.py` | Volume RPC client |

## Appendix B: Outbound RPC Call Sites in VolumeManager

| Location | RPC Target | Purpose |
|----------|------------|---------|
| `manager.py:973` | Scheduler | Reschedule failed create |
| `manager.py:2470` | Volume | Migration initialize_connection |
| `manager.py:2522` | Volume | Migration terminate_connection |
| `manager.py:2574` | Volume | Get capabilities |
| `manager.py:2640` | Volume | Create volume on destination |
| `manager.py:2821` | Volume | Update migrated volume |
| `manager.py:5018` | Volume | Failover completed |
| `manager.py:5195` | Backup | Continue backup |

## Appendix C: Signal Handling in Kubernetes

```python
# Ensuring SIGTERM reaches cinder-volume process

# Option 1: Direct command (recommended)
command: ["cinder-volume"]
args: ["--config-file=/etc/cinder/cinder.conf"]

# Option 2: Shell wrapper with exec
command: ["/bin/sh", "-c"]
args: ["exec cinder-volume --config-file=/etc/cinder/cinder.conf"]

# Option 3: Use tini as init process
# In Dockerfile:
# RUN apt-get install -y tini
# ENTRYPOINT ["/usr/bin/tini", "--"]
# CMD ["cinder-volume", "--config-file=/etc/cinder/cinder.conf"]
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-XX-XX | - | Initial draft |
| 1.1 | 2024-XX-XX | - | Added outbound RPC requirements |
| 1.2 | 2024-XX-XX | - | Added host identity and service handover section |

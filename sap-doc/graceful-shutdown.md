# Graceful Shutdown for Cinder Services

**Commit:** [de113e65ed](https://github.com/sapcc/cinder/commit/de113e65ed) - `[SAP] Implement graceful shutdown for cinder services`

## Overview

Cinder services run as Kubernetes containers in the SAP deployment model. When a pod is stopped (for example during a rolling update), Kubernetes sends `SIGTERM` to the container.

By default, Cinder services exit as soon as they receive that signal. For cinder-volume and cinder-backup this is a problem: those services run long operations (volume create/clone/delete, backup create/restore) that can be interrupted mid-way, leaving volumes and backups in an inconsistent state.

Graceful shutdown lets cinder-volume and cinder-backup drain in-flight operations before exiting, so that operations which were already running when the termination signal arrived are allowed to complete.

## Scope

Graceful shutdown is implemented for **two** services:

- **cinder-volume**
- **cinder-backup**

**cinder-api** and **cinder-scheduler** are **not** covered by graceful shutdown. They terminate as soon as they receive the termination signal, without waiting for in-flight work. This is intentional:

- cinder-api is stateless with respect to long-running operations. Clients retry API calls that are interrupted by a shutdown, and the API service is restarted by the orchestrator.
- cinder-scheduler is stateless. Scheduling decisions are made on demand from the message queue, so an interrupted schedule is simply re-issued when a healthy scheduler picks up the message.

Nothing in the graceful shutdown mechanism should be relied upon for these two services.

## How It Works

When cinder-volume or cinder-backup receives a termination signal, the service shuts down in three phases:

1. **Stop accepting new work.** New RPC calls are rejected so that the scheduler routes new operations to healthy backends, and new background tasks are refused.
2. **Drain in-flight operations.** The service waits for the RPC handler greenthreads that were already running to complete. The wait is bounded by `graceful_shutdown_timeout` (default 120 seconds).
3. **Clean up and exit.** Coordination is stopped, the thread pool is drained, and the process exits naturally.

While the service is draining, the following mechanisms keep in-flight work safe across a rolling update:

- **Worker heartbeats.** In-flight operations keep touching the worker DB entries (and, for backups, the backup `updated_at`) so that a newly started pod does not consider them stale and reset them to error.
- **Freshness checks.** Startup cleanup skips worker entries and backups that were updated within `service_down_time`.

## Configuration

The drain wait is configured with `graceful_shutdown_timeout`. The default of 120 seconds is set in `cinder/service.py`; the option itself is provided by oslo.service.

## Eventlet dependency

This graceful shutdown implementation targets SAP's Epoxy release, which still runs cinder under eventlet (all cinder binaries call `eventlet.monkey_patch()`). The drain therefore relies on the eventlet executor's GreenPool (`pool.waitall()`), and the heartbeats run as eventlet greenthreads.

Upstream has deprecated the eventlet executor and plans to remove it in favor of the threading executor. The threading migration for graceful shutdown will be handled in the upstream effort, not in this Epoxy-targeted change.

## Deployment Requirements

In the SAP deployment model these services run as Kubernetes containers. Cinder itself has no knowledge of Kubernetes; the graceful shutdown mechanism relies only on receiving a termination signal. The following deployment settings are required for the mechanism to work:

- The container entrypoint must forward the termination signal to the Cinder process (for example `dumb-init --single-child`).
- The pod must allow enough time for in-flight operations to complete (`terminationGracePeriodSeconds` should be comfortably larger than `graceful_shutdown_timeout`).

These are deployment concerns, not Cinder code concerns.

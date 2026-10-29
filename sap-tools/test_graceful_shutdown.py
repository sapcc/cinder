#!/usr/bin/env python3
"""
Graceful Shutdown Integration Test for Cinder Volume and Backup Services.

Tests that the graceful shutdown patch works correctly in qa-de-1 by:
1. Deleting pods to trigger SIGTERM (simulates rolling update)
2. Verifying shutdown log sequence appears in pod logs
3. Verifying in-flight operations complete successfully during drain

Outputs:
  - Console output with evidence timeline
  - Per-test raw log files in output directory
  - Markdown report summarizing all results with evidence

Prerequisites:
  - Patch deployed via: cc-autodeploy deploy cinder-qa-de-1
  - kubectl context 'qa-de-1' configured
  - openstack CLI available (clouds.yaml with 'qa-de-1' cloud)
  - terminationGracePeriodSeconds=900 already active on pods

Usage:
  python3 sap-tools/test_graceful_shutdown.py
  python3 sap-tools/test_graceful_shutdown.py --test test_idle_shutdown
  python3 sap-tools/test_graceful_shutdown.py --test test_inflight_volume_create
  python3 sap-tools/test_graceful_shutdown.py --test test_inflight_backup
  python3 sap-tools/test_graceful_shutdown.py --test test_scheduler_reroutes
  python3 sap-tools/test_graceful_shutdown.py --list
"""

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


# =============================================================================
# Configuration
# =============================================================================
# All environment-specific values are overridable via env vars (GS_*), with
# qa-de-1 defaults. See sap-tools/README.md for how to target another region.

KUBE_CONTEXT = os.environ.get("GS_KUBE_CONTEXT", "qa-de-1")
KUBE_NAMESPACE = os.environ.get("GS_KUBE_NAMESPACE", "monsoon3")
OS_CLOUD = os.environ.get("GS_OS_CLOUD", "qa-de-1")
OPENSTACK_BIN = os.environ.get("GS_OPENSTACK_BIN",
                               "/Users/I530566/.sap-py3/bin/openstack")
HAMMER_BIN = os.environ.get("GS_HAMMER_BIN",
                            "/Users/I530566/.sap-py3/bin/hammer")

VOLUME_DEPLOYMENT = os.environ.get("GS_VOLUME_DEPLOYMENT",
                                   "cinder-volume-vmware-vc-a-0")
VOLUME_CONTAINER = VOLUME_DEPLOYMENT
BACKUP_DEPLOYMENT = os.environ.get("GS_BACKUP_DEPLOYMENT",
                                   "cinder-volume-backup-vmware-vc-a-0")
BACKUP_CONTAINER = BACKUP_DEPLOYMENT

# Image for volume-from-image test (Debian 11 vmdk, ~800MB)
TEST_IMAGE_ID = os.environ.get("GS_TEST_IMAGE_ID",
                               "7b92aab0-d95d-4319-8146-0f9b7a7f80ae")
TEST_VOLUME_SIZE = int(os.environ.get("GS_TEST_VOLUME_SIZE", "16"))  # GB
TEST_VOLUME_TYPE = os.environ.get("GS_TEST_VOLUME_TYPE", "vmware")

# Pre-created resources for backup/restore tests (avoid 5+ min setup per run)
# These must exist and be 'available' before running backup/restore tests.
PRECREATED_SOURCE_VOLUME_ID = os.environ.get(
    "GS_PRECREATED_SOURCE_VOLUME_ID",
    "04e49377-8470-4341-82a7-404c9fe3287f")
PRECREATED_BACKUP_ID = os.environ.get(
    "GS_PRECREATED_BACKUP_ID",
    "29b80a02-0cfc-473f-aae7-e410ce651eae")

# Timeouts
POD_TERMINATE_TIMEOUT = 120
POD_READY_TIMEOUT = 180
VOLUME_CREATE_TIMEOUT = 600
BACKUP_CREATE_TIMEOUT = 900
POLL_INTERVAL = 5
LOG_WAIT_AFTER_TERMINATE = 10

# Expected log patterns during graceful shutdown (in order)
# Epoxy (PR #358): SIG_IGN is installed silently (no log line). The
# canonical markers are the "Initiating graceful shutdown" line
# (Phase 1 starts, rpcserver.stop() deregisters consumers), the
# manager's "Shutdown signaled" line (drain begins), and the final
# "Service ... shutdown complete" line (Phase 3 teardown done).
# All three are emitted by the current Epoxy code.
SHUTDOWN_LOG_SEQUENCE = [
    "Initiating graceful shutdown",
    "Shutdown signaled, rejecting new threadpool tasks",
    "Service cinder-volume shutdown complete",
]

SHUTDOWN_LOG_SEQUENCE_WITH_TASKS = [
    "Initiating graceful shutdown",
    "Shutdown signaled, rejecting new threadpool tasks",
    "Service cinder-volume shutdown complete",
]

SHUTDOWN_LOG_SEQUENCE_BACKUP = [
    "Initiating graceful shutdown",
    "Shutdown signaled, rejecting new threadpool tasks",
    "Service cinder-backup shutdown complete",
]


def _apply_config(os_cloud: str, kube_context: str,
                  volume_deployment: str, backup_deployment: str,
                  volume_type: str = "", volume_size: int = 0) -> None:
    """Apply CLI overrides to module-level config variables."""
    global OS_CLOUD, KUBE_CONTEXT, VOLUME_DEPLOYMENT, VOLUME_CONTAINER
    global BACKUP_DEPLOYMENT, BACKUP_CONTAINER, TEST_VOLUME_TYPE, TEST_VOLUME_SIZE
    OS_CLOUD = os_cloud
    KUBE_CONTEXT = kube_context
    VOLUME_DEPLOYMENT = volume_deployment
    VOLUME_CONTAINER = volume_deployment
    BACKUP_DEPLOYMENT = backup_deployment
    BACKUP_CONTAINER = backup_deployment
    if volume_type:
        TEST_VOLUME_TYPE = volume_type
    if volume_size:
        TEST_VOLUME_SIZE = volume_size


# =============================================================================
# Output Directory & Report
# =============================================================================

OUTPUT_DIR: Optional[Path] = None


def init_output_dir() -> Path:
    """Create timestamped output directory for test artifacts."""
    global OUTPUT_DIR
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    # Place output relative to the script's own directory
    script_dir = Path(__file__).resolve().parent
    OUTPUT_DIR = script_dir / "test-results" / f"graceful-shutdown-{timestamp}"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Output directory: {OUTPUT_DIR}")
    return OUTPUT_DIR


def save_log_file(filename: str, content: str) -> Path:
    """Save raw log content to a file in the output directory."""
    if not OUTPUT_DIR:
        return Path("/dev/null")
    path = OUTPUT_DIR / filename
    path.write_text(content)
    return path


# =============================================================================
# Helpers
# =============================================================================

def run_cmd(cmd: list[str], timeout: int = 60, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode != 0:
        print(f"  STDERR: {result.stderr.strip()}")
        raise RuntimeError(
            f"Command failed (rc={result.returncode}): {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return result


def kubectl(*args, timeout: int = 60, check: bool = True) -> subprocess.CompletedProcess:
    """Run a kubectl command against qa-de-1."""
    cmd = ["kubectl", "--context", KUBE_CONTEXT, "-n", KUBE_NAMESPACE] + list(args)
    return run_cmd(cmd, timeout=timeout, check=check)


def openstack(*args, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    """Run an openstack CLI command."""
    cmd = [OPENSTACK_BIN, "--os-cloud", OS_CLOUD] + list(args)
    return run_cmd(cmd, timeout=timeout, check=check)


def openstack_admin(*args, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    """Run an openstack CLI command with admin credentials.

    Uses ccloud-multitool to get admin credentials inline.
    """
    # Build the shell command that sets up admin env and runs openstack
    shell_cmd = (
        'source ~/.sap-py3/bin/activate && '
        f'eval "$(ccloud-multitool {KUBE_CONTEXT})" && '
        'eval "$(ccloud-multitool admin)" && '
        'openstack ' + ' '.join(
            f'"{a}"' if ' ' in a else a for a in args)
    )
    cmd = ["zsh", "-c", shell_cmd]
    print(f"  $ openstack (admin) {' '.join(args)}")
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout, check=False)


def get_pod_for_deployment(deployment: str) -> Optional[str]:
    """Get the running pod name for a deployment."""
    result = kubectl(
        "get", "pods",
        "-l", f"name={deployment}",
        "-o", "jsonpath={.items[?(@.status.phase=='Running')].metadata.name}",
    )
    pods = result.stdout.strip().split()
    if not pods or pods == ['']:
        result = kubectl(
            "get", "pods",
            "--field-selector", "status.phase=Running",
            "-o", "jsonpath={.items[*].metadata.name}",
        )
        all_pods = result.stdout.strip().split()
        pods = [p for p in all_pods if deployment in p]
    return pods[0] if pods else None


def wait_for_new_pod_ready(deployment: str, old_pod: str,
                           timeout: int = POD_READY_TIMEOUT) -> Optional[str]:
    """Wait for a new pod (different from old_pod) to become Ready."""
    print(f"  Waiting for new pod for {deployment} (timeout={timeout}s)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = kubectl(
            "get", "pods",
            "-o", "jsonpath={range .items[*]}{.metadata.name} {.status.phase} "
                  "{.status.conditions[?(@.type=='Ready')].status}{'\\n'}{end}",
            check=False,
        )
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) >= 3:
                name, phase, ready = parts[0], parts[1], parts[2]
                if (deployment in name and name != old_pod
                        and phase == "Running" and ready == "True"):
                    print(f"  New pod ready: {name}")
                    return name
        time.sleep(5)
    print(f"  WARNING: No new ready pod found after {timeout}s")
    return None


def start_log_stream(pod_name: str, container: str, output_file: Path) -> subprocess.Popen:
    """Start streaming pod logs to a file in the background.

    Must be started BEFORE deleting the pod so we capture the entire
    shutdown sequence. The process will exit when the container terminates.
    Returns the Popen handle (call .wait() or .terminate() when done).
    """
    cmd = [
        "kubectl", "--context", KUBE_CONTEXT, "-n", KUBE_NAMESPACE,
        "logs", "-f", pod_name, "-c", container,
    ]
    print(f"  $ {' '.join(cmd)} > {output_file.name}")
    fh = open(output_file, "w")
    proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.DEVNULL)
    # Give it a moment to connect
    time.sleep(1)
    return proc


def stop_log_stream(proc: subprocess.Popen, timeout: int = 10) -> None:
    """Wait for log stream to finish (container exited) or terminate it."""
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait(timeout=5)


def get_pod_logs(pod_name: str, container: str, previous: bool = False,
                 since_seconds: Optional[int] = None) -> str:
    """Get logs from a pod/container."""
    args = ["logs", pod_name, "-c", container]
    if previous:
        args.append("--previous")
    if since_seconds:
        args.extend(["--since", f"{since_seconds}s"])
    result = kubectl(*args, timeout=120, check=False)
    return result.stdout


def get_volume_status(volume_id: str) -> str:
    """Get current volume status."""
    result = openstack("volume", "show", volume_id, "-f", "json", check=False)
    if result.returncode != 0:
        return "unknown"
    data = json.loads(result.stdout)
    return data.get("status", "unknown")


def get_backup_status(backup_id: str) -> str:
    """Get current backup status."""
    result = openstack("volume", "backup", "show", backup_id, "-f", "json", check=False)
    if result.returncode != 0:
        return "unknown"
    data = json.loads(result.stdout)
    return data.get("status", "unknown")


def create_backup(volume_id: str, name: str) -> Optional[str]:
    """Create a backup of a volume and return backup ID."""
    result = openstack(
        "volume", "backup", "create",
        "--name", name,
        volume_id,
        "-f", "json",
        check=False,
    )
    if result.returncode != 0:
        print(f"  ERROR: backup create failed: {result.stderr}")
        return None
    data = json.loads(result.stdout)
    return data.get("id")


def create_volume_from_image(name: str,
                             hint_same_host: Optional[str] = None) -> Optional[str]:
    """Create a volume from image and return volume ID.

    If hint_same_host is provided, passes --hint same_host=<volume_id> to
    force the scheduler to place this volume on the same backend.
    """
    args = [
        "volume", "create",
        "--image", TEST_IMAGE_ID,
        "--size", str(TEST_VOLUME_SIZE),
        "--type", TEST_VOLUME_TYPE,
    ]
    if hint_same_host:
        args.extend(["--hint", f"same_host={hint_same_host}"])
    args.extend(["-f", "json", name])
    result = openstack(*args)
    data = json.loads(result.stdout)
    return data.get("id")


def create_volume(name: str, size: int = 1) -> Optional[str]:
    """Create a simple empty volume and return volume ID."""
    result = openstack(
        "volume", "create",
        "--size", str(size),
        "--type", TEST_VOLUME_TYPE,
        "-f", "json",
        name,
    )
    data = json.loads(result.stdout)
    return data.get("id")


def get_volume_host(volume_id: str) -> Optional[str]:
    """Get the volume's host using hammer (reads directly from DB).

    Returns the full host string like 'cinder-volume-vmware-vc-a-0@vmware_fcd#pool'.
    Returns None if host cannot be determined.
    """
    cmd = [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "volume-show",
           volume_id, "--no-color"]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=90,
        env={**os.environ, "COLUMNS": "300"},
    )
    if result.returncode != 0:
        print(f"  WARNING: hammer failed: {result.stderr.strip()}")
        return None
    for line in result.stdout.split("\n"):
        if "│ host" in line and "│" in line:
            # Parse: "│ host  │ cinder-volume-vmware-vc-a-0@vmware_fcd#pool │"
            parts = line.split("│")
            if len(parts) >= 3:
                host = parts[2].strip()
                if host and host != "None":
                    return host
    return None


def host_to_deployment(host: str) -> str:
    """Convert a cinder volume host to a k8s deployment name.

    'cinder-volume-vmware-vc-a-0@vmware_fcd#pool' -> 'cinder-volume-vmware-vc-a-0'
    """
    return host.split("@")[0]


def delete_volume(volume_id: str) -> None:
    """Best-effort volume cleanup."""
    print(f"  Cleaning up volume {volume_id[:8]}...")
    openstack("volume", "delete", volume_id, "--force", check=False)


def delete_backup(backup_id: str) -> None:
    """Best-effort backup cleanup."""
    print(f"  Cleaning up backup {backup_id[:8]}...")
    openstack("volume", "backup", "delete", backup_id, "--force", check=False)


def restore_backup(backup_id: str, name: str) -> Optional[str]:
    """Restore a backup to a new volume and return the new volume ID."""
    result = openstack(
        "volume", "backup", "restore",
        backup_id, name,
        "-f", "json",
        check=False,
    )
    if result.returncode != 0:
        print(f"  ERROR: backup restore failed: {result.stderr}")
        return None
    data = json.loads(result.stdout)
    return data.get("volume_id") or data.get("id")


def get_backup_host(backup_id: str) -> Optional[str]:
    """Get the backup's host using hammer (reads directly from DB).

    Returns the host string like 'cinder-backup-vc-b-2'.
    """
    cmd = [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "backup-show",
           backup_id, "--no-color"]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=90,
        env={**os.environ, "COLUMNS": "300"},
    )
    if result.returncode != 0:
        print(f"  WARNING: hammer failed: {result.stderr.strip()}")
        return None
    for line in result.stdout.split("\n"):
        if "│ host" in line and "│" in line:
            parts = line.split("│")
            if len(parts) >= 3:
                host = parts[2].strip()
                if host and host != "None":
                    return host
    return None


def backup_host_to_deployment(backup_host: str) -> str:
    """Convert a cinder backup host to a k8s deployment name.

    'cinder-backup-vc-b-2' -> 'cinder-volume-backup-vmware-vc-b-2'
    """
    # Strip 'cinder-backup-' prefix, get the vc identifier
    vc_part = backup_host.replace("cinder-backup-", "")
    return f"cinder-volume-backup-vmware-{vc_part}"


def create_snapshot(volume_id: str, name: str) -> Optional[str]:
    """Create a snapshot of a volume and return snapshot ID."""
    result = openstack(
        "volume", "snapshot", "create",
        "--volume", volume_id,
        "-f", "json",
        name,
        check=False,
    )
    if result.returncode != 0:
        print(f"  ERROR: snapshot create failed: {result.stderr}")
        return None
    data = json.loads(result.stdout)
    return data.get("id")


def get_snapshot_status(snapshot_id: str) -> str:
    """Get current snapshot status."""
    result = openstack("volume", "snapshot", "show", snapshot_id, "-f", "json", check=False)
    if result.returncode != 0:
        return "unknown"
    data = json.loads(result.stdout)
    return data.get("status", "unknown")


def poll_snapshot_status(snapshot_id: str, target_statuses: list[str],
                         timeout: int = 300) -> str:
    """Poll snapshot status until it reaches a target."""
    deadline = time.time() + timeout
    last_status = None
    while time.time() < deadline:
        status = get_snapshot_status(snapshot_id)
        if status != last_status:
            print(f"  Snapshot {snapshot_id[:8]}... status: {status} [{time.strftime('%H:%M:%S')}]")
            last_status = status
        if status in target_statuses:
            return status
        if status in ("error", "error_deleting"):
            return status
        time.sleep(POLL_INTERVAL)
    return last_status or "timeout"


def delete_snapshot(snapshot_id: str) -> None:
    """Best-effort snapshot cleanup."""
    print(f"  Cleaning up snapshot {snapshot_id[:8]}...")
    openstack("volume", "snapshot", "delete", snapshot_id, "--force", check=False)


def create_volume_from_volume(source_volume_id: str, name: str, size: int = 0) -> Optional[str]:
    """Clone a volume and return new volume ID."""
    args = ["volume", "create", "--source", source_volume_id, "-f", "json"]
    if size:
        args.extend(["--size", str(size)])
    args.append(name)  # positional argument
    result = openstack(*args, check=False)
    if result.returncode != 0:
        print(f"  ERROR: volume clone failed: {result.stderr}")
        return None
    data = json.loads(result.stdout)
    return data.get("id")


def poll_volume_status(volume_id: str, target_statuses: list[str],
                       timeout: int = VOLUME_CREATE_TIMEOUT,
                       fail_statuses: list[str] = None) -> str:
    """Poll volume status until it reaches a target or fails."""
    if fail_statuses is None:
        fail_statuses = ["error", "error_deleting"]
    deadline = time.time() + timeout
    last_status = None
    while time.time() < deadline:
        status = get_volume_status(volume_id)
        if status != last_status:
            print(f"  Volume {volume_id[:8]}... status: {status}")
            last_status = status
        if status in target_statuses:
            return status
        if status in fail_statuses:
            return status
        time.sleep(POLL_INTERVAL)
    return last_status or "timeout"


def poll_backup_status(backup_id: str, target_statuses: list[str],
                       timeout: int = BACKUP_CREATE_TIMEOUT,
                       fail_statuses: list[str] = None) -> str:
    """Poll backup status until it reaches a target or fails."""
    if fail_statuses is None:
        fail_statuses = ["error"]
    deadline = time.time() + timeout
    last_status = None
    while time.time() < deadline:
        status = get_backup_status(backup_id)
        if status != last_status:
            print(f"  Backup {backup_id[:8]}... status: {status}")
            last_status = status
        if status in target_statuses:
            return status
        if status in fail_statuses:
            return status
        time.sleep(POLL_INTERVAL)
    return last_status or "timeout"


# =============================================================================
# Evidence Extraction
# =============================================================================

import re

# Cinder log timestamp pattern: "2026-05-13 20:24:49,738 ..."
_LOG_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})')


def _extract_timestamp(log_line: str) -> str:
    """Extract the timestamp from a cinder log line. Returns '' if not found."""
    m = _LOG_TS_RE.match(log_line.strip())
    if m:
        return m.group(1)
    return ""


def _find_log_line(logs: str, search_terms: list[str],
                   after_pos: int = 0) -> tuple[str, str, int]:
    """Find the first log line containing any of the search terms.

    Returns (timestamp, full_line_trimmed, position_in_logs).
    Returns ('', '', -1) if not found.
    """
    lines = logs[after_pos:].split("\n")
    pos = after_pos
    for line in lines:
        if any(term in line for term in search_terms):
            ts = _extract_timestamp(line)
            trimmed = line.strip()
            if len(trimmed) > 200:
                trimmed = trimmed[:200] + "..."
            return ts, trimmed, pos
        pos += len(line) + 1
    return "", "", -1

def extract_evidence_lines(logs: str, search_terms: list[str],
                           max_line_len: int = 200) -> list[str]:
    """Find log lines matching any of the search terms."""
    results = []
    for line in logs.split("\n"):
        if any(term in line for term in search_terms):
            trimmed = line.strip()
            if len(trimmed) > max_line_len:
                trimmed = trimmed[:max_line_len] + "..."
            results.append(trimmed)
    return results


def build_evidence(old_pod_logs: str, new_pod_logs: str, pod_name: str,
                   new_pod_name: str, resource_id: str,
                   operation: str, final_status: str) -> list[tuple[str, str]]:
    """Build an evidence timeline from collected logs.

    Returns list of (label, detail) tuples representing the narrative:
      1. Pod was asked to do X
      2. Pod started doing X
      3. Pod was asked to die (SIGTERM)
      4. New pod came up
      5. Old pod completed the operation
    """
    evidence = []
    short_id = resource_id[:8] if resource_id else ""

    if old_pod_logs:
        lines = old_pod_logs.split("\n")

        # 1. Operation was accepted
        op_terms = {
            "volume_create": ["create_volume", "Creating volume"],
            "backup_create": ["create_backup", "Creating backup", "BackupManager"],
        }
        terms = op_terms.get(operation, ["create_volume"])
        for line in lines:
            if any(t in line for t in terms):
                evidence.append((
                    f"1. OLD POD ({pod_name}): Operation '{operation}' accepted",
                    line.strip()[:200]
                ))
                break
        if not any("1." in e[0] for e in evidence):
            evidence.append((
                f"1. OLD POD ({pod_name}): Operation '{operation}' was dispatched to this pod",
                f"(volume/backup {short_id} was created targeting this pod)"
            ))

        # 2. Operation started executing
        exec_terms = ["copy_image_to_volume", "downloading", "_copy_image_data",
                      "image", "ChunkedBackupDriver", "backup_volume", "VolumeDriver"]
        for line in lines:
            if any(t in line for t in exec_terms) and short_id in line:
                evidence.append((
                    f"2. OLD POD: Operation in progress",
                    line.strip()[:200]
                ))
                break
        if not any("2." in e[0] for e in evidence):
            # Check for any create_volume flow entry
            for line in lines:
                if "create_volume" in line and "flow" in line.lower():
                    evidence.append((
                        f"2. OLD POD: Operation in progress (taskflow executing)",
                        line.strip()[:200]
                    ))
                    break

        # 3. SIGTERM received
        for line in lines:
            if "Initiating graceful shutdown" in line:
                evidence.append((
                    f"3. OLD POD: SIGTERM received — graceful shutdown initiated",
                    line.strip()[:200]
                ))
                break

        # 3b. RPC stopped (no new messages)
        for line in lines:
            if "Stopping RPC server" in line:
                evidence.append((
                    f"   OLD POD: RPC server stopped — no new messages accepted",
                    line.strip()[:200]
                ))
                break

        # 3c. Waiting for in-flight
        for line in lines:
            if ("Phase 2: Waiting for in-flight operations" in line or
                    "waiting for" in line.lower() and
                    "in-flight RPC handler greenthreads" in line):
                evidence.append((
                    f"   OLD POD: Draining — waiting for in-flight operations",
                    line.strip()[:200]
                ))
                break

        # 5. Operation completed, clean exit
        for line in lines:
            if "threadpool tasks completed" in line:
                evidence.append((
                    f"5. OLD POD: In-flight operation completed during drain",
                    line.strip()[:200]
                ))
                break

        for line in lines:
            if "shutdown complete" in line:
                evidence.append((
                    f"   OLD POD: Service shutdown complete — clean exit",
                    line.strip()[:200]
                ))
                break

    # 4. New pod came up
    if new_pod_name:
        if new_pod_logs:
            new_lines = new_pod_logs.split("\n")
            started_line = None
            for line in new_lines:
                if any(t in line for t in ["Starting", "started", "Ready"]):
                    started_line = line.strip()[:200]
                    break
            evidence.append((
                f"4. NEW POD ({new_pod_name}): Replacement pod started",
                started_line or f"Pod {new_pod_name} is Running and Ready"
            ))
        else:
            evidence.append((
                f"4. NEW POD ({new_pod_name}): Replacement pod running",
                "Pod is Running and Ready"
            ))

    # Result
    survived = final_status == "available"
    evidence.append((
        f"RESULT: {operation} {short_id}",
        f"Final status: '{final_status}' — operation {'SURVIVED' if survived else 'FAILED DURING'} pod termination"
    ))

    return evidence


def print_evidence(title: str, evidence: list[tuple[str, str]]) -> str:
    """Print evidence timeline to console and return as string."""
    lines = []
    lines.append(f"\n  {'─'*66}")
    lines.append(f"  EVIDENCE: {title}")
    lines.append(f"  {'─'*66}")
    for label, detail in evidence:
        lines.append(f"  ▶ {label}")
        if detail:
            for dline in detail.split("\n"):
                lines.append(f"      {dline}")
    lines.append(f"  {'─'*66}")
    output = "\n".join(lines)
    print(output)
    return output


# =============================================================================
# Test Results
# =============================================================================

@dataclass
class TestResult:
    name: str
    passed: bool
    duration: float = 0.0
    message: str = ""
    warnings: list[str] = field(default_factory=list)
    evidence: list[tuple[str, str]] = field(default_factory=list)
    log_files: list[str] = field(default_factory=list)


# =============================================================================
# Volume-pod concurrency runner (shared by sameop_* and mixed_* test families)
# =============================================================================
#
# Goal: prove that multiple in-flight operations on a SINGLE volume pod all
# survive a pod kill, with a per-test timing diagram capturing operation
# launch / in-flight / complete times alongside pod kill / SIGTERM / exit /
# replacement-ready times.
#
# The runner is intentionally minimal: each test declares N operation specs,
# the runner sets up + launches them, verifies they are genuinely in-flight,
# kills the pod once, polls everything to a final state, and writes a
# <test>-timeline.md artifact with a Mermaid Gantt diagram.
# =============================================================================


@dataclass
class BatchOp:
    """One operation in a volume-pod concurrency test.

    The runner calls ``setup_fn(target_host, deployment)`` first (returns any
    pre-required resource id list, used for placement attribution). It then
    calls ``launch_fn(target_host, deployment, setup_state)`` to actually
    submit the cinder operation; that returns the resource id whose status
    the runner will poll.
    """
    label: str
    setup_fn: callable                                      # noqa: E501
    launch_fn: callable                                     # noqa: E501
    inflight_statuses: tuple = ("creating", "extending",
                                "downloading", "deleting")
    success_statuses: tuple = ("available",)
    treat_unknown_as_success: bool = False  # for snapshot-delete

    # Filled in by the runner
    resource_id: str = ""
    setup_resource_ids: list = field(default_factory=list)
    setup_state: dict = field(default_factory=dict)
    t_launched: float = 0.0
    t_inflight: float = 0.0
    t_completed: float = 0.0
    final_status: str = ""
    final_size: Optional[int] = None  # for extend ops


def _now_rel(start: float) -> float:
    return time.time() - start


def get_pod_phase(pod_name: str) -> Optional[str]:
    """Return current pod phase, or None if pod no longer exists."""
    result = kubectl("get", "pod", pod_name,
                     "-o", "jsonpath={.status.phase}", check=False)
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def wait_for_pod_exit(pod_name: str,
                      timeout: int = POD_TERMINATE_TIMEOUT) -> Optional[float]:
    """Poll until the named pod terminates (gone or terminal phase).

    Returns the wall-clock seconds when exit was observed, or None on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        phase = get_pod_phase(pod_name)
        if phase is None:
            return time.time()
        if phase in ("Succeeded", "Failed"):
            return time.time()
        time.sleep(2)
    return None


def _grep_log_first_timestamp(log_path: Path,
                              search_terms: list[str]) -> Optional[float]:
    """Return wall-clock epoch seconds of the first matching log line.

    Returns None if the log file is missing, empty, or has no match.
    Cinder log format: '2026-05-21 12:34:56,789 ...' (UTC).
    """
    if not log_path.exists():
        return None
    try:
        with log_path.open() as fh:
            for line in fh:
                if any(term in line for term in search_terms):
                    ts = _extract_timestamp(line)
                    if ts:
                        try:
                            from datetime import timezone
                            dt = datetime.strptime(ts.split(",")[0],
                                                   "%Y-%m-%d %H:%M:%S")
                            # Cinder logs are UTC; explicitly tag so
                            # .timestamp() doesn't interpret as local time.
                            dt = dt.replace(tzinfo=timezone.utc)
                            return dt.timestamp()
                        except ValueError:
                            return None
    except OSError:
        return None
    return None


def hint_same_host_arg(anchor_volume_id: Optional[str]) -> list:
    """Build the --hint argument fragment for same-host placement."""
    if not anchor_volume_id:
        return []
    return ["--hint", f"same_host={anchor_volume_id}"]


def write_timing_diagram(test_name: str,
                         start: float,
                         ops: list,
                         pod_events: dict,
                         deployment: str,
                         old_pod: str,
                         new_pod: Optional[str]) -> Optional[Path]:
    """Write a per-test timing-diagram markdown artifact and return its path.

    pod_events keys (relative seconds; missing keys are tolerated):
        'kill_issued', 'sigterm_logged', 'phase2_drain', 'old_pod_exited',
        'new_pod_ready'
    """
    if not OUTPUT_DIR:
        return None
    path = OUTPUT_DIR / f"{test_name}-timeline.md"

    lines = []
    lines.append(f"# {test_name} — Timing Diagram")
    lines.append("")
    lines.append(f"**Deployment:** `{deployment}`  ")
    lines.append(f"**Old pod (killed):** `{old_pod}`  ")
    if new_pod:
        lines.append(f"**New pod:** `{new_pod}`  ")
    lines.append("")

    # Operation table
    lines.append("## Operation Table")
    lines.append("")
    lines.append("| # | Op | Resource | Launch | In-flight | Complete | Final |")
    lines.append("|---|----|----------|-------:|----------:|---------:|-------|")
    for i, op in enumerate(ops, 1):
        rid = (op.resource_id or "")[:8]
        launch = f"{op.t_launched:.1f}s" if op.t_launched else "-"
        inflight = f"{op.t_inflight:.1f}s" if op.t_inflight else "-"
        complete = f"{op.t_completed:.1f}s" if op.t_completed else "-"
        lines.append(f"| {i} | {op.label} | `{rid}` | {launch} | "
                     f"{inflight} | {complete} | {op.final_status} |")
    lines.append("")

    # Pod lifecycle table
    lines.append("## Pod Lifecycle")
    lines.append("")
    lines.append("| Event | Time |")
    lines.append("|-------|-----:|")
    for key, label in [
        ("kill_issued", "kill issued"),
        ("sigterm_logged", "SIGTERM logged on old pod"),
        ("phase2_drain", "Phase 2 drain started"),
        ("old_pod_exited", "old pod exited"),
        ("new_pod_ready", "new pod ready"),
    ]:
        v = pod_events.get(key)
        if v is not None:
            lines.append(f"| {label} | {v:.1f}s |")
    lines.append("")

    # Mermaid Gantt
    lines.append("## Mermaid Gantt")
    lines.append("")
    lines.append("```mermaid")
    lines.append("gantt")
    lines.append(f"title {test_name}")
    lines.append("dateFormat X")
    lines.append("axisFormat %Ss")
    lines.append("")
    lines.append("section Pod lifecycle")
    for key, label in [
        ("kill_issued", "kill issued"),
        ("sigterm_logged", "SIGTERM logged"),
        ("phase2_drain", "Phase 2 drain"),
        ("old_pod_exited", "old pod exited"),
        ("new_pod_ready", "new pod ready"),
    ]:
        v = pod_events.get(key)
        if v is not None:
            lines.append(f"{label:<22}:milestone, {int(round(v))}, 0")
    lines.append("")
    lines.append("section Operations")
    for op in ops:
        if not op.t_launched and not op.t_completed:
            continue
        start_s = int(round(op.t_launched))
        # gantt requires positive duration; fall back to 1s if equal
        end_s = int(round(op.t_completed)) if op.t_completed else start_s
        dur = max(1, end_s - start_s)
        lines.append(f"{op.label:<22}:active, {start_s}, {dur}")
    lines.append("```")
    lines.append("")

    # Raw chronological event list
    lines.append("## Event Sequence")
    lines.append("")
    events: list[tuple[float, str]] = []
    for op in ops:
        if op.t_launched:
            events.append((op.t_launched, f"launch `{op.label}`"))
        if op.t_inflight:
            events.append((op.t_inflight, f"`{op.label}` observed in-flight"))
        if op.t_completed:
            events.append((op.t_completed,
                           f"`{op.label}` completed → {op.final_status}"))
    for key, label in [
        ("kill_issued", "kill issued"),
        ("sigterm_logged", "SIGTERM logged on old pod"),
        ("phase2_drain", "Phase 2 drain started"),
        ("old_pod_exited", "old pod exited"),
        ("new_pod_ready", "new pod ready"),
    ]:
        v = pod_events.get(key)
        if v is not None:
            events.append((v, label))
    events.sort(key=lambda e: e[0])
    for t, label in events:
        lines.append(f"- `{t:6.1f}s` — {label}")
    lines.append("")

    path.write_text("\n".join(lines))
    return path


def run_volume_pod_concurrency_test(test_name: str,
                                    description: str,
                                    ops: list,
                                    inflight_wait_timeout: int = 240,
                                    final_timeout: int = 1200,
                                    wall_clock_budget: int = 1800,
                                    stall_seconds: int = 600) -> TestResult:
    """Shared runner for same-op + mixed-op volume-pod concurrency tests.

    Steps:
      1. Create a small anchor volume to identify the target volume pod.
      2. Run each op's setup_fn (creates prerequisites pinned to that backend).
      3. Verify all setup resources landed on the same deployment.
      4. Start log stream on the target pod.
      5. Run each op's launch_fn (launches the actual operation under test).
      6. Wait until ALL ops are observed in-flight (or fail fast).
      7. Kill the pod once.
      8. Poll every op until success / error / timeout.
      9. Wait for old pod exit + new pod ready.
     10. Write <test>-timeline.md and return TestResult.

    Failure messages are classified so vCenter / env flake is distinguishable
    from graceful-shutdown regressions.
    """
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  {description}")
    print(f"{'='*70}")
    start = time.time()
    anchor_volume_id: Optional[str] = None
    setup_volume_ids: list = []  # all aux volumes created by setup_fns
    pod_events: dict = {}
    log_stream = None
    log_file: Optional[Path] = None
    new_pod: Optional[str] = None
    deployment = ""
    pod_name = ""

    def _cleanup(volume_ids: list):
        for vid in volume_ids:
            if not vid:
                continue
            time.sleep(2)
            try:
                status = get_volume_status(vid)
                if status in ("available", "error", "in-use"):
                    delete_volume(vid)
            except Exception:  # noqa: BLE001
                pass

    # Stall + budget watchdog. The runner shares two flags with all polling
    # loops via a closure-captured dict so any inner loop can bail early.
    watchdog = {
        "abort": False,
        "abort_reason": "",
        "last_progress": time.time(),
    }

    def _mark_progress(label: str = "") -> None:
        """Call from every checkpoint so the stall guard knows we're alive."""
        watchdog["last_progress"] = time.time()
        if label:
            print(f"  [progress] {label}")

    def _check_watchdog() -> Optional[str]:
        """Return a reason string if the test should abort, else None."""
        elapsed = time.time() - start
        if elapsed > wall_clock_budget:
            return (f"wall-clock budget ({wall_clock_budget}s) exceeded "
                    f"after {elapsed:.0f}s")
        stall = time.time() - watchdog["last_progress"]
        if stall > stall_seconds:
            return (f"stalled for {stall:.0f}s (no progress checkpoint); "
                    f"budget {wall_clock_budget}s")
        if watchdog["abort"]:
            return watchdog["abort_reason"] or "aborted"
        return None

    import threading

    def _watchdog_thread():
        # Background ticker prints elapsed time every 30s so the operator
        # can see liveness, and trips the abort flag when budget/stall hit.
        while not watchdog["abort"]:
            time.sleep(30)
            elapsed = time.time() - start
            stall = time.time() - watchdog["last_progress"]
            if elapsed > wall_clock_budget:
                watchdog["abort"] = True
                watchdog["abort_reason"] = (
                    f"wall-clock budget {wall_clock_budget}s exceeded "
                    f"(elapsed {elapsed:.0f}s)")
                print(f"  [watchdog] ABORT: {watchdog['abort_reason']}")
                return
            if stall > stall_seconds:
                watchdog["abort"] = True
                watchdog["abort_reason"] = (
                    f"stalled for {stall:.0f}s without progress "
                    f"checkpoint")
                print(f"  [watchdog] ABORT: {watchdog['abort_reason']}")
                return
            print(f"  [watchdog] elapsed={elapsed:.0f}s "
                  f"stall={stall:.0f}s budget={wall_clock_budget}s")

    wd_thread = threading.Thread(target=_watchdog_thread, daemon=True)
    wd_thread.start()

    try:
        # 1. Anchor volume pins the test to one volume pod
        print("\n  [setup] creating anchor volume (1GB) to identify target pod")
        anchor_volume_id = create_volume(
            f"test-gs-vpc-anchor-{int(time.time())}", size=1)
        if not anchor_volume_id:
            return TestResult(test_name, False, time.time() - start,
                              "setup failure: anchor volume create failed")
        anchor_status = poll_volume_status(anchor_volume_id, ["available"],
                                           timeout=120)
        if anchor_status != "available":
            return TestResult(
                test_name, False, time.time() - start,
                f"setup failure: anchor stuck in '{anchor_status}'")

        host = get_volume_host(anchor_volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                              "setup failure: could not read anchor host")
        deployment = host_to_deployment(host)
        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(
                test_name, False, time.time() - start,
                f"setup failure: no running pod for {deployment}")
        print(f"  [setup] target deployment: {deployment}")
        print(f"  [setup] target pod: {pod_name}")

        # 2. Per-op setup
        print(f"\n  [setup] running per-op setup for {len(ops)} ops")
        for op in ops:
            reason = _check_watchdog()
            if reason:
                return TestResult(test_name, False, time.time() - start,
                                  f"aborted during setup: {reason}")
            try:
                op.setup_state = op.setup_fn(host, deployment,
                                             anchor_volume_id) or {}
                _mark_progress(f"setup done: {op.label}")
            except Exception as e:  # noqa: BLE001
                return TestResult(
                    test_name, False, time.time() - start,
                    f"setup failure: {op.label} setup raised: {e}")
            for vid in op.setup_resource_ids:
                if vid not in setup_volume_ids:
                    setup_volume_ids.append(vid)

        # 3. Setup volume placement is informational only — the runner's
        #    majority-shard logic in step 5b discovers the actual target
        #    based on launched-op placement, and re-targets the log stream
        #    accordingly. Setup volumes on observer shards naturally pull
        #    their dependent ops to those shards (e.g. a clone reads from
        #    its source, so the source's shard wins).
        for vid in setup_volume_ids:
            if vid == anchor_volume_id:
                continue
            vhost = get_volume_host(vid)
            if vhost:
                vdep = host_to_deployment(vhost)
                if vdep != deployment:
                    print(f"  [placement] setup volume {vid[:8]} on "
                          f"{vdep} (anchor was on {deployment}); "
                          f"runner will follow the work")

        # 4. Start log stream BEFORE launching anything kill-relevant.
        #    Note: this attaches to the anchor's pod. If the first launched
        #    op lands on a DIFFERENT shard (scheduler skew via hint best-
        #    effort), we re-target the log stream below.
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)
        # Tiny grace so the streamer is attached before launches log to it
        time.sleep(1)

        # 5. Launch all ops (best-effort same_host placement).
        #    The scheduler's same_host hint is advisory; ops may spread
        #    across shards. We launch all of them, then determine the
        #    "majority shard" — the deployment that received the most
        #    ops — and kill THAT pod. Ops that landed elsewhere are
        #    treated as observers (logged in the timeline but not part
        #    of the pass criteria). The test still validates that
        #    multiple ops on a SINGLE killed pod survive.
        print(f"\n  [launch] firing {len(ops)} ops")
        for op in ops:
            reason = _check_watchdog()
            if reason:
                stop_log_stream(log_stream, timeout=5)
                return TestResult(test_name, False, time.time() - start,
                                  f"aborted during launch: {reason}")
            try:
                op.resource_id = op.launch_fn(host, deployment,
                                              anchor_volume_id,
                                              op.setup_state) or ""
            except Exception as e:  # noqa: BLE001
                stop_log_stream(log_stream, timeout=5)
                return TestResult(
                    test_name, False, time.time() - start,
                    f"launch failure: {op.label} launch raised: {e}")
            if not op.resource_id:
                stop_log_stream(log_stream, timeout=5)
                return TestResult(
                    test_name, False, time.time() - start,
                    f"launch failure: {op.label} no resource id")
            op.t_launched = _now_rel(start)
            print(f"    {op.label}: launched at {op.t_launched:.1f}s "
                  f"resource={op.resource_id[:8]}")
            _mark_progress(f"launched {op.label}")

        # 5b + 6 COMBINED: verify in-flight in a tight loop, then resolve
        # placement lazily AFTER the kill. IMPORTANT: hammer placement
        # queries are slow (10-40s under load) and MUST NOT run inside this
        # loop — they would burn the ops' in-flight window. We poll ALL ops
        # every 1s using only fast openstack status calls.
        print(f"\n  [placement + in-flight] combined resolution "
              f"(timeout={inflight_wait_timeout}s)")
        deadline = time.time() + inflight_wait_timeout
        while time.time() < deadline:
            reason = _check_watchdog()
            if reason:
                stop_log_stream(log_stream, timeout=5)
                return TestResult(test_name, False, time.time() - start,
                                  f"aborted in placement+inflight: {reason}")
            for op in ops:
                # Check in-flight status FIRST (fast)
                if not op.t_inflight:
                    if op.label.startswith("snapshot_delete"):
                        status = get_snapshot_status(op.resource_id)
                    else:
                        status = get_volume_status(op.resource_id)
                    if status in op.inflight_statuses:
                        op.t_inflight = _now_rel(start)
                        print(f"    {op.label} in-flight ({status}) at "
                              f"{op.t_inflight:.1f}s")
                        _mark_progress(f"in-flight: {op.label}")
                    elif status in op.success_statuses:
                        op.t_inflight = _now_rel(start)
                        op.t_completed = op.t_inflight
                        op.final_status = status
                        print(f"    {op.label} completed before kill "
                              f"({status}) — test invalid")
                        stop_log_stream(log_stream, timeout=5)
                        return TestResult(
                            test_name, False, time.time() - start,
                            f"not in-flight at kill: {op.label} reached "
                            f"'{status}' before pod could be killed")

            # Break as soon as all ops are in-flight. Placement can be
            # resolved lazily after kill — the important thing is to kill
            # ASAP once all ops are confirmed in their in-flight window.
            all_inflight = all(op.t_inflight for op in ops)
            if all_inflight:
                break
            time.sleep(1)  # 1s polling (fast)

        # Best-effort placement resolution for any ops still unresolved
        # (one more pass without waiting)
        for op in ops:
            if "_actual_dep" not in op.setup_state:
                if op.label.startswith("snapshot_delete"):
                    src_vid = op.setup_state.get("src_volume_id")
                    vhost = get_volume_host(src_vid) if src_vid else None
                else:
                    vhost = get_volume_host(op.resource_id)
                if vhost:
                    op.setup_state["_actual_dep"] = host_to_deployment(vhost)

        # Derive placement groups
        placements: dict = {}  # deployment -> [ops]
        for op in ops:
            dep = op.setup_state.get("_actual_dep", "?")
            placements.setdefault(dep, []).append(op)
            if "_actual_dep" not in op.setup_state:
                print(f"    {op.label}: placement unresolved")

        # Pick majority shard
        target_dep = max(
            placements.keys(),
            key=lambda d: (len(placements[d]), 1 if d == deployment else 0))
        target_ops = placements[target_dep]
        observer_ops = [op for op in ops if op not in target_ops]
        print(f"  [placement] target shard: {target_dep} "
              f"({len(target_ops)} ops)")
        if observer_ops:
            print(f"  [placement] observer ops: "
                  f"{[(op.label, op.setup_state.get('_actual_dep')) for op in observer_ops]}")

        if len(target_ops) < 2:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(
                test_name, False, time.time() - start,
                f"placement failure: scheduler scattered ops; only "
                f"{len(target_ops)} landed on majority shard {target_dep} "
                f"(need >=2 for concurrency test)")

        # If majority shard != anchor deployment, re-target log stream
        if target_dep != deployment:
            print(f"  [placement] re-targeting log stream from "
                  f"{deployment} to {target_dep}")
            stop_log_stream(log_stream, timeout=5)
            deployment = target_dep
            pod_name = get_pod_for_deployment(deployment) or pod_name
            log_stream = start_log_stream(pod_name, deployment, log_file)
            time.sleep(1)

        # Final in-flight check on target ops specifically
        not_inflight = [op for op in target_ops if not op.t_inflight]
        if not_inflight:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(
                test_name, False, time.time() - start,
                f"not in-flight at kill: "
                f"{[op.label for op in not_inflight]}")

        # Mark observer ops in-flight (best-effort, non-gating)
        for op in observer_ops:
            if not op.t_inflight:
                if op.label.startswith("snapshot_delete"):
                    status = get_snapshot_status(op.resource_id)
                else:
                    status = get_volume_status(op.resource_id)
                if status in op.inflight_statuses or status in op.success_statuses:
                    op.t_inflight = _now_rel(start)

        # 8. Kill pod
        print(f"\n  [kill] all {len(target_ops)} target ops in-flight; "
              f"deleting pod {pod_name}")
        kubectl("delete", "pod", pod_name, "--wait=false")
        pod_events["kill_issued"] = _now_rel(start)
        _mark_progress("pod kill issued")

        # 9. Poll all ops (target + observer) until terminal
        print(f"\n  [drain] polling ops to terminal state "
              f"(timeout={final_timeout}s)")
        op_deadline = time.time() + final_timeout
        while time.time() < op_deadline:
            reason = _check_watchdog()
            if reason:
                print(f"  [drain] watchdog abort: {reason}")
                break
            all_done = True
            for op in ops:
                if op.t_completed:
                    continue
                if op.label.startswith("snapshot_delete"):
                    status = get_snapshot_status(op.resource_id)
                else:
                    status = get_volume_status(op.resource_id)
                if status in op.success_statuses:
                    op.t_completed = _now_rel(start)
                    op.final_status = status
                    print(f"    {op.label} → {status} at "
                          f"{op.t_completed:.1f}s")
                    _mark_progress(f"complete: {op.label} → {status}")
                elif (op.treat_unknown_as_success
                      and status == "unknown"):
                    op.t_completed = _now_rel(start)
                    op.final_status = "deleted"
                    print(f"    {op.label} → deleted (snapshot gone) at "
                          f"{op.t_completed:.1f}s")
                elif status == "error":
                    op.t_completed = _now_rel(start)
                    op.final_status = "error"
                    print(f"    {op.label} → error at "
                          f"{op.t_completed:.1f}s")
                else:
                    all_done = False
            if all_done:
                break
            time.sleep(POLL_INTERVAL)

        for op in ops:
            if not op.t_completed:
                if op.label.startswith("snapshot_delete"):
                    op.final_status = get_snapshot_status(op.resource_id)
                else:
                    op.final_status = get_volume_status(op.resource_id)
                op.t_completed = _now_rel(start)

        # 10. Pod lifecycle: wait for old pod exit
        exit_wall = wait_for_pod_exit(pod_name,
                                      timeout=POD_TERMINATE_TIMEOUT * 2)
        if exit_wall:
            pod_events["old_pod_exited"] = exit_wall - start

        # 11. Stop log stream + extract pod log timestamps
        stop_log_stream(log_stream, timeout=30)
        log_stream = None

        sig_ts = _grep_log_first_timestamp(
            log_file,
            ["Initiating graceful shutdown"])
        if sig_ts:
            pod_events["sigterm_logged"] = sig_ts - start
        p2_ts = _grep_log_first_timestamp(
            log_file, ["Shutdown signaled, rejecting new threadpool tasks"])
        if p2_ts:
            pod_events["phase2_drain"] = p2_ts - start

        # 12. Wait for replacement pod ready
        new_pod = wait_for_new_pod_ready(deployment, pod_name)
        if new_pod:
            pod_events["new_pod_ready"] = _now_rel(start)

        # 13. Attribution: verify old pod log mentions every TARGET op id
        attribution_misses: list = []
        if log_file and log_file.exists():
            try:
                old_log_text = log_file.read_text()
            except OSError:
                old_log_text = ""
            for op in target_ops:
                if op.resource_id and op.resource_id not in old_log_text:
                    attribution_misses.append(op.label)

        # 14. Write timing diagram (all ops, target + observer)
        timeline_path = write_timing_diagram(
            test_name, start, ops, pod_events,
            deployment, pod_name, new_pod)

        log_files: list = [str(log_file)] if log_file else []
        if timeline_path:
            log_files.append(str(timeline_path))

        # 15. Assess result — gate ONLY on target ops; observers are
        #     informational.
        target_success = [
            op for op in target_ops
            if op.final_status in op.success_statuses
            or (op.treat_unknown_as_success
                and op.final_status in ("deleted", "unknown"))]
        n_target_success = len(target_success)
        n_target_total = len(target_ops)

        observer_success = [
            op for op in observer_ops
            if op.final_status in op.success_statuses
            or (op.treat_unknown_as_success
                and op.final_status in ("deleted", "unknown"))]

        warnings: list = []
        if attribution_misses:
            warnings.append(
                f"target resources not seen in old-pod log: "
                f"{attribution_misses}")
        if "old_pod_exited" not in pod_events:
            warnings.append("old pod exit not observed within timeout")
        if not new_pod:
            warnings.append("replacement pod did not become ready")
        if observer_ops:
            warnings.append(
                f"{len(observer_ops)} observer ops on non-target shards: "
                f"{[(op.label, op.setup_state.get('_actual_dep')) for op in observer_ops]} "
                f"({len(observer_success)} succeeded)")

        passed = (n_target_success == n_target_total
                  and n_target_total >= 2
                  and not attribution_misses
                  and "old_pod_exited" in pod_events
                  and new_pod is not None)

        if passed:
            msg = (f"{n_target_success}/{n_target_total} target ops "
                   f"succeeded on killed pod; "
                   f"old pod exited; new pod ready")
            return TestResult(test_name, True, time.time() - start, msg,
                              warnings, [], log_files)

        # Classified failure message
        if n_target_success != n_target_total:
            cause = "post-kill recovery failure"
        elif attribution_misses:
            cause = "attribution failure"
        elif "old_pod_exited" not in pod_events:
            cause = "old pod did not exit"
        else:
            cause = "replacement pod not ready"
        msg = (f"{cause}: {n_target_success}/{n_target_total} target ops "
               f"succeeded")
        return TestResult(test_name, False, time.time() - start, msg,
                          warnings, [], log_files)

    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        if log_stream:
            stop_log_stream(log_stream, timeout=5)
        return TestResult(test_name, False, time.time() - start,
                          f"unhandled exception: {e}")
    finally:
        # Stop watchdog thread
        watchdog["abort"] = True
        cleanup_ids = []
        for op in ops:
            if op.resource_id and not op.label.startswith("snapshot_delete"):
                cleanup_ids.append(op.resource_id)
        cleanup_ids.extend(setup_volume_ids)
        if anchor_volume_id:
            cleanup_ids.append(anchor_volume_id)
        # Dedup, keep order
        seen = set()
        deduped = []
        for vid in cleanup_ids:
            if vid and vid not in seen:
                seen.add(vid)
                deduped.append(vid)
        _cleanup(deduped)


# =============================================================================
# Tests
# =============================================================================

def test_idle_shutdown() -> TestResult:
    """Test 1: Idle pod graceful shutdown produces correct log sequence.

    Verifies that when no operations are in-flight, a pod delete triggers
    the full graceful shutdown sequence and the pod exits cleanly.
    """
    test_name = "test_idle_shutdown"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify graceful shutdown log sequence on idle cinder-volume pod")
    print(f"{'='*70}")
    start = time.time()

    # 1. Get current pod
    pod_name = get_pod_for_deployment(VOLUME_DEPLOYMENT)
    if not pod_name:
        return TestResult(test_name, False, message="Could not find running pod")
    print(f"  Current pod: {pod_name}")

    # 2. Start log stream BEFORE deleting
    log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
    log_stream = start_log_stream(pod_name, VOLUME_CONTAINER, log_file)

    # 3. Delete pod
    print(f"\n  Deleting pod: {pod_name} [{time.strftime('%H:%M:%S')}]")
    kubectl("delete", "pod", pod_name, "--wait=false")

    # Wait for termination
    deadline = time.time() + POD_TERMINATE_TIMEOUT
    while time.time() < deadline:
        result = kubectl("get", "pod", pod_name, "-o", "jsonpath={.status.phase}", check=False)
        if result.returncode != 0:
            print(f"  Pod terminated (gone) [{time.strftime('%H:%M:%S')}]")
            break
        phase = result.stdout.strip()
        if phase in ("Succeeded", "Failed"):
            print(f"  Pod terminated (phase={phase}) [{time.strftime('%H:%M:%S')}]")
            break
        time.sleep(3)

    # Stop log stream and read captured logs
    stop_log_stream(log_stream, timeout=10)
    old_pod_logs = log_file.read_text() if log_file.exists() else ""

    # Save raw logs
    log_files = []
    if old_pod_logs:
        log_files.append(str(log_file))

    # Wait for new pod
    new_pod = wait_for_new_pod_ready(VOLUME_DEPLOYMENT, pod_name)
    new_pod_logs = ""
    if new_pod:
        time.sleep(3)
        new_pod_logs = get_pod_logs(new_pod, VOLUME_CONTAINER, since_seconds=30)
        if new_pod_logs:
            p = save_log_file(f"{test_name}-new-pod.log", new_pod_logs)
            log_files.append(str(p))

    # Build evidence
    evidence = build_evidence(
        old_pod_logs, new_pod_logs, pod_name, new_pod or "",
        resource_id="", operation="idle_shutdown", final_status="clean_exit"
    )
    # Override with simpler narrative for idle test
    evidence = []
    if old_pod_logs:
        for line in old_pod_logs.split("\n"):
            if "Initiating graceful shutdown" in line:
                evidence.append(("1. SIGTERM received — graceful shutdown initiated",
                                line.strip()[:200]))
                break
        for line in old_pod_logs.split("\n"):
            if "Shutdown signaled, rejecting new threadpool tasks" in line:
                evidence.append(("2. Drain initiated (manager rejecting new tasks)",
                                line.strip()[:200]))
                break
        for line in old_pod_logs.split("\n"):
            if "Green thread pool drained" in line:
                evidence.append(("3. No in-flight operations (idle pod, drained fast)",
                                line.strip()[:200]))
                break
        for line in old_pod_logs.split("\n"):
            if "shutdown complete" in line:
                evidence.append(("4. Service exited cleanly",
                                line.strip()[:200]))
                break
    if new_pod:
        evidence.append(("5. Replacement pod started", f"Pod {new_pod} is Running and Ready"))

    evidence_str = print_evidence("Idle Pod Graceful Shutdown", evidence)

    # Assert log sequence
    log_ok = False
    warnings = []
    if old_pod_logs:
        log_ok = _check_log_sequence(old_pod_logs, SHUTDOWN_LOG_SEQUENCE)
        if log_ok:
            print(f"  OK: All {len(SHUTDOWN_LOG_SEQUENCE)} shutdown log patterns found in order")
        else:
            missing = _find_missing_patterns(old_pod_logs, SHUTDOWN_LOG_SEQUENCE)
            print(f"  FAIL: Missing log patterns: {missing}")
            warnings.append(f"Missing patterns: {missing}")
    else:
        warnings.append("Could not retrieve pod logs")

    duration = time.time() - start
    passed = log_ok and (new_pod is not None)
    msg = "Graceful shutdown sequence verified on idle pod" if passed else "Incomplete"
    return TestResult(test_name, passed, duration, msg, warnings, evidence, log_files)


def test_inflight_volume_create() -> TestResult:
    """Test 2: In-flight volume create completes during graceful shutdown.

    Creates a volume from image (slow operation), then deletes the pod while
    the operation is in progress. Verifies the volume reaches 'available'.
    """
    test_name = "test_inflight_volume_create"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight volume-from-image survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None

    try:
        # 1. Create volume from image
        print(f"\n  Creating volume from image {TEST_IMAGE_ID[:8]}... (size={TEST_VOLUME_SIZE}GB)")
        volume_id = create_volume_from_image(f"test-gs-volume-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create volume")
        print(f"  Volume ID: {volume_id}")

        # 2. Wait for it to enter 'creating'
        print("  Waiting for operation to start...")
        time.sleep(3)
        status = get_volume_status(volume_id)
        print(f"  Volume status: {status} [{time.strftime('%H:%M:%S')}]")

        if status not in ("creating", "downloading", "available"):
            return TestResult(test_name, False, time.time() - start,
                             f"Unexpected initial status: {status}")

        if status == "available":
            return TestResult(test_name, True, time.time() - start,
                             "Volume created before pod could be killed (too fast)",
                             ["Could not verify drain-during-operation behavior"])

        # 3. Determine which pod is processing this volume via hammer
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host via hammer")
        deployment = host_to_deployment(host)
        container = deployment  # container name matches deployment name
        print(f"  Volume host: {host}")
        print(f"  Target deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             f"Could not find running pod for deployment {deployment}")
        print(f"  Target pod: {pod_name}")

        # 4. Start streaming logs BEFORE killing the pod
        #    This captures the entire shutdown sequence including what happens
        #    after SIGTERM — the stream ends when the container exits.
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, container, log_file)

        # 5. Kill the pod while operation is in-flight
        kill_time = time.strftime('%H:%M:%S')
        print(f"\n  Operation in-flight (status={status}). Killing pod! [{kill_time}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 6. Poll volume status until it completes.
        #    NOTE: Due to the documented race condition (graceful-shutdown-race-condition.rst),
        #    the new pod's do_cleanup() may briefly set the volume to 'error' while the
        #    old pod is still completing the operation. The old pod's CreateVolumeOnFinishTask
        #    will then unconditionally write 'available'. So if we see 'error', keep polling
        #    to see if it recovers to 'available'.
        print("\n  Polling volume status (old pod is draining in background)...")
        final_status = None
        deadline = time.time() + VOLUME_CREATE_TIMEOUT
        last_vol_status = None
        saw_error = False
        error_recovery_deadline = None
        ERROR_RECOVERY_WINDOW = 300  # seconds to wait after seeing 'error' for recovery

        while time.time() < deadline:
            vol_status = get_volume_status(volume_id)
            if vol_status != last_vol_status:
                print(f"  Volume {volume_id[:8]}... status: {vol_status} [{time.strftime('%H:%M:%S')}]")
                last_vol_status = vol_status
            if vol_status == "available":
                final_status = "available"
                break
            if vol_status in ("error", "error_deleting"):
                if not saw_error:
                    saw_error = True
                    error_recovery_deadline = time.time() + ERROR_RECOVERY_WINDOW
                    print(f"  Volume entered 'error' — waiting up to {ERROR_RECOVERY_WINDOW}s for recovery (race condition window)")
                elif time.time() > error_recovery_deadline:
                    final_status = vol_status
                    print(f"  Volume did not recover from 'error' within {ERROR_RECOVERY_WINDOW}s")
                    break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = last_vol_status or "timeout"

        complete_time = time.strftime('%H:%M:%S')

        # 7. Wait for the log stream to finish (container exited)
        #    Give it generous time — the old pod may still be draining
        stop_log_stream(log_stream, timeout=120)
        old_pod_logs = log_file.read_text() if log_file.exists() else ""
        log_files = []
        if old_pod_logs:
            log_files.append(str(log_file))

        # 8. Wait for new pod and grab its logs
        new_pod = wait_for_new_pod_ready(deployment, pod_name)
        new_pod_logs = ""
        if new_pod:
            time.sleep(5)
            new_pod_logs = get_pod_logs(new_pod, container, since_seconds=120)
            if new_pod_logs:
                p = save_log_file(f"{test_name}-new-pod.log", new_pod_logs)
                log_files.append(str(p))

        # 8. Build evidence timeline from observed facts + log lines with timestamps
        evidence = []
        vol_short = volume_id[:8]

        if old_pod_logs:
            # 1. Volume create was called (action_track entry)
            ts, line, _ = _find_log_line(old_pod_logs, [f"[{volume_id}] ACTION:'volume_create' MSG:'called'",
                                                         f"[{volume_id}] ACTION",
                                                         "ACTION:'volume_create' MSG:'called'"])
            if ts:
                evidence.append((f"[{ts}] create_volume called", line))
            else:
                # Fallback: look for create_volume in manager
                ts, line, _ = _find_log_line(old_pod_logs, ["create_volume", f"ACTION:'volume_cre"])
                if ts:
                    evidence.append((f"[{ts}] create_volume called", line))

            # 2. Operation in progress (driver executing)
            ts, line, _ = _find_log_line(old_pod_logs, ["copy_image_to_volume", "_fetch_stream_optimized",
                                                         "CreateVolumeFromSpecTask", "Downloading images"])
            if ts:
                evidence.append((f"[{ts}] Volume create in progress (driver executing)", line))

            # 3. SIGTERM received / graceful shutdown
            ts, line, _ = _find_log_line(old_pod_logs, ["Initiating graceful shutdown"])
            if ts:
                evidence.append((f"[{ts}] SIGTERM received — graceful shutdown initiated", line))

            # 3b. RPC stopped
            ts, line, _ = _find_log_line(old_pod_logs, ["Stopping RPC server"])
            if ts:
                evidence.append((f"[{ts}] RPC server stopped (no new messages accepted)", line))

            # 3c. Waiting for in-flight tasks
            ts, line, _ = _find_log_line(old_pod_logs, ["Phase 2: Waiting for in-flight operations", "in-flight RPC handler greenthreads"])
            if ts:
                evidence.append((f"[{ts}] Waiting for in-flight operations to finish", line))

            # 4. Volume create completed (action_track 'done' or flow completed)
            ts, line, _ = _find_log_line(old_pod_logs, [f"[{volume_id}] ACTION:'volume_create' MSG:'done'",
                                                         f"[{volume_id}] ACTION:'volume_create' MSG:'completed'",
                                                         "CreateVolumeOnFinishTask"])
            if ts:
                evidence.append((f"[{ts}] Volume create completed on draining pod", line))

            # 4b. action_track failure entry (if operation failed)
            ts, line, _ = _find_log_line(old_pod_logs, [f"[{volume_id}] ACTION:'volume_create' FAILED",
                                                         f"ACTION:'volume_create' FAILED"])
            if ts:
                evidence.append((f"[{ts}] Volume create FAILED", line))

            # 5. Tasks completed / shutdown complete
            ts, line, _ = _find_log_line(old_pod_logs, ["threadpool tasks completed"])
            if ts and "threadpool" in line:
                evidence.append((f"[{ts}] All in-flight tasks completed", line))

            ts, line, _ = _find_log_line(old_pod_logs, ["shutdown complete"])
            if ts:
                evidence.append((f"[{ts}] Service shutdown complete — pod exited cleanly", line))

        else:
            # No logs captured
            evidence.append((
                f"[{kill_time}] Pod {pod_name} deleted — SIGTERM sent",
                f"Volume {volume_id} was in 'creating' state"
            ))

        # New pod started (from new pod logs)
        if new_pod:
            if new_pod_logs:
                ts, line, _ = _find_log_line(new_pod_logs, ["Starting", "cinder-volume node"])
                if ts:
                    evidence.append((f"[{ts}] New pod {new_pod} started", line))
                else:
                    evidence.append((f"[--] New pod {new_pod} running and Ready", ""))
            else:
                evidence.append((f"[--] New pod {new_pod} running and Ready", ""))

        # Volume final status
        evidence.append((
            f"[{complete_time}] Volume {vol_short} final status: '{final_status}'",
            f"{'In-flight operation SURVIVED pod termination' if final_status == 'available' else 'Operation FAILED'}"
        ))

        print_evidence("In-Flight Volume Create During Graceful Shutdown", evidence)

        # 9. Assert log sequence
        log_ok = False
        warnings = []
        if old_pod_logs:
            log_ok = _check_log_sequence(old_pod_logs, SHUTDOWN_LOG_SEQUENCE_WITH_TASKS)
            if not log_ok:
                log_ok = _check_log_sequence(old_pod_logs, SHUTDOWN_LOG_SEQUENCE)
            if log_ok:
                print(f"  OK: Shutdown log sequence confirmed")
            else:
                missing = _find_missing_patterns(old_pod_logs, SHUTDOWN_LOG_SEQUENCE)
                warnings.append(f"Missing log patterns: {missing}")
        else:
            warnings.append("Could not retrieve terminated pod logs (pod exited before logs could be captured)")

        passed = (final_status == "available")
        msg = (f"Volume reached '{final_status}'"
               + (" — graceful shutdown confirmed in logs" if log_ok else ""))
        return TestResult(test_name, passed, time.time() - start, msg, warnings, evidence, log_files)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if volume_id:
            time.sleep(5)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)
            else:
                print(f"  Skipping cleanup - volume status is '{status}'")


def test_inflight_backup() -> TestResult:
    """Test 3: In-flight backup completes during graceful shutdown.

    Creates a volume, starts a backup, then deletes the backup pod while
    the backup is in progress. Verifies the backup reaches 'available'.
    """
    test_name = "test_inflight_backup"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight backup survives backup pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None
    backup_id = None

    try:
        # 1. Get current backup pod
        pod_name = get_pod_for_deployment(BACKUP_DEPLOYMENT)
        if not pod_name:
            return TestResult(test_name, False, message="Could not find running backup pod")
        print(f"  Current backup pod: {pod_name}")

        # 2. Create a volume to back up
        print("\n  Creating test volume for backup (10GB)...")
        volume_id = create_volume(f"test-gs-backup-src-{int(time.time())}", size=10)
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create source volume")
        print(f"  Source volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=300)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Source volume stuck in '{vol_status}'")

        # 3. Start backup
        print(f"\n  Creating backup of volume {volume_id[:8]}...")
        backup_id = create_backup(volume_id, f"test-gs-backup-{int(time.time())}")
        if not backup_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create backup")
        print(f"  Backup ID: {backup_id}")

        # 4. Wait for backup to start
        time.sleep(5)
        status = get_backup_status(backup_id)
        print(f"  Backup status: {status} [{time.strftime('%H:%M:%S')}]")

        if status == "available":
            return TestResult(test_name, True, time.time() - start,
                             "Backup completed before pod could be killed",
                             ["Could not verify drain-during-operation behavior"])
        if status not in ("creating",):
            return TestResult(test_name, False, time.time() - start,
                             f"Unexpected backup status: {status}")

        # 5. Start log stream BEFORE killing the pod
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, BACKUP_CONTAINER, log_file)

        # 6. Kill the backup pod
        print(f"\n  Backup in-flight (status={status}). Killing pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 7. Poll backup status until it completes
        print("\n  Polling backup status (old pod is draining in background)...")
        final_status = None
        deadline = time.time() + BACKUP_CREATE_TIMEOUT
        last_bkp_status = None

        while time.time() < deadline:
            bkp_status = get_backup_status(backup_id)
            if bkp_status != last_bkp_status:
                print(f"  Backup {backup_id[:8]}... status: {bkp_status} [{time.strftime('%H:%M:%S')}]")
                last_bkp_status = bkp_status
            if bkp_status == "available":
                final_status = "available"
                break
            if bkp_status in ("error",):
                final_status = bkp_status
                break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = last_bkp_status or "timeout"

        # 8. Stop log stream and read captured logs
        stop_log_stream(log_stream, timeout=30)
        old_pod_logs = log_file.read_text() if log_file.exists() else ""

        # Save raw logs
        log_files = []
        if old_pod_logs:
            log_files.append(str(log_file))

        # 7. Wait for new pod
        new_pod = wait_for_new_pod_ready(BACKUP_DEPLOYMENT, pod_name)
        new_pod_logs = ""
        if new_pod:
            time.sleep(5)
            new_pod_logs = get_pod_logs(new_pod, BACKUP_CONTAINER, since_seconds=120)
            if new_pod_logs:
                p = save_log_file(f"{test_name}-new-pod.log", new_pod_logs)
                log_files.append(str(p))

        # 8. Evidence
        evidence = build_evidence(
            old_pod_logs, new_pod_logs, pod_name, new_pod or "",
            resource_id=backup_id, operation="backup_create",
            final_status=final_status,
        )
        print_evidence("In-Flight Backup During Graceful Shutdown", evidence)

        # 9. Assert log sequence
        log_ok = False
        warnings = []
        if old_pod_logs:
            log_ok = _check_log_sequence(old_pod_logs, SHUTDOWN_LOG_SEQUENCE_BACKUP)
            if log_ok:
                print(f"  OK: Shutdown log sequence confirmed")
            else:
                missing = _find_missing_patterns(old_pod_logs, SHUTDOWN_LOG_SEQUENCE_BACKUP)
                warnings.append(f"Missing log patterns: {missing}")
        else:
            warnings.append("Could not retrieve terminated pod logs")

        passed = (final_status == "available")
        msg = (f"Backup reached '{final_status}'"
               + (" — graceful shutdown confirmed in logs" if log_ok else ""))
        return TestResult(test_name, passed, time.time() - start, msg, warnings, evidence, log_files)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if backup_id:
            time.sleep(5)
            status = get_backup_status(backup_id)
            if status in ("available", "error"):
                delete_backup(backup_id)
        if volume_id:
            time.sleep(10)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)
            else:
                print(f"  Skipping volume cleanup - status is '{status}'")


def test_scheduler_reroutes() -> TestResult:
    """Test 4: New volume creates succeed while a pod is draining.

    Deletes a volume pod, then immediately creates volumes. They should be
    scheduled to other healthy pods and complete successfully.
    """
    test_name = "test_scheduler_reroutes"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify new volume creates succeed while a pod is draining")
    print(f"{'='*70}")
    start = time.time()
    volume_ids = []

    try:
        # 1. Get current pod
        pod_name = get_pod_for_deployment(VOLUME_DEPLOYMENT)
        if not pod_name:
            return TestResult(test_name, False, message="Could not find running pod")
        print(f"  Current pod: {pod_name}")

        # 2. Start a long-running op to keep pod draining
        print("\n  Creating long-running volume to keep pod in drain state...")
        drain_vol_id = create_volume_from_image(f"test-gs-drain-{int(time.time())}")
        if drain_vol_id:
            volume_ids.append(drain_vol_id)
            time.sleep(3)

        # 3. Delete the pod
        print(f"\n  Killing pod [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")
        print("  Waiting 10s for drain to engage...")
        time.sleep(10)

        # 4. Create 3 volumes — should route to other pods
        print("\n  Creating 3 volumes while pod is draining...")
        for i in range(3):
            vid = create_volume(f"test-gs-reroute-{i}-{int(time.time())}", size=1)
            if vid:
                volume_ids.append(vid)
                print(f"    Volume {i+1}: {vid}")

        # 5. Poll all volumes to available
        all_available = True
        for vid in volume_ids:
            status = poll_volume_status(vid, ["available"], timeout=300)
            if status != "available":
                print(f"  FAIL: Volume {vid[:8]} ended in '{status}'")
                all_available = False

        # 6. Check old pod logs
        old_pod_logs = get_pod_logs(pod_name, VOLUME_CONTAINER)
        log_files = []
        if old_pod_logs:
            p = save_log_file(f"{test_name}-old-pod.log", old_pod_logs)
            log_files.append(str(p))

        warnings = []
        evidence = []
        if old_pod_logs:
            if "Rejecting create_volume request" in old_pod_logs:
                evidence.append(("reject_if_draining triggered",
                                "Defense-in-depth: RPC arrived after drain, was rejected"))
            if "Initiating graceful shutdown" in old_pod_logs:
                evidence.append(("Graceful shutdown confirmed on old pod",
                                "Deregistered consumer (rpcserver.stop()); new casts stay queued for the replacement pod"))
        else:
            warnings.append("Could not retrieve pod logs")

        evidence.append((
            "New volumes created during drain",
            f"All {len(volume_ids)} volumes reached 'available'" if all_available
            else f"Some volumes failed"
        ))
        print_evidence("Scheduler Rerouting During Drain", evidence)

        # 7. Wait for new pod
        wait_for_new_pod_ready(VOLUME_DEPLOYMENT, pod_name)

        passed = all_available
        msg = "All volumes reached 'available' during pod drain" if passed else "Some volumes failed"
        return TestResult(test_name, passed, time.time() - start, msg, warnings, evidence, log_files)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        for vid in volume_ids:
            time.sleep(2)
            status = get_volume_status(vid)
            if status in ("available", "error"):
                delete_volume(vid)


def test_inflight_cast_during_drain_completes() -> TestResult:
    """Test 4b: Casts routed to a draining backend are not lost (deregister).

    Validates the Epoxy deregister behavior (rpcserver.stop() at drain
    start, PR #358 commit "Deregister RPC consumers"): a volume-create
    cast targeted at the draining backend via a same_host hint must NOT
    be consumed + acked + dropped by the draining pod (the pre-fix
    failure: message lost, volume stuck creating/error). With the fix
    the consumer is deregistered, so the cast stays queued and is
    completed by the replacement pod.
    """
    test_name = "test_inflight_cast_during_drain_completes"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify a cast to a draining backend completes via the replacement pod")
    print(f"{'='*70}")
    start = time.time()
    volume_ids = []

    try:
        # 1. Get current pod
        pod_name = get_pod_for_deployment(VOLUME_DEPLOYMENT)
        if not pod_name:
            return TestResult(test_name, False, message="Could not find running pod")
        print(f"  Current pod: {pod_name}")

        # 2. Start a long-running op to keep the pod draining
        print("\n  Creating long-running volume to keep pod in drain state...")
        drain_vol_id = create_volume_from_image(
            f"test-gs-drain-{int(time.time())}")
        if not drain_vol_id:
            return TestResult(test_name, False,
                              message="Could not create drain volume")
        volume_ids.append(drain_vol_id)
        time.sleep(3)
        drain_host = get_volume_host(drain_vol_id)
        print(f"  Drain volume host: {drain_host}")

        # 3. Delete the pod (drain engages)
        print(f"\n  Killing pod [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")
        print("  Waiting 10s for drain to engage (consumer deregistered)...")
        time.sleep(10)

        # 4. Create a volume with same_host hint -> forces routing to the
        #    draining backend. With the deregister fix this cast is NOT
        #    consumed by the draining pod; it stays queued and is picked
        #    up by the replacement pod. (Pre-fix: acked + dropped -> lost.)
        print("\n  Creating volume targeted at the draining backend...")
        hint = hint_same_host_arg(drain_vol_id)
        result = openstack(
            "volume", "create",
            "--size", "1",
            "--type", TEST_VOLUME_TYPE,
            *hint,
            "-f", "json",
            f"test-gs-drain-cast-{int(time.time())}",
        )
        data = json.loads(result.stdout)
        cast_vol_id = data.get("id")
        if cast_vol_id:
            volume_ids.append(cast_vol_id)
            print(f"    Cast volume: {cast_vol_id} (hint same_host={drain_vol_id})")

        # 5. Poll the cast volume to available. If the deregister fix is
        #    broken the cast is acked+dropped -> stuck creating -> error.
        cast_ok = False
        if cast_vol_id:
            status = poll_volume_status(cast_vol_id, ["available", "error"],
                                        timeout=300)
            cast_ok = (status == "available")
            print(f"  Cast volume final status: {status}")

        # 6. Old pod log evidence
        old_pod_logs = get_pod_logs(pod_name, VOLUME_CONTAINER)
        log_files = []
        if old_pod_logs:
            p = save_log_file(f"{test_name}-old-pod.log", old_pod_logs)
            log_files.append(str(p))

        warnings = []
        evidence = []
        if old_pod_logs:
            if "Initiating graceful shutdown" in old_pod_logs:
                evidence.append(("Graceful shutdown confirmed on old pod",
                                "Deregistered consumer (rpcserver.stop()); cast stayed queued"))
            if "Rejecting create_volume request" in old_pod_logs:
                warnings.append(
                    "Cast arrived before deregister and was rejected "
                    "(defense-in-depth); volume should still complete")
        else:
            warnings.append("Could not retrieve pod logs")

        evidence.append((
            "Cast to draining backend completed",
            f"Volume {cast_vol_id[:8] if cast_vol_id else '?'} reached 'available' via replacement pod"
            if cast_ok else "Cast volume lost (stuck creating/error)"
        ))
        print_evidence("Deregister Validation (Cast During Drain)", evidence)

        # 7. Wait for new pod
        wait_for_new_pod_ready(VOLUME_DEPLOYMENT, pod_name)

        passed = cast_ok
        msg = ("Cast to draining backend completed via replacement pod"
               if passed else "Cast volume was lost during drain")
        return TestResult(test_name, passed, time.time() - start, msg,
                          warnings, evidence, log_files)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start,
                          f"Exception: {e}")
    finally:
        for vid in volume_ids:
            time.sleep(2)
            status = get_volume_status(vid)
            if status in ("available", "error"):
                delete_volume(vid)


# =============================================================================
# Log Sequence Checking
# =============================================================================

def _check_log_sequence(logs: str, patterns: list[str]) -> bool:
    """Check that all patterns appear in logs in order."""
    pos = 0
    for pattern in patterns:
        idx = logs.find(pattern, pos)
        if idx == -1:
            return False
        pos = idx + len(pattern)
    return True


def _find_missing_patterns(logs: str, patterns: list[str]) -> list[str]:
    """Find which patterns are missing from logs."""
    missing = []
    pos = 0
    for pattern in patterns:
        idx = logs.find(pattern, pos)
        if idx == -1:
            missing.append(pattern)
        else:
            pos = idx + len(pattern)
    return missing


# =============================================================================
# Markdown Report
# =============================================================================

def generate_report(results: list[TestResult]) -> str:
    """Generate a markdown report of test results."""
    now = datetime.now()
    lines = []
    lines.append("# Cinder Graceful Shutdown Test Report")
    lines.append("")
    lines.append(f"**Date:** {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Environment:** {OS_CLOUD} (kubectl context: {KUBE_CONTEXT})")
    lines.append(f"**Volume deployment:** {VOLUME_DEPLOYMENT}")
    lines.append(f"**Backup deployment:** {BACKUP_DEPLOYMENT}")
    lines.append("")

    # Summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Result | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Passed | {passed}/{total} |")
    lines.append(f"| Failed | {total - passed}/{total} |")
    lines.append("")

    # Quick table
    lines.append("| Test | Result | Duration | Message |")
    lines.append("|------|--------|----------|---------|")
    for r in results:
        status = "PASS ✓" if r.passed else "FAIL ✗"
        lines.append(f"| {r.name} | {status} | {r.duration:.1f}s | {r.message} |")
    lines.append("")

    # Detailed results
    for r in results:
        lines.append(f"## {r.name}")
        lines.append("")
        status = "PASSED" if r.passed else "FAILED"
        lines.append(f"**Status:** {status}  ")
        lines.append(f"**Duration:** {r.duration:.1f}s  ")
        lines.append(f"**Message:** {r.message}")
        lines.append("")

        if r.warnings:
            lines.append("### Warnings")
            lines.append("")
            for w in r.warnings:
                lines.append(f"- {w}")
            lines.append("")

        if r.evidence:
            lines.append("### Evidence Timeline")
            lines.append("")
            lines.append("```")
            for label, detail in r.evidence:
                lines.append(f"▶ {label}")
                if detail:
                    for dline in detail.split("\n"):
                        lines.append(f"    {dline}")
            lines.append("```")
            lines.append("")

        if r.log_files:
            lines.append("### Log Files")
            lines.append("")
            for lf in r.log_files:
                lines.append(f"- `{lf}`")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# Test 5: In-flight volume delete survives pod termination
# =============================================================================


def test_inflight_volume_delete() -> TestResult:
    """Test 5: In-flight volume delete survives pod termination.

    Creates a volume-from-image (large, slow to delete on vmware), then
    deletes it and kills the pod while delete is in progress.
    Verifies the volume is fully removed.
    """
    test_name = "test_inflight_volume_delete"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight volume delete survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None

    try:
        # 1. Create a volume from image (gives it backing data to delete)
        print("\n  Creating volume from image (16GB, gives vmware data to clean up)...")
        volume_id = create_volume_from_image(f"test-gs-delete-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create volume")
        print(f"  Volume ID: {volume_id}")

        # 2. Wait for volume to be available
        vol_status = poll_volume_status(volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 3. Get volume host and find target pod
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Target deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running pod")
        print(f"  Target pod: {pod_name}")

        # 4. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 5. Delete the volume (starts async delete on the volume pod)
        print(f"\n  Deleting volume {volume_id[:8]}...")
        openstack("volume", "delete", volume_id, check=False)
        time.sleep(2)

        # 6. Kill the pod while delete is in progress
        vol_status = get_volume_status(volume_id)
        print(f"  Volume status: {vol_status} [{time.strftime('%H:%M:%S')}]")
        if vol_status in ("deleting",):
            print(f"  Delete in-flight (status={vol_status}). Killing pod! [{time.strftime('%H:%M:%S')}]")
        else:
            print(f"  Volume status is '{vol_status}' (may have already completed)")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 7. Poll volume status — expect it to disappear (NotFound) or error
        print("\n  Polling volume status (old pod is draining in background)...")
        deadline = time.time() + 300
        final_status = None
        while time.time() < deadline:
            status = get_volume_status(volume_id)
            if status == "unknown":
                # Volume is gone — delete succeeded
                final_status = "deleted"
                print(f"  Volume {volume_id[:8]} deleted successfully [{time.strftime('%H:%M:%S')}]")
                break
            if status == "error_deleting":
                final_status = "error_deleting"
                break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = get_volume_status(volume_id)

        # 8. Stop log stream
        stop_log_stream(log_stream, timeout=30)
        old_pod_logs = log_file.read_text() if log_file.exists() else ""

        # 9. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        passed = (final_status == "deleted")
        msg = f"Volume delete {'completed' if passed else 'FAILED'} (status: {final_status})"
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        # If volume still exists, force delete
        if volume_id:
            status = get_volume_status(volume_id)
            if status not in ("unknown",):
                openstack("volume", "delete", volume_id, "--force", check=False)


# =============================================================================
# Test 6: In-flight volume clone survives pod termination
# =============================================================================


def test_inflight_volume_clone() -> TestResult:
    """Test 6: In-flight volume clone survives pod termination.

    Creates a volume, then clones it and kills the pod while the clone
    is in progress. Verifies the clone reaches 'available'.
    """
    test_name = "test_inflight_volume_clone"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight volume clone survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    source_volume_id = None
    clone_volume_id = None

    try:
        # 1. Create source volume from image (needs data to clone)
        print("\n  Creating source volume from image (16GB)...")
        source_volume_id = create_volume_from_image(f"test-gs-clone-src-{int(time.time())}")
        if not source_volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create source volume")
        print(f"  Source volume ID: {source_volume_id}")

        vol_status = poll_volume_status(source_volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Source volume stuck in '{vol_status}'")

        # 2. Get volume host and find target pod
        host = get_volume_host(source_volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Target deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running pod")
        print(f"  Target pod: {pod_name}")

        # 3. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 4. Start clone
        print(f"\n  Cloning volume {source_volume_id[:8]}...")
        clone_volume_id = create_volume_from_volume(
            source_volume_id, f"test-gs-clone-{int(time.time())}")
        if not clone_volume_id:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create clone")
        print(f"  Clone volume ID: {clone_volume_id}")
        time.sleep(3)

        # 5. Kill the pod
        clone_status = get_volume_status(clone_volume_id)
        print(f"  Clone status: {clone_status} [{time.strftime('%H:%M:%S')}]")
        print(f"  Killing pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 6. Poll clone status
        print("\n  Polling clone status (old pod is draining in background)...")
        error_recovery_deadline = None
        final_status = poll_volume_status(clone_volume_id, ["available"],
                                          timeout=600, fail_statuses=[])
        # Check for error→available recovery
        if final_status == "error":
            print(f"  Clone in 'error' — waiting up to 300s for recovery...")
            deadline = time.time() + 300
            while time.time() < deadline:
                status = get_volume_status(clone_volume_id)
                if status == "available":
                    final_status = "available"
                    break
                time.sleep(POLL_INTERVAL)

        # 7. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 8. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        passed = (final_status == "available")
        msg = f"Clone volume reached '{final_status}'"
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if clone_volume_id:
            status = get_volume_status(clone_volume_id)
            if status in ("available", "error"):
                delete_volume(clone_volume_id)
        if source_volume_id:
            time.sleep(5)
            status = get_volume_status(source_volume_id)
            if status in ("available", "error"):
                delete_volume(source_volume_id)


# =============================================================================
# Test 7: In-flight snapshot create survives pod termination
# =============================================================================


def test_inflight_snapshot_create() -> TestResult:
    """Test 7: In-flight snapshot create survives pod termination.

    Creates a volume, starts a snapshot, then kills the pod while snapshot
    creation is in progress. Verifies the snapshot reaches 'available'.
    """
    test_name = "test_inflight_snapshot_create"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight snapshot create survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None
    snapshot_id = None

    try:
        # 1. Create source volume from image
        print("\n  Creating source volume from image (16GB)...")
        volume_id = create_volume_from_image(f"test-gs-snap-src-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create source volume")
        print(f"  Volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 2. Get volume host and target pod
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Target deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running pod")
        print(f"  Target pod: {pod_name}")

        # 3. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 4. Start snapshot creation
        print(f"\n  Creating snapshot of volume {volume_id[:8]}...")
        snapshot_id = create_snapshot(volume_id, f"test-gs-snap-{int(time.time())}")
        if not snapshot_id:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create snapshot")
        print(f"  Snapshot ID: {snapshot_id}")
        time.sleep(2)

        # 5. Kill the pod
        snap_status = get_snapshot_status(snapshot_id)
        print(f"  Snapshot status: {snap_status} [{time.strftime('%H:%M:%S')}]")
        print(f"  Killing pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 6. Poll snapshot status
        print("\n  Polling snapshot status (old pod is draining in background)...")
        final_status = poll_snapshot_status(snapshot_id, ["available"], timeout=300)

        # 7. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 8. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        passed = (final_status == "available")
        msg = f"Snapshot reached '{final_status}'"
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if snapshot_id:
            time.sleep(5)
            status = get_snapshot_status(snapshot_id)
            if status in ("available", "error"):
                delete_snapshot(snapshot_id)
        if volume_id:
            time.sleep(5)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)


# =============================================================================
# Test 8: In-flight snapshot delete survives pod termination
# =============================================================================


def test_inflight_snapshot_delete() -> TestResult:
    """Test 8: In-flight snapshot delete survives pod termination.

    Creates a volume and snapshot, then deletes the snapshot and kills the
    pod while the delete is in progress. Verifies the snapshot is removed.
    """
    test_name = "test_inflight_snapshot_delete"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight snapshot delete survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None
    snapshot_id = None

    try:
        # 1. Create source volume from image
        print("\n  Creating source volume from image (16GB)...")
        volume_id = create_volume_from_image(f"test-gs-snapdel-src-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create source volume")
        print(f"  Volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 2. Create snapshot
        print(f"\n  Creating snapshot of volume {volume_id[:8]}...")
        snapshot_id = create_snapshot(volume_id, f"test-gs-snapdel-{int(time.time())}")
        if not snapshot_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create snapshot")
        print(f"  Snapshot ID: {snapshot_id}")

        snap_status = poll_snapshot_status(snapshot_id, ["available"], timeout=300)
        if snap_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Snapshot stuck in '{snap_status}'")

        # 3. Get volume host and target pod
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running pod")
        print(f"  Target pod: {pod_name}")

        # 4. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 5. Delete the snapshot
        print(f"\n  Deleting snapshot {snapshot_id[:8]}...")
        openstack("volume", "snapshot", "delete", snapshot_id, check=False)
        time.sleep(2)

        # 6. Kill the pod
        snap_status = get_snapshot_status(snapshot_id)
        print(f"  Snapshot status: {snap_status} [{time.strftime('%H:%M:%S')}]")
        print(f"  Killing pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 7. Poll snapshot status — expect it to disappear
        print("\n  Polling snapshot status (old pod is draining)...")
        deadline = time.time() + 300
        final_status = None
        while time.time() < deadline:
            status = get_snapshot_status(snapshot_id)
            if status == "unknown":
                final_status = "deleted"
                print(f"  Snapshot {snapshot_id[:8]} deleted successfully [{time.strftime('%H:%M:%S')}]")
                snapshot_id = None  # Don't try cleanup
                break
            if status == "error_deleting":
                final_status = "error_deleting"
                break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = get_snapshot_status(snapshot_id)

        # 8. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 9. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        passed = (final_status == "deleted")
        msg = f"Snapshot delete {'completed' if passed else 'FAILED'} (status: {final_status})"
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if snapshot_id:
            openstack("volume", "snapshot", "delete", snapshot_id, "--force", check=False)
        if volume_id:
            time.sleep(10)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)


# =============================================================================
# Test 9: In-flight backup survives VOLUME pod termination
# =============================================================================


def test_inflight_backup_kill_volume_pod() -> TestResult:
    """Test 9: Volume pod termination during backup preparation.

    Starts a backup, then kills the VOLUME pod (not backup pod) while it's
    preparing the volume for backup (creating temp snapshot/clone).
    The volume pod should drain gracefully, complete the preparation,
    and the backup should finish successfully.
    """
    test_name = "test_inflight_backup_kill_volume_pod"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify backup survives VOLUME pod termination during prep")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None
    backup_id = None

    try:
        # 1. Create a volume from image (needs data for backup to be meaningful)
        print("\n  Creating source volume from image (16GB)...")
        volume_id = create_volume_from_image(f"test-gs-bkpvol-src-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create source volume")
        print(f"  Volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 2. Get volume host and find the VOLUME pod to kill
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Volume deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running volume pod")
        print(f"  Target VOLUME pod: {pod_name}")

        # 3. Start log stream on the VOLUME pod
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 4. Start backup — this triggers the volume pod to prepare the volume
        print(f"\n  Creating backup (volume pod will prepare snapshot)...")
        backup_id = create_backup(volume_id, f"test-gs-bkpvol-{int(time.time())}")
        if not backup_id:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create backup")
        print(f"  Backup ID: {backup_id}")

        # 5. Wait briefly for the volume pod to start preparation
        #    The volume goes to 'backing-up' when the volume driver is active
        time.sleep(5)
        vol_status = get_volume_status(volume_id)
        bkp_status = get_backup_status(backup_id)
        print(f"  Volume status: {vol_status}, Backup status: {bkp_status} [{time.strftime('%H:%M:%S')}]")

        # 6. Kill the VOLUME pod (not backup pod!)
        print(f"\n  Killing VOLUME pod while backup prep is in progress! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 7. Poll backup status — should eventually reach 'available'
        print("\n  Polling backup status (volume pod draining, backup pod waiting)...")
        final_status = None
        deadline = time.time() + 600  # backups can be slow
        last_bkp_status = None
        while time.time() < deadline:
            bkp_status = get_backup_status(backup_id)
            if bkp_status != last_bkp_status:
                print(f"  Backup {backup_id[:8]}... status: {bkp_status} [{time.strftime('%H:%M:%S')}]")
                last_bkp_status = bkp_status
            if bkp_status == "available":
                final_status = "available"
                break
            if bkp_status == "error":
                final_status = "error"
                break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = last_bkp_status or "timeout"

        # 8. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 9. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        passed = (final_status == "available")
        msg = f"Backup reached '{final_status}' after volume pod termination"
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if backup_id:
            time.sleep(5)
            status = get_backup_status(backup_id)
            if status in ("available", "error"):
                delete_backup(backup_id)
        if volume_id:
            time.sleep(10)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)
            else:
                print(f"  Skipping volume cleanup — status is '{status}'")


# =============================================================================
# Test 10: In-flight volume extend survives pod termination
# =============================================================================


def test_inflight_volume_extend() -> TestResult:
    """Test 10: In-flight volume extend survives pod termination.

    Creates a volume from image (16GB), then extends it to 32GB and kills the
    pod while the extend is in progress. Verifies the volume reaches 'available'
    with the new size.
    """
    test_name = "test_inflight_volume_extend"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight volume extend survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None
    new_size = TEST_VOLUME_SIZE * 2  # 32GB

    try:
        # 1. Create volume from image (needs backing VMDK for extend to take time)
        print(f"\n  Creating volume from image ({TEST_VOLUME_SIZE}GB)...")
        volume_id = create_volume_from_image(f"test-gs-extend-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create volume")
        print(f"  Volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 2. Get volume host and find target pod
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Target deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running pod")
        print(f"  Target pod: {pod_name}")

        # 3. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 4. Extend volume (16GB -> 32GB)
        print(f"\n  Extending volume {volume_id[:8]} from {TEST_VOLUME_SIZE}GB "
              f"to {new_size}GB...")
        result = openstack("volume", "set", "--size", str(new_size),
                           volume_id, check=False)
        if result.returncode != 0:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             f"Extend command failed: {result.stderr.strip()}")
        time.sleep(3)

        # 5. Kill the pod while extend is in progress
        vol_status = get_volume_status(volume_id)
        print(f"  Volume status: {vol_status} [{time.strftime('%H:%M:%S')}]")
        if vol_status == "extending":
            print(f"  Extend in-flight (status=extending). "
                  f"Killing pod! [{time.strftime('%H:%M:%S')}]")
        elif vol_status == "available":
            # Extend may have completed already (fast driver)
            print(f"  WARNING: Volume already 'available' — extend may have "
                  f"completed before pod kill. Killing pod anyway.")
        else:
            print(f"  Volume status is '{vol_status}'. "
                  f"Killing pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 6. Poll volume status
        print("\n  Polling volume status (old pod is draining in background)...")
        final_status = poll_volume_status(volume_id, ["available"],
                                          timeout=600, fail_statuses=[])
        # Check for error→available recovery
        if final_status == "error":
            print(f"  Volume in 'error' — waiting up to 300s for recovery...")
            deadline = time.time() + 300
            while time.time() < deadline:
                status = get_volume_status(volume_id)
                if status == "available":
                    final_status = "available"
                    break
                time.sleep(POLL_INTERVAL)

        # 7. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 8. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        # 9. Verify final size
        final_size = None
        if final_status == "available":
            result = openstack("volume", "show", volume_id, "-f", "json",
                               check=False)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                final_size = data.get("size")
                print(f"  Final volume size: {final_size}GB (expected {new_size}GB)")

        passed = (final_status == "available" and final_size == new_size)
        if final_status == "available" and final_size != new_size:
            msg = (f"Volume is 'available' but size is {final_size}GB "
                   f"(expected {new_size}GB)")
        else:
            msg = f"Volume reached '{final_status}' with size {final_size}GB"
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start, f"Exception: {e}")
    finally:
        if volume_id:
            time.sleep(5)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)


# =============================================================================
# Test 11: Multiple in-flight operations survive pod termination
# =============================================================================


def test_inflight_multiple_operations() -> TestResult:
    """Test 11: Multiple concurrent in-flight operations survive pod termination.

    Creates a small volume to identify the target pod, then fires off 3
    volume-from-image creates simultaneously and an extend. Kills the pod
    within 10s — while all operations are still actively downloading the
    800MB image (30-60s each). Verifies all volumes reach 'available'.
    This validates that pool.waitall() correctly waits for ALL in-flight
    RPC handler greenthreads, not just one.
    """
    test_name = "test_inflight_multiple_operations"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify multiple concurrent operations survive pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_ids = []  # All volumes to track/cleanup
    anchor_volume_id = None  # Small volume used to find host + test extend

    try:
        # 1. Create a small empty volume (fast) to identify target host/pod
        #    This also serves as the extend target later.
        print(f"\n  Creating small anchor volume (1GB) to identify target pod...")
        anchor_volume_id = create_volume(
            f"test-gs-multi-anchor-{int(time.time())}", size=1)
        if not anchor_volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create anchor volume")
        volume_ids.append(anchor_volume_id)
        print(f"  Anchor volume ID: {anchor_volume_id}")

        vol_status = poll_volume_status(anchor_volume_id, ["available"],
                                        timeout=120)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Anchor volume stuck in '{vol_status}'")

        # 2. Get the host and target pod
        host = get_volume_host(anchor_volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Target deployment: {deployment}")

        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running pod")
        print(f"  Target pod: {pod_name}")

        # 3. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 4. Fire off ALL operations simultaneously:
        #    - 3 volume-from-image creates (each downloads 800MB, takes 30-60s)
        #    - 1 extend on the anchor volume (has 30s artificial delay)
        print(f"\n  Launching 3 volume-from-image creates + 1 extend...")

        img_volume_ids = []
        for i in range(1, 4):
            vol_id = create_volume_from_image(
                f"test-gs-multi-{i}-{int(time.time())}",
                hint_same_host=anchor_volume_id)
            if vol_id:
                img_volume_ids.append(vol_id)
                volume_ids.append(vol_id)
                print(f"  Image volume {i} ID: {vol_id}")
            else:
                print(f"  WARNING: Failed to create image volume {i}")

        # Extend the anchor volume (1GB -> 2GB, has 30s delay in driver)
        print(f"  Extending anchor volume ({anchor_volume_id[:8]}) "
              f"from 1GB to 2GB...")
        openstack("volume", "set", "--size", "2",
                  anchor_volume_id, check=False)

        # 5. Poll until EACH image volume has been observed in-flight at least
        #    once, then kill. Launches are spread out by API latency under
        #    load, so requiring all-in-flight simultaneously is too strict.
        #    Track per-volume observation like the concurrency runner.
        print(f"\n  Waiting for each image volume to be observed in-flight...")
        inflight_deadline = time.time() + 240
        observed_inflight = {vid: False for vid in img_volume_ids}
        while time.time() < inflight_deadline and not all(
                observed_inflight.values()):
            for vid in img_volume_ids:
                if observed_inflight[vid]:
                    continue
                status = get_volume_status(vid)
                if status in ("creating", "downloading"):
                    observed_inflight[vid] = True
                    print(f"    {vid[:8]}: in-flight ({status})")
                elif status in ("available", "error"):
                    print(f"    {vid[:8]}: {status} (⚠ completed/failed "
                          f"before observation)")
            time.sleep(1)

        # 6. Verify ALL operations are still in-flight (not completed!)
        print(f"\n  Volume statuses before kill (must all be in-progress):")
        all_inflight = True
        for vid in volume_ids:
            status = get_volume_status(vid)
            in_progress = status in ("creating", "extending")
            marker = "✓ in-flight" if in_progress else "⚠ NOT in-flight!"
            print(f"    {vid[:8]}: {status} ({marker})")
            if vid != anchor_volume_id and not in_progress:
                # Anchor might be 'available' if extend already completed
                # but image volumes MUST be in 'creating'
                all_inflight = False

        if not all_inflight:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Some operations completed before pod kill — "
                             "test cannot validate graceful shutdown")

        # 7. Kill the pod — operations are genuinely in-flight!
        print(f"\n  All operations confirmed in-flight. "
              f"Killing pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 8. Poll all volumes until they reach available (or timeout)
        print("\n  Polling volume statuses (old pod is draining)...")
        results_per_volume = {}
        deadline = time.time() + 900  # 15 min max

        while time.time() < deadline:
            all_done = True
            for vid in volume_ids:
                if vid in results_per_volume:
                    continue
                status = get_volume_status(vid)
                if status == "available":
                    results_per_volume[vid] = "available"
                    print(f"    {vid[:8]}: available ✅ "
                          f"[{time.strftime('%H:%M:%S')}]")
                elif status == "error":
                    results_per_volume[vid] = "error"
                    print(f"    {vid[:8]}: error (will check for recovery) "
                          f"[{time.strftime('%H:%M:%S')}]")
                else:
                    all_done = False
            if all_done:
                break
            time.sleep(POLL_INTERVAL)

        # 9. For volumes in 'error', wait up to 300s for recovery
        for vid in list(results_per_volume.keys()):
            if results_per_volume[vid] == "error":
                print(f"    {vid[:8]}: waiting up to 300s for "
                      f"error→available recovery...")
                recovery_deadline = time.time() + 300
                while time.time() < recovery_deadline:
                    status = get_volume_status(vid)
                    if status == "available":
                        results_per_volume[vid] = "available"
                        print(f"    {vid[:8]}: recovered → available ✅")
                        break
                    time.sleep(POLL_INTERVAL)

        # Check any volumes that never got a final status
        for vid in volume_ids:
            if vid not in results_per_volume:
                status = get_volume_status(vid)
                results_per_volume[vid] = status
                print(f"    {vid[:8]}: timed out in '{status}'")

        # 10. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 11. Wait for new pod
        new_pod = wait_for_new_pod_ready(deployment, pod_name)

        # 12. Check extend result on anchor volume
        extend_size = None
        result = openstack("volume", "show", anchor_volume_id, "-f", "json",
                           check=False)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            extend_size = data.get("size")
            print(f"\n  Anchor volume final size: {extend_size}GB "
                  f"(expected 2GB)")

        # 13. Assess results
        num_available = sum(1 for s in results_per_volume.values()
                           if s == "available")
        total = len(volume_ids)
        extend_ok = (extend_size == 2)

        passed = (num_available == total and extend_ok)
        msg = (f"{num_available}/{total} volumes reached 'available', "
               f"extend {'OK' if extend_ok else 'FAILED'} "
               f"(size={extend_size}GB)")
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start,
                         f"Exception: {e}")
    finally:
        for vid in volume_ids:
            time.sleep(2)
            status = get_volume_status(vid)
            if status in ("available", "error"):
                delete_volume(vid)


# =============================================================================
# Test 12: In-flight backup restore survives backup pod termination
# =============================================================================


def test_inflight_restore_kill_backup_pod() -> TestResult:
    """Test 12: In-flight backup restore survives backup pod termination.

    Creates a fresh source volume and backup (avoiding pre-created resource
    corruption issues), then restores to a new volume and kills the backup
    pod 15s after the restore starts. Verifies the restored volume reaches
    'available'.
    """
    test_name = "test_inflight_restore_kill_backup_pod"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight backup restore survives backup pod termination")
    print(f"{'='*70}")
    start = time.time()
    source_volume_id = None
    backup_id = None
    restored_volume_id = None

    try:
        # 1. Create source volume from image (gives us data to backup)
        print("\n  Creating source volume from image (16GB)...")
        source_volume_id = create_volume_from_image(
            f"test-gs-restore-src-{int(time.time())}")
        if not source_volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create source volume")
        print(f"  Source volume ID: {source_volume_id}")

        vol_status = poll_volume_status(source_volume_id, ["available"],
                                        timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Source volume stuck in '{vol_status}'")

        # 2. Create backup of the source volume
        print(f"\n  Creating backup of source volume...")
        backup_id = create_backup(source_volume_id,
                                  f"test-gs-restore-bak-{int(time.time())}")
        if not backup_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create backup")
        print(f"  Backup ID: {backup_id}")

        backup_status = poll_backup_status(backup_id, ["available"],
                                           timeout=BACKUP_CREATE_TIMEOUT)
        if backup_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Backup stuck in '{backup_status}'")

        # 3. Get backup host and find the correct backup pod
        backup_host = get_backup_host(backup_id)
        if not backup_host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine backup host")
        backup_deployment = backup_host_to_deployment(backup_host)
        print(f"  Backup host: {backup_host}")
        print(f"  Target backup deployment: {backup_deployment}")

        pod_name = get_pod_for_deployment(backup_deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running backup pod")
        print(f"  Target backup pod: {pod_name}")

        # 4. Start log stream on backup pod
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, backup_deployment, log_file)

        # 5. Start restore to a new volume
        print(f"\n  Restoring backup {backup_id[:8]} to new volume...")
        restored_volume_id = restore_backup(
            backup_id, f"test-gs-restore-vol-{int(time.time())}")
        if not restored_volume_id:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Failed to start restore")
        print(f"  Restored volume ID: {restored_volume_id}")

        # 6. Wait for volume to enter 'restoring-backup' status
        print("  Waiting for volume to enter 'restoring-backup'...")
        deadline = time.time() + 60
        in_restore = False
        while time.time() < deadline:
            status = get_volume_status(restored_volume_id)
            if status == "restoring-backup":
                in_restore = True
                print(f"  Volume status: restoring-backup ✓ "
                      f"[{time.strftime('%H:%M:%S')}]")
                break
            time.sleep(2)

        if not in_restore:
            status = get_volume_status(restored_volume_id)
            if status == "available":
                print(f"  WARNING: Restore already completed before we could "
                      f"kill the pod!")
            else:
                stop_log_stream(log_stream, timeout=5)
                return TestResult(test_name, False, time.time() - start,
                                 f"Volume never entered 'restoring-backup' "
                                 f"(status: {status})")

        # 7. Wait 15s for data transfer to be in progress
        print("  Waiting 15s to ensure data transfer is in progress...")
        time.sleep(15)

        # 8. Kill the backup pod
        status = get_volume_status(restored_volume_id)
        print(f"  Volume status: {status} [{time.strftime('%H:%M:%S')}]")
        print(f"  Killing backup pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 9. Poll restored volume status until available (or error)
        print("\n  Polling restored volume status "
              "(old pod is draining in background)...")
        final_status = poll_volume_status(
            restored_volume_id, ["available"],
            timeout=900, fail_statuses=["error_restoring", "error"])

        # 10. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 11. Wait for new backup pod
        new_pod = wait_for_new_pod_ready(backup_deployment, pod_name)

        # 12. Also check backup status recovered
        backup_final = get_backup_status(backup_id)
        print(f"  Backup final status: {backup_final}")

        passed = (final_status == "available")
        msg = (f"Restored volume reached '{final_status}', "
               f"backup status '{backup_final}'")
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start,
                         f"Exception: {e}")
    finally:
        if restored_volume_id:
            time.sleep(5)
            status = get_volume_status(restored_volume_id)
            if status in ("available", "error", "error_restoring"):
                delete_volume(restored_volume_id)
        if backup_id:
            time.sleep(5)
            status = get_backup_status(backup_id)
            if status in ("available", "error"):
                delete_backup(backup_id)
        if source_volume_id:
            time.sleep(5)
            status = get_volume_status(source_volume_id)
            if status in ("available", "error"):
                delete_volume(source_volume_id)


def test_inflight_restore_kill_during_attach() -> TestResult:
    """Test 13: Backup restore survives pod kill during data transfer.

    Kills the backup pod 15s after restore starts — during the data transfer
    phase (chunks being read from Swift and written to volume). The
    greenthread is actively doing I/O when SIGTERM arrives.
    """
    return _run_restore_kill_test(
        test_name="test_inflight_restore_kill_during_attach",
        description="Kill during Phase 3 (data transfer, 15s delay)",
        kill_delay=15,
    )


def test_inflight_restore_kill_during_detach() -> TestResult:
    """Test 14: Backup restore survives pod kill during post-transfer phase.

    Kills the backup pod 40s after restore starts — after data transfer
    completes and during the Phase 3→4 boundary (30s GS-TEST delay before
    detach). Tests that detach + finalization complete during drain.
    """
    return _run_restore_kill_test(
        test_name="test_inflight_restore_kill_during_detach",
        description="Kill during Phase 3→4 (post-transfer, pre-detach, 40s delay)",
        kill_delay=40,
    )


def test_inflight_restore_kill_during_finalize() -> TestResult:
    """Test 15: Backup restore survives pod kill during finalize phase.

    Kills the backup pod 70s after restore starts — after detach completes
    and during the Phase 4→5 boundary (30s GS-TEST delay before status
    update). Tests that the final status write completes during drain.
    """
    return _run_restore_kill_test(
        test_name="test_inflight_restore_kill_during_finalize",
        description="Kill during Phase 4→5 (post-detach, pre-status update, 70s delay)",
        kill_delay=70,
    )


def _run_restore_kill_test(test_name: str, description: str,
                           kill_delay: int) -> TestResult:
    """Shared implementation for restore pod-kill tests.

    Args:
        test_name: Name for the test result
        description: Human-readable description of what phase is being tested
        kill_delay: Seconds to wait after 'restoring-backup' before killing pod
    """
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  {description}")
    print(f"  Kill delay: {kill_delay}s after restore starts")
    print(f"{'='*70}")
    start = time.time()
    backup_id = PRECREATED_BACKUP_ID
    restored_volume_id = None

    try:
        # 1. Verify pre-created backup is available
        print(f"\n  Verifying pre-created backup {backup_id[:8]} is available...")
        backup_status = get_backup_status(backup_id)
        if backup_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Pre-created backup not available "
                             f"(status: {backup_status}). "
                             f"ID: {backup_id}")
        print(f"  Backup {backup_id[:8]}: available ✓")

        # 2. Verify pre-created source volume is available
        print(f"  Verifying pre-created source volume "
              f"{PRECREATED_SOURCE_VOLUME_ID[:8]} is available...")
        vol_status = get_volume_status(PRECREATED_SOURCE_VOLUME_ID)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Pre-created source volume not available "
                             f"(status: {vol_status}). "
                             f"ID: {PRECREATED_SOURCE_VOLUME_ID}")
        print(f"  Source volume {PRECREATED_SOURCE_VOLUME_ID[:8]}: available ✓")

        # 3. Get backup host and find the correct backup pod
        backup_host = get_backup_host(backup_id)
        if not backup_host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine backup host")
        backup_deployment = backup_host_to_deployment(backup_host)
        print(f"  Backup host: {backup_host}")
        print(f"  Target backup deployment: {backup_deployment}")

        pod_name = get_pod_for_deployment(backup_deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running backup pod")
        print(f"  Target backup pod: {pod_name}")

        # 4. Start log stream on backup pod
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, backup_deployment, log_file)

        # 5. Start restore to a new volume
        print(f"\n  Restoring backup {backup_id[:8]} to new volume...")
        restored_volume_id = restore_backup(
            backup_id, f"test-gs-restore-vol-{int(time.time())}")
        if not restored_volume_id:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Failed to start restore")
        print(f"  Restored volume ID: {restored_volume_id}")

        # 6. Wait for volume to enter 'restoring-backup' status
        print("  Waiting for volume to enter 'restoring-backup'...")
        deadline = time.time() + 60
        in_restore = False
        while time.time() < deadline:
            status = get_volume_status(restored_volume_id)
            if status == "restoring-backup":
                in_restore = True
                print(f"  Volume status: restoring-backup ✓ "
                      f"[{time.strftime('%H:%M:%S')}]")
                break
            time.sleep(2)

        if not in_restore:
            status = get_volume_status(restored_volume_id)
            if status == "available":
                print(f"  WARNING: Restore already completed before we could "
                      f"kill the pod!")
            else:
                stop_log_stream(log_stream, timeout=5)
                return TestResult(test_name, False, time.time() - start,
                                 f"Volume never entered 'restoring-backup' "
                                 f"(status: {status})")

        # 7. Wait the specified delay before killing
        print(f"  Waiting {kill_delay}s before killing pod...")
        time.sleep(kill_delay)

        # 8. Kill the backup pod
        status = get_volume_status(restored_volume_id)
        print(f"  Volume status: {status} [{time.strftime('%H:%M:%S')}]")
        if status == "available":
            print(f"  WARNING: Restore completed before pod kill — "
                  f"kill_delay too long for this operation")
        print(f"  Killing backup pod! [{time.strftime('%H:%M:%S')}]")
        kubectl("delete", "pod", pod_name, "--wait=false")

        # 9. Poll restored volume status until available (or error)
        print("\n  Polling restored volume status "
              "(old pod is draining in background)...")
        final_status = poll_volume_status(
            restored_volume_id, ["available"],
            timeout=900, fail_statuses=["error_restoring", "error"])

        # Check for error recovery
        if final_status in ("error", "error_restoring"):
            print(f"  Volume in '{final_status}' — waiting up to 300s "
                  f"for recovery...")
            deadline = time.time() + 300
            while time.time() < deadline:
                status = get_volume_status(restored_volume_id)
                if status == "available":
                    final_status = "available"
                    break
                time.sleep(POLL_INTERVAL)

        # 10. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 11. Wait for new backup pod
        new_pod = wait_for_new_pod_ready(backup_deployment, pod_name)

        # 12. Also check backup status recovered
        backup_final = get_backup_status(backup_id)
        print(f"  Backup final status: {backup_final}")

        passed = (final_status == "available")
        msg = (f"Restored volume reached '{final_status}', "
               f"backup status '{backup_final}'")
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start,
                         f"Exception: {e}")
    finally:
        # Only clean up the restored volume — never delete the pre-created
        # source volume or backup!
        if restored_volume_id:
            time.sleep(5)
            status = get_volume_status(restored_volume_id)
            if status in ("available", "error", "error_restoring"):
                delete_volume(restored_volume_id)


# =============================================================================
# Test 16: In-flight volume migration (same vCenter) survives pod termination
# =============================================================================


def test_inflight_migrate_same_vc() -> TestResult:
    """Test 16: In-flight volume migration (same vCenter) survives pod termination.

    Creates a volume on one backend, initiates migration to another backend
    on the same vCenter (vc-a-0 ↔ vc-a-1). With a 30s artificial delay in
    the driver, kills the source pod while migration is in-flight. Verifies
    the volume reaches 'available' with migration_status='success' on the
    destination host.
    """
    test_name = "test_inflight_migrate_same_vc"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight same-vCenter migration survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None

    try:
        # 1. Create a volume (small, just need FCD metadata)
        print("\n  Creating volume (1GB)...")
        volume_id = create_volume(f"test-gs-migrate-{int(time.time())}", size=1)
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create volume")
        print(f"  Volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=120)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 2. Get the current host
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Source deployment: {deployment}")

        # 3. Determine destination host (swap vc-a-0 ↔ vc-a-1)
        if "vc-a-0" in host:
            dest_host = host.replace("vc-a-0", "vc-a-1")
        elif "vc-a-1" in host:
            dest_host = host.replace("vc-a-1", "vc-a-0")
        else:
            return TestResult(test_name, False, time.time() - start,
                             f"Unexpected host format: {host}")
        print(f"  Destination host: {dest_host}")

        # 4. Get source pod
        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running source pod")
        print(f"  Source pod: {pod_name}")

        # 5. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 6. Initiate migration (requires admin)
        print(f"\n  Initiating migration to {dest_host}...")
        result = openstack_admin("volume", "migrate",
                                 "--host", dest_host, volume_id)
        if result.returncode != 0:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             f"Migration command failed: {result.stderr.strip()}")

        # 7. Wait for migration to be in-flight
        print("  Waiting for migration to start...")
        time.sleep(5)
        # Check migration_status via hammer
        cmd = [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "volume-show",
               volume_id, "--no-color"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                                env={**os.environ, "COLUMNS": "300"})
        migration_status = ""
        for line in result.stdout.split("\n"):
            if "migration_status" in line:
                parts = line.split("│")
                if len(parts) >= 3:
                    migration_status = parts[2].strip()
        print(f"  Migration status: {migration_status}")

        if migration_status not in ("migrating", "starting", "success"):
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             f"Unexpected migration_status: {migration_status}")

        if migration_status == "success":
            print("  WARNING: Migration already completed (too fast)")
        else:
            # 8. Kill the source pod while migration is in-flight
            print(f"  Migration in-flight. Killing source pod! "
                  f"[{time.strftime('%H:%M:%S')}]")
            kubectl("delete", "pod", pod_name, "--wait=false")

        # 9. Poll volume status until available + migration success
        print("\n  Polling volume status (old pod draining)...")
        deadline = time.time() + 300
        final_status = None
        final_migration = None
        while time.time() < deadline:
            vol_status = get_volume_status(volume_id)
            # Check migration_status
            result = subprocess.run(
                [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "volume-show",
                 volume_id, "--no-color"],
                capture_output=True, text=True, timeout=30,
                env={**os.environ, "COLUMNS": "300"})
            mig_status = ""
            for line in result.stdout.split("\n"):
                if "migration_status" in line:
                    parts = line.split("│")
                    if len(parts) >= 3:
                        mig_status = parts[2].strip()

            if vol_status == "available" and mig_status == "success":
                final_status = "available"
                final_migration = "success"
                print(f"  Volume available, migration_status=success ✅ "
                      f"[{time.strftime('%H:%M:%S')}]")
                break
            elif mig_status == "error":
                final_status = vol_status
                final_migration = "error"
                print(f"  Migration FAILED (status={vol_status}, "
                      f"migration_status=error)")
                break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = get_volume_status(volume_id)
            final_migration = "timeout"

        # 10. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 11. Wait for new pod
        if migration_status != "success":
            new_pod = wait_for_new_pod_ready(deployment, pod_name)

        # 12. Verify volume is on destination host
        new_host = get_volume_host(volume_id)
        print(f"  Final host: {new_host}")
        host_correct = (new_host and dest_host.split("#")[0] in new_host)

        passed = (final_status == "available" and
                  final_migration == "success" and host_correct)
        msg = (f"Volume status='{final_status}', "
               f"migration='{final_migration}', "
               f"host={'correct' if host_correct else 'WRONG'}")
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start,
                         f"Exception: {e}")
    finally:
        if volume_id:
            time.sleep(5)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)


# =============================================================================
# Test 17: In-flight volume migration (cross vCenter) survives pod termination
# =============================================================================


def test_inflight_migrate_cross_vc() -> TestResult:
    """Test 17: In-flight volume migration (cross-datastore) survives pod termination.

    Creates a volume on one datastore, initiates migration to a different
    datastore on the same host. This is a real FCD relocate that copies data
    between NFS datastores (takes 30-60s+ for 16GB). Kills the source pod
    while migration is in-flight. Verifies the volume reaches 'available'
    with migration_status='success'.
    """
    test_name = "test_inflight_migrate_cross_vc"
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"  Verify in-flight cross-datastore migration survives pod termination")
    print(f"{'='*70}")
    start = time.time()
    volume_id = None

    try:
        # 1. Create a volume from image (16GB — gives real data to migrate)
        print("\n  Creating volume from image (16GB)...")
        volume_id = create_volume_from_image(
            f"test-gs-migrate-xds-{int(time.time())}")
        if not volume_id:
            return TestResult(test_name, False, time.time() - start,
                             "Failed to create volume")
        print(f"  Volume ID: {volume_id}")

        vol_status = poll_volume_status(volume_id, ["available"], timeout=600)
        if vol_status != "available":
            return TestResult(test_name, False, time.time() - start,
                             f"Volume stuck in '{vol_status}'")

        # 2. Get the current host and pool
        host = get_volume_host(volume_id)
        if not host:
            return TestResult(test_name, False, time.time() - start,
                             "Could not determine volume host")
        deployment = host_to_deployment(host)
        print(f"  Volume host: {host}")
        print(f"  Source deployment: {deployment}")

        # 3. Determine destination — same host, different datastore
        #    Source: cinder-volume-vmware-vc-a-X@vmware_fcd#nfs_stnpca2_md004_ds01
        #    Dest:   cinder-volume-vmware-vc-a-X@vmware_fcd#nfs_stnpca1_bb097_ds01
        #    This forces a real data copy between NFS datastores.
        backend = host.split("#")[0]  # e.g. cinder-volume-vmware-vc-a-1@vmware_fcd
        source_pool = host.split("#")[1] if "#" in host else ""

        # Pick a different pool on the same backend
        dest_host = None
        result = openstack_admin("volume", "backend", "pool", "list",
                                 "-f", "value")
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if backend in line and source_pool not in line:
                    dest_host = line.strip()
                    break

        if not dest_host:
            return TestResult(test_name, False, time.time() - start,
                             f"Could not find alternative pool on {backend}")
        print(f"  Destination host: {dest_host}")

        # 4. Get source pod
        pod_name = get_pod_for_deployment(deployment)
        if not pod_name:
            return TestResult(test_name, False, time.time() - start,
                             "Could not find running source pod")
        print(f"  Source pod: {pod_name}")

        # 5. Start log stream
        log_file = OUTPUT_DIR / f"{test_name}-old-pod.log"
        log_stream = start_log_stream(pod_name, deployment, log_file)

        # 6. Initiate migration (requires admin)
        print(f"\n  Initiating cross-datastore migration to {dest_host}...")
        result = openstack_admin("volume", "migrate",
                                 "--host", dest_host, volume_id)
        if result.returncode != 0:
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             f"Migration command failed: {result.stderr.strip()}")

        # 7. Wait for migration to be in-flight
        print("  Waiting 10s for migration to start...")
        time.sleep(10)
        cmd = [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "volume-show",
               volume_id, "--no-color"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                                env={**os.environ, "COLUMNS": "300"})
        migration_status = ""
        for line in result.stdout.split("\n"):
            if "migration_status" in line:
                parts = line.split("│")
                if len(parts) >= 3:
                    migration_status = parts[2].strip()
        print(f"  Migration status: {migration_status}")

        if migration_status == "success":
            print("  WARNING: Migration already completed (too fast)")
        elif migration_status == "error":
            stop_log_stream(log_stream, timeout=5)
            return TestResult(test_name, False, time.time() - start,
                             "Migration failed immediately")
        else:
            # 8. Kill the source pod while migration is in-flight
            print(f"  Migration in-flight. Killing source pod! "
                  f"[{time.strftime('%H:%M:%S')}]")
            kubectl("delete", "pod", pod_name, "--wait=false")

        # 9. Poll volume status
        print("\n  Polling volume status (old pod draining, data copying)...")
        deadline = time.time() + 900
        final_status = None
        final_migration = None
        while time.time() < deadline:
            vol_status = get_volume_status(volume_id)
            result = subprocess.run(
                [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "volume-show",
                 volume_id, "--no-color"],
                capture_output=True, text=True, timeout=30,
                env={**os.environ, "COLUMNS": "300"})
            mig_status = ""
            for line in result.stdout.split("\n"):
                if "migration_status" in line:
                    parts = line.split("│")
                    if len(parts) >= 3:
                        mig_status = parts[2].strip()

            if vol_status == "available" and mig_status == "success":
                final_status = "available"
                final_migration = "success"
                print(f"  Volume available, migration_status=success ✅ "
                      f"[{time.strftime('%H:%M:%S')}]")
                break
            elif mig_status == "error":
                final_status = vol_status
                final_migration = "error"
                print(f"  Migration FAILED [{time.strftime('%H:%M:%S')}]")
                break
            time.sleep(POLL_INTERVAL)

        if final_status is None:
            final_status = get_volume_status(volume_id)
            final_migration = "timeout"

        # 10. Stop log stream
        stop_log_stream(log_stream, timeout=30)

        # 11. Wait for new pod
        if migration_status not in ("success",):
            new_pod = wait_for_new_pod_ready(deployment, pod_name)

        # 12. Verify final host
        new_host = get_volume_host(volume_id)
        print(f"  Final host: {new_host}")
        host_correct = (new_host and dest_host.split("#")[1] in new_host)

        passed = (final_status == "available" and
                  final_migration == "success" and host_correct)
        msg = (f"Volume status='{final_status}', "
               f"migration='{final_migration}', "
               f"host={'correct' if host_correct else new_host}")
        return TestResult(test_name, passed, time.time() - start, msg)

    except Exception as e:
        return TestResult(test_name, False, time.time() - start,
                         f"Exception: {e}")
    finally:
        if volume_id:
            time.sleep(5)
            status = get_volume_status(volume_id)
            if status in ("available", "error"):
                delete_volume(volume_id)


# =============================================================================
# Volume-pod concurrency test family
# =============================================================================
#
# Two suites both built on run_volume_pod_concurrency_test():
#   - sameop:  3 instances of the SAME operation, one killed pod
#   - mixed:   different operation types, one killed pod
#
# Each test produces a per-test timing diagram artifact
# (`<test>-timeline.md`) showing operation launch / in-flight / complete
# times alongside pod kill / SIGTERM / Phase 2 drain / old-pod-exit /
# new-pod-ready times as both a markdown table and a Mermaid Gantt chart.
# =============================================================================


# ---------- BatchOp factories (one per operation kind) ----------------------

def _make_op_create_from_image(label: str) -> BatchOp:
    """One create-volume-from-image op, pinned via --hint same_host=anchor."""

    def setup(host, deployment, anchor):
        return {}

    def launch(host, deployment, anchor, state):
        return create_volume_from_image(
            f"test-gs-vpc-{label}-{int(time.time())}",
            hint_same_host=anchor)

    return BatchOp(
        label=label,
        setup_fn=setup,
        launch_fn=launch,
        inflight_statuses=("creating", "downloading"),
        success_statuses=("available",),
    )


def _make_op_extend(label: str) -> BatchOp:
    """One extend op. Setup pre-creates a small empty volume on target host.

    Uses an empty 1GB volume (fast setup, ~5s) rather than image-create
    (~60s). The FCD driver's [GS-TEST] 30s artificial delay in
    extend_volume() keeps the op in-flight long enough to land a kill
    regardless of source data size.
    """

    def setup(host, deployment, anchor):
        vid = create_volume(
            f"test-gs-vpc-{label}-src-{int(time.time())}", size=1)
        if not vid:
            raise RuntimeError(f"{label} setup: volume create failed")
        status = poll_volume_status(vid, ["available"], timeout=180)
        if status != "available":
            raise RuntimeError(
                f"{label} setup: volume stuck in '{status}'")
        return {"src_volume_id": vid}

    def launch(host, deployment, anchor, state):
        vid = state["src_volume_id"]
        # Extend from 1GB to 2GB (size doesn't affect the [GS-TEST] delay)
        result = openstack("volume", "set", "--size", "2",
                           vid, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"extend command failed: {result.stderr.strip()}")
        # Poll briefly for status to flip 'available' → 'extending' so
        # the runner's in-flight loop doesn't false-trigger on the
        # pre-extend 'available' state. Long timeout covers scheduler
        # dispatch latency under load (10-100s).
        deadline = time.time() + 180
        while time.time() < deadline:
            s = get_volume_status(vid)
            if s == "extending":
                break
            time.sleep(1)
        return vid

    op = BatchOp(
        label=label,
        setup_fn=setup,
        launch_fn=launch,
        inflight_statuses=("extending",),
        success_statuses=("available",),
    )

    # Track setup volume so the runner can include it in placement checks
    # and cleanup. We populate this from inside setup via a closure trick:
    # rewrap setup to also append to op.setup_resource_ids.
    orig_setup = op.setup_fn

    def setup_with_tracking(host, deployment, anchor):
        state = orig_setup(host, deployment, anchor)
        if state.get("src_volume_id"):
            op.setup_resource_ids.append(state["src_volume_id"])
        return state

    op.setup_fn = setup_with_tracking
    return op


def _make_op_clone(label: str,
                   shared_source_state_key: str = "clone_source") -> BatchOp:
    """One clone op. Setup ensures a shared source volume exists on host.

    All clone ops in the same test share one source volume created by the
    first op's setup. Subsequent setups detect the existing source via the
    anchor + hammer placement check.
    """
    # Use a module-level cache keyed by (test_run_id, source) — but the
    # runner doesn't expose a per-test scratchpad. Instead, the first
    # clone op's setup creates the source and stores its id on the op
    # itself; later clones read it via shared module state.
    # Simpler: each clone op creates its OWN source. That doubles cost but
    # keeps ops independent. With 3 clones that's 3 sources; acceptable.

    def setup(host, deployment, anchor):
        src = create_volume_from_image(
            f"test-gs-vpc-{label}-src-{int(time.time())}",
            hint_same_host=anchor)
        if not src:
            raise RuntimeError(f"{label} setup: source create failed")
        status = poll_volume_status(src, ["available"], timeout=600)
        if status != "available":
            raise RuntimeError(
                f"{label} setup: source stuck in '{status}'")
        return {"clone_source_id": src}

    def launch(host, deployment, anchor, state):
        src = state["clone_source_id"]
        return create_volume_from_volume(
            src, f"test-gs-vpc-{label}-{int(time.time())}",
            size=TEST_VOLUME_SIZE)

    op = BatchOp(
        label=label,
        setup_fn=setup,
        launch_fn=launch,
        inflight_statuses=("creating", "downloading"),
        success_statuses=("available",),
    )

    orig_setup = op.setup_fn

    def setup_with_tracking(host, deployment, anchor):
        state = orig_setup(host, deployment, anchor)
        if state.get("clone_source_id"):
            op.setup_resource_ids.append(state["clone_source_id"])
        return state

    op.setup_fn = setup_with_tracking
    return op


def _make_op_snapshot_delete(label: str) -> BatchOp:
    """One snapshot-delete op. Setup pre-creates volume + snapshot."""

    def setup(host, deployment, anchor):
        vid = create_volume(
            f"test-gs-vpc-{label}-src-{int(time.time())}", size=1)
        if not vid:
            raise RuntimeError(f"{label} setup: volume create failed")
        status = poll_volume_status(vid, ["available"], timeout=120)
        if status != "available":
            raise RuntimeError(
                f"{label} setup: volume stuck in '{status}'")
        snap = create_snapshot(
            vid, f"test-gs-vpc-{label}-snap-{int(time.time())}")
        if not snap:
            raise RuntimeError(f"{label} setup: snapshot create failed")
        snap_status = poll_snapshot_status(snap, ["available"], timeout=300)
        if snap_status != "available":
            raise RuntimeError(
                f"{label} setup: snapshot stuck in '{snap_status}'")
        return {"src_volume_id": vid, "snapshot_id": snap}

    def launch(host, deployment, anchor, state):
        snap = state["snapshot_id"]
        openstack("volume", "snapshot", "delete", snap, check=False)
        return snap

    op = BatchOp(
        label=label,
        setup_fn=setup,
        launch_fn=launch,
        inflight_statuses=("deleting",),
        success_statuses=(),  # success = snapshot disappears
        treat_unknown_as_success=True,
    )

    orig_setup = op.setup_fn

    def setup_with_tracking(host, deployment, anchor):
        state = orig_setup(host, deployment, anchor)
        if state.get("src_volume_id"):
            op.setup_resource_ids.append(state["src_volume_id"])
        return state

    op.setup_fn = setup_with_tracking
    return op


# ---------- Same-op suite ---------------------------------------------------

def test_volumepod_sameop_create_x3() -> TestResult:
    """Same-op concurrency: 3 x create-from-image on one killed volume pod."""
    return run_volume_pod_concurrency_test(
        test_name="test_volumepod_sameop_create_x3",
        description=("3 concurrent create-from-image ops on one volume pod; "
                     "kill pod after all are 'creating'"),
        ops=[
            _make_op_create_from_image("create-1"),
            _make_op_create_from_image("create-2"),
            _make_op_create_from_image("create-3"),
        ],
        inflight_wait_timeout=120,
        final_timeout=900,
    )


def test_volumepod_sameop_extend_x3() -> TestResult:
    """Same-op concurrency: 3 x extend on one killed volume pod."""
    return run_volume_pod_concurrency_test(
        test_name="test_volumepod_sameop_extend_x3",
        description=("3 concurrent extend ops on one volume pod; "
                     "kill pod after all are 'extending'"),
        ops=[
            _make_op_extend("extend-1"),
            _make_op_extend("extend-2"),
            _make_op_extend("extend-3"),
        ],
        inflight_wait_timeout=120,
        final_timeout=900,
    )


def test_volumepod_sameop_clone_x3() -> TestResult:
    """Same-op concurrency: 3 x clone on one killed volume pod."""
    return run_volume_pod_concurrency_test(
        test_name="test_volumepod_sameop_clone_x3",
        description=("3 concurrent clone ops on one volume pod; "
                     "kill pod after all are 'creating'"),
        ops=[
            _make_op_clone("clone-1"),
            _make_op_clone("clone-2"),
            _make_op_clone("clone-3"),
        ],
        inflight_wait_timeout=120,
        final_timeout=900,
    )


# ---------- Mixed-op suite --------------------------------------------------

def test_volumepod_mixed_create3_extend1() -> TestResult:
    """Mixed-op concurrency: 3 creates + 1 extend on one killed volume pod.

    Canonicalised version of the legacy ``test_inflight_multiple_operations``
    case, now driven by the shared runner with timing-diagram output.
    """
    return run_volume_pod_concurrency_test(
        test_name="test_volumepod_mixed_create3_extend1",
        description=("3 create-from-image + 1 extend on one volume pod; "
                     "kill pod after all are in-flight"),
        ops=[
            _make_op_create_from_image("create-1"),
            _make_op_create_from_image("create-2"),
            _make_op_create_from_image("create-3"),
            _make_op_extend("extend-1"),
        ],
        inflight_wait_timeout=120,
        final_timeout=900,
    )


def test_volumepod_mixed_create_extend_clone() -> TestResult:
    """Mixed-op concurrency: create + extend + clone on one killed volume pod."""
    return run_volume_pod_concurrency_test(
        test_name="test_volumepod_mixed_create_extend_clone",
        description=("1 create-from-image + 1 extend + 1 clone on one "
                     "volume pod; kill pod after all are in-flight"),
        ops=[
            _make_op_create_from_image("create-1"),
            _make_op_extend("extend-1"),
            _make_op_clone("clone-1"),
        ],
        inflight_wait_timeout=120,
        final_timeout=900,
    )


def test_volumepod_mixed_create_extend_snapshot_delete() -> TestResult:
    """Mixed-op concurrency: create + extend + snapshot-delete on killed pod."""
    return run_volume_pod_concurrency_test(
        test_name="test_volumepod_mixed_create_extend_snapshot_delete",
        description=("1 create-from-image + 1 extend + 1 snapshot-delete "
                     "on one volume pod; kill pod after all are in-flight"),
        ops=[
            _make_op_create_from_image("create-1"),
            _make_op_extend("extend-1"),
            _make_op_snapshot_delete("snapshot_delete-1"),
        ],
        inflight_wait_timeout=180,  # snapshot setup adds time
        final_timeout=900,
    )


# =============================================================================
# Main
# =============================================================================

ALL_TESTS = {
    "test_idle_shutdown": test_idle_shutdown,
    "test_inflight_volume_create": test_inflight_volume_create,
    "test_inflight_backup": test_inflight_backup,
    "test_scheduler_reroutes": test_scheduler_reroutes,
    "test_inflight_cast_during_drain_completes": test_inflight_cast_during_drain_completes,
    "test_inflight_volume_delete": test_inflight_volume_delete,
    "test_inflight_volume_clone": test_inflight_volume_clone,
    "test_inflight_snapshot_create": test_inflight_snapshot_create,
    "test_inflight_snapshot_delete": test_inflight_snapshot_delete,
    "test_inflight_backup_kill_volume_pod": test_inflight_backup_kill_volume_pod,
    "test_inflight_volume_extend": test_inflight_volume_extend,
    "test_inflight_multiple_operations": test_inflight_multiple_operations,
    "test_inflight_restore_kill_backup_pod": test_inflight_restore_kill_backup_pod,
    "test_inflight_restore_kill_during_attach": test_inflight_restore_kill_during_attach,
    "test_inflight_restore_kill_during_detach": test_inflight_restore_kill_during_detach,
    "test_inflight_restore_kill_during_finalize": test_inflight_restore_kill_during_finalize,
    "test_inflight_migrate_same_vc": test_inflight_migrate_same_vc,
    "test_inflight_migrate_cross_vc": test_inflight_migrate_cross_vc,
    # Volume-pod concurrency family (shared runner + timing diagrams)
    "test_volumepod_sameop_create_x3": test_volumepod_sameop_create_x3,
    "test_volumepod_sameop_extend_x3": test_volumepod_sameop_extend_x3,
    "test_volumepod_sameop_clone_x3": test_volumepod_sameop_clone_x3,
    "test_volumepod_mixed_create3_extend1": test_volumepod_mixed_create3_extend1,
    "test_volumepod_mixed_create_extend_clone": test_volumepod_mixed_create_extend_clone,
    "test_volumepod_mixed_create_extend_snapshot_delete": test_volumepod_mixed_create_extend_snapshot_delete,
}


# Logical groupings for --group execution. Names map to lists of test names.
TEST_GROUPS: dict = {
    "volume-pod-sameop": [
        "test_volumepod_sameop_create_x3",
        "test_volumepod_sameop_extend_x3",
        "test_volumepod_sameop_clone_x3",
    ],
    "volume-pod-mixed": [
        "test_volumepod_mixed_create3_extend1",
        "test_volumepod_mixed_create_extend_clone",
        "test_volumepod_mixed_create_extend_snapshot_delete",
    ],
    "volume-pod-concurrency": [
        "test_volumepod_sameop_create_x3",
        "test_volumepod_sameop_extend_x3",
        "test_volumepod_sameop_clone_x3",
        "test_volumepod_mixed_create3_extend1",
        "test_volumepod_mixed_create_extend_clone",
        "test_volumepod_mixed_create_extend_snapshot_delete",
    ],
    # KVM-focused group: tests that work without [GS-TEST] artificial
    # delays. Use with: --volume-type premium --volume-size 16
    # Image-creates are naturally slow (~30-60s download) so they
    # reliably stay in-flight during a pod kill.
    "kvm-graceful-shutdown": [
        "test_idle_shutdown",
        "test_inflight_volume_create",
        "test_inflight_cast_during_drain_completes",
        "test_inflight_volume_delete",
        "test_inflight_volume_extend",
        "test_inflight_volume_clone",
        "test_inflight_snapshot_create",
        "test_inflight_snapshot_delete",
        "test_inflight_multiple_operations",
        "test_volumepod_sameop_create_x3",
        "test_volumepod_mixed_create3_extend1",
    ],
}


def print_summary(results: list[TestResult]) -> None:
    """Print test summary to console."""
    print(f"\n{'='*70}")
    print("TEST RESULTS SUMMARY")
    print(f"{'='*70}")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.name} ({r.duration:.1f}s)")
        if r.message:
            print(f"         {r.message}")
        for w in r.warnings:
            print(f"         WARNING: {w}")

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\n  {passed}/{total} tests passed")

    if OUTPUT_DIR:
        print(f"\n  Log files and report: {OUTPUT_DIR}/")

    if passed < total:
        print("  SOME TESTS FAILED")
    else:
        print("  ALL TESTS PASSED")


def main():
    parser = argparse.ArgumentParser(
        description="Graceful shutdown integration tests for Cinder in qa-de-1"
    )
    parser.add_argument(
        "--test", "-t",
        choices=list(ALL_TESTS.keys()),
        help="Run a specific test (default: run all)",
    )
    parser.add_argument(
        "--group", "-g",
        choices=list(TEST_GROUPS.keys()),
        help="Run all tests in a named group "
             "(e.g. volume-pod-sameop, volume-pod-mixed, "
             "volume-pod-concurrency)",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available tests and groups",
    )
    parser.add_argument(
        "--cloud",
        default=OS_CLOUD,
        help=f"OpenStack cloud name (default: {OS_CLOUD})",
    )
    parser.add_argument(
        "--context",
        default=KUBE_CONTEXT,
        help=f"Kubernetes context (default: {KUBE_CONTEXT})",
    )
    parser.add_argument(
        "--volume-deployment",
        default=VOLUME_DEPLOYMENT,
        help=f"Volume deployment to test (default: {VOLUME_DEPLOYMENT})",
    )
    parser.add_argument(
        "--backup-deployment",
        default=BACKUP_DEPLOYMENT,
        help=f"Backup deployment to test (default: {BACKUP_DEPLOYMENT})",
    )
    parser.add_argument(
        "--volume-type",
        default=TEST_VOLUME_TYPE,
        help=f"Cinder volume type for test volumes (default: {TEST_VOLUME_TYPE})",
    )
    parser.add_argument(
        "--volume-size",
        type=int,
        default=TEST_VOLUME_SIZE,
        help=f"Volume size in GB for image-create tests (default: {TEST_VOLUME_SIZE})",
    )
    args = parser.parse_args()

    if args.list:
        print("Available tests:")
        for name, func in ALL_TESTS.items():
            print(f"  {name}: {func.__doc__.strip().split(chr(10))[0]}")
        print("\nAvailable groups:")
        for gname, members in TEST_GROUPS.items():
            print(f"  {gname}: {len(members)} tests")
            for m in members:
                print(f"    - {m}")
        return

    # Apply CLI overrides
    _apply_config(
        os_cloud=args.cloud,
        kube_context=args.context,
        volume_deployment=args.volume_deployment,
        backup_deployment=args.backup_deployment,
        volume_type=args.volume_type,
        volume_size=args.volume_size,
    )

    # Init output directory
    init_output_dir()

    # Pre-flight checks
    print("\nPre-flight checks...")
    print(f"  OpenStack cloud: {OS_CLOUD}")
    print(f"  Kubernetes context: {KUBE_CONTEXT}")
    print(f"  Volume deployment: {VOLUME_DEPLOYMENT}")
    print(f"  Backup deployment: {BACKUP_DEPLOYMENT}")

    result = kubectl("get", "pods", "-l", f"name={VOLUME_DEPLOYMENT}", "--no-headers", check=False)
    if result.returncode != 0:
        result = kubectl("get", "pods", "--no-headers", check=False)
        if result.returncode != 0:
            print(f"\n  ERROR: kubectl cannot access cluster: {result.stderr.strip()}")
            sys.exit(1)

    result = openstack("volume", "list", "--limit", "1", check=False)
    if result.returncode != 0:
        print(f"\n  ERROR: openstack CLI failed: {result.stderr.strip()}")
        sys.exit(1)

    print("  Pre-flight checks passed\n")

    # Run tests
    if args.test and args.group:
        print("  ERROR: --test and --group are mutually exclusive")
        sys.exit(2)
    if args.test:
        tests_to_run = {args.test: ALL_TESTS[args.test]}
    elif args.group:
        members = TEST_GROUPS[args.group]
        tests_to_run = {name: ALL_TESTS[name] for name in members}
    else:
        tests_to_run = ALL_TESTS

    results = []
    for name, test_func in tests_to_run.items():
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"\n  EXCEPTION in {name}: {e}")
            traceback.print_exc()
            results.append(TestResult(name, False, message=f"Unhandled exception: {e}"))

        # Wait between tests for pods to stabilize
        if len(tests_to_run) > 1:
            print("\n  Waiting 30s between tests for pod stabilization...")
            time.sleep(30)

    # Generate report
    report_md = generate_report(results)
    if OUTPUT_DIR:
        report_path = OUTPUT_DIR / "report.md"
        report_path.write_text(report_md)
        print(f"\n  Report saved to: {report_path}")

    print_summary(results)

    if not all(r.passed for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()

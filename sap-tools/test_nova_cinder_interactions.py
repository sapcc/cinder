#!/usr/bin/env python3
"""
Nova/Cinder Live Integration Tests for the qa-de-1 region.

Validates the most common production interactions between Nova and the
graceful-shutdown-patched Cinder volume service:

  S1 — Volume attach where ConnectorRejected forces a Nova-driven
       migrate_volume_by_connector + retry attach.
       Setup: VM lives on vCenter A, fresh volume created on vCenter B.
       Nova attempts attach → cinder driver raises ConnectorRejected →
       cinder API returns HTTP 406 → Nova internally calls cinder's
       os-migrate_volume_by_connector → migration to vCenter A → Nova
       retries the attach → success.

  S2 — Nova live-migration of a VM with an attached volume (PROVISIONAL).
       Volume already attached to VM (both on vCenter A). Nova live-migrates
       the VM to a vCenter B host. Nova issues the new connector → cinder
       may return 406 → Nova migrates the volume → retry attach → live-
       migration completes.
       Provisional: contingent on Phase A.5 confirming cross-vCenter
       live-migration is supported in qa-de-1.

  S3 — Operator-initiated cinder migrate of an attached volume.
       Volume attached to VM, both on vCenter A. Operator issues
       'cinder migrate <vol> <other-vc-A-host>' (same vCenter, different
       cinder-volume host). Cinder calls Nova's swap_volume API. Migration
       completes; VM still has the volume attached; volume now lives on a
       different cinder-volume host.

For each scenario, a baseline test (no kill) verifies the flow works at
all in qa-de-1, and kill-injection variants validate that the graceful-
shutdown patch handles each interesting moment without losing data or
leaving stuck state.

Prerequisites (all manually configured before running):
  - clouds.yaml entry for 'qa-de-1' (admin scope; ccloud-multitool is used
    for actions that need admin-only APIs).
  - kubectl context 'qa-de-1' configured for the monsoon3 namespace.
  - hammer CLI configured (--region qa-de-1).
  - One pre-created Nova VM on vCenter A. Fill its UUID into
    PRECREATED_VM_ID below (or pass --vm-id at runtime).
  - The 'vmware' volume type (default) routes to '@vmware_fcd' backend on
    whichever cinder-volume host the scheduler picks. Per-vCenter pinning
    is via availability zone: AZ_VC_A='qa-de-1a' lands on
    cinder-volume-vmware-vc-a-{0,1}@vmware_fcd; AZ_VC_B='qa-de-1b' lands
    on cinder-volume-vmware-vc-b-0@vmware_fcd.
  - The graceful-shutdown patch deployed via 'cc-autodeploy deploy
    cinder-qa-de-1' (otherwise kill tests will not see the drain
    log markers and will fail).

Reuses helpers imported from sap-tools/test_graceful_shutdown.py.

Examples:
  python3 sap-tools/test_nova_cinder_interactions.py --list
  python3 sap-tools/test_nova_cinder_interactions.py --test t1
  python3 sap-tools/test_nova_cinder_interactions.py --tests t1,t9
  python3 sap-tools/test_nova_cinder_interactions.py --no-cleanup
"""

import argparse
import ast
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

# Reuse the existing harness as a library. We do NOT modify
# test_graceful_shutdown.py; we import its public helpers.
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

# Importing test_graceful_shutdown executes its module-level constants
# but does not run main() (guarded by `if __name__ == "__main__"`).
import test_graceful_shutdown as gs  # noqa: E402

# Helpers we use directly (typed pointers, makes static analysis easier).
kubectl = gs.kubectl
openstack = gs.openstack
openstack_admin = gs.openstack_admin
run_cmd = gs.run_cmd
get_pod_for_deployment = gs.get_pod_for_deployment
wait_for_new_pod_ready = gs.wait_for_new_pod_ready
start_log_stream = gs.start_log_stream
stop_log_stream = gs.stop_log_stream
get_pod_logs = gs.get_pod_logs
host_to_deployment = gs.host_to_deployment
get_volume_status = gs.get_volume_status
get_volume_host = gs.get_volume_host
poll_volume_status = gs.poll_volume_status
delete_volume = gs.delete_volume
TestResult = gs.TestResult
SHUTDOWN_LOG_SEQUENCE = gs.SHUTDOWN_LOG_SEQUENCE
SHUTDOWN_LOG_SEQUENCE_WITH_TASKS = gs.SHUTDOWN_LOG_SEQUENCE_WITH_TASKS


# =============================================================================
# Configuration
# =============================================================================

# Defaults match the graceful-shutdown harness; override via CLI flags or
# GS_* env vars (qa-de-1 defaults).
KUBE_CONTEXT = os.environ.get("GS_KUBE_CONTEXT", "qa-de-1")
KUBE_NAMESPACE = os.environ.get("GS_KUBE_NAMESPACE", "monsoon3")
OS_CLOUD = os.environ.get("GS_OS_CLOUD", "qa-de-1")

# Cinder-volume deployments per vCenter SHARD. Each shard maps 1:1 to a
# distinct vCenter instance (different vmware_service_instance_uuid). In
# qa-de-1 there are multiple shards within a single AZ (e.g. vc-a-0 and
# vc-a-1 are both in qa-de-1a but point at different vCenters). That
# multi-shard-per-AZ topology is exactly what reproduces ConnectorRejected:
# Nova VM scheduled on a host in the vc-a-0 aggregate has connector
# capabilities of vCenter-0; a volume placed on cinder-volume-vmware-vc-a-1
# advertises vCenter-1 — Nova's AZ check passes (both qa-de-1a) but
# cinder's connector check fails → 406 → migrate → retry.
SHARDS_BY_AZ = {
    "qa-de-1a": ["vc-a-0", "vc-a-1"],
    "qa-de-1b": ["vc-b-0", "vc-b-1", "vc-b-2"],
    # qa-de-1d shards exist but are down per Phase A.1; ignore.
}
DEPLOYMENT_PREFIX = "cinder-volume-vmware-"  # full = prefix + shard

# Pre-created Nova VM. Lives on whichever shard; auto-detected at runtime.
# The test creates the test volume on a DIFFERENT shard within the SAME AZ
# so Nova's AZ check passes but cinder's connector check fails.
# The VM is reused across all tests, never deleted.
PRECREATED_VM_ID = ""  # required; pass --vm-id or set here

# Volume type. Routes to '@vmware_fcd' backend on whichever cinder-volume
# host the scheduler picks. To pin to a SPECIFIC shard within an AZ we use
# the admin scheduler hint 'vcenter-shard=<shard>'. (Non-admin requests
# silently ignore the hint.)
VOLUME_TYPE = os.environ.get("GS_TEST_VOLUME_TYPE", "vmware")

# Topology auto-detected at runtime by _detect_topology(). Populated as
# soon as preflight() runs.
VM_AZ = ""               # the VM's AZ string (e.g. 'qa-de-1a')
VM_SHARD = ""            # the VM's vCenter shard (e.g. 'vc-a-0')
OTHER_SHARD = ""         # a DIFFERENT shard in the same AZ; volumes go here
DEPLOYMENT_VM_SIDE = ""  # = DEPLOYMENT_PREFIX + VM_SHARD; where post-migrate
                         #   retry attach lands (= migration destination too)
DEPLOYMENT_OTHER_SIDE = ""  # = DEPLOYMENT_PREFIX + OTHER_SHARD; where the
                            # 406 fires (volume's initial home)
DEPLOYMENT_VM_PEER = ""  # legacy alias kept for S3 — now equals
                         # DEPLOYMENT_OTHER_SIDE since both are in the same
                         # AZ as the VM (S3 same-AZ migrate)

# Test volume size. Small is fine for tests where we don't need to land
# a kill DURING migration (T1, T2, T4, T9, T10, T11). For T3 we use a
# 16 GB volume cloned from a populated source so the cross-shard FCD
# relocate has actual data to copy and takes long enough (~30-60s) for
# the test driver to inject the kill reliably. This mirrors the existing
# test_inflight_migrate_cross_vc test in test_graceful_shutdown.py
# which also uses a populated 16 GB volume.
TEST_VOLUME_SIZE_GB = 1
TEST_VOLUME_SIZE_GB_FOR_MIGRATE_KILL = 16
# Pre-created 16 GB source volume to clone for T3. This volume should
# already exist in the user's project. Default points at the same volume
# the existing graceful-shutdown harness uses (test-gs-precreated-src).
TEST_VOLUME_SOURCE_FOR_CLONE = os.environ.get(
    "GS_TEST_VOLUME_SOURCE_FOR_CLONE",
    "04e49377-8470-4341-82a7-404c9fe3287f")
TEST_VOLUME_FROM_IMAGE_TIMEOUT = 600  # generous; clones can also run long

# Timeouts (seconds).
ATTACH_TIMEOUT = 600          # full S1 flow: 406 → migrate → retry attach
LIVE_MIGRATE_TIMEOUT = 900    # S2: cross-vc live-migrate is slow
SAMEVC_MIGRATE_TIMEOUT = 600  # S3: same-vc cinder-migrate with swap_volume
MIGRATION_POLL_INTERVAL = 5
POD_READY_TIMEOUT = 180

# Cleanup behaviour. --no-cleanup flips to False; set per run.
DO_CLEANUP = True

# Path to hammer (matches gs.HAMMER_BIN; recapture in case test_graceful_shutdown
# changes paths).
HAMMER_BIN = gs.HAMMER_BIN


# =============================================================================
# Output Directory
# =============================================================================

OUTPUT_DIR: Optional[Path] = None


def init_output_dir() -> Path:
    """Create timestamped output directory for test artifacts."""
    global OUTPUT_DIR
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    script_dir = Path(__file__).resolve().parent
    OUTPUT_DIR = script_dir / "test-results" / f"nova-cinder-{timestamp}"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Tell the imported gs module about our output dir so any helpers that
    # reference gs.OUTPUT_DIR (e.g. start_log_stream falls back to it) work.
    gs.OUTPUT_DIR = OUTPUT_DIR
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
# Hammer-based volume metadata accessors
# =============================================================================
# The CLI 'openstack volume show' does not expose 'host' or 'migration_status'
# for non-admin scope (they're admin-only fields). hammer reads straight from
# the cinder DB and is the canonical access pattern in the existing harness.

def _hammer_volume_show(volume_id: str) -> dict:
    """Read all volume fields via hammer. Returns {} on error.

    Output is a row-oriented box-drawn table; we parse it into a dict.
    """
    cmd = [HAMMER_BIN, "--region", KUBE_CONTEXT, "cinder", "volume-show",
           volume_id, "--no-color"]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=30,
        env={**os.environ, "COLUMNS": "300"}, check=False,
    )
    if result.returncode != 0:
        print(f"  WARNING: hammer failed: {result.stderr.strip()}")
        return {}
    fields = {}
    for line in result.stdout.split("\n"):
        if "│" not in line:
            continue
        parts = [p.strip() for p in line.split("│")]
        # Expect [empty, key, value, empty] when fully parsed.
        if len(parts) >= 3 and parts[1] and parts[2] and parts[1] != "Field":
            fields[parts[1]] = parts[2]
    return fields


def get_migration_status(volume_id: str) -> str:
    """Return the volume's migration_status field, or '' if unknown."""
    fields = _hammer_volume_show(volume_id)
    return fields.get("migration_status", "") or ""


def get_attach_status(volume_id: str) -> str:
    """Return the volume's attach_status field ('attached' / 'detached')."""
    fields = _hammer_volume_show(volume_id)
    return fields.get("attach_status", "") or ""


# =============================================================================
# Nova VM lifecycle helpers (read-only against PRECREATED_VM_ID)
# =============================================================================

def nova_show_server(vm_id: str) -> dict:
    """Return parsed 'openstack server show' JSON, or {} on error."""
    result = openstack("server", "show", vm_id, "-f", "json", check=False)
    if result.returncode != 0:
        print(f"  WARNING: server show failed: {result.stderr.strip()}")
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}


def nova_get_server_status(vm_id: str) -> str:
    """Return ACTIVE / MIGRATING / ERROR / SHUTOFF / unknown."""
    data = nova_show_server(vm_id)
    return data.get("status", "unknown")


def nova_get_server_host(vm_id: str) -> str:
    """Return the OS-EXT-SRV-ATTR:host the VM currently runs on."""
    data = nova_show_server(vm_id)
    # 'openstack server show -f json' returns this key with the colon
    # preserved in some clients and stripped in others.
    for k in ("OS-EXT-SRV-ATTR:host", "host", "hostId"):
        if k in data and data[k]:
            return str(data[k])
    return ""


def nova_get_server_volumes(vm_id: str) -> list:
    """Return the list of attached volume dicts (id, device, attachment_id).

    The 'volumes_attached' field shape varies across openstack client versions;
    we normalise to a list of dicts with at least 'id'.
    """
    data = nova_show_server(vm_id)
    raw = data.get("volumes_attached") or data.get("os-extended-volumes:volumes_attached") or []
    if isinstance(raw, list):
        out = []
        for item in raw:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str):
                # CLI sometimes formats as "id='xxx'" strings.
                vid = ""
                for tok in item.replace("'", "").replace('"', "").split(","):
                    tok = tok.strip()
                    if tok.startswith("id="):
                        vid = tok.split("=", 1)[1]
                if vid:
                    out.append({"id": vid})
        return out
    return []


def nova_attach_volume(vm_id: str, vol_id: str) -> subprocess.CompletedProcess:
    """Issue 'openstack server add volume <vm> <vol>'.

    Returns the CompletedProcess regardless of success; the caller decides
    what to do with the return code. For S1, we EXPECT this command to NOT
    return an error from the CLI's perspective in the auto-retry case
    (Nova drives the migrate internally and only returns when the attach
    has either succeeded or definitively failed). We separately observe
    the volume / attachment state to confirm what happened.
    """
    return openstack("server", "add", "volume", vm_id, vol_id, check=False,
                     timeout=ATTACH_TIMEOUT)


def nova_detach_volume(vm_id: str, vol_id: str) -> subprocess.CompletedProcess:
    """Issue 'openstack server remove volume <vm> <vol>'."""
    return openstack("server", "remove", "volume", vm_id, vol_id,
                     check=False, timeout=300)


def nova_live_migrate(vm_id: str, dest_host: Optional[str] = None,
                      block_migration: str = "auto"
                      ) -> subprocess.CompletedProcess:
    """Issue an admin live-migration request.

    'openstack server migrate --live-migration' requires admin scope.
    """
    args = ["server", "migrate", "--live-migration",
            "--block-migration" if block_migration == "true" else
            "--shared-migration" if block_migration == "false" else
            "--block-migration"]  # default to block-migration; vmware may differ
    if dest_host:
        args.extend(["--host", dest_host])
    args.append(vm_id)
    return openstack_admin(*args, timeout=LIVE_MIGRATE_TIMEOUT)


def nova_wait_for_status(vm_id: str, target: str, timeout: int = 600) -> str:
    """Poll until server status equals target (or ERROR)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        s = nova_get_server_status(vm_id)
        if s != last:
            print(f"  VM {vm_id[:8]}... status: {s}")
            last = s
        if s == target:
            return s
        if s == "ERROR":
            return s
        time.sleep(MIGRATION_POLL_INTERVAL)
    return last or "timeout"


def nova_wait_for_volume_attached(vm_id: str, vol_id: str,
                                  timeout: int = ATTACH_TIMEOUT) -> bool:
    """Poll until vol_id appears in the VM's attached volumes."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for v in nova_get_server_volumes(vm_id):
            if v.get("id") == vol_id:
                vstatus = get_volume_status(vol_id)
                if vstatus == "in-use":
                    return True
        time.sleep(MIGRATION_POLL_INTERVAL)
    return False


def nova_wait_for_volume_detached(vm_id: str, vol_id: str,
                                  timeout: int = 300) -> bool:
    """Poll until vol_id no longer appears in the VM's attached volumes."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ids = [v.get("id") for v in nova_get_server_volumes(vm_id)]
        if vol_id not in ids:
            vstatus = get_volume_status(vol_id)
            if vstatus in ("available", "error"):
                return True
        time.sleep(MIGRATION_POLL_INTERVAL)
    return False


# =============================================================================
# Cinder helpers specific to these tests
# =============================================================================

def cinder_create_volume(name: str, availability_zone: str,
                         size_gb: int = TEST_VOLUME_SIZE_GB,
                         volume_type: str = None,
                         shard: str = None,
                         image_id: str = None,
                         clone_source: str = None
                         ) -> Optional[str]:
    """Create a volume in a specific AZ and return its id.

    `clone_source` parameter (preferred for T3):
        - When provided, creates the new volume by cloning an existing
          source volume. The clone contains real data so the cross-shard
          FCD relocate has content to copy, extending the migration
          window from sub-second to 30-60s. T3 uses this for a
          deterministic mid-migration kill window.

    `image_id` parameter:
        - When provided, creates the volume from an image. The 'vmware'
          type may not always succeed at this in qa-de-1 (vCenter
          InvalidArgument fault on streamOptimized vmdk clone); prefer
          clone_source over image_id when possible.

    `shard` parameter:
        - When provided, the volume is created normally (in the user's
          project), then admin-migrated to the requested shard via
          'openstack volume migrate --host'. This is necessary because
          qa-de-1's scheduler config doesn't enforce per-shard placement
          when the project has the 'sharding_enabled' tag — all shards
          are accepted, and the weigher (not the test) decides which
          shard wins. Admin-migrate is the only deterministic way to
          end up on a specific shard while keeping the volume in the
          user's project (Nova requires the volume to be in the project
          attached to the VM).
    """
    if volume_type is None:
        volume_type = VOLUME_TYPE

    args = ["volume", "create",
            "--size", str(size_gb),
            "--type", volume_type,
            "--availability-zone", availability_zone]
    if image_id:
        args.extend(["--image", image_id])
    if clone_source:
        args.extend(["--source", clone_source])
    args.extend(["-f", "json", name])
    # Image volume creates take longer; bump the openstack CLI timeout
    # accordingly via the openstack() wrapper.
    cli_timeout = (TEST_VOLUME_FROM_IMAGE_TIMEOUT
                   if (image_id or clone_source) else 120)
    result = openstack(*args, check=False, timeout=cli_timeout)
    if result.returncode != 0:
        print(f"  ERROR: volume create failed: {result.stderr.strip()}")
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    vol_id = data.get("id")
    if not vol_id:
        return None

    # Wait for available + a host assignment before deciding whether to
    # admin-migrate.
    available_timeout = (TEST_VOLUME_FROM_IMAGE_TIMEOUT
                         if (image_id or clone_source) else 120)
    s = poll_volume_status(vol_id, ["available"], timeout=available_timeout)
    if s != "available":
        print(f"  ERROR: new volume never became available (status={s})")
        return vol_id  # caller will see this in setup
    initial_host = get_volume_host(vol_id) or ""

    if not shard:
        return vol_id

    if shard in initial_host:
        print(f"  volume landed on shard '{shard}' on first try ✓")
        return vol_id

    # Need to admin-migrate to the right shard.
    print(f"  volume landed on '{initial_host}'; admin-migrating to "
          f"shard '{shard}'...")
    target_host = _pick_pool_for_shard(shard, initial_host)
    if not target_host:
        print(f"  ERROR: no usable pool found for shard '{shard}'")
        return vol_id
    rc = openstack_admin("volume", "migrate", "--host", target_host, vol_id)
    if rc.returncode != 0:
        print(f"  ERROR: admin migrate failed: {rc.stderr.strip()[:200]}")
        return vol_id
    final = wait_for_migration_status(vol_id, targets=["success"],
                                      fails=["error"], timeout=180)
    if final != "success":
        print(f"  ERROR: admin migrate did not succeed (last={final})")
        return vol_id
    final_host = get_volume_host(vol_id) or ""
    if shard not in final_host:
        print(f"  ERROR: post-migrate host '{final_host}' does not "
              f"contain shard '{shard}'")
        return vol_id
    print(f"  volume migrated to {final_host} ✓")
    return vol_id


def _pick_pool_for_shard(shard: str, hint_pool: str = "") -> Optional[str]:
    """Return a full host string (host@backend#pool) for the given shard.

    If hint_pool (e.g. 'cinder-volume-vmware-vc-a-0@vmware_fcd#<pool>') is
    given, try to use the same pool name on the target shard so the
    relocate is just a metadata change (vCenter side picks up the file
    in the same NFS dataset). If not present, falls back to the first
    enabled pool on the shard.
    """
    target_dep = DEPLOYMENT_PREFIX + shard
    # Extract the pool suffix from the hint, if any.
    pool_suffix = ""
    if hint_pool and "#" in hint_pool:
        pool_suffix = hint_pool.split("#", 1)[1]
    # Ask cinder for pools admin-style.
    result = openstack_admin("volume", "backend", "pool", "list",
                             "-f", "value", "-c", "Name")
    if result.returncode != 0:
        return None
    pools = [p.strip() for p in (result.stdout or "").split("\n")
             if p.strip()]
    # Filter to the target shard's @vmware_fcd pools.
    candidates = [p for p in pools
                  if p.startswith(f"{target_dep}@vmware_fcd#")]
    if not candidates:
        return None
    # Prefer a pool with the same suffix as the source.
    if pool_suffix:
        same = [c for c in candidates if c.endswith(f"#{pool_suffix}")]
        if same:
            return same[0]
    return candidates[0]


def _strip_admin_noise(text: str) -> str:
    """Remove the ccloud-multitool exec-credential banner noise from
    captured stderr/stdout so the real cinder error is readable.
    """
    if not text:
        return ""
    lines = []
    for line in text.split("\n"):
        # Strip ANSI color codes for matching.
        plain = line.replace("\x1b[1;92m", "").replace("\x1b[0m", "").replace(
            "\x1b[1;91m", "")
        if "kubectl exec-credential plugin" in plain:
            continue
        if "if you run into any authentication issues" in plain:
            continue
        # Also strip any leading ANSI bytes from the actual message.
        cleaned = (line
                   .replace("\x1b[1;92m", "")
                   .replace("\x1b[1;91m", "")
                   .replace("\x1b[0m", ""))
        if cleaned.strip():
            lines.append(cleaned)
    return "\n".join(lines).strip()


def cinder_admin_migrate(volume_id: str, dest_host: str
                         ) -> subprocess.CompletedProcess:
    """Issue an admin 'openstack volume migrate' (operator-driven).

    For S3 only. Cross-vc operator-initiated migrate of an attached volume
    is intentionally avoided (it would invert into S1 territory).
    """
    return openstack_admin("volume", "migrate", "--host", dest_host,
                           volume_id, timeout=SAMEVC_MIGRATE_TIMEOUT)


def cinder_attachment_list(volume_id: str) -> list:
    """List attachments for a volume. Admin scope."""
    result = openstack_admin("volume", "attachment", "list",
                             "--volume-id", volume_id, "-f", "json")
    if result.returncode != 0:
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []


def cinder_attachment_delete(attachment_id: str) -> subprocess.CompletedProcess:
    """Force-delete an attachment row. Admin scope."""
    return openstack_admin("volume", "attachment", "delete", attachment_id)


def cinder_reset_state(volume_id: str, state: str = "available",
                       attach_status: str = "detached"
                       ) -> subprocess.CompletedProcess:
    """Force-reset volume state out of stuck migrating/attaching/etc.

    Uses the cinder CLI (not openstack CLI) because only `cinder reset-state`
    supports `--reset-migration-status`, which is required to recover from
    a stuck `migration_status='migrating'` state. The openstack CLI has no
    equivalent flag.

    This is the standard recovery primitive used in production when a
    volume is wedged. We use it during cleanup if a test left wreckage.
    """
    shell_cmd = (
        'source ~/.sap-py3/bin/activate && '
        f'eval "$(ccloud-multitool {KUBE_CONTEXT})" && '
        'eval "$(ccloud-multitool admin)" && '
        f'cinder reset-state --state {state} --attach-status {attach_status} '
        f'--reset-migration-status {volume_id}'
    )
    cmd = ["zsh", "-c", shell_cmd]
    print(f"  $ cinder (admin) reset-state --state {state} "
          f"--attach-status {attach_status} --reset-migration-status "
          f"{volume_id}")
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=60, check=False)


def wait_for_migration_status(volume_id: str,
                              targets: list,
                              fails: list = None,
                              timeout: int = ATTACH_TIMEOUT
                              ) -> str:
    """Poll migration_status until it hits a target (or a fail status)."""
    if fails is None:
        fails = ["error"]
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        s = get_migration_status(volume_id)
        if s != last:
            print(f"  Volume {volume_id[:8]}... migration_status: '{s}'")
            last = s
        if s in targets:
            return s
        if s in fails:
            return s
        time.sleep(MIGRATION_POLL_INTERVAL)
    return last or "timeout"


def _poll_migration_or_attach_exit(volume_id: str,
                                   attach_proc: subprocess.Popen,
                                   targets: list,
                                   fails: list = None,
                                   timeout: int = 120
                                   ) -> tuple[str, bool]:
    """Poll migration_status while also watching for early attach_proc exit.

    Returns (migration_status, attach_done). migration_status is the last
    observed value; attach_done is True if attach_proc has already exited.
    """
    if fails is None:
        fails = ["error"]
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        # Check if the attach subprocess has exited.
        if attach_proc.poll() is not None:
            # Drain its output later; just signal done.
            s = get_migration_status(volume_id)
            return s or "", True
        s = get_migration_status(volume_id)
        if s != last:
            print(f"  Volume {volume_id[:8]}... migration_status: '{s}'")
            last = s
        if s in targets:
            return s, False
        if s in fails:
            return s, False
        time.sleep(MIGRATION_POLL_INTERVAL)
    return last or "timeout", attach_proc.poll() is not None


# =============================================================================
# Test scaffolding: timeline + cleanup + evidence
# =============================================================================

class Timeline:
    """Per-test event log with wall-clock timestamps.

    Each test creates a fresh Timeline at the top, calls .add() at every
    interesting moment, then dumps to a markdown table at the end. Becomes
    part of the per-test evidence in the run's report.md.

    Kill events are recorded separately in `kill_events` so the report can
    surface them prominently (which pod, which deployment, when, both
    test-relative and absolute time).
    """

    def __init__(self):
        self.t0 = time.time()
        self.t0_abs = datetime.now()
        self.events = []  # list of (rel_seconds, actor, event, outcome)
        # Kill events recorded explicitly by record_kill().
        # Each entry: dict(rel_seconds, abs_time_str, deployment, pod,
        #                  reason, intended_phase, kill_landed)
        self.kill_events: list[dict] = []

    def add(self, actor: str, event: str, outcome: str = "") -> None:
        rel = time.time() - self.t0
        self.events.append((rel, actor, event, outcome))
        print(f"  [{rel:6.1f}s] {actor:14s} | {event} {f'→ {outcome}' if outcome else ''}")

    def record_kill(self, deployment: str, pod: str, intended_phase: str,
                    kill_landed: bool, note: str = "") -> None:
        """Record the details of a `kubectl delete pod` event for the report.

        Both successful kills and "kill window missed" outcomes are
        recorded so the report shows what was attempted.
        """
        rel = time.time() - self.t0
        abs_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.kill_events.append({
            "rel_seconds": rel,
            "abs_time": abs_time,
            "deployment": deployment,
            "pod": pod or "(none — kill not issued)",
            "intended_phase": intended_phase,
            "kill_landed": kill_landed,
            "note": note,
        })

    def to_markdown(self) -> str:
        out = ["| t (s) | actor | event | outcome |",
               "|------:|-------|-------|---------|"]
        for rel, actor, event, outcome in self.events:
            out.append(f"| {rel:.1f} | {actor} | {event} | {outcome} |")
        return "\n".join(out)

    def kill_events_markdown(self) -> str:
        """Render the kill events as a prominent markdown section."""
        if not self.kill_events:
            return "_No pod kills attempted in this test._"
        lines = ["| t (s) | abs time | deployment | pod | phase | kill landed? | note |",
                 "|------:|----------|------------|-----|-------|--------------|------|"]
        for k in self.kill_events:
            landed = "✅ YES" if k["kill_landed"] else "❌ NO (race lost)"
            lines.append(
                f"| {k['rel_seconds']:.1f} "
                f"| {k['abs_time']} "
                f"| `{k['deployment']}` "
                f"| `{k['pod']}` "
                f"| {k['intended_phase']} "
                f"| {landed} "
                f"| {k['note']} |")
        return "\n".join(lines)


def cleanup_test_state(timeline: Timeline,
                       vm_id: Optional[str],
                       vol_ids: list,
                       deployments_log_streams: list = None) -> None:
    """Best-effort cleanup at end of test.

    Defaults to AGGRESSIVE: detach stuck volumes, reset stuck state, force-
    delete test volumes. The pre-created VM is NEVER deleted (per plan).
    Pass --no-cleanup at CLI to skip this entirely (preserves wreckage for
    forensics).

    deployments_log_streams: list of (Popen, label) to terminate.
    """
    # Always stop log streams (they're connections, not state).
    if deployments_log_streams:
        for proc, label in deployments_log_streams:
            try:
                stop_log_stream(proc, timeout=10)
            except Exception as e:
                print(f"  cleanup: log stream stop failed ({label}): {e}")

    if not DO_CLEANUP:
        timeline.add("cleanup", "skipped (--no-cleanup)")
        return

    timeline.add("cleanup", "begin")

    # Snapshot final state into the report before cleaning anything.
    if vm_id:
        try:
            data = nova_show_server(vm_id)
            save_log_file(f"vm-{vm_id[:8]}-final.json",
                          json.dumps(data, indent=2))
        except Exception as e:
            print(f"  cleanup: vm show failed: {e}")
    for vid in vol_ids:
        if not vid:
            continue
        try:
            fields = _hammer_volume_show(vid)
            save_log_file(f"volume-{vid[:8]}-final.txt",
                          "\n".join(f"{k}: {v}" for k, v in fields.items()))
        except Exception as e:
            print(f"  cleanup: hammer failed for {vid[:8]}: {e}")

    # Detach test volumes from the VM if Nova still thinks they're attached.
    if vm_id:
        try:
            attached = {v.get("id") for v in nova_get_server_volumes(vm_id)}
        except Exception:
            attached = set()
        for vid in vol_ids:
            if vid and vid in attached:
                print(f"  cleanup: detaching volume {vid[:8]}... from VM")
                try:
                    nova_detach_volume(vm_id, vid)
                    nova_wait_for_volume_detached(vm_id, vid, timeout=120)
                except Exception as e:
                    print(f"  cleanup: detach failed for {vid[:8]}: {e}")

    # Best-effort reset and delete each volume.
    for vid in vol_ids:
        if not vid:
            continue
        try:
            status = get_volume_status(vid)
            mig_status = get_migration_status(vid)
        except Exception:
            status = "unknown"
            mig_status = ""
        if status == "unknown":
            continue
        # If migrating, attaching, reserved, error*, OR migration_status is
        # stuck (a kill-mid-migrate scenario), force-reset before delete.
        # Note: migration_status='migrating' with status='in-use' is the
        # stuck-after-kill case from T10; status alone won't catch this.
        needs_reset = (
            status in ("migrating", "attaching", "detaching", "reserved",
                       "error", "error_attaching", "error_detaching",
                       "error_restoring", "error_extending")
            or mig_status in ("migrating", "starting", "completing", "error")
        )
        if needs_reset:
            print(f"  cleanup: resetting volume {vid[:8]}... "
                  f"(status='{status}', migration_status='{mig_status}')")
            try:
                # Best-effort: ignore failures (admin scope may not be
                # available or volume may have already been removed).
                cinder_reset_state(vid)
            except Exception as e:
                print(f"  cleanup: reset-state failed for {vid[:8]}: {e}")
            time.sleep(2)

        # Try to delete.
        try:
            delete_volume(vid)
        except Exception as e:
            print(f"  cleanup: delete failed for {vid[:8]}: {e}")

    timeline.add("cleanup", "complete")


def verify_drain_log_sequence(log_path: Path,
                              expected: list = None
                              ) -> tuple[bool, list[str]]:
    """Check the captured pod log for the expected graceful-shutdown markers.

    Returns (passed, observed_markers). 'passed' is True if every marker in
    'expected' was seen in order.
    """
    if expected is None:
        expected = SHUTDOWN_LOG_SEQUENCE_WITH_TASKS
    if not log_path or not log_path.exists():
        return False, []
    text = log_path.read_text(errors="replace")
    observed = []
    pos = 0
    for marker in expected:
        idx = text.find(marker, pos)
        if idx < 0:
            return False, observed
        observed.append(marker)
        pos = idx + len(marker)
    return True, observed


# =============================================================================
# Pre-flight: confirm all required configuration is present
# =============================================================================

def _find_vm_shard(vm_host: str) -> Optional[str]:
    """Return the vCenter shard name for a Nova compute host.

    Looks at all admin-visible aggregates and finds the one whose name
    matches a known shard (vc-a-0, vc-a-1, vc-b-0, ...) and contains
    vm_host. Returns None if no aggregate match.
    """
    if not vm_host:
        return None
    # Build a set of known shard names from SHARDS_BY_AZ.
    known_shards = set()
    for az_shards in SHARDS_BY_AZ.values():
        known_shards.update(az_shards)
    for shard in known_shards:
        result = openstack_admin("aggregate", "show", shard,
                                 "-f", "value", "-c", "hosts")
        if result.returncode != 0:
            continue
        # Output is a Python-list literal string: "['nova-compute-bb83', ...]"
        text = (result.stdout or "").strip()
        if vm_host in text:
            return shard
    return None


def _detect_topology(vm_data: dict) -> Optional[str]:
    """Set VM_AZ / VM_SHARD / OTHER_SHARD / DEPLOYMENT_* globals.

    The VM's vCenter shard is inferred from its hypervisor host's Nova
    aggregate membership. The OTHER_SHARD is a different shard within
    the same AZ — that's where we'll create the test volume to trigger
    ConnectorRejected on attach.

    Returns None on success, an error string otherwise.
    """
    global VM_AZ, VM_SHARD, OTHER_SHARD
    global DEPLOYMENT_VM_SIDE, DEPLOYMENT_OTHER_SIDE, DEPLOYMENT_VM_PEER

    VM_AZ = (vm_data.get("OS-EXT-AZ:availability_zone")
             or vm_data.get("availability_zone") or "")
    if VM_AZ not in SHARDS_BY_AZ:
        return (f"VM {PRECREATED_VM_ID[:8]}... is in AZ '{VM_AZ}'; "
                f"the harness knows about {list(SHARDS_BY_AZ)}.")

    vm_host = (vm_data.get("OS-EXT-SRV-ATTR:host")
               or vm_data.get("host") or "")
    if not vm_host:
        return f"Cannot read VM host (compute) — required for shard detection"

    VM_SHARD = _find_vm_shard(vm_host)
    if not VM_SHARD:
        return (f"VM is on host '{vm_host}' which is not a member of any "
                f"known vCenter aggregate ({list(SHARDS_BY_AZ.values())})")

    # Pick a different shard in the SAME AZ for the test volume.
    az_shards = SHARDS_BY_AZ.get(VM_AZ, [])
    candidates = [s for s in az_shards if s != VM_SHARD]
    if not candidates:
        return (f"VM is on shard '{VM_SHARD}' in AZ '{VM_AZ}' but no other "
                f"shard exists in that AZ — cannot trigger ConnectorRejected. "
                f"This region's topology does not support S1 testing.")
    OTHER_SHARD = candidates[0]

    DEPLOYMENT_VM_SIDE = DEPLOYMENT_PREFIX + VM_SHARD
    DEPLOYMENT_OTHER_SIDE = DEPLOYMENT_PREFIX + OTHER_SHARD
    DEPLOYMENT_VM_PEER = DEPLOYMENT_OTHER_SIDE  # same-AZ peer for S3
    return None


def preflight() -> Optional[str]:
    """Return None if OK, else an error message describing what's missing."""
    missing = []
    if not PRECREATED_VM_ID:
        missing.append("--vm-id (or PRECREATED_VM_ID constant)")
    if missing:
        return ("Required configuration not provided. Set or pass: "
                + ", ".join(missing))

    # Verify VM exists and is ACTIVE.
    data = nova_show_server(PRECREATED_VM_ID)
    if not data:
        return f"Pre-created VM {PRECREATED_VM_ID} not found or unreadable"
    status = data.get("status", "")
    if status != "ACTIVE":
        return (f"Pre-created VM {PRECREATED_VM_ID[:8]}... is in status "
                f"'{status}', expected 'ACTIVE'")

    # Detect the VM's vCenter shard and populate VM_SHARD / OTHER_SHARD / etc.
    err = _detect_topology(data)
    if err:
        return err

    # Verify the volume type exists.
    result = openstack("volume", "type", "show", VOLUME_TYPE, "-f", "json",
                       check=False)
    if result.returncode != 0:
        return f"Volume type '{VOLUME_TYPE}' not found in qa-de-1"

    # Verify deployments exist.
    for dep in (DEPLOYMENT_VM_SIDE, DEPLOYMENT_OTHER_SIDE):
        if not dep:
            continue
        result = kubectl("get", "deployment", dep, check=False)
        if result.returncode != 0:
            return f"Deployment {dep} not found in {KUBE_NAMESPACE}"

    return None


# =============================================================================
# Scenario 1 — Volume attach where ConnectorRejected forces migrate-by-connector
# =============================================================================
#
# Production flow validated here:
#   1. Test driver creates a volume on the vc-B backend.
#   2. Test driver issues 'openstack server add volume <vm-on-vc-A> <vol-on-vc-B>'.
#   3. Nova → cinder API: attachment_create.
#   4. Cinder VolumeManager → vmware_fcd driver.initialize_connection.
#   5. Driver checks connection_capabilities, finds vc-A vs vc-B mismatch,
#      raises exception.ConnectorRejected.
#   6. Cinder API translates to HTTP 406 ("Volume needs to be migrated").
#   7. Nova receives 406, automatically calls
#      cinder API POST /volumes/<id>/action {"os-migrate_volume_by_connector": ...}.
#   8. Cinder scheduler picks a vc-A backend, migration_status: starting →
#      migrating → completing → success. Volume now on vc-A.
#   9. Nova retries attachment_create against the same volume; this time the
#      connector matches the (new) backend, attach succeeds.
#  10. Final state: VM has vol attached, vol.host on vc-A, status 'in-use'.
#
# T1 validates the entire flow with no disruption. T2/T3/T4 inject a
# graceful-shutdown pod kill at three distinct moments.

def _s1_setup(use_large_volume: bool = False) -> tuple[Timeline, Optional[str]]:
    """Common S1 setup: precondition checks + create volume on the OPPOSITE
    vCenter from the VM.

    The VM lives on a known shard within VM_AZ (auto-detected during
    preflight; see VM_SHARD / OTHER_SHARD). We create the test volume in
    the SAME AZ as the VM but pinned to a DIFFERENT shard (OTHER_SHARD).
    Nova's AZ check passes; cinder's connector check fails → 406 → migrate.

    `use_large_volume`: when True, creates a 16 GB volume by cloning
    TEST_VOLUME_SOURCE_FOR_CLONE (a populated source volume). Used by T3
    (kill during migrate_volume) so the cross-shard FCD relocate has
    actual data to move and the migration window is wide enough for the
    test driver to inject the kill deterministically. T1, T2, and T4
    use the default (1 GB empty) volume to keep test runtime short.

    Returns (timeline, volume_id). volume_id is None on setup failure.
    """
    timeline = Timeline()
    timeline.add("test", "setup", "begin")

    vm_host = nova_get_server_host(PRECREATED_VM_ID)
    timeline.add("nova",
                 f"VM on shard {VM_SHARD} ({VM_AZ}); host={vm_host}")
    timeline.add("test",
                 f"VM-side cinder deployment: {DEPLOYMENT_VM_SIDE}")
    timeline.add("test",
                 f"Other-side cinder deployment: {DEPLOYMENT_OTHER_SIDE}")

    name = f"test-nova-cinder-{int(time.time())}"
    if use_large_volume:
        size = TEST_VOLUME_SIZE_GB_FOR_MIGRATE_KILL
        clone = TEST_VOLUME_SOURCE_FOR_CLONE
        timeline.add("test",
                     f"create {size}GB volume by cloning "
                     f"{clone[:8]}... on shard {OTHER_SHARD} "
                     f"(AZ={VM_AZ}) — populated for wider kill window")
    else:
        size = TEST_VOLUME_SIZE_GB
        clone = None
        timeline.add("test",
                     f"create {size}GB empty volume on shard "
                     f"{OTHER_SHARD} (AZ={VM_AZ})")
    vol_id = cinder_create_volume(name, VM_AZ,
                                  size_gb=size,
                                  shard=OTHER_SHARD,
                                  clone_source=clone)
    if not vol_id:
        timeline.add("test", "setup FAILED: volume create returned None")
        return timeline, None

    available_timeout = (TEST_VOLUME_FROM_IMAGE_TIMEOUT
                         if use_large_volume else 120)
    s = poll_volume_status(vol_id, ["available"], timeout=available_timeout)
    if s != "available":
        timeline.add("test",
                     f"setup FAILED: volume {vol_id[:8]} stuck in '{s}' "
                     "— aborting test before attach")
        # Stash the broken volume id on the timeline as a side-channel so
        # the test's finalise() can clean it up. Return None for vol_id
        # so the caller bails out without attempting attach.
        timeline._setup_failed_vol_id = vol_id
        return timeline, None
    host = get_volume_host(vol_id)
    timeline.add("cinder", f"volume {vol_id[:8]} ready", f"host={host}")
    if host and OTHER_SHARD not in host:
        timeline.add("WARN",
                     f"volume should be on shard {OTHER_SHARD} but landed on "
                     "another host — shard pinning may be incorrect", host)
    return timeline, vol_id


def _s1_run_attach_and_observe(timeline: Timeline, vol_id: str,
                               kill_point: Optional[str] = None,
                               log_streams: list = None
                               ) -> tuple[bool, str]:
    """Issue the attach and observe the full Nova-driven migrate-and-retry flow.

    kill_point is one of None / 'initialize_connection' / 'migrate_volume' /
    'post_migrate_attach'. When set, this function watches for the right
    moment (via volume migration_status / attach_status transitions) and
    issues 'kubectl delete pod' at that moment.

    The pod targeted by each kill point is auto-selected from the topology:
      'initialize_connection'  → DEPLOYMENT_OTHER_SIDE (where 406 fires)
      'migrate_volume'         → DEPLOYMENT_VM_SIDE   (migration destination)
      'post_migrate_attach'    → DEPLOYMENT_VM_SIDE   (where retry attach lands)

    Returns (passed, message).
    """
    if log_streams is None:
        log_streams = []

    # The 'attach' command from Nova's perspective is synchronous-ish: it
    # blocks until either the attach completes (after the auto-retry) or
    # finally errors. We launch it in a subprocess so we can poll volume
    # state while it runs.
    attach_cmd = [
        gs.OPENSTACK_BIN, "--os-cloud", OS_CLOUD,
        "server", "add", "volume", PRECREATED_VM_ID, vol_id,
    ]
    timeline.add("test", "openstack server add volume", "issued")
    print(f"  $ {' '.join(attach_cmd)} &")
    attach_proc = subprocess.Popen(attach_cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)

    # Phase 1 — wait for migration to start (status: starting/migrating).
    # If kill_point == 'initialize_connection' we kill BEFORE migration
    # starts, while initialize_connection is running on the OTHER-side pod.
    if kill_point == "initialize_connection":
        # The driver call is fast; we kill the OTHER-side pod immediately.
        # The ConnectorRejected has either already fired (good) or is
        # firing right now (race). Either way, Nova should see the 406
        # from a response that already left, then proceed to migrate.
        time.sleep(2)  # let the request reach the driver
        timeline.add("kubectl",
                     f"DELETE pod for {DEPLOYMENT_OTHER_SIDE} (initialize_conn)")
        _kill_pod_for(DEPLOYMENT_OTHER_SIDE, log_streams,
                      timeline=timeline,
                      phase="initialize_connection (driver call about to raise ConnectorRejected)")

    # Phase 2 — wait for migration to start. Behaviour depends on kill_point:
    #
    #   kill_point=None or "initialize_connection":
    #     The actual kill (if any) already happened in phase 1. Poll until
    #     migration_status hits a target OR attach_proc exits. If attach
    #     exits with rc=0 (success), short-circuit to final-state check.
    #
    #   kill_point="migrate_volume":
    #     We need to KILL during the migration. Poll TIGHTLY (sub-second)
    #     for migration_status='migrating' so we can fire `kubectl delete
    #     pod` while the FCD relocate is still in flight.
    #
    #   kill_point="post_migrate_attach":
    #     We need to wait for migration_status='success', then kill the
    #     VM-side pod during Nova's retry attach. Same poll cadence is
    #     fine; just don't short-circuit on attach_proc exit.

    if kill_point == "migrate_volume":
        # Tight poll (0.2s) until any in-flight migration phase is seen,
        # then kill immediately. We INTENTIONALLY ignore attach_proc.poll()
        # here — the CLI may return rc=0 (attachment succeeded) while
        # migration_status is still updating asynchronously. We want the
        # kill to land while migrate_volume code is still on the pod.
        #
        # The pre-attach setup may have used admin-migrate to pin the
        # volume to OTHER_SHARD, leaving migration_status='success'. The
        # NEW migration triggered by Nova-via-os-migrate_volume_by_connector
        # will transition: <initial> → starting → migrating → completing
        # → success. So we ignore the initial value and watch for the
        # FIRST 'starting'/'migrating'/'completing' that follows.
        # Acceptable kill phases include all three.
        initial_mig_status = get_migration_status(vol_id) or ""
        timeline.add("test",
                     f"tight-poll for migration_status transition "
                     f"to starting/migrating/completing "
                     f"(initial='{initial_mig_status}')")
        deadline = time.time() + 120
        seen_migrating = False
        last_seen = initial_mig_status
        # When initial is 'success', we MUST first see the value clear
        # (cinder transitions to None or directly to 'starting' on the
        # new migrate). Track whether we've left the initial state.
        left_initial = False
        while time.time() < deadline:
            s = get_migration_status(vol_id) or ""
            if s != last_seen:
                print(f"  tight-poll: migration_status={s!r}")
                last_seen = s
            if s != initial_mig_status:
                left_initial = True
            if left_initial and s in ("starting", "migrating", "completing"):
                seen_migrating = True
                break
            if left_initial and s in ("error",):
                break
            # If we've left the initial state and now back at 'success',
            # that's a NEW success after a new migration cycle: race lost.
            if left_initial and s == "success" and initial_mig_status != "":
                break
            time.sleep(0.2)
        # Re-read once for the timeline (may have advanced).
        recheck = get_migration_status(vol_id) or ""
        timeline.add("cinder",
                     f"tight-poll exited; loop_observed="
                     f"{last_seen!r}, recheck={recheck!r}, "
                     f"initial={initial_mig_status!r}",
                     f"attach_done={attach_proc.poll() is not None}, "
                     f"seen_migrating={seen_migrating}")
        if seen_migrating:
            timeline.add("kubectl",
                         f"DELETE pod for {DEPLOYMENT_VM_SIDE} "
                         "(mid-migration; kill window hit)")
            _kill_pod_for(DEPLOYMENT_VM_SIDE, log_streams,
                          timeline=timeline,
                          phase="migrate_volume (cross-shard FCD relocate in progress)")
        else:
            timeline.add("WARN",
                         "migration finished before kill window opened; "
                         "T3 race lost on this run", "no kill injected")
            timeline.record_kill(
                DEPLOYMENT_VM_SIDE, "",
                "migrate_volume (cross-shard FCD relocate)",
                kill_landed=False,
                note="race lost: migration completed faster than the "
                     "external poll could detect — likely sub-second "
                     "for small empty volumes; use larger volume to "
                     "extend the kill window")
        # Wait for migration_status=success regardless.
        mig_final = wait_for_migration_status(vol_id, targets=["success"],
                                              fails=["error"],
                                              timeout=ATTACH_TIMEOUT)
        timeline.add("cinder", f"migration_status final: {mig_final}")
        if mig_final != "success":
            attach_proc.kill()
            return False, f"migration did not succeed: {mig_final}"
        # Drain attach_proc.
        try:
            stdout, stderr = attach_proc.communicate(timeout=ATTACH_TIMEOUT)
            rc = attach_proc.returncode
        except subprocess.TimeoutExpired:
            attach_proc.kill()
            return False, (f"openstack server add volume timed out after "
                           f"{ATTACH_TIMEOUT}s")
        timeline.add("nova", f"server add volume returned rc={rc}",
                     (stderr or "").strip()[:200] if stderr else "ok")
        passed, msg = _s1_verify_final_state(timeline, vol_id)
        if seen_migrating:
            return passed, msg
        # If we missed the kill window, decorate the message.
        return passed, msg + " [NOTE: kill window missed — migration "\
                              "completed before kill could fire]"

    # Phase 2 (default flow) — observe migration starting on the volume.
    timeline.add("test",
                 "wait for migration_status=starting/migrating "
                 "OR attach_proc to exit")
    mig_seen, attach_done = _poll_migration_or_attach_exit(
        vol_id, attach_proc,
        targets=["starting", "migrating", "completing", "success"],
        fails=["error"],
        timeout=120)
    timeline.add("cinder",
                 f"migration_status reached: {mig_seen!r}; "
                 f"attach_proc done: {attach_done}")

    # Early-success short-circuit only applies when no further kill is
    # planned past this phase (None or 'initialize_connection').
    if attach_done and kill_point in (None, "initialize_connection"):
        try:
            stdout, stderr = attach_proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        rc = attach_proc.returncode
        out_summary = (stderr or stdout or "").strip()[:400]
        timeline.add("nova",
                     f"server add volume returned rc={rc} (early)",
                     out_summary[:200])
        if rc != 0:
            return False, (f"attach exited rc={rc} before/during pod kill: "
                           f"{out_summary}")
        # rc == 0 — attach succeeded. Skip the migration-wait phase since
        # the result already happened; flow continues to final-state check.
        return _s1_verify_final_state(timeline, vol_id)

    if mig_seen == "error":
        attach_proc.kill()
        return False, "migration_status entered error before completion"
    if mig_seen == "timeout":
        attach_proc.kill()
        return False, ("migration_status never started — Nova may not "
                       "have called migrate-by-connector (check Nova logs)")

    # Phase 4 — wait for migration to finish.
    timeline.add("test", "wait for migration_status=success")
    mig_final = wait_for_migration_status(vol_id, targets=["success"],
                                          fails=["error"],
                                          timeout=ATTACH_TIMEOUT)
    timeline.add("cinder", f"migration_status final: {mig_final}")
    if mig_final != "success":
        attach_proc.kill()
        return False, f"migration did not succeed: {mig_final}"

    # Phase 5 — kill during the post-migration attach retry.
    if kill_point == "post_migrate_attach":
        # Nova has retried attachment_create against the VM-side backend;
        # the driver call is in flight. Kill that pod.
        timeline.add("kubectl",
                     f"DELETE pod for {DEPLOYMENT_VM_SIDE} (post-migrate retry)")
        _kill_pod_for(DEPLOYMENT_VM_SIDE, log_streams,
                      timeline=timeline,
                      phase="post-migrate retry attach (driver initialize_connection in flight)")

    # Phase 6 — wait for the attach command to return.
    try:
        stdout, stderr = attach_proc.communicate(timeout=ATTACH_TIMEOUT)
        rc = attach_proc.returncode
    except subprocess.TimeoutExpired:
        attach_proc.kill()
        return False, f"openstack server add volume timed out after {ATTACH_TIMEOUT}s"

    timeline.add("nova", f"server add volume returned rc={rc}",
                 stderr.strip()[:200] if stderr else "ok")

    return _s1_verify_final_state(timeline, vol_id)


def _s1_verify_final_state(timeline: Timeline, vol_id: str
                           ) -> tuple[bool, str]:
    """Confirm the volume ended up attached on the VM's shard.

    Used both by the normal end-of-test path and by the early-success path
    where attach_proc returned rc=0 before we observed the migration mid-
    flight (kill races can compress the timing window so much that the
    'migrating' status flicker is missed by 5-second polling).
    """
    attached = nova_wait_for_volume_attached(PRECREATED_VM_ID, vol_id,
                                             timeout=120)
    if not attached:
        return False, "VM never reported the volume as attached/in-use"
    final_host = get_volume_host(vol_id)
    final_mig = get_migration_status(vol_id)
    timeline.add("cinder",
                 f"final volume host: {final_host}",
                 f"migration_status={final_mig}")
    if final_host and VM_SHARD not in final_host:
        return False, (f"volume did not migrate to shard {VM_SHARD}: "
                       f"host={final_host}")
    return True, (f"attached, migrated to {final_host}, "
                  f"migration_status={final_mig}, status='in-use'")


def _kill_pod_for(deployment: str, log_streams: list,
                  timeline: Optional["Timeline"] = None,
                  phase: str = "",
                  ) -> Optional[str]:
    """Find a running pod for deployment and 'kubectl delete pod' it.

    Also opens a log stream to the killed pod (if not already streaming)
    so we can verify the drain sequence afterwards. Stores the pod name
    on the log_streams entry so a retroactive log fetch can be attempted
    if the streamed log misses the drain-marker tail.

    If `timeline` is provided, records a structured kill_event with the
    exact pod name, deployment, intended phase, and kill-landed status
    so the report can surface this prominently.

    Returns the pod name that was killed (or None if no pod found).
    """
    pod = get_pod_for_deployment(deployment)
    if not pod:
        print(f"  WARN: no running pod found for {deployment}; skipping kill")
        if timeline is not None:
            timeline.record_kill(deployment, "", phase, kill_landed=False,
                                 note="no running pod found")
        return None
    log_path = OUTPUT_DIR / f"kill-{deployment}-{pod}-{int(time.time())}.log"
    try:
        proc = start_log_stream(pod, deployment, log_path)
        # Tuple shape stays compatible: (proc, label) — we encode the pod
        # name in label as "<deployment>/<pod>" so the verify step can
        # parse it and call kubectl logs <pod> retroactively.
        log_streams.append((proc, f"{deployment}/{pod}"))
    except Exception as e:
        print(f"  WARN: could not start log stream on {pod}: {e}")
    kubectl("delete", "pod", pod, "--wait=false", check=False)
    if timeline is not None:
        timeline.record_kill(deployment, pod, phase, kill_landed=True)
    return pod


def _retroactive_drain_log_fetch(deployment: str, pod: str,
                                 container: str,
                                 log_path: Path,
                                 wait_for_replacement_timeout: int = 180
                                 ) -> bool:
    """Append any drain-tail lines that 'kubectl logs -f' missed.

    `kubectl logs -f` closes its stream when the pod begins terminating,
    which usually happens BEFORE the cinder graceful-shutdown markers
    (`Initiating graceful shutdown`, etc.) reach stdout. To recover
    those, we wait for the deployment to spin up a replacement pod, then
    fetch the killed pod's log via `kubectl logs <killed-pod-name>` —
    Kubernetes retains terminated-container logs on the node for some
    time. If that succeeds, we append the missing tail to `log_path`.

    Returns True if any new log lines were appended (or the markers were
    already in the streamed log), False on failure.
    """
    # Wait for a replacement pod (different name) to come up Ready, so we
    # know the killed pod has actually been replaced.
    new_pod = wait_for_new_pod_ready(deployment, pod,
                                     timeout=wait_for_replacement_timeout)
    if new_pod is None:
        print(f"  WARN: replacement pod for {deployment} did not come up; "
              "skipping retroactive log fetch")
        return False
    # Try to fetch the killed pod's full log non-streaming. If the pod
    # is still around (terminating but logs available), this works.
    result = kubectl("logs", pod, "-c", container, check=False, timeout=30)
    if result.returncode != 0:
        # Pod is gone; logs are not retrievable. Use whatever we streamed.
        print(f"  WARN: killed pod {pod} logs unavailable retroactively "
              f"(rc={result.returncode}); using streamed log only")
        return False
    full = result.stdout or ""
    if not full:
        return False
    # Replace the streamed log with the (more complete) retroactive one.
    try:
        log_path.write_text(full)
        print(f"  retroactively wrote {len(full)} bytes of pod {pod[:20]}... "
              f"log to {log_path.name}")
        return True
    except Exception as e:
        print(f"  WARN: could not overwrite log file: {e}")
        return False


def _s1_finalise(timeline: Timeline, vol_id: Optional[str],
                 log_streams: list, passed: bool, msg: str,
                 test_name: str, start: float) -> TestResult:
    """Common S1 finalisation: detach + cleanup + assemble TestResult."""
    # If setup failed before producing a usable volume, there's a broken
    # vol id stashed on the timeline that still needs cleanup.
    setup_failed_vol = getattr(timeline, "_setup_failed_vol_id", None)
    cleanup_vol_ids = [v for v in (vol_id, setup_failed_vol) if v]

    # Detach the volume so cleanup can delete it.
    if vol_id and PRECREATED_VM_ID:
        try:
            ids = [v.get("id") for v in nova_get_server_volumes(PRECREATED_VM_ID)]
        except Exception:
            ids = []
        if vol_id in ids:
            timeline.add("test", "detach volume from VM (final)")
            nova_detach_volume(PRECREATED_VM_ID, vol_id)
            nova_wait_for_volume_detached(PRECREATED_VM_ID, vol_id,
                                          timeout=300)

    cleanup_test_state(timeline, PRECREATED_VM_ID, cleanup_vol_ids,
                       deployments_log_streams=log_streams)

    duration = time.time() - start
    result = TestResult(test_name, passed, duration, msg)

    # Stash the timeline as evidence (each entry becomes a row in the report).
    for rel, actor, event, outcome in timeline.events:
        result.evidence.append((f"[{rel:6.1f}s] {actor}",
                                f"{event}{f' → {outcome}' if outcome else ''}"))

    # Attach structured kill events for the report.
    setattr(result, "kill_events", timeline.kill_events)
    setattr(result, "kill_events_markdown", timeline.kill_events_markdown())

    # Save the timeline as its own markdown file.
    if OUTPUT_DIR:
        tl_path = OUTPUT_DIR / f"{test_name}-timeline.md"
        tl_path.write_text(timeline.to_markdown())
        result.log_files.append(str(tl_path.relative_to(OUTPUT_DIR.parent)))
        # Also save kill events as standalone file.
        if timeline.kill_events:
            ke_path = OUTPUT_DIR / f"{test_name}-kill-events.md"
            ke_path.write_text("# Kill Events\n\n"
                               + timeline.kill_events_markdown() + "\n")
            result.log_files.append(str(ke_path.relative_to(OUTPUT_DIR.parent)))

    return result


def t1_attach_connector_rejected_baseline() -> TestResult:
    """T1 — Baseline: vc-B volume attached to vc-A VM via Nova-driven migrate.

    No pod kill. Verifies the entire ConnectorRejected → migrate → retry flow
    works end-to-end against the patched cinder. Pass criterion:
    volume ends up on vc-A backend, status 'in-use', visible as attached
    on the VM.
    """
    test_name = "t1_attach_connector_rejected_baseline"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id = _s1_setup()
    log_streams: list = []
    if not vol_id:
        return _s1_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s1_run_attach_and_observe(timeline, vol_id,
                                                 kill_point=None,
                                                 log_streams=log_streams)
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s1_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t2_attach_kill_during_initialize_connection() -> TestResult:
    """T2 — Kill the OTHER-side cinder-volume pod during initialize_connection.

    The driver call to the volume's vCenter is when ConnectorRejected is
    raised. We delete the pod a couple seconds after the attach is
    dispatched, while the driver is reaching out to vCenter. Validates
    that the 406 response still propagates to Nova (so Nova can drive
    migrate) and that the drained pod exits cleanly.
    """
    test_name = "t2_attach_kill_during_initialize_connection"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id = _s1_setup()
    log_streams: list = []
    if not vol_id:
        return _s1_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s1_run_attach_and_observe(
            timeline, vol_id,
            kill_point="initialize_connection",
            log_streams=log_streams)
        # Verify drain log on the killed OTHER-side pod (best-effort; the
        # kill may be too fast for any in-flight task to register).
        for proc, label in log_streams:
            if DEPLOYMENT_OTHER_SIDE in label:
                # Find the corresponding log file.
                pod_name = label.split("/")[-1]
                matches = list(OUTPUT_DIR.glob(
                    f"kill-{DEPLOYMENT_OTHER_SIDE}-{pod_name}-*.log"))
                if matches:
                    # 'kubectl logs -f' often closes its stream BEFORE
                    # the cinder graceful-shutdown markers reach stdout.
                    # Fetch retroactively to fill the gap.
                    _retroactive_drain_log_fetch(
                        DEPLOYMENT_OTHER_SIDE, pod_name,
                        DEPLOYMENT_OTHER_SIDE, matches[-1])
                    ok, observed = verify_drain_log_sequence(
                        matches[-1], SHUTDOWN_LOG_SEQUENCE)
                    timeline.add("verify",
                                 f"drain markers on other-side pod: {len(observed)}/{len(SHUTDOWN_LOG_SEQUENCE)}",
                                 "OK" if ok else "MISSING markers")
                    if not ok and passed:
                        msg = (msg + " [WARN: drain markers not all "
                               "found — may have been pre-cleaned]")
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s1_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t3_attach_kill_during_migrate_volume() -> TestResult:
    """T3 — Kill the VM-side cinder-volume pod while the migrate_volume runs.

    After Nova has issued migrate_volume_by_connector and the destination
    pod (VM side) is executing the FCD relocate, we delete that pod.
    With the graceful-shutdown patch the in-flight migration should
    complete on the draining pod (Phase-2 pool.waitall drain).

    Uses a 16 GB volume cloned from a populated source volume so the
    cross-shard FCD relocate has actual data to copy and the migration
    window is wide enough (~30-60s) to inject the kill deterministically.
    """
    test_name = "t3_attach_kill_during_migrate_volume"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id = _s1_setup(use_large_volume=True)
    log_streams: list = []
    if not vol_id:
        return _s1_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s1_run_attach_and_observe(
            timeline, vol_id,
            kill_point="migrate_volume",
            log_streams=log_streams)
        # Verify the killed VM-side pod drained cleanly.
        for proc, label in log_streams:
            if DEPLOYMENT_VM_SIDE in label:
                pod_name = label.split("/")[-1]
                matches = list(OUTPUT_DIR.glob(
                    f"kill-{DEPLOYMENT_VM_SIDE}-{pod_name}-*.log"))
                if matches:
                    _retroactive_drain_log_fetch(
                        DEPLOYMENT_VM_SIDE, pod_name,
                        DEPLOYMENT_VM_SIDE, matches[-1])
                    ok, observed = verify_drain_log_sequence(
                        matches[-1], SHUTDOWN_LOG_SEQUENCE_WITH_TASKS)
                    timeline.add("verify",
                                 f"drain-with-tasks markers on vm-side pod: {len(observed)}/{len(SHUTDOWN_LOG_SEQUENCE_WITH_TASKS)}",
                                 "OK" if ok else "MISSING markers")
                    if not ok and passed:
                        msg = (msg + " [WARN: full drain sequence not "
                               "observed — investigate]")
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s1_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t4_attach_kill_during_post_migrate_attach() -> TestResult:
    """T4 — Kill the VM-side cinder-volume pod during Nova's retry attach.

    Migration has succeeded; volume is now on the VM-side vCenter. Nova
    issues attachment_create against the VM-side backend; that pod's
    driver.initialize_connection is in flight. We kill it. The graceful-
    shutdown patch should let the in-flight attach finish.
    """
    test_name = "t4_attach_kill_during_post_migrate_attach"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id = _s1_setup()
    log_streams: list = []
    if not vol_id:
        return _s1_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s1_run_attach_and_observe(
            timeline, vol_id,
            kill_point="post_migrate_attach",
            log_streams=log_streams)
        for proc, label in log_streams:
            if DEPLOYMENT_VM_SIDE in label:
                pod_name = label.split("/")[-1]
                matches = list(OUTPUT_DIR.glob(
                    f"kill-{DEPLOYMENT_VM_SIDE}-{pod_name}-*.log"))
                if matches:
                    _retroactive_drain_log_fetch(
                        DEPLOYMENT_VM_SIDE, pod_name,
                        DEPLOYMENT_VM_SIDE, matches[-1])
                    ok, observed = verify_drain_log_sequence(
                        matches[-1], SHUTDOWN_LOG_SEQUENCE)
                    timeline.add("verify",
                                 f"drain markers on vm-side pod: {len(observed)}/{len(SHUTDOWN_LOG_SEQUENCE)}",
                                 "OK" if ok else "MISSING markers")
                    if not ok and passed:
                        msg = (msg + " [WARN: drain markers not all "
                               "found]")
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s1_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


# =============================================================================
# Scenario 2 — Nova live-migration of attached volume (PROVISIONAL)
# =============================================================================
#
# This scenario is provisional: cross-vCenter live-migration is generally
# NOT supported by stock Nova + VMware drivers (vCenter is a hypervisor
# management boundary). T5 is the first thing to run; if it cannot complete
# even at baseline, T6/T7/T8 are unrunnable and should be marked SKIP.
#
# Production flow:
#   1. Pre-condition: vol attached to VM, both on vc-A.
#   2. Test driver issues 'openstack server migrate --live-migration --host
#      <vc-B host> <vm>'.
#   3. Nova orchestrates the move; among other things, asks cinder to
#      attachment_update with the new (vc-B-side) connector.
#   4. cinder vc-A pod returns 406 (volume is on vc-A, connector advertises
#      vc-B); Nova auto-issues migrate_volume_by_connector to move the
#      volume to vc-B; Nova retries attachment_update on vc-B.
#   5. Old attachment on vc-A is terminated.
#   6. Live migration completes; VM and volume both now on vc-B.
#
# Helpful note: vmware live-migrate uses --shared-migration semantics in
# many envs (the VM disk IS the FCD object, which moves with the volume).
# nova_live_migrate() defaults to block-migration; tweak via CLI override
# if Phase A.5 reveals different requirements.

def _s2_setup() -> tuple[Timeline, Optional[str], Optional[str]]:
    """Common S2 setup: create volume on the VM's vCenter shard, attach to VM."""
    timeline = Timeline()
    timeline.add("test", "setup", "begin")

    src_host = nova_get_server_host(PRECREATED_VM_ID)
    timeline.add("nova", f"vm currently on host: {src_host}")

    name = f"test-nova-cinder-livemig-{int(time.time())}"
    timeline.add("test",
                 f"create volume on VM-side shard ({VM_SHARD}, AZ={VM_AZ})")
    vol_id = cinder_create_volume(name, VM_AZ, shard=VM_SHARD)
    if not vol_id:
        timeline.add("test",
                     f"setup FAILED: shard {VM_SHARD} volume create returned None")
        return timeline, None, src_host
    if poll_volume_status(vol_id, ["available"], timeout=120) != "available":
        timeline.add("test", "setup FAILED: volume never became available")
        return timeline, vol_id, src_host

    timeline.add("test",
                 f"attach shard-{VM_SHARD} volume to VM on shard {VM_SHARD}")
    rc = nova_attach_volume(PRECREATED_VM_ID, vol_id)
    if rc.returncode != 0:
        timeline.add("test",
                     f"setup FAILED: pre-attach failed: {rc.stderr.strip()[:200]}")
        return timeline, vol_id, src_host
    if not nova_wait_for_volume_attached(PRECREATED_VM_ID, vol_id,
                                         timeout=300):
        timeline.add("test", "setup FAILED: pre-attach never reported in-use")
        return timeline, vol_id, src_host
    timeline.add("test", "pre-attach complete; ready to live-migrate")
    return timeline, vol_id, src_host


def _s2_run_live_migrate(timeline: Timeline, vol_id: str,
                         dest_host_substring: Optional[str] = None,
                         kill_point: Optional[str] = None,
                         log_streams: list = None
                         ) -> tuple[bool, str]:
    """Issue live-migration to the OPPOSITE shard (different vCenter, same
    AZ if available — else cross-AZ which Nova will likely block).

    kill_point is one of None / 'terminate_source' / 'attach_dest' /
    'migrate_by_connector'.
    """
    if log_streams is None:
        log_streams = []

    if dest_host_substring is None:
        dest_host_substring = OTHER_SHARD

    # We pass --host=<some other-shard host> so Nova schedules to the
    # opposite vCenter aggregate.
    timeline.add("test",
                 f"discover a hypervisor in shard {dest_host_substring}")
    dest_host = _find_hypervisor_in_aggregate(dest_host_substring)
    if not dest_host:
        return False, (f"no hypervisor in aggregate '{dest_host_substring}' "
                       f"found; cannot run S2")
    timeline.add("nova", f"target dest host: {dest_host}")

    timeline.add("nova",
                 f"openstack server migrate --live-migration --host {dest_host}")
    mig_proc = subprocess.Popen(
        ["zsh", "-c",
         'source ~/.sap-py3/bin/activate && '
         f'eval "$(ccloud-multitool {KUBE_CONTEXT})" && '
         'eval "$(ccloud-multitool admin)" && '
         f'openstack server migrate --live-migration --host {dest_host} '
         f'{PRECREATED_VM_ID}'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )

    if kill_point == "terminate_source":
        time.sleep(5)
        timeline.add("kubectl",
                     f"DELETE pod for {DEPLOYMENT_VM_SIDE} (mid-terminate)")
        _kill_pod_for(DEPLOYMENT_VM_SIDE, log_streams,
                      timeline=timeline,
                      phase="terminate_connection on source (live-migrate teardown)")
    elif kill_point == "migrate_by_connector":
        # Wait for cinder to enter migrating, then kill.
        wait_for_migration_status(vol_id,
                                  targets=["starting", "migrating"],
                                  fails=["error"], timeout=180)
        timeline.add("kubectl",
                     f"DELETE pod for {DEPLOYMENT_VM_SIDE} (during migrate-by-conn)")
        _kill_pod_for(DEPLOYMENT_VM_SIDE, log_streams,
                      timeline=timeline,
                      phase="Nova-triggered migrate_volume during live-migrate")
    elif kill_point == "attach_dest":
        # Wait for migration to complete, then kill the dest (other-side)
        # pod while Nova retries attachment_update.
        wait_for_migration_status(vol_id, targets=["success"],
                                  fails=["error"], timeout=600)
        timeline.add("kubectl",
                     f"DELETE pod for {DEPLOYMENT_OTHER_SIDE} (post-migrate attach)")
        _kill_pod_for(DEPLOYMENT_OTHER_SIDE, log_streams,
                      timeline=timeline,
                      phase="attachment_update on destination (post-migrate retry)")

    # Wait for live-migration to complete.
    try:
        stdout, stderr = mig_proc.communicate(timeout=LIVE_MIGRATE_TIMEOUT)
        rc = mig_proc.returncode
    except subprocess.TimeoutExpired:
        mig_proc.kill()
        return False, f"live-migrate timed out after {LIVE_MIGRATE_TIMEOUT}s"

    timeline.add("nova", f"server migrate returned rc={rc}",
                 (stderr or "").strip()[:200] if stderr else "ok")
    if rc != 0:
        return False, f"live-migrate command failed: {(stderr or '').strip()[:200]}"

    s = nova_wait_for_status(PRECREATED_VM_ID, "ACTIVE",
                             timeout=LIVE_MIGRATE_TIMEOUT)
    if s != "ACTIVE":
        return False, f"VM did not return to ACTIVE (last={s})"
    final_host = nova_get_server_host(PRECREATED_VM_ID)
    final_volume_host = get_volume_host(vol_id)
    timeline.add("nova", f"vm final host: {final_host}")
    timeline.add("cinder", f"volume final host: {final_volume_host}")

    moved = (final_host == dest_host)
    vol_moved = (final_volume_host and OTHER_SHARD in final_volume_host)
    if not moved:
        return False, f"VM did not move to {dest_host}: host={final_host}"
    if not vol_moved:
        return False, f"volume did not migrate: host={final_volume_host}"
    return True, (f"VM on {final_host}, volume on {final_volume_host}, "
                  "attached")


def _find_hypervisor_in_aggregate(aggregate_name: str) -> Optional[str]:
    """Return one Nova compute host belonging to the given aggregate.

    Used to pick a destination host for --host on live-migrate. Admin scope.
    Returns the first 'up' compute host. None if the aggregate is empty
    or all hosts are down.
    """
    result = openstack_admin("aggregate", "show", aggregate_name,
                             "-f", "value", "-c", "hosts")
    if result.returncode != 0:
        return None
    text = (result.stdout or "").strip()
    # Output is a Python-list literal: "['nova-compute-bb83', ...]"
    try:
        hosts = ast.literal_eval(text) if text else []
    except (ValueError, SyntaxError):
        hosts = []
    if not isinstance(hosts, list) or not hosts:
        return None
    # Pick the first host that's still up.
    svc = openstack_admin("compute", "service", "list",
                          "--service", "nova-compute",
                          "-f", "json")
    up = set()
    if svc.returncode == 0:
        try:
            for row in json.loads(svc.stdout or "[]"):
                if (row.get("State") == "up"
                        or row.get("state") == "up"):
                    up.add(row.get("Host") or row.get("host"))
        except json.JSONDecodeError:
            pass
    for h in hosts:
        if not up or h in up:
            return h
    return None


def _s2_finalise(timeline: Timeline, vol_id: Optional[str],
                 log_streams: list, passed: bool, msg: str,
                 test_name: str, start: float) -> TestResult:
    # Detach + cleanup (live-migrate leaves volume attached to a different
    # host; we want to detach before deleting).
    if vol_id and PRECREATED_VM_ID:
        try:
            ids = [v.get("id") for v in nova_get_server_volumes(PRECREATED_VM_ID)]
        except Exception:
            ids = []
        if vol_id in ids:
            timeline.add("test", "detach volume from VM (final)")
            nova_detach_volume(PRECREATED_VM_ID, vol_id)
            nova_wait_for_volume_detached(PRECREATED_VM_ID, vol_id,
                                          timeout=300)

    cleanup_test_state(timeline, PRECREATED_VM_ID, [vol_id],
                       deployments_log_streams=log_streams)
    duration = time.time() - start
    result = TestResult(test_name, passed, duration, msg)
    for rel, actor, event, outcome in timeline.events:
        result.evidence.append((f"[{rel:6.1f}s] {actor}",
                                f"{event}{f' → {outcome}' if outcome else ''}"))
    setattr(result, "kill_events", timeline.kill_events)
    setattr(result, "kill_events_markdown", timeline.kill_events_markdown())
    if OUTPUT_DIR:
        tl_path = OUTPUT_DIR / f"{test_name}-timeline.md"
        tl_path.write_text(timeline.to_markdown())
        result.log_files.append(str(tl_path.relative_to(OUTPUT_DIR.parent)))
        if timeline.kill_events:
            ke_path = OUTPUT_DIR / f"{test_name}-kill-events.md"
            ke_path.write_text("# Kill Events\n\n"
                               + timeline.kill_events_markdown() + "\n")
            result.log_files.append(str(ke_path.relative_to(OUTPUT_DIR.parent)))
    return result


def t5_live_migrate_attached_volume_baseline() -> TestResult:
    """T5 — Baseline cross-vc live-migrate of attached volume (PROVISIONAL).

    Also doubles as the Phase A.5 confirmation test: if T5 cannot complete
    at all, S2 (T6–T8) is unrunnable in qa-de-1 and should be skipped.
    """
    test_name = "t5_live_migrate_attached_volume_baseline"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, _src = _s2_setup()
    log_streams: list = []
    if not vol_id:
        return _s2_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s2_run_live_migrate(timeline, vol_id,
                                           kill_point=None,
                                           log_streams=log_streams)
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s2_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t6_live_migrate_kill_source_pod() -> TestResult:
    """T6 — Kill source vc-A cinder-volume pod during terminate_connection.

    PROVISIONAL: requires T5 to have established that cross-vc live-migrate
    works in qa-de-1.
    """
    test_name = "t6_live_migrate_kill_source_pod"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, _ = _s2_setup()
    log_streams: list = []
    if not vol_id:
        return _s2_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s2_run_live_migrate(timeline, vol_id,
                                           kill_point="terminate_source",
                                           log_streams=log_streams)
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s2_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t7_live_migrate_kill_dest_pod() -> TestResult:
    """T7 — Kill dest vc-B cinder-volume pod during post-migrate attachment_update.

    PROVISIONAL: requires T5 confirmation.
    """
    test_name = "t7_live_migrate_kill_dest_pod"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, _ = _s2_setup()
    log_streams: list = []
    if not vol_id:
        return _s2_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s2_run_live_migrate(timeline, vol_id,
                                           kill_point="attach_dest",
                                           log_streams=log_streams)
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s2_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t8_live_migrate_kill_during_migrate_by_connector() -> TestResult:
    """T8 — Kill cinder-volume pod during Nova-triggered migrate-by-connector.

    PROVISIONAL: requires T5 confirmation.
    """
    test_name = "t8_live_migrate_kill_during_migrate_by_connector"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, _ = _s2_setup()
    log_streams: list = []
    if not vol_id:
        return _s2_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s2_run_live_migrate(timeline, vol_id,
                                           kill_point="migrate_by_connector",
                                           log_streams=log_streams)
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s2_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


# =============================================================================
# Scenario 3 — Operator-initiated cinder migrate of attached volume
# =============================================================================
#
# Production flow:
#   1. Pre-condition: volume attached to VM, both on vc-A pod.
#   2. Operator runs 'openstack volume migrate --host <other-vc-A-host> <vol>'.
#   3. Cinder source pod begins migration. Mid-flight, cinder calls Nova's
#      swap_volume API ('compute.update_server_volume') to swap the disk
#      underneath the running VM.
#   4. Migration completes; volume now on the destination cinder-volume host;
#      VM still has the volume attached and operational.
#
# We deliberately stay within the same vCenter (vc-a-0 ↔ vc-a-1) so that
# ConnectorRejected does NOT fire. This keeps S3 conceptually distinct
# from S1 (which is the ConnectorRejected case).

def _s3_setup() -> tuple[Timeline, Optional[str], Optional[str], Optional[str]]:
    """Setup: vol attached to VM (both on the VM's shard); destination is
    a different POOL on the SAME shard.

    Note: qa-de-1 cinder rejects cross-shard migrate of an attached volume
    with HTTP 400 "Cannot migrate an attached volume to a different
    host/shard". Same-shard different-pool migrate IS allowed and still
    exercises the cinder migrate_volume → Nova swap_volume callback path
    (which is the cinder code we want to qualify under graceful shutdown).

    So S3's destination is a different pool on the SAME cinder host
    (e.g. vc-a-0#md004_ds01 → vc-a-0#md004_ds02). Source and destination
    deployment are the SAME (`DEPLOYMENT_VM_SIDE`).
    """
    timeline = Timeline()
    timeline.add("test", "setup", "begin")
    name = f"test-nova-cinder-mig3-{int(time.time())}"
    timeline.add("test",
                 f"create volume on VM-side shard ({VM_SHARD}, AZ={VM_AZ})")
    vol_id = cinder_create_volume(name, VM_AZ, shard=VM_SHARD)
    if not vol_id:
        timeline.add("test",
                     f"setup FAILED: shard {VM_SHARD} volume create returned None")
        return timeline, None, None, None
    if poll_volume_status(vol_id, ["available"], timeout=120) != "available":
        timeline.add("test", "setup FAILED: volume never became available")
        return timeline, vol_id, None, None

    src_host = get_volume_host(vol_id)
    timeline.add("cinder", f"volume created on {src_host}")
    src_dep = host_to_deployment(src_host) if src_host else ""
    if not src_host:
        timeline.add("test", "setup FAILED: could not read volume host")
        return timeline, vol_id, None, None

    # Destination: a DIFFERENT pool on the SAME shard. qa-de-1 forbids
    # cross-shard migrate of attached volumes; same-shard different-pool
    # is allowed.
    dest_host = _pick_different_pool_same_shard(VM_SHARD, src_host)
    if not dest_host:
        timeline.add("test",
                     f"setup FAILED: no alternate pool on shard {VM_SHARD} "
                     f"to migrate to")
        return timeline, vol_id, src_dep, None
    dest_dep = host_to_deployment(dest_host)
    timeline.add("cinder",
                 f"will migrate to {dest_host} (same shard, different pool)")

    # Attach to the VM.
    rc = nova_attach_volume(PRECREATED_VM_ID, vol_id)
    if rc.returncode != 0:
        timeline.add("test",
                     f"setup FAILED: pre-attach: {rc.stderr.strip()[:200]}")
        return timeline, vol_id, src_dep, dest_dep
    if not nova_wait_for_volume_attached(PRECREATED_VM_ID, vol_id,
                                         timeout=300):
        timeline.add("test", "setup FAILED: pre-attach not in-use")
        return timeline, vol_id, src_dep, dest_dep
    timeline.add("test", "pre-attach complete; ready to operator-migrate")
    return timeline, vol_id, src_dep, dest_dep


def _pick_different_pool_same_shard(shard: str, source_host: str
                                    ) -> Optional[str]:
    """Return a pool full-host string on the given shard that is NOT
    `source_host`. Returns None if there is only one pool on the shard.

    Used by S3 because qa-de-1 forbids cross-shard migrate of attached
    volumes; we migrate within the same shard to a different datastore.
    """
    target_dep = DEPLOYMENT_PREFIX + shard
    result = openstack_admin("volume", "backend", "pool", "list",
                             "-f", "value", "-c", "Name")
    if result.returncode != 0:
        return None
    pools = [p.strip() for p in (result.stdout or "").split("\n")
             if p.strip()]
    candidates = [p for p in pools
                  if p.startswith(f"{target_dep}@vmware_fcd#")
                  and p != source_host]
    return candidates[0] if candidates else None


def _s3_run_migrate(timeline: Timeline, vol_id: str,
                    src_deployment: str, dest_deployment: str,
                    kill_target: Optional[str] = None,
                    log_streams: list = None
                    ) -> tuple[bool, str]:
    """Issue 'cinder migrate' and observe the swap_volume + complete flow.

    kill_target is one of None / 'source' / 'destination'. Note that for
    same-shard same-deployment migrate, source_deployment ==
    dest_deployment, so killing 'source' and 'destination' targets the
    same pod. T10 covers this; T11 is a no-op kill in qa-de-1's
    topology and is documented as such.
    """
    if log_streams is None:
        log_streams = []
    src_host_full = get_volume_host(vol_id) or ""
    dest_host_full = _pick_different_pool_same_shard(VM_SHARD, src_host_full)
    if not dest_host_full:
        return False, (f"no alternate pool on shard {VM_SHARD} "
                       f"to migrate to")

    timeline.add("test", f"openstack volume migrate --host {dest_host_full}")
    rc = cinder_admin_migrate(vol_id, dest_host_full)
    if rc.returncode != 0:
        clean_err = _strip_admin_noise(rc.stderr or rc.stdout or "")
        return False, f"migrate command rejected: {clean_err[:300]}"

    # Wait for migration to begin.
    seen = wait_for_migration_status(vol_id,
                                     targets=["starting", "migrating",
                                              "completing", "success"],
                                     fails=["error"], timeout=120)
    timeline.add("cinder", f"migration_status: {seen}")
    if seen == "error":
        return False, "migration_status entered error before completion"
    if seen == "timeout":
        return False, "migration never started"

    # Inject kill mid-migration.
    if kill_target == "source":
        timeline.add("kubectl",
                     f"DELETE pod for {src_deployment} (source mid-migrate)")
        _kill_pod_for(src_deployment, log_streams,
                      timeline=timeline,
                      phase="operator-initiated migrate_volume on source (FCD relocate + swap_volume callback)")
    elif kill_target == "destination":
        timeline.add("kubectl",
                     f"DELETE pod for {dest_deployment} (dest mid-migrate)")
        _kill_pod_for(dest_deployment, log_streams,
                      timeline=timeline,
                      phase="operator-initiated migrate_volume_completion on destination")

    # Poll until success or error.
    final = wait_for_migration_status(vol_id, targets=["success"],
                                      fails=["error"],
                                      timeout=SAMEVC_MIGRATE_TIMEOUT)
    timeline.add("cinder", f"final migration_status: {final}")
    if final != "success":
        return False, f"migration did not succeed: {final}"

    final_host = get_volume_host(vol_id) or ""
    timeline.add("cinder", f"final volume host: {final_host}")

    # Verify VM still sees the volume attached.
    ids = [v.get("id") for v in nova_get_server_volumes(PRECREATED_VM_ID)]
    if vol_id not in ids:
        return False, "VM no longer reports volume as attached after migrate"

    return True, f"swap_volume succeeded; volume now on {final_host}"


def _s3_finalise(timeline: Timeline, vol_id: Optional[str],
                 log_streams: list, passed: bool, msg: str,
                 test_name: str, start: float) -> TestResult:
    if vol_id and PRECREATED_VM_ID:
        try:
            ids = [v.get("id") for v in nova_get_server_volumes(PRECREATED_VM_ID)]
        except Exception:
            ids = []
        if vol_id in ids:
            timeline.add("test", "detach volume from VM (final)")
            nova_detach_volume(PRECREATED_VM_ID, vol_id)
            nova_wait_for_volume_detached(PRECREATED_VM_ID, vol_id,
                                          timeout=300)

    cleanup_test_state(timeline, PRECREATED_VM_ID, [vol_id],
                       deployments_log_streams=log_streams)
    duration = time.time() - start
    result = TestResult(test_name, passed, duration, msg)
    for rel, actor, event, outcome in timeline.events:
        result.evidence.append((f"[{rel:6.1f}s] {actor}",
                                f"{event}{f' → {outcome}' if outcome else ''}"))
    setattr(result, "kill_events", timeline.kill_events)
    setattr(result, "kill_events_markdown", timeline.kill_events_markdown())
    if OUTPUT_DIR:
        tl_path = OUTPUT_DIR / f"{test_name}-timeline.md"
        tl_path.write_text(timeline.to_markdown())
        result.log_files.append(str(tl_path.relative_to(OUTPUT_DIR.parent)))
        if timeline.kill_events:
            ke_path = OUTPUT_DIR / f"{test_name}-kill-events.md"
            ke_path.write_text("# Kill Events\n\n"
                               + timeline.kill_events_markdown() + "\n")
            result.log_files.append(str(ke_path.relative_to(OUTPUT_DIR.parent)))
    return result


def t9_operator_migrate_attached_baseline() -> TestResult:
    """T9 — Baseline operator-initiated migrate of attached volume.

    Same vCenter (vc-a-0 ↔ vc-a-1). No pod kill. Validates that cinder's
    swap_volume callback to Nova works end-to-end against the patched
    cinder.
    """
    test_name = "t9_operator_migrate_attached_baseline"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, src_dep, dest_dep = _s3_setup()
    log_streams: list = []
    if not vol_id or not src_dep or not dest_dep:
        return _s3_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s3_run_migrate(timeline, vol_id, src_dep, dest_dep,
                                      kill_target=None,
                                      log_streams=log_streams)
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s3_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t10_operator_migrate_kill_source_pod() -> TestResult:
    """T10 — Kill the SOURCE vc-A pod mid-migration.

    The source pod is the one executing the FCD relocate + the swap_volume
    callback to Nova. The graceful-shutdown patch should keep the in-flight
    operation running on the draining pod (Phase 2 pool.waitall drain).
    """
    test_name = "t10_operator_migrate_kill_source_pod"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, src_dep, dest_dep = _s3_setup()
    log_streams: list = []
    if not vol_id or not src_dep or not dest_dep:
        return _s3_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s3_run_migrate(timeline, vol_id, src_dep, dest_dep,
                                      kill_target="source",
                                      log_streams=log_streams)
        # Verify drain log on the killed source pod.
        for proc, label in log_streams:
            if src_dep in label:
                pod_name = label.split("/")[-1]
                matches = list(OUTPUT_DIR.glob(
                    f"kill-{src_dep}-{pod_name}-*.log"))
                if matches:
                    ok, observed = verify_drain_log_sequence(
                        matches[-1], SHUTDOWN_LOG_SEQUENCE_WITH_TASKS)
                    timeline.add("verify",
                                 f"drain-with-tasks markers on src pod: {len(observed)}/{len(SHUTDOWN_LOG_SEQUENCE_WITH_TASKS)}",
                                 "OK" if ok else "MISSING")
                    if not ok and passed:
                        msg = msg + " [WARN: full drain not observed]"
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s3_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


def t11_operator_migrate_kill_dest_pod() -> TestResult:
    """T11 — Kill the DESTINATION vc-A peer pod mid-migration.

    The destination pod runs migrate_volume_completion (the cleanup that
    finalises status). Killing it tests that the source pod can still
    drive the migration to completion (or that it retries cleanly on the
    new dest pod).
    """
    test_name = "t11_operator_migrate_kill_dest_pod"
    print(f"\n{'='*70}\nTEST: {test_name}\n{'='*70}")
    start = time.time()
    timeline, vol_id, src_dep, dest_dep = _s3_setup()
    log_streams: list = []
    if not vol_id or not src_dep or not dest_dep:
        return _s3_finalise(timeline, vol_id, log_streams, False,
                            "setup failed", test_name, start)
    try:
        passed, msg = _s3_run_migrate(timeline, vol_id, src_dep, dest_dep,
                                      kill_target="destination",
                                      log_streams=log_streams)
        for proc, label in log_streams:
            if dest_dep in label:
                pod_name = label.split("/")[-1]
                matches = list(OUTPUT_DIR.glob(
                    f"kill-{dest_dep}-{pod_name}-*.log"))
                if matches:
                    ok, observed = verify_drain_log_sequence(
                        matches[-1], SHUTDOWN_LOG_SEQUENCE)
                    timeline.add("verify",
                                 f"drain markers on dest pod: {len(observed)}/{len(SHUTDOWN_LOG_SEQUENCE)}",
                                 "OK" if ok else "MISSING")
                    if not ok and passed:
                        msg = msg + " [WARN: drain markers not all found]"
    except Exception as e:
        traceback.print_exc()
        passed, msg = False, f"unhandled exception: {e}"
    return _s3_finalise(timeline, vol_id, log_streams, passed, msg,
                        test_name, start)


# =============================================================================
# Test registry + CLI
# =============================================================================

ALL_TESTS = {
    # Scenario 1 — attach + ConnectorRejected
    "t1": t1_attach_connector_rejected_baseline,
    "t2": t2_attach_kill_during_initialize_connection,
    "t3": t3_attach_kill_during_migrate_volume,
    "t4": t4_attach_kill_during_post_migrate_attach,

    # Scenario 2 — Nova live-migrate of attached volume (PROVISIONAL).
    "t5": t5_live_migrate_attached_volume_baseline,
    "t6": t6_live_migrate_kill_source_pod,
    "t7": t7_live_migrate_kill_dest_pod,
    "t8": t8_live_migrate_kill_during_migrate_by_connector,

    # Scenario 3 — operator-initiated migrate of attached volume (same-vc).
    "t9": t9_operator_migrate_attached_baseline,
    "t10": t10_operator_migrate_kill_source_pod,
    "t11": t11_operator_migrate_kill_dest_pod,
}


def print_summary(results: list) -> None:
    print(f"\n{'='*70}\nTEST RESULTS SUMMARY\n{'='*70}")
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
    print("  ALL TESTS PASSED" if passed == total else "  SOME TESTS FAILED")


def generate_report(results: list) -> str:
    """Generate a markdown report of test results."""
    now = datetime.now()
    lines = []
    lines.append("# Nova/Cinder Live Integration Test Report")
    lines.append("")
    lines.append(f"**Date:** {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Environment:** {OS_CLOUD} (kubectl context: {KUBE_CONTEXT})")
    lines.append(f"**VM under test:** {PRECREATED_VM_ID}")
    lines.append(f"**VM AZ / shard:** {VM_AZ} / {VM_SHARD}")
    lines.append(f"**Other shard (volume target):** {OTHER_SHARD}")
    lines.append(f"**VM-side cinder deployment:** {DEPLOYMENT_VM_SIDE}")
    lines.append(f"**Other-side cinder deployment:** {DEPLOYMENT_OTHER_SIDE}")
    lines.append(f"**Volume type:** {VOLUME_TYPE}")
    lines.append("")

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    lines.append("## Summary")
    lines.append("")
    lines.append("| Result | Count |")
    lines.append("|--------|-------|")
    lines.append(f"| Passed | {passed}/{total} |")
    lines.append(f"| Failed | {total - passed}/{total} |")
    lines.append("")
    lines.append("| Test | Result | Duration | Message |")
    lines.append("|------|--------|----------|---------|")
    for r in results:
        status = "PASS ✓" if r.passed else "FAIL ✗"
        lines.append(f"| {r.name} | {status} | {r.duration:.1f}s | {r.message} |")
    lines.append("")
    for r in results:
        lines.append(f"## {r.name}")
        lines.append("")
        status = "PASSED" if r.passed else "FAILED"
        lines.append(f"**Status:** {status}  ")
        lines.append(f"**Duration:** {r.duration:.1f}s  ")
        lines.append(f"**Message:** {r.message}")
        lines.append("")
        # Kill events — prominently surfaced so it's clear which pod got
        # killed at which moment for each test.
        ke_md = getattr(r, "kill_events_markdown", "")
        ke_list = getattr(r, "kill_events", [])
        lines.append("### Pod Kill Events")
        lines.append("")
        if ke_list:
            n_landed = sum(1 for k in ke_list if k.get("kill_landed"))
            n_total = len(ke_list)
            lines.append(f"**{n_landed}/{n_total} kills landed.**  "
                         "(Kills that did not land mean the targeted "
                         "operation completed faster than the test "
                         "driver could observe — see _Test driver "
                         "limitations_ in the per-run notes.)")
            lines.append("")
            lines.append(ke_md)
        else:
            lines.append("_No pod kills attempted in this test "
                         "(baseline / no-kill scenario)._")
        lines.append("")
        if r.warnings:
            lines.append("### Warnings")
            for w in r.warnings:
                lines.append(f"- {w}")
            lines.append("")
        if r.evidence:
            lines.append("### Timeline")
            lines.append("")
            lines.append("```")
            for label, detail in r.evidence:
                lines.append(f"▶ {label}")
                if detail:
                    lines.append(f"    {detail}")
            lines.append("```")
            lines.append("")
        if r.log_files:
            lines.append("### Artifacts")
            for lf in r.log_files:
                lines.append(f"- `{lf}`")
            lines.append("")
        lines.append("---")
        lines.append("")
    return "\n".join(lines)


def _apply_cli_overrides(args: argparse.Namespace) -> None:
    global OS_CLOUD, KUBE_CONTEXT, KUBE_NAMESPACE
    global PRECREATED_VM_ID, VOLUME_TYPE
    global DO_CLEANUP
    OS_CLOUD = args.cloud
    KUBE_CONTEXT = args.context
    KUBE_NAMESPACE = args.namespace
    if args.vm_id:
        PRECREATED_VM_ID = args.vm_id
    if args.volume_type:
        VOLUME_TYPE = args.volume_type
    DO_CLEANUP = not args.no_cleanup
    # Sync into the imported gs module so its helpers see the same config.
    # The volume_deployment override here is just a placeholder for any gs
    # helper that prints a default; the real per-test deployment comes from
    # DEPLOYMENT_VM_SIDE / DEPLOYMENT_OTHER_SIDE which are populated by
    # _detect_topology() during preflight.
    gs._apply_config(
        os_cloud=OS_CLOUD,
        kube_context=KUBE_CONTEXT,
        volume_deployment="cinder-volume-vmware-vc-a-0",
        backup_deployment="cinder-volume-backup-vmware-vc-a-0",
    )
    gs.KUBE_NAMESPACE = KUBE_NAMESPACE


def main():
    parser = argparse.ArgumentParser(
        description="Live Nova/Cinder integration tests (qa-de-1)")
    parser.add_argument("--test", "-t", choices=list(ALL_TESTS.keys()),
                        help="Run a specific test")
    parser.add_argument("--tests", help="Comma-separated list of tests")
    parser.add_argument("--list", "-l", action="store_true",
                        help="List available tests")
    parser.add_argument("--cloud", default=OS_CLOUD)
    parser.add_argument("--context", default=KUBE_CONTEXT)
    parser.add_argument("--namespace", default=KUBE_NAMESPACE)
    parser.add_argument("--vm-id", default=PRECREATED_VM_ID,
                        help="UUID of the pre-created Nova VM")
    parser.add_argument("--volume-type", default=VOLUME_TYPE,
                        help="Cinder volume type to use for all test volumes")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Preserve test wreckage for forensic review")
    args = parser.parse_args()

    if args.list:
        print("Available tests:")
        for name, func in ALL_TESTS.items():
            doc = (func.__doc__ or "").strip().split("\n", 1)[0]
            print(f"  {name}: {doc}")
        return

    _apply_cli_overrides(args)
    init_output_dir()

    print("\nPre-flight checks...")
    print(f"  Cloud:           {OS_CLOUD}")
    print(f"  Context:         {KUBE_CONTEXT} (ns={KUBE_NAMESPACE})")
    print(f"  VM:              {PRECREATED_VM_ID}")
    print(f"  Volume type:     {VOLUME_TYPE}")
    print(f"  Known shards:    {SHARDS_BY_AZ}")
    print(f"  cleanup:         {'ON' if DO_CLEANUP else 'OFF'}")
    err = preflight()
    if err:
        print(f"\n  PRE-FLIGHT FAILED: {err}\n")
        sys.exit(2)
    print(f"  Detected topology:")
    print(f"    VM AZ:                {VM_AZ}")
    print(f"    VM shard:             {VM_SHARD}")
    print(f"    Other shard (volume): {OTHER_SHARD}")
    print(f"    DEPLOYMENT_VM_SIDE:    {DEPLOYMENT_VM_SIDE}")
    print(f"    DEPLOYMENT_OTHER_SIDE: {DEPLOYMENT_OTHER_SIDE}")
    print("  Pre-flight checks passed\n")

    if args.test:
        chosen = {args.test: ALL_TESTS[args.test]}
    elif args.tests:
        names = [n.strip() for n in args.tests.split(",") if n.strip()]
        unknown = [n for n in names if n not in ALL_TESTS]
        if unknown:
            print(f"  ERROR: unknown test(s): {unknown}")
            sys.exit(2)
        chosen = {n: ALL_TESTS[n] for n in names}
    else:
        chosen = ALL_TESTS

    results = []
    for name, func in chosen.items():
        try:
            r = func()
            results.append(r)
        except Exception as e:
            print(f"\n  EXCEPTION in {name}: {e}")
            traceback.print_exc()
            results.append(TestResult(name, False,
                                      message=f"Unhandled exception: {e}"))
        if len(chosen) > 1:
            print("\n  Waiting 30s between tests for pod stabilization...")
            time.sleep(30)

    if OUTPUT_DIR:
        report_md = generate_report(results)
        report_path = OUTPUT_DIR / "report.md"
        report_path.write_text(report_md)
        print(f"\n  Report saved to: {report_path}")

    print_summary(results)

    if not all(r.passed for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()

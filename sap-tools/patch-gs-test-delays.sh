#!/bin/bash
# Apply or revert the [GS-TEST] artificial 30s delays to a cinder worktree.
#
# These delays give the graceful-shutdown integration tests (PR #358,
# test_graceful_shutdown.py) deterministic in-flight windows for fast
# driver operations. Each delay sleeps 120s.
#   - fcd.py copy_image_to_volume     -> image-backed creates (multiple_operations, sameop create)
#   - fcd.py create_cloned_volume     -> test_inflight_volume_clone / sameop clone
#   - fcd.py extend_volume            -> test_inflight_volume_extend
#   - fcd.py _migrate_unattached      -> test_inflight_migrate_same_vc
#   - fcd.py _migrate_attached_same_vc -> nova T9-T11 migrate tests
#   - backup/manager.py Phase 3->4    -> test_inflight_restore_kill_during_detach
#   - backup/manager.py Phase 4->5    -> test_inflight_restore_kill_during_finalize
#   - nfs_base.py extend_volume       -> test_inflight_volume_extend (--volume-type nfs)
#   - nfs_base.py _clone_source_to_destination_volume -> test_inflight_volume_clone (nfs)
#   - nfs_base.py create_snapshot     -> test_inflight_snapshot_create (nfs)
#   - nfs_base.py create_volume       -> test_inflight_volume_create (nfs)
#   - nfs.py delete_volume            -> test_inflight_volume_delete (nfs)
#
# LOCAL-ONLY test tooling. The delays are NEVER committed; the PR branch
# must stay pristine. Apply to the working copy that will be built into the
# qa-de-1 test image, run the tests, then revert.
#
# Usage:
#   ./patch-gs-test-delays.sh [apply|revert] [WORKTREE_DIR]
#
# Defaults: mode=apply, worktree = parent directory of this script
# (i.e. the cinder worktree that contains sap-tools/).
set -euo pipefail

MODE="${1:-apply}"
WORKTREE="${2:-$(cd "$(dirname "$0")/.." && pwd)}"

FCD="$WORKTREE/cinder/volume/drivers/vmware/fcd.py"
BACKUP="$WORKTREE/cinder/backup/manager.py"
NFS_BASE="$WORKTREE/cinder/volume/drivers/netapp/dataontap/nfs_base.py"
NFS_GENERIC="$WORKTREE/cinder/volume/drivers/nfs.py"
MARKER="[GS-TEST] artificial delay"

[ -f "$FCD" ] || { echo "ERROR: fcd.py not found at $FCD (WORKTREE wrong?)" >&2; exit 1; }
[ -f "$BACKUP" ] || { echo "ERROR: backup/manager.py not found at $BACKUP (WORKTREE wrong?)" >&2; exit 1; }
[ -f "$NFS_BASE" ] || { echo "ERROR: nfs_base.py not found at $NFS_BASE (WORKTREE wrong?)" >&2; exit 1; }
[ -f "$NFS_GENERIC" ] || { echo "ERROR: nfs.py not found at $NFS_GENERIC (WORKTREE wrong?)" >&2; exit 1; }

if [ "$MODE" = "revert" ]; then
    python3 - "$FCD" "$BACKUP" "$NFS_BASE" "$NFS_GENERIC" "$MARKER" <<'PY'
import re
import sys

fcd, backup, nfs_base, nfs_generic, marker = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
pat = re.compile(r"(?m)^[ \t]*import eventlet; eventlet\.sleep\((30|60|120)\)[ \t]*# "
                 + re.escape(marker) + r"[^\n]*\n")
for path in (fcd, backup, nfs_base, nfs_generic):
    src = open(path).read()
    if marker not in src:
        print(f"already clean: {path}")
        continue
    n = src.count(marker)
    open(path, "w").write(pat.sub("", src))
    print(f"reverted: {path} ({n} delay line(s) removed)")
PY
    exit 0
fi

[ "$MODE" = "apply" ] || { echo "ERROR: mode must be 'apply' or 'revert'" >&2; exit 1; }

if grep -q "$MARKER" "$FCD" "$BACKUP" "$NFS_BASE" "$NFS_GENERIC"; then
    echo "ERROR: delays already applied; run 'revert' first" >&2
    exit 1
fi

python3 - "$FCD" "$BACKUP" "$NFS_BASE" "$NFS_GENERIC" "$MARKER" <<'PY'
import re
import sys

fcd, backup, nfs_base, nfs_generic, marker = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

def apply_delay(path, pattern, label):
    src = open(path).read()
    matches = list(re.finditer(pattern, src))
    if len(matches) != 1:
        raise SystemExit(
            f"{path}: anchor {label!r} matched {len(matches)} times (want 1)")
    m = matches[0]
    first = m.group(0).split("\n")[0]
    indent = re.match(r"[ \t]*", first).group(0)
    delay = (f"{indent}import eventlet; eventlet.sleep(120)  # {marker}\n")
    open(path, "w").write(src[:m.start()] + delay + src[m.start():])
    print(f"patched {path}: {label} -> {first.strip()[:60]!r}")

apply_delay(fcd, re.compile(
    r"(?m)^( +)super\(VMwareVStorageObjectDriver,\n"
    r"[ \t]*self\)\.copy_image_to_volume\(context, volume, image_service,"),
    "copy_image_to_volume (image-backed create)")

apply_delay(fcd, re.compile(
    r"(?m)^( +)if src_vref\['attach_status'\] == 'attached':\n"
    r"[ \t]*attachments = src_vref\.volume_attachment\n"),
    "create_cloned_volume")

apply_delay(fcd, re.compile(
    r"(?m)^( +)self\.volumeops\.extend_fcd\(fcd_loc, new_size \* units\.Ki\)\n"
    r"[ \t]*\n"
    r"[ \t]*def _clone_fcd"), "extend_volume")

apply_delay(fcd, re.compile(
    r"(?m)^( +)self\.volumeops\.relocate_fcd\(fcd_loc, ds_ref, volume\.name,"),
    "_migrate_unattached relocate")

apply_delay(fcd, re.compile(
    r"(?m)^( +)self\.volumeops\.relocate_one_disk\(attachedvm, ds_ref, rp_ref,"),
    "_migrate_attached_same_vc relocate")

apply_delay(backup, re.compile(
    r"(?m)^( +)self\._detach_device\(context, attach_info, volume, properties,\n"
    r" +force=True\)"), "restore Phase 3->4 (before detach)")

apply_delay(backup, re.compile(
    r"(?m)^( +)# Regardless of whether the restore was successful, do some"),
    "restore Phase 4->5 (before final status)")

apply_delay(nfs_base, re.compile(
    r"(?m)^( +)LOG\.info\('Extending volume %s\.', volume\['name'\]\)\n"),
    "nfs extend_volume")

apply_delay(nfs_base, re.compile(
    r"(?m)^( +)share = self\._get_volume_location\(source\['id'\]\)\n"),
    "nfs _clone_source_to_destination_volume (clone / create-from-snapshot)")

apply_delay(nfs_base, re.compile(
    r"(?m)^( +)self\._clone_backing_file_for_volume\(snapshot\['volume_name'\],\n"),
    "nfs create_snapshot")

apply_delay(nfs_base, re.compile(
    r"(?m)^( +)self\._do_create_volume\(volume\)\n"),
    "nfs create_volume")

apply_delay(nfs_generic, re.compile(
    r"(?m)^( +)info_path = self\._local_path_volume_info\(volume\)\n"),
    "nfs delete_volume")
PY

echo "OK: [GS-TEST] delays applied to $WORKTREE"
echo "    Run the graceful-shutdown tests, then: ./patch-gs-test-delays.sh revert"

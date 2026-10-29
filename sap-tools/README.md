# Graceful Shutdown — Integration Test Suite (sap-tools)

Integration tests for the cinder graceful-shutdown feature (sapcc/cinder
PR #358). They exercise in-flight volume/backup operations against a **live**
cinder deployment: create/delete/clone/snapshot/extend/migrate, backup +
restore (kill at multiple phases), scheduler rerouting, and the RPC-deregister
behavior — killing a cinder-volume or cinder-backup pod mid-operation and
verifying the operation completes during the graceful drain.

**These tests are destructive**: they kill pods and create/delete real volumes
and backups in the target region. Run them in a QA/test region, never prod.

---

## How it works

The tests need the in-flight window of fast driver operations to be
deterministic so a pod kill can land mid-operation. To achieve this without
putting test code in the PR, `patch-gs-test-delays.sh` inserts 120s
`eventlet.sleep()` lines at specific driver anchor points **into a local
working copy** before the test image is built, and removes them afterwards.
The delays are **never committed** — they live only in the built image.

| Delay anchor | Covers |
|---|---|
| `fcd.py copy_image_to_volume` | image-backed creates (cached or not) |
| `fcd.py create_cloned_volume` | clones |
| `fcd.py extend_volume` | extends |
| `fcd.py _migrate_unattached` / `_migrate_attached_same_vc` | migrations, nova T9-T11 |
| `backup/manager.py` restore Phase 3→4 / 4→5 | restore kill-during-detach/finalize |
| `nfs_base.py` extend/clone/snapshot/create | netapp (`--volume-type nfs`) |
| `nfs.py delete_volume` | netapp delete |

---

## Prerequisites (qa-de-1 defaults)

- `kubectl` → target region (e.g. `u8s sync` for qa-de-1 kubelogin)
- `openstack` + `hammer` CLI (path overridable via `GS_OPENSTACK_BIN` /
  `GS_HAMMER_BIN`; default `~/.sap-py3/bin/`)
- `clouds.yaml` entry for the region (`GS_OS_CLOUD`, default `qa-de-1`)
- A built + deployed cinder image **with the `[GS-TEST]` delays applied**
  (see workflow below)
- Pre-created resources (overridable via env vars, qa-de-1 defaults):
  - `GS_TEST_IMAGE_ID` — image for create-from-image tests (Debian-11 vmdk)
  - `GS_PRECREATED_SOURCE_VOLUME_ID` — an `available` source volume for
    backup/restore tests
  - `GS_PRECREATED_BACKUP_ID` — an `available` backup
  - nova tests additionally need a pre-created VM (`--vm-id`) and detect
    shards at runtime

Other env vars: `GS_KUBE_CONTEXT`, `GS_KUBE_NAMESPACE`, `GS_VOLUME_DEPLOYMENT`,
`GS_BACKUP_DEPLOYMENT`, `GS_TEST_VOLUME_SIZE`, `GS_TEST_VOLUME_TYPE`,
`GS_TEST_VOLUME_SOURCE_FOR_CLONE`.

---

## Workflow: build, deploy, test, revert

```sh
# 0. On the machine building images (Docker running):
cd <cinder worktree on the PR branch>

# 1. Apply the artificial delays to the BUILD working copy (local only)
./sap-tools/patch-gs-test-delays.sh apply

# 2. Build + deploy the graceful-shutdown image (dip picks up the working-tree
#    diff: PR commits + delays + any local fixes)
cc-autodeploy deploy cinder-qa-de-1-graceful

# 3. Verify the image + delays are live, then run the suite
cd sap-tools
python3 test_graceful_shutdown.py            # full 24-test suite (~2h)
python3 test_graceful_shutdown.py --list     # list tests
python3 test_graceful_shutdown.py --test test_inflight_volume_extend
# netapp backends (see caveat below):
python3 test_graceful_shutdown.py --volume-type nfs --test test_inflight_volume_extend

# 4. Nova interaction tests (needs --vm-id)
python3 test_nova_cinder_interactions.py --vm-id <vm-uuid>

# 5. ALWAYS revert the delays afterwards
cd <cinder worktree>
./sap-tools/patch-gs-test-delays.sh revert
```

Results land in `sap-tools/test-results/graceful-shutdown-<timestamp>/`
(report.md + per-test pod logs + timelines).

---

## Known caveats

- **netapp (kvm-stnpca) pods are `NetappFiler`-CRD-owned.** The
  `cinder-volume-kvm-stnpca*` deployments run the NetApp NFS driver and are
  reconciled by the netapp operator; `cc-autodeploy`'s image patch is reverted.
  To test netapp (`--volume-type nfs`), update the `NetappFiler` CRs'
  image field (and re-apply after every reconcile).
- **The delays must be in the built image.** Deploying the PR without
  `patch-gs-test-delays.sh apply` first means fast operations complete before
  the test can kill the pod → "not in-flight at kill" failures.
- **Under load**, scheduler dispatch latency can be 10-100s; the harness
  accounts for it (no hammer calls inside the in-flight poll loop), but a
  heavily loaded region may still produce timing flakes — re-run the test.
- `test_deregister.py` is a local kombu-memory regression harness for the
  deregister mechanics; not part of the live-cluster suite.

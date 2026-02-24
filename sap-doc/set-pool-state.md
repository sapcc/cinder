# Set Pool State API for NetApp NFS Driver

## Overview

This feature allows administrators to set the state of a NetApp NFS storage pool to either **available** or **drain**. When a pool is set to "drain", the Cinder scheduler will stop scheduling new volumes to that pool, allowing you to gracefully migrate workloads off of it.

**Commit:** [aaed57c4ef](https://github.com/sapcc/cinder/commit/aaed57c4ef) - `[SAP] Add new admin API to set pool state`

## How It Works

1. An admin calls the `set_pool_state` API with a pool name and desired state
2. The API sends an RPC message to the volume manager on the target host
3. The volume manager updates the in-memory pool stats with the new state
4. For NetApp NFS drivers, the `set_pool_state()` method stores the pool state as a JSON string in the NetApp volume's **comment field**:
   ```json
   {"pool_state": "down", "pool_down_reason": "pool marked as draining"}
   ```
5. When pool stats are refreshed, the driver reads the comment field to restore the pool state
6. The `sap_pool_down_filter` scheduler filter rejects pools with `pool_state=down` for new volume placements

## Valid Pool States

| State | Description |
|-------|-------------|
| `available` | Pool is up and accepting new volumes (pool_state = "up") |
| `drain` | Pool is down, no new volumes will be scheduled (pool_state = "down") |

## Installation of the Cinder Client Extension

To use this feature from the command line, you need to install the SAP Cinder client extension.

### Prerequisites

- Python 3.8+
- python-cinderclient installed

### Installation Steps

```bash
# Clone the repository
git clone https://github.wdf.sap.corp/I530566/python-sap-cinderclient-ext.git

# Change to the directory
cd python-sap-cinderclient-ext

# Install the package
pip install .
```

The extension will automatically register itself with the `cinder` CLI client.

## Usage

### Command Line (with python-sap-cinderclient-ext)

**Set a pool to drain mode (stop accepting new volumes):**

```bash
cinder set-pool-state <host>@<backend>#<pool_name> drain
```

**Set a pool back to available (accept new volumes):**

```bash
cinder set-pool-state <host>@<backend>#<pool_name> available
```

### Examples

```bash
# Drain a NetApp NFS pool
cinder set-pool-state cinder-volume-netapp-a-0@netapp-nfs#10.0.0.1:/vol1 drain

# Make a pool available again
cinder set-pool-state cinder-volume-netapp-a-0@netapp-nfs#10.0.0.1:/vol1 available
```

### Direct API Usage (cURL)

If you prefer to use the API directly:

**Set a pool to drain mode:**

```bash
curl -X PUT \
  -H "X-Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"host": "cinder-host@netapp-nfs#10.0.0.1:/vol1", "status": "drain"}' \
  https://<cinder-api>/v3/<project_id>/os-sap-contrib/set_pool_state
```

**Set a pool back to available:**

```bash
curl -X PUT \
  -H "X-Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"host": "cinder-host@netapp-nfs#10.0.0.1:/vol1", "status": "available"}' \
  https://<cinder-api>/v3/<project_id>/os-sap-contrib/set_pool_state
```

## API Reference

### Endpoint

**PUT** `/v3/{project_id}/os-sap-contrib/set_pool_state`

### Request Body

```json
{
    "host": "<host>@<backend>#<pool_name>",
    "status": "drain" | "available"
}
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `host` | string | Yes | The full Cinder host string including the pool name (format: `host@backend#pool`) |
| `status` | string | Yes | Either `"available"` or `"drain"` |

### Response

- **202 Accepted** - The request was accepted and the pool state change is being processed
- **400 Bad Request** - Invalid status value or malformed request
- **403 Forbidden** - User does not have permission to set pool state

## Effect on Scheduling

When a pool's state is set to `"drain"`:

- The `pool_state` becomes `"down"`
- The `pool_down_reason` is set to `"pool marked as draining"`
- The `sap_pool_down_filter` scheduler filter will reject the pool for new volume placements
- **Existing volumes on the pool continue to function normally** (attach, detach, snapshot, etc.)

When a pool's state is set to `"available"`:

- The `pool_state` becomes `"up"`
- The `pool_down_reason` is cleared
- The pool will be considered for new volume placements by the scheduler

## Required Policy

The `sap:set_pool_state` policy must allow the user to perform this action. By default, this is restricted to admin users.

Policy definition (in `cinder/policies/sap.py`):

```python
policy.DocumentedRuleDefault(
    name=SET_POOL_STATE_POLICY,
    check_str=base.RULE_ADMIN_API,
    description="Set the pool state for the host",
    operations=[
        {
            'method': 'PUT',
            'path': '/os-sap-contrib/set_pool_state'
        }
    ]
)
```

## Use Cases

### Graceful Pool Retirement

When you need to retire a storage pool:

1. Set the pool to drain: `cinder set-pool-state <pool> drain`
2. Migrate existing volumes off the pool
3. Once empty, decommission the storage

### Maintenance Window

When performing maintenance on storage:

1. Set the pool to drain before maintenance: `cinder set-pool-state <pool> drain`
2. Perform maintenance
3. Set the pool back to available: `cinder set-pool-state <pool> available`

### Capacity Management

When a pool is approaching capacity limits:

1. Set the pool to drain to prevent new allocations
2. Add capacity or migrate volumes
3. Set back to available when ready

## Troubleshooting

### Verify Pool State

Use the scheduler stats to verify the pool state:

```bash
cinder-manage host list
```

Or check the Cinder volume service logs for messages like:

```
INFO Pool state for volume <volume_name>: down
```

### Pool State Not Persisting

If the pool state doesn't persist after a service restart, verify that:

1. The NetApp driver has the `set_volume_comment` method available
2. The ZAPI client is properly configured
3. The volume comment was successfully written to the NetApp backend

Check the logs for warnings like:

```
WARNING Driver <driver_name> can't set the pool state
```

## Related Files

| File | Description |
|------|-------------|
| `cinder/api/contrib/sap.py` | API endpoint implementation |
| `cinder/policies/sap.py` | Policy definitions |
| `cinder/volume/api.py` | Volume API `set_pool_state` method |
| `cinder/volume/rpcapi.py` | RPC API for `set_pool_state` |
| `cinder/volume/manager.py` | Volume manager `set_pool_state` implementation |
| `cinder/volume/drivers/netapp/dataontap/nfs_cmode.py` | NetApp driver `set_pool_state` implementation |
| `cinder/scheduler/filters/sap_pool_down_filter.py` | Scheduler filter for pool state |

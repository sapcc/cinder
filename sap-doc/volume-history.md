# Volume History Tracking

## Overview

This feature provides a complete audit trail of volume lifecycle events by recording every mutation to a volume's database row in a new `volume_history` table. Each history record captures JSON deltas of old/new values along with contextual metadata (user_id, project_id, request_id, action type).

**Branch:** `feature/volume-history-tracking`

## How It Works

1. When a volume operation occurs (create, update, destroy, attach, detach), the DB API layer captures the changes
2. A history record is created with:
   - The volume ID
   - The action type (create, update, destroy, attach, detach)
   - A JSON object containing the changed fields as `{field_name: [old_value, new_value]}`
   - Context metadata (project_id, user_id, request_id)
3. History records are soft-deleted when the parent volume is destroyed
4. History records are purged by the existing `cinder-manage db purge` job

## Configuration

Volume history tracking is enabled by default but can be disabled for high-throughput environments where the additional DB overhead is a concern.

### Config Option

In `cinder.conf`:

```ini
[DEFAULT]
# Enable or disable volume history tracking (default: True)
volume_history_enabled = True
```

### When to Disable

Consider disabling history tracking if:
- You have a very high volume of status transitions (thousands per minute)
- DB latency is critical and every millisecond counts
- You don't need audit trail functionality

### Performance Impact

When enabled, the overhead per operation is:

| Operation | Extra SELECT | Extra INSERT |
|-----------|--------------|--------------|
| `volume_update()` | 1 (by PK) | 1 |
| `volume_destroy()` | 1 (by PK) | 1 |
| `volume_create()` | 0 | 1 |
| `volume_attached()` | 0 | 1 |
| `volume_detached()` | 0 | 1 |

All operations occur within the same DB transaction, and SELECT queries use the primary key index.

## Tracked Actions

| Action | Description | Changes Captured |
|--------|-------------|------------------|
| `create` | Volume creation | All initial field values (old = null) |
| `update` | Volume field update | Only fields that actually changed |
| `destroy` | Volume deletion | Status change to 'deleted' |
| `attach` | Volume attachment | status, attach_status changes |
| `detach` | Volume detachment | status, attach_status changes |

## Database Schema

### volume_history Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(36) | Primary key (UUID) |
| `volume_id` | VARCHAR(36) | Foreign key to volumes.id |
| `project_id` | VARCHAR(255) | Project that performed the action |
| `user_id` | VARCHAR(255) | User that performed the action |
| `request_id` | VARCHAR(255) | OpenStack request ID for correlation |
| `action` | VARCHAR(50) | Action type (create, update, destroy, etc.) |
| `changes` | TEXT | JSON-encoded dict of changes |
| `created_at` | DATETIME | When the action occurred |
| `updated_at` | DATETIME | When the record was last updated |
| `deleted_at` | DATETIME | When the record was soft-deleted |
| `deleted` | BOOLEAN | Soft-delete flag |

### Changes JSON Format

```json
{
    "field_name": [old_value, new_value],
    "status": ["available", "in-use"],
    "attach_status": ["detached", "attached"]
}
```

For create actions, old_value is always `null`:
```json
{
    "id": [null, "550e8400-e29b-41d4-a716-446655440000"],
    "display_name": [null, "my-volume"],
    "size": [null, 10],
    "status": [null, "creating"]
}
```

## Querying History

### DB API

```python
from cinder import db

# Get all history records for a volume
history = db.volume_history_get_all_by_volume(context, volume_id)

for record in history:
    print(f"Action: {record.action}")
    print(f"Changes: {record.changes}")
    print(f"By user: {record.user_id}")
    print(f"At: {record.created_at}")
```

## Migration

The feature adds a new Alembic migration:

- **Migration ID:** `633b14d87cec`
- **Description:** Add volume_history table
- **Dependencies:** `9c74c1c6971f` (quota_add_backup_defaults)

Run the migration with:

```bash
cinder-manage db sync
```

## Soft-Delete and Purge Behavior

Volume history records follow Cinder's standard soft-delete pattern:

1. When a volume is destroyed via `volume_destroy()`, the `VolumeHistory` model is included in `VOLUME_DEPENDENT_MODELS`
2. This causes all history records for that volume to be soft-deleted (deleted=True, deleted_at set)
3. The `cinder-manage db purge` command will permanently delete these records after the configured retention period

## Implementation Details

### Hook Points

History is recorded at these locations in `cinder/db/sqlalchemy/api.py`:

| Function | Action | What's Captured |
|----------|--------|-----------------|
| `volume_create()` | create | All initial values |
| `volume_update()` | update | Only changed fields |
| `volume_destroy()` | destroy | Status transition to 'deleted' |
| `volume_attached()` | attach | Status and attach_status changes |
| `volume_detached()` | detach | Status and attach_status changes |

### Helper Function

```python
def _record_volume_history(context, volume_id, action, changes,
                           project_id=None, user_id=None):
    """Record a volume state change in the volume_history table."""
```

This helper:
- Checks if `CONF.volume_history_enabled` is True (returns early if disabled)
- Creates a new `VolumeHistory` record
- Serializes the changes dict to JSON
- Extracts project_id, user_id, and request_id from context
- Only creates a record if there are actual changes

## Use Cases

### Audit Trail

Track who made changes to a volume and when:

```python
history = db.volume_history_get_all_by_volume(context, volume_id)
for h in history:
    changes = json.loads(h.changes)
    print(f"{h.created_at}: {h.action} by {h.user_id}")
    for field, (old, new) in changes.items():
        print(f"  {field}: {old} -> {new}")
```

### Debugging Issues

Correlate volume state changes with OpenStack request IDs:

```python
# Find all changes made during a specific request
for h in history:
    if h.request_id == 'req-abc123':
        print(f"Found related change: {h.action}")
```

### Compliance

Maintain a record of all volume operations for regulatory compliance.

## Limitations

- **No REST API:** This iteration only provides the DB layer. A REST API endpoint may be added in a future iteration.
- **No real-time notifications:** History is recorded synchronously during DB operations.
- **Large volumes may accumulate many records:** Consider periodic archival for long-lived volumes with frequent updates.

## Related Files

| File | Description |
|------|-------------|
| `cinder/common/config.py` | `volume_history_enabled` config option |
| `cinder/db/sqlalchemy/models.py` | VolumeHistory model definition |
| `cinder/db/sqlalchemy/api.py` | History recording and query functions |
| `cinder/db/api.py` | Pass-through DB API |
| `cinder/db/migrations/versions/633b14d87cec_add_volume_history_table.py` | Alembic migration |
| `cinder/tests/unit/test_db_api.py` | Unit tests (DBAPIVolumeHistoryTestCase) |
| `cinder/tests/unit/db/test_migrations.py` | Migration test (_check_633b14d87cec) |

# Cinder Release Notes: Bobcat to Epoxy

This document summarizes the key features and bug fixes in OpenStack Cinder from the 2023.2 (Bobcat) release through 2025.1 (Epoxy), covering the upgrade path from Antelope. Driver-specific notes are limited to VMware and NetApp drivers.

---

## 2023.2 Bobcat (October 2023)

### Core Cinder Changes

#### Critical Changes
- **Service Token Required**: Nova must be configured to send service tokens for detach operations (Bug #2004555). This is a breaking change if not configured properly.
- Legacy `sqlalchemy-migrate` migrations removed

#### Security
- User attachment delete requests rejected for attachments used by nova instances to prevent data leaks

#### Bug Fixes
- Fixed sparse volume restore regression
- Fixed Ceph backup restore to non-RBD volumes

### NetApp Driver
- **NetApp ONTAP NFS**: Active/Active environment support (including replication)

---

## 2024.1 Caracal (April 2024)

### Core Cinder Changes

#### New Features
- **New API**: `os-extend_volume_completion` volume action for Nova notification

#### Upgrade Notes
- RBD driver now uses trash functionality for deletions - must enable scheduled RBD trash purging or the driver's `enable_deferred_deletion` option
- New `rbd_concurrent_flatten_operations` config option (default: 3)

#### Security
- CVE-2024-32498: qcow2 external data file images now rejected to prevent host information exposure

#### Bug Fixes
- Fixed volume reimage data leak scenario (sparse copy disabled during reimage)
- Fixed quota warnings for backups
- Fixed rollback of volume status if reimage operation fails

### NetApp Driver
- **Space Allocation**: New feature for iSCSI/FCP drivers allows ONTAP and host to see actual space correctly when host deletes data. Controlled via `netapp:space_allocation` extra spec.

---

## 2024.2 Dalmatian (October 2024)

### Core Cinder Changes

#### New Features
- **Clone Across Pools**: New driver capability for efficient cross-pool cloning without falling back to "attach and dd"

#### Bug Fixes
- Fixed Ceph backup process error handling (stderr now preserved for debugging)
- Fixed cinder-manage quota sync command
- Fixed cleanup of snapshot status when backup source is a snapshot

### NetApp Driver
- **NetApp ONTAP iSCSI/FC**: Active/Active environment support (including replication)

### VMware Driver
- No significant changes in this release

---

## 2025.1 Epoxy (April 2025)

### Core Cinder Changes

#### New Features
- **New WSGI Module**: `cinder.wsgi.api:application` for easier deployment with gunicorn/uWSGI
- **Ceph Backup**: Option to keep only last n snapshots per backup (`backup_ceph_snap_retention_count`)
- **Cgroups v2**: Support for LVM backend migrations
- **cinder-manage**: New `volume update_service` command to fix service_uuid references blocking database purges
- Service_uuid now automatically updated for volumes when new cinder-volume service is created

#### Bug Fixes
- Fixed NFS snapshot connection_info update for attached volumes
- Fixed RBD volume-to-image upload with different formats
- Fixed reimage operation with volume snapshot-backed images
- Fixed driver-assisted migration on retype operations
- Fixed cinder-manage purge deleted rows foreign key constraint errors
- Fixed REST API returning 500 instead of 409 for attachment update conflicts

### NetApp Driver
- **Certificate-Based Authentication**: New option for operators preferring certificate auth over username/password
- **Synchronous Mirror Support**: New `netapp_replication_policy` option for sync mirror and other replication policies
- **FlexGroup Snapshots**: Support for creating snapshots on FlexGroup pools (ZAPI and REST for ONTAP 9.14+)
- **total_volumes Capability**: New capability for filtering backends when pool reaches max volumes (1024 limit)
- **Consistency Groups**: Extended support to NVMe/TCP protocol
- **Bug Fix**: Fixed issue where driver didn't account for storage limits when provisioning volumes

### VMware Driver
- No significant changes in this release

---

## Summary of Key Themes

| Theme | Description |
|-------|-------------|
| **Security Hardening** | CVE-2024-32498 fix, service token requirements, attachment deletion restrictions |
| **RBD Changes** | Trash-based deletion, concurrent flatten limits, deferred deletion |
| **NetApp Active/Active** | NFS (Bobcat), iSCSI/FC (Dalmatian) gained Active/Active HA support |
| **NetApp Auth** | Certificate-based authentication added in Epoxy |
| **NetApp Replication** | Synchronous mirror support added in Epoxy |

---

## Critical Upgrade Considerations

1. **Service Tokens (Bobcat+)**: Nova must send service tokens or volume detach will fail. Configure Nova with service tokens AND ensure cinder recognizes the nova service user's role (default: `service` role).

2. **RBD Trash (Caracal+)**: Enable RBD trash purging on the Ceph cluster OR enable the Cinder RBD driver's `enable_deferred_deletion` option.

3. **CVE-2024-32498 (Caracal+)**: qcow2 images with external data files will be rejected with `ImageUnacceptable` error.

4. **Database Purge Issues**: If database purges fail due to service_uuid foreign key issues, use `cinder-manage volume update_service` command (available in Epoxy, backported to earlier releases).

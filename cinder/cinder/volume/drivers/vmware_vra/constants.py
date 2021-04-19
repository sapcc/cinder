"""
vRA REST URL mappings
"""

#Blueprint
LOGIN_API = "/csp/gateway/am/api/login"
BLUEPRINT_REQUESTS_API = "/blueprint/api/blueprint-requests"
BLUEPRINTS_API = "/blueprint/api/blueprints"

#IAAS
PROJECTS_GET_API = "/iaas/api/projects"
RESOURCE_TRACKER_API = "/iaas/api/request-tracker/"
CREATE_VOLUME_API = "/iaas/api/block-devices"
BLOCK_DEVICE_API = "/iaas/api/block-devices/"
CREATE_VOLUME_SNAPSHOT_API = "/iaas/api/block-devices/{id}/operations/snapshots"
GET_VOLUME_SNAPSHOT_API = "/iaas/api/block-devices/{volume_id}/snapshots/{snapshot_id}"
GET_ALL_SNAPSHOTS_API = "/iaas/api/block-devices/{volume_id}/snapshots"
DELETE_VOLUME_API = "/iaas/api/block-devices/{id}?purge=true&forceDelete=true"
EXTEND_VOLUME_API = "/iaas/api/block-devices/{id}?capacityInGB={capacityInGB}"

#Catalog
CATALOG_ITEM_API = "/catalog/api/items/"
CATALOG_ITEM_REQUEST = "/catalog/api/items/{catalog_item_id}/request"

#Deployment
DEPLOYMENT_REQUEST_API = "/deployment/api/deployments/{depId}/requests"

#Catalog item names
CATALOG_CREATE_VOLUME_FROM_SNAPSHOT = "Create Volume from Snapshot"
CATALOG_CREATE_VOLUME_CLONE = "Clone Volume"
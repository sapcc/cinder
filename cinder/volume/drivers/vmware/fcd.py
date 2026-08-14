# Copyright (c) 2017 VMware, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
VMware VStorageObject driver

Volume driver based on VMware VStorageObject aka First Class Disk (FCD). This
driver requires a minimum vCenter version of 6.5.
"""
import time

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units
from oslo_utils import versionutils
from oslo_vmware import exceptions as vexc
from oslo_vmware import image_transfer
from oslo_vmware.objects import datastore
from oslo_vmware import vim_util

from cinder.common import constants
from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder.volume.drivers.vmware import vmdk
from cinder.volume.drivers.vmware import volumeops as vops
from cinder.volume import volume_utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

LOCATION_DRIVER_NAME = 'VMwareVcFcdDriver'


@interface.volumedriver
class VMwareVStorageObjectDriver(vmdk.VMwareVcVmdkDriver):
    """Volume driver based on VMware VStorageObject"""

    # 1.0 - initial version based on vSphere 6.5 vStorageObject APIs
    # 1.1 - support for vStorageObject snapshot APIs
    # 1.2 - support for SPBM storage policies
    # 1.3 - support for retype
    VERSION = '1.3.0'

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "VMware_CI"

    # minimum supported vCenter version
    MIN_SUPPORTED_VC_VERSION = '6.5'

    STORAGE_TYPE = constants.VSTORAGE

    # flag this driver as not supporting independent snapshots
    has_independent_snapshots = False

    # FCD Cross vcenter migration is available in 8.0.3
    FCD_CROSS_VC_MIGRATION_VC_VERSION = '8.0.3'

    # SAP: the attachment version
    # Any changes to the connection_info returned by
    # the driver's initialize_connection should
    # increment this version.
    # 1.0.0 - initial version
    # 1.0.1 - properly set data's 'datacenter' as MoRef value
    ATTACHMENT_VERSION = '1.0.1'

    def _driver_name(self):
        return LOCATION_DRIVER_NAME

    def do_setup(self, context):
        """Any initialization the volume driver needs to do while starting.

        :param context: The admin context.
        """
        super(VMwareVStorageObjectDriver, self).do_setup(context)
        self.volumeops.set_vmx_version('vmx-13')
        vc_67_compatible = versionutils.is_compatible(
            '6.7.0', self._vc_version, same_major=False)
        cross_vc_migration = versionutils.is_compatible(
            self.FCD_CROSS_VC_MIGRATION_VC_VERSION, self._vc_version,
            same_major=False)
        # self._use_fcd_snapshot = vc_67_compatible
        # Hard code this for now until we decide we want real snapshots
        self._use_fcd_snapshot = False
        self._storage_policy_enabled = vc_67_compatible
        self._use_fcd_cross_vc_migration = cross_vc_migration
        self._admin_context = context

        if CONF.sap_allow_independent_snapshots:
            # If we setup cinder to allow independent snapshots, we can
            # use them.  This means snapshots will be clones.
            self.has_independent_snapshots = True

    def get_volume_stats(self, refresh=False):
        """Collects volume backend stats.

        :param refresh: Whether to discard any cached values and force a full
                        refresh of stats.
        :returns: dict of appropriate values.
        """
        stats = super(VMwareVStorageObjectDriver, self).get_volume_stats(
            refresh=refresh)
        stats['storage_protocol'] = self.STORAGE_TYPE
        return stats

    def _select_ds_fcd(self, volume):
        (host, rp, folder, summary) = self._select_ds_for_volume(volume)
        return summary.datastore

    def _get_temp_image_folder_from_volume(self, volume):
        (host_ref, _resource_pool,
            folder, summary) = self._select_ds_for_volume(volume)

        folder_path = volume.name + '/'
        dc_ref = self.volumeops.get_dc(host_ref)
        self.volumeops.create_datastore_folder(
            summary.name, folder_path, dc_ref)

        return (dc_ref, summary, folder_path)

    def _get_disk_type(self, volume):
        extra_spec_disk_type = super(
            VMwareVStorageObjectDriver, self)._get_disk_type(volume)
        return vops.VirtualDiskType.get_virtual_disk_type(extra_spec_disk_type)

    def _get_storage_profile_id(self, volume):
        if self._storage_policy_enabled:
            return super(
                VMwareVStorageObjectDriver, self)._get_storage_profile_id(
                    volume)

    def _provider_location_to_moref_location(self, ds_location):
        """Translate the provider location to the moref format.

        We store the provider location in the database in the format:
        <fcd_id>@<datastore name>

        Translate that to the moref format:
        <fcd_id>@<datastore moref>
        """
        fcd_id, ds_name = ds_location.split('@')
        ds_ref = self.ds_sel.get_ds_ref_by_name(ds_name)
        return "%s@%s" % (fcd_id, vim_util.get_moref_value(ds_ref))

    def _snap_provider_location_to_ds_name_location(self, moref_location):
        """Translate the provider location to the datastore name for snapshot.

        snapshot provider location is in the format of a json string:
        {"fcd_location": "<fcd_id>@<datastore moref>",
         "fcd_snapshot_id": "<snapshotid>"}
        convert this to
        {"fcd_location": "<fcd_id>@<datastore name>",
         "fcd_snapshot_id": "<snapshotid>"}
        """
        # first get the moref snap provider location object.
        fcd_snap_loc = vops.FcdSnapshotLocation.from_provider_location(
            moref_location
        )
        # now convert the snap fcd location to the datastore name format.
        snap_location_str = self._provider_location_to_ds_name_location(
            fcd_snap_loc.fcd_loc.provider_location()
        )
        # create a new fcd snap location object with the snap location
        snap_loc = vops.FcdLocation.from_provider_location(snap_location_str)
        # replace the existing fcd_loc with the new datastore snap location
        fcd_snap_loc.fcd_loc = snap_loc
        return fcd_snap_loc.provider_location()

    def _snap_provider_location_to_moref_location(self, ds_location):
        """Translate the provider location to the moref format for snapshot.


        snapshot provider location is in the format of a json string:
        {"fcd_location": "<fcd_id>@<datastore name>",
         "fcd_snapshot_id": "<snapshotid>"}
        convert this to
        {"fcd_location": "<fcd_id>@<datastore moref>",
         "fcd_snapshot_id": "<snapshotid>"}
        """
        # first get the datastore snap provider location object.
        fcd_snap_loc = vops.FcdSnapshotLocation.from_provider_location(
            ds_location
        )
        if not fcd_snap_loc:
            return None

        # now convert the snap fcd location to the moref format.
        snap_location_str = self._provider_location_to_moref_location(
            fcd_snap_loc.fcd_loc.provider_location()
        )
        # create a new fcd snap location object with the snap location
        snap_loc = vops.FcdLocation.from_provider_location(snap_location_str)
        fcd_snap_loc.fcd_loc = snap_loc
        return fcd_snap_loc.provider_location()

    @volume_utils.trace
    def _get_adapter_type(self, volume):
        """Get the adapter type for the volume.

        :param volume: Volume object
        :returns: Adapter type
        """
        if volume.bootable:
            # Fetch the adapter type from the image metadata
            image_metadata = volume.glance_metadata
            if image_metadata:
                return image_metadata.get(
                    'vmware_adaptertype',
                    super(VMwareVStorageObjectDriver,
                          self)._get_adapter_type(volume)
                )
            else:
                return super(VMwareVStorageObjectDriver,
                             self)._get_adapter_type(volume)
        else:
            return super(VMwareVStorageObjectDriver,
                         self)._get_adapter_type(volume)

    def set_qos_on_fcd(self, fcd_loc, qos_profile_name):
        ds_ref = fcd_loc.ds_ref()
        context = self._admin_context
        netapp_api = self._remote_netapp_api
        netapp_fqdn = self.volumeops.get_netapp_for_ds(ds_ref)
        netapp_host = self.get_netapp_cinder_host(netapp_fqdn)
        vmdk_path = self.volumeops.get_vmdk_path_for_fcd(fcd_loc=fcd_loc)
        if qos_profile_name and netapp_host:
            self.volumeops.set_qos(context, ds_ref, netapp_api, netapp_host,
                                   vmdk_path, qos_profile_name)

    @volume_utils.trace
    def create_volume(self, volume):
        """Create a new volume on the backend.

        :param volume: Volume object containing specifics to create.
        :returns: (Optional) dict of database updates for the new volume.
        """
        if volume.glance_metadata:
            LOG.debug("FCD volume is being created from image.")
            return

        disk_type = self._get_disk_type(volume)
        ds_ref = self._select_ds_fcd(volume)
        profile_id = self._get_storage_profile_id(volume)
        key_id = self._register_kmip_key_id(volume)
        fcd_loc = self.volumeops.create_fcd(
            volume.id, volume.name, volume.size * units.Ki, ds_ref,
            disk_type, profile_id=profile_id, key_id=key_id)
        qos_profile_name = volume.volume_type.extra_specs.get(
            'vmware:netapp_qos_profile')
        try:
            self.set_qos_on_fcd(fcd_loc, qos_profile_name)
        except Exception:
            LOG.warning("Can't apply %s QOS profile on %s volume",
                        qos_profile_name, volume.id)

        # Convert the provider_location from the moref format to the
        # datastore name format to store in the cinder DB.
        provider_location = self._provider_location_to_ds_name_location(
            fcd_loc.provider_location()
        )

        return {'provider_location': provider_location}

    @volume_utils.trace
    def _delete_fcd(self, provider_loc, delete_folder=True):
        fcd_loc = vops.FcdLocation.from_provider_location(provider_loc)
        try:
            return self.volumeops.delete_fcd(
                fcd_loc, delete_folder=delete_folder
            )
        except vexc.VimException as ex:
            if "could not be found" in str(ex):
                LOG.warning("FCD not found: %s.", fcd_loc.fcd_id)
                return True
            else:
                raise ex

    @volume_utils.trace
    def delete_volume(self, volume):
        """Delete a volume from the backend.

        :param volume: The volume to delete.
        """
        if not volume.provider_location:
            LOG.warning("FCD provider location is empty for volume %s",
                        volume.id)
        else:
            try:
                # we store the PL with a datastore name, but volumeops uses
                # the moref format, so we need to convert it.
                provider_loc = self._provider_location_to_moref_location(
                    volume.provider_location
                )
                self._delete_fcd(provider_loc)
            except vexc.VimException as ex:
                if "could not be found" in str(ex):
                    LOG.warning("FCD deletion failed for %s not found. "
                                "delete_volume is considered successful.",
                                volume.id)
                else:
                    raise ex

    def init_kvm_hw(self, volume, connector, initiator_data):
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            ))
        vmdk_path = self.volumeops.get_vmdk_path_for_fcd(fcd_loc=fcd_loc)
        mount_path = self.volumeops._get_mount_path(fcd_loc.ds_ref())
        _, dir_path, file_path = vops.split_datastore_path(vmdk_path)
        raw_file_path = file_path.replace('.vmdk', '-flat.vmdk')
        connection_info = {
            'driver_volume_type': 'nfs',
            'data': {
                'export': mount_path,
                'name': "%s%s" % (dir_path, raw_file_path),
                'format': 'raw',
                'version': self.ATTACHMENT_VERSION,
            },
            'mount_point_base': '/var/lib/cinder/mnt'
        }
        return connection_info

    @volume_utils.trace
    def initialize_connection(self, volume, connector, initiator_data=None):
        """Allow connection to connector and return connection info.

        :param volume: The volume to be attached.
        :param connector: Dictionary containing information about what is being
                          connected to.
        :param initiator_data: (Optional) A dictionary of driver_initiator_data
                               objects with key-value pairs that have been
                               saved for this initiator by a driver in previous
                               initialize_connection calls.
        :returns: A dictionary of connection information.
        """
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            )
        )
        summary = self.volumeops.get_summary(fcd_loc.ds_ref())
        if summary.type == "NFS41":
            # We connect to KVM not VMware, only if DS is NFS
            if 'connection_capabilities' not in connector:
                data = self.init_kvm_hw(volume, connector, initiator_data)
                return data
        # Check that connection_capabilities match
        # This ensures the connector is bound to the same vCenter service
        if 'connection_capabilities' in connector:
            missing = set(self._get_connection_capabilities()) -\
                set(connector['connection_capabilities'])
            if missing:
                raise exception.ConnectorRejected(
                    reason="Connector is missing %s" % ', '.join(missing))

        # We don't need this parameters unless backup is created/restored
        backup = False
        backing_moref = ""
        vmdk_path = ""
        datacenter = ""
        if 'cinder-volume-backup' in connector['host']:
            backup = True

        if backup:
            backing = self.volumeops.get_backing(volume.name, volume.id)
            if not backing:
                create_params = {vmdk.CREATE_PARAM_DISK_LESS: True}
                backing = self._create_backing(volume,
                                               create_params=create_params)
                self.volumeops.attach_fcd(backing, fcd_loc)
            backing_moref = backing.value
            vmdk_path = self.volumeops.get_vmdk_path(backing)
            datacenter = vim_util.get_moref_value(
                self.volumeops.get_dc(backing))
        else:
            try:
                vmdk_path = self.volumeops.get_vmdk_path_for_fcd(
                    fcd_loc=fcd_loc)
                datacenter = vim_util.get_moref_value(
                    self.volumeops.get_dc(fcd_loc.ds_ref()))
            except Exception:
                LOG.warning("Can't find the fcd object: %s, "
                            "this can be due to nova migration or "
                            "VMware issue", fcd_loc.fcd_id)

        connection_info = {
            'driver_volume_type': self.STORAGE_TYPE,
            'data': {
                'volume_id': volume.id,
                'name': volume.name,
                'id': fcd_loc.fcd_id,
                'ds_ref_val': fcd_loc.ds_ref_val,
                'ds_name': volume_utils.extract_host(volume.host,
                                                     level='pool'),
                'adapter_type': self._get_adapter_type(volume),
                'profile_id': self._get_storage_profile_id(volume),
                'volume': backing_moref,
                'vmdk_size': volume.size * units.Gi,
                'vmdk_path': vmdk_path,
                'datacenter': datacenter,
                'version': self.ATTACHMENT_VERSION,
            }
        }

        kmip_key_id = self._get_kmip_key_id(volume)
        if kmip_key_id:
            connection_info['data']['kmip_key_id'] = kmip_key_id

        # This is needed by the backup process (os-brick)
        if self._is_os_brick_connector(connector):
            connection_info['data']['config'] = self._get_connector_config()

        # instruct os-brick to use ImportVApp and HttpNfc upload for
        # disconnecting the volume
        #
        # If we are migrating to this volume, we need to
        # create a writeable handle for the migration to work.
        if self._is_volume_subject_to_import_vapp(volume):
            connection_info['data']['import_data'] = \
                self._get_connection_import_data(volume)

        LOG.debug("Connection info for volume %(name)s: %(connection_info)s.",
                  {'name': volume.name, 'connection_info': connection_info})
        return connection_info

    @volume_utils.trace
    def terminate_connection(self, volume, connector, force=False, **kwargs):
        if (connector and ('connection_capabilities' not in connector)):
            try:
                # If the volume was extended on KVM host, the VMware VMDK file
                # still contains the old size, so to refresh geometry we call
                # VslmExtendDisk_Task to fix it
                fcd_loc = vops.FcdLocation.from_provider_location(
                    self._provider_location_to_moref_location(
                        volume.provider_location
                    )
                )
                self.volumeops.extend_fcd(fcd_loc, volume.size * units.Ki)
            except Exception:
                pass
        # Checking if the connection was used to restore from a backup. In
        # that case, the VMDK connector in os-brick created a new backing
        # which will replace the initial one. Here we set the proper name
        # and backing uuid for the new backing, because os-brick doesn't do it.
        if (connector and self._is_os_brick_connector(connector)
                and self._is_volume_subject_to_import_vapp(volume)):
            try:
                # we store the PL with a datastore name, but volumeops uses
                # the moref format, so we need to convert it.
                backing = self.volumeops.get_backing_by_uuid(volume.id)
                provider_loc = self._provider_location_to_moref_location(
                    volume.provider_location
                )
                if backing:
                    self._delete_fcd(provider_loc, delete_folder=False)
            except vexc.VimException as ex:
                if "could not be found" in str(ex):
                    pass
                else:
                    raise ex

            # hack to give vcenter time to release the vmdk lock
            # If we remove this, then calling move_vmdk_file or
            # copy_vmdk_file will fail with a lock error.
            time.sleep(10)

            (_, _, _, summary) = self._select_ds_for_volume(volume)
            conv_prov_loc = self._provider_location_to_ds_name_location
            dest_vmdk_path = f"[{summary.name}] {volume.id}/{volume.id}.vmdk"
            backing = self.volumeops.get_backing_by_uuid(volume.id)
            # If there is no backing, we did the restore using NFS client
            if backing:
                self.volumeops.rename_backing(backing, volume.name)
                self.volumeops.update_backing_disk_uuid(backing, volume.id)
                profile_id = self._get_storage_profile_id(volume)
                vmware_host_ip = self.configuration.vmware_host_ip

                # Now move the vmdk into the original folder here?
                dest_dc = self.volumeops.get_dc(backing)
                src_vmdk_path = self.volumeops.get_vmdk_path(backing)

                disk_device = self.volumeops._get_disk_device(backing)
                self.volumeops.detach_disk_from_backing(backing, disk_device)
                self.volumeops.move_vmdk_file(dest_dc, src_vmdk_path,
                                              dest_vmdk_path)
                fcd_loc = vops.FcdLocation.from_provider_location(
                    provider_loc)
                fcd_id = fcd_loc.fcd_id
                fcd_loc = self.volumeops.update_fcd_after_backup_restore(
                    volume, backing, profile_id, vmware_host_ip,
                    dest_vmdk_path, fcd_id)
                provider_location = conv_prov_loc(
                    fcd_loc.provider_location()
                )
                volume.update({'provider_location': provider_location})
                volume.save()
        else:
            backing = self.volumeops.get_backing_by_uuid(volume.id)
            fcd_loc = vops.FcdLocation.from_provider_location(
                self._provider_location_to_moref_location(
                    volume.provider_location))
            if backing:
                try:
                    self.volumeops.detach_fcd(backing, fcd_loc)
                except Exception:
                    LOG.warning("Can't detach backing for volume %s",
                                volume.id)
                finally:
                    LOG.debug("Cleaning up backing for volume %s", volume.id)
                    self._delete_temp_backing(backing)

    def _validate_container_format(self, container_format, image_id):
        if container_format and container_format != 'bare':
            msg = _("Container format: %s is unsupported, only 'bare' "
                    "is supported.") % container_format
            LOG.error(msg)
            raise exception.ImageUnacceptable(image_id=image_id, reason=msg)

    def _destroy_backing(self, backing):
        """Destroys the shadowVM VirtualMachine object"""
        disk_device = self.volumeops._get_disk_device(backing)
        self.volumeops.detach_disk_from_backing(backing,
                                                disk_device)
        self.volumeops.delete_backing(backing)

    @volume_utils.trace
    def copy_image_to_volume(self, context, volume, image_service, image_id,
                             disable_sparse=False):
        """Fetch the image from image_service and write it to the volume.

        :param context: Security/policy info for the request.
        :param volume: The volume to create.
        :param image_service: The image service to use.
        :param image_id: The image identifier.
        :param disable_sparse: Enable or disable sparse copy. Default=False.
                               This parameter is ignored by VMware driver.
        :returns: Model updates.
        """
        super(VMwareVStorageObjectDriver,
              self).copy_image_to_volume(context, volume, image_service,
                                         image_id, disable_sparse)
        backing = self.volumeops.get_backing_by_uuid(volume.id)
        ds_ref = self.volumeops.get_datastore(backing)
        dc_ref = self.volumeops.get_dc(ds_ref)
        vmdk_path = self.volumeops.get_vmdk_path(backing)
        device = self.volumeops.get_disk_device(backing, vmdk_path)
        fcd_id_imp = None
        if hasattr(device, 'vDiskId'):
            fcd_id_imp = device.vDiskId.id
            fcd_loc = vops.FcdLocation.from_provider_location("%s@%s" %
                                                              (fcd_id_imp,
                                                               ds_ref.value))
            self.volumeops.rename_fcd(fcd_loc, ds_ref, volume.name)

        self._destroy_backing(backing)
        ds_path = datastore.DatastorePath.parse(vmdk_path)
        dc_path = self.volumeops.get_inventory_path(dc_ref)

        vmdk_url = datastore.DatastoreURL(
            'https', self.configuration.vmware_host_ip, ds_path.rel_path,
            dc_path, ds_path.datastore)
        if not fcd_id_imp:
            fcd_loc = self.volumeops.register_disk(
                str(vmdk_url), volume.name, ds_ref)

        profile_id = self._get_storage_profile_id(volume)
        if profile_id:
            key_id = self._register_kmip_key_id(volume)
            self.volumeops.update_fcd_policy(
                fcd_loc, profile_id, key_id=key_id, is_new=True)
            if key_id:
                # VMware moves the disk to another path after encryption
                vmdk_path = self._volumeops.get_vmdk_path_for_fcd(
                    fcd_loc=fcd_loc)

        self.volumeops.update_fcd_vmdk_uuid(ds_ref,
                                            vmdk_path, volume.id)

        # Extend the volume if needed
        # break this up to 2 lines to pass pep8
        metadata = image_service.show(context, image_id)
        if hasattr(metadata, 'virtual_size'):
            image_gib = int(metadata['virtual_size'] / units.Gi)
        else:
            image_gib = int(self.volumeops.get_vmdk_size_for_fcd(
                fcd_loc=fcd_loc) / units.Gi)
        image_size = 1 if image_gib == 0 else image_gib
        self._extend_if_needed(fcd_loc, image_size, volume.size)
        qos_profile_name = volume.volume_type.extra_specs.get(
            'vmware:netapp_qos_profile')
        try:
            self.set_qos_on_fcd(fcd_loc, qos_profile_name)
        except Exception:
            LOG.warning("Can't apply %s QOS profile on %s volume",
                        qos_profile_name, volume.id)
        provider_location = self._provider_location_to_ds_name_location(
            fcd_loc.provider_location()
        )
        volume.update({'provider_location': provider_location})
        volume.save()

    def _register_kmip_key_id(self, obj):
        if getattr(obj, 'encryption_key_id', None):
            owner = self.configuration.vmware_host_ip
            return self._kmip_api.kmip_register(
                obj.encryption_key_id,
                owner,
                "default")
        return None

    def _get_kmip_key_id(self, obj):
        barbican_id = getattr(obj, 'encryption_key_id', None)
        if barbican_id:
            return self._kmip_api.get_kmip_id(barbican_id)
        return None

    @volume_utils.trace
    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image.

        :param context: Security/policy info for the request.
        :param volume: The volume to copy.
        :param image_service: The image service to use.
        :param image_meta: Information about the image.
        :returns: Model updates.
        """
        self._validate_disk_format(image_meta['disk_format'])

        # convert the datastore name provider location to what the
        # volumeops uses, which is the moref format.
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            )
        )

        attached = False
        try:
            create_params = {vmdk.CREATE_PARAM_DISK_LESS: True}
            backing = self._create_backing(volume, create_params=create_params)
            self.volumeops.attach_fcd(backing, fcd_loc)
            attached = True

            vmdk_file_path = self.volumeops.get_vmdk_path(backing)
            conf = self.configuration

            # retrieve store information from extra-specs
            store_id = volume.volume_type.extra_specs.get(
                'image_service:store_id')

            # TODO (whoami-rajat): Remove store_id and base_image_ref
            #  parameters when oslo.vmware calls volume_utils wrapper of
            #  upload_volume instead of image_utils.upload_volume
            image_transfer.upload_image(
                context,
                conf.vmware_image_transfer_timeout_secs,
                image_service,
                image_meta['id'],
                volume.project_id,
                session=self.session,
                host=conf.vmware_host_ip,
                port=conf.vmware_host_port,
                vm=backing,
                vmdk_file_path=vmdk_file_path,
                vmdk_size=volume.size * units.Gi,
                image_name=image_meta['name'],
                store_id=store_id,
                base_image_ref=volume_utils.get_base_image_ref(volume))
        finally:
            if attached:
                self.volumeops.detach_fcd(backing, fcd_loc)
            backing = self.volumeops.get_backing_by_uuid(volume.id)
            if backing:
                self._delete_temp_backing(backing)

    @volume_utils.trace
    def extend_volume(self, volume, new_size):
        """Extend the size of a volume.

        :param volume: The volume to extend.
        :param new_size: The new desired size of the volume.
        """
        # convert the datastore name provider location to what the
        # volumeops uses, which is the moref format.
        if volume['attach_status'] == 'attached':
            attachments = volume.volume_attachment
            connector = None
            for attach in attachments:
                connector = attach.connector
            if 'connection_capabilities' not in connector:
                LOG.debug('Non-VMware hypervisor for volume:%s,'
                          'extend will be done via nova', volume['id'])
                return

        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            )
        )
        self.volumeops.extend_fcd(fcd_loc, new_size * units.Ki)

    def _clone_fcd(self, provider_loc, volume, dest_ds_ref,
                   disk_type=vops.VirtualDiskType.THIN,
                   profile_id=None, key_id=None):
        # Must pass in the moref format for the provider location
        fcd_loc = vops.FcdLocation.from_provider_location(provider_loc)
        cf = self._session.vim.client.factory
        consumer = self.volumeops.get_fcd_consumer(fcd_loc.ds_ref(),
                                                   fcd_loc.id(cf))
        if consumer and not self._use_fcd_cross_vc_migration:
            # The volume is attached, so we need to clone the volume
            (host, rp, folder, _) = self._select_ds_for_volume(volume)
            return self.volumeops.clone_fcd_attached(
                consumer, volume, fcd_loc, dest_ds_ref, disk_type,
                host, rp, folder, profile_id,
                self.configuration.vmware_host_ip
            )
        else:
            return self.volumeops.clone_fcd(
                volume, fcd_loc, dest_ds_ref,
                disk_type, profile_id=profile_id, key_id=key_id
            )

    def _create_volume_from_fcd_kvm(self, volume, src_vref):
        """We need to call out to netapp in case of clone

        :param volume: target volume
        :param src_vref: source volume
        """
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                src_vref.provider_location
            )
        )
        disk_type = self._get_disk_type(src_vref)
        ds_ref = fcd_loc.ds_ref()
        profile_id = self._get_storage_profile_id(volume)
        key_id = self._register_kmip_key_id(volume)
        new_fcd_loc = self.volumeops.create_fcd(
            volume.id, volume.name, src_vref.size * units.Ki, ds_ref,
            disk_type, profile_id=profile_id, key_id=key_id)
        vmdk_path_new = self.volumeops.get_vmdk_path_for_fcd(
            fcd_loc=new_fcd_loc)
        vmdk_path_src = self.volumeops.get_vmdk_path_for_fcd(
            fcd_loc=fcd_loc)
        netapp_api = self._remote_netapp_api
        netapp_fqdn = self.volumeops.get_netapp_for_ds(fcd_loc.ds_ref())
        netapp_host = self.get_netapp_cinder_host(netapp_fqdn)
        src_mpath = self.volumeops._get_mount_path(fcd_loc.ds_ref())
        src_ds_mpath = src_mpath.split(':')[1]
        src_lif_ip = src_mpath.split(':')[0]
        netapp_vol = src_ds_mpath.split('/')[1]
        ds_split = vops.split_datastore_path
        vserver = netapp_api.get_vserver_for_ip(self._admin_context,
                                                host=netapp_host,
                                                lif_ip=src_lif_ip)
        _, folder_path, src_vmdk_file = ds_split(vmdk_path_src)
        _, new_folder_path, dst_vmdk_file = ds_split(vmdk_path_new)
        src_flat_file = src_vmdk_file.replace('.vmdk', '-flat.vmdk')
        dst_flat_file = dst_vmdk_file.replace('.vmdk', '-flat.vmdk')
        if len(src_ds_mpath.split('/')) == 3:
            # This case if we have qtree DS
            src_qtree = src_ds_mpath.split('/')[2]
            src_path = "%s/%s%s" % (src_qtree, folder_path,
                                    src_flat_file)
            dest_path = "%s/%s%s" % (src_qtree, new_folder_path,
                                     dst_flat_file)
        else:
            # Normal DS expected that the vol_name=DS_NAME
            src_path = "%s%s" % (folder_path, src_flat_file)
            dest_path = "%s%s" % (new_folder_path, dst_flat_file)
        netapp_api.clone_file(self._admin_context, host=netapp_host,
                              flex_vol=netapp_vol, src_path=src_path,
                              dest_path=dest_path, vserver=vserver,
                              dest_exists=True, is_snapshot=True)

        cur_size = src_vref.size
        self._extend_if_needed(new_fcd_loc, cur_size, volume.size)
        p_location = self._provider_location_to_ds_name_location(
            new_fcd_loc.provider_location()
        )

        return {'provider_location': p_location}

    def _create_snap_kvm_fcd(self, snapshot):
        """Creates snapshot via NetApp rpc

        :param snapshot: The snapshot to create.
        """
        volume = snapshot.volume
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            )
        )
        disk_type = self._get_disk_type(volume)
        ds_ref = self._select_ds_fcd(volume)
        profile_id = self._get_storage_profile_id(volume)
        key_id = self._register_kmip_key_id(volume)
        snapshot_name = "snapshot-%s" % snapshot['id']
        # We create a new empty fcd with the same size as the volume
        fcd_loc_snap = self.volumeops.create_fcd(
            snapshot.id, snapshot_name, volume.size * units.Ki, ds_ref,
            disk_type, profile_id=profile_id, key_id=key_id)
        vmdk_path_snap = self.volumeops.get_vmdk_path_for_fcd(
            fcd_loc=fcd_loc_snap)
        vmdk_path_vol = self.volumeops.get_vmdk_path_for_fcd(
            fcd_loc=fcd_loc)
        netapp_api = self._remote_netapp_api
        netapp_fqdn = self.volumeops.get_netapp_for_ds(fcd_loc.ds_ref())
        netapp_host = self.get_netapp_cinder_host(netapp_fqdn)
        src_mpath = self.volumeops._get_mount_path(fcd_loc.ds_ref())
        src_ds_mpath = src_mpath.split(':')[1]
        src_lif_ip = src_mpath.split(':')[0]
        netapp_vol = src_ds_mpath.split('/')[1]
        ds_split = vops.split_datastore_path
        vserver = netapp_api.get_vserver_for_ip(self._admin_context,
                                                host=netapp_host,
                                                lif_ip=src_lif_ip)
        _, folder_path, src_vmdk_file = ds_split(vmdk_path_vol)
        _, snap_folder_path, dst_vmdk_file = ds_split(vmdk_path_snap)
        src_flat_file = src_vmdk_file.replace('.vmdk', '-flat.vmdk')
        dst_flat_file = dst_vmdk_file.replace('.vmdk', '-flat.vmdk')
        if len(src_ds_mpath.split('/')) == 3:
            # This case if we have qtree DS
            src_qtree = src_ds_mpath.split('/')[2]
            src_path = "%s/%s%s" % (src_qtree, folder_path,
                                    src_flat_file)
            dest_path = "%s/%s%s" % (src_qtree, snap_folder_path,
                                     dst_flat_file)
        else:
            # Normal DS expected that the vol_name=DS_NAME
            src_path = "%s%s" % (folder_path, src_flat_file)
            dest_path = "%s%s" % (snap_folder_path, dst_flat_file)
        netapp_api.clone_file(self._admin_context, host=netapp_host,
                              flex_vol=netapp_vol, src_path=src_path,
                              dest_path=dest_path, vserver=vserver,
                              dest_exists=True, is_snapshot=True)

        p_location = self._provider_location_to_ds_name_location(
            fcd_loc_snap.provider_location()
        )

        return p_location

    @volume_utils.trace
    def create_snapshot(self, snapshot):
        """Creates a snapshot.

        :param snapshot: Information for the snapshot to be created.
        """
        if snapshot.volume['attach_status'] == 'attached':
            attachments = snapshot.volume.volume_attachment
            connector = None
            for attach in attachments:
                connector = attach.connector
            if 'connection_capabilities' not in connector:
                p_location = self._create_snap_kvm_fcd(snapshot)
                return {'provider_location': p_location}

        if self._use_fcd_snapshot:
            fcd_loc = vops.FcdLocation.from_provider_location(
                provider_location=self._provider_location_to_moref_location(
                    snapshot.volume.provider_location
                )
            )
            description = "snapshot-%s" % snapshot.id
            fcd_snap_loc = self.volumeops.create_fcd_snapshot(
                fcd_loc, description=description)
            p_location = self._snap_provider_location_to_ds_name_location(
                fcd_snap_loc.provider_location()
            )
            return {'provider_location': p_location}

        # This is a clone operattion, not a snapshot operation.
        ds_ref = self._select_ds_fcd(snapshot.volume)
        # convert the datastore name provider location to what the
        # volumeops uses, which is the moref format.
        provider_location = self._provider_location_to_moref_location(
            snapshot.volume.provider_location
        )
        profile_id = self._get_storage_profile_id(snapshot.volume)
        key_id = self._register_kmip_key_id(snapshot)
        cloned_fcd_loc = self._clone_fcd(provider_location, snapshot, ds_ref,
                                         profile_id=profile_id,
                                         key_id=key_id)
        # Now convert the fcd snapshot provider location to the
        # datastore format
        provider_location = self._provider_location_to_ds_name_location(
            cloned_fcd_loc.provider_location()
        )
        # this is an fcd provider location format because
        # it's not a snapshot.
        return {'provider_location': provider_location}

    @volume_utils.trace
    def delete_snapshot(self, snapshot):
        """Deletes a snapshot.

        :param snapshot: The snapshot to delete.
        """
        if not snapshot.provider_location:
            LOG.debug("FCD snapshot location is empty.")
            return

        # We might be in a situation where the snapshot is still a vmdk
        # based snapshot, but the owning volume has been migrated to fcd.
        # in which case the provider_location still looks like a vmdk path.
        # If so, we need to call the parent class to delete the snapshot.
        if '/' in snapshot.provider_location:
            return super(VMwareVStorageObjectDriver,
                         self).delete_snapshot(snapshot)
        snap_location = self._snap_provider_location_to_moref_location(
            snapshot.provider_location
        )
        if snap_location:
            fcd_snap_loc = vops.FcdSnapshotLocation.from_provider_location(
                snap_location)
            return self.volumeops.delete_fcd_snapshot(fcd_snap_loc)
        else:
            provider_loc = self._provider_location_to_moref_location(
                snapshot.provider_location
            )
            return self._delete_fcd(provider_loc)

    def _extend_if_needed(self, fcd_loc, cur_size, new_size):
        if new_size > cur_size:
            self.volumeops.extend_fcd(fcd_loc, new_size * units.Ki)

    @volume_utils.trace
    def _create_volume_from_fcd(self, provider_location, cur_size, volume):
        ds_ref = self._select_ds_fcd(volume)
        disk_type = self._get_disk_type(volume)
        profile_id = self._get_storage_profile_id(volume)
        # convert the datastore name provider location to what the
        # volumeops uses, which is the moref format.
        provider_loc = self._provider_location_to_moref_location(
            provider_location
        )
        key_id = self._register_kmip_key_id(volume)
        cloned_fcd_loc = self._clone_fcd(
            provider_loc, volume, ds_ref, disk_type=disk_type,
            profile_id=profile_id, key_id=key_id)
        self._extend_if_needed(cloned_fcd_loc, cur_size, volume.size)
        qos_profile_name = volume.volume_type.extra_specs.get(
            'vmware:netapp_qos_profile')
        try:
            self.set_qos_on_fcd(cloned_fcd_loc, qos_profile_name)
        except Exception:
            LOG.warning("Can't apply %s QOS profile on %s volume",
                        qos_profile_name, volume.id)
        # Convert the provider location from the moref format to the
        # datastore name format to store in the cinder DB.
        p_location = self._provider_location_to_ds_name_location(
            cloned_fcd_loc.provider_location()
        )
        return {'provider_location': p_location}

    @volume_utils.trace
    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot.

        :param volume: The volume to be created.
        :param snapshot: The snapshot from which to create the volume.
        :returns: A dict of database updates for the new volume.
        """
        # First convert the datastore provider location to a moref format
        snap_location = self._snap_provider_location_to_moref_location(
            snapshot.provider_location
        )
        if snap_location:
            fcd_snap_loc = vops.FcdSnapshotLocation.from_provider_location(
                snap_location)
            profile_id = self._get_storage_profile_id(volume)
            key_id = self._register_kmip_key_id(volume)
            fcd_loc = self.volumeops.create_fcd_from_snapshot(
                fcd_snap_loc, volume.name, volume.id, profile_id=profile_id,
                key_id=key_id)
            self._extend_if_needed(fcd_loc, snapshot.volume_size, volume.size)
            # Convert the provider location from the moref format to the
            # datastore name format to store in the cinder DB.
            provider_location = self._provider_location_to_ds_name_location(
                fcd_loc.provider_location()
            )
            return {'provider_location': provider_location}
        else:
            return self._create_volume_from_fcd(snapshot.provider_location,
                                                snapshot.volume.size, volume)

    @volume_utils.trace
    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume.

        :param volume: New Volume object
        :param src_vref: Source Volume object
        """

        if src_vref['attach_status'] == 'attached':
            attachments = src_vref.volume_attachment
            connector = None
            for attach in attachments:
                connector = attach.connector
            if 'connection_capabilities' not in connector:
                return self._create_volume_from_fcd_kvm(volume, src_vref)
        return self._create_volume_from_fcd(
            src_vref.provider_location, src_vref.size, volume)

    @volume_utils.trace
    def retype(self, context, volume, new_type, diff, host):
        if not self._storage_policy_enabled:
            return True

        profile = self._get_storage_profile(volume)
        new_profile = self._get_extra_spec_storage_profile(new_type['id'])
        if profile == new_profile:
            LOG.debug("Storage profile matches between new type and old type.")
            return True

        if self._in_use(volume):
            LOG.warning("Cannot change storage profile of attached FCD.")
            return False

        # convert the datastore name provider location to what the
        # volumeops uses, which is the moref format.
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            )
        )
        new_profile_id = self.ds_sel.get_profile_id(new_profile)
        self.volumeops.update_fcd_policy(fcd_loc, new_profile_id.uniqueId)
        return True

    @volume_utils.trace
    def native_cross_vc_migrate_volume(self, context, volume, host,
                                       vcenter, fcd_loc):
        dest_host = host['host']
        cross_vc = False
        if self._vcenter_instance_uuid != vcenter:
            cross_vc = True
        if volume['attach_status'] == 'attached':
            if self._vcenter_instance_uuid != vcenter:
                return self._migrate_attached_cross_vc(context, dest_host,
                                                       volume, fcd_loc)
            else:
                return self._migrate_attached_same_vc(context, dest_host,
                                                      volume, fcd_loc)
        else:
            return self._migrate_unattached(context, dest_host, volume,
                                            fcd_loc, cross_vc)

    @volume_utils.trace
    def shadow_cross_vc_migrate_volume(self, context, volume, host,
                                       vcenter, fcd_loc):
        dest_host = host['host']
        if volume['attach_status'] == 'attached':
            if self._vcenter_instance_uuid != vcenter:
                # This is a cross vcenter migration
                return self._migrate_attached_cross_vc(
                    context, dest_host, volume, fcd_loc)
            else:
                # we can migrate to another datastore in the same vcenter
                return self._migrate_attached_same_vc(
                    context, dest_host, volume, fcd_loc)
        else:
            if self._vcenter_instance_uuid != vcenter:
                # This is a cross vcenter migration
                return self._migrate_unattached_cross_vc_legacy(
                    context, dest_host, volume, fcd_loc)
            else:
                return self._migrate_unattached(
                    context, dest_host, volume, fcd_loc)

    @volume_utils.trace
    def migrate_volume(self, context, volume, host):
        """Migrate a volume to the specified host.

        If the backing is not created, returns success.
        """
        false_ret = (False, None)
        allowed_statuses = ['available', 'reserved', 'in-use', 'maintenance',
                            'extending']
        if volume['status'] not in allowed_statuses:
            LOG.debug('Only %s volumes can be migrated using backend '
                      'assisted migration. Falling back to generic migration.',
                      " or ".join(allowed_statuses))
            return false_ret

        if 'location_info' not in host['capabilities']:
            return false_ret
        info = host['capabilities']['location_info']
        try:
            (driver_name, vcenter) = info.split(':')
        except ValueError:
            return false_ret

        if driver_name != self._driver_name():
            return false_ret

        # convert the provider location to the moref format
        # so we can pass it to the volumeops
        fcd_loc = vops.FcdLocation.from_provider_location(
            self._provider_location_to_moref_location(
                volume.provider_location
            )
        )

        if self._use_fcd_cross_vc_migration:
            return self.native_cross_vc_migrate_volume(context, volume, host,
                                                       vcenter, fcd_loc)
        else:
            return self.shadow_cross_vc_migrate_volume(context, volume, host,
                                                       vcenter, fcd_loc)

    def _get_fcd_location(self, fcd_id, datastore_ref):
        """Wrapper for the remote API to return the fcd location."""
        # We have to do this to avoid a circular reference from the
        # Remote api back to this file.
        return vops.FcdLocation(fcd_id, datastore_ref)

    @volume_utils.trace
    def _migrate_unattached(self, context, dest_host, volume, fcd_loc,
                            cross_vc=False):

        @volume_utils.trace
        def _qtree_ds(remote_ds_info, local_ds_info):
            # Check if the source and destination DS are hosted in the same vol
            # mount_path explaned: <ip>:/<vol_name>/<qtree>
            # lpath 192.168.8.1:/nfs_stnpca2_st1_ds1/nfs_stnpca2_st1_ds1_vc_b_2
            # mpath 192.168.8.1:/nfs_stnpca2_st1_ds1/nfs_stnpca2_st1_ds1_vc_b_0
            if local_ds_info.type != "NFS41":
                return False
            try:
                mpath = remote_ds_info['mount_path'].split(':')[1]
                lpath = self.volumeops._get_mount_path(
                    local_ds_info.datastore)
                lpath = lpath.split(':')[1]
                if len(lpath.split('/')) == 3 and len(mpath.split('/')) == 3:
                    if mpath.split('/')[1] == lpath.split('/')[1]:
                        return True
            except Exception:
                return False

        ds_info = self._remote_api.select_ds_for_volume(context,
                                                        cinder_host=dest_host,
                                                        volume=volume)
        if cross_vc:
            service_locator = self._remote_api.get_service_locator_info(
                context,
                dest_host)

        else:
            service_locator = None

        (_, _, _, src_ds_info) = self._select_ds_for_volume(volume)

        ds_ref = vim_util.get_moref(ds_info['datastore'], 'Datastore')
        new_profile_id = ds_info.get('profile_id')

        if ds_info['datastore_url'] != src_ds_info['url']:
            if cross_vc and _qtree_ds(ds_info, src_ds_info):
                disk_type = self._get_disk_type(volume)
                key_id = self._register_kmip_key_id(volume)
                prov_loc, new_disk_path = self._remote_api.create_fcd(
                    context, dest_host, volume.id, volume.name,
                    volume.size * units.Ki, ds_ref.value, disk_type,
                    profile_id=new_profile_id, key_id=key_id)
                tgt_ds_mpath = ds_info['mount_path'].split(':')[1]
                netapp_api = self._remote_netapp_api
                netapp_fqdn = self.volumeops.get_netapp_for_ds(
                    fcd_loc.ds_ref())
                netapp_host = self.get_netapp_cinder_host(netapp_fqdn)
                self.volumeops.migrate_unattached_qtree(fcd_loc,
                                                        tgt_ds_mpath,
                                                        new_disk_path,
                                                        netapp_api,
                                                        netapp_host,
                                                        context)
                # delete the source fcd after migration complete
                self.volumeops.delete_fcd(fcd_loc)
                volume.update({'provider_location': prov_loc})
                volume.save()
                return (True, None)
            else:
                self.volumeops.relocate_fcd(fcd_loc, ds_ref, volume.name,
                                            service_locator, new_profile_id)

        # if we are migrating to a different DS on the same host
        # there is no reason to call the remote_api to get the
        # provider location.
        dest_host_name = volume_utils.extract_host(dest_host, 'host')
        src_host_name = volume_utils.extract_host(volume['host'], 'host')
        if dest_host_name == src_host_name:
            fcd_loc_new = vops.FcdLocation(fcd_loc.fcd_id, ds_ref.value)
            # Convert the provider location from the moref format to the
            # datastore name format to store in the cinder DB.
            prov_loc = self._provider_location_to_ds_name_location(
                fcd_loc_new.provider_location()
            )
        else:
            prov_loc = self._remote_api.get_fcd_provider_location(
                context, dest_host, fcd_loc.fcd_id, ds_ref.value)

        volume.update({'provider_location': prov_loc})
        volume.save()
        key_id = self._get_kmip_key_id(volume)
        if cross_vc:
            if self._use_fcd_cross_vc_migration:
                # Use the native FCD cross vc migration from 8.0U3 and >
                fcd_loc_moid = vops.FcdLocation(fcd_loc.fcd_id, ds_ref.value)
                prov_loc_moid = fcd_loc_moid.provider_location()
                self._remote_api.update_fcd_policy(
                    context, dest_host, prov_loc_moid, new_profile_id,
                    key_id=key_id)
            else:
                # TODO(hemna): Add the temporary shadow migration
                LOG.error("TODO: Need to add shadow migration for cross vc.")
                raise NotImplementedError()
        # todo-update policy-onremote vc and move it to folder
        else:
            fcd_loc_moid = vops.FcdLocation(fcd_loc.fcd_id, ds_ref.value)
            self.volumeops.update_fcd_policy(fcd_loc_moid, new_profile_id,
                                             key_id=key_id)

        return (True, None)

    @volume_utils.trace
    def _migrate_attached_same_vc(self, context, dest_host, volume, fcd_loc):
        get_vm_by_uuid = self.volumeops.get_backing_by_uuid
        # reusing the get_backing_by_uuid to lookup the attacher vm
        if volume['multiattach']:
            raise NotImplementedError()
        attachments = volume.volume_attachment
        instance_uuid = attachments[0]['instance_uuid']
        attachedvm = get_vm_by_uuid(instance_uuid)
        ds_info = self._remote_api.select_ds_for_volume(context,
                                                        cinder_host=dest_host,
                                                        volume=volume)
        rp_ref = vim_util.get_moref(ds_info['resource_pool'], 'ResourcePool')
        ds_ref = vim_util.get_moref(ds_info['datastore'], 'Datastore')
        self.volumeops.relocate_one_disk(attachedvm, ds_ref, rp_ref,
                                         volume_id=volume.id,
                                         profile_id=ds_info.get('profile_id'))
        fcd_loc_new = vops.FcdLocation(fcd_loc.fcd_id, ds_ref.value)
        # Convert the provider location from the moref format to the
        # datastore name format to store in the cinder DB.
        prov_loc = self._provider_location_to_ds_name_location(
            fcd_loc_new.provider_location()
        )
        volume.update({'provider_location': prov_loc})
        volume.save()

        dc_ref = self.volumeops.get_dc(ds_ref)
        vmdk_path = self.volumeops.get_vmdk_path_for_fcd(fcd_loc=fcd_loc_new)

        self._update_fcd_attachment_info_for_nova(
            context, volume, fcd_loc_new,
            vmdk_path,
            dc_ref
        )
        return (True, None)

    @volume_utils.trace
    def _migrate_attached_cross_vc(self, context, dest_host, volume, fcd_loc):
        # Qtree DS has different pool name cross vc, need to update prov_loc
        ds_info = self._remote_api.select_ds_for_volume(context,
                                                        cinder_host=dest_host,
                                                        volume=volume)
        ds_ref = vim_util.get_moref(ds_info['datastore'], 'Datastore')
        prov_loc = self._remote_api.get_fcd_provider_location(
            context, dest_host, fcd_loc.fcd_id, ds_ref.value)
        volume.update({'provider_location': prov_loc})
        volume.save()
        return (True, None)

    @volume_utils.trace
    def _migrate_unattached_cross_vc_legacy(self, context, dest_host, volume,
                                            fcd_loc):
        # Migrate to other vc on older than 8.0u3 will create a temporary
        # Shadow vm backing
        ds_info = self._remote_api.select_ds_for_volume(context,
                                                        cinder_host=dest_host,
                                                        volume=volume)
        service_locator = self._remote_api.get_service_locator_info(
            context, dest_host)
        ds_ref = vim_util.get_moref(ds_info['datastore'], 'Datastore')
        new_profile_id = ds_info.get('profile_id')
        hosts = self.volumeops.get_connected_hosts(fcd_loc.ds_ref())
        host = vim_util.get_moref(hosts[0], 'HostSystem')
        create_params = {vmdk.CREATE_PARAM_DISK_LESS: True}
        backing = self._create_backing(volume, host, create_params)
        self.volumeops.attach_fcd(backing, fcd_loc)
        host_ref = vim_util.get_moref(ds_info['host'], 'HostSystem')
        rp_ref = vim_util.get_moref(ds_info['resource_pool'], 'ResourcePool')
        self.volumeops.relocate_backing(backing, ds_ref, rp_ref, host_ref,
                                        profile_id=ds_info.get('profile_id'),
                                        service=service_locator)
        fcd_loc_new = vops.FcdLocation(fcd_loc.fcd_id, ds_ref.value)
        self._remote_api.destory_backing(context, dest_host, volume)
        self._remote_api.update_fcd_policy(
            context, dest_host,
            fcd_loc_new.provider_location(),
            new_profile_id)

        # cleanup on target volume mgr
        return (True, None)

from cinder import interface
from cinder.volume import driver
from cinder.volume import configuration
from cinder.volume.drivers.vmware_vra import volumeops
from config import volume_config

from oslo_log import log as logging
from oslo_config import cfg

LOG = logging.getLogger(__name__)

@interface.volumedriver
class Volume(driver.VolumeDriver):

    def __init__(self, *args, **kwargs):
        super(Volume, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_config.vmdk_opts)
        self.volumeops = volumeops.VraVolumeOps()

    def manage_existing(self, volume, existing_ref):
        pass

    def manage_existing_get_size(self, volume, existing_ref):
        pass

    def unmanage(self, volume):
        pass

    def create_volume(self, volume):
        self.volumeops.create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        print('CREATE VOLUME FROM SNAPHSOT')

    def create_cloned_volume(self, volume, src_vref):
        pass

    def delete_volume(self, volume):
        pass

    def clone_image(self, context, volume, image_location,
                    image_meta, image_service):
        pass

    def extend_volume(self, volume, new_size):
        pass

    def create_export(self, context, volume, connector):
        pass

    def ensure_export(self, context, volume):
        pass

    def remove_export(self, context, volume):
        raise NotImplementedError()

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        pass

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        pass

    def revert_to_snapshot(self, context, volume, snapshot):
        pass

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        pass

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        pass

    def retype(self, context, volume, new_type, diff, host):
        return False, None

    def terminate_connection(self, volume, connector, **kwargs):
        pass

    def check_for_setup_error(self):
        pass

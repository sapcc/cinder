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
        LOG.debug("volume: {}, existing_ref: {}".format(volume, existing_ref))

    def manage_existing_get_size(self, volume, existing_ref):
        LOG.debug("volume: {}, existing_ref: {}".format(volume, existing_ref))

    def unmanage(self, volume):
        LOG.debug("volume: {}".format(volume))

    def create_volume(self, volume):
        LOG.debug("volume: {}".format(volume))
        self.volumeops.create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        LOG.debug("volume: {}, snapshot: {}".format(volume, snapshot))
        self.volumeops.create_volume_from_snapshot(volume, snapshot)

    def create_cloned_volume(self, volume, src_vref):
        LOG.debug("volume: {}, src_vref: {}".format(volume, src_vref))
        self.volumeops.clone_volume(volume, src_vref)

    def delete_volume(self, volume):
        LOG.debug("volume: {}".format(volume))
        self.volumeops.delete_volume(volume)

    def clone_image(self, context, volume, image_location,
                    image_meta, image_service):
        LOG.debug("volume: {}, image_location: {}, image_meta: {}, image_service: {}".
                  format(volume, image_location, image_meta, image_service))

    def extend_volume(self, volume, new_size):
        LOG.debug("volume: {}, new_size:{}".format(volume, new_size))

    def create_export(self, context, volume, connector):
        LOG.debug("volume: {}, connector: {}".format(volume, connector))

    def ensure_export(self, context, volume):
        LOG.debug("volume: {}".format(volume))

    def remove_export(self, context, volume):
        LOG.debug("volume: {}".format(volume))
        raise NotImplementedError()

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        LOG.debug("snapshot: {}".format(snapshot))
        self.volumeops.create_snapshot(snapshot)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        LOG.debug("snapshot: {}".format(snapshot))

    def revert_to_snapshot(self, context, volume, snapshot):
        LOG.debug("volume:{}, snapshot: {}".format(volume, snapshot))

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        LOG.debug("volume:{}, image_service: {}, image_id: {}".format(volume, image_service, image_id))

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        LOG.debug("volume:{}, image_service: {}, image_meta: {}".format(volume, image_service, image_meta))

    def retype(self, context, volume, new_type, diff, host):
        LOG.debug("volume:{}, new_type: {}, diff: {}, host: {}".format(volume, new_type, diff, host))
        return False, None

    def terminate_connection(self, volume, connector, **kwargs):
        LOG.debug("volume:{}, connector: {}".format(volume, connector))

    def check_for_setup_error(self):
        pass

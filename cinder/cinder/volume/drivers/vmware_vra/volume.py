from cinder import interface
from cinder.volume import driver
from cinder.volume import configuration
from cinder.volume.drivers.vmware_vra import volumeops

from oslo_log import log as logging
from oslo_config import cfg

LOG = logging.getLogger(__name__)

vmdk_opts = [
    cfg.StrOpt('vmware_host_ip',
               help='IP address for connecting to VMware vRA server.'),
    cfg.PortOpt('vmware_host_port',
                default=443,
                help='Port number for connecting to VMware vRA server.'),
    cfg.StrOpt('vmware_host_username',
               help='Username for authenticating with VMware vRA '
                    'server.'),
    cfg.StrOpt('vmware_host_password',
               help='Password for authenticating with VMware vRA '
                    'server.',
               secret=True),
]

CONF = cfg.CONF
CONF.register_opts(vmdk_opts, group=configuration.SHARED_CONF_GROUP)


@interface.volumedriver
class Volume(driver.VolumeDriver):

    def __init__(self, *args, **kwargs):
        super(Volume, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(vmdk_opts)
        self.vra_host = self.configuration.vmware_host_ip
        self.vra_username = self.configuration.vmware_host_username
        self.vra_password = self.configuration.vmware_host_password
        self.volumeops = volumeops.VraVolumeOps(self.vra_host,
                                                self.vra_username,
                                                self.vra_password)

    def manage_existing(self, volume, existing_ref):
        pass

    def manage_existing_get_size(self, volume, existing_ref):
        pass

    def unmanage(self, volume):
        pass

    def create_volume(self, volume):
        self.volumeops.create_volume(volume)

    def create_volume_from_snaphot(self, snapshot):
        pass

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
        raise NotImplementedError()

    def ensure_export(self, context, volume):
        raise NotImplementedError()

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

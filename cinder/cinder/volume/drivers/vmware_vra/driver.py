import volume

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VMwareVolumeDriver(volume.Volume):

    def __init__(self, *args, **kwargs):
        LOG.debug(50 * "-" + "VMwareVolumeDriver initialized" + 50 * "-")
        super(VMwareVolumeDriver, self).__init__(*args, **kwargs)

    def accept_transfer(self, context, volume, new_user, new_project):
        pass

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: The volume to be attached
        :param connector: Dictionary containing information about what is being
                          connected to.
        :returns conn_info: A dictionary of connection information.
        """
        return

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector.

        :param volume: The volume to be disconnected.
        :param connector: A dictionary describing the connection with details
                          about the initiator. Can be None.
        """
        return

    def get_driver_options(self):
        pass

    def check_for_setup_error(self):
        return

    def get_volume_stats(self, refresh=False):
        pass


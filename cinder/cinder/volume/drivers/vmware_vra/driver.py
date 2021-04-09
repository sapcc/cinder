import volume

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VMwareVolumeDriver(volume.Volume):

    def __init__(self, *args, **kwargs):
        super(VMwareVolumeDriver, self).__init__(*args, **kwargs)

    def accept_transfer(self, context, volume, new_user, new_project):
        LOG.debug("volume: {}, new_user: {}, new_project: {}".format(volume, new_user, new_project))

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: The volume to be attached
        :param connector: Dictionary containing information about what is being
                          connected to.
        :returns conn_info: A dictionary of connection information.
        """

        connection_info = {'driver_volume_type': 'vmdk'}
        connection_info['data'] = {
            'volume': volume.name,
            'volume_id': volume.id,
            'name': volume.name
        }

        return connection_info

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector.

        :param volume: The volume to be disconnected.
        :param connector: A dictionary describing the connection with details
                          about the initiator. Can be None.
        """
        LOG.debug("volume: {}, connector: {}".format(volume, connector))
        return

    def get_driver_options(self):
        pass

    def check_for_setup_error(self):
        return

    def get_volume_stats(self, refresh=False):
        pass

    def remove_export(self, context, volume):
        LOG.debug("volume: {}".format(volume))

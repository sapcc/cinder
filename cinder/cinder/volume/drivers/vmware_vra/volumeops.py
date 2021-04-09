import vra_facada

from oslo_log import log as logging
from cinder import objects

LOG = logging.getLogger(__name__)
LINKED_CLONE_TYPE = 'linked'
FULL_CLONE_TYPE = 'full'


class VraVolumeOps(object):

    def __init__(self):
        self.vra = vra_facada.VraFacada()
        self.vra.client.login()

    def create_volume(self, volume):
        # volume.volume_type
        # volume_type = objects.VolumeType.get_by_name_or_id(
        #     context, volume.volume_type.id)

        project = self.vra.project
        project_id = project.fetch(volume.project_id)

        vol = self.vra.volume
        vol.load(volume)
        vol.create(project_id)

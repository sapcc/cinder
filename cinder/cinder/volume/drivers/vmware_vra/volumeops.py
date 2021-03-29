from oslo_log import log as logging
import synchronization as sync
from VraRestClient import VraRestClient
from cinder import objects

LOG = logging.getLogger(__name__)
LINKED_CLONE_TYPE = 'linked'
FULL_CLONE_TYPE = 'full'


class VraVolumeOps(object):

    def __init__(self, vra_host, vra_username, vra_password):
        self.vra_host = vra_host
        self.vra_username = vra_username
        self.vra_password = vra_password

        self.api_scheduler = sync.Scheduler(rate=2,
                                            limit=1)
        self.vraClient = VraRestClient(self.api_scheduler, "https://" + self.vra_host,
                                       self.vra_username, self.vra_password, "System Domain")

    def create_volume(self, volume):
        bp_name = "Standalone Volume"
        blueprint = self.vraClient.getBlueprint(bp_name)
        print("Blueprint id: {}".format(blueprint["id"]))
        if not blueprint:
            raise ValueError("Blueprint id not found for name {}".format(bp_name))

        project_id = self.__get_project_id(volume)

        # volume.volume_type
        # volume_type = objects.VolumeType.get_by_name_or_id(
        #     context, volume.volume_type.id)

        volume_payload = {
            "capacityInGB": volume.size,
            "name": volume.display_name,
            "projectId": project_id,
            "description": "Volume create",
            "persistent": True,
            "tags": [
                {
                    "key": "openstack_volume_id",
                    "value": volume.id
                }
            ],
            "customProperties": {
                "openstack_volume_id": volume.id
            }
        }

        self.vraClient.iaas_create_volume(volume_payload)

    # TO DO - move into utils so we don't break SRP
    def __get_project_id(self, volume):
        vra_project = self.vraClient.fetchVraProjects()
        projId = None
        for proj in vra_project:
            if proj['customProperties']['openstackProjId'] == volume.project_id:
                projId = proj['id']

        if not projId:
            raise ValueError('Project id not found in vRA for id: {}'.format(
                volume.project_id))

        return projId

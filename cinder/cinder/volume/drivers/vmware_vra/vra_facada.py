import constants
import json
import vra_utils
from oslo_config import cfg
from oslo_log import log as logging
from vra_lib import client as vra_client

LOG = logging.getLogger(__name__)

RESOURCE_TRACKER_SLEEP = 5.0

class ResourceNotImplemented(Exception):
    pass


class Resource(object):

    def __init__(self, client):
        self.client = client
        self.openstack_id = None
        self.openstack_payload = None
        self.project = None
        self.id = None
        self.revision = None

    def load(self, payload):
        """
        Load Resource from OpenStack openstack_payload
        """
        raise ResourceNotImplemented()

    def fetch(self):
        """
        Load Resource from vRA
        """
        raise ResourceNotImplemented()

    def all(self):
        """
        Fetch a list of all resources
        """
        raise ResourceNotImplemented()

    def all_revisions(self):
        """
        Fetch a list of all resources revisions
        """
        raise ResourceNotImplemented()

    def save(self):
        """
        Create or update resource in vRA
        """
        raise ResourceNotImplemented()

    def delete(self):
        """
        Delete a resource from vRA
        """
        raise ResourceNotImplemented()

    def track(self, resource_track_id):
        tracker = vra_utils.track_status_waiter(self.client, resource_track_id,
                                                RESOURCE_TRACKER_SLEEP)
        if tracker['status'] == 'FAILED':
            LOG.error(tracker['message'])
            raise Exception(tracker['message'])

    def save_and_track(self, path, payload):
        response = self.client.post(
            path=path,
            json=payload
        )

        content = json.loads(response.content)
        self.track(content['id'])


class Project(Resource):
    """
    vRA Project class
    """
    def __init__(self, client):
        super(Project, self).__init__(client)


    def fetch(self, project_id):
        """
        Get project
        """
        vra_projects = self.all()
        projId = None
        for proj in vra_projects:
            if proj['customProperties']['openstackProjId'] == project_id:
                projId = proj['id']

        if not projId:
            raise ValueError('Project id not found in vRA for id: {}'.format(
                project_id))

        return projId

    def all(self):
        """
        Fetch all available vRA projects

        :return: HTTP Response content
        """
        LOG.info("Fetching vRA Projects...")
        r = self.client.get(
            path=constants.PROJECTS_GET_API
        )
        content = json.loads(r.content)
        LOG.debug('vRA Projects content: {}'.format(content))
        return content["content"]


class Volume(Resource):

    def __init__(self, client):
        super(Volume, self).__init__(client)

    def fetch(self):
        """
        Get volume
        """
        pass

    def load(self, volume_payload):
        self.volume = volume_payload
        print(self.volume)

    def create(self, project_id):
        """
        Create FCD disk in vRA

        :param project_id: vRA project id
        :return:
        """

        volume_payload = {
            "capacityInGB": self.volume.size,
            "name": self.volume.id,
            "projectId": project_id,
            "description": self.volume.display_description,
            "persistent": True,
            "tags": [
                {
                    "key": "openstack_volume_id",
                    "value": self.volume.id
                }
            ],
            "customProperties": {
                "openstack_volume_id": self.volume.id,
                "provisioningType": "thin"
            }
        }
        self.save_and_track(constants.CREATE_VOLUME_API, volume_payload)
        LOG.info('vRA Create volume initialized')


class VraFacada(object):

    def __init__(self):
        vra_config = vra_client.VraClientConfig()
        c = cfg.CONF.vsphere

        #TO-DO Maybe we can move this config init outside
        vra_config.host = c.host
        vra_config.port = c.port
        vra_config.username = c.username
        vra_config.password = c.password
        vra_config.organization = c.organization
        vra_config.connection_retries = c.connection_retries
        vra_config.connection_retries_seconds = c.connection_retries_seconds
        vra_config.connection_timeout_seconds = c.connection_timeout_seconds
        vra_config.connection_throttling_rate = c.connection_throttling_rate
        vra_config.connection_throttling_limit_seconds = c.connection_throttling_limit_seconds
        vra_config.connection_throttling_timeout_seconds = c.connection_throttling_timeout_seconds
        vra_config.connection_query_limit = c.connection_query_limit
        vra_config.connection_certificate_check = c.connection_certificate_check
        vra_config.cloud_zone = c.cloud_zone
        vra_config.logger = LOG

        self.client = vra_client.VraClient(vra_config)

    @property
    def volume(self):
        return Volume(self.client)

    @property
    def project(self):
        return Project(self.client)

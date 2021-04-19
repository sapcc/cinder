import constants
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
        """
        Create vRA volume
        :param volume: Openstack volume
        :return:
        """
        # volume.volume_type
        # volume_type = objects.VolumeType.get_by_name_or_id(
        #     context, volume.volume_type.id)

        project = self.vra.project
        project_id = project.fetch(volume.project_id)

        vol = self.vra.volume
        vol.load(volume)
        vol.create(project_id)

    def create_snapshot(self, snapshot):
        """
        Create volume snapshot
        :param snapshot: Snapshot info from Openstack
        :return:
        """

        LOG.info("Start creating volume snapshot with snapshot info: {}".
                 format(snapshot))

        vol = self.vra.volume
        vra_volume = vol.fetch(snapshot.volume_id)

        snapshot_obj = self.vra.snapshot
        snapshot_obj.load(snapshot)
        snapshot_obj.create(vra_volume['id'])

    def clone_volume(self, volume, src_vref):
        """
        Create vRA volume clone

        :param volume: New openstack volume being created
        :param src_vref: Source openstack volume to clone
        :return:
        """
        vol = self.vra.volume
        vol.load(volume)

        project = self.vra.project
        project_id = project.fetch(volume.project_id)

        catalog = self.vra.catalog_item
        catalog_item = catalog.fetch(constants.CATALOG_CREATE_VOLUME_CLONE)[0]
        vol.clone_volume(src_vref, project_id, catalog_item['id'])

    def create_volume_from_snapshot(self, volume, snapshot):
        """
        Create vRA volume from snapshot
        :param volume: Openstack volume
        :param snapshot: Openstack snapshot
        :return:
        """
        vol = self.vra.volume
        vol.load(volume)

        project = self.vra.project
        project_id = project.fetch(volume.project_id)

        catalog = self.vra.catalog_item
        catalog_item = catalog.fetch(constants.CATALOG_CREATE_VOLUME_FROM_SNAPSHOT)[0]

        snapshot_obj = self.vra.snapshot
        snapshot_obj.load(snapshot)

        vol.create_volume_from_snapshot(catalog_item['id'], snapshot.id, project_id, snapshot.volume_id)

    def delete_volume(self, volume):
        vol = self.vra.volume
        vol.load(volume)
        vol.delete()

    def delete_volume_snapshot(self, snapshot):
        vol = self.vra.volume
        vra_existing_volume = vol.fetch(snapshot.volume_id)

        snapshot_obj = self.vra.snapshot
        snapshot_obj.load(snapshot)

        vra_snapshots = snapshot_obj.all(vra_existing_volume['id'])

        vra_snapshot = self.__filter_snapshot(vra_snapshots, snapshot)
        snapshot_obj.delete(vra_existing_volume['id'], vra_snapshot['id'])

    def __filter_snapshot(self, vra_snapshots, snapshot):
        filtered_snapshot = None
        for vra_snapshot in vra_snapshots:
            if vra_snapshot['name'] == snapshot.id:
                filtered_snapshot = vra_snapshot

        if filtered_snapshot is None:
            raise Exception("vRA snapshot not found")

        return filtered_snapshot

    def delete_volume(self, volume):
        vol = self.vra.volume
        vol.load(volume)
        vol.delete()

    def extend_volume(self, volume, new_size):
        vol = self.vra.volume
        vol.load(volume)

        vol.extend(new_size)

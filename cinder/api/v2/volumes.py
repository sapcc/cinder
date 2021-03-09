# Copyright 2011 Justin Santa Barbara
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

"""The volumes api."""


from oslo_config import cfg
from oslo_log import log as logging
from oslo_log import versionutils
from oslo_utils import uuidutils
from six.moves import http_client
import webob
from webob import exc

from cinder.api import api_utils
from cinder.api import common
from cinder.api.contrib import scheduler_hints
from cinder.api import microversions as mv
from cinder.api.openstack import wsgi
from cinder.api.schemas import volumes
from cinder.api.v2.views import volumes as volume_views
from cinder.api import validation
from cinder import exception
from cinder import group as group_api
from cinder.i18n import _
from cinder.image import glance
from cinder import objects
from cinder.policies import volume_metadata as metadata_policy
from cinder import utils
from cinder import volume as cinder_volume
from cinder.volume import volume_utils

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class VolumeController(wsgi.Controller):
    """The Volumes API controller for the OpenStack API."""

    _view_builder_class = volume_views.ViewBuilder

    def __init__(self, ext_mgr):
        self.volume_api = cinder_volume.API()
        self.group_api = group_api.API()
        self.ext_mgr = ext_mgr
        super(VolumeController, self).__init__()

    def show(self, req, id):
        """Return data about the given volume."""
        context = req.environ['cinder.context']

        # Not found exception will be handled at the wsgi level
        vol = self.volume_api.get(context, id, viewable_admin_meta=True)
        req.cache_db_volume(vol)

        all_admin_metadata = context.authorize(
            metadata_policy.GET_ADMIN_METADATA_POLICY, fatal=False)

        api_utils.add_visible_admin_metadata(
            vol, all_admin_metadata=all_admin_metadata)

        return self._view_builder.detail(req, vol)

    def delete(self, req, id):
        """Delete a volume."""
        context = req.environ['cinder.context']

        cascade = utils.get_bool_param('cascade', req.params)

        LOG.info("Delete volume with id: %s", id)

        # Not found exception will be handled at the wsgi level
        volume = self.volume_api.get(context, id)
        self.volume_api.delete(context, volume, cascade=cascade)
        return webob.Response(status_int=http_client.ACCEPTED)

    def index(self, req):
        """Returns a summary list of volumes."""
        return self._get_volumes(req, is_detail=False)

    def detail(self, req):
        """Returns a detailed list of volumes."""
        return self._get_volumes(req, is_detail=True)

    def _get_volumes(self, req, is_detail):
        """Returns a list of volumes, transformed through view builder."""

        context = req.environ['cinder.context']

        params = req.params.copy()
        marker, limit, offset = common.get_pagination_params(params)
        sort_keys, sort_dirs = common.get_sort_params(params)
        filters = params

        # NOTE(wanghao): Always removing glance_metadata since we support it
        # only in API version >= VOLUME_LIST_GLANCE_METADATA.
        filters.pop('glance_metadata', None)
        api_utils.remove_invalid_filter_options(
            context,
            filters,
            self._get_volume_filter_options())

        # NOTE(thingee): v2 API allows name instead of display_name
        if 'name' in sort_keys:
            sort_keys[sort_keys.index('name')] = 'display_name'

        if 'name' in filters:
            filters['display_name'] = filters.pop('name')

        self.volume_api.check_volume_filters(filters)
        volumes = self.volume_api.get_all(context, marker, limit,
                                          sort_keys=sort_keys,
                                          sort_dirs=sort_dirs,
                                          filters=filters,
                                          viewable_admin_meta=True,
                                          offset=offset)

        all_admin_metadata = context.authorize(
            metadata_policy.GET_ADMIN_METADATA_POLICY, fatal=False)

        for volume in volumes:
            api_utils.add_visible_admin_metadata(
                volume, all_admin_metadata=all_admin_metadata)

        req.cache_db_volumes(volumes.objects)

        if is_detail:
            volumes = self._view_builder.detail_list(req, volumes)
        else:
            volumes = self._view_builder.summary_list(req, volumes)
        return volumes

    def _image_uuid_from_ref(self, image_ref, context):
        # If the image ref was generated by nova api, strip image_ref
        # down to an id.
        image_uuid = None
        try:
            image_uuid = image_ref.split('/').pop()
        except AttributeError:
            msg = _("Invalid imageRef provided.")
            raise exc.HTTPBadRequest(explanation=msg)

        image_service = glance.get_default_image_service()

        # First see if this is an actual image ID
        if uuidutils.is_uuid_like(image_uuid):
            try:
                image = image_service.show(context, image_uuid)
                if 'id' in image:
                    return image['id']
            except Exception:
                # Pass and see if there is a matching image name
                pass

        # Could not find by ID, check if it is an image name
        try:
            params = {'filters': {'name': image_ref}}
            images = list(image_service.detail(context, **params))
            if len(images) > 1:
                msg = _("Multiple matches found for '%s', use an ID to be more"
                        " specific.") % image_ref
                raise exc.HTTPConflict(explanation=msg)
            for img in images:
                return img['id']
        except exc.HTTPConflict:
            raise
        except Exception:
            # Pass the other exception and let default not found error
            # handling take care of it
            pass

        msg = _("Invalid image identifier or unable to "
                "access requested image.")
        raise exc.HTTPBadRequest(explanation=msg)

    @wsgi.response(http_client.ACCEPTED)
    @validation.schema(volumes.create, mv.V2_BASE_VERSION)
    def create(self, req, body):
        """Creates a new volume."""

        LOG.debug('Create volume request body: %s', body)
        context = req.environ['cinder.context']

        # NOTE (pooja_jadhav) To fix bug 1774155, scheduler hints is not
        # loaded as a standard extension. If user passes
        # OS-SCH-HNT:scheduler_hints in the request body, then it will be
        # validated in the create method and this method will add
        # scheduler_hints in body['volume'].
        body = scheduler_hints.create(req, body)
        volume = body['volume']

        kwargs = {}
        self.validate_name_and_description(volume, check_length=False)

        # NOTE(thingee): v2 API allows name instead of display_name
        if 'name' in volume:
            volume['display_name'] = volume.pop('name')

        # NOTE(thingee): v2 API allows description instead of
        #                display_description
        if 'description' in volume:
            volume['display_description'] = volume.pop('description')

        if 'image_id' in volume:
            volume['imageRef'] = volume.pop('image_id')

        req_volume_type = volume.get('volume_type', None)
        if req_volume_type:
            # Not found exception will be handled at the wsgi level
            kwargs['volume_type'] = (
                objects.VolumeType.get_by_name_or_id(context, req_volume_type))

        kwargs['metadata'] = volume.get('metadata', None)

        snapshot_id = volume.get('snapshot_id')
        if snapshot_id is not None:
            # Not found exception will be handled at the wsgi level
            kwargs['snapshot'] = self.volume_api.get_snapshot(context,
                                                              snapshot_id)
        else:
            kwargs['snapshot'] = None

        source_volid = volume.get('source_volid')
        if source_volid is not None:
            # Not found exception will be handled at the wsgi level
            kwargs['source_volume'] = \
                self.volume_api.get_volume(context,
                                           source_volid)
        else:
            kwargs['source_volume'] = None

        kwargs['group'] = None
        kwargs['consistencygroup'] = None
        consistencygroup_id = volume.get('consistencygroup_id')
        if consistencygroup_id is not None:
            # Not found exception will be handled at the wsgi level
            kwargs['group'] = self.group_api.get(context, consistencygroup_id)

        size = volume.get('size', None)
        if size is None and kwargs['snapshot'] is not None:
            size = kwargs['snapshot']['volume_size']
        elif size is None and kwargs['source_volume'] is not None:
            size = kwargs['source_volume']['size']

        LOG.info("Create volume of %s GB", size)

        image_ref = volume.get('imageRef')
        if image_ref is not None:
            image_uuid = self._image_uuid_from_ref(image_ref, context)
            kwargs['image_id'] = image_uuid

        kwargs['availability_zone'] = volume.get('availability_zone', None)
        kwargs['scheduler_hints'] = volume.get('scheduler_hints', None)
        kwargs['multiattach'] = utils.get_bool_param('multiattach', volume)

        if kwargs.get('multiattach', False):
            msg = ("The option 'multiattach' "
                   "is deprecated and will be removed in a future "
                   "release.  The default behavior going forward will "
                   "be to specify multiattach enabled volume types.")
            versionutils.report_deprecated_feature(LOG, msg)

        try:
            new_volume = self.volume_api.create(
                context, size, volume.get('display_name'),
                volume.get('display_description'), **kwargs)
        except exception.VolumeTypeDefaultMisconfiguredError as err:
            raise webob.exc.HTTPInternalServerError(explanation=err.msg)

        retval = self._view_builder.detail(req, new_volume)

        return retval

    def _get_volume_filter_options(self):
        """Return volume search options allowed by non-admin."""
        return common.get_enabled_resource_filters('volume')['volume']

    @validation.schema(volumes.update, mv.V2_BASE_VERSION,
                       mv.get_prior_version(mv.SUPPORT_VOLUME_SCHEMA_CHANGES))
    @validation.schema(volumes.update_volume_v353,
                       mv.SUPPORT_VOLUME_SCHEMA_CHANGES)
    def update(self, req, id, body):
        """Update a volume."""
        context = req.environ['cinder.context']
        update_dict = body['volume']

        self.validate_name_and_description(update_dict, check_length=False)

        # NOTE(thingee): v2 API allows name instead of display_name
        if 'name' in update_dict:
            update_dict['display_name'] = update_dict.pop('name')

        # NOTE(thingee): v2 API allows description instead of
        #                display_description
        if 'description' in update_dict:
            update_dict['display_description'] = update_dict.pop('description')

        # Not found and Invalid exceptions will be handled at the wsgi level
        try:
            volume = self.volume_api.get(context, id, viewable_admin_meta=True)
            volume_utils.notify_about_volume_usage(context, volume,
                                                   'update.start')
            self.volume_api.update(context, volume, update_dict)
        except exception.InvalidVolumeMetadataSize as error:
            raise webob.exc.HTTPRequestEntityTooLarge(explanation=error.msg)

        volume.update(update_dict)

        all_admin_metadata = context.authorize(
            metadata_policy.GET_ADMIN_METADATA_POLICY, fatal=False)

        api_utils.add_visible_admin_metadata(
            volume, all_admin_metadata=all_admin_metadata)

        volume_utils.notify_about_volume_usage(context, volume,
                                               'update.end')

        return self._view_builder.detail(req, volume)


def create_resource(ext_mgr):
    return wsgi.Resource(VolumeController(ext_mgr))

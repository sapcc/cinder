# Copyright (c) 2020 SAP SE
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

"""
RPC server and client for communicating with other netapp drivers directly.
This allows remote calls to be made to a running netapp driver to do work on
volumes.
"""
from oslo_log import log as logging
import oslo_messaging as messaging

from cinder import rpc
from cinder.volume.rpcapi import VolumeAPI
from cinder.volume import volume_utils


LOG = logging.getLogger(__name__)


class SAPNetappDriverRemoteApi(rpc.RPCAPI):
    RPC_API_VERSION = VolumeAPI.RPC_API_VERSION
    RPC_DEFAULT_VERSION = RPC_API_VERSION
    TOPIC = VolumeAPI.TOPIC
    BINARY = VolumeAPI.BINARY

    def _get_cctxt(self, version=None, host=None, **kwargs):
        kwargs['server'] = volume_utils.extract_host(host)
        return super(SAPNetappDriverRemoteApi, self)._get_cctxt(
            version=version, **kwargs)

    @volume_utils.trace
    def swap_files(self, ctxt, host, vol_name, original_file, new_file):
        cctxt = self._get_cctxt(host=host)
        return cctxt.call(ctxt, 'swap_files', vol_name=vol_name,
                          original_file=original_file, new_file=new_file)

    @volume_utils.trace
    def get_file_sizes_by_dir(self, ctxt, host, path):
        cctxt = self._get_cctxt(host=host)
        return cctxt.call(ctxt, 'get_file_sizes_by_dir', path=path)

    @volume_utils.trace
    def get_vserver_for_ip(self, ctxt, host, lif_ip):
        cctxt = self._get_cctxt(host=host)
        return cctxt.call(ctxt, 'get_vserver_for_ip', lif_ip=lif_ip)

    @volume_utils.trace
    def clone_file(self, ctxt, host, flex_vol, src_path,
                   dest_path, vserver, dest_exists=False, is_snapshot=False):
        cctxt = self._get_cctxt(host=host)
        return cctxt.call(ctxt, 'clone_file', flex_vol=flex_vol,
                          src_path=src_path, dest_path=dest_path,
                          vserver=vserver, dest_exists=dest_exists,
                          is_snapshot=is_snapshot)

    def file_assign_qos(self, ctxt, host, vol_name,
                        qos_policy_group_name, path):
        cctxt = self._get_cctxt(host=host)
        return cctxt.call(ctxt, 'file_assign_qos', vol_name=vol_name,
                          qos_policy_group_name=qos_policy_group_name,
                          path=path)

    @volume_utils.trace
    def rename_file_or_dir(self, ctxt, host, old_name, new_name):
        cctxt = self._get_cctxt(host=host)
        return cctxt.call(ctxt, 'rename_file_or_dir',
                          old_name=old_name, new_name=new_name)


class SAPNetappDriverRemoteService(object):
    RPC_API_VERSION = SAPNetappDriverRemoteApi.RPC_API_VERSION

    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, driver):
        self._driver = driver

    def swap_files(self, ctxt, vol_name, original_file, new_file):
        # swaps 2 files using temp_file
        return self._driver._swap_files(flexvol_name=vol_name,
                                        original_file=original_file,
                                        new_file=new_file)

    def get_file_sizes_by_dir(self, ctxt, path):
        # Returns used bytes of a file
        return self._driver.zapi_client.get_file_sizes_by_dir(path)

    def get_vserver_for_ip(self, ctxt, lif_ip):
        # Returns vserver name from LIF ip
        return self._driver._get_vserver_for_ip(lif_ip)

    def clone_file(self, ctxt, flex_vol, src_path, dest_path, vserver,
                   dest_exists, is_snapshot):
        # Clones a file on ONTAP
        self._driver.zapi_client.clone_file(flex_vol=flex_vol,
                                            src_path=src_path,
                                            dest_path=dest_path,
                                            vserver=vserver,
                                            dest_exists=dest_exists,
                                            is_snapshot=is_snapshot)

    def file_assign_qos(self, ctxt, vol_name,
                        qos_policy_group_name, path):
        # Sets QOS policy on a file
        set_qos = self._driver.zapi_client.file_assign_qos
        return set_qos(flex_vol=vol_name,
                       qos_policy_group_name=qos_policy_group_name,
                       qos_policy_group_is_adaptive=True,
                       file_path=path)

    def rename_file_or_dir(self, ctxt, old_name, new_name):
        # Renames a file or directory on ONTAP via ZAPI file-rename-file
        return self._driver.zapi_client.rename_file(old_name, new_name)

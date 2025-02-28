# Copyright (C) 2025 SAP SE
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
from oslo_log import log as logging
import requests

from cinder import utils

LOG = logging.getLogger(__name__)


class KMIPRestApiClient:

    def __init__(self, kmip_url, barbican_url):
        self._kmip_url = kmip_url
        self._barbican_url = barbican_url

    @utils.retry(requests.RequestException)
    def kmip_register(self, key_uuid: str,
                      owner: str, policy: str) -> str:
        """Registers a new KMIP object in the managed_objects.

        :returns: the ID of the object created in KMIP
        """
        key_url = f"{self._barbican_url}/v1/secrets/{key_uuid}"
        payload = {
            "url": key_url,
            "owner": owner,
            "policy": policy
        }
        LOG.debug("Registering barbican key to KMIP. Request=%s",
                  payload)
        response = requests.post(
            f"{self._kmip_url}/kmip/kmip_register",
            json=payload)
        data = response.json()
        LOG.debug("Registered barbican key to KMIP. Result=%s",
                  data)
        return str(data.get("uid"))

    @utils.retry(requests.RequestException)
    def get_kmip_id(self, barbican_uuid: str) -> str:
        """Retrieves the KMIP ID of a Barbican key

        :returns: the ID of the key in KMIP
        """
        LOG.debug("Retrieving KMIP id for barbican_uuid=%s",
                  barbican_uuid)
        response = requests.get(
            f"{self._kmip_url}/kmip/get_kmip_id_from_barbican"
            f"?barbican_id={barbican_uuid}")
        data = response.json()
        return str(data.get("kmip_id"))

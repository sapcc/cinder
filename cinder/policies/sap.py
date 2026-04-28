# Copyright (c) 2025 SAP Corporation
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

from oslo_policy import policy

from cinder.policies import base


SET_POOL_STATE_POLICY = "sap:set_pool_state"
SET_AGGREGATE_ID_POLICY = "sap:set_aggregate_id"
GET_AGGREGATE_ID_POLICY = "sap:get_aggregate_id"
RECOUNT_STATS_POLICY = "sap:recount_host_stats"

sap_policies = [
    policy.DocumentedRuleDefault(
        name=RECOUNT_STATS_POLICY,
        check_str=base.RULE_ADMIN_API,
        description="Recount host stats allocated capacity",
        operations=[
            {
                'method': 'PUT',
                'path': '/os-sap-contrib/recount_host_stats'
            }
        ]),
    policy.DocumentedRuleDefault(
        name=SET_POOL_STATE_POLICY,
        check_str=base.RULE_ADMIN_API,
        description="Set the pool state for the host",
        operations=[
            {
                'method': 'PUT',
                'path': '/os-sap-contrib/set_pool_state'
            }
        ]),
    policy.DocumentedRuleDefault(
        name=SET_AGGREGATE_ID_POLICY,
        check_str=base.RULE_ADMIN_API,
        description="Set or remove the aggregate_id for a pool",
        operations=[
            {
                'method': 'PUT',
                'path': '/os-sap-contrib/set_aggregate_id'
            }
        ]),
    policy.DocumentedRuleDefault(
        name=GET_AGGREGATE_ID_POLICY,
        check_str=base.RULE_ADMIN_API,
        description="Get the aggregate_id for a pool",
        operations=[
            {
                'method': 'PUT',
                'path': '/os-sap-contrib/get_aggregate_id'
            }
        ]),
]


def list_rules():
    return sap_policies

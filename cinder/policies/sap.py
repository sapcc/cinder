from oslo_policy import policy

from cinder.policies import base


SET_POOL_STATE_POLICY = "sap:set_pool_state"
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
]


def list_rules():
    return sap_policies

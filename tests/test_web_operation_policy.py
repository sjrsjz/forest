from __future__ import annotations

import pytest

from forest.errors import ForestError
from forest.web_operation_policy import (
    WEB_OPERATION_POLICY_DEFAULT,
    authorize_web_operation,
    validate_web_operation_policy,
)


def _rule(**changes):
    return {
        "target_component_id": "agent.runtime",
        "action_key": "agent.run.cancel",
        "enabled": True,
        "allowed_roles": ["ADMIN"],
        "allowed_subject_refs": None,
        **changes,
    }


def test_default_policy_preserves_role_gate_and_exact_rule_can_narrow_it():
    authorize_web_operation(
        WEB_OPERATION_POLICY_DEFAULT,
        web_subject_ref="web-subject/admin",
        web_role="ADMIN",
        target_component_id="agent.runtime",
        action_key="agent.run.cancel",
    )
    policy = {
        "protocol": "forest.web-operation-policy/1",
        "rules": [_rule(allowed_subject_refs=["web-subject/operator"])],
    }
    authorize_web_operation(
        policy,
        web_subject_ref="web-subject/operator",
        web_role="ADMIN",
        target_component_id="agent.runtime",
        action_key="agent.run.cancel",
    )
    with pytest.raises(ForestError) as denied:
        authorize_web_operation(
            policy,
            web_subject_ref="web-subject/admin",
            web_role="ADMIN",
            target_component_id="agent.runtime",
            action_key="agent.run.cancel",
        )
    assert denied.value.code == "WEB_OPERATION_POLICY_DENIED"


def test_policy_can_disable_exact_operation_without_affecting_other_actions():
    policy = {
        "protocol": "forest.web-operation-policy/1",
        "rules": [_rule(enabled=False)],
    }
    with pytest.raises(ForestError) as denied:
        authorize_web_operation(
            policy,
            web_subject_ref="web-subject/admin",
            web_role="ADMIN",
            target_component_id="agent.runtime",
            action_key="agent.run.cancel",
        )
    assert denied.value.code == "WEB_OPERATION_POLICY_DENIED"
    authorize_web_operation(
        policy,
        web_subject_ref="web-subject/admin",
        web_role="ADMIN",
        target_component_id="agent.runtime",
        action_key="configuration.read",
    )


@pytest.mark.parametrize(
    "rules",
    [
        [_rule(), _rule()],
        [_rule(allowed_roles=["ADMIN", "ADMIN"])],
        [_rule(allowed_subject_refs=[])],
        [_rule(target_component_id="")],
    ],
)
def test_policy_rejects_ambiguous_or_unbounded_rules(rules):
    with pytest.raises(ValueError):
        validate_web_operation_policy(
            {"protocol": "forest.web-operation-policy/1", "rules": rules}
        )

import json

import pytest

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.errors import ForestError
from forest.management.agent_api import AgentManagementContract
from forest.management.deployment_accounts_contract import (
    DeploymentAccountsManagementContract,
)
from forest.management.memory_api import MemoryManagementContract
from forest.management.module_api import (
    EmptyInput,
    ModuleManagementContract,
    ModuleOperation,
    management_scope_provider_registry,
)


async def noop(request, body):
    return None


def test_contract_rejects_duplicate_operation_identities():
    operation = ModuleOperation("same", "query", "config.read", EmptyInput, noop)
    with pytest.raises(ValueError, match="unique"):
        ModuleManagementContract((operation, operation))


def test_scope_provider_registry_rejects_duplicate_namespaces():
    class Provider:
        kind = "duplicate"

    with pytest.raises(ValueError, match="identities must be unique"):
        management_scope_provider_registry((Provider(), Provider()))


def test_agent_secret_is_projected_before_authority_and_never_serialized():
    contract = AgentManagementContract()
    operation, body, payload = contract.prepare(
        "configuration.plan",
        "action",
        {
            "expected_revision": 0,
            "values": {**DEFAULT_AGENT_SETTINGS, "model": "contract-model"},
            "api_key": "private-contract-key",
        },
    )
    assert operation.authority == "agent.config.plan"
    assert body.api_key == "private-contract-key"
    encoded = json.dumps(payload)
    assert "private-contract-key" not in encoded
    assert payload["credentials_changed"] is True
    assert payload["secret_digest"].startswith("sha256:")


def test_account_contract_projects_passwords_out_of_management_payload():
    contract = DeploymentAccountsManagementContract(object())
    operation, _, payload = contract.prepare(
        "account.create", "action",
        {"username": "viewer", "password": "new-password-value",
         "role": "viewer", "current_password": "admin-password-value"},
    )
    assert operation.principal == "admin"
    assert payload == {
        "username": "viewer", "role": "viewer", "credentials_changed": True
    }
    assert "password" not in json.dumps(payload)

    operation, _, payload = contract.prepare(
        "account.change.apply", "action",
        {"account_id": "account-1", "expected_revision": 1, "plan_id": "plan-1",
         "plan_digest": "digest-1", "current_password": "admin-password-value"},
    )
    assert operation.principal == "authenticated"
    assert "current_password" not in payload


def test_query_contract_normalizes_transport_strings_before_signing():
    contract = MemoryManagementContract()
    operation, body, payload = contract.prepare(
        "sources", "query", {"limit": "50", "group_source_id": "group:42"}
    )
    assert operation.authority == "memory.forget.read"
    assert body.limit == 50
    assert payload == {"limit": 50, "group_source_id": "group:42"}

    operation, body, payload = contract.prepare(
        "sources",
        "query",
        {
            "limit": "25", "kind": "INPUT", "has_image": "true",
            "occurred_from": "2026-09-01T00:00:00Z",
        },
    )
    assert operation.authority == "memory.forget.read"
    assert body.has_image is True
    assert payload == {
        "limit": 25, "kind": "INPUT", "has_image": True,
        "occurred_from": "2026-09-01T00:00:00Z",
    }
    operation, _, payload = contract.prepare(
        "trunks", "query", {"limit": "10", "channel_id": "933179609"}
    )
    assert operation.authority == "memory.forget.read"
    assert payload == {"limit": 10, "channel_id": "933179609"}


def test_contract_rejects_unknown_kind_and_returns_serializable_validation_details():
    contract = MemoryManagementContract()
    with pytest.raises(ForestError) as missing:
        contract.prepare("sources", "action", {})
    assert missing.value.code == "MODULE_MANAGEMENT_OPERATION_NOT_FOUND"
    with pytest.raises(ForestError) as invalid:
        contract.prepare("sources", "query", {"limit": "invalid"})
    assert invalid.value.code == "MODULE_MANAGEMENT_INPUT_INVALID"
    json.dumps(invalid.value.details)

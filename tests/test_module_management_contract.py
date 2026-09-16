import json

import pytest

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.errors import ForestError
from forest.management.agent_api import AgentManagementContract
from forest.management.memory_api import MemoryManagementContract
from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation


async def noop(request, body):
    return None


def test_contract_rejects_duplicate_operation_identities():
    operation = ModuleOperation("same", "query", "config.read", EmptyInput, noop)
    with pytest.raises(ValueError, match="unique"):
        ModuleManagementContract((operation, operation))


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


def test_query_contract_normalizes_transport_strings_before_signing():
    contract = MemoryManagementContract()
    operation, body, payload = contract.prepare(
        "sources", "query", {"limit": "50", "group_source_id": "group:42"}
    )
    assert operation.authority == "memory.forget.read"
    assert body.limit == 50
    assert payload == {"limit": 50, "group_source_id": "group:42"}


def test_contract_rejects_unknown_kind_and_returns_serializable_validation_details():
    contract = MemoryManagementContract()
    with pytest.raises(ForestError) as missing:
        contract.prepare("sources", "action", {})
    assert missing.value.code == "MODULE_MANAGEMENT_OPERATION_NOT_FOUND"
    with pytest.raises(ForestError) as invalid:
        contract.prepare("sources", "query", {"limit": "invalid"})
    assert invalid.value.code == "MODULE_MANAGEMENT_INPUT_INVALID"
    json.dumps(invalid.value.details)

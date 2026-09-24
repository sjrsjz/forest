from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.package_contribution_directories import FrozenContributionDirectories
from forest.packages import digest_json
from forest.runtime_modules import RuntimeAgentToolContribution


@pytest.mark.asyncio
async def test_agent_tool_dispatch_requires_the_handed_off_effect_and_frozen_route():
    contribution = RuntimeAgentToolContribution(
        name="inspect",
        service_id="module.inspect",
        version=1,
        declaration={
            "name": "inspect",
            "description": "Inspect one package-owned object.",
            "parameters": {
                "type": "object",
                "properties": {"object_id": {"type": "string"}},
                "required": ["object_id"],
                "additionalProperties": False,
            },
            "strict": True,
        },
        approval_required=False,
        supports_query=False,
    )
    command = {
        "protocol": "forest.agent-tool-execute/1",
        "tool_call_id": "tool:1",
        "effect_operation_id": "effect:1",
        "effect_attempt_id": "attempt:1",
        "component_id": "module.owner",
        "generation_id": "generation:owner:1",
        "service_id": "module.inspect",
        "contract_digest": contribution.contract_digest,
        "binding_digest": "",
        "authority_trigger_ref": "trigger:1",
        "authority_principal_ref": "principal:1",
        "arguments": {"object_id": "object:1"},
    }
    binding = {
        "name": "inspect",
        "component_id": "module.owner",
        "generation_id": "generation:owner:1",
        "service_id": "module.inspect",
        "contract_digest": contribution.contract_digest,
        "approval_required": False,
        "supports_query": False,
    }
    command["binding_digest"] = digest_json(binding)
    wire_digest = digest_json(command)
    invoked = []

    class Service:
        route = SimpleNamespace(
            provider_generation_id="generation:owner:1",
            service_id="module.inspect",
            version=1,
        )

        async def invoke(self, payload, **values):
            invoked.append((payload, values))
            return {"status": "SUCCEEDED"}

    class Effects:
        async def load(self, effect_operation_id):
            assert effect_operation_id == "effect:1"
            return {
                "effect": {"state": "DISPATCHING"},
                "attempt": {
                    "state": "DISPATCHING",
                    "attempt_id": "attempt:1",
                    "executor_component_id": "module.owner",
                    "executor_generation_id": "generation:owner:1",
                    "wire_payload_digest": wire_digest,
                },
            }

    directory = FrozenContributionDirectories(
        declarations=({"kind": "agent-tools", "max_execution_timeout_s": 30},),
        routes={"agent-tools": (("module.owner", contribution, Service()),)},
        domain_queries=None,
        domain_query_declarations=(),
        agent_tool_effects=Effects(),
    )
    request = {
        "kind": "agent-tools",
        "action": "dispatch-prepared",
        "tool_name": "inspect",
        "target_component_id": "module.owner",
        "target_generation_id": "generation:owner:1",
        "target_service_id": "module.inspect",
        "target_service_version": 1,
        "contract_digest": contribution.contract_digest,
        "binding_digest": command["binding_digest"],
        "effect_operation_id": "effect:1",
        "attempt_id": "attempt:1",
        "permit": {
            "protocol": "forest.effect-permit/1",
            "effect_operation_id": "effect:1",
            "attempt_id": "attempt:1",
            "executor_component_id": "module.owner",
            "executor_generation_id": "generation:owner:1",
            "wire_payload_digest": wire_digest,
        },
        "command": command,
        "operation_id": "effect:1",
        "timeout": 10,
    }
    scope = PackageCallScope(
        "bot:1",
        "agent.runtime",
        "generation:agent:1",
        "instance:1",
        "worker:1",
        "call:1",
        "operation:1",
        "conversation",
        "conversation:1",
    )
    result = await directory.invoke(
        request,
        graph_identity=SimpleNamespace(graph_id="graph:1", graph_revision=1),
        scope=scope,
        parent_call_id="call:1",
        remaining_timeout=lambda value: value,
    )
    assert result == {"status": "SUCCEEDED"}
    assert invoked[0][0] == command

    request["target_generation_id"] = "generation:owner:2"
    with pytest.raises(ForestError) as caught:
        await directory.invoke(
            request,
            graph_identity=SimpleNamespace(graph_id="graph:1", graph_revision=1),
            scope=scope,
            parent_call_id="call:1",
            remaining_timeout=lambda value: value,
        )
    assert caught.value.code == "AGENT_TOOL_ROUTE_STALE"

from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.package_contribution_directories import FrozenContributionDirectories
from forest.shell_command_protocol import ShellCommandExecutionRequest
from forest.shell_management_authority import ShellManagementAuthorization


@pytest.mark.asyncio
async def test_shell_directory_execution_uses_its_declared_consumer_identity():
    directory = FrozenContributionDirectories(
        consumer_component_id="module.shell-consumer",
        declarations=({"kind": "shell-commands", "max_execution_timeout_s": 30},),
        routes={"shell-commands": ()},
        domain_queries=None,
        domain_query_declarations=(),
    )

    def scope(component_id):
        return PackageCallScope(
            "bot/test", component_id, "generation/shell", "instance/shell",
            "worker/shell", "call/shell", "operation/shell", "shell.request", "request/shell",
        )

    async def execute(component_id):
        return await directory.invoke(
            {"kind": "shell-commands", "action": "execute"},
            graph_identity=None,
            scope=scope(component_id),
            parent_call_id="call/shell",
            remaining_timeout=lambda value: value,
        )

    with pytest.raises(ForestError) as accepted:
        await execute("module.shell-consumer")
    assert accepted.value.code == "CONTRIBUTION_DIRECTORY_REQUEST_INVALID"
    with pytest.raises(ForestError) as forbidden:
        await execute("shell.command")
    assert forbidden.value.code == "SHELL_EXECUTION_AUTHORITY_INVALID"


@pytest.mark.asyncio
async def test_shell_management_uses_sealed_participant_generation():
    calls = []

    class Participant:
        component_id = "module.shell-consumer"
        generation_id = "generation/shell"

        async def invoke_in(self, conn, service_id, payload, **kwargs):
            calls.append((conn, service_id, payload, kwargs))
            return {
                "protocol": "forest.shell-management-claim-verified/1",
                "request_id": "request/shell",
                "principal_ref": "qq:42",
                "ticket_id": "ticket/shell",
            }

    participant = Participant()
    instance = SimpleNamespace(unit_of_work_participant=lambda: participant)
    registry = SimpleNamespace(
        graph_identity=SimpleNamespace(
            generations={"module.shell-consumer": "generation/shell"}
        ),
        instances={"module.shell-consumer": instance},
        directory_consumers={"shell-commands": "module.shell-consumer"},
    )
    registry.contract_router = SimpleNamespace(registry=registry)

    class Runtime:
        async def verify_active_generation_in(self, _conn, **kwargs):
            calls.append(("generation", kwargs))

    class Audit:
        async def append_in(self, _conn, envelope):
            calls.append(("audit", envelope))

    host = SimpleNamespace(
        registry=registry,
        _kernel=SimpleNamespace(runtime=Runtime(), audit=Audit()),
        store=SimpleNamespace(bot_id="bot/test"),
    )
    request = ShellCommandExecutionRequest(
        command_id="module.reload", command_version=1, arguments={},
        request_id="request/shell", principal_ref="qq:42",
        conversation_ref="onebot:group:933179609",
        claim_token="claim/shell", shell_generation_id="generation/shell",
    )
    authorization = ShellManagementAuthorization(
        host=host, request=request,
        component_id="component.worker-host", generation_id="generation/target",
        service_id="runtime.graph-replacement.request", service_version=2,
        action_id="module.reload", operation_id="operation/shell",
        input_digest="sha256:" + "a" * 64, plan_digest=None,
    )
    await authorization.verify_in(
        object(), ticket=authorization,
        component_id=authorization.component_id,
        operation_id=authorization.operation_id,
        action_key=authorization.action_id,
        input_digest=authorization.input_digest,
        plan_digest=None,
    )
    assert calls[0][1] == "shell.verify-management-claim"
    assert calls[0][2]["target_generation_id"] == "generation/target"
    assert calls[1] == (
        "generation", {
            "component_id": "component.worker-host",
            "generation_id": "generation/target",
            "admission_lane": "CONTROL",
        },
    )
    assert calls[2][0] == "audit"

    participant.generation_id = "generation/stale"
    calls.clear()
    with pytest.raises(ForestError) as rejected:
        await authorization.verify_in(
            object(), ticket=authorization,
            component_id=authorization.component_id,
            operation_id=authorization.operation_id,
            action_key=authorization.action_id,
            input_digest=authorization.input_digest,
            plan_digest=None,
        )
    assert rejected.value.code == "SHELL_MANAGEMENT_AUTHORITY_UNAVAILABLE"
    assert calls == []

from __future__ import annotations

from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.runtime_modules import (
    ModuleLifecycleState,
    OwnedWorkLifecycle,
    RuntimeAgentToolContribution,
    RuntimeArtifactIdentity,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
    RuntimeShellCommandContribution,
)

_ARTIFACT = RuntimeArtifactIdentity(
    "test.tools",
    "1.0.0",
    "sha256:" + "a" * 64,
    "bot",
    "test.in-process/1",
    "installed",
)


def _tool(name: str, service_id: str) -> RuntimeAgentToolContribution:
    return RuntimeAgentToolContribution(
        name,
        service_id,
        1,
        {
            "name": name,
            "description": f"Invoke {name}",
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "strict": True,
        },
    )


def _runtime():
    return SimpleNamespace(
        scope_kind="bot",
        generation_authority=None,
        bot=SimpleNamespace(store=None),
    )


def test_manifest_service_dependencies_define_startup_order_without_late_bound_cycle():
    provider = RuntimeModuleDescriptor(
        "module.provider",
        (),
        lambda *_: object(),
        _ARTIFACT,
        provided_services=(("demo.request", "1.0"),),
    )
    consumer = RuntimeModuleDescriptor(
        "module.consumer",
        (),
        lambda *_: object(),
        _ARTIFACT,
        provided_services=(("demo.reply", "1.0"),),
        startup_service_requires=(("demo.request", ">=1.0,<2.0"),),
    )
    registry = RuntimeModuleRegistry(_runtime(), (consumer, provider))
    assert registry.order == ["module.provider", "module.consumer"]

    with pytest.raises(ForestError, match="Startup service has no provider"):
        RuntimeModuleRegistry(_runtime(), (consumer,))


def test_replacement_closure_includes_service_and_transaction_references():
    provider = RuntimeModuleDescriptor(
        "module.provider",
        (),
        lambda *_: object(),
        _ARTIFACT,
        provided_services=(("demo.request", "1.0"),),
    )
    consumer = RuntimeModuleDescriptor(
        "module.consumer",
        (),
        lambda *_: object(),
        _ARTIFACT,
        retention_service_requires=(("demo.request", ">=1.0,<2.0"),),
        retention_requires=("module.transaction",),
    )
    transaction = RuntimeModuleDescriptor(
        "module.transaction",
        (),
        lambda *_: object(),
        _ARTIFACT,
        retention_requires=("module.consumer",),
    )
    independent = RuntimeModuleDescriptor(
        "module.independent", (), lambda *_: object(), _ARTIFACT
    )
    registry = RuntimeModuleRegistry(
        _runtime(), (independent, transaction, consumer, provider)
    )
    assert set(registry.replacement_closure("module.provider")) == {
        "module.provider",
        "module.consumer",
        "module.transaction",
    }
    assert registry.replacement_closure("module.independent") == (
        "module.independent",
    )
    with pytest.raises(ForestError) as error:
        registry.replacement_closure("module.missing")
    assert error.value.code == "RUNTIME_MODULE_NOT_FOUND"


def _command(service_id: str) -> RuntimeShellCommandContribution:
    return RuntimeShellCommandContribution(
        "module.inspect",
        1,
        service_id,
        1,
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "protocol": {"const": "forest.shell.arguments/1"},
                "command_id": {"const": "module.inspect"},
                "arguments": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
            "required": ["protocol", "command_id", "arguments"],
            "additionalProperties": False,
        },
        "module.inspect",
        "BOT",
        "P1",
        ("runtime.status.read",),
    )


def test_agent_tool_contribution_freezes_contract_identity():
    tool = _tool("inspect", "module.inspect")

    assert tool.as_fact() == {
        "protocol": "forest.agent-tool-contribution/1",
        "name": "inspect",
        "service_id": "module.inspect",
        "version": 1,
        "declaration": tool.declaration,
        "contract_digest": tool.contract_digest,
        "approval_required": True,
        "supports_query": False,
    }
    assert tool.contract_digest.startswith("sha256:")


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (_tool("inspect", "module.inspect-a"), _tool("inspect", "module.inspect-b")),
        (_tool("inspect_a", "module.inspect"), _tool("inspect_b", "module.inspect")),
    ],
)
def test_runtime_graph_rejects_ambiguous_agent_tool_directory(left, right):
    descriptors = (
        RuntimeModuleDescriptor(
            "module.a",
            (),
            lambda *_: object(),
            _ARTIFACT,
            agent_tools=(left,),
        ),
        RuntimeModuleDescriptor(
            "module.b",
            (),
            lambda *_: object(),
            _ARTIFACT,
            agent_tools=(right,),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_AGENT_TOOL_CONFLICT"


def test_agent_tool_declaration_must_match_its_directory_name():
    with pytest.raises(ValueError, match="Agent tool contribution"):
        RuntimeAgentToolContribution(
            "inspect",
            "module.inspect",
            1,
            {
                "name": "different",
                "description": "Mismatch",
                "parameters": {"type": "object"},
            },
        )


def test_runtime_graph_rejects_ambiguous_shell_command_directory():
    descriptors = (
        RuntimeModuleDescriptor(
            "module.a",
            (),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.inspect-a"),),
        ),
        RuntimeModuleDescriptor(
            "module.b",
            (),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.inspect-b"),),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_SHELL_COMMAND_CONFLICT"


def test_contribution_consumers_follow_provider_lifecycle_order():
    descriptors = (
        RuntimeModuleDescriptor(
            "agent.runtime",
            (),
            lambda *_: object(),
            _ARTIFACT,
        ),
        RuntimeModuleDescriptor(
            "shell.command",
            (),
            lambda *_: object(),
            _ARTIFACT,
        ),
        RuntimeModuleDescriptor(
            "module.provider",
            (),
            lambda *_: object(),
            _ARTIFACT,
            agent_tools=(_tool("inspect", "module.inspect"),),
            shell_commands=(_command("module.shell-inspect"),),
        ),
    )

    registry = RuntimeModuleRegistry(_runtime(), descriptors)

    assert registry.order.index("module.provider") < registry.order.index("agent.runtime")
    assert registry.order.index("module.provider") < registry.order.index("shell.command")
    assert registry.describe()[0]["requires"] == ["module.provider"]
    assert registry.describe()[1]["requires"] == ["module.provider"]


def test_dynamic_command_dependency_participates_in_cycle_rejection():
    descriptors = (
        RuntimeModuleDescriptor(
            "shell.command",
            (),
            lambda *_: object(),
            _ARTIFACT,
        ),
        RuntimeModuleDescriptor(
            "module.provider",
            ("shell.command",),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.shell-inspect"),),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_MODULE_DEPENDENCY_CYCLE"


@pytest.mark.asyncio
async def test_replacement_drain_preserves_unselected_owned_work_and_router():
    events = []

    class Authority:
        async def freeze_graph_admission(self, _lease, _graph, *, components):
            events.append(("freeze-routes", components))

        async def resume_graph_admission(self, _lease, _graph, *, components):
            events.append(("resume-routes", components))

    class Module:
        requires_replacement_quiesce = True

        def __init__(self, name):
            self.name = name

        async def quiesce(self):
            events.append(("quiesce", self.name))

        async def resume(self):
            events.append(("resume", self.name))

        def activate_generation(self):
            events.append(("activate", self.name))

        def freeze_owned_admission(self):
            events.append(("freeze-work", self.name))

        async def drain_owned_work(self):
            events.append(("drain-work", self.name))

        def open_owned_admission(self):
            events.append(("open-work", self.name))

        def owned_tasks(self):
            return ()

    descriptors = tuple(
        RuntimeModuleDescriptor(
            name,
            (),
            lambda *_: object(),
            _ARTIFACT,
            owned_work_lifecycle=OwnedWorkLifecycle.FREEZE_DRAIN,
        )
        for name in ("module.changed", "module.retained")
    )
    runtime = SimpleNamespace(
        scope_kind="bot",
        generation_authority=Authority(),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: "lease"),
    )
    registry = RuntimeModuleRegistry(runtime, descriptors)
    registry.graph_identity = object()
    registry.instances = {name: Module(name) for name in registry.order}
    registry.states = {name: ModuleLifecycleState.ACTIVE for name in registry.order}
    registry.contract_router.seal()
    registry.contract_router.activate()

    await registry.drain_owned_work_for_replacement(
        "reload/changed", components=("module.changed",)
    )
    assert registry.contract_router.graph_admission.is_set()
    assert events == [
        ("freeze-routes", ("module.changed",)),
        ("freeze-work", "module.changed"),
        ("drain-work", "module.changed"),
        ("quiesce", "module.changed"),
    ]
    with pytest.raises(ForestError) as conflict:
        await registry.drain_owned_work_for_replacement(
            "reload/changed", components=("module.retained",)
        )
    assert conflict.value.code == "RUNTIME_REPLACEMENT_OPERATION_CONFLICT"

    await registry.resume_owned_work_after_failed_replacement()
    assert registry.contract_router.graph_admission.is_set()
    assert events[-4:] == [
        ("resume", "module.changed"),
        ("activate", "module.changed"),
        ("open-work", "module.changed"),
        ("resume-routes", ("module.changed",)),
    ]

from __future__ import annotations

from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.runtime_modules import (
    ModuleLifecycleState,
    OwnedWorkLifecycle,
    RuntimeAgentToolContribution,
    RuntimeArtifactIdentity,
    RuntimeEventPublicationContribution,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
    RuntimeShellCommandContribution,
)
from forest.worker_limits import WorkerLimits

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


def test_worker_claims_cover_old_and_new_generations_during_graph_prewarm():
    worker_artifact = RuntimeArtifactIdentity(
        "test.worker", "1.0.0", "sha256:" + "b" * 64,
        "bot", "forest.python-worker/1", "installed",
    )
    old_limits = WorkerLimits(max_inflight=4)
    new_limits = WorkerLimits(max_inflight=8)

    def worker(component_id, limits):
        return RuntimeModuleDescriptor(
            component_id, (), None, worker_artifact, worker_limits=limits,
        )

    previous = RuntimeModuleRegistry(
        _runtime(), (worker("module.changed", old_limits),
                     worker("module.retained", old_limits)),
    )
    previous.graph_identity = object()
    previous.instances = {item: object() for item in previous.order}
    previous.states = {item: ModuleLifecycleState.ACTIVE for item in previous.order}
    previous.generation_bindings = {
        item.component_id: SimpleNamespace(
            descriptor_digest=previous._descriptor_fact(item)["descriptor_digest"]
        )
        for item in previous.descriptors
    }
    candidate = RuntimeModuleRegistry(
        _runtime(), (worker("module.changed", new_limits),
                     worker("module.retained", old_limits),
                     worker("module.added", old_limits)),
    )

    impact = candidate.preview_graph_transition(previous)

    assert impact.replace == ("module.changed",)
    assert impact.retain == ("module.retained",)
    assert impact.add == ("module.added",)
    assert impact.worker_capacity.active == (
        ("module.changed", old_limits), ("module.retained", old_limits),
    )
    assert impact.worker_capacity.preparing == (
        ("module.changed", new_limits), ("module.added", old_limits),
    )
    assert len(impact.worker_capacity.peak) == 4
    assert impact.worker_capacity.as_fact() == {
        "active": [
            {"component_id": "module.changed", "limits": old_limits.as_fact()},
            {"component_id": "module.retained", "limits": old_limits.as_fact()},
        ],
        "preparing": [
            {"component_id": "module.changed", "limits": new_limits.as_fact()},
            {"component_id": "module.added", "limits": old_limits.as_fact()},
        ],
    }


def test_worker_resource_claim_is_required_only_for_worker_driver():
    worker_artifact = RuntimeArtifactIdentity(
        "test.worker", "1.0.0", "sha256:" + "b" * 64,
        "bot", "forest.python-worker/1", "installed",
    )
    with pytest.raises(ValueError, match="exact Worker limits"):
        RuntimeModuleDescriptor("module.worker", (), None, worker_artifact)
    with pytest.raises(ValueError, match="exact Worker limits"):
        RuntimeModuleDescriptor(
            "module.native", (), lambda *_: object(), _ARTIFACT,
            worker_limits=WorkerLimits(),
        )


def test_explicit_retention_dependency_must_be_in_candidate_graph():
    consumer = RuntimeModuleDescriptor(
        "module.consumer", (), lambda *_: object(), _ARTIFACT,
        retention_requires=("module.transaction",),
    )
    with pytest.raises(ForestError) as error:
        RuntimeModuleRegistry(_runtime(), (consumer,))
    assert error.value.code == "RUNTIME_MODULE_RETENTION_DEPENDENCY_MISSING"
    assert error.value.details == {
        "component_id": "module.consumer",
        "missing": ["module.transaction"],
    }


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
            "module.tool-consumer",
            (),
            lambda *_: object(),
            _ARTIFACT,
            contribution_directories=("agent-tools",),
        ),
        RuntimeModuleDescriptor(
            "module.command-consumer",
            (),
            lambda *_: object(),
            _ARTIFACT,
            contribution_directories=("shell-commands",),
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

    assert registry.order.index("module.provider") < registry.order.index("module.tool-consumer")
    assert registry.order.index("module.provider") < registry.order.index("module.command-consumer")
    assert registry.directory_consumers == {
        "agent-tools": "module.tool-consumer",
        "shell-commands": "module.command-consumer",
    }
    assert registry.contract_router.agent_tools("module.tool-consumer")[0][0] == "module.provider"
    with pytest.raises(ForestError) as forbidden:
        registry.contract_router.agent_tools("agent.runtime")
    assert forbidden.value.code == "RUNTIME_AGENT_TOOL_DISCOVERY_FORBIDDEN"
    assert {item["component_id"]: item["requires"] for item in registry.describe()} == {
        "module.provider": [],
        "module.tool-consumer": ["module.provider"],
        "module.command-consumer": ["module.provider"],
    }


def test_dynamic_command_dependency_participates_in_cycle_rejection():
    descriptors = (
        RuntimeModuleDescriptor(
            "module.command-consumer",
            (),
            lambda *_: object(),
            _ARTIFACT,
            contribution_directories=("shell-commands",),
        ),
        RuntimeModuleDescriptor(
            "module.provider",
            ("module.command-consumer",),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.shell-inspect"),),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_MODULE_DEPENDENCY_CYCLE"


def test_routing_directory_starts_before_contributors_and_has_one_consumer():
    router = RuntimeModuleDescriptor(
        "module.route-owner", (), lambda *_: object(), _ARTIFACT,
        contribution_directories=("routing-routes",),
    )
    publisher = RuntimeModuleDescriptor(
        "module.publisher", (), lambda *_: object(), _ARTIFACT,
        event_publications=(RuntimeEventPublicationContribution("demo.event", "1.0", {}),),
    )
    registry = RuntimeModuleRegistry(_runtime(), (publisher, router))
    assert registry.order == ["module.route-owner", "module.publisher"]
    assert registry.replacement_closure("module.publisher") == (
        "module.route-owner", "module.publisher",
    )
    assert registry.replacement_closure("module.route-owner") == (
        "module.route-owner", "module.publisher",
    )
    assert registry.contract_router.event_publications("module.route-owner")[0][0] == "module.publisher"
    with pytest.raises(ForestError) as forbidden:
        registry.contract_router.event_publications("routing.graph")
    assert forbidden.value.code == "RUNTIME_EVENT_DIRECTORY_FORBIDDEN"

    with pytest.raises(ForestError) as missing:
        RuntimeModuleRegistry(_runtime(), (publisher,))
    assert missing.value.code == "RUNTIME_CONTRIBUTION_DIRECTORY_MISSING"

    with pytest.raises(ForestError) as duplicate:
        RuntimeModuleRegistry(_runtime(), (router, RuntimeModuleDescriptor(
            "module.other-route-owner", (), lambda *_: object(), _ARTIFACT,
            contribution_directories=("routing-routes",),
        )))
    assert duplicate.value.code == "RUNTIME_CONTRIBUTION_DIRECTORY_CONFLICT"


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
        for name in ("agent.runtime", "module.retained")
    )
    runtime = SimpleNamespace(
        scope_kind="bot",
        generation_authority=Authority(),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: "lease"),
        authority_lease=lambda **_kwargs: "lease",
    )
    registry = RuntimeModuleRegistry(runtime, descriptors)
    registry.graph_identity = object()
    registry.instances = {name: Module(name) for name in registry.order}
    registry.states = {name: ModuleLifecycleState.ACTIVE for name in registry.order}
    registry.contract_router.seal()
    registry.contract_router.activate()

    await registry.drain_owned_work_for_replacement(
        "reload/changed", components=("agent.runtime",)
    )
    assert registry.contract_router.graph_admission.is_set()
    assert events == [
        ("freeze-routes", ("agent.runtime",)),
        ("freeze-work", "agent.runtime"),
        ("drain-work", "agent.runtime"),
        ("quiesce", "agent.runtime"),
    ]
    with pytest.raises(ForestError) as conflict:
        await registry.drain_owned_work_for_replacement(
            "reload/changed", components=("module.retained",)
        )
    assert conflict.value.code == "RUNTIME_REPLACEMENT_OPERATION_CONFLICT"

    await registry.resume_owned_work_after_failed_replacement()
    assert registry.contract_router.graph_admission.is_set()
    assert events[-4:] == [
        ("resume", "agent.runtime"),
        ("activate", "agent.runtime"),
        ("open-work", "agent.runtime"),
        ("resume-routes", ("agent.runtime",)),
    ]

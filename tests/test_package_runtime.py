import asyncio
import hashlib
import json
import shutil
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.package_capabilities import PackageCallScope
from forest.package_runtime import (
    PackageGenerationAdapter,
    PackageGenerationState,
    PackageUnitOfWorkParticipant,
    package_runtime_descriptor,
)
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RuntimeContractRouter,
    RuntimeServiceInvocation,
)
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationContext
from forest.runtime_modules import (
    RuntimeArtifactIdentity,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
)


class NoServiceLedger:
    pass


class EffectServiceLedger:
    async def begin(self, *_args):
        return SimpleNamespace(execute=True, error=None, result=None)

    async def succeed(self, *_args):
        return None

    async def fail(self, *_args):
        return None


class Configuration:
    validation_digest = digest_json(json.loads((
        Path(__file__).parents[1]
        / "src/forest/builtin/trigger_policy/schemas/config.json"
    ).read_text(encoding="utf-8")))

    async def desired(self, *, include_value=False):
        value = {
            "protocol": "forest.conversation.trigger-policy/1",
            "allowed_group_ids": ["933179609"],
            "group_trigger_mode": "MENTION",
            "private_enabled": False,
        }
        return SimpleNamespace(
            revision=1,
            payload_id="payload:configuration:1",
            payload_digest=digest_json(value),
            validation_digest=self.validation_digest,
            value=value if include_value else None,
        )


def test_package_call_scope_binds_exact_runtime_scope():
    common = dict(
        component_id="deployment.audit", generation_id="generation/one",
        instance_id="instance/one", worker_incarnation="incarnation/one",
        call_id="call/one", call_operation_id="operation/one",
        causal_root_kind="management", causal_root_id="operation/one",
    )
    bot = PackageCallScope(bot_id="bot/one", **common)
    assert (bot.scope_kind, bot.scope_id, bot.bot_id) == (
        "bot", "bot/one", "bot/one"
    )
    deployment = PackageCallScope(
        bot_id=None, scope_kind="deployment", scope_id="deployment/one", **common
    )
    assert (deployment.scope_kind, deployment.scope_id, deployment.bot_id) == (
        "deployment", "deployment/one", None
    )
    with pytest.raises(ValueError, match="Non-Bot scope"):
        PackageCallScope(
            bot_id="bot/one", scope_kind="deployment", scope_id="deployment/one",
            **common,
        )


def test_deployment_package_management_scope_never_invents_bot_id(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(Path(__file__).parents[1] / "examples/counter", source)
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'scope = "bot"', 'scope = "deployment"'
        ), encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    adapter = PackageGenerationAdapter(
        snapshot=snapshot, bot_id=None, scope_id="deployment/one",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={}, resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None, state_commands=None,
        service_operation_ledger=NoServiceLedger(),
    )
    adapter.state = PackageGenerationState.ACTIVE
    adapter.generation_identity = SimpleNamespace(generation_id="generation/one")
    adapter._prepared_binding = SimpleNamespace(
        instance_id="instance/one", worker_incarnation="incarnation/one"
    )
    scope, _ = adapter._management_scope("operation/one")
    assert (scope.scope_kind, scope.scope_id, scope.bot_id) == (
        "deployment", "deployment/one", None
    )


def test_deployment_package_descriptor_has_no_bot_kernel_dependency(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(Path(__file__).parents[1] / "examples/counter", source)
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'scope = "bot"', 'scope = "deployment"'
        ), encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    descriptor = package_runtime_descriptor(
        snapshot, bot_id=None, scope_id="deployment/one",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        resolved_contracts={}, generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=SimpleNamespace(), state_commands=SimpleNamespace(),
        service_operation_ledger=NoServiceLedger(),
    )
    assert descriptor.requires == ()
    registry = RuntimeModuleRegistry(
        SimpleNamespace(scope_kind="deployment", generation_authority=None),
        (descriptor,),
    )
    assert registry.order == ["example.counter"]


def test_official_onebot_adapter_graph_can_omit_optional_shell_provider(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/adapter_onebot"
    snapshot = PackageCatalog(tmp_path / "catalog").build_official(source, scope="bot")
    effects = snapshot.component["effect_services"]
    descriptor = package_runtime_descriptor(
        snapshot, bot_id="bot/optional-shell", module_configuration=object(),
        grants=SimpleNamespace(entries=()),
        resolved_contracts={item["name"]: "1.0" for item in snapshot.component["requires"]},
        generation_verifier=object(), generation_member_verifier=object(),
        state_queries=object(), state_commands=object(),
        service_operation_ledger=object(), unit_of_work_authority=object(),
        unit_of_work_coordinator_factory=lambda *_args: object(),
        configuration_apply_coordinator_factory=lambda *_args: object(),
        effect_driver_factories={item["name"]: object() for item in effects},
        effect_driver_bindings=tuple(sorted(
            (item["name"], item["kind"], "fixture/source") for item in effects
        )),
        trust_level="official", plan_only=True,
    )
    assert ("shell.ingress.inspect", "1.0") in descriptor.optional_service_requires

    artifact = RuntimeArtifactIdentity(
        "test.optional-shell", "1.0.0", "sha256:" + "f" * 64,
        "bot", "test.in-process/1", "installed",
    )
    provider_services = {
        "kernel.runtime": (
            ("shell.gate.inspect", "1.0"), ("core.rescue.inspect", "1.0"),
        ),
        "conversation.scheduler": (("conversation.trigger.classify", "1.0"),),
        "codec.provider": (
            ("onebot.codec.normalize", "1.0"),
            ("onebot.codec.prepare-send", "1.0"),
        ),
    }
    provider_ids = (
        set(descriptor.requires) | set(descriptor.retention_requires)
        | set(provider_services)
    )
    stubs = tuple(
        RuntimeModuleDescriptor(
            component_id, (), lambda *_: object(), artifact,
            provided_services=provider_services.get(component_id, ()),
        )
        for component_id in sorted(provider_ids)
    )
    registry = RuntimeModuleRegistry(
        SimpleNamespace(scope_kind="bot", generation_authority=None),
        (*stubs, descriptor),
    )
    assert "shell.command" not in registry.order
    assert registry._service_retention_requires["adapter.onebot"] == (
        "codec.provider", "conversation.scheduler", "kernel.runtime",
    )


async def test_management_preflight_wraps_the_domain_transaction(tmp_path):
    source = tmp_path / "worker-host"
    shutil.copytree(
        Path(__file__).parents[1] / "src/forest/builtin/component_worker_host", source
    )
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'id = "module.reload"\nport = "component.reload.enqueue"',
            'id = "module.reload"\nport = "component.reload.enqueue"\n'
            'transaction_preflight = "fixture.serialized"',
            1,
        ),
        encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    events = []

    @asynccontextmanager
    async def preflight(_payload, *, operation_id):
        assert operation_id == "operation/one"
        events.append("host-lock")

        async def verify_in(conn):
            assert conn == "transaction"
            events.append("verify")

        try:
            yield verify_in
        finally:
            events.append("unlock")

    class Commands:
        async def invoke_management_action(
            self, _scope, _declaration, _payload, *, before_write,
            result_validator, **_kwargs,
        ):
            events.append("transaction")
            await before_write("transaction")
            events.append("write")
            result = {
                "protocol": "forest.component-reload/1",
                "request_id": "reload:" + "a" * 32,
                "component_id": "agent.runtime",
                "expected_graph_revision": 1,
                "requested_by": "web:admin",
                "state": "QUEUED",
                "attempt_count": 0,
                "receipt": None,
                "error_code": None,
                "created_at": "now",
                "updated_at": "now",
            }
            assert result_validator(result)
            events.append("validate")
            return result

    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={}, resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None, state_commands=None,
        domain_commands=Commands(), service_operation_ledger=NoServiceLedger(),
        owned_work_authority=SimpleNamespace(),
        management_action_preflights={"module.reload": preflight},
    )
    adapter.state = PackageGenerationState.ACTIVE
    adapter.generation_identity = SimpleNamespace(generation_id="generation/one")
    adapter._prepared_binding = SimpleNamespace(
        instance_id="instance/one", worker_incarnation="incarnation/one"
    )
    command = next(
        item for item in snapshot.component["domain_commands"]
        if item["name"] == "component.reload.enqueue"
    )
    payload = {
        "component_id": "agent.runtime", "expected_graph_revision": 1,
        "requested_by": "web:admin",
    }

    async def authorize(conn, **_kwargs):
        assert conn == "transaction"
        events.append("authorize")

    with pytest.raises(ForestError) as wrong_action:
        await adapter.invoke_management_action(
            command, payload, action_id="module.missing", operation_id="operation/one",
            plan_digest=None, request_digest=digest_json(payload), timeout_seconds=2,
            authorization=object(), authorization_guard=authorize,
        )
    assert wrong_action.value.code == "MODULE_MANAGEMENT_BINDING_INVALID"
    assert events == []
    result = await adapter.invoke_management_action(
        command, payload, action_id="module.reload", operation_id="operation/one",
        plan_digest=None, request_digest=digest_json(payload), timeout_seconds=2,
        authorization=object(), authorization_guard=authorize,
    )
    assert result["state"] == "QUEUED"
    assert events == [
        "host-lock", "transaction", "authorize", "verify", "write", "validate", "unlock"
    ]
    adapter._management_action_preflights.clear()
    with pytest.raises(ForestError) as missing_binding:
        await adapter.invoke_management_action(
            command, payload, action_id="module.reload", operation_id="operation/one",
            plan_digest=None, request_digest=digest_json(payload), timeout_seconds=2,
            authorization=object(), authorization_guard=authorize,
        )
    assert missing_binding.value.code == "MODULE_MANAGEMENT_PREFLIGHT_UNAVAILABLE"
    assert events == [
        "host-lock", "transaction", "authorize", "verify", "write", "validate", "unlock"
    ]


async def test_delivery_management_action_runs_authorized_uow_plan(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(
        Path(__file__).parents[1] / "src/forest/builtin/delivery"
    )
    events = []

    class Coordinator:
        async def execute(
            self, scope, declaration, payload, *, before_steps, result_validator, **_kwargs
        ):
            assert scope.component_id == "delivery"
            assert declaration["name"] == "delivery.reconcile"
            assert payload["requested_by"] == "web:admin"
            events.append("transaction")
            await before_steps("transaction")
            events.append("effect")
            result = {
                "intent_id": "intent/1", "state": "SENT",
                "accepted": True, "conflict_code": None,
            }
            assert result_validator(result)
            events.append("delivery")
            return result

    adapter = PackageGenerationAdapter(
        snapshot=snapshot, bot_id="bot/one",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={}, resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None, state_commands=None,
        unit_of_work_coordinator=Coordinator(),
        owned_work_authority=SimpleNamespace(),
        service_operation_ledger=NoServiceLedger(),
    )
    adapter.state = PackageGenerationState.ACTIVE
    adapter.generation_identity = SimpleNamespace(generation_id="generation/delivery")
    adapter._prepared_binding = SimpleNamespace(
        instance_id="instance/delivery", worker_incarnation="worker/delivery"
    )
    plan = next(item for item in snapshot.component["unit_of_work_plans"]
                if item["name"] == "delivery.reconcile")
    payload = {
        "work": {
            "intent_id": "intent/1", "attempt_id": "attempt/1",
            "effect_operation_ref": "effect/1",
        },
        "observed_status": "SENT", "error_code": None, "receipt": None,
        "effect": {
            "protocol": "forest.effect-observe/1",
            "effect_operation_id": "effect/1", "attempt_id": "effect-attempt/1",
            "observation_operation_id": "observation/1",
            "status": "SUCCEEDED", "evidence_kind": "reconciliation",
            "evidence_payload": None,
        },
        "evidence": {"kind": "platform-log", "reference": "log/77"},
        "requested_by": "web:admin",
    }

    async def authorize(conn, **kwargs):
        assert conn == "transaction"
        assert kwargs["component_id"] == "delivery"
        assert kwargs["action_key"] == "delivery.unknown.resolve"
        events.append("authorize")

    result = await adapter.invoke_management_action(
        plan, payload, action_id="delivery.unknown.resolve",
        operation_id="operation/reconcile", plan_digest=None,
        request_digest=digest_json(payload), timeout_seconds=5,
        authorization=object(), authorization_guard=authorize,
    )
    assert result["state"] == "SENT"
    assert events == ["transaction", "authorize", "effect", "delivery"]


def test_management_only_memory_services_have_no_runtime_routes(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph"
    )
    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={},
        resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None,
        state_commands=None,
        owned_work_authority=SimpleNamespace(),
        service_operation_ledger=NoServiceLedger(),
    )
    public = {
        (item["service_id"], item["version"])
        for item in adapter.runtime_contract["services"]
    }
    routed = set(adapter.runtime_service_handlers())
    assert ("memory.query.request", 1) in public & routed
    for name in (
        "memory.trial.result", "memory.trial.association-evidence",
        "memory.trial.image", "memory.source.forget.preview"
    ):
        assert (name, 1) not in public
        assert (name, 1) not in routed
        assert name in adapter._provided_services

def test_package_publishes_agent_tool_from_its_own_service(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(Path(__file__).parents[1] / "examples/counter", source)
    (source / "schemas/tool-declaration.json").write_text(
        json.dumps(
            {
                "name": "counter_increment",
                "description": "Increment the package-owned counter.",
                "parameters": {
                    "type": "object",
                    "properties": {"increment": {"type": "integer", "minimum": 1}},
                    "required": ["increment"],
                    "additionalProperties": False,
                },
                "strict": True,
            }
        ),
        encoding="utf-8",
    )
    with (source / "module.toml").open("a", encoding="utf-8") as stream:
        stream.write(
            "\n[[components.agent_tools]]\n"
            'name = "counter_increment"\n'
            'service_id = "counter.increment"\n'
            "service_version = 1\n"
            'declaration_schema = "schemas/tool-declaration.json"\n'
            "approval_required = true\n"
            "supports_query = false\n"
        )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    descriptor = package_runtime_descriptor(
        snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=SimpleNamespace(),
        state_commands=SimpleNamespace(),
        service_operation_ledger=NoServiceLedger(),
    )
    assert descriptor.agent_tools[0].as_fact() == {
        "protocol": "forest.agent-tool-contribution/1",
        "name": "counter_increment",
        "service_id": "counter.increment",
        "version": 1,
        "declaration": snapshot.schema("schemas/tool-declaration.json"),
        "contract_digest": descriptor.agent_tools[0].contract_digest,
        "approval_required": True,
        "supports_query": False,
    }


def test_installed_package_cannot_enable_official_domain_storage(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/counter"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    with pytest.raises(ForestError) as caught:
        package_runtime_descriptor(
            snapshot,
            bot_id="bot-package-runtime",
            module_configuration=Configuration(),
            grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
            resolved_contracts={},
            generation_verifier=lambda **_values: None,
            generation_member_verifier=lambda **_values: None,
            state_queries=None,
            state_commands=None,
            service_operation_ledger=NoServiceLedger(),
            trust_level="installed",
        )
    assert caught.value.code == "PACKAGE_STORAGE_TRUST_REQUIRED"


async def test_package_generation_uses_isolated_worker_and_explicit_reload_states(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/trigger_policy"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    verified = []

    async def verify(**identity):
        verified.append(identity)

    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={},
        resolved_contracts={},
        generation_verifier=verify,
        generation_member_verifier=verify,
        state_queries=None,
        state_commands=None,
        service_operation_ledger=NoServiceLedger(),
    )
    context = PreparedGenerationContext(
        activation_id="9be186b5-667b-4f48-8051-4b360c087632",
        component_id="conversation.trigger-policy",
        requires=(),
        package_id=snapshot.manifest["package_id"],
        package_version=snapshot.manifest["version"],
        package_digest=snapshot.digest,
        driver="forest.python-worker/1",
        trust_level="official",
        descriptor_digest="sha256:" + "2" * 64,
        route_contract_digest=digest_json(adapter.runtime_contract),
        management_contract_digest=digest_json(None),
        management_ui_digest=digest_json([]),
        state_schema=1,
        state_contract_digest=digest_json(adapter.state_contract),
        storage=None,
    )
    await adapter.prepare_generation(context)
    await adapter.bind_generation(
        BoundGenerationIdentity(
            component_id=context.component_id,
            generation_id="generation:package-runtime:1",
            graph_id="graph:package-runtime:1",
            graph_revision=1,
            graph_digest="sha256:" + "3" * 64,
            activation_id=context.activation_id,
        )
    )
    await adapter.start()
    handler = adapter.runtime_service_handlers()[("conversation.trigger.classify", 1)]
    route = FrozenServiceRoute(
        service_id="conversation.trigger.classify",
        version=1,
        caller_component_id="adapter.onebot",
        provider_component_id="conversation.trigger-policy",
        provider_generation_id="generation:package-runtime:1",
        graph_id="graph:package-runtime:1",
        graph_revision=1,
        admission_lane="DATA",
        owned_work=False,
    )
    invocation = RuntimeServiceInvocation(
        "classify:package-runtime:1",
        10,
        "test.package-runtime",
        "source:package-runtime:1",
    )
    assert not callable(handler)
    assert "trigger_policy" not in sys.modules
    normalized = {
        "channel_kind": "group",
        "channel_id": "933179609",
        "self_id": "42",
        "author": {"is_self": False},
        "segments": [{"type": "mention", "external_user_id": "42"}],
    }
    with pytest.raises(ForestError, match="not accepting calls") as candidate:
        await handler.invoke_routed(route, invocation, (), {"normalized": normalized})
    assert candidate.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"

    adapter.activate_generation()
    result = await handler.invoke_routed(
        route, invocation, (), {"normalized": normalized}
    )
    assert result["decision"] == "TRIGGER"
    assert result["classifier_generation_id"] == "generation:package-runtime:1"
    assert verified[-1] == {
        "component_id": "conversation.trigger-policy",
        "generation_id": "generation:package-runtime:1",
        "admission_lane": "DATA",
    }

    await adapter.quiesce()
    assert adapter.state is PackageGenerationState.QUIESCENT
    await adapter.resume()
    adapter.activate_generation()
    assert (
        await handler.invoke_routed(
            route, invocation, (), {"normalized": normalized}
        )
    )["decision"] == "TRIGGER"
    worker = adapter._worker
    await adapter.quiesce()
    await adapter.close()
    assert adapter.state is PackageGenerationState.STOPPED
    assert worker is not None and not worker.alive


async def test_effect_driver_is_owned_by_the_same_package_generation(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/provider_chat_completions"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    config = {
        "protocol": "forest.provider.chat-completions/1",
        "base_url": "https://example.invalid/v1",
        "credential_ref": "secret://test/provider",
        "timeout_seconds": 30,
        "max_connections": 2,
        "capabilities": {
            "deployment_id": "test",
            "model": "test-model",
            "capability_revision": "test/1",
            "model_revision": "test/1",
            "input_modalities": ["text"],
            "supports_tools": True,
            "supports_streaming": True,
            "stream_usage": True,
            "max_output_tokens": 1024,
            "image_roles": ["user"],
            "image_detail": "auto",
            "system_role": "system",
            "output_token_parameter": "max_completion_tokens",
            "tool_image_mapping": "omit",
        },
    }

    class ProviderConfiguration:
        async def desired(self, *, include_value=False):
            return SimpleNamespace(
                revision=7,
                payload_id="payload:provider:7",
                payload_digest=digest_json(config),
                validation_digest=digest_json(
                    snapshot.schema(snapshot.component["config_schema"])
                ),
                value=config if include_value else None,
            )

    events = []

    class Driver:
        async def start(self):
            events.append("start")

        async def invoke(self, payload, *, scope, invocation):
            events.append(("invoke", scope.generation_id, invocation.operation_id))
            prepared = payload["prepared"]
            return {
                "status": "RESPONDED",
                "request_id": prepared["request_id"],
                "transport_attempt_id": prepared["transport_attempt_id"],
                "payload_digest": prepared["payload_digest"],
                "text": "ok",
                "tool_calls": [],
                "usage": None,
                "finish_reason": "stop",
                "provider_response_id": None,
                "provider_model": "test-model",
                "error_code": None,
            }

        async def quiesce(self):
            events.append("quiesce")

        async def resume(self):
            events.append("resume")

        async def close(self):
            events.append("close")

    class LookupDriver:
        async def invoke(self, _payload, *, scope, invocation):
            raise AssertionError("Read lookup is not invoked in this lifecycle test")

        async def start(self):
            pass

        async def quiesce(self):
            pass

        async def resume(self):
            pass

        async def close(self):
            pass

    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-package-runtime",
        module_configuration=ProviderConfiguration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={},
        resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None,
        state_commands=None,
            service_operation_ledger=EffectServiceLedger(),
            effect_driver_factories={
                "provider.model.dispatch": lambda *_values: Driver(),
                "provider.model.result.lookup": lambda *_values: LookupDriver(),
            },
    )
    context = PreparedGenerationContext(
        activation_id="effect-generation",
        component_id="provider.chat-completions",
        requires=(),
        package_id=snapshot.manifest["package_id"],
        package_version=snapshot.manifest["version"],
        package_digest=snapshot.digest,
        driver="forest.python-worker/1",
        trust_level="official",
        descriptor_digest="sha256:" + "2" * 64,
        route_contract_digest=digest_json(adapter.runtime_contract),
        management_contract_digest=digest_json(None),
        management_ui_digest=digest_json([]),
        state_schema=1,
        state_contract_digest=digest_json(adapter.state_contract),
        storage=None,
    )
    await adapter.prepare_generation(context)
    await adapter.bind_generation(
        BoundGenerationIdentity(
            "provider.chat-completions",
            "generation:provider:1",
            "graph:provider:1",
            1,
            "sha256:" + "3" * 64,
            context.activation_id,
        )
    )
    await adapter.start()
    adapter.activate_generation()
    assert events == ["start"]
    await adapter.quiesce()
    assert events[-1] == "quiesce"
    await adapter.resume()
    adapter.activate_generation()
    assert events[-1] == "resume"
    await adapter.quiesce()
    await adapter.close()
    assert events[-2:] == ["quiesce", "close"]


async def test_duplex_effect_ingress_is_generation_bound_and_drained(tmp_path):
    source = Path(__file__).parent / "fixtures/ingress_component"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)

    class Configuration:
        async def desired(self, *, include_value=False):
            return SimpleNamespace(
                revision=1,
                payload_id="payload:ingress:1",
                payload_digest=digest_json({}),
                validation_digest=digest_json(
                    snapshot.schema(snapshot.component["config_schema"])
                ),
                value={} if include_value else None,
            )

    events = []
    drivers = []

    class Driver:
        def __init__(self):
            self.handler = None
            self.accepting = False

        def bind_ingress(self, handler):
            assert self.handler is None
            self.handler = handler
            events.append("bind")

        async def start(self):
            assert self.handler is not None
            events.append("start")

        def activate_ingress(self):
            self.accepting = True
            events.append("activate")

        async def emit(self, frame_id, *, delay_ms=0):
            if not self.accepting:
                raise RuntimeError("ingress closed")
            return await self.handler(
                {"frame_id": frame_id, "delay_ms": delay_ms},
                operation_id="fixture-frame:" + frame_id,
                causal_root_kind="fixture.frame",
                causal_root_id=frame_id,
            )

        async def freeze_ingress(self):
            self.accepting = False
            events.append("freeze")

        async def invoke(self, _payload, *, scope, invocation):
            return {"accepted": True}

        async def quiesce(self):
            events.append("quiesce")

        async def resume(self):
            events.append("resume")

        async def close(self):
            self.accepting = False
            events.append("close")

    def create_driver(*_values):
        driver = Driver()
        drivers.append(driver)
        return driver

    async def verify(**_identity):
        return None

    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={},
        resolved_contracts={},
        generation_verifier=verify,
        generation_member_verifier=verify,
        state_queries=None,
        state_commands=None,
        service_operation_ledger=EffectServiceLedger(),
        effect_driver_factories={"fixture.transport.send": create_driver},
    )
    assert ("fixture.transport.frame", 1) not in adapter.runtime_service_handlers()
    assert all(
        item["service_id"] != "fixture.transport.frame"
        for item in adapter.runtime_contract["services"]
    )
    context = PreparedGenerationContext(
        activation_id="ingress-generation",
        component_id="fixture.ingress",
        requires=(),
        package_id=snapshot.manifest["package_id"],
        package_version=snapshot.manifest["version"],
        package_digest=snapshot.digest,
        driver="forest.python-worker/1",
        trust_level="official",
        descriptor_digest="sha256:" + "2" * 64,
        route_contract_digest=digest_json(adapter.runtime_contract),
        management_contract_digest=digest_json(None),
        management_ui_digest=digest_json([]),
        state_schema=1,
        state_contract_digest=digest_json(adapter.state_contract),
        storage=None,
    )
    await adapter.prepare_generation(context)
    await adapter.bind_generation(
        BoundGenerationIdentity(
            "fixture.ingress",
            "generation:ingress:1",
            "graph:ingress:1",
            1,
            "sha256:" + "3" * 64,
            context.activation_id,
        )
    )
    await adapter.start()
    driver = drivers[0]
    assert events == ["bind", "start"]
    with pytest.raises(RuntimeError, match="ingress closed"):
        await driver.emit("before-publication")

    adapter.activate_generation()
    assert await driver.emit("frame-1") == {
        "frame_id": "frame-1",
        "generation_id": "generation:ingress:1",
    }
    in_flight = asyncio.create_task(driver.emit("frame-drain", delay_ms=100))
    await asyncio.sleep(0.02)
    await adapter.quiesce()
    assert (await in_flight)["frame_id"] == "frame-drain"
    assert events[-2:] == ["freeze", "quiesce"]
    with pytest.raises(RuntimeError, match="ingress closed"):
        await driver.emit("after-freeze")

    await adapter.resume()
    assert events[-1] == "resume"
    with pytest.raises(RuntimeError, match="ingress closed"):
        await driver.emit("before-reactivation")
    adapter.activate_generation()
    assert (await driver.emit("frame-2"))["generation_id"] == "generation:ingress:1"
    await adapter.quiesce()
    await adapter.close()
    assert events[-3:] == ["freeze", "quiesce", "close"]


async def test_package_job_type_is_published_and_bound_to_exact_generation(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/process_executor"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    configuration = {
        "protocol": "forest.process.executor/1",
        "profiles": [
            {
                "profile_id": "fixture.echo",
                "executable": "/usr/bin/echo",
                "executable_sha256": "sha256:" + "0" * 64,
                "arguments_schema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False,
                },
                "argv": [{"literal": "hello"}],
                "working_directory": "/tmp",
                "environment": {},
                "run_as": {"uid": 0, "gid": 0},
                "network_policy": "inherit",
                "require_posix_limits": False,
                "limits": {
                    "cpu_seconds": 1,
                    "memory_bytes": 16777216,
                    "wall_seconds": 1,
                    "output_bytes": 1024,
                    "open_files": 8,
                    "processes": 1,
                    "terminate_grace_seconds": 1,
                },
            }
        ],
    }

    class Configuration:
        async def desired(self, *, include_value=False):
            return SimpleNamespace(
                revision=3,
                payload_id="payload:process:3",
                payload_digest=digest_json(configuration),
                validation_digest=digest_json(
                    snapshot.schema(snapshot.component["config_schema"])
                ),
                value=configuration if include_value else None,
            )

    events = []

    class Driver:
        async def start(self):
            events.append("start")

        async def invoke(self, payload, *, scope, invocation):
            events.append(
                (
                    "invoke",
                    payload["profile_id"],
                    scope.generation_id,
                    invocation.operation_id,
                    type(scope).__name__,
                )
            )
            return {
                "protocol": "forest.process-spawn-result/1",
                "exit_code": 0,
                "stdout": None,
                "stderr": None,
                "stdout_total_bytes": 0,
                "stderr_total_bytes": 0,
                "stdout_truncated": False,
                "stderr_truncated": False,
                "network_policy": "inherit",
            }

        async def quiesce(self):
            events.append("quiesce")

        async def resume(self):
            events.append("resume")

        async def close(self):
            events.append("close")

    def driver_factory(*_values):
        return Driver()

    async def verify_generation(**_values):
        return None

    descriptor = package_runtime_descriptor(
        snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(
            grant_set_digest="sha256:" + "1" * 64,
            entries=(
                SimpleNamespace(
                    capability="service.call:process.native.spawn",
                    resource_kind="component",
                    resource_id="process.executor",
                ),
            ),
        ),
        resolved_contracts={},
        generation_verifier=verify_generation,
        generation_member_verifier=verify_generation,
        state_queries=None,
        state_commands=None,
        service_operation_ledger=EffectServiceLedger(),
        effect_driver_factories={"process.native.spawn": driver_factory},
        effect_driver_bindings=((
            "process.native.spawn", "process.native", "forest.host-native/1",
        ),),
        configuration_apply_coordinator_factory=lambda *_args: SimpleNamespace(),
    )
    assert descriptor.job_types[0].as_fact() == {
        "protocol": "forest.job-type-contribution/1",
        "job_type": "native.execute",
        "job_version": 1,
        "service_id": "job.process.execute",
        "service_version": 1,
        "input_schema": snapshot.schema("schemas/execute-input.json"),
        "input_schema_digest": digest_json(
            snapshot.schema("schemas/execute-input.json")
        ),
        "max_attempts": 1,
        "timeout_seconds": 3660,
        "max_concurrency": 2,
    }

    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(
            grant_set_digest="sha256:" + "1" * 64,
            entries=(
                SimpleNamespace(
                    capability="service.call:process.native.spawn",
                    resource_kind="component",
                    resource_id="process.executor",
                ),
            ),
        ),
        service_routes={},
        resolved_contracts={},
        generation_verifier=verify_generation,
        generation_member_verifier=verify_generation,
        state_queries=None,
        state_commands=None,
        service_operation_ledger=EffectServiceLedger(),
        effect_driver_factories={"process.native.spawn": driver_factory},
    )
    context = PreparedGenerationContext(
        activation_id="process-generation",
        component_id="process.executor",
        requires=(),
        package_id=snapshot.manifest["package_id"],
        package_version=snapshot.manifest["version"],
        package_digest=snapshot.digest,
        driver="forest.python-worker/1",
        trust_level="official",
        descriptor_digest="sha256:" + "2" * 64,
        route_contract_digest=digest_json(adapter.runtime_contract),
        management_contract_digest=digest_json(None),
        management_ui_digest=digest_json([]),
        state_schema=1,
        state_contract_digest=digest_json(adapter.state_contract),
        storage=None,
    )
    await adapter.prepare_generation(context)
    await adapter.bind_generation(
        BoundGenerationIdentity(
            "process.executor",
            "generation:process:1",
            "graph:process:1",
            1,
            "sha256:" + "3" * 64,
            context.activation_id,
        )
    )
    await adapter.start()
    adapter.activate_generation()
    endpoint = adapter.runtime_service_handlers()[("job.process.execute", 1)]
    route = FrozenServiceRoute(
        service_id="job.process.execute",
        version=1,
        caller_component_id="job.scheduler",
        provider_component_id="process.executor",
        provider_generation_id="generation:process:1",
        graph_id="graph:process:1",
        graph_revision=1,
        admission_lane="DATA",
        owned_work=True,
    )
    invocation = RuntimeServiceInvocation(
        "job.execute:fixture:1", 3660, "job.scheduler", "job-fixture"
    )

    async def not_cancelled():
        return False

    async def cancelled():
        return True

    with pytest.raises(ForestError) as rejected:
        await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "job.execute:fixture:cancelled", 3660, "job.scheduler", "job-fixture"
            ),
            (
                DurableJobExecutionRequest(
                    "job-fixture", "native.execute", 1, 1, "admin:test",
                    {"profile_id": "fixture.echo", "arguments": {}}, cancelled,
                ),
            ),
            {},
        )
    assert rejected.value.code == "JOB_CANCELLED"
    assert events == ["start"]

    result = await endpoint.invoke_routed(
        route,
        invocation,
        (
            DurableJobExecutionRequest(
                "job-fixture",
                "native.execute",
                1,
                1,
                "admin:test",
                {"profile_id": "fixture.echo", "arguments": {}},
                not_cancelled,
            ),
        ),
        {},
    )
    assert result["profile_id"] == "fixture.echo"
    assert result["exit_code"] == 0
    assert events == [
        "start",
        (
            "invoke",
            "fixture.echo",
            "generation:process:1",
            "op:" + hashlib.sha256(
                json.dumps(
                    ["job.execute:fixture:1", "native-process-spawn:v1"],
                    separators=(",", ":"),
                ).encode()
            ).hexdigest(),
            "PackageJobCallScope",
        ),
    ]
    await adapter.quiesce()
    await adapter.close()
    assert events[-2:] == ["quiesce", "close"]


async def test_read_service_cannot_use_a_granted_state_write_port(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(
        Path(__file__).parents[1] / "examples/counter"
    )
    writes = []

    class StateCommands:
        async def commit_state(self, scope, **request):
            writes.append((scope.call_id, request))
            return {"value": request["value"], "revision": 1}

    adapter = PackageGenerationAdapter(
        snapshot=snapshot, bot_id="bot/read-boundary",
        module_configuration=Configuration(),
        grants=SimpleNamespace(
            grant_set_digest="sha256:" + "1" * 64,
            entries=(
                SimpleNamespace(
                    capability="state.write", resource_kind="component",
                    resource_id="example.counter",
                ),
            ),
        ),
        service_routes={}, resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None, state_commands=StateCommands(),
        service_operation_ledger=NoServiceLedger(),
    )
    identity = object()
    adapter._worker = SimpleNamespace(
        identity=identity,
        call_causal_root=lambda _call_id: ("test", "root"),
        call_operation_id=lambda _call_id: "operation/one",
    )
    adapter.state = PackageGenerationState.ACTIVE
    adapter.generation_identity = SimpleNamespace(
        generation_id="generation/one", graph_id="graph/one", graph_revision=1
    )
    adapter._prepared_binding = SimpleNamespace(
        instance_id="instance/one", worker_incarnation="worker/one"
    )
    route = FrozenServiceRoute(
        service_id="counter.increment", version=1,
        caller_component_id="example.counter",
        provider_component_id="example.counter",
        provider_generation_id="generation/one",
        graph_id="graph/one", graph_revision=1,
        admission_lane="DATA", owned_work=False,
    )
    invocation = RuntimeServiceInvocation(
        operation_id="operation/one", timeout_seconds=5,
        causal_root_kind="test", causal_root_id="root",
    )

    async def dispatch(_worker, _scope, call_id):
        return await adapter._state_gateway(
            identity, call_id, "commit",
            {
                "key": "counter", "value": {"count": 1},
                "expected_revision": 0, "operation_id": "state/one",
            },
        )

    async def invoke(*, read_only, inbound=invocation):
        return await adapter._run_service_operation(
            route=route, invocation=inbound,
            service_id="counter.increment", input_value={},
            durable=False, read_only=read_only,
            dispatch=dispatch, normalize=lambda value: value,
            result_valid=lambda value: isinstance(value, dict),
        )

    with pytest.raises(ForestError) as denied:
        await invoke(read_only=True)
    assert denied.value.code == "SERVICE_READ_ONLY_VIOLATION"
    assert writes == []
    assert adapter._writable_calls == set()

    with pytest.raises(ForestError) as nested_denied:
        await invoke(read_only=False, inbound=RuntimeServiceInvocation(
            operation_id="operation/two", timeout_seconds=5,
            causal_root_kind="test", causal_root_id="root", read_only=True,
        ))
    assert nested_denied.value.code == "SERVICE_READ_ONLY_VIOLATION"
    assert writes == []

    assert (await invoke(read_only=False))["revision"] == 1
    assert len(writes) == 1
    assert adapter._writable_calls == set()


async def test_read_only_route_rejects_unclassified_and_mutating_kernel_services():
    calls = []

    async def read_service(payload):
        calls.append(("read", payload))
        return {"value": "read"}

    async def write_service(payload):
        calls.append(("write", payload))
        return {"value": "write"}

    provider = SimpleNamespace(
        runtime_contract={
            "services": (
                {"service_id": "sample.read", "version": 1, "semantics": "read"},
                {"service_id": "sample.write", "version": 1, "semantics": "idempotent"},
                {"service_id": "sample.unknown", "version": 1},
            ),
        },
        runtime_service_handlers=lambda: {
            ("sample.read", 1): read_service,
            ("sample.write", 1): write_service,
            ("sample.unknown", 1): write_service,
        },
        generation_identity=SimpleNamespace(generation_id="generation/provider"),
    )
    caller = SimpleNamespace(runtime_contract={
        "requires": (
            {"service_id": "sample.read", "version": 1},
            {"service_id": "sample.write", "version": 1},
            {"service_id": "sample.unknown", "version": 1},
        ),
    })
    registry = SimpleNamespace(
        instances={"caller": caller, "provider": provider},
        states={
            "caller": SimpleNamespace(value="ACTIVE"),
            "provider": SimpleNamespace(value="ACTIVE"),
        },
        graph_identity=SimpleNamespace(
            graph_id="graph/one", graph_revision=1,
            generations={
                "caller": "generation/caller",
                "provider": "generation/provider",
            },
        ),
        generation_authority=None,
    )
    router = RuntimeContractRouter(registry)
    router.register("provider", provider)
    handles = {
        name: router.request(
            caller_component_id="caller", service_id=f"sample.{name}",
            version=1, admission_lane="DATA", owned_work=False,
        )
        for name in ("read", "write", "unknown")
    }
    router.seal()

    async def invoke(name, *, read_only):
        return await handles[name].invoke(
            {"value": name}, operation_id=f"operation/{name}",
            causal_root_kind="test", causal_root_id="root",
            read_only=read_only,
        )

    assert await invoke("read", read_only=True) == {"value": "read"}
    for name in ("write", "unknown"):
        with pytest.raises(ForestError) as denied:
            await invoke(name, read_only=True)
        assert denied.value.code == "SERVICE_READ_ONLY_VIOLATION"
    assert calls == [("read", {"value": "read"})]
    assert await invoke("write", read_only=False) == {"value": "write"}


async def test_unit_of_work_participant_requires_declared_read_only_port():
    calls = []

    async def invoke(_conn, _generation, name, _payload, invocation):
        calls.append((name, invocation.read_only))
        return {"ok": True}

    participant = PackageUnitOfWorkParticipant(
        "example.component", "generation/one", invoke,
        frozenset({"example.preview"}),
    )
    arguments = dict(
        operation_id="operation/one", causal_root_kind="test",
        causal_root_id="root", parent_call_id="call/one", timeout_seconds=5,
        read_only=True,
    )
    with pytest.raises(ForestError) as denied:
        await participant.invoke_in(None, "example.write", {}, **arguments)
    assert denied.value.code == "PACKAGE_UOW_READ_PARTICIPANT_INVALID"
    assert calls == []

    assert await participant.invoke_in(None, "example.preview", {}, **arguments) == {"ok": True}
    assert calls == [("example.preview", True)]

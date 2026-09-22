import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.package_runtime import (
    PackageGenerationAdapter,
    PackageGenerationState,
    package_runtime_descriptor,
)
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationContext


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
            value=value if include_value else None,
        )


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
            "provider.model.dispatch": lambda *_values: Driver()
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


async def test_package_job_type_is_published_and_bound_to_exact_generation(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/process_executor"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    configuration = {"protocol": "forest.process.executor/1", "profiles": []}

    class Configuration:
        async def desired(self, *, include_value=False):
            return SimpleNamespace(
                revision=3,
                payload_id="payload:process:3",
                payload_digest=digest_json(configuration),
                value=configuration if include_value else None,
            )

    events = []

    class Driver:
        async def start(self):
            events.append("start")

        async def invoke_job(self, request, *, scope, invocation):
            events.append(
                (
                    "invoke_job",
                    request.job_id,
                    scope.generation_id,
                    invocation.operation_id,
                )
            )
            return {
                "protocol": "forest.native-execution-result/1",
                "profile_id": request.input["profile_id"],
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
    descriptor = package_runtime_descriptor(
        snapshot,
        bot_id="bot-package-runtime",
        module_configuration=Configuration(),
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        resolved_contracts={},
        startup_components=(),
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None,
        state_commands=None,
        service_operation_ledger=EffectServiceLedger(),
        effect_driver_factories={"job.process.execute": driver_factory},
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
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64, entries=()),
        service_routes={},
        resolved_contracts={},
        generation_verifier=lambda **_values: None,
        generation_member_verifier=lambda **_values: None,
        state_queries=None,
        state_commands=None,
        service_operation_ledger=EffectServiceLedger(),
        effect_driver_factories={"job.process.execute": driver_factory},
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
    assert result["exit_code"] == 0
    assert events == [
        "start",
        (
            "invoke_job",
            "job-fixture",
            "generation:process:1",
            "job.execute:fixture:1",
        ),
    ]
    await adapter.quiesce()
    await adapter.close()
    assert events[-2:] == ["quiesce", "close"]

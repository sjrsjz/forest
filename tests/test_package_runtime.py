import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.package_runtime import PackageGenerationAdapter, PackageGenerationState
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationContext


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
        grants=SimpleNamespace(grant_set_digest="sha256:" + "1" * 64),
        service_routes={},
        resolved_contracts={},
        generation_verifier=verify,
        generation_member_verifier=verify,
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
        await handler.invoke_routed(route, (), {"normalized": normalized})
    assert candidate.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"

    adapter.activate_generation()
    result = await handler.invoke_routed(route, (), {"normalized": normalized})
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
        await handler.invoke_routed(route, (), {"normalized": normalized})
    )["decision"] == "TRIGGER"
    worker = adapter._worker
    await adapter.quiesce()
    await adapter.close()
    assert adapter.state is PackageGenerationState.STOPPED
    assert worker is not None and not worker.alive

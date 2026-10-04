import json
import sys
from pathlib import Path
from types import SimpleNamespace

from forest.builtin.agent_runtime.python.agent_protocol import (
    CORE_PROMPT,
    CORE_PROMPT_SHA256,
    PARSER_REVISION,
)
from forest.package_runtime import PackageGenerationAdapter
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationContext


class _Configuration:
    def __init__(self, validation_digest):
        self.validation_digest = validation_digest

    async def desired(self, *, include_value=False):
        value = {
            "protocol": "forest.agent.runtime/1",
            "agent_spec_revision": "agent-spec/1",
            "preset_revision": "preset/1",
            "preset": {
                "payload_id": "payload:preset:1",
                "digest": "sha256:" + "1" * 64,
                "owner_id": "preset:1",
            },
            "run_timeout_seconds": 300,
            "max_steps": 32,
            "output_tokens": 4096,
            "stream": True,
        }
        return SimpleNamespace(
            revision=1,
            payload_id="payload:agent-configuration:1",
            payload_digest=digest_json(value),
            validation_digest=self.validation_digest,
            value=value if include_value else None,
        )

    async def owner_id(self, revision):
        assert revision == 1
        return "revision:1"


class _NoServiceLedger:
    pass


class _NoOwnedWorkAuthority:
    async def invoke_owned_work(self, *_args, **_kwargs):
        return None


async def test_agent_protocol_is_owned_by_an_isolated_builtin_generation(tmp_path):
    source = Path(__file__).parents[1] / "src/forest/builtin/agent_runtime"
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    configuration = _Configuration(
        digest_json(json.loads((source / "schemas/config.json").read_text(encoding="utf-8")))
    )

    async def verify_generation(**_values):
        return None

    adapter = PackageGenerationAdapter(
        snapshot=snapshot,
        bot_id="bot-agent-package",
        module_configuration=configuration,
        grants=SimpleNamespace(grant_set_digest="sha256:" + "2" * 64, entries=()),
        service_routes={},
        resolved_contracts={},
        generation_verifier=verify_generation,
        generation_member_verifier=verify_generation,
        state_queries=None,
        state_commands=None,
        owned_work_authority=_NoOwnedWorkAuthority(),
        service_operation_ledger=_NoServiceLedger(),
        contribution_directories={"agent-tools": ()},
    )
    context = PreparedGenerationContext(
        activation_id="agent-package-generation",
        component_id="agent.runtime",
        requires=(),
        package_id=snapshot.manifest["package_id"],
        package_version=snapshot.manifest["version"],
        package_digest=snapshot.digest,
        driver="forest.python-worker/1",
        trust_level="official",
        descriptor_digest="sha256:" + "3" * 64,
        route_contract_digest=digest_json(adapter.runtime_contract),
        management_contract_digest=digest_json(None),
        management_ui_digest=digest_json([]),
        state_schema=1,
        state_contract_digest=digest_json(adapter.state_contract),
        storage=None,
    )
    await adapter.prepare_generation(context)
    identity = BoundGenerationIdentity(
        component_id="agent.runtime",
        generation_id="generation:agent-package:1",
        graph_id="graph:agent-package:1",
        graph_revision=1,
        graph_digest="sha256:" + "4" * 64,
        activation_id=context.activation_id,
    )
    await adapter.bind_generation(identity)
    await adapter.start()
    try:
        assert adapter._worker.identity.configuration_payload_id == (
            "payload:agent-configuration:1"
        )
        assert adapter._worker.identity.configuration_payload_digest == digest_json(
            {
                "protocol": "forest.agent.runtime/1",
                "agent_spec_revision": "agent-spec/1",
                "preset_revision": "preset/1",
                "preset": {
                    "payload_id": "payload:preset:1",
                    "digest": "sha256:" + "1" * 64,
                    "owner_id": "preset:1",
                },
                "run_timeout_seconds": 300,
                "max_steps": 32,
                "output_tokens": 4096,
                "stream": True,
            }
        )
        assert adapter._worker.identity.configuration_owner_id == "revision:1"
        adapter.activate_generation()
        assert ("agent.execute", 2) in adapter.runtime_service_handlers()
        endpoint = adapter.runtime_service_handlers()[("agent.protocol.describe", 1)]
        route = FrozenServiceRoute(
            service_id="agent.protocol.describe",
            version=1,
            caller_component_id="kernel.management",
            provider_component_id="agent.runtime",
            provider_generation_id=identity.generation_id,
            graph_id=identity.graph_id,
            graph_revision=identity.graph_revision,
            admission_lane="CONTROL",
            owned_work=False,
        )
        result = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "agent-protocol-describe:1",
                10,
                "test.agent-runtime-package",
                "agent-protocol:1",
            ),
            ({},),
            {},
        )
        assert result == {
            "protocol_id": "forest-agent-protocol",
            "protocol_version": 1,
            "core_prompt_digest": CORE_PROMPT_SHA256,
            "parser_revision": PARSER_REVISION,
            "core_prompt": CORE_PROMPT,
            "core_prompt_bytes": len(CORE_PROMPT.encode("utf-8")),
        }
        assert "agent_runtime" not in sys.modules
    finally:
        await adapter.quiesce()
        await adapter.close()

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.bootstrap_materialization import materialize_bootstrap_configuration
from forest.official_bot_provisioning import official_bootstrap_contributions
from forest.packages import PackageCatalog


class RecordingPayloads:
    def __init__(self):
        self.writes = []

    async def epoch(self):
        return 1

    async def put_bytes(self, **values):
        self.writes.append(values)
        return SimpleNamespace(
            payload_id="payload:test-preset",
            content_digest="sha256:" + hashlib.sha256(values["content"]).hexdigest(),
        )


@pytest.mark.asyncio
async def test_package_templates_materialize_profile_links_and_agent_payload(tmp_path):
    catalog = PackageCatalog(tmp_path / "packages")
    contributions = official_bootstrap_contributions(catalog)
    profile = {item.component_id: item.initial_value.copy() for item in contributions}
    profile["provider.chat-completions"]["capabilities"] = {
        **profile["provider.chat-completions"]["capabilities"],
        "max_output_tokens": 2048,
    }
    profile["agent.runtime"]["preset_system"] = ["You are Forest."]
    profile["adapter.onebot"]["self_id"] = "123456789"
    profile["embedding.multimodal"]["profile"] = {
        **profile["embedding.multimodal"]["profile"],
        "model_revision": "model/new",
    }
    payloads = RecordingPayloads()
    root = Path(__file__).resolve().parents[1] / "src/forest/builtin"

    async def materialize(package_name):
        return await materialize_bootstrap_configuration(
            catalog.build(root / package_name), profile, payloads=payloads,
        )

    provider = await materialize("provider_chat_completions")
    agent = await materialize("agent_runtime")
    onebot = await materialize("adapter_onebot")
    memory = await materialize("memory_graph")
    assert provider == profile["provider.chat-completions"]
    assert agent["output_tokens"] == 2048
    assert agent["preset"]["payload_id"] == "payload:test-preset"
    assert json.loads(payloads.writes[0]["content"]) == {"system": ["You are Forest."]}
    assert payloads.writes[0]["owner_component_id"] == "agent.runtime"
    assert onebot["bindings"][0]["self_id"] == "123456789"
    assert onebot["bindings"][0]["enabled"] is True
    assert memory["embedding"]["model_revision"] == "model/new"

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from forest.errors import ForestError
from forest.http_effect_driver import HttpEffectDriver
from forest.packages import PackageCatalog, auto_install_official_sources
from forest.sdk_tools import AgentToolExecutor


def test_external_tools_have_separate_read_observation_ports_and_owned_settings(tmp_path):
    root = Path(__file__).parents[1]
    catalog = PackageCatalog(tmp_path / "catalog")
    assert not any("plugins" in source.parts for source in auto_install_official_sources("bot"))
    for name, count in (("deepseek_web", 2), ("http_tools", 2), ("onebot_tools", 13)):
        package = catalog.build(root / "plugins" / name)
        tools = package.component["agent_tools"]
        ports = {row["name"]: row for row in package.component["provides"]}
        assert len(tools) == count and package.management_ui() is not None
        for tool in tools:
            assert ports[tool["service_id"]]["semantics"] == "at-most-once"
            assert ports[tool["query_service_id"]]["semantics"] == "read"
            assert tool["service_id"] != tool["query_service_id"]
            assert ports[tool["service_id"]]["allowed_callers"] == ["agent.runtime"]


async def test_tool_result_query_never_executes_or_writes_and_replay_keeps_original_result():
    records, writes, executions = {}, [], []

    class State:
        async def get(self, key):
            return {"value": records.get(key), "revision": 1 if key in records else 0}

        async def commit(self, key, value, expected_revision, operation_id):
            assert expected_revision == 0 and key not in records
            records[key] = value
            writes.append(operation_id)
            return {"revision": 1}

    class Payloads:
        async def put_json(self, value, **ownership):
            return {"payload_id": "result:1", "digest": "sha256:" + "a" * 64,
                    "owner_component_id": "plugin.test", **ownership}

    ctx = SimpleNamespace(state=State(), payloads=Payloads(),
                          identity={"generation_id": "generation:1"},
                          operations=SimpleNamespace(stable=lambda step: "op:" + step))
    command = {"protocol": "forest.agent-tool-execute/1", "component_id": "plugin.test",
               "generation_id": "generation:1", "tool_call_id": "tool:1", "service_id": "plugin.test.read",
               "effect_operation_id": "effect:1", "effect_attempt_id": "attempt:1", "arguments": {"url": "https://example.test"}}

    async def execute(request, _ctx):
        executions.append(request)
        return {"source": request["arguments"]["url"]}

    executor = AgentToolExecutor("plugin.test")
    query = {**command, "protocol": "forest.agent-tool-query/1"}
    assert (await executor.query(query, ctx))["status"] == "UNKNOWN"
    assert records == {} and writes == [] and executions == []
    result = await executor.execute(command, ctx, execute)
    assert result["status"] == "SUCCEEDED"
    assert await executor.query(query, ctx) == result
    assert await executor.execute(command, ctx, execute) == result
    assert len(executions) == len(writes) == 1
    with pytest.raises(ForestError) as caught:
        await executor.query({**query, "arguments": {"url": "https://changed.test"}}, ctx)
    assert caught.value.code == "AGENT_TOOL_OPERATION_CONFLICT"
    assert len(writes) == 1


class Payloads:
    def __init__(self):
        self.values = []

    async def epoch(self):
        return 0

    async def put_bytes(self, **value):
        self.values.append(value)
        return SimpleNamespace(payload_id="response:1", content_digest="sha256:" + hashlib.sha256(value["content"]).hexdigest())


def policy():
    return {"transport": {"allowed_origins": ["https://api.example.test"],
                          "allow_public_urls": False, "allowed_private_hosts": [],
                          "timeout_seconds": 10, "max_response_bytes": 4096,
                          "credential_ref": "secret://test/key", "credential_purpose": "test.api-key"}}


async def test_http_pins_dns_and_keeps_credentials_and_body_out_of_worker_frames(monkeypatch):
    requests, resolved = [], []
    monkeypatch.setattr("socket.getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))])

    async def handler(request):
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "application/json", "set-cookie": "private=value"},
                              stream=httpx.ByteStream(json.dumps({"message": "result"}).encode()))

    async def credential(reference, purpose):
        resolved.append((reference, purpose))
        return "test-only-key"

    async def verify(**kwargs):
        return None

    payloads = Payloads()
    identity = SimpleNamespace(component_id="tools.test", generation_id="generation:test")
    driver = HttpEffectDriver(policy(), identity, runtime=SimpleNamespace(verify_generation_member=verify),
                              payloads=payloads, credential_resolver=credential,
                              client_factory=lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(handler), **kw))
    await driver.start()
    try:
        result = await driver.invoke({"method": "POST", "url": "https://api.example.test/search", "headers": {},
                                      "body": "{}", "read_only": True}, scope=identity,
                                     invocation=SimpleNamespace(operation_id="operation:1"))
        assert result["outcome"] == "RECEIVED"
        assert result["body"]["owner_component_id"] == "tools.test"
        assert "test-only-key" not in json.dumps(result) and "set-cookie" not in result["headers"]
        assert requests[0].url.host == "8.8.8.8" and requests[0].headers["host"] == "api.example.test"
        assert requests[0].extensions["sni_hostname"] == "api.example.test"
        assert requests[0].headers["authorization"] == "Bearer test-only-key"
        assert resolved == [("secret://test/key", "test.api-key")]
        assert json.loads(payloads.values[0]["content"]) == {"message": "result"}
        denied = await driver.invoke({"method": "GET", "url": "https://other.test/", "headers": {}, "body": None,
                                      "read_only": True}, scope=identity, invocation=SimpleNamespace(operation_id="operation:2"))
        assert denied["outcome"] == "FAILED" and denied["error_code"] == "HTTP_TARGET_FORBIDDEN"
        assert len(requests) == len(resolved) == 1
    finally:
        await driver.close()
    assert driver.client is None


@pytest.mark.parametrize("read_only,outcome", [(True, "FAILED"), (False, "UNKNOWN")])
async def test_http_timeout_never_claims_a_write_was_not_applied(monkeypatch, read_only, outcome):
    monkeypatch.setattr("socket.getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))])

    async def handler(request):
        raise httpx.ReadTimeout("result lost")

    async def verify(**kwargs):
        return None

    configuration = policy()
    configuration["transport"]["credential_ref"] = None
    identity = SimpleNamespace(component_id="tools.test", generation_id="generation:test")
    driver = HttpEffectDriver(configuration, identity, runtime=SimpleNamespace(verify_generation_member=verify),
                              payloads=Payloads(), credential_resolver=None,
                              client_factory=lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(handler), **kw))
    await driver.start()
    try:
        result = await driver.invoke({"method": "POST", "url": "https://api.example.test/", "headers": {},
                                      "body": "{}", "read_only": read_only}, scope=identity,
                                     invocation=SimpleNamespace(operation_id="operation:1"))
        assert result["outcome"] == outcome and result["body"] is None
    finally:
        await driver.close()

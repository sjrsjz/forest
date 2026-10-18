"""Real isolated Workers, local HTTP server and simulated OneBot only."""

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from shutil import copytree
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from test_bot_host import (
    FakeEmbeddingClient,
    FakeMediaClient,
    FakeMilvusAuditClient,
    FakeMilvusClient,
    FakeWebSocketClient,
    RecordingModelClient,
)

from forest.bot_host import BotHost, BotSettings
from forest.builtin_packages import bootstrap_installed_package
from forest.packages import PackageCatalog, digest_json


@pytest.fixture
async def external_database():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private test database")
    database = "forest_external_tools_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + database, "", ""))
    try:
        yield uri
    finally:
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        await admin.close()


@pytest.mark.postgres
async def test_external_tools_install_execute_authorize_and_hot_reload(tmp_path, external_database):
    uri = external_database
    http_requests, commands, clients = [], [], []

    async def http_handler(reader, writer):
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode().split("\r\n")
            method, path, _ = lines[0].split(" ")
            headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
            body = await reader.readexactly(int(headers.get("Content-Length", "0")))
            http_requests.append((method, path, body))
            if path == "/v1/tools/search":
                assert json.loads(body)["query"] == "forest test"
                value = {"results": [{"title": "Fixture", "url": "https://example.test/source",
                                      "summary": "Source evidence", "content": "", "date": None,
                                      "site_name": "fixture", "rerank_score": 2.5, "authority": 3}]}
            elif path == "/v1/tools/fetch":
                value = {"title": "Fixture", "content": "# Retrieved page\nSource evidence"}
            else:
                value = {"method": method, "path": path, "received": body.decode()}
            content = json.dumps(value).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                         + str(len(content)).encode() + b"\r\nConnection: close\r\n\r\n" + content)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(http_handler, "127.0.0.1", 0)
    origin = "http://127.0.0.1:" + str(server.sockets[0].getsockname()[1])
    settings = BotSettings(
        bot_id="bot-external-tools-" + uuid4().hex[:10], dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="test-model-key",
        model="test-model", model_deployment_id="provider.chat-completions/test", model_max_output_tokens=1024,
        model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings", embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test", embedding_model_revision="test/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B", embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token", allowed_group_ids=("101",),
        group_trigger_mode="MENTION", private_enabled=True, shell_administrator_ids=("42",),
        agent_system_prompt="External tool integration fixture.",
    )

    class OneBot(FakeWebSocketClient):
        async def get_group_list(self, **kwargs):
            return [{"group_id": "101", "group_name": "Allowed"},
                    {"group_id": "102", "group_name": "Outside scope"}]

        async def query_api(self, action, arguments, **kwargs):
            if action == "get_group_info":
                return {"group_id": "101", "group_name": "Allowed", "member_count": 2, "max_member_count": 100}
            member = {"group_id": "101", "user_id": arguments.get("user_id", "43"),
                      "nickname": "Fixture", "card": "", "role": "member"}
            return {"items": [member], "total": 1, "next_after_user_id": None} if action == "get_group_member_list" else member

        async def command_api(self, action, arguments, **kwargs):
            commands.append((action, arguments))
            return {"outcome": "SUCCEEDED", "error_code": None}

    async def websocket(*args, **kwargs):
        client = OneBot(settings.onebot_self_id)
        clients.append(client)
        return client

    cases = [
        ("web_search", {"query": "forest test", "count": 2, "ori_question": None, "localization": "", "need_content": False}),
        ("web_fetch", {"url": "https://example.test/source"}),
        ("http_get", {"url": origin + "/read", "method": "GET", "headers": []}),
        ("http_request", {"url": origin + "/write", "method": "POST", "headers": [], "body": "write-body"}),
        ("onebot_account_info", {}), ("onebot_groups", {"query": "", "after_group_id": None}),
        ("onebot_group_info", {"group_id": "101"}), ("onebot_member_info", {"group_id": "101", "user_id": "43"}),
        ("onebot_members", {"group_id": "101", "after_user_id": None}),
        ("onebot_send_group", {"group_id": "101", "message": "Tool [CQ:at,qq=43][CQ:face,id=178]"}),
        ("onebot_send_private", {"user_id": "43", "message": "Private tool message"}),
        ("onebot_ban_member", {"group_id": "101", "user_id": "43", "duration": 60}),
        ("onebot_ban_all", {"group_id": "101", "enable": True}),
        ("onebot_kick_member", {"group_id": "101", "user_id": "43", "reject_add_request": False}),
        ("onebot_set_card", {"group_id": "101", "user_id": "43", "card": "Fixture"}),
        ("onebot_set_group_name", {"group_id": "101", "group_name": "Renamed"}),
        ("onebot_delivery_status", None),
    ]
    mode = {"denied": False}

    def reply(prepared, number):
        wire = json.loads(prepared.body)
        prior_tools = [row for row in wire["messages"] if row["role"] == "tool"]
        if mode["denied"]:
            if prior_tools and "ONEBOT_MANAGEMENT_DENIED" in json.dumps(prior_tools):
                return "Permission denial observed."
            tool, arguments = "onebot_ban_member", {"group_id": "101", "user_id": "43", "duration": 60}
        else:
            if number > len(cases):
                return "All external tools completed."
            tool, arguments = cases[number - 1]
            if arguments is None:
                match = re.search(rb"outbound:[a-f0-9]{64}", prepared.body)
                assert match is not None
                arguments = {"intent_id": match.group().decode()}
        trigger = prepared.manifest["trigger_claims"][0]["last_trigger_ref"]
        return {"role": "assistant", "content": None, "tool_calls": [{"id": "external_call_" + str(number),
                "type": "function", "function": {"name": tool, "arguments": json.dumps({
                    "authority_trigger_id": trigger, "arguments": arguments})}}]}

    RecordingModelClient.requests = []
    RecordingModelClient.first_gate = None
    RecordingModelClient.not_sent_attempts = 0
    RecordingModelClient.reply_factory = reply
    catalog = PackageCatalog(tmp_path / "catalog")
    host = BotHost(settings, model_client_factory=RecordingModelClient,
                   embedding_client_factory=FakeEmbeddingClient, media_client_factory=FakeMediaClient,
                   vector_index_client_factory=FakeMilvusClient, vector_index_audit_client_factory=FakeMilvusAuditClient,
                   websocket_client_factory=websocket, package_catalog=catalog)
    staging = PackageCatalog(tmp_path / "staging")

    async def install(source, configuration=None):
        snapshot = staging.build(source)
        archive = staging.export_archive(snapshot.digest)
        catalog.publish_reviewed_bot_archive(
            archive, expected_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
            expected_package_digest=snapshot.digest, expected_manifest_digest=digest_json(snapshot.manifest),
            expected_package_id=snapshot.manifest["package_id"], expected_component_id=snapshot.component["id"],
            expected_runtime="python-worker",
        )
        approved = tuple(snapshot.component["capabilities"]["requests"])
        if configuration is not None:
            await bootstrap_installed_package(
                snapshot.digest, catalog, host.store, host.schema_bundle_installer, kernel=host._kernel,
                initial_configuration=configuration, approved_capabilities=approved,
                effect_driver_registry=host._official_effect_drivers,
            )
        contract = host.registry.get("component.worker-host").management_contract
        selection = await contract.query("selection", {})
        before = host.registry.graph_identity.graph_revision
        proposal = {"component_id": snapshot.component["id"], "source_kind": "installed",
                    "package_digest": snapshot.digest, "manifest_digest": digest_json(snapshot.manifest),
                    "approved_capabilities": list(approved), "desired_enabled": True,
                    "expected_configuration_revision": 1, "expected_selection_revision": selection["revision"],
                    "expected_graph_revision": before}
        plan = await contract.query("validate", proposal)
        await contract.action(
            "module.enable", operation_id="install:" + snapshot.digest[7:],
            payload={**proposal, "plan_digest": plan["plan_digest"]},
            authorization=SimpleNamespace(actor_ref="subject/test-admin"),
            authorization_guard=lambda *args, **kwargs: asyncio.sleep(0),
        )
        async with asyncio.timeout(240):
            while True:
                latest = (await host._kernel.runtime.status())["latest_replacement"]
                if latest is not None and latest["state"] == "FAILED":
                    pytest.fail(str(latest))
                member = next((row for row in host.registry.descriptors if row.component_id == snapshot.component["id"]), None)
                if member and member.artifact.package_digest == snapshot.digest and host.registry.graph_identity.graph_revision > before:
                    print("Installed", snapshot.component["id"], flush=True)
                    return snapshot
                await asyncio.sleep(.2)

    async def trigger(message_id, user_id):
        event = json.dumps({"self_id": int(settings.onebot_self_id), "post_type": "message",
                            "message_type": "group", "sub_type": "normal",
                            "time": int(datetime.now(timezone.utc).timestamp()),
                            "message_id": message_id, "user_id": user_id, "group_id": 101,
                            "sender": {"user_id": user_id, "nickname": "Fixture", "card": "", "role": "member"},
                            "message": [{"type": "at", "data": {"qq": settings.onebot_self_id}},
                                        {"type": "text", "data": {"text": "Exercise external tools"}}]}).encode()
        await asyncio.gather(clients[-1].handler(event), clients[-1].handler(event))

    async def wait_run(count):
        namespace = next(row.storage.spec.namespace for row in host.registry.descriptors if row.component_id == "agent.runtime")
        from forest.schema_bundles import module_schema_namespace
        runtime = module_schema_namespace("forest.kernel.runtime")
        polls = 0
        async with asyncio.timeout(180):
            while True:
                async with host.store.pool.connection() as conn:
                    runs = await (await conn.execute(sql.SQL("SELECT state,error_code FROM {}.agent_runs WHERE bot_id=%s ORDER BY created_at")
                                                     .format(sql.Identifier(namespace)), (settings.bot_id,))).fetchall()
                if len(runs) >= count and runs[-1]["state"] in {"COMPLETED", "FAILED", "BLOCKED_UNKNOWN"}:
                    if runs[-1]["state"] != "COMPLETED":
                        async with host.store.pool.connection() as conn:
                            calls = await (await conn.execute(sql.SQL(
                                "SELECT tool_name,state,error_code FROM {}.tool_calls ORDER BY created_at"
                            ).format(sql.Identifier(namespace)))).fetchall()
                            errors = await (await conn.execute(sql.SQL(
                                "SELECT service_name,error_code,count(*) FROM {}.service_call_attempts "
                                "WHERE status IN ('FAILED','UNKNOWN','REJECTED') GROUP BY service_name,error_code"
                            ).format(sql.Identifier(runtime)))).fetchall()
                        print("Failed calls", calls, "Service errors", errors, flush=True)
                    assert runs[-1]["state"] == "COMPLETED", runs
                    return namespace
                polls += 1
                if polls % 100 == 0:
                    async with host.store.pool.connection() as conn:
                        errors = await (await conn.execute(sql.SQL(
                            "SELECT service_name,error_code,count(*) FROM {}.service_call_attempts "
                            "WHERE status IN ('FAILED','UNKNOWN','REJECTED') GROUP BY service_name,error_code"
                        ).format(sql.Identifier(runtime)))).fetchall()
                        active = await (await conn.execute(sql.SQL(
                            "SELECT service_name,clock_timestamp()-created_at AS age FROM {}.service_call_attempts "
                            "WHERE status='ACTIVE' ORDER BY created_at DESC LIMIT 8"
                        ).format(sql.Identifier(runtime)))).fetchall()
                    print("Runs", runs, "Errors", errors, "Active", active, flush=True)
                await asyncio.sleep(.2)

    try:
        await host.start()
        binding = next(row for row in host.registry.descriptors if row.component_id == "adapter.onebot")
        binding_config = await host.registry.get(binding.component_id).module_configuration.desired(include_value=True)
        binding_ref = binding_config.value["bindings"][0]["binding_ref"]
        for name in ("deepseek_web", "http_tools", "onebot_tools"):
            source = Path(__file__).parents[1] / "plugins" / name
            configuration = json.loads((source / "defaults.json").read_text(encoding="utf-8"))
            if "transport" in configuration:
                configuration["transport"].update(allowed_origins=[origin], allowed_private_hosts=["127.0.0.1"])
            if name == "deepseek_web":
                configuration["endpoint"] = origin
            elif name == "http_tools":
                configuration["write_principals"] = ["qq:42"]
            else:
                configuration.update(binding_ref=binding_ref, allowed_group_ids=["101"], operator_ids=["42"])
            await install(source, configuration)
        await trigger(9001, 42)
        namespace = await wait_run(1)
        async with host.store.pool.connection() as conn:
            tools = await (await conn.execute(sql.SQL("SELECT tool_name,state,error_code FROM {}.tool_calls WHERE bot_id=%s ORDER BY created_at")
                                              .format(sql.Identifier(namespace)), (settings.bot_id,))).fetchall()
        assert len(tools) == len(cases) and all(row["state"] == "SUCCEEDED" for row in tools), tools
        assert len(http_requests) == 4 and len(commands) == 5
        async with asyncio.timeout(30):
            while sum(len(client.sent) for client in clients) < 3:
                await asyncio.sleep(.1)
        assert sum(len(client.sent) for client in clients) == 3
        sent = [json.loads(message.wire_body) for client in clients for message in client.sent]
        group_tool = next(message for message in sent if message.get("group_id") == 101
                          and any(part["type"] == "face" for part in message["message"]))
        assert {"type": "at", "data": {"qq": "43"}} in group_tool["message"]
        assert {"type": "face", "data": {"id": "178"}} in group_tool["message"]
        mode["denied"] = True
        await trigger(9002, 44)
        await wait_run(2)
        assert len(commands) == 5
        source = tmp_path / "onebot-tools-v2"
        copytree(Path(__file__).parents[1] / "plugins/onebot_tools", source)
        manifest = source / "module.toml"
        manifest.write_text(manifest.read_text(encoding="utf-8").replace('version = "1.0.0"', 'version = "1.0.1"'), encoding="utf-8")
        old_generation = host.registry.graph_identity.generations["tools.onebot"]
        await install(source)
        assert host.registry.graph_identity.generations["tools.onebot"] != old_generation
        assert len(http_requests) == 4 and len(commands) == 5
    finally:
        RecordingModelClient.reply_factory = None
        await host.close()
        server.close()
        await server.wait_closed()

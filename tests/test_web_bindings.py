"""Full Web -> private Bot -> protocol Worker -> actual OneBot HTTP roundtrip."""

import asyncio
import hashlib
import hmac
import json
from contextlib import asynccontextmanager

import pytest
from test_web_bots import (  # noqa: F401
    ORIGIN,
    login,
    post,
    ready,
    running,
    setup,
    web_bots_database,
)

from forest.bootstrap import BootstrapState
from forest.errors import ForestError
from forest.management.app import WebSettings
from forest.management.binding_catalog import BindingCatalog
from forest.memory_types import SourceRef
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres
TOKEN = "only-a-test-access-token"
SECRET = "only-a-test-signature-secret"


@asynccontextmanager
async def onebot_server(self_id="10001"):
    state = {"requests": [], "sends": [], "unknown_next": False, "self_id": self_id}
    tasks = set()

    async def handle(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            header = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1").split("\r\n")
            headers = dict(line.lower().split(": ", 1) for line in header[1:] if ": " in line)
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            action = header[0].split(" ")[1]
            state["requests"].append(action)
            assert headers["authorization"] == "bearer " + TOKEN
            if action == "/get_login_info":
                data = {"user_id": int(state["self_id"]), "nickname": "Test OneBot"}
            else:
                assert action in {"/send_group_msg", "/send_private_msg"}
                state["sends"].append(body)
                if state["unknown_next"]:
                    state["unknown_next"] = False
                    return  # Actual body arrived; intentionally lose its response.
                data = {"message_id": 8000 + len(state["sends"])}
            payload = json.dumps({"status": "ok", "retcode": 0, "data": data}).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                         + str(len(payload)).encode() + b"\r\n\r\n" + payload)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    state["origin"] = "http://127.0.0.1:" + str(server.sockets[0].getsockname()[1])
    try:
        yield state
    finally:
        server.close()
        await server.wait_closed()
        for task in tuple(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def wait_for(read, condition, timeout=30):
    value = None
    try:
        async with asyncio.timeout(timeout):
            while True:
                value = await read()
                if condition(value):
                    return value
                await asyncio.sleep(0.05)
    except TimeoutError:
        pytest.fail(f"Expected state did not arrive: {value}")


async def get(client, path):
    response = await client.get(path)
    assert response.status_code == 200, response.text
    return response.json()


async def online(client, path, revision=1):
    return await wait_for(lambda: get(client, path + "/configuration"),
                          lambda row: row["state"] == "ONLINE" and row["effective_revision"] == revision)


async def receive(client, binding_id, message_id, text, *, user_id=20001, group_id=30001,
                  secret=SECRET, self_id=10001, mention=False):
    segments = [{"type": "text", "data": {"text": text}}]
    if mention:
        segments.insert(0, {"type": "at", "data": {"qq": str(self_id)}})
    raw = json.dumps({"time": 1700000000 + message_id, "self_id": self_id,
        "post_type": "message", "message_type": "group", "sub_type": "normal",
        "message_id": message_id, "user_id": user_id, "group_id": group_id,
        "message": segments, "sender": {"user_id": user_id, "nickname": "用户" + str(user_id),
                                         "card": "群名片", "role": "member"}}, ensure_ascii=False).encode()
    return await client.post("/adapters/onebot/v11/" + binding_id, content=raw,
        headers={"content-type": "application/json", "x-signature": "sha1=" + hmac.new(secret.encode(), raw, hashlib.sha1).hexdigest()})


async def configure(client, base, origin, *, revision=0, key="initial", **changes):
    values = {"enabled": True, "api_base_url": origin, "response_mode": "component",
              "response_template": "{sender}@{group}:{text}", "admin_user_ids": ["20001"], **changes}
    body = {"expected_revision": revision, "values": values, "access_token": TOKEN, "webhook_secret": SECRET}
    plan = await post(client, base + "/change-plans", body, key + "-plan")
    assert TOKEN not in json.dumps(plan) and SECRET not in json.dumps(plan)
    assert await post(client, base + "/change-plans", body, key + "-plan") == plan
    accepted = await post(client, base + "/management-requests",
                          {k: plan[k] for k in ("plan_id", "plan_digest", "expected_revision")}, key + "-apply", status=202)
    assert accepted["revision"] == revision + 1
    return await online(client, base, revision + 1)


async def test_web_binding_ingress_context_delivery_unknown_and_restart(tmp_path, web_bots_database):  # noqa: F811
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with onebot_server() as server:
        async with running(settings) as (app, client):
            await setup(client, token)
            bot = await post(client, "/management/v1/bots", {"display_name": "接入测试"}, "bot", status=202)
            await ready(client, bot)
            root = "/management/v1/bots/" + bot["bot_id"]
            payload = {"self_id": "10001", "display_name": "测试QQ"}
            binding = await post(client, root + "/bindings", payload, "binding", status=201)
            assert await post(client, root + "/bindings", payload, "binding", status=201) == binding
            binding_id = binding["binding_id"]
            base = root + "/bindings/" + binding_id
            row = await configure(client, base, server["origin"])
            assert row["webhook_url"].endswith(binding_id)
            assert TOKEN not in json.dumps(row) and SECRET not in json.dumps(row)
            wrong = await receive(client, binding_id, 1, "do not store", secret="wrong")
            assert wrong.status_code in {400, 401, 403}
            assert (await get(client, root + "/conversations"))["conversations"] == []
            assert (await receive(client, binding_id, 2, "ordinary message")).status_code == 200
            queues = await wait_for(lambda: get(client, root + "/conversations"),
                lambda value: value["conversations"] and value["conversations"][0]["context_applied_seq"] == 1)
            assert queues["conversations"][0]["pending_triggers"] == 0
            assert server["sends"] == []
            assert (await receive(client, binding_id, 3, "answer this", mention=True)).status_code == 200
            first = await wait_for(lambda: get(client, root + "/delivery"),
                                  lambda value: value["parts"] and value["parts"][0]["state"] == "SENT")
            assert first["parts"][0]["reply_route"]["channel_id"] == "30001"
            assert first["parts"][0]["payload_state"] == "ACTIVE"
            assert first["parts"][0]["payload"] == [{"type": "text", "text": "群名片@30001:answer this"}]
            assert len(server["sends"]) == 1
            assert server["sends"][0]["message"][0]["data"]["text"] == "群名片@30001:answer this"
            assert (await receive(client, binding_id, 3, "answer this", mention=True)).status_code == 200
            await asyncio.sleep(0.3)
            assert len(server["sends"]) == 1
            # Exact platform receipt makes a self echo an alias of the canonical reply.
            assert (await receive(client, binding_id, 8001, "self reply", user_id=10001)).status_code == 200
            assert (await get(client, root + "/conversations"))["conversations"][0]["next_ingress_seq"] == 3
            server["unknown_next"] = True
            assert (await receive(client, binding_id, 4, "/chat uncertain")).status_code == 200
            uncertain = await wait_for(lambda: get(client, root + "/delivery"),
                                      lambda value: any(p["state"] == "UNKNOWN" for p in value["parts"]))
            unknown = next(p for p in uncertain["parts"] if p["state"] == "UNKNOWN")
            assert len(server["sends"]) == 2
            assert (await receive(client, binding_id, 5, "/chat later")).status_code == 200
            await wait_for(lambda: get(client, root + "/delivery"), lambda value: len(value["parts"]) == 3)
            await asyncio.sleep(0.3)
            assert len(server["sends"]) == 2
            resolved = await post(client, root + f"/delivery/{unknown['intent_id']}/0/resolution",
                {"expected_revision": unknown["revision"], "decision": "SKIP", "reason": "Verified original target; skip uncertain part"}, "resolve")
            assert resolved["state"] == "SKIPPED"
            await wait_for(lambda: get(client, root + "/delivery"),
                           lambda value: sum(p["state"] == "SENT" for p in value["parts"]) == 2)
            assert len(server["sends"]) == 3
            # Pause keeps durable intake alive and ordinary Context waits for resume.
            managed = await app.state.runtime.fleet.attach(bot["bot_id"])
            state = await managed.store.runtime()
            await managed.store.set_maintenance(managed.kernel._authority(), state["maintenance_epoch"], False)
            assert (await receive(client, binding_id, 6, "while paused", group_id=30002)).status_code == 200
            paused = await get(client, root + "/conversations")
            assert any(c["channel_id"] == "30002" for c in paused["conversations"])
            state = await managed.store.runtime()
            await managed.store.set_maintenance(managed.kernel._authority(), state["maintenance_epoch"], True)
            await wait_for(lambda: get(client, root + "/conversations"),
                           lambda value: all(c["context_applied_seq"] == c["next_ingress_seq"] - 1 for c in value["conversations"]))
        async with running(settings) as (app, client):
            await login(client)
            await online(client, base)
            assert len((await get(client, root + "/bindings"))["bindings"]) == 1
            assert (await receive(client, binding_id, 7, "/chat after restart", group_id=30002)).status_code == 200
            await wait_for(lambda: get(client, root + "/delivery"),
                           lambda value: sum(p["state"] == "SENT" for p in value["parts"]) == 3)
            assert len(server["sends"]) == 4
            managed = await app.state.runtime.fleet.attach(bot["bot_id"])
            async with managed.store.pool.connection() as conn:
                source = await (await conn.execute(
                    "SELECT o.source_unit_id FROM public.bot_memory_source_occurrences o "
                    "JOIN public.bot_message_inbox i USING(bot_id,event_id) "
                    "WHERE i.bot_id=%s AND o.binding_id=%s AND i.platform_message_id='3'",
                    (bot["bot_id"], binding_id),
                )).fetchone()
            payloads = PayloadStore(managed.store)
            lease = managed.kernel._authority()
            await payloads.forget_sources(lease, "remove-first-web-message", (SourceRef(source["source_unit_id"]),),
                expected_deletion_epoch=await payloads.epoch(lease))
            for _ in range(2):
                status = await get(client, root + "/delivery")
                original = next(part for part in status["parts"] if part["intent_id"] == first["parts"][0]["intent_id"])
                assert original["state"] == "SENT" and original["receipt"]["message_id"] == "8001"
                assert original["payload"] is None and original["payload_state"] == "UNAVAILABLE"
                assert "answer this" not in json.dumps(status) and len(server["sends"]) == 4
                while (await payloads.scrub(lease))["remaining"]:
                    pass


async def test_binding_registration_recovers_cross_database_ack_and_account_isolation(
    tmp_path, web_bots_database, monkeypatch,  # noqa: F811
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        bots = [await post(client, "/management/v1/bots", {"display_name": "Bot " + str(n)},
                           "create-" + str(n), status=202) for n in range(2)]
        await asyncio.gather(*(ready(client, bot) for bot in bots))
        roots = ["/management/v1/bots/" + bot["bot_id"] for bot in bots]
        original = BindingCatalog.registered
        failed = False

        async def lost_ack(self, **kwargs):
            nonlocal failed
            if not failed:
                failed = True
                raise ForestError("REGISTRY_UNAVAILABLE", "Synthetic acknowledgement loss", 503)
            return await original(self, **kwargs)

        monkeypatch.setattr(BindingCatalog, "registered", lost_ack)
        body = {"self_id": "10001", "display_name": "不可重复归属"}
        result = await client.post(roots[0] + "/bindings", json=body, headers={"idempotency-key": "stable-create"})
        assert result.status_code == 503
        rows = (await get(client, roots[0] + "/bindings"))["bindings"]
        assert len(rows) == 1 and rows[0]["registration_stage"] == "RESERVED"
        assert rows[0]["state"] == "DISABLED"  # Target committed, directory has not acknowledged it.
        restored = await post(client, roots[0] + "/bindings", body, "stable-create", status=201)
        assert restored["binding_id"] == rows[0]["binding_id"]
        assert (await get(client, roots[0] + "/bindings"))["bindings"][0]["registration_stage"] == "REGISTERED"
        assert (await get(client, roots[1] + "/bindings"))["bindings"] == []
        duplicate = await post(client, roots[1] + "/bindings", body, "other-owner", status=409)
        assert duplicate["code"] == "ACCOUNT_ALREADY_BOUND"
        changed = await post(client, roots[0] + "/bindings", {**body, "self_id": "10002"}, "stable-create", status=409)
        assert changed["code"] == "IDEMPOTENCY_CONFLICT"
        cross = await client.get(roots[1] + "/bindings/" + rows[0]["binding_id"] + "/configuration")
        assert cross.status_code == 404
        await post(client, "/management/v1/logout", {}, "logout")
        denied = await client.post(roots[0] + "/bindings", json={**body, "self_id": "10003"},
                                   headers={"idempotency-key": "after-logout"})
        assert denied.status_code == 401


async def test_binding_web_plan_cas_secret_replay_and_identity_probe(tmp_path, web_bots_database):  # noqa: F811
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with onebot_server() as server, running(settings) as (_, client):
        await setup(client, token)
        bot = await post(client, "/management/v1/bots", {"display_name": "配置竞争"}, "create", status=202)
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        binding = await post(client, root + "/bindings", {"self_id": "10001", "display_name": "账号"}, "create-binding", status=201)
        base = root + "/bindings/" + binding["binding_id"]
        await configure(client, base, server["origin"])
        body = {"expected_revision": 1, "values": {"enabled": True, "api_base_url": server["origin"],
            "response_mode": "component", "response_template": "changed"}}
        a, b = await asyncio.gather(post(client, base + "/change-plans", body, "a"),
                                    post(client, base + "/change-plans", body, "b"))
        apply_body = {k: a[k] for k in ("expected_revision", "plan_id", "plan_digest")}
        result = await post(client, base + "/management-requests", apply_body, "apply-a", status=202)
        assert await post(client, base + "/management-requests", apply_body, "apply-a", status=202) == result
        conflict = await post(client, base + "/management-requests",
            {k: b[k] for k in apply_body}, "apply-b", status=409)
        assert conflict["code"] == "REVISION_CONFLICT"
        await online(client, base, 2)
        assert await post(client, base + "/change-plans", body, "a") == a
        server["self_id"] = "10002"
        await post(client, base + "/change-plans", {"expected_revision": 2, "values": body["values"]}, "wrong-identity-plan")
        wrong = await post(client, base + "/change-plans", {"expected_revision": 2, "values": body["values"]}, "wrong-identity-plan")
        await post(client, base + "/management-requests", {k: wrong[k] for k in apply_body}, "wrong-identity-apply", status=202)
        row = await wait_for(lambda: get(client, base + "/configuration"),
                             lambda value: value["error_code"] == "ONEBOT_IDENTITY_MISMATCH")
        assert row["revision"] == 3 and row["effective_revision"] == 2
        assert row["self_id"] == "10001"

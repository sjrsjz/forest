"""Real PostgreSQL + native Worker + loopback HTTP platform lifecycle tests."""

import asyncio
import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from test_bindings import VALUES, BindingEnvironment
from test_onebot import encoded, event, response, signature, socket_api
from test_web_authority import environment  # noqa: F401

import forest
from forest.errors import ForestError
from forest.kernel import Kernel
from forest.management.fleet import BotFleet
from forest.packages import PackageCatalog
from forest.platform import BindingRuntime
from forest.shell_ingress import ShellIngressGuard
from forest.storage.input_payloads import InputContentStore

pytestmark = pytest.mark.postgres
WEBHOOK = "webhook-test-secret"


@pytest_asyncio.fixture
async def platform(environment, tmp_path):  # noqa: F811
    env = environment
    await env.store.release_host(env.lease)
    kernel = Kernel(env.store, lease_seconds=30)
    await kernel.start()
    env.lease = kernel.lease
    packages = {}
    catalog = PackageCatalog(tmp_path / "packages")
    for name in ("onebot", "reply"):
        snapshot = catalog.build(Path(forest.__file__).parent / "builtin" / name)
        packages[snapshot.component["id"]] = snapshot
    api_response = response(
        {
            "status": "ok",
            "retcode": 0,
            "data": {
                "user_id": 10001,
                "nickname": "Fake OneBot",
                "message_id": 998,
            },
        }
    )
    async with socket_api(api_response) as (origin, requests, _):
        binding = BindingEnvironment(env)
        binding.secret = env.vault.encrypt(
            json.dumps(
                {
                    "access_token": "api-test-token",
                    "webhook_secret": WEBHOOK,
                }
            )
        )
        await binding.create()
        values = {
            **VALUES,
            "api_base_url": origin,
            "response_mode": "component",
            "response_template": "{sender}@{group}:{text}",
            "admin_user_ids": ["20001"],
        }
        await binding.apply(await binding.plan(values=values))
        runtime = BindingRuntime(
            SimpleNamespace(store=env.store, kernel=kernel), env.vault, packages
        )
        row = (await runtime.bindings.desired(kernel._authority()))[0]
        try:
            yield SimpleNamespace(
                env=env,
                kernel=kernel,
                runtime=runtime,
                binding=binding,
                desired=row,
                requests=requests,
            )
        finally:
            await runtime.close()
            await kernel.close()


async def receive(p, number=1, **changes):
    body = encoded(event(message_id=number, **changes))
    return await p.runtime.receive(p.binding.binding_id, body, signature(body, WEBHOOK))


async def wait_for(predicate, timeout=10):
    async with asyncio.timeout(timeout):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.02)


async def test_signed_all_message_ingress_keeps_authors_and_pending_image_reference(platform):
    p = platform
    await p.runtime._replace(p.desired)
    first = await receive(p, 1)
    duplicate = await receive(p, 1)
    assert duplicate["accepted"] and duplicate["kind"] == "DUPLICATE"
    assert duplicate["event_id"] == first["event_id"]
    await receive(
        p,
        2,
        user_id=20002,
        sender={"nickname": "同名用户"},
        message=[
            {"type": "text", "data": {"text": "图片不是网址文本"}},
            {
                "type": "image",
                "data": {"file": "opaque.image", "url": "https://media.test/x?secret=private"},
            },
        ],
    )
    async with p.env.store.pool.connection() as conn, conn.transaction():
        stored = await (
            await conn.execute("SELECT * FROM public.bot_message_inbox ORDER BY ingress_seq")
        ).fetchall()
        content = InputContentStore(p.env.store)
        clock = await content.payloads.lock_in(conn)
        assert all(row["segments"] is None and row["payload_id"] for row in stored)
        rows = [await content.read_input_in(conn, row["event_id"],
            expected_deletion_epoch=clock["deletion_epoch"], required=True) for row in stored]
        raw_media = await (await conn.execute("SELECT * FROM public.bot_inbound_media_refs")).fetchone()
        assert raw_media["source"] is None and raw_media["source_payload_id"]
        media = await content.read_media_source_in(conn, raw_media["media_ref"],
            expected_deletion_epoch=clock["deletion_epoch"])
    assert len(rows) == 2 and [r["ingress_seq"] for r in rows] == [1, 2]
    assert [r["author"]["external_user_id"] for r in rows] == ["20001", "20002"]
    assert rows[0]["author"]["principal_ref"] != rows[1]["author"]["principal_ref"]
    assert all(r["trigger_decision"] == "NO_TRIGGER" for r in rows)
    assert rows[1]["segments"][1] == {
        "type": "image",
        "media_ref": media["media_ref"],
        "fetch_status": "PENDING_FETCH",
    }
    assert "https://media.test" not in json.dumps(rows[1]["segments"])
    assert media["status"] == "PENDING_FETCH" and "https://media.test" in json.dumps(
        media["source"]
    )
    bad = encoded(event(post_type="meta_event", self_id=99999))
    with pytest.raises(ForestError):
        await p.runtime.receive(p.binding.binding_id, bad, signature(bad, WEBHOOK))


@pytest.mark.parametrize("empty_message", ["", []], ids=["empty-cq", "empty-array"])
async def test_empty_protocol_message_is_durably_retained_with_author(platform, empty_message):
    p = platform
    await p.runtime._replace(p.desired)
    accepted = await receive(p, 1, message=empty_message)
    duplicate = await receive(p, 1, message=empty_message)
    assert accepted["accepted"] and duplicate["kind"] == "DUPLICATE"
    assert duplicate["event_id"] == accepted["event_id"]
    async with p.env.store.pool.connection() as conn, conn.transaction():
        rows = await (await conn.execute("SELECT * FROM public.bot_message_inbox")).fetchall()
        assert len(rows) == 1 and rows[0]["segments"] is None and rows[0]["payload_id"]
        content = InputContentStore(p.env.store)
        clock = await content.payloads.lock_in(conn)
        restored = await content.read_input_in(conn, rows[0]["event_id"],
            expected_deletion_epoch=clock["deletion_epoch"], required=True)
    assert len(rows) == 1 and rows[0]["ingress_seq"] == 1
    assert restored["segments"] == [{"type": "text", "text": ""}]
    assert rows[0]["author"]["external_user_id"] == "20001"
    actor = await p.runtime.messages.acquire_actor(
        p.kernel._authority(), rows[0]["conversation_id"], "empty-message-reader"
    )
    selected = await p.runtime.messages.select_input(actor, "empty-message-silent-drain")
    context = await p.runtime.messages.context(actor)
    assert selected["kind"] == "SILENT_DRAIN" and context["context_applied_seq"] == 1
    assert context["units"][0]["segments"] == [{"type": "text", "text": ""}]
    assert context["units"][0]["author"]["external_user_id"] == "20001"


async def test_failed_codec_attempt_and_lost_ingress_ack_do_not_poison_retries(
    platform, monkeypatch
):
    p = platform
    await p.runtime._replace(p.desired)
    invoke = p.kernel.invoke_control_with_receipt
    attempts = []

    async def fail_once(component, service, payload, **kwargs):
        attempts.append(kwargs["operation_id"])
        if len(attempts) == 1:
            payload = {**payload, "body_b64": base64.b64encode(b"invalid-json").decode()}
        return await invoke(component, service, payload, **kwargs)

    monkeypatch.setattr(p.kernel, "invoke_control_with_receipt", fail_once)
    with pytest.raises(ForestError):
        await receive(p)
    ingest = p.runtime.messages.ingest

    async def lost_ack(*args, **kwargs):
        await ingest(*args, **kwargs)
        raise ConnectionError("synthetic lost database acknowledgement")

    monkeypatch.setattr(p.runtime.messages, "ingest", lost_ack)
    with pytest.raises(ConnectionError):
        await receive(p)
    monkeypatch.setattr(p.runtime.messages, "ingest", ingest)
    assert (await receive(p))["accepted"]
    # The retry after a committed Inbox result uses its durable metadata ACK;
    # it no longer calls the reloadable codec for an already accepted event.
    assert len(attempts) == len(set(attempts)) == 2
    async with p.env.store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_message_inbox")
            ).fetchone()
        )["n"] == 1
    calls = [await p.env.store.read_call(operation) for operation in attempts]
    assert [call["status"] for call in calls] == ["FAILED", "COMPLETED"]


async def test_preparation_uses_actual_completed_worker_generation_after_reload(
    platform, monkeypatch
):
    p = platform
    await p.runtime._replace(p.desired)
    before = p.kernel.components["adapter.onebot"].worker.identity.generation_id
    invoke = p.kernel.invoke_control_with_receipt

    async def reload_before_call(*args, **kwargs):
        await p.kernel.install(
            p.runtime.packages["adapter.onebot"], operation_id="replace-before-format"
        )
        return await invoke(*args, **kwargs)

    monkeypatch.setattr(p.kernel, "invoke_control_with_receipt", reload_before_call)
    row = p.runtime.connection(p.binding.binding_id).row
    await p.runtime.delivery.enqueue(
        p.kernel._authority(), operation_id="reload-format-source",
        route={"bot_id": p.env.store.bot_id, "binding_id": p.binding.binding_id,
               "platform": "onebot-v11", "self_id": row["self_id"],
               "channel_kind": "group", "channel_id": "30001", "thread_id": "",
               "binding_epoch": row["binding_epoch"], "visibility": "group"},
        parts=[[{"type": "text", "text": "literal [CQ:at,qq=all]"}]],
        conversation_id="format-conversation", response_seq=1,
    )
    claim = await p.runtime.delivery.claim(p.kernel._authority(), "format-worker")
    prepared = await p.runtime.prepare(claim)
    current = p.kernel.components["adapter.onebot"].worker.identity.generation_id
    assert prepared.executor_ref["generation_id"] == current != before
    assert prepared.executor_ref["admission_lane"] == "CONTROL"
    assert all(request[0] == "POST /get_login_info HTTP/1.1" for request in p.requests)
    body = json.loads(prepared.prepared.wire_body)
    assert body["message"][0]["type"] == "text"


async def test_activation_lost_ack_adopts_committed_epoch_and_close_disconnects(
    platform, monkeypatch
):
    p = platform
    activate = p.runtime.bindings.activate

    async def lost_ack(*args, **kwargs):
        await activate(*args, **kwargs)
        raise ConnectionError("lost acknowledgement")

    monkeypatch.setattr(p.runtime.bindings, "activate", lost_ack)
    await p.runtime._replace(p.desired)
    connection = p.runtime.connection(p.binding.binding_id)
    assert connection.row["connection_epoch"] == 1 and not connection.client._client.is_closed
    assert not p.runtime._pending
    assert (await receive(p))["accepted"]
    await p.runtime.close()
    assert connection.client._client.is_closed and not p.runtime._clients
    assert (await p.binding.read())["state"] == "OFFLINE"


async def test_cancel_after_publication_keeps_owner_until_runtime_close(platform, monkeypatch):
    p = platform
    published = asyncio.Event()
    activate = p.runtime.bindings.activate

    async def pause_after_commit(*args, **kwargs):
        await activate(*args, **kwargs)
        published.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(p.runtime.bindings, "activate", pause_after_commit)
    task = asyncio.create_task(p.runtime._replace(p.desired))
    p.runtime._jobs[p.binding.binding_id] = task
    await asyncio.wait_for(published.wait(), 10)
    await p.runtime.close()
    assert task.cancelled()
    assert not p.runtime._pending and not p.runtime._clients and not p.runtime.connections
    assert (await p.binding.read())["state"] == "OFFLINE"


async def test_unknown_publication_retains_client_and_cleanup_retries_after_store_returns(
    platform, monkeypatch
):
    p = platform
    activate, current = p.runtime.bindings.activate, p.runtime.bindings.current

    async def lost_ack(*args, **kwargs):
        await activate(*args, **kwargs)
        raise ConnectionError("lost commit acknowledgement")

    async def unavailable(*args, **kwargs):
        raise ConnectionError("database unavailable")

    monkeypatch.setattr(p.runtime.bindings, "activate", lost_ack)
    monkeypatch.setattr(p.runtime.bindings, "current", unavailable)
    with pytest.raises(ConnectionError):
        await p.runtime._replace(p.desired)
    assert p.runtime.failure.code == "BINDING_PUBLICATION_UNCERTAIN"
    assert len(p.runtime._pending) == len(p.runtime._clients) == 1
    client = next(iter(p.runtime._clients))
    assert not client._client.is_closed
    with pytest.raises(ForestError, match="remain owned"):
        await p.runtime.close()
    assert client in p.runtime._clients
    monkeypatch.setattr(p.runtime.bindings, "current", current)
    await p.runtime.close()
    assert client._client.is_closed and not p.runtime._pending
    assert (await p.binding.read())["state"] == "OFFLINE"


async def test_cancel_during_identity_probe_closes_unpublished_candidate(platform, monkeypatch):
    p = platform
    entered = asyncio.Event()

    async def probe(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    from forest.adapters.onebot import OneBotHTTPClient

    monkeypatch.setattr(OneBotHTTPClient, "get_login_info", probe)
    task = asyncio.create_task(p.runtime._replace(p.desired))
    await asyncio.wait_for(entered.wait(), 10)
    clients = list(p.runtime._clients)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(clients) == 1 and clients[0]._client.is_closed
    assert not p.runtime._clients and not p.runtime._pending
    assert (await p.binding.read())["effective_revision"] == 0


async def test_supervisor_failure_is_visible_to_fleet_and_closes_local_admission(
    platform, monkeypatch
):
    p = platform
    await p.runtime._replace(p.desired)

    async def broken(*args):
        raise RuntimeError("synthetic supervisor defect")

    monkeypatch.setattr(p.runtime.bindings, "desired", broken)
    await p.runtime.start()

    async def failed():
        return p.runtime.failure

    assert (await wait_for(failed)).code == "BINDING_SUPERVISOR_FAILED"
    bot = SimpleNamespace(ready=True, cleanup_task=None, platform=p.runtime, kernel=p.kernel)
    assert not BotFleet._usable(bot)
    with pytest.raises(ForestError):
        p.runtime.connection(p.binding.binding_id)


async def test_reply_component_failure_does_not_interrupt_protocol_worker(platform, monkeypatch):
    p = platform
    await p.runtime._replace(p.desired)
    adapter = p.kernel.components["adapter.onebot"].worker
    install = p.kernel.install

    async def broken_reply(snapshot, **kwargs):
        if snapshot.component["id"] == "forest.reply":
            raise ForestError("SYNTHETIC_REPLY_FAILURE", "reply failed")
        return await install(snapshot, **kwargs)

    monkeypatch.setattr(p.kernel, "install", broken_reply)
    with pytest.raises(ForestError):
        await p.runtime.ensure_components()
    assert (await receive(p))["accepted"]
    assert p.kernel.components["adapter.onebot"].worker is adapter and adapter.alive
    monkeypatch.setattr(p.kernel, "install", install)
    await p.runtime.ensure_components(("forest.reply",))
    assert "forest.reply" not in p.runtime.errors


async def test_runtime_close_survives_waiter_cancellation_without_abandoning_client(
    platform, monkeypatch
):
    p = platform
    await p.runtime._replace(p.desired)
    connection = p.runtime.connection(p.binding.binding_id)
    close = connection.client.aclose
    entered, release = asyncio.Event(), asyncio.Event()
    count = 0

    async def blocked():
        nonlocal count
        count += 1
        entered.set()
        await release.wait()
        await close()

    monkeypatch.setattr(connection.client, "aclose", blocked)
    waiter = asyncio.create_task(p.runtime.close())
    await asyncio.wait_for(entered.wait(), 5)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not p.runtime._close_task.done() and connection.client in p.runtime._clients
    assert (await p.binding.read())["state"] == "OFFLINE"
    release.set()
    await p.runtime.close()
    assert count == 1 and not p.runtime._clients


async def test_native_worker_group_reply_and_shell_control_continue_independently_when_paused(
    platform,
):
    p = platform
    await p.runtime._replace(p.desired)
    await p.runtime.start()
    await receive(
        p,
        1,
        message=[
            {"type": "at", "data": {"qq": "10001"}},
            {"type": "text", "data": {"text": "你好"}},
        ],
    )

    async def sent(lane):
        async with p.env.store.pool.connection() as conn:
            return await (
                await conn.execute(
                    "SELECT i.* FROM public.bot_send_intents i JOIN public.bot_send_parts p USING(bot_id,intent_id) "
                    "WHERE i.send_lane=%s AND p.state='SENT'",
                    (lane,),
                )
            ).fetchone()

    await wait_for(lambda: sent("CHAT"))
    assert any(
        "你好" in request[2].decode() for request in p.requests if "send_group_msg" in request[0]
    )
    await p.env.store.set_maintenance(p.kernel._authority(), 1, False)
    await receive(p, 2, message="#forest status")
    control = await wait_for(lambda: sent("CONTROL"))
    assert control["send_lane"] == "CONTROL" and p.runtime.failure is None
    async with p.env.store.pool.connection() as conn:
        request = await (await conn.execute("SELECT * FROM public.bot_shell_requests")).fetchone()
        inbox = await (
            await conn.execute(
                "SELECT trigger_decision FROM public.bot_message_inbox ORDER BY ingress_seq"
            )
        ).fetchall()
    assert request["status"] == "COMPLETED"
    assert [r["trigger_decision"] for r in inbox] == ["TRIGGER", "CONTROL_COMMAND"]


async def test_reserved_shell_ingress_survives_a_broken_reloadable_adapter(
    platform, monkeypatch
):
    p = platform
    await p.runtime._replace(p.desired)

    async def broken_adapter(*args, **kwargs):
        raise ForestError("BROKEN_ADAPTER", "synthetic codec failure", 503)

    monkeypatch.setattr(p.kernel, "invoke_control_with_receipt", broken_adapter)
    assert (await receive(p, 31, message="#forest core status"))["accepted"]
    with pytest.raises(ForestError) as caught:
        await receive(p, 32, message="ordinary input")
    assert caught.value.code == "BROKEN_ADAPTER"
    async with p.env.store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT platform_message_id,trigger_decision FROM public.bot_message_inbox"
            )
        ).fetchall()
    assert [tuple(row.values()) for row in rows] == [("31", "CONTROL_COMMAND")]


async def test_rejection_budget_returns_429_before_raw_payload_or_inbox(platform):
    p = platform
    await p.runtime._replace(p.desired)
    p.runtime.shell_ingress = ShellIngressGuard(
        p.env.store.bot_id, rejection_burst=1, rejection_refill_seconds=60
    )
    outsider = {"user_id": 29999, "nickname": "outsider", "role": "member"}
    assert (
        await receive(p, 41, user_id=29999, sender=outsider, message="#forest help")
    )["accepted"]
    async with p.env.store.pool.connection() as conn:
        before_payloads = await (
            await conn.execute("SELECT count(*) AS n FROM public.bot_payload_headers")
        ).fetchone()
    with pytest.raises(ForestError) as caught:
        await receive(p, 42, user_id=29999, sender=outsider, message="#forest help")
    assert caught.value.code == "SHELL_REJECTION_OVERLOADED"
    assert caught.value.status == 429
    async with p.env.store.pool.connection() as conn:
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_payload_headers) AS payloads,"
                "(SELECT count(*) FROM public.bot_message_inbox) AS inbox,"
                "(SELECT count(*) FROM public.bot_shell_requests) AS shells"
            )
        ).fetchone()
    assert counts == {"payloads": before_payloads["n"], "inbox": 1, "shells": 1}
    stats = p.runtime.shell_ingress.snapshot()
    assert stats["rejection_budget"]["rate_limited"] == 1

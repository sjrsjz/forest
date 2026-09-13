"""Real Binding configuration bodies, deletion, connection I/O and recovery."""

import asyncio
import json

import psycopg
import pytest
from test_bindings import VALUES, BindingEnvironment
from test_bindings import binding as binding
from test_onebot import encoded, event, signature
from test_platform import WEBHOOK, receive
from test_platform import platform as platform
from test_web_authority import environment as environment

from forest.adapters.onebot import prepare_send
from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def rows(b, query, params=()):
    async with b.env.store.pool.connection() as conn:
        return await (await conn.execute(query, params)).fetchall()


async def erase(b, operation, *, name=None):
    source = (
        await rows(
            b,
            "SELECT source_unit_id FROM public.bot_payload_domains "
            "WHERE kind='CONTROL_INPUT' AND owner_id=%s",
            ("input:" + digest_json([operation]),),
        )
    )[0]
    payloads = PayloadStore(b.env.store)
    async with b.env.store.pool.connection() as conn:
        epoch = (
            await (
                await conn.execute("SELECT deletion_epoch FROM public.bot_memory_clock")
            ).fetchone()
        )["deletion_epoch"]
    result = await payloads.forget_sources(
        b.env.lease,
        name or "erase:" + operation,
        (SourceRef(source["source_unit_id"]),),
        expected_deletion_epoch=epoch,
    )
    while (await payloads.scrub(b.env.lease, limit=256))["remaining"]:
        pass
    return result


async def rejected(code, operation):
    with pytest.raises(ForestError) as exc:
        await operation
    assert exc.value.code == code


async def test_actual_values_references_and_independent_full_replacement_survive_old_deletion(
    binding,
):
    b = binding
    a = await b.plan(values={**VALUES, "response_template": "OLD-CANARY"}, operation="plan-a")
    await b.apply(a, "apply-a")
    await b.activate()
    new = {**VALUES, "response_template": "INDEPENDENT-NEW"}
    plan = await b.plan(expected=1, values=new, operation="plan-b")
    await b.apply(plan, "apply-b")
    await b.activate(2)
    raw = (await rows(b, "SELECT * FROM public.bot_bindings"))[0]
    revisions = await rows(b, "SELECT * FROM public.bot_binding_revisions ORDER BY revision")
    assert raw["display_name"] is raw["config"] is None
    assert raw["initial_payload_id"] and all(row["values"] is None for row in revisions)
    assert (raw["config_payload_id"], raw["config_payload_digest"]) == (
        revisions[1]["values_payload_id"],
        revisions[1]["values_payload_digest"],
    )
    body = (
        await rows(
            b,
            "SELECT content FROM public.bot_payload_bodies WHERE payload_id=%s",
            (raw["config_payload_id"],),
        )
    )[0]
    wire = bytes(body["content"])
    assert digest_json(json.loads(wire)) == raw["config_payload_digest"]
    assert json.loads(wire) == {
        "family": "binding",
        "kind": "REVISION",
        "resource": b.binding_id,
        "identity": "2",
        "values": new,
    }
    await erase(b, "plan-a")
    assert (await b.read())["config"] == new
    assert (
        await b.bindings.execution_config(
            b.env.lease, b.binding_id, expected_binding_epoch=raw["binding_epoch"]
        )
    )["config"] == new
    assert await b.bindings.credentials(b.env.lease, b.binding_id) == b.secret
    assert not await rows(
        b,
        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
        (revisions[0]["values_payload_id"],),
    )
    with pytest.raises(psycopg.Error):
        async with b.env.store.pool.connection() as conn:
            await conn.execute(
                "UPDATE public.bot_bindings SET initial_payload_id=%s,initial_payload_digest=%s",
                (raw["config_payload_id"], raw["config_payload_digest"]),
            )
    with pytest.raises(psycopg.Error):
        async with b.env.store.pool.connection() as conn:
            await conn.execute("UPDATE public.bot_bindings SET config='{}'::jsonb")


async def test_missing_desired_keeps_independent_effective_and_other_binding_readable(binding):
    b = binding
    await b.apply(await b.plan(operation="good"))
    old = await b.activate()
    await b.apply(await b.plan(1, {**VALUES, "response_template": "bad-new"}, operation="bad"))
    other = BindingEnvironment(b.env)
    await other.create(self_id="10002", operation="other-create")
    await other.apply(await other.plan(operation="other-plan"))
    await other.activate()
    await erase(b, "bad")
    read = await b.read()
    assert read["desired_values"] is None and read["desired_values_payload_state"] == "UNAVAILABLE"
    assert read["config"] == VALUES and read["effective_revision"] == 1 and read["revision"] == 2
    desired = await b.bindings.desired(b.env.lease)
    assert len(desired) == 2 and all(row["config"] is not None for row in desired)
    await b.bindings.disconnect(
        b.env.lease,
        b.binding_id,
        client_incarnation=old["client_incarnation"],
        expected_binding_epoch=old["binding_epoch"],
    )
    recovered = await b.bindings.activate(
        b.env.lease,
        b.binding_id,
        1,
        enabled=True,
        client_incarnation="effective-recovery",
        expected_binding_epoch=old["binding_epoch"],
        recover_effective=True,
        expected_desired_revision=2,
    )
    assert recovered["effective_revision"] == 1 and recovered["revision"] == 2
    await erase(other, "other-create")
    assert (await other.read())["display_name"] is None
    assert (await other.read())["config"] == VALUES
    assert (await b.read())["display_name"] is not None


async def test_deleted_effective_blocks_new_ingress_and_wire_but_known_delivery_can_complete(
    binding,
):
    b = binding
    await b.apply(await b.plan(operation="effective"))
    route = await b.activate()
    delivery = DeliveryStore(b.env.store, b.bindings)
    target = {
        "bot_id": b.env.store.bot_id,
        "binding_id": b.binding_id,
        "platform": "onebot-v11",
        "self_id": "10001",
        "channel_kind": "group",
        "channel_id": "30001",
        "thread_id": "",
        "binding_epoch": route["binding_epoch"],
        "visibility": "group",
    }
    await delivery.enqueue(
        b.env.lease,
        operation_id="send-old",
        route=target,
        parts=[[{"type": "text", "text": "known-send"}]],
        conversation_id="known-conversation",
        response_seq=1,
    )
    claim = await delivery.claim(b.env.lease, "sender")
    wire = prepare_send("group", "30001", claim.payload)
    permit = await delivery.permit(
        b.env.lease,
        claim,
        action=wire.action,
        wire_body=wire.wire_body,
        wire_digest=wire.wire_digest,
        interval_seconds=0,
    )
    parents = await rows(
        b,
        "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
        (permit.wire_payload_id,),
    )
    assert route["config_payload_id"] in {p["parent_payload_id"] for p in parents}
    await delivery.mark_started(permit)
    await erase(b, "effective")
    # A known platform observation must remain recordable; it cannot authorize
    # another HTTP effect or rehydrate the deleted request bytes.
    completed = await delivery.complete(
        permit, "SENT", {"platform_message_id": "known-platform-id"}
    )
    assert completed["state"] == "SENT"
    assert not await rows(
        b, "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s", (permit.wire_payload_id,)
    )
    messages = MessageStore(b.env.store, b.bindings)
    raw = encoded(event(message_id=50))
    await rejected(
        "CONTROL_CONTENT_REVOKED",
        messages.stage_platform_input(
            b.env.lease,
            b.binding_id,
            raw,
            expected_binding_epoch=route["binding_epoch"],
        ),
    )
    await rejected(
        "CONTROL_CONTENT_REVOKED",
        b.bindings.execution_config(
            b.env.lease,
            b.binding_id,
            expected_binding_epoch=route["binding_epoch"],
        ),
    )
    closed = await b.bindings.disconnect(
        b.env.lease,
        b.binding_id,
        client_incarnation=route["client_incarnation"],
        expected_binding_epoch=route["binding_epoch"],
    )
    assert closed["state"] == "OFFLINE"


async def test_runtime_supervisor_closes_revoked_client_without_more_http(platform):
    p = platform
    await p.runtime._replace(p.desired)
    connection = p.runtime.connection(p.binding.binding_id)
    original = len(p.requests)
    plan_op = (
        await rows(
            p.binding,
            "SELECT operation_id FROM public.bot_web_operations WHERE action='binding.plan'",
        )
    )[0]
    await erase(p.binding, plan_op["operation_id"])
    # The surviving Python object still contains old settings. Fresh execution
    # must refuse before codec/HTTP and the actual supervisor must discard it.
    body = encoded(event(message_id=88))
    await rejected(
        "CONTROL_CONTENT_REVOKED",
        p.runtime.receive(p.binding.binding_id, body, signature(body, WEBHOOK)),
    )
    task = asyncio.create_task(p.runtime.run_forever())
    try:
        async with asyncio.timeout(8):
            while not connection.client._client.is_closed:
                await asyncio.sleep(0.02)
        assert not connection.accepting and connection.row["config"] is None
        assert p.binding.binding_id not in p.runtime.connections
        assert len(p.requests) == original
        assert (await p.binding.read())["state"] == "OFFLINE"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_runtime_can_recover_effective_when_newer_desired_body_was_forgotten(platform):
    p = platform
    await p.runtime._replace(p.desired)
    before = p.runtime.connection(p.binding.binding_id)
    await p.binding.apply(
        await p.binding.plan(
            1, {**p.desired["desired_values"], "response_template": "new"}, operation="newer"
        )
    )
    await erase(p.binding, "newer")
    await p.runtime._disconnect(before)
    await before.close()
    p.runtime.connections.pop(p.binding.binding_id)
    p.runtime._clients.discard(before.client)
    p.runtime._applied.pop(p.binding.binding_id)
    desired = (await p.runtime.bindings.desired(p.kernel._authority()))[0]
    await p.runtime._replace(desired)
    after = p.runtime.connection(p.binding.binding_id)
    assert after.row["effective_revision"] == 1 and after.row["revision"] == 2
    assert after.row["binding_epoch"] == before.row["binding_epoch"] + 1
    assert (await receive(p, 99))["accepted"]


async def test_actual_template_worker_result_carries_config_without_tainting_original_input(
    platform,
):
    p = platform
    await p.runtime._replace(p.desired)
    receipt = await receive(
        p,
        111,
        message=[
            {"type": "at", "data": {"qq": "10001"}},
            {"type": "text", "data": {"text": "immutable-user-text"}},
        ],
    )
    messages = p.runtime.messages
    actor = await messages.acquire_actor(
        p.kernel._authority(),
        (
            await rows(
                p.binding,
                "SELECT conversation_id FROM public.bot_message_inbox WHERE event_id=%s",
                (receipt["event_id"],),
            )
        )[0]["conversation_id"],
        "template-author",
        ttl_seconds=120,
    )
    selected = await messages.select_input(actor, "template-input")
    context = await messages.context(actor, run_id=selected["run_id"])
    result = await p.runtime.execute(actor, selected, context)
    current = p.runtime.connection(p.binding.binding_id).row
    call = (
        await rows(
            p.binding,
            "SELECT * FROM public.bot_calls WHERE result_payload_id=%s",
            (result.result_parent_ids[0],),
        )
    )[0]
    parents = await rows(
        p.binding,
        "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
        (call["input_payload_id"],),
    )
    assert current["config_payload_id"] in {r["parent_payload_id"] for r in parents}
    assert "immutable-user-text" in json.dumps(result.parts)
    plan_op = (
        await rows(
            p.binding,
            "SELECT operation_id FROM public.bot_web_operations WHERE action='binding.plan'",
        )
    )[0]
    await erase(p.binding, plan_op["operation_id"])
    stored = (
        await rows(
            p.binding,
            "SELECT h.state,b.content FROM public.bot_message_inbox i "
            "JOIN public.bot_payload_headers h ON h.bot_id=i.bot_id AND h.payload_id=i.payload_id "
            "LEFT JOIN public.bot_payload_bodies b ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id WHERE i.event_id=%s",
            (receipt["event_id"],),
        )
    )[0]
    assert stored["state"] == "ACTIVE" and b"immutable-user-text" in bytes(stored["content"])
    assert not await rows(
        p.binding,
        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=ANY(%s)",
        ([call["input_payload_id"], result.result_parent_ids[0]],),
    )
    await rejected("CONTROL_CONTENT_REVOKED", p.runtime.execute(actor, selected, context))


async def test_sql_metadata_and_missing_plan_receipt_are_rejected(binding):
    b = binding
    plan = await b.plan(operation="real-plan")
    original = (
        await rows(b, "SELECT * FROM public.bot_binding_plans WHERE plan_id=%s", (plan["plan_id"],))
    )[0]
    for values in (("values", "'{}'::jsonb"), ("settings_enabled", "false")):
        with pytest.raises(psycopg.Error):
            async with b.env.store.pool.connection() as conn:
                await conn.execute(
                    "UPDATE public.bot_binding_plans SET " + values[0] + "=" + values[1]
                )
    # A well-formed values payload is not itself a signed plan. The deferred
    # source constraint requires the actual immutable plan/operation bridge.
    with pytest.raises(psycopg.Error, match="BINDING_PLAN_RECEIPT_REQUIRED"):
        async with b.env.store.pool.connection() as conn, conn.transaction():
            await b.bindings._row(conn, b.binding_id)
            await b.env.store._host(conn, b.env.lease)
            ref = await b.bindings.content.put_values_in(
                conn,
                family="binding",
                kind="PLAN",
                resource=b.binding_id,
                identity="no-receipt",
                values=VALUES,
                parents=(original["values_payload_id"],),
                epoch=0,
            )
            await conn.execute(
                "INSERT INTO public.bot_binding_plans(bot_id,binding_id,plan_id,subject_id,issuer_id,"
                "expected_revision,values,secret_ciphertext,plan_digest,expires_at,values_payload_id,values_payload_digest,settings_enabled) "
                "VALUES (%s,%s,'no-receipt',%s,%s,0,NULL,%s,'fake',clock_timestamp()+interval '1 minute',%s,%s,true)",
                (
                    b.env.store.bot_id,
                    b.binding_id,
                    original["subject_id"],
                    original["issuer_id"],
                    b.secret,
                    ref.payload_id,
                    ref.digest,
                ),
            )
    assert len(await rows(b, "SELECT * FROM public.bot_binding_plans")) == 1

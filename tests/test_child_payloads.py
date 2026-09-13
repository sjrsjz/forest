"""Private analysis source closure, exact retries, late costs, and old schemas."""

import asyncio
import hashlib
import json
from importlib.resources import files

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_agent_store import prepare, query, respond, start
from test_children import acquire, family, finish, spec
from test_context_payloads import forget_event
from test_memory_config import environment as environment
from test_memory_media import configure
from test_memory_media import media as media
from test_memory_queries import documents, embed, source
from test_messages import fails, receive
from test_messages import messages as messages
from test_models import completion, server
from test_models import respond as http_respond
from test_p4_upgrade import p3_authority
from test_upgrade import upgrade_database as upgrade_database

from forest.memory_types import SourceRef
from forest.models import ChatCompletionsClient, ModelCapabilities, prepare_model_request
from forest.packages import digest_json
from forest.storage.agent_memory import AgentMemoryStore
from forest.storage.bot import BotStore
from forest.storage.child_payloads import ChildPayloadStore
from forest.storage.children import TOOL_NAME, ChildStore
from forest.storage.delivery import DeliveryStore
from forest.storage.memory_index import MemoryIndexStore
from forest.storage.memory_queries import MemoryQueryStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("after_dispatch", [False, True])
async def test_bound_memory_config_revocation_fences_child_http_and_candidate_even_with_active_payloads(
    media, after_dispatch
):
    env, images, _ = media
    await configure(env, [], max_result_tokens=16000)
    recalled = await source(media, 100, group="another-group", text="PRIVATE-MEMORY-SOURCE")
    index = MemoryIndexStore(images.messages, images)
    queries = MemoryQueryStore(index)
    await documents(media, index, [recalled["source_unit_id"]])
    state, actor, head = await start((images.messages, env.lease, None), spec=spec())
    snapshot = await state.snapshot(actor, head["run_id"])
    memory = AgentMemoryStore(state, queries)
    decision = await memory.begin(actor, snapshot)
    planned = await queries.prepare(
        env.lease,
        1,
        query_id=decision["query_id"],
        conversation_id=actor.conversation_id,
        input_cut=decision["input_cut"],
        source_unit_ids=decision["source_unit_ids"],
    )
    await embed(media, index, planned["facet_ids"], query_receipt=planned)
    await queries.retrieve(env.lease, decision["query_id"])
    bundle, _ = await memory.finish(actor, snapshot)
    assert (
        bundle["evidence"] and bundle["evidence"][0]["source_unit_id"] == recalled["source_unit_id"]
    )
    request = prepare_model_request(
        snapshot,
        ModelCapabilities("parent", "test", "v1"),
        request_id="parent-request",
        transport_attempt_id="parent-attempt",
        evidence=bundle,
        output_tokens=64,
        stream=False,
    )
    permit = await state.prepare_model(actor, head["run_id"], "parent-permit", request)
    await respond(
        state,
        actor,
        permit,
        request,
        calls=[
            {
                "id": "private",
                "name": TOOL_NAME,
                "arguments": {
                    "authority_trigger_id": snapshot["trigger_ids"][0],
                    "arguments": {
                        "task": "Analyze the recalled answer",
                        "source_unit_ids": [snapshot["units"][0]["unit_id"]],
                    },
                },
            }
        ],
    )
    children = ChildStore(state)
    await children.spawn(actor, head["run_id"])
    lease, row, prepared, attempt = await acquire(children, env.lease)
    async with env.store.pool.connection() as conn:
        parents = await (
            await conn.execute(
                "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
                (row["snapshot_payload_id"],),
            )
        ).fetchall()
    assert bundle["payload_id"] in {p["parent_payload_id"] for p in parents}
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, request):
        entered.set()
        await release.wait()
        await http_respond(writer, json.dumps(completion(text="REVOKED-BOUND-CANDIDATE")).encode())

    async with server(handler) as (endpoint, requests):
        client = ChatCompletionsClient(endpoint, "local-test-key")
        task = None
        try:
            if after_dispatch:
                task = asyncio.create_task(
                    client.dispatch(
                        prepared,
                        permitcheck=lambda value: children.check_permit(lease, attempt, value),
                    )
                )
                async with asyncio.timeout(5):
                    await entered.wait()
            await configure(env, [], revision=1, enabled=False)
            # Revocation here changes authorization only, not deletion state.
            async with env.store.pool.connection() as conn:
                assert (
                    await (
                        await conn.execute(
                            "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
                            (row["snapshot_payload_id"],),
                        )
                    ).fetchone()
                )["state"] == "ACTIVE"
            if after_dispatch:
                release.set()
                result = await task
                closed = await children.complete(lease, attempt, result)
                assert closed["status"] == "FAILED" and closed["result_payload_id"] is None
                observation = (await query(state, "SELECT * FROM public.bot_child_model_attempts"))[
                    0
                ]
                assert observation["result_payload_id"] is None
                assert observation["settled"] and observation["charged_tokens"] == 30
                assert "REVOKED-BOUND-CANDIDATE" not in str(observation)
                assert len(requests) == 1
            else:
                await fails(
                    "AGENT_MEMORY_REVOKED",
                    client.dispatch(
                        prepared,
                        permitcheck=lambda value: children.check_permit(lease, attempt, value),
                    ),
                )
                assert requests == [] and not entered.is_set()
            await fails("AGENT_MEMORY_REVOKED", children.adopt(actor, head["run_id"]))
            assert not await query(state, "SELECT * FROM public.bot_conversation_replies")
        finally:
            release.set()
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await client.aclose()


async def erase(children, host, snapshot):
    await forget_event(children.messages, host, {"event_id": snapshot["units"][0]["unit_id"]})


async def scrub(children, host):
    payloads = PayloadStore(children.store)
    while (await payloads.scrub(host))["remaining"]:
        pass


async def test_deleted_child_permit_and_frozen_unsent_retry_cannot_dispatch(messages):
    children, _, _, snapshot = await family(messages)
    lease, row, prepared, attempt = await acquire(children, messages[1])
    await finish(children, lease, prepared, attempt, status="NOT_SENT")
    replacement = await children.claim(messages[1], row["child_id"], "replacement")
    resumed = await children.load(replacement)
    exact = children.replay(resumed)
    assert exact.body == prepared.body and exact.request_id == prepared.request_id
    assert exact.transport_attempt_id != prepared.transport_attempt_id
    second = await children.prepare(replacement, exact)
    await erase(children, messages[1], snapshot)
    await scrub(children, messages[1])
    await fails("AGENT_MEMORY_REVOKED", children.check_permit(replacement, second, exact))
    await fails("AGENT_MEMORY_REVOKED", children.load(replacement))
    attempts = await query(
        children.agent, "SELECT * FROM public.bot_child_model_attempts ORDER BY created_at"
    )
    assert [a["status"] for a in attempts] == ["NOT_SENT", "PERMITTED"]
    assert all(a["request_body"] is None and a["manifest"] is None for a in attempts)
    assert attempts[0]["wire_digest"] == attempts[1]["wire_digest"] == prepared.payload_digest
    assert not await query(
        children.agent,
        "SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) WHERE h.owner_kind='agent.child'",
    )
    async with children.store.pool.connection() as conn, conn.transaction():
        with pytest.raises(psycopg.Error):
            await conn.execute(
                "UPDATE public.bot_child_model_attempts SET request_body=%s", (prepared.body,)
            )


async def test_real_http_result_after_deletion_only_settles_fee_and_never_enters_parent(messages):
    children, actor, head, snapshot = await family(messages)
    lease, _, prepared, attempt = await acquire(children, messages[1])
    entered, release = asyncio.Event(), asyncio.Event()
    received = []

    async def handler(writer, request):
        received.append(request["body"])
        entered.set()
        await release.wait()
        body = completion(text="LATE-CHILD-SECRET")
        body["usage"] = {"prompt_tokens": 6, "completion_tokens": 7, "total_tokens": 13}
        await http_respond(writer, json.dumps(body).encode())

    async with server(handler) as (endpoint, _):
        client = ChatCompletionsClient(endpoint, "test-only-key")
        pending = asyncio.create_task(
            client.dispatch(
                prepared, permitcheck=lambda request: children.check_permit(lease, attempt, request)
            )
        )
        try:
            async with asyncio.timeout(5):
                await entered.wait()
            await erase(children, messages[1], snapshot)
            await scrub(children, messages[1])
            release.set()
            result = await pending
            closed = await children.complete(lease, attempt, result)
            assert closed["status"] == "FAILED" and closed["error_code"] == "AGENT_MEMORY_REVOKED"
            assert received == [prepared.body]
            assert (await children.complete(lease, attempt, result))["result_payload_id"] is None
            row = (await query(children.agent, "SELECT * FROM public.bot_child_model_attempts"))[0]
            assert row["result_payload_id"] is None and row["result"] is None
            assert row["result_digest"] == digest_json(result.as_dict())
            assert row["result_metadata"]["usage"]["total_tokens"] == 13
            assert row["settled"] and row["charged_tokens"] == 13
            assert "LATE-CHILD-SECRET" not in str(row)
            await fails("AGENT_MEMORY_REVOKED", children.adopt(actor, head["run_id"]))
            assert not await query(children.agent, "SELECT * FROM public.bot_conversation_replies")
        finally:
            release.set()
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            await client.aclose()


async def test_adopted_child_lineage_reaches_parent_reply_and_cannot_reappear_in_next_run(messages):
    children, actor, head, snapshot = await family(messages)
    lease, _, prepared, attempt = await acquire(children, messages[1])
    await finish(children, lease, prepared, attempt, text="PRIVATE-CHILD-CANARY")
    await children.adopt(actor, head["run_id"])
    child = (await query(children.agent, "SELECT * FROM public.bot_agent_children"))[0]
    tool = (await query(children.agent, "SELECT * FROM public.bot_agent_tools"))[0]
    parents = await query(
        children.agent,
        "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
        (tool["result_payload_id"],),
    )
    assert child["result_payload_id"] in [r["parent_payload_id"] for r in parents]
    assert child["snapshot"] is child["spec"] is child["result"] is None
    await children.agent.close_tools(actor, head["run_id"])
    next_snapshot, request, permit = await prepare(children.agent, actor, head)
    assert "PRIVATE-CHILD-CANARY" in str(next_snapshot)
    await respond(children.agent, actor, permit, request, text="DERIVED-PARENT-CANARY")
    await children.agent.commit_final(actor, head["run_id"], "reply", DeliveryStore(children.store))
    await erase(children, messages[1], snapshot)
    await scrub(children, messages[1])
    await receive(
        children.messages, messages[1], "new-trigger", trigger=True, text="new visible request"
    )
    batch = await children.messages.select_input(actor, "next-run")
    newer = await children.agent.ensure_run(actor, batch["run_id"], spec())
    frozen = await children.agent.snapshot(actor, newer["run_id"])
    assert "CANARY" not in str(frozen) and "new visible request" in str(frozen)
    assert not await query(
        children.agent,
        "SELECT * FROM public.bot_agent_children WHERE parent_run_id=%s",
        (newer["run_id"],),
    )


async def test_child_output_and_frozen_payload_references_are_immutable(messages):
    children, _, _, _ = await family(messages)
    lease, _, prepared, attempt = await acquire(children, messages[1])
    await finish(children, lease, prepared, attempt)
    for statement in (
        "UPDATE public.bot_agent_children SET snapshot='{}'::jsonb",
        "UPDATE public.bot_agent_children SET spec='{}'::jsonb",
        "UPDATE public.bot_agent_children SET snapshot_payload_id=spec_payload_id",
        "UPDATE public.bot_agent_children SET result_payload_id=NULL",
        "UPDATE public.bot_child_model_attempts SET result_payload_id=NULL",
        "UPDATE public.bot_child_model_attempts SET result_metadata='{}'::jsonb",
        "UPDATE public.bot_child_model_attempts SET request_payload_id=manifest_payload_id",
    ):
        async with children.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error):
                await conn.execute(statement)
    async with children.store.pool.connection() as conn, conn.transaction():
        with pytest.raises(psycopg.Error, match="Child wire digest differs"):
            await conn.execute(
                "INSERT INTO public.bot_child_model_attempts(bot_id,attempt_id,child_id,request_id,request_payload_id,wire_digest,manifest_payload_id,metadata,owner_id,lease_epoch,host_incarnation,host_owner_epoch,status,budget_day,reserved_tokens,deadline_at) "
                "SELECT bot_id,'forged-pointer',child_id,request_id,manifest_payload_id,wire_digest,manifest_payload_id,metadata,owner_id,lease_epoch,host_incarnation,host_owner_epoch,'PERMITTED',budget_day,reserved_tokens,deadline_at FROM public.bot_child_model_attempts"
            )


async def test_child_metadata_rejects_untracked_bodies_and_wrong_scalar_types_in_sql(messages):
    children, _, _, _ = await family(messages)
    lease, row, prepared, attempt = await acquire(children, messages[1])
    saved = row["spec_metadata"]
    bad_specs = (
        {**saved, "body": "METADATA-CANARY"},
        {**saved, "config_revision": {"body": "METADATA-CANARY"}},
        {**saved, "depth": "1"},
        {**saved, "values": {**saved["values"], "prompt": "METADATA-CANARY"}},
        {**saved, "values": {**saved["values"], "child_timeout": {"text": "METADATA-CANARY"}}},
        {**saved, "values": {**saved["values"], "child_timeout": True}},
        {**saved, "values": {**saved["values"], "child_timeout": 1.5}},
        {**saved, "values": {**saved["values"], "stream": "METADATA-CANARY"}},
        {**saved, "values": {**saved["values"], "daily_token_budget": 1000000001}},
    )
    for value in bad_specs:
        async with children.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.errors.CheckViolation) as caught:
                await conn.execute(
                    "INSERT INTO public.bot_agent_children SELECT "
                    "(jsonb_populate_record(NULL::public.bot_agent_children, "
                    "to_jsonb(c)||jsonb_build_object('child_id','bad-spec','spec_metadata',%s::jsonb))).* "
                    "FROM public.bot_agent_children c WHERE child_id=%s",
                    (Jsonb(value), row["child_id"]),
                )
            assert caught.value.diag.constraint_name == "bot_child_spec_metadata_closed"
    bad_results = (
        {"status": "RESPONDED", "body": "METADATA-CANARY"},
        {"status": {"text": "METADATA-CANARY"}},
        {"status": "METADATA-CANARY"},
        {"status": "RESPONDED", "usage": {"body": "METADATA-CANARY"}},
        {"status": "RESPONDED", "usage": {"total_tokens": {"text": "METADATA-CANARY"}}},
        {"status": "RESPONDED", "usage": {"total_tokens": True}},
        {"status": "RESPONDED", "usage": {"total_tokens": -1}},
        {"status": "RESPONDED", "usage": {"total_tokens": 1.5}},
        {"status": "RESPONDED", "usage": {"total_tokens": 1000000001}},
        {"status": "RESPONDED", "discarded_reason": "METADATA-CANARY"},
        {"status": "RESPONDED", "usage": None},
        {},
        None,  # JSON null, distinct from the SQL NULL used before an observation.
    )
    for value in bad_results:
        async with children.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.errors.CheckViolation) as caught:
                await conn.execute(
                    "UPDATE public.bot_child_model_attempts SET result_metadata=%s WHERE attempt_id=%s",
                    (Jsonb(value), attempt),
                )
            assert caught.value.diag.constraint_name == "bot_child_result_metadata_closed"
    await finish(children, lease, prepared, attempt)
    assert (
        await query(children.agent, "SELECT result_metadata FROM public.bot_child_model_attempts")
    )[0]["result_metadata"]["status"] == "RESPONDED"


async def test_released_028_child_exact_request_and_spec_migrate_then_scrub_without_revival(
    upgrade_database,
):
    await p3_authority(upgrade_database)
    original = b'{"model":"child","messages":[{"role":"user","content":"OLD-CHILD-WIRE"}]}'
    old_spec = {
        "config_revision": 0,
        "depth": 1,
        "delegation_policy_revision": 1,
        "values": {
            **spec()["values"],
            "system_prompt": "OLD-CHILD-PROMPT",
            "stream": {"body": "OLD-NESTED-CANARY"},
            "model_timeout": "OLD-SCALAR-CANARY",
        },
    }
    old_result = {
        "status": "NOT_SENT",
        "text": "OLD-CHILD-RESPONSE",
        "usage": {"total_tokens": 0, "completion_tokens": {"body": "OLD-NESTED-CANARY"}},
    }
    old_snapshot = {
        "units": [{"text": "OLD-CHILD-SNAPSHOT"}],
        "conversation": {"channel_id": "old-group"},
    }
    async with await psycopg.AsyncConnection.connect(
        upgrade_database, autocommit=True, row_factory=dict_row
    ) as conn:
        async with conn.transaction():
            for path in sorted(
                files("forest.storage").joinpath("bot_migrations").iterdir(), key=lambda p: p.name
            ):
                if path.name.endswith(".sql") and 21 <= int(path.name[:3]) <= 28:
                    body = path.read_text(encoding="utf-8")
                    await conn.execute(body)
                    await conn.execute(
                        "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                        (path.name, hashlib.sha256(body.encode()).hexdigest()),
                    )
            await conn.execute(
                "INSERT INTO public.bot_agent_tools(bot_id,run_id,logical_step_id,tool_call_id,call_index,tool_name,arguments,arguments_digest,operation_id,status) "
                "VALUES ('bot-a','open-run','step:unsent','old-child-tool',0,'forest_analyze',%s,'oldargs','old-child-op','WAIT_CHILD')",
                (Jsonb({"task": "OLD-CHILD-TASK"}),),
            )
            await conn.execute(
                "INSERT INTO public.bot_agent_children(bot_id,child_id,parent_run_id,parent_step_id,tool_call_id,parent_input_revision,parent_snapshot_digest,authority_trigger_id,principal_ref,source_unit_ids,snapshot,snapshot_digest,spec,deadline_at,status,attempt_id,attempt_count) "
                "VALUES ('bot-a','old-child','open-run','step:unsent','old-child-tool',1,'old-parent','trigger-1','old-user','[]',%s,%s,%s,clock_timestamp()+interval '1 hour','RETRY','old-attempt',1)",
                (Jsonb(old_snapshot), digest_json(old_snapshot), Jsonb(old_spec)),
            )
            await conn.execute(
                "INSERT INTO public.bot_child_model_attempts(bot_id,attempt_id,child_id,request_id,request_body,wire_digest,manifest,metadata,owner_id,lease_epoch,host_incarnation,host_owner_epoch,status,budget_day,reserved_tokens,charged_tokens,settled,deadline_at,result,result_digest) "
                "VALUES ('bot-a','old-attempt','old-child','old-request',%s,%s,%s,%s,'old-owner',1,'old-host',1,'NOT_SENT',current_date,32,0,true,clock_timestamp()+interval '1 hour',%s,%s)",
                (
                    original,
                    "sha256:" + hashlib.sha256(original).hexdigest(),
                    Jsonb({"input_digest": digest_json(old_snapshot)}),
                    Jsonb(
                        {
                            "estimated_input_tokens": 16,
                            "max_output_tokens": 16,
                            "stream": False,
                            "serializer_revision": "chat-v1",
                        }
                    ),
                    Jsonb(old_result),
                    digest_json(old_result),
                ),
            )
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        payloads = ChildPayloadStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute("SELECT * FROM public.bot_agent_children")).fetchone()
            child = await payloads.hydrate_in(conn, row)
            assert child["snapshot"] == old_snapshot and child["spec"] == old_spec
            assert row["spec"] is row["snapshot"] is None
            assert "OLD-CHILD-PROMPT" not in str(row["spec_metadata"])
            assert "CANARY" not in str(row["spec_metadata"])
            assert "stream" not in row["spec_metadata"]["values"]
            assert "model_timeout" not in row["spec_metadata"]["values"]
            attempt = await (
                await conn.execute("SELECT * FROM public.bot_child_model_attempts")
            ).fetchone()
            restored = await payloads.hydrate_attempt_in(conn, attempt)
            assert restored["request_body"] == original and restored["status"] == "NOT_SENT"
            assert restored["result"] == old_result
            assert attempt["result_digest"] == digest_json(old_result)
            assert attempt["result_metadata"] == {
                "status": "NOT_SENT",
                "usage": {"total_tokens": 0},
            }
            assert attempt["request_body"] is attempt["manifest"] is None
        lease = await store.acquire_host("new", "new-child-host", ttl_seconds=120)
        raw = PayloadStore(store)
        result = await raw.forget_sources(
            lease, "forget", (SourceRef("one-original-source"),), expected_deletion_epoch=0
        )
        assert result["legacy_conservative"]
        while (await raw.scrub(lease))["remaining"]:
            pass
        async with store.pool.connection() as conn, conn.transaction():
            await fails("AGENT_MEMORY_REVOKED", payloads.hydrate_in(conn, row))
            await fails("AGENT_MEMORY_REVOKED", payloads.hydrate_attempt_in(conn, attempt))
        await store.migrate()
        async with store.pool.connection() as conn:
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) WHERE h.owner_id LIKE 'legacy-child%'"
                )
            ).fetchall()
            saved = await (
                await conn.execute("SELECT * FROM public.bot_child_model_attempts")
            ).fetchone()
            assert (saved["request_id"], saved["wire_digest"], saved["status"]) == (
                attempt["request_id"],
                attempt["wire_digest"],
                "NOT_SENT",
            )
    finally:
        await store.close()

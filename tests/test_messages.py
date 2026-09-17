"""Real PostgreSQL input ordering, trigger cuts, recovery and actor fencing."""

import asyncio
import multiprocessing
import os
import sys
from dataclasses import replace
from itertools import permutations
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from binding_seed import create_online_binding, signed_ticket_factory
from psycopg import sql
from test_delivery import permit as delivery_permit

from forest.dispatch_trace import DispatchTraceStore
from forest.errors import ForestError
from forest.packages import digest_json
from forest.routing_graph import RouteGraphStore
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore
from forest.storage.input_payloads import InputContentStore
from forest.storage.messages import MessageStore
from forest.storage.runtime_graph import RuntimeModuleGraphStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def messages():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private test PostgreSQL instance")
    name = "forest_messages_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("host-a", "incarnation-a", ttl_seconds=300)
        for binding_id, self_id in (("binding-a", "1000"), ("binding-b", "2000")):
            await create_online_binding(store, lease, binding_id, self_id, name=binding_id)
        yield MessageStore(store, BindingStore(store)), lease, uri
    finally:
        await store.close()
        assert name.startswith("forest_messages_") and len(name) == 48
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def event(number, *, group="group-1", user="user-1", text=None, self_id="1000"):
    return {
        "platform": "onebot-v11", "self_id": self_id, "channel_kind": "group",
        "channel_id": group, "thread_id": "", "platform_message_id": str(number),
        "sent_at": "2026-09-13T00:00:00Z",
        "author": {"external_user_id": user, "display_name": "同名用户", "is_self": user == self_id},
        "segments": [{"type": "text", "text": text or "message " + str(number)}],
    }


async def receive(messages, lease, number, *, trigger=False, **kwargs):
    return await messages.ingest(lease, "binding-a", event(number, **kwargs),
                                 decision="TRIGGER" if trigger else "NO_TRIGGER")


async def owner(messages, lease, conversation_id, actor_id="actor-a"):
    return await messages.acquire_actor(lease, conversation_id, actor_id, ttl_seconds=120)


async def fails(code, awaitable):
    with pytest.raises(ForestError) as caught:
        await awaitable
    assert caught.value.code == code


async def test_concurrent_dedup_has_no_ingress_gaps_or_duplicate_claims(messages):
    repository, lease, _ = messages
    results = await asyncio.gather(*[
        receive(repository, lease, n % 10, trigger=True) for n in range(40)
    ])
    assert sum(not row["duplicate"] for row in results) == 10
    assert sorted({row["ingress_seq"] for row in results}) == list(range(1, 11))
    conversation_id = results[0]["conversation_id"]
    actor = await owner(repository, lease, conversation_id)
    batch = await repository.select_input(actor, "first-input")
    assert len(batch["trigger_ids"]) == 10
    assert batch["end_seq"] == 10
    view = await repository.context(actor, run_id=batch["run_id"])
    assert len(view["units"]) == 10
    assert len(view["triggers"]) == 10
    await fails("INGEST_CONFLICT", receive(repository, lease, 0, text="changed"))


async def test_ingress_freezes_content_addressed_route_graph_and_projects_dispatch_trace(
    messages, monkeypatch
):
    repository, lease, _ = messages
    received = await receive(repository, lease, "route-trace")
    assert received["graph_digest"].startswith("sha256:")
    duplicate = await receive(repository, lease, "route-trace")
    assert duplicate["duplicate"] is True
    assert duplicate["graph_digest"] == received["graph_digest"]
    snapshot_builder = RouteGraphStore._snapshot_in

    async def unexpected_rebuild(*args, **kwargs):
        raise AssertionError("stable RouteGraph should reuse its content-addressed snapshot")

    monkeypatch.setattr(RouteGraphStore, "_snapshot_in", unexpected_rebuild)
    stable = await receive(repository, lease, "route-trace-stable")
    monkeypatch.setattr(RouteGraphStore, "_snapshot_in", snapshot_builder)
    assert stable["graph_digest"] == received["graph_digest"]

    ticket = await signed_ticket_factory(repository.store)
    graph_operation = "read-frozen-route-graph"
    graph_payload = {"graph_digest": received["graph_digest"]}
    graph = await RouteGraphStore(repository.store).read_version(
        await ticket("module.read", graph_operation, graph_payload),
        graph_operation,
        received["graph_digest"],
    )
    assert graph["graph_digest"] == received["graph_digest"]
    assert graph["protocol"] == "forest.route-graph/1"

    trace_operation = "read-dispatch-trace"
    trace_payload = {"event_id": received["event_id"], "limit": 10}
    result = await DispatchTraceStore(repository.store).read(
        await ticket("module.read", trace_operation, trace_payload),
        trace_operation,
        trace_payload,
    )
    assert len(result["traces"]) == 1
    trace = result["traces"][0]
    assert trace["graph_digest"] == received["graph_digest"]
    assert trace["status"] == "PASSED"
    assert trace["entries"] == [{
        "kind": "ingress",
        "node_id": "runtime:adapter.onebot",
        "component_id": "adapter.onebot",
        "generation_id": None,
        "status": "NO_TRIGGER",
        "reason": "",
        "started_at": trace["received_at"],
        "completed_at": trace["received_at"],
        "references": {},
    }]

    async with repository.store.pool.connection() as conn:
        count = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_route_graph_snapshots WHERE bot_id=%s",
                (repository.store.bot_id,),
            )
        ).fetchone()
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute(
                "UPDATE public.bot_message_inbox SET graph_digest=NULL "
                "WHERE bot_id=%s AND event_id=%s",
                (repository.store.bot_id, received["event_id"]),
            )
    assert count["n"] == 1


async def test_dispatch_trace_cursor_pages_exact_roots_and_is_bound_to_filters(messages):
    repository, lease, _ = messages
    accepted = [await receive(repository, lease, f"trace-page-{index}") for index in range(5)]
    ticket = await signed_ticket_factory(repository.store)
    traces = DispatchTraceStore(repository.store)

    async def page(operation_id, payload):
        return await traces.read(
            await ticket("module.read", operation_id, payload), operation_id, payload
        )

    first_payload = {"channel_id": "group-1", "limit": 2}
    first = await page("trace-page-first", first_payload)
    assert [row["event_id"] for row in first["traces"]] == [
        accepted[4]["event_id"], accepted[3]["event_id"]
    ]
    assert first["next_before"]

    second_payload = {**first_payload, "before": first["next_before"]}
    second = await page("trace-page-second", second_payload)
    assert [row["event_id"] for row in second["traces"]] == [
        accepted[2]["event_id"], accepted[1]["event_id"]
    ]
    assert second["next_before"]

    third_payload = {**first_payload, "before": second["next_before"]}
    third = await page("trace-page-third", third_payload)
    assert [row["event_id"] for row in third["traces"]] == [accepted[0]["event_id"]]
    assert "next_before" not in third

    mismatched = {"channel_id": "group-2", "limit": 2, "before": first["next_before"]}
    with pytest.raises(ForestError) as rejected:
        await page("trace-page-filter-mismatch", mismatched)
    assert rejected.value.code == "DISPATCH_TRACE_CURSOR_INVALID"


async def test_dispatch_trace_keeps_frozen_native_generation_after_host_restart(messages):
    repository, first_lease, _ = messages
    store = repository.store
    descriptor = {
        "protocol": "forest.runtime-module/1",
        "component_id": "adapter.onebot",
        "requires": [],
        "factory_ref": "forest.onebot_runtime:OneBotRuntime",
        "management_contributions": [],
        "management_contract": None,
    }
    descriptor = {**descriptor, "descriptor_digest": digest_json(descriptor)}
    graphs = RuntimeModuleGraphStore(store)
    first = await graphs.prepare(first_lease, [descriptor])
    await graphs.publish(first_lease, first)
    received = await receive(repository, first_lease, "native-generation-before-restart")

    await store.release_host(first_lease)
    second_lease = await store.acquire_host("host-b", "incarnation-b", ttl_seconds=300)
    second = await graphs.prepare(second_lease, [descriptor])
    await graphs.publish(second_lease, second)
    assert first.generations["adapter.onebot"] != second.generations["adapter.onebot"]

    ticket = await signed_ticket_factory(store)
    operation = "read-old-native-generation-trace"
    payload = {"event_id": received["event_id"], "limit": 10}
    trace = (await DispatchTraceStore(store).read(
        await ticket("module.read", operation, payload), operation, payload
    ))["traces"][0]
    assert trace["entries"][0]["generation_id"] == first.generations["adapter.onebot"]
    filtered_operation = "filter-old-native-generation-trace"
    filtered_payload = {
        "component_id": "adapter.onebot",
        "generation_id": first.generations["adapter.onebot"],
        "limit": 10,
    }
    filtered = await DispatchTraceStore(store).read(
        await ticket("module.read", filtered_operation, filtered_payload),
        filtered_operation,
        filtered_payload,
    )
    assert [item["event_id"] for item in filtered["traces"]] == [received["event_id"]]
    current_filter_operation = "reject-current-generation-for-old-trace"
    current_filter_payload = {
        "component_id": "adapter.onebot",
        "generation_id": second.generations["adapter.onebot"],
        "limit": 10,
    }
    assert (await DispatchTraceStore(store).read(
        await ticket("module.read", current_filter_operation, current_filter_payload),
        current_filter_operation,
        current_filter_payload,
    ))["traces"] == []
    current = await RouteGraphStore(store).snapshot()
    current_adapter = next(
        node for node in current["nodes"] if node["node_id"] == "runtime:adapter.onebot"
    )
    assert current_adapter["generation_id"] == second.generations["adapter.onebot"]


async def test_ingress_graph_fence_orders_concurrent_component_publication(messages, monkeypatch):
    repository, lease, _ = messages
    store = repository.store

    async def prepare(generation, expected_epoch):
        await store.register_generation(
            lease,
            "trace.component",
            generation,
            "package-" + generation,
            "config-" + generation,
            "worker-" + generation,
        )
        await store.mark_ready(lease, generation)
        operation = await store.begin_operation(
            lease,
            "trace.component",
            "publish-" + generation,
            "plan-" + generation,
            generation,
            expected_epoch,
        )
        await store.freeze(lease, operation["operation_id"], operation["op_epoch"])
        return operation

    first_operation = await prepare("trace-gen-1", 0)
    await store.publish(lease, first_operation["operation_id"], first_operation["op_epoch"])
    second_operation = await prepare("trace-gen-2", 1)

    entered, release = asyncio.Event(), asyncio.Event()
    original = RouteGraphStore.freeze_in

    async def hold_graph(self, conn):
        entered.set()
        await release.wait()
        return await original(self, conn)

    monkeypatch.setattr(RouteGraphStore, "freeze_in", hold_graph)
    admission = asyncio.create_task(receive(repository, lease, "before-publication"))
    publication = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        publication = asyncio.create_task(
            store.publish(lease, second_operation["operation_id"], second_operation["op_epoch"])
        )
        await asyncio.sleep(0.05)
        assert publication.done() is False
        release.set()
        before = await admission
        await publication
    finally:
        release.set()
        await asyncio.gather(
            admission, *([publication] if publication is not None else []),
            return_exceptions=True,
        )
        monkeypatch.setattr(RouteGraphStore, "freeze_in", original)
    after = await receive(repository, lease, "after-publication")
    assert before["graph_digest"] != after["graph_digest"]

    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT graph_digest,snapshot FROM public.bot_route_graph_snapshots "
                "WHERE bot_id=%s AND graph_digest=ANY(%s)",
                (store.bot_id, [before["graph_digest"], after["graph_digest"]]),
            )
        ).fetchall()
    graphs = {row["graph_digest"]: row["snapshot"] for row in rows}
    before_node = next(
        node for node in graphs[before["graph_digest"]]["nodes"]
        if node["node_id"] == "component:trace.component"
    )
    after_node = next(
        node for node in graphs[after["graph_digest"]]["nodes"]
        if node["node_id"] == "component:trace.component"
    )
    assert before_node["generation_id"] == "trace-gen-1"
    assert after_node["generation_id"] == "trace-gen-2"


async def test_group_trunk_is_shared_but_equal_message_ids_from_two_accounts_are_distinct(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1)
    second = await repository.ingest(lease, "binding-b", event(1, self_id="2000"), decision="NO_TRIGGER")
    assert first["conversation_id"] != second["conversation_id"]
    async with repository.store.pool.connection() as conn:
        rows = await (await conn.execute("SELECT * FROM public.bot_conversations ORDER BY binding_id")).fetchall()
        sources = await (await conn.execute("SELECT source_event_id FROM public.bot_message_inbox")).fetchall()
    assert rows[0]["group_source_id"] == rows[1]["group_source_id"]
    assert sources[0]["source_event_id"] != sources[1]["source_event_id"]
    assert [r["next_ingress_seq"] for r in rows] == [2, 2]


@pytest.mark.parametrize("anonymous_id", ["shared-anonymous-id", None])
async def test_anonymous_identity_is_scoped_to_the_receiving_account(messages, anonymous_id):
    repository, lease, _ = messages
    first, second = event(1), event(1, self_id="2000")
    for value in (first, second):
        value["author"]["anonymous"] = {"id": anonymous_id, "name": "同名匿名用户"}
    left = await repository.ingest(lease, "binding-a", first, decision="NO_TRIGGER")
    right = await repository.ingest(lease, "binding-b", second, decision="NO_TRIGGER")
    async with repository.store.pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT author FROM public.bot_message_inbox WHERE event_id=ANY(%s) ORDER BY event_id",
            ([left["event_id"], right["event_id"]],),
        )).fetchall()
    principals = [r["author"]["principal_ref"] for r in rows]
    assert all(principal.startswith("anonymous:") for principal in principals)
    assert principals[0] != principals[1]


@pytest.mark.parametrize("trigger", [False, True])
async def test_empty_content_keeps_source_author_sequence_and_host_trigger_decision(messages, trigger):
    repository, lease, _ = messages
    empty = event(1)
    empty["segments"] = []
    first = await repository.ingest(
        lease, "binding-a", empty, decision="TRIGGER" if trigger else "NO_TRIGGER",
    )
    assert first["ingress_seq"] == 1
    canonical = {**empty, "segments": [{"type": "text", "text": ""}]}
    duplicate = await repository.ingest(
        lease, "binding-a", canonical, decision="TRIGGER" if trigger else "NO_TRIGGER",
    )
    assert duplicate["duplicate"] and duplicate["event_id"] == first["event_id"]
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "empty-input")
    assert batch["kind"] == ("TRIGGER_INPUT" if trigger else "SILENT_DRAIN")
    context = await repository.context(actor, run_id=batch["run_id"])
    assert context["context_applied_seq"] == 1
    assert context["units"][0]["segments"] == [{"type": "text", "text": ""}]
    assert context["units"][0]["author"]["external_user_id"] == "user-1"
    assert context["units"][0]["source"]["channel_id"] == "group-1"
    assert bool(context["triggers"]) is trigger


async def test_platform_timestamps_do_not_reorder_context_and_authors_are_distinct(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, user="user-a")
    older = event(2, user="user-b")
    older["sent_at"] = "2020-01-01T00:00:00Z"
    await repository.ingest(lease, "binding-a", older, decision="NO_TRIGGER")
    actor = await owner(repository, lease, first["conversation_id"])
    await repository.select_input(actor, "silent")
    view = await repository.context(actor)
    assert [u["platform_message_id"] for u in view["units"]] == ["1", "2"]
    authors = [u["author"] for u in view["units"]]
    assert authors[0]["display_name"] == authors[1]["display_name"]
    assert authors[0]["principal_ref"] != authors[1]["principal_ref"]
    assert all(u["source"]["channel_id"] == "group-1" for u in view["units"])


async def test_safe_point_cuts_at_last_trigger_and_tool_continuation_excludes_tail(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    initial = await repository.select_input(actor, "first")
    for n in range(2, 7):
        await receive(repository, lease, n, trigger=n in {3, 5})
    # A background wakeup cannot mutate a running call's input.
    assert (await repository.select_input(actor, "background"))["status"] == "RESUME"
    assert (await repository.context(actor, run_id=initial["run_id"]))["context_applied_seq"] == 1
    second = await repository.select_input(actor, "safe-2", mode="SAFE_POINT")
    assert (second["start_seq"], second["end_seq"], second["input_revision"]) == (2, 5, 2)
    assert second["run_id"] == initial["run_id"]
    assert len(second["trigger_ids"]) == 2
    await receive(repository, lease, 7)
    continued = await repository.select_input(actor, "safe-3", mode="SAFE_POINT")
    assert continued["status"] == "CONTINUE"
    view = await repository.context(actor, run_id=initial["run_id"])
    assert [u["ingress_seq"] for u in view["units"]] == [1, 2, 3, 4, 5]
    assert len(view["triggers"]) == 3


async def test_classification_gap_prevents_trigger_overtake_then_wakes_without_new_event(messages):
    repository, lease, _ = messages
    first = await repository.ingest(lease, "binding-a", event(1))
    await receive(repository, lease, 2, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    assert (await repository.select_input(actor, "blocked"))["status"] == "WAIT_INPUT"
    await repository.classify(lease, first["conversation_id"], first["event_id"], "FAILED",
                              reason="parser_timeout")
    batch = await repository.select_input(actor, "unblocked")
    assert batch["end_seq"] == 2
    view = await repository.context(actor, run_id=batch["run_id"])
    assert view["units"][0]["segments"] == event(1)["segments"]
    assert view["units"][0]["trigger_decision"] == "FAILED"


async def test_dead_preprocessing_is_explicitly_failed_not_erased(messages):
    repository, lease, _ = messages
    first = await repository.ingest(lease, "binding-a", event(1))
    async with repository.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_message_inbox SET classification_deadline=clock_timestamp()-interval '1 second'")
    assert await repository.expire_classifications(lease) == 1
    actor = await owner(repository, lease, first["conversation_id"])
    assert (await repository.select_input(actor, "silent"))["kind"] == "SILENT_DRAIN"
    assert (await repository.context(actor))["units"][0]["segments"] == event(1)["segments"]


async def test_silent_stream_archives_locally_without_run_or_reply(messages):
    repository, lease, _ = messages
    repository.context_token_budget = 1800
    first = None
    for n in range(100):
        first = await receive(repository, lease, n, text="普通发言" * 30)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "silent")
    assert batch["kind"] == "SILENT_DRAIN"
    assert batch["run_id"] is None
    assert batch["end_seq"] == 100
    assert (await repository.select_input(actor, "finish-drain"))["status"] == "IDLE"
    view = await repository.context(actor)
    assert view["archive"]["count"] > 0
    assert view["archive"]["count"] + len(view["units"]) == 100
    assert not view["budget_exceeded"]
    async with repository.store.pool.connection() as conn:
        for table in ("bot_conversation_sessions", "bot_conversation_runs", "bot_conversation_replies"):
            assert (await (await conn.execute(sql.SQL("SELECT count(*) AS n FROM public.{}").format(sql.Identifier(table)))).fetchone())["n"] == 0


async def test_actor_single_owner_stale_writer_and_cross_group_independence(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1)
    other = await receive(repository, lease, 2, group="group-2")
    actor = await owner(repository, lease, first["conversation_id"])
    await fails("ACTOR_BUSY", owner(repository, lease, first["conversation_id"], "actor-b"))
    other_actor = await owner(repository, lease, other["conversation_id"], "actor-b")
    await asyncio.gather(repository.select_input(actor, "a"), repository.select_input(other_actor, "b"))
    async with repository.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_conversations SET actor_expires_at=clock_timestamp()-interval '1 second' WHERE conversation_id=%s", (actor.conversation_id,))
    replacement = await owner(repository, lease, first["conversation_id"], "actor-b")
    assert replacement.actor_epoch == actor.actor_epoch + 1
    await fails("STALE_ACTOR", repository.select_input(actor, "stale"))
    await repository.select_input(replacement, "new")


async def test_input_receipt_replay_and_recovery_claim_do_not_duplicate_sources(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    assert (await repository.select_input(actor, "first"))["input_batch_id"] == batch["input_batch_id"]
    await repository.end_run(actor, run_id=batch["run_id"], reason="worker_lost", requeue=True)
    second = await repository.select_input(actor, "recover")
    assert second["kind"] == "RECOVERY_CLAIM"
    assert second["start_seq"] is None and second["end_seq"] is None
    assert second["run_id"] != batch["run_id"]
    assert len((await repository.context(actor, run_id=second["run_id"]))["units"]) == 1


async def test_old_and_new_recovery_triggers_are_both_claimed(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    await repository.end_run(actor, run_id=batch["run_id"], reason="worker_lost", requeue=True)
    await receive(repository, lease, 2)
    await receive(repository, lease, 3, trigger=True)
    recovered = await repository.select_input(actor, "mixed")
    assert (recovered["start_seq"], recovered["end_seq"]) == (2, 3)
    assert len(recovered["trigger_ids"]) == 2
    assert len((await repository.context(actor, run_id=recovered["run_id"]))["units"]) == 3


async def test_pause_keeps_inbox_but_revokes_actor_business_writes(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1)
    actor = await owner(repository, lease, first["conversation_id"])
    await repository.store.set_maintenance(lease, 1, data_admission=False)
    await receive(repository, lease, 2)
    await fails("DATA_ADMISSION_CLOSED", repository.select_input(actor, "paused"))
    await repository.store.set_maintenance(lease, 2, data_admission=True)
    await fails("STALE_ACTOR", repository.select_input(actor, "old-policy"))


async def test_image_source_is_retained_without_uri_leaking_into_context(messages):
    repository, lease, _ = messages
    image = event(1)
    image["segments"] = [{"type": "image", "media_ref": {
        "kind": "onebot-source", "uri": "https://private.invalid/image?token=secret",
        "platform_file": "file-id", "state": "PENDING_CAPTURE",
    }}]
    first = await repository.ingest(lease, "binding-a", image, decision="TRIGGER")
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    view = await repository.context(actor, run_id=batch["run_id"])
    assert view["units"][0]["segments"][0]["type"] == "image"
    assert "secret" not in str(view) and "private.invalid" not in str(view)
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        row = await (await conn.execute("SELECT source,source_payload_id,media_ref FROM public.bot_inbound_media_refs")).fetchone()
        assert row["source"] is None and row["source_payload_id"]
        hydrated = await InputContentStore(repository.store).read_media_source_in(
            conn, row["media_ref"], expected_deletion_epoch=0,
        )
    assert hydrated["source"]["uri"].endswith("token=secret")


async def test_wrong_bot_host_binding_and_self_trigger_are_rejected(messages):
    repository, lease, _ = messages
    await fails("BOT_SCOPE_MISMATCH", receive(repository, replace(lease, bot_id="other"), 1))
    await fails("BINDING_IDENTITY_MISMATCH", repository.ingest(lease, "binding-b", event(1)))
    await fails("SELF_TRIGGER", receive(repository, lease, 1, trigger=True, user="1000"))
    await fails("STALE_BINDING", repository.ingest(lease, "binding-a", event(1), expected_binding_epoch=99))


async def test_final_reply_atomically_answers_only_included_triggers_and_silent_tail(messages):
    repository, lease, _ = messages
    delivery = DeliveryStore(repository.store)
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    await receive(repository, lease, 2, trigger=True)
    await receive(repository, lease, 3)
    reply = await repository.finalize_reply(
        actor, "answer-1", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "answer one"}]], delivery=delivery,
    )
    assert reply["response_seq"] == 1
    assert reply["delivery"]["intent_id"]
    repeated = await repository.finalize_reply(
        actor, "answer-1", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "answer one"}]], delivery=delivery,
    )
    assert repeated["reply_id"] == reply["reply_id"]
    async with repository.store.pool.connection() as conn:
        triggers = await (await conn.execute("SELECT status FROM public.bot_conversation_triggers ORDER BY ingress_seq")).fetchall()
    assert [t["status"] for t in triggers] == ["ANSWERED", "PENDING"]
    next_batch = await repository.select_input(actor, "next")
    assert next_batch["session_id"] == batch["session_id"]
    assert next_batch["run_id"] != batch["run_id"]
    assert next_batch["end_seq"] == 2
    reply2 = await repository.finalize_reply(
        actor, "answer-2", run_id=next_batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "answer two"}]], delivery=delivery,
    )
    assert reply2["response_seq"] == 2
    drain = await repository.select_input(actor, "drain")
    assert drain["kind"] == "SILENT_DRAIN" and drain["end_seq"] == 3
    view = await repository.context(actor)
    # The tail arrived earlier than both replies but was not visible to their
    # models; it is appended afterwards, never backdated into model history.
    assert [u["kind"] for u in view["units"]] == ["INPUT", "REPLY", "INPUT", "REPLY", "INPUT"]
    assert (await repository.select_input(actor, "close"))["status"] == "IDLE"
    await receive(repository, lease, 4, trigger=True)
    later = await repository.select_input(actor, "later")
    assert later["session_id"] != batch["session_id"]


async def test_failed_delivery_enqueue_rolls_back_reply_context_and_trigger_answer(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")

    class FailAfterIntent(DeliveryStore):
        async def enqueue_in(self, *args, **kwargs):
            await super().enqueue_in(*args, **kwargs)
            raise RuntimeError("lost before final commit")

    with pytest.raises(RuntimeError, match="lost before"):
        await repository.finalize_reply(
            actor, "answer", run_id=batch["run_id"], expected_input_revision=1,
            reply_parts=[[{"type": "text", "text": "answer"}]], delivery=FailAfterIntent(repository.store),
        )
    view = await repository.context(actor, run_id=batch["run_id"])
    assert len(view["triggers"]) == 1
    assert len(view["units"]) == 1
    async with repository.store.pool.connection() as conn:
        assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_conversation_replies")).fetchone())["n"] == 0
    reply = await repository.finalize_reply(
        actor, "answer", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "answer"}]], delivery=DeliveryStore(repository.store),
    )
    assert reply["response_seq"] == 1


async def test_cancel_preserves_applied_input_and_independent_new_trigger(messages):
    repository, lease, _ = messages
    delivery = DeliveryStore(repository.store)
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    await receive(repository, lease, 2, trigger=True)
    await repository.end_run(actor, run_id=batch["run_id"], reason="cancel_requested", status="CANCELLED", delivery=delivery)
    await fails("RUN_NOT_ACTIVE", repository.finalize_reply(
        actor, "late", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "late"}]], delivery=delivery,
    ))
    next_batch = await repository.select_input(actor, "next")
    assert next_batch["start_seq"] == 2
    assert len(next_batch["trigger_ids"]) == 1
    assert len((await repository.context(actor, run_id=next_batch["run_id"]))["units"]) == 2


async def test_oversized_trigger_is_pinned_and_explicitly_reports_budget_failure(messages):
    repository, lease, _ = messages
    repository.context_token_budget = 512
    first = await receive(repository, lease, 1, trigger=True, text="long trigger " * 500)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    assert batch["status"] == "INPUT_BUDGET_EXCEEDED"
    assert (await repository.select_input(actor, "first"))["status"] == "INPUT_BUDGET_EXCEEDED"
    view = await repository.context(actor, run_id=batch["run_id"])
    assert view["budget_exceeded"]
    assert view["units"][0]["segments"][0]["text"] == "long trigger " * 500


async def test_sources_and_committed_batches_are_database_immutable(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1)
    actor = await owner(repository, lease, first["conversation_id"])
    await repository.select_input(actor, "first")
    async with repository.store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute("UPDATE public.bot_message_inbox SET author='{}'::jsonb")
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute("DELETE FROM public.bot_context_input_batches")


async def test_classification_completion_permutations_preserve_prefix_and_trigger_cut(messages):
    repository, lease, _ = messages
    for order in permutations((1, 2, 3)):
        group = "permutation-" + "".join(map(str, order))
        rows = {n: await repository.ingest(lease, "binding-a", event(n, group=group)) for n in (1, 2, 3)}
        classified = set()
        for n in order:
            await repository.classify(lease, rows[n]["conversation_id"], rows[n]["event_id"],
                                      "TRIGGER" if n == 2 else "NO_TRIGGER")
            classified.add(n)
            prefix = next((k - 1 for k in (1, 2, 3) if k not in classified), 3)
            async with repository.store.pool.connection() as conn:
                actual = await (await conn.execute(
                    "SELECT classified_through_seq FROM public.bot_conversations WHERE conversation_id=%s",
                    (rows[1]["conversation_id"],),
                )).fetchone()
            assert actual["classified_through_seq"] == prefix
        actor = await owner(repository, lease, rows[1]["conversation_id"])
        batch = await repository.select_input(actor, "select")
        assert (batch["start_seq"], batch["end_seq"]) == (1, 2)
        assert [u["ingress_seq"] for u in (await repository.context(actor, run_id=batch["run_id"]))["units"]] == [1, 2]


async def test_trigger_between_silent_batches_prevents_old_empty_decision_draining_new_tail(messages):
    repository, lease, _ = messages
    first = None
    for n in range(1, 301):
        first = await receive(repository, lease, n)
    actor = await owner(repository, lease, first["conversation_id"])
    initial = await repository.select_input(actor, "silent-1")
    assert initial["end_seq"] == 256
    await receive(repository, lease, 301, trigger=True)
    await receive(repository, lease, 302)
    next_batch = await repository.select_input(actor, "silent-2")
    assert next_batch["kind"] == "TRIGGER_INPUT"
    assert (next_batch["start_seq"], next_batch["end_seq"]) == (257, 301)
    view = await repository.context(actor, run_id=next_batch["run_id"])
    assert max(u["ingress_seq"] for u in view["units"]) == 301


async def test_actor_expiring_during_final_transaction_cannot_publish_an_answer(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "first")
    short = await repository.renew_actor(actor, ttl_seconds=0.08)

    class DelayedIntent(DeliveryStore):
        async def enqueue_in(self, *args, **kwargs):
            result = await super().enqueue_in(*args, **kwargs)
            # Fault injection for a blocked commit, never production external I/O.
            await asyncio.sleep(0.15)
            return result

    await fails("STALE_ACTOR", repository.finalize_reply(
        short, "late", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "expired"}]], delivery=DelayedIntent(repository.store),
    ))
    async with repository.store.pool.connection() as conn:
        assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_conversation_replies")).fetchone())["n"] == 0
        assert (await (await conn.execute("SELECT status FROM public.bot_conversation_triggers")).fetchone())["status"] == "CLAIMED"


async def test_shell_gate_reserves_namespace_and_never_stores_unknown_raw_arguments(messages):
    repository, lease, _ = messages
    control = event(1, text="#forest confirm approval-id SECRET-ONE-TIME-TOKEN")
    first = await repository.ingest(lease, "binding-a", control, decision="TRIGGER")
    assert first["command_id"] and first["trigger_decision"] == "CONTROL_COMMAND"
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "silent")
    assert batch["kind"] == "SILENT_DRAIN"
    view = await repository.context(actor)
    assert "SECRET" not in str(view)
    assert view["units"][0]["context_role"] == "control_data"
    async with repository.store.pool.connection() as conn:
        command = await (await conn.execute("SELECT * FROM public.bot_shell_requests")).fetchone()
        inbox = await (await conn.execute("SELECT * FROM public.bot_message_inbox")).fetchone()
    assert command["command_name"] == "unknown"
    assert "SECRET" not in str(command) and "SECRET" not in str(inbox)
    duplicate = await repository.ingest(lease, "binding-a", control)
    assert duplicate["command_id"] == first["command_id"] and duplicate["duplicate"]


async def test_shell_gate_does_not_execute_quotes_prefix_collisions_or_other_mentions(messages):
    repository, lease, _ = messages
    cases = [
        [{"type": "text", "text": "#forestX status"}],
        [{"type": "text", "text": "```\n#forest status\n```"}],
        [{"type": "reply", "platform_message_id": "old"}, {"type": "text", "text": "#forest status"}],
        [{"type": "mention", "external_user_id": "other"}, {"type": "text", "text": "#forest status"}],
    ]
    for number, segments in enumerate(cases):
        value = event(number)
        value["segments"] = segments
        accepted = await repository.ingest(lease, "binding-a", value, decision="NO_TRIGGER")
        assert accepted["command_id"] is None
    allowed = event(10)
    allowed["segments"] = [{"type": "text", "text": "  "},
                           {"type": "mention", "external_user_id": "1000"},
                           {"type": "text", "text": "  #fore"}, {"type": "text", "text": "st core status"}]
    accepted = await repository.ingest(lease, "binding-a", allowed)
    claimed = await repository.claim_control(lease, accepted["command_id"], "core")
    assert claimed["command_name"] == "core.status"


async def test_control_completes_during_classification_gap_without_actor_or_chat_reply(messages):
    repository, lease, _ = messages
    first = await repository.ingest(lease, "binding-a", event(1))
    command = await receive(repository, lease, 2, text="#forest status")
    await repository.store.set_maintenance(lease, 1, data_admission=False)
    pending = await repository.pending_controls(lease)
    assert [p["command_id"] for p in pending] == [command["command_id"]]
    claim = await repository.claim_control(lease, command["command_id"], "core")
    assert claim["source"]["author"]["external_user_id"] == "user-1"
    completed = await repository.complete_control(
        lease, claim, "core-status", reply_parts=[[{"type": "text", "text": "Bot paused"}]],
        outcome={"code": "STATUS", "paused": True}, delivery=DeliveryStore(repository.store),
    )
    assert completed["status"] == "COMPLETED"
    assert completed["delivery"]["send_lane"] == "CONTROL"
    async with repository.store.pool.connection() as conn:
        conversation = await (await conn.execute("SELECT * FROM public.bot_conversations WHERE conversation_id=%s", (first["conversation_id"],))).fetchone()
        replies = await (await conn.execute("SELECT count(*) AS n FROM public.bot_conversation_replies")).fetchone()
    assert conversation["classified_through_seq"] == conversation["context_applied_seq"] == 0
    assert conversation["active_session_id"] is None and conversation["next_response_seq"] == 1
    assert replies["n"] == 0


async def test_control_claim_replacement_fences_late_terminal_write(messages):
    repository, lease, _ = messages
    command = await receive(repository, lease, 1, text="#forest help")
    old = await repository.claim_control(lease, command["command_id"], "worker-old", ttl_seconds=0.02)
    await asyncio.sleep(0.04)
    newer = await repository.claim_control(lease, command["command_id"], "worker-new")
    await fails("STALE_COMMAND", repository.complete_control(
        lease, old, "old", reply_parts=[[{"type": "text", "text": "old"}]],
        outcome={"code": "OLD"}, delivery=DeliveryStore(repository.store),
    ))
    result = await repository.complete_control(
        lease, newer, "new", reply_parts=[[{"type": "text", "text": "help"}]],
        outcome={"code": "HELP"}, delivery=DeliveryStore(repository.store),
    )
    assert result["status"] == "COMPLETED"
    replay = await repository.complete_control(
        lease, newer, "new", reply_parts=[[{"type": "text", "text": "help"}]],
        outcome={"code": "HELP"}, delivery=DeliveryStore(repository.store),
    )
    assert replay["delivery"]["intent_id"] == result["delivery"]["intent_id"]


async def outgoing_chat(repository, lease):
    first = await receive(repository, lease, 1, group="42", trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "input")
    delivery = DeliveryStore(repository.store)
    reply = await repository.finalize_reply(
        actor, "reply", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "exact output"}]], delivery=delivery,
    )
    claimed = await delivery.claim(lease, "sender")
    permit = await delivery_permit(delivery, lease, claimed)
    await delivery.mark_started(permit)
    return actor, delivery, permit, reply


async def test_late_chat_receipt_creates_precise_alias_and_changes_context_only_at_safe_point(messages):
    repository, lease, _ = messages
    actor, delivery, permit, reply = await outgoing_chat(repository, lease)
    echo = await receive(repository, lease, 777, group="42", user="1000", text="exact output")
    await repository.select_input(actor, "early-echo")
    view = await repository.context(actor)
    assert [u["kind"] for u in view["units"]] == ["INPUT", "REPLY", "INPUT"]
    await delivery.complete(permit, "SENT", {"message_id": "777"})
    assert await repository.reconcile_echoes(lease) == 1
    # Background reconciliation does not mutate the active view behind an actor.
    assert len((await repository.context(actor))["units"]) == 3
    await repository.select_input(actor, "alias-safe-point")
    view = await repository.context(actor)
    assert [u["kind"] for u in view["units"]] == ["INPUT", "REPLY"]
    assert view["units"][1]["reply_id"] == reply["reply_id"]
    assert view["context_applied_seq"] == 2
    current = await row_for_event(repository, echo["event_id"])
    assert current["origin"] == "chat_output" and current["segments"] is None
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        hydrated = await InputContentStore(repository.store).read_input_in(
            conn, echo["event_id"], expected_deletion_epoch=0, required=True,
        )
    assert hydrated["segments"][0]["text"] == "exact output"
    assert await repository.reconcile_echoes(lease) == 0
    # Same text under another platform identity remains a distinct self message.
    await receive(repository, lease, 778, group="42", user="1000", text="exact output")
    assert await repository.reconcile_echoes(lease) == 0
    await repository.select_input(actor, "different-message")
    assert len((await repository.context(actor))["units"]) == 3


async def row_for_event(repository, event_id):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM public.bot_message_inbox WHERE event_id=%s", (event_id,))).fetchone()


async def test_echo_alias_requires_exact_self_binding_target_and_confirmed_receipt(messages):
    repository, lease, _ = messages
    _, delivery, permit, _ = await outgoing_chat(repository, lease)
    wrong_group = await receive(repository, lease, 888, group="43", user="1000", text="exact output")
    wrong_binding = await repository.ingest(
        lease, "binding-b", event(888, group="42", user="2000", self_id="2000", text="exact output"),
        decision="NO_TRIGGER",
    )
    wrong_sender = await receive(repository, lease, 888, group="42", user="user-2", text="exact output")
    await delivery.complete(permit, "SENT", {"message_id": "888"})
    assert await repository.reconcile_echoes(lease) == 0
    for accepted in (wrong_group, wrong_binding, wrong_sender):
        assert (await row_for_event(repository, accepted["event_id"]))["origin"] == "platform_input"


async def test_unknown_delivery_is_not_guessed_as_echo_from_payload_or_unconfirmed_id(messages):
    repository, lease, _ = messages
    _, delivery, permit, _ = await outgoing_chat(repository, lease)
    echo = await receive(repository, lease, 999, group="42", user="1000", text="exact output")
    await delivery.complete(permit, "UNKNOWN", {"message_id": "999"})
    assert await repository.reconcile_echoes(lease) == 0
    assert (await row_for_event(repository, echo["event_id"]))["origin"] == "platform_input"
    await delivery.complete(permit, "SENT", {"message_id": "999"})
    assert await repository.reconcile_echoes(lease) == 1


async def test_late_control_echo_keeps_one_safe_canonical_control_output(messages):
    repository, lease, _ = messages
    command = await receive(repository, lease, 1, group="42", text="#forest help")
    delivery = DeliveryStore(repository.store)
    claim = await repository.claim_control(lease, command["command_id"], "core")
    await repository.complete_control(
        lease, claim, "control-help", reply_parts=[[{"type": "text", "text": "safe help"}]],
        outcome={"code": "HELP"}, delivery=delivery,
    )
    claimed = await delivery.claim(lease, "sender")
    permit = await delivery_permit(delivery, lease, claimed)
    await delivery.mark_started(permit)
    echo = await receive(repository, lease, 999, group="42", user="1000", text="safe help")
    actor = await owner(repository, lease, command["conversation_id"])
    await repository.select_input(actor, "unconfirmed")
    assert len((await repository.context(actor))["units"]) == 2
    await delivery.complete(permit, "SENT", {"message_id": "999"})
    assert await repository.reconcile_echoes(lease) == 1
    await repository.select_input(actor, "confirmed")
    units = (await repository.context(actor))["units"]
    assert [u["kind"] for u in units] == ["INPUT", "CONTROL"]
    assert all(u["context_role"] == "control_data" for u in units)
    assert units[1]["control_command_id"] == command["command_id"]
    assert (await row_for_event(repository, echo["event_id"]))["origin"] == "control_output"
    await repository.select_input(actor, "again")
    assert len((await repository.context(actor))["units"]) == 2


def _process_receive(uri, lease, gate, queue):
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    async def run():
        store = BotStore(uri, "bot-a")
        await store.open()
        try:
            gate.wait(10)
            repository = MessageStore(store)
            result = await receive(repository, lease, "multiprocess", trigger=True)
            queue.put(("ok", result["duplicate"], result["ingress_seq"]))
        finally:
            await store.close()
    try:
        asyncio.run(run())
    except Exception as exc:
        queue.put(("error", type(exc).__name__))


async def test_multiprocess_duplicate_reception_shares_one_durable_sequence(messages):
    _, lease, uri = messages
    context = multiprocessing.get_context("spawn")
    gate, queue = context.Event(), context.Queue()
    processes = [context.Process(target=_process_receive, args=(uri, lease, gate, queue)) for _ in range(2)]
    try:
        for process in processes:
            process.start()
        gate.set()
        results = await asyncio.gather(*[asyncio.to_thread(queue.get, True, 20) for _ in processes])
        assert sorted(results) == [("ok", False, 1), ("ok", True, 1)]
    finally:
        for process in processes:
            await asyncio.to_thread(process.join, 10)
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, 5)
        queue.close()

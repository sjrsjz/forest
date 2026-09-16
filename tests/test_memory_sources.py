"""Real PostgreSQL source provenance, committed cuts and legacy admission."""

import asyncio

import psycopg
import pytest
from test_jobs import configure, enqueue, manual, request
from test_jobs import environment as environment
from test_jobs import jobs as jobs
from test_messages import event, owner, receive
from test_messages import messages as messages

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.delivery import DeliveryStore
from forest.storage.memory_sources import MemorySourceStore

pytestmark = pytest.mark.postgres


async def rows(repository, query, parameters=()):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


async def drain(repository, lease, receipt, operation="drain"):
    actor = await owner(repository, lease, receipt["conversation_id"])
    selection = await repository.select_input(actor, operation)
    return actor, selection


async def source_read(sources, lease, receipt, cut=0, **kwargs):
    return (await sources.read(lease, receipt["conversation_id"], input_cut=cut, **kwargs))[
        "sources"
    ]


async def test_concurrent_admission_assigns_one_order_and_only_committed_batch_is_eligible(
    messages,
):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    receipts = await asyncio.gather(
        *[receive(repository, lease, n % 10, user="user-" + str(n % 10)) for n in range(40)]
    )
    assert await source_read(sources, lease, receipts[0]) == []
    registered = await rows(
        repository, "SELECT * FROM public.bot_memory_source_units ORDER BY source_order_seq"
    )
    assert len(registered) == 10
    assert [unit["source_order_seq"] for unit in registered] == list(range(1, 11))
    assert {unit["order_origin"] for unit in registered} == {"LIVE"}
    assert len({unit["principal_ref"] for unit in registered}) == 10
    actor, selected = await drain(repository, lease, receipts[0])
    assert selected["kind"] == "SILENT_DRAIN"
    readable = await source_read(sources, lease, receipts[0], 10)
    assert len(readable) == 10
    assert {unit["source"]["channel_id"] for unit in readable} == {"group-1"}
    assert {unit["source"]["realm_id"] for unit in readable} == {"qq-global"}
    assert all(unit["source"]["group_source_id"] == unit["source_id"] for unit in readable)
    assert {unit["author"]["identity_origin"] for unit in readable} == {"adapter_verified"}
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_eligibility")) == 10
    assert len(await rows(repository, "SELECT * FROM public.bot_context_input_batches")) == 1
    assert (await repository.select_input(actor, "idle"))["status"] == "IDLE"


async def test_fact_revision_advances_only_for_new_durable_source_facts(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1)
    view = await sources.read(lease, first["conversation_id"], input_cut=0)
    assert view["fact_revision"] == 1 and view["deletion_epoch"] == 0
    await receive(repository, lease, 1)
    assert (await sources.read(lease, first["conversation_id"], input_cut=0))["fact_revision"] == 1
    actor, _ = await drain(repository, lease, first)
    assert (await sources.read(lease, first["conversation_id"], input_cut=1))["fact_revision"] == 2
    await repository.select_input(actor, "drain")
    await sources.backfill(lease)
    await receive(repository, lease, 1)
    assert (await sources.read(lease, first["conversation_id"], input_cut=1))["fact_revision"] == 2
    second = await repository.ingest(
        lease, "binding-a", {**event(1), "thread_id": "second"}, decision="NO_TRIGGER"
    )
    assert (await sources.read(lease, second["conversation_id"], input_cut=0))["fact_revision"] == 3
    await drain(repository, lease, second)
    assert (await sources.read(lease, second["conversation_id"], input_cut=1))["fact_revision"] == 4


async def test_group_trunk_spans_bindings_but_account_events_are_not_guessed_equal(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1)
    second = await repository.ingest(
        lease, "binding-b", event(1, self_id="2000"), decision="NO_TRIGGER"
    )
    await drain(repository, lease, first)
    # No equivalent occurrence exists in binding-b: eligible cross-conversation
    # group memory is available even before binding-b drains its own event.
    shared = await source_read(sources, lease, second)
    assert len(shared) == 1
    assert shared[0]["source_order_seq"] == 1
    await drain(repository, lease, second)
    shared = await source_read(sources, lease, second, 1)
    assert len(shared) == 2
    assert len({row["source_id"] for row in shared}) == 1
    assert len({row["source_event_id"] for row in shared}) == 2


async def test_shared_occurrence_cannot_bypass_current_thread_tail_cut(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1)
    other = {**event(1), "thread_id": "thread-two"}
    second = await repository.ingest(lease, "binding-a", other, decision="NO_TRIGGER")
    await drain(repository, lease, first)
    assert await source_read(sources, lease, second) == []
    await drain(repository, lease, second)
    assert await source_read(sources, lease, second, 0) == []
    assert len(await source_read(sources, lease, second, 1)) == 1
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_units")) == 1
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_occurrences")) == 2


async def test_reused_platform_source_cannot_change_author_or_body_in_another_thread(messages):
    repository, lease, _ = messages
    await receive(repository, lease, 1)
    with pytest.raises(ForestError) as caught:
        await repository.ingest(
            lease,
            "binding-a",
            {**event(1, user="forged-other", text="different"), "thread_id": "second"},
            decision="NO_TRIGGER",
        )
    assert caught.value.code == "MEMORY_SOURCE_CONFLICT"
    assert len(await rows(repository, "SELECT * FROM public.bot_message_inbox")) == 1
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_occurrences")) == 1


async def test_direct_is_restricted_while_distinct_groups_share_with_explicit_source(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    a = await receive(repository, lease, 1)
    b = await receive(repository, lease, 2, group="group-2")
    private_event = {**event(3, group="user-1"), "channel_kind": "private"}
    direct = await repository.ingest(lease, "binding-a", private_event, decision="NO_TRIGGER")
    for receipt in (a, b, direct):
        await drain(repository, lease, receipt)
    shared = await source_read(sources, lease, b, 1)
    assert {unit["source"]["channel_id"] for unit in shared} == {"group-1", "group-2"}
    private = await source_read(sources, lease, direct, 1)
    assert {unit["source_kind"] for unit in private} == {"GROUP", "DIRECT"}
    restricted = [unit for unit in private if unit["source_kind"] == "DIRECT"]
    assert restricted[0]["visibility"] == "source_restricted"


async def test_last_trigger_cut_remains_frozen_after_tail_is_later_drained(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1, trigger=True)
    actor, batch = await drain(repository, lease, first, "initial")
    await receive(repository, lease, 2)
    await receive(repository, lease, 3, trigger=True)
    await receive(repository, lease, 4)
    second = await repository.select_input(actor, "boundary", mode="SAFE_POINT")
    assert second["end_seq"] == 3
    assert len(await source_read(sources, lease, first, 1)) == 1
    assert len(await source_read(sources, lease, first, 3)) == 3
    await repository.end_run(actor, run_id=batch["run_id"], reason="test", status="FAILED")
    await repository.select_input(actor, "tail")
    assert len(await source_read(sources, lease, first, 3)) == 3
    assert len(await source_read(sources, lease, first, 4)) == 4
    with pytest.raises(ForestError) as caught:
        await source_read(sources, lease, first, 5)
    assert caught.value.code == "MEMORY_INPUT_CUT"


async def test_native_image_parts_have_stable_ids_and_private_observation_refs(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    image = event(1)
    image["segments"] = [
        {"type": "text", "text": "看这张图"},
        {"type": "image", "media_ref": {"kind": "remote", "uri": "https://example.invalid/a.png"}},
        {"type": "mention", "external_user_id": "user-2"},
    ]
    first = await repository.ingest(lease, "binding-a", image, decision="NO_TRIGGER")
    second = await repository.ingest(
        lease, "binding-a", {**image, "thread_id": "second"}, decision="NO_TRIGGER"
    )
    await drain(repository, lease, first)
    await drain(repository, lease, second)
    raw = (await source_read(sources, lease, first, 1))[0]
    assert [part["part_id"] for part in raw["parts"]] == ["0", "1", "2"]
    assert raw["parts"][1]["type"] == "image"
    assert raw["parts"][1]["media_ref"].startswith("source-media:")
    assert "example.invalid" not in str(raw) and "caption" not in str(raw)
    refs = await rows(repository, "SELECT * FROM public.bot_memory_source_media")
    assert len(refs) == 2 and len({ref["media_ref"] for ref in refs}) == 2
    assert {ref["source_unit_id"] for ref in refs} == {raw["source_unit_id"]}
    assert (
        len(await source_read(sources, lease, first, 1, source_unit_ids=[raw["source_unit_id"]]))
        == 1
    )
    assert await source_read(sources, lease, first, 1, source_unit_ids=["another-bot-source"]) == []
    metadata = (await source_read(sources, lease, first, 1, include_payload=False))[0]
    assert "parts" not in metadata and "author" not in metadata


async def test_reply_is_registered_only_in_atomic_commit_and_keeps_dependency_cut(
    messages, monkeypatch
):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1, trigger=True)
    actor, batch = await drain(repository, lease, first)
    second = await repository.ingest(
        lease, "binding-a", {**event(1), "thread_id": "second"}, decision="NO_TRIGGER"
    )
    original = MemorySourceStore.register_reply_in

    async def fail_after_source(self, *args, **kwargs):
        await original(self, *args, **kwargs)
        raise RuntimeError("after source before final commit")

    with monkeypatch.context() as patch:
        patch.setattr(MemorySourceStore, "register_reply_in", fail_after_source)
        with pytest.raises(RuntimeError, match="after source"):
            await repository.finalize_reply(
                actor,
                "reply",
                run_id=batch["run_id"],
                expected_input_revision=1,
                reply_parts=[[{"type": "text", "text": "正式回答"}]],
                delivery=DeliveryStore(repository.store),
            )
    assert not await rows(
        repository, "SELECT * FROM public.bot_memory_source_units WHERE kind='REPLY'"
    )
    assert not await rows(repository, "SELECT * FROM public.bot_conversation_replies")
    kwargs = dict(
        run_id=batch["run_id"],
        expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "正式回答"}]],
        delivery=DeliveryStore(repository.store),
    )
    reply = await repository.finalize_reply(actor, "reply", **kwargs)
    assert (await repository.finalize_reply(actor, "reply", **kwargs))["reply_id"] == reply[
        "reply_id"
    ]
    reply_unit = (
        await rows(repository, "SELECT * FROM public.bot_memory_source_units WHERE kind='REPLY'")
    )[0]
    assert not reply_unit["lineage_complete"]  # Private steps await full payload migration.
    assert reply_unit["identity_origin"] == "host_output"
    # Reply itself has no current-thread occurrence; its input dependency does.
    assert await source_read(sources, lease, second) == []
    await drain(repository, lease, second)
    assert {unit["kind"] for unit in await source_read(sources, lease, second, 1)} == {
        "INPUT",
    }
    diagnostic = await sources.payloads.read_json(
        lease, reply_unit["payload_id"], expected_deletion_epoch=0
    )
    author = diagnostic["author"]
    assert author["is_self"] and author["external_user_id"] == "1000"
    assert diagnostic["parts"] == [{"type": "text", "text": "正式回答", "part_id": "0"}]


async def test_complete_candidate_cannot_recall_through_an_incomplete_parent(messages, monkeypatch):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1, trigger=True)
    actor, batch = await drain(repository, lease, first)
    await repository.finalize_reply(
        actor,
        "answer",
        run_id=batch["run_id"],
        expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "先前正式回复"}]],
        delivery=DeliveryStore(repository.store),
    )
    parent = (
        await rows(repository, "SELECT * FROM public.bot_memory_source_units WHERE kind='REPLY'")
    )[0]
    assert not parent["lineage_complete"]
    original = MemorySourceStore._register_in

    async def register_with_parent(self, conn, conversation, **kwargs):
        # Exercise the trusted registrar's actual dependency contract, with a
        # real Inbox/Actor/batch. Do not forge eligibility or mutate fixed rows.
        assert kwargs["kind"] == "INPUT"
        return await original(
            self,
            conn,
            conversation,
            **{**kwargs, "dependencies": (SourceRef(parent["source_unit_id"]),)},
        )

    with monkeypatch.context() as patch:
        patch.setattr(MemorySourceStore, "_register_in", register_with_parent)
        child = await receive(repository, lease, 2, text="我引用了先前正式回复")
    await receive(repository, lease, 3, text="独立来源")
    await repository.select_input(actor, "drain-followups")
    child_source = (
        await rows(
            repository,
            "SELECT u.* FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) WHERE o.event_id=%s",
            (child["event_id"],),
        )
    )[0]
    assert child_source["lineage_complete"]
    dependencies = await rows(
        repository,
        "SELECT source_unit_id FROM public.bot_payload_sources WHERE payload_id=%s",
        (child_source["payload_id"],),
    )
    assert parent["source_unit_id"] in {row["source_unit_id"] for row in dependencies}
    # All sources are in the same group, eligible, ACTIVE and within cut. Only
    # the incomplete ancestor prevents this otherwise complete child returning.
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_eligibility")) == 4
    assert {
        row["state"]
        for row in await rows(repository, "SELECT state FROM public.bot_payload_headers")
    } == {"ACTIVE"}
    assert (
        await source_read(
            sources, lease, first, 3, source_unit_ids=[child_source["source_unit_id"]]
        )
        == []
    )
    recalled = await source_read(sources, lease, first, 3)
    assert len(recalled) == 2 and all(row["kind"] == "INPUT" for row in recalled)
    assert any(row["parts"][0]["text"] == "独立来源" for row in recalled)


async def test_scrubbed_source_replay_keeps_metadata_without_restoring_new_payload(messages):
    repository, lease, _ = messages
    original_bodies = await rows(repository, "SELECT * FROM public.bot_payload_bodies ORDER BY payload_id")
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1, text="source-canary")
    await drain(repository, lease, first)
    raw = (await source_read(sources, lease, first, 1))[0]
    await sources.payloads.forget_sources(
        lease, "forget", (SourceRef(raw["source_unit_id"]),), expected_deletion_epoch=0
    )
    assert await source_read(sources, lease, first, 1) == []
    await sources.payloads.scrub(lease)
    repeated = await receive(repository, lease, 1, text="source-canary")
    assert repeated["duplicate"]
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_units")) == 1
    assert await rows(repository, "SELECT * FROM public.bot_payload_bodies ORDER BY payload_id") == original_bodies
    # Input content now shares the source suppression barrier; immutable identity
    # remains, with neither the old column nor a payload body restoring its text.
    assert await rows(repository, "SELECT segments FROM public.bot_message_inbox") == [
        {"segments": None}
    ]
    from forest.storage.input_payloads import InputContentStore

    async with repository.store.pool.connection() as conn, conn.transaction():
        unavailable = await InputContentStore(repository.store).read_input_in(
            conn, first["event_id"], expected_deletion_epoch=1
        )
        assert unavailable["payload_state"] == "UNAVAILABLE"
        assert unavailable["author"]["principal_ref"]
        assert "source-canary" not in str(unavailable)
    async with repository.store.pool.connection() as conn:
        for query in (
            "UPDATE public.bot_memory_source_units SET source_order_seq=100",
            "DELETE FROM public.bot_memory_source_occurrences",
            "UPDATE public.bot_memory_source_eligibility SET context_revision=100",
        ):
            with pytest.raises(psycopg.Error):
                await conn.execute(query)


async def test_suppressed_latest_candidate_is_filtered_before_limit(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1)
    await receive(repository, lease, 2)
    await drain(repository, lease, first)
    candidates = await source_read(sources, lease, first, 2)
    await sources.payloads.forget_sources(
        lease,
        "forget-newest",
        (SourceRef(candidates[0]["source_unit_id"]),),
        expected_deletion_epoch=0,
    )
    survivor = await source_read(sources, lease, first, 2, limit=1)
    assert len(survivor) == 1 and survivor[0]["source_unit_id"] == candidates[1]["source_unit_id"]


async def test_legacy_backfill_resumes_bounded_cursors_and_uses_actual_batch_eligibility(
    messages, monkeypatch
):
    repository, lease, _ = messages

    class OldWriter:
        def unit_id(self, source_event_id):
            return MemorySourceStore(repository).unit_id(source_event_id)

        async def register_input_in(self, *args, **kwargs):
            pass

        async def accept_batch_in(self, *args, **kwargs):
            pass

        async def register_reply_in(self, *args, **kwargs):
            pass

    with monkeypatch.context() as patch:
        patch.setattr(repository, "_memory_sources", OldWriter)
        first = await receive(repository, lease, 1, trigger=True)
        await receive(repository, lease, 2)
        actor, batch = await drain(repository, lease, first)
        await repository.finalize_reply(
            actor,
            "legacy",
            run_id=batch["run_id"],
            expected_input_revision=1,
            reply_parts=[[{"type": "text", "text": "旧正式回答"}]],
            delivery=DeliveryStore(repository.store),
        )
    assert not await rows(repository, "SELECT * FROM public.bot_memory_source_units")
    processed, scanned = 0, 0
    for _ in range(10):
        sources = MemorySourceStore(repository)  # New runtime on every chunk.
        result = await sources.backfill(lease, limit=1)
        assert result["scanned"] <= 1
        processed += result["processed"]
        scanned += result["scanned"]
        if not result["more_possible"]:
            break
    else:
        pytest.fail("Backfill did not finish")
    assert processed == scanned == 3
    assert {
        row["order_origin"]
        for row in await rows(repository, "SELECT order_origin FROM public.bot_memory_source_units")
    } == {"RECONSTRUCTED"}
    # The official reply is registered, but its legacy lineage is incomplete.
    assert len(await source_read(sources, lease, first, 1)) == 1
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_eligibility")) == 2
    await repository.select_input(actor, "legacy-tail")
    assert len(await source_read(sources, lease, first, 2)) == 2
    await receive(repository, lease, 3)
    live = await rows(
        repository, "SELECT * FROM public.bot_memory_source_units WHERE order_origin='LIVE'"
    )
    assert len(live) == 1
    assert await sources.backfill(lease, limit=1) == {
        "processed": 0,
        "scanned": 0,
        "more_possible": False,
    }


async def test_host_expiry_at_final_guard_rolls_back_source_and_inbox_together(
    messages, monkeypatch
):
    repository, lease, _ = messages
    original = MemorySourceStore.register_input_in
    original_bodies = await rows(repository, "SELECT * FROM public.bot_payload_bodies ORDER BY payload_id")

    async def expire_after_source(self, conn, *args, **kwargs):
        value = await original(self, conn, *args, **kwargs)
        await conn.execute(
            "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()-interval '1 second'"
        )
        return value

    monkeypatch.setattr(MemorySourceStore, "register_input_in", expire_after_source)
    with pytest.raises(ForestError) as caught:
        await receive(repository, lease, 1)
    assert caught.value.code == "STALE_HOST"
    for table in (
        "bot_memory_source_units",
        "bot_memory_sources",
        "bot_message_inbox",
    ):
        assert not await rows(repository, "SELECT * FROM public." + table)
    assert await rows(repository, "SELECT * FROM public.bot_payload_bodies ORDER BY payload_id") == original_bodies


async def test_job_is_system_source_and_cancelled_prompt_is_never_retrievable(jobs):
    env, job_store, conversation = jobs
    repository = job_store.messages
    sources = MemorySourceStore(repository)
    await configure(env, job_store, conversation)
    await manual(env, job_store)
    receipt = await enqueue(env, job_store)
    other = await repository.ingest(
        env.lease, "binding-a", event(10, group="group-2"), decision="NO_TRIGGER"
    )
    # Cancel before admission into a model; real user trigger later spans history.
    await request(
        env,
        job_store,
        "cancel",
        "cancel",
        {
            "job_id": "reminder",
            "expected_revision": 1,
            "scope": "pending",
            "reason": "test cancellation",
        },
    )
    await repository.ingest(env.lease, "binding-a", event(11), decision="TRIGGER")
    actor = await owner(repository, env.lease, conversation)
    selected = await repository.select_input(actor, "after-cancel")
    assert selected["end_seq"] == 3
    raw = await source_read(sources, env.lease, {"conversation_id": conversation}, 3)
    assert all(unit["identity_origin"] != "host_job" for unit in raw)
    assert all(
        unit["source_kind"] == "GROUP" for unit in await source_read(sources, env.lease, other, 0)
    )
    unit = (
        await rows(
            repository,
            "SELECT u.*,s.source_kind,s.platform,s.visibility FROM public.bot_memory_source_units u JOIN public.bot_memory_sources s USING(bot_id,source_id) WHERE identity_origin='host_job'",
        )
    )[0]
    assert (unit["source_kind"], unit["platform"], unit["visibility"]) == (
        "SYSTEM",
        "forest",
        "source_restricted",
    )
    assert unit["principal_ref"].startswith("delegation:")
    assert (
        await rows(
            repository,
            "SELECT origin FROM public.bot_message_inbox WHERE event_id=%s",
            (receipt["event_id"],),
        )
    )[0]["origin"] == "job"


async def test_group_reply_inherits_system_dependency_restriction(jobs):
    env, job_store, conversation = jobs
    repository = job_store.messages
    sources = MemorySourceStore(repository)
    await configure(env, job_store, conversation)
    await manual(env, job_store)
    await enqueue(env, job_store)
    actor = await owner(repository, env.lease, conversation)
    selected = await repository.select_input(actor, "system-run")
    await repository.finalize_reply(
        actor,
        "system-reply",
        run_id=selected["run_id"],
        expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "系统委托的回答"}]],
        delivery=DeliveryStore(repository.store),
    )
    local = await source_read(sources, env.lease, {"conversation_id": conversation}, 2)
    assert {unit["source_kind"] for unit in local} == {"GROUP", "SYSTEM"}
    assert all(unit["kind"] != "REPLY" for unit in local)
    assert [unit["source"]["realm_id"] for unit in local if unit["source_kind"] == "SYSTEM"] == [
        "forest-internal"
    ]
    other = await repository.ingest(
        env.lease, "binding-a", event(2, group="group-2"), decision="NO_TRIGGER"
    )
    shared = await source_read(sources, env.lease, other)
    assert len(shared) == 1
    assert all(unit["kind"] == "INPUT" and unit["source_kind"] == "GROUP" for unit in shared)

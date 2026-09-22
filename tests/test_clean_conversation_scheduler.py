from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.runtime_modules import official_storage_bundle
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.conversation_scheduler import (
    ContextViewPolicy,
    ConversationSchedulerRepository,
    SchedulerContextFact,
    SchedulerPayloadRef,
)
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller


@pytest_asyncio.fixture
async def clean_scheduler():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_conversation_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = CleanBotStore(uri, "bot-conversation")
    await store.open()
    installer = SchemaBundleInstaller(store)
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    scheduler_storage = official_storage_bundle(
        "conversation.scheduler",
        "forest.conversation.scheduler",
        "forest.conversation-scheduler.repository/1",
        "storage/official/conversation_scheduler.sql",
    )
    for storage in (payload_storage, scheduler_storage):
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    scheduler = ConversationSchedulerRepository(
        store,
        namespace=scheduler_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    try:
        yield store, scheduler_storage, payloads, scheduler
    finally:
        await store.close()
        assert name.startswith("forest_conversation_") and len(name) == 52
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _message_refs(payloads, conversation: str, number: int, user: str):
    epoch = await payloads.epoch()
    source = PayloadSource(
        source_component_id="adapter.test",
        source_kind="platform.event",
        source_id=f"{conversation}/event/{number}",
        source_revision=1,
        observation_conversation_id=conversation,
        author_subject_id=user,
    )
    content_owner = ("adapter.test", "normalized.message", f"{conversation}/event/{number}")
    author_owner = ("adapter.test", "author.snapshot", f"{conversation}/{user}/{number}")
    content = await payloads.put_bytes(
        owner_component_id=content_owner[0],
        owner_kind=content_owner[1],
        owner_id=content_owner[2],
        media_type="application/json",
        content=(f'{{"parts":[{{"type":"text","text":"message {number}"}}]}}').encode(),
        sources=(source,),
        expected_deletion_epoch=epoch,
    )
    author = await payloads.put_bytes(
        owner_component_id=author_owner[0],
        owner_kind=author_owner[1],
        owner_id=author_owner[2],
        media_type="application/json",
        content=(f'{{"subject":"{user}","display":"same-name"}}').encode(),
        sources=(source,),
        expected_deletion_epoch=epoch,
    )
    return (
        SchedulerPayloadRef(content.payload_id, content.content_digest, *content_owner),
        SchedulerPayloadRef(author.payload_id, author.content_digest, *author_owner),
    )


async def _admit(scheduler, payloads, conversation: str, number: int, user: str):
    content, author = await _message_refs(payloads, conversation, number, user)
    received = datetime.now(timezone.utc)
    return await scheduler.admit_message(
        operation_id=f"admit/{conversation}/{number}",
        conversation_id=conversation,
        source_component_id="adapter.test",
        binding_ref=f"binding/{conversation}",
        platform="test",
        channel_kind="GROUP",
        channel_ref=conversation,
        thread_ref=None,
        event_id=f"event/{number}",
        source_event_ref=f"wire/{conversation}/{number}",
        event_kind="group.message",
        platform_message_ref=f"message/{number}",
        author_subject_ref=user,
        content=content,
        author=author,
        sent_at=received - timedelta(milliseconds=1),
        received_at=received,
        classification_deadline=received + timedelta(minutes=2),
    )


async def _classify(
    scheduler,
    conversation: str,
    number: int,
    decision: str,
    user: str,
):
    values = {}
    if decision == "TRIGGER":
        values = {"trigger_id": f"trigger/{conversation}/{number}", "principal_ref": user}
    return await scheduler.complete_classification(
        operation_id=f"classify/{conversation}/{number}",
        conversation_id=conversation,
        event_id=f"event/{number}",
        decision=decision,
        classifier_component_id="conversation.classifier",
        classifier_generation_id="generation/classifier/1",
        rule_revision="rules/1",
        reason_code=decision.lower(),
        route_ref=f"route/{number}" if decision in {"CONTROL_COMMAND", "ROUTED"} else None,
        **values,
    )


@pytest.mark.postgres
async def test_clean_scheduler_cuts_through_last_trigger_and_silently_drains_tail(
    clean_scheduler,
):
    _store, storage, payloads, scheduler = clean_scheduler
    lowered = storage.baseline.lower()
    for forbidden in (
        b"jsonb",
        b"model_turn",
        b"tool_call",
        b"business_command",
        b"message_handler",
        b"onebot",
    ):
        assert forbidden not in lowered

    conversation = "group/main"
    decisions = {
        1: "NO_TRIGGER",
        2: "CONTROL_COMMAND",
        3: "TRIGGER",
        4: "NO_TRIGGER",
        5: "TRIGGER",
        6: "NO_TRIGGER",
    }
    users = {1: "user/a", 2: "admin/z", 3: "user/b", 4: "user/a", 5: "user/c", 6: "user/b"}
    for number in range(1, 7):
        await _admit(scheduler, payloads, conversation, number, users[number])
        await _classify(scheduler, conversation, number, decisions[number], users[number])

    actor = await scheduler.acquire_actor(conversation, "worker/one")
    first = await scheduler.select_input(
        actor,
        "select/first",
        boundary="IDLE",
        session_ref="session/one",
        run_ref="run/one",
    )
    assert first["kind"] == "TRIGGER_INPUT"
    assert (first["start_seq"], first["end_seq"]) == (1, 5)
    assert first["event_ids"] == [f"event/{number}" for number in range(1, 6)]
    assert first["trigger_ids"] == ["trigger/group/main/3", "trigger/group/main/5"]
    assert first["triggers"] == [
        {
            "trigger_id": "trigger/group/main/3",
            "principal_ref": "user/b",
            "ingress_seq": 3,
        },
        {
            "trigger_id": "trigger/group/main/5",
            "principal_ref": "user/c",
            "ingress_seq": 5,
        },
    ]
    async with _store.pool.connection() as conn, conn.transaction():
        verified = await scheduler.verify_input_batch_occurrence_in(
            conn,
            input_batch_ref=first["input_batch_id"],
            conversation_ref=conversation,
            event_ref="event/4",
            context_revision=first["context_revision"],
        )
        assert verified["ingress_seq"] == 4
        with pytest.raises(ForestError) as caught:
            await scheduler.verify_input_batch_occurrence_in(
                conn,
                input_batch_ref=first["input_batch_id"],
                conversation_ref=conversation,
                event_ref="event/6",
                context_revision=first["context_revision"],
            )
        assert caught.value.code == "CONVERSATION_INPUT_BATCH_UNAVAILABLE"

    safe = await scheduler.select_input(
        actor,
        "select/safe",
        boundary="MODEL_SAFE_POINT",
        session_ref="session/one",
        run_ref="run/one",
    )
    assert safe["status"] == "CONTINUE"
    assert safe["selected_input_end_seq"] == 5
    assert await scheduler.select_input(
        actor,
        "select/safe",
        boundary="MODEL_SAFE_POINT",
        session_ref="session/one",
        run_ref="run/one",
    ) == safe

    snapshot = await scheduler.context_snapshot(actor)
    assert [unit["ingress_seq"] for unit in snapshot["units"]] == [1, 2, 3, 4, 5]
    assert [unit["author_subject_ref"] for unit in snapshot["units"]] == [
        "user/a",
        "admin/z",
        "user/b",
        "user/a",
        "user/c",
    ]
    assert snapshot["units"][1]["trigger_decision"] == "CONTROL_COMMAND"

    drain = await scheduler.select_input(
        actor,
        "select/end",
        boundary="SESSION_END",
        session_ref="session/one",
        run_ref="run/one",
    )
    assert drain["kind"] == "SILENT_DRAIN"
    assert (drain["start_seq"], drain["end_seq"]) == (6, 6)
    final = await scheduler.context_snapshot(actor)
    assert [unit["ingress_seq"] for unit in final["units"]] == list(range(1, 7))


@pytest.mark.postgres
async def test_context_view_compaction_is_exact_local_and_keeps_canonical_history(
    clean_scheduler,
):
    store, storage, payloads, scheduler = clean_scheduler
    conversation = "group/compact"
    policy = ContextViewPolicy(
        high_watermark_units=4,
        retain_units=2,
        policy_revision="context-view-unit-window-v1",
    )
    actor = None
    last = None
    for number in range(1, 7):
        await _admit(scheduler, payloads, conversation, number, f"user/{number % 2}")
        await _classify(scheduler, conversation, number, "NO_TRIGGER", f"user/{number % 2}")
        if actor is None:
            actor = await scheduler.acquire_actor(conversation, "worker/compact")
        last = await scheduler.select_input(
            actor,
            f"select/compact/{number}",
            boundary="IDLE",
            context_view_policy=policy,
        )
    assert last is not None
    assert last["context_view_revision"] == 1

    before_run = await scheduler.context_snapshot(actor)
    assert before_run["context_view_unit_count"] == 3
    assert before_run["context_archived_unit_count"] == 3

    await _admit(scheduler, payloads, conversation, 7, "user/1")
    await _classify(scheduler, conversation, 7, "TRIGGER", "user/1")
    last = await scheduler.select_input(
        actor,
        "select/compact/run-start",
        boundary="IDLE",
        session_ref="session/compact",
        run_ref="run/compact",
        context_view_policy=policy,
    )
    assert last["kind"] == "TRIGGER_INPUT"
    assert last["context_view_revision"] == 1

    snapshot = await scheduler.context_snapshot(actor)
    assert snapshot["context_view_revision"] == 1
    assert snapshot["context_view_unit_count"] == 4
    assert snapshot["context_archived_unit_count"] == 3
    assert snapshot["archive"] == {
        "compaction_id": snapshot["archive"]["compaction_id"],
        "published_view_revision": 1,
        "source_set_digest": snapshot["archive"]["source_set_digest"],
        "policy_revision": "context-view-unit-window-v1",
        "archived_unit_count": 3,
        "cumulative_archived_unit_count": 3,
        "first_context_revision": 1,
        "last_context_revision": 3,
    }
    assert snapshot["units"][0]["kind"] == "CONTROL"
    assert snapshot["units"][0]["source_kind"] == "context.archive"
    assert [unit["ingress_seq"] for unit in snapshot["units"][1:]] == [4, 5, 6, 7]
    assert [unit["author_subject_ref"] for unit in snapshot["units"][1:]] == [
        "user/0",
        "user/1",
        "user/0",
        "user/1",
    ]

    for number in range(8, 13):
        await _admit(scheduler, payloads, conversation, number, f"user/{number % 2}")
        await _classify(
            scheduler,
            conversation,
            number,
            "TRIGGER" if number == 12 else "NO_TRIGGER",
            f"user/{number % 2}",
        )
    extension = await scheduler.select_input(
        actor,
        "select/compact/same-run-extension",
        boundary="MODEL_SAFE_POINT",
        session_ref="session/compact",
        run_ref="run/compact",
        context_view_policy=policy,
    )
    assert extension["kind"] == "TRIGGER_INPUT"
    assert extension["context_view_revision"] == 1
    extended = await scheduler.context_snapshot(actor)
    assert extended["context_view_revision"] == 1
    assert extended["context_view_unit_count"] == 9
    assert extended["context_archived_unit_count"] == 3

    namespace = sql.Identifier(storage.spec.namespace)
    async with store.pool.connection() as conn:
        canonical_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS value FROM {}.context_units WHERE bot_id=%s "
                        "AND conversation_id=%s").format(namespace),
                (store.bot_id, conversation),
            )
        ).fetchone()
        archived = await (
            await conn.execute(
                sql.SQL("SELECT unit_id,context_revision,unit_index FROM "
                        "{}.context_compaction_units WHERE bot_id=%s AND conversation_id=%s "
                        "ORDER BY archived_index").format(namespace),
                (store.bot_id, conversation),
            )
        ).fetchall()
        compaction = await (
            await conn.execute(
                sql.SQL("SELECT archive_payload_id,archive_payload_digest FROM "
                        "{}.context_compactions WHERE bot_id=%s AND conversation_id=%s").format(
                    namespace
                ),
                (store.bot_id, conversation),
            )
        ).fetchone()
    assert canonical_count["value"] == 12
    assert [row["context_revision"] for row in archived] == [1, 2, 3]
    async with store.pool.connection() as conn, conn.transaction():
        reference = await payloads.verify_in(
            conn,
            payload_id=compaction["archive_payload_id"],
            digest=compaction["archive_payload_digest"],
            owner_component_id="conversation.scheduler",
            owner_kind="context.archive",
            owner_id=snapshot["archive"]["compaction_id"],
        )
        archive_body = json.loads(
            await payloads.read_bytes_in(
                conn,
                reference.payload_id,
                expected_deletion_epoch=reference.accepted_deletion_epoch,
            )
        )
    assert archive_body["protocol"] == "forest.context-archive-reference/1"
    assert "summary" not in archive_body
    assert archive_body["source_set_digest"] == snapshot["archive"]["source_set_digest"]

    replay = await scheduler.select_input(
        actor,
        "select/compact/run-start",
        boundary="IDLE",
        session_ref="session/compact",
        run_ref="run/compact",
        context_view_policy=ContextViewPolicy(100, 50),
    )
    assert replay == last


@pytest.mark.postgres
async def test_single_large_silent_drain_is_compacted_before_commit_returns(clean_scheduler):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/compact-bulk-silent"
    policy = ContextViewPolicy(
        high_watermark_units=4,
        retain_units=2,
        policy_revision="context-view-unit-window-v1",
    )
    for number in range(1, 101):
        await _admit(scheduler, payloads, conversation, number, f"user/{number % 3}")
        await _classify(scheduler, conversation, number, "NO_TRIGGER", f"user/{number % 3}")

    actor = await scheduler.acquire_actor(conversation, "worker/compact-bulk-silent")
    drained = await scheduler.select_input(
        actor,
        "select/compact-bulk-silent",
        boundary="IDLE",
        context_view_policy=policy,
    )
    assert drained["kind"] == "SILENT_DRAIN"
    assert (drained["start_seq"], drained["end_seq"]) == (1, 100)
    assert drained["context_view_revision"] == 1

    snapshot = await scheduler.context_snapshot(actor)
    assert snapshot["context_view_unit_count"] == 2
    assert snapshot["context_archived_unit_count"] == 98
    assert [unit["ingress_seq"] for unit in snapshot["units"][1:]] == [99, 100]
    assert [unit["author_subject_ref"] for unit in snapshot["units"][1:]] == [
        "user/0",
        "user/1",
    ]


@pytest.mark.postgres
async def test_classification_watermark_waits_for_gap_then_trigger_becomes_selectable(
    clean_scheduler,
):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/gap"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _admit(scheduler, payloads, conversation, 2, "user/b")
    second = await _classify(scheduler, conversation, 2, "TRIGGER", "user/b")
    assert second["classified_through_seq"] == 0
    actor = await scheduler.acquire_actor(conversation, "worker/gap")
    waiting = await scheduler.select_input(
        actor,
        "select/wait-gap",
        boundary="IDLE",
        session_ref="session/gap",
        run_ref="run/gap",
    )
    assert waiting["status"] == "WAIT_INPUT"
    first = await _classify(scheduler, conversation, 1, "NO_TRIGGER", "user/a")
    assert first["classified_through_seq"] == 2
    assert await scheduler.select_input(
        actor,
        "select/wait-gap",
        boundary="IDLE",
        session_ref="session/gap",
        run_ref="run/gap",
    ) == waiting
    selected = await scheduler.select_input(
        actor,
        "select/after-gap",
        boundary="IDLE",
        session_ref="session/gap",
        run_ref="run/gap",
    )
    assert selected["kind"] == "TRIGGER_INPUT"
    assert (selected["start_seq"], selected["end_seq"]) == (1, 2)


@pytest.mark.postgres
async def test_active_agent_absorbs_only_through_new_last_trigger_and_recovery_does_not_duplicate(
    clean_scheduler,
):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/active"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/active")
    initial = await scheduler.select_input(
        actor,
        "select/active/initial",
        boundary="IDLE",
        session_ref="session/active/one",
        run_ref="run/active/one",
    )
    assert initial["event_ids"] == ["event/1"]

    await _admit(scheduler, payloads, conversation, 2, "user/b")
    await _classify(scheduler, conversation, 2, "NO_TRIGGER", "user/b")
    ordinary = await scheduler.select_input(
        actor,
        "select/active/ordinary",
        boundary="MODEL_SAFE_POINT",
        session_ref="session/active/one",
        run_ref="run/active/one",
    )
    assert ordinary["status"] == "CONTINUE"
    assert ordinary["selected_input_end_seq"] == 1

    for number, decision in ((3, "TRIGGER"), (4, "NO_TRIGGER")):
        await _admit(scheduler, payloads, conversation, number, f"user/{number}")
        await _classify(scheduler, conversation, number, decision, f"user/{number}")
    safe = await scheduler.select_input(
        actor,
        "select/active/trigger",
        boundary="MODEL_SAFE_POINT",
        session_ref="session/active/one",
        run_ref="run/active/one",
    )
    assert safe["kind"] == "TRIGGER_INPUT"
    assert (safe["start_seq"], safe["end_seq"]) == (2, 3)
    assert safe["event_ids"] == ["event/2", "event/3"]

    requeued = await scheduler.requeue_run(
        actor,
        operation_id="requeue/active/one",
        session_ref="session/active/one",
        run_ref="run/active/one",
        reason_code="worker_lost",
    )
    assert requeued == ("trigger/group/active/1", "trigger/group/active/3")
    assert await scheduler.requeue_run(
        actor,
        operation_id="requeue/active/one",
        session_ref="session/active/one",
        run_ref="run/active/one",
        reason_code="worker_lost",
    ) == requeued
    recovered = await scheduler.select_input(
        actor,
        "select/active/recover",
        boundary="IDLE",
        session_ref="session/active/two",
        run_ref="run/active/two",
    )
    assert recovered["kind"] == "RECOVERY_CLAIM"
    assert recovered["event_ids"] == []
    assert recovered["trigger_ids"] == list(requeued)
    snapshot = await scheduler.context_snapshot(actor)
    assert [unit["ingress_seq"] for unit in snapshot["units"]] == [1, 2, 3]


@pytest.mark.postgres
async def test_terminal_run_failure_settles_exact_claims_and_releases_lane(
    clean_scheduler,
):
    store, storage, payloads, scheduler = clean_scheduler
    conversation = "group/protocol-failed"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/protocol-failed")
    selected = await scheduler.select_input(
        actor,
        "select/protocol-failed",
        boundary="IDLE",
        session_ref="session/protocol-failed",
        run_ref="run/protocol-failed",
    )
    assert selected["trigger_ids"] == ["trigger/group/protocol-failed/1"]
    failed = await scheduler.fail_run(
        actor,
        operation_id="fail/protocol-failed",
        session_ref="session/protocol-failed",
        run_ref="run/protocol-failed",
        reason_code="AGENT_PROTOCOL_FAILED",
    )
    assert failed == ("trigger/group/protocol-failed/1",)
    assert await scheduler.fail_run(
        actor,
        operation_id="fail/protocol-failed",
        session_ref="session/protocol-failed",
        run_ref="run/protocol-failed",
        reason_code="AGENT_PROTOCOL_FAILED",
    ) == failed
    assert (await scheduler.execution_state(actor))["active"] is False
    async with store.pool.connection() as conn:
        trigger = await (
            await conn.execute(
                sql.SQL(
                    "SELECT status,claimed_session_ref,claimed_run_ref,terminal_reason_code "
                    "FROM {}.conversation_triggers WHERE bot_id=%s AND trigger_id=%s"
                ).format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id, failed[0]),
            )
        ).fetchone()
    assert dict(trigger) == {
        "status": "FAILED",
        "claimed_session_ref": "session/protocol-failed",
        "claimed_run_ref": "run/protocol-failed",
        "terminal_reason_code": "AGENT_PROTOCOL_FAILED",
    }


@pytest.mark.postgres
async def test_terminal_run_cancellation_settles_exact_claims_and_releases_lane(
    clean_scheduler,
):
    store, storage, payloads, scheduler = clean_scheduler
    conversation = "group/protocol-cancelled"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/protocol-cancelled")
    selected = await scheduler.select_input(
        actor,
        "select/protocol-cancelled",
        boundary="IDLE",
        session_ref="session/protocol-cancelled",
        run_ref="run/protocol-cancelled",
    )
    assert selected["trigger_ids"] == ["trigger/group/protocol-cancelled/1"]
    cancelled = await scheduler.cancel_run(
        actor,
        operation_id="cancel/protocol-cancelled",
        session_ref="session/protocol-cancelled",
        run_ref="run/protocol-cancelled",
        reason_code="ADMIN_REQUESTED",
    )
    assert cancelled == ("trigger/group/protocol-cancelled/1",)
    assert await scheduler.cancel_run(
        actor,
        operation_id="cancel/protocol-cancelled",
        session_ref="session/protocol-cancelled",
        run_ref="run/protocol-cancelled",
        reason_code="ADMIN_REQUESTED",
    ) == cancelled
    assert (await scheduler.execution_state(actor))["active"] is False
    async with store.pool.connection() as conn:
        trigger = await (
            await conn.execute(
                sql.SQL(
                    "SELECT status,claimed_session_ref,claimed_run_ref,terminal_reason_code "
                    "FROM {}.conversation_triggers WHERE bot_id=%s AND trigger_id=%s"
                ).format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id, cancelled[0]),
            )
        ).fetchone()
    assert dict(trigger) == {
        "status": "CANCELLED",
        "claimed_session_ref": "session/protocol-cancelled",
        "claimed_run_ref": "run/protocol-cancelled",
        "terminal_reason_code": "ADMIN_REQUESTED",
    }


@pytest.mark.postgres
async def test_active_session_is_a_recovery_candidate_even_without_new_mail(
    clean_scheduler,
):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/restart"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/before-restart")
    selected = await scheduler.select_input(
        actor,
        "select/restart/initial",
        boundary="IDLE",
        session_ref="session/restart",
        run_ref="run/restart",
    )
    assert selected["kind"] == "TRIGGER_INPUT"

    execution = await scheduler.execution_state(actor)
    assert execution["active"]
    assert (execution["session_ref"], execution["run_ref"]) == (
        "session/restart",
        "run/restart",
    )
    assert [item["trigger_id"] for item in execution["claimed_triggers"]] == [
        "trigger/group/restart/1"
    ]
    await scheduler.release_actor(actor)

    assert conversation in await scheduler.claimable_conversations()


@pytest.mark.postgres
async def test_waiting_external_decision_defers_recovery_candidate_durably(
    clean_scheduler,
):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/deferred-approval"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/waiting")
    await scheduler.select_input(
        actor,
        "select/deferred/initial",
        boundary="IDLE",
        session_ref="session/deferred",
        run_ref="run/deferred",
    )

    deferred = await scheduler.defer_actor(
        actor,
        delay_seconds=0.2,
        reason_code="AGENT_WAITING_APPROVAL",
        reason_ref="approval/deferred",
    )
    assert deferred["resume_reason_code"] == "AGENT_WAITING_APPROVAL"
    assert deferred["resume_reason_ref"] == "approval/deferred"
    await scheduler.release_actor(actor)

    assert conversation not in await scheduler.claimable_conversations()
    with pytest.raises(ForestError) as early:
        await scheduler.acquire_actor(conversation, "worker/too-early")
    assert early.value.code == "CONVERSATION_DEFERRED"

    await asyncio.sleep(0.25)
    assert conversation in await scheduler.claimable_conversations()
    resumed = await scheduler.acquire_actor(conversation, "worker/resumed")
    state = await scheduler.execution_state(resumed)
    assert state["active"] is True
    await scheduler.release_actor(resumed)


@pytest.mark.postgres
async def test_external_decision_resumes_only_its_exact_durable_blocker(clean_scheduler):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/approval-wake"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/waiting")
    await scheduler.select_input(
        actor,
        "select/approval-wake/initial",
        boundary="IDLE",
        session_ref="session/approval-wake",
        run_ref="run/approval-wake",
    )
    await scheduler.defer_actor(
        actor,
        delay_seconds=300,
        reason_code="AGENT_WAITING_APPROVAL",
        reason_ref="approval/right",
    )
    await scheduler.release_actor(actor)

    wrong = await scheduler.resume_deferred(
        conversation,
        reason_code="AGENT_WAITING_APPROVAL",
        reason_ref="approval/wrong",
    )
    assert wrong["resumed"] is False
    assert conversation not in await scheduler.claimable_conversations()

    resumed = await scheduler.resume_deferred(
        conversation,
        reason_code="AGENT_WAITING_APPROVAL",
        reason_ref="approval/right",
    )
    assert resumed["resumed"] is True
    assert resumed["resume_not_before"] is None
    assert resumed["resume_reason_code"] is None
    assert resumed["resume_reason_ref"] is None
    assert conversation in await scheduler.claimable_conversations()


@pytest.mark.postgres
async def test_admission_and_selection_converge_and_other_lanes_do_not_share_row_lock(
    clean_scheduler,
):
    store, storage, payloads, scheduler = clean_scheduler
    conversation = "group/converge"
    content, author = await _message_refs(payloads, conversation, 1, "user/a")
    received = datetime.now(timezone.utc)
    values = dict(
        operation_id="admit/converge/1",
        conversation_id=conversation,
        source_component_id="adapter.test",
        binding_ref="binding/converge",
        platform="test",
        channel_kind="GROUP",
        channel_ref=conversation,
        thread_ref=None,
        event_id="event/1",
        source_event_ref="wire/converge/1",
        event_kind="group.message",
        platform_message_ref="message/1",
        author_subject_ref="user/a",
        content=content,
        author=author,
        received_at=received,
        classification_deadline=received + timedelta(minutes=2),
    )
    left, right = await asyncio.gather(
        scheduler.admit_message(**values), scheduler.admit_message(**values)
    )
    assert left["event_id"] == right["event_id"] == "event/1"
    assert left["ingress_seq"] == right["ingress_seq"] == 1
    assert left["duplicate"] is not right["duplicate"]
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/converge")
    selected, replay = await asyncio.gather(
        scheduler.select_input(
            actor,
            "select/converge",
            boundary="IDLE",
            session_ref="session/converge",
            run_ref="run/converge",
        ),
        scheduler.select_input(
            actor,
            "select/converge",
            boundary="IDLE",
            session_ref="session/converge",
            run_ref="run/converge",
        ),
    )
    assert selected == replay

    other = "group/independent"
    await _admit(scheduler, payloads, other, 1, "user/b")
    await _classify(scheduler, other, 1, "NO_TRIGGER", "user/b")
    other_actor = await scheduler.acquire_actor(other, "worker/other")
    async with store.pool.connection() as blocker, blocker.transaction():
        await blocker.execute(
            sql.SQL("SELECT 1 FROM {}.conversation_lanes WHERE bot_id=%s "
                    "AND conversation_id=%s FOR UPDATE").format(
                sql.Identifier(storage.spec.namespace)
            ),
            (store.bot_id, conversation),
        )
        independent = await asyncio.wait_for(
            scheduler.select_input(other_actor, "select/other", boundary="IDLE"),
            timeout=2,
        )
        assert independent["kind"] == "SILENT_DRAIN"

    async with store.pool.connection() as conn:
        counts = await (
            await conn.execute(
                sql.SQL("SELECT (SELECT count(*) FROM {}.input_batches) AS batches,"
                        "(SELECT count(*) FROM {}.context_units WHERE conversation_id=%s) AS units").format(
                    sql.Identifier(storage.spec.namespace),
                    sql.Identifier(storage.spec.namespace),
                ),
                (conversation,),
            )
        ).fetchone()
        assert counts == {"batches": 2, "units": 1}


@pytest.mark.postgres
async def test_scheduler_rejects_trigger_mutation_and_stale_actor(clean_scheduler):
    store, storage, payloads, scheduler = clean_scheduler
    conversation = "group/invariants"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/old", lease_seconds=1)
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.conversation_triggers SET ingress_seq=2 "
                        "WHERE bot_id=%s").format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id,),
            )
        await conn.rollback()
        await conn.execute(
            sql.SQL("UPDATE {}.conversation_lanes SET actor_expires_at=clock_timestamp()-"
                    "interval '1 second' WHERE bot_id=%s AND conversation_id=%s").format(
                sql.Identifier(storage.spec.namespace)
            ),
            (store.bot_id, conversation),
        )
    replacement = await scheduler.acquire_actor(conversation, "worker/new")
    assert replacement.epoch == actor.epoch + 1
    with pytest.raises(ForestError) as caught:
        await scheduler.select_input(
            actor,
            "select/stale",
            boundary="IDLE",
            session_ref="session/stale",
            run_ref="run/stale",
        )
    assert caught.value.code == "STALE_CONVERSATION_ACTOR"


@pytest.mark.postgres
async def test_final_reply_context_and_answered_triggers_commit_atomically(clean_scheduler):
    store, storage, payloads, scheduler = clean_scheduler
    conversation = "group/reply"
    await _admit(scheduler, payloads, conversation, 1, "user/a")
    await _classify(scheduler, conversation, 1, "TRIGGER", "user/a")
    actor = await scheduler.acquire_actor(conversation, "worker/reply")
    selected = await scheduler.select_input(
        actor,
        "select/reply",
        boundary="IDLE",
        session_ref="session/reply",
        run_ref="run/reply",
    )
    epoch = await payloads.epoch()
    source = PayloadSource(
        source_component_id="agent.runtime",
        source_kind="agent.run",
        source_id="run/reply",
        source_revision=1,
        observation_conversation_id=conversation,
    )
    facts = []
    for unit_id, kind, owner_kind, body in (
        ("step/reply/1", "AGENT_STEP", "closed.step", b'{"tool":"done"}'),
        ("reply/reply/1", "REPLY", "final.reply", b'{"text":"answer"}'),
    ):
        payload = await payloads.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind=owner_kind,
            owner_id=unit_id,
            media_type="application/json",
            content=body,
            sources=(source,),
            expected_deletion_epoch=epoch,
        )
        facts.append(
            SchedulerContextFact(
                unit_id=unit_id,
                kind=kind,
                source_component_id="agent.runtime",
                source_kind=owner_kind,
                source_id=unit_id,
                payload=SchedulerPayloadRef(
                    payload.payload_id,
                    payload.content_digest,
                    "agent.runtime",
                    owner_kind,
                    unit_id,
                ),
            )
        )
    committed = await scheduler.append_context(
        actor,
        operation_id="append/reply/1",
        facts=facts,
        session_ref="session/reply",
        run_ref="run/reply",
        answer_trigger_ids=selected["trigger_ids"],
        reply_ref="reply/reply/1",
    )
    assert committed["answered_trigger_ids"] == selected["trigger_ids"]
    # Exact write replay remains a receipt even when its body is later unavailable.
    async with store.pool.connection() as conn, conn.transaction():
        await payloads.forget_in(
            conn,
            operation_id="forget/reply-body",
            payload_ids=(facts[-1].payload.payload_id,),
            expected_deletion_epoch=epoch,
        )
    assert await scheduler.append_context(
        actor,
        operation_id="append/reply/1",
        facts=facts,
        session_ref="session/reply",
        run_ref="run/reply",
        answer_trigger_ids=selected["trigger_ids"],
        reply_ref="reply/reply/1",
    ) == committed
    snapshot = await scheduler.context_snapshot(actor)
    assert [unit["kind"] for unit in snapshot["units"]] == ["INPUT", "AGENT_STEP", "REPLY"]

    async with store.pool.connection() as conn:
        trigger = await (
            await conn.execute(
                sql.SQL("SELECT status,answered_reply_ref FROM {}.conversation_triggers "
                        "WHERE bot_id=%s AND trigger_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id, selected["trigger_ids"][0]),
            )
        ).fetchone()
    assert trigger == {"status": "ANSWERED", "answered_reply_ref": "reply/reply/1"}

    await _admit(scheduler, payloads, conversation, 2, "user/c")
    await _classify(scheduler, conversation, 2, "TRIGGER", "user/c")
    successor = await scheduler.select_input(
        actor,
        "select/reply/successor",
        boundary="SESSION_END",
        session_ref="session/reply",
        run_ref="run/reply",
        next_run_ref="run/reply/successor",
    )
    assert successor["kind"] == "TRIGGER_INPUT"
    assert successor["run_ref"] == "run/reply/successor"
    assert successor["trigger_ids"] == ["trigger/group/reply/2"]

    other = "group/no-reply-bypass"
    await _admit(scheduler, payloads, other, 1, "user/b")
    await _classify(scheduler, other, 1, "TRIGGER", "user/b")
    other_actor = await scheduler.acquire_actor(other, "worker/bypass")
    bypass = await scheduler.select_input(
        other_actor,
        "select/bypass",
        boundary="IDLE",
        session_ref="session/bypass",
        run_ref="run/bypass",
    )
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.conversation_triggers SET status='ANSWERED',"
                        "answered_reply_ref='reply/missing' WHERE bot_id=%s AND trigger_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id, bypass["trigger_ids"][0]),
            )


@pytest.mark.postgres
async def test_dead_classification_becomes_failed_context_instead_of_permanent_gap(
    clean_scheduler,
):
    _store, _storage, payloads, scheduler = clean_scheduler
    conversation = "group/dead-classifier"
    content, author = await _message_refs(payloads, conversation, 1, "user/a")
    received = datetime.now(timezone.utc) - timedelta(minutes=2)
    admitted = await scheduler.admit_message(
        operation_id="admit/dead-classifier/1",
        conversation_id=conversation,
        source_component_id="adapter.test",
        binding_ref="binding/dead-classifier",
        platform="test",
        channel_kind="GROUP",
        channel_ref=conversation,
        thread_ref=None,
        event_id="event/1",
        source_event_ref="wire/dead-classifier/1",
        event_kind="group.message",
        platform_message_ref="message/1",
        author_subject_ref="user/a",
        content=content,
        author=author,
        received_at=received,
        classification_deadline=received + timedelta(minutes=1),
    )
    assert admitted["classification_state"] == "PENDING"
    assert await scheduler.expire_classifications() == 1
    assert await scheduler.expire_classifications() == 0
    actor = await scheduler.acquire_actor(conversation, "worker/dead-classifier")
    drained = await scheduler.select_input(
        actor, "select/dead-classifier", boundary="IDLE"
    )
    assert drained["kind"] == "SILENT_DRAIN"
    snapshot = await scheduler.context_snapshot(actor)
    assert snapshot["classified_through_seq"] == 1
    assert snapshot["units"][0]["trigger_decision"] == "FAILED"

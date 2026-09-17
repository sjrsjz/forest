from __future__ import annotations

import asyncio
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
from forest.storage.bot import BotStore
from forest.storage.conversation_memory_uow import ConversationMemoryInputUnitOfWork
from forest.storage.conversation_scheduler import (
    ConversationSchedulerRepository,
    SchedulerPayloadRef,
)
from forest.storage.memory_graph_authority import (
    MemoryEvidenceRef,
    MemoryForgetUnitOfWork,
    MemoryGraphRepository,
    MemoryOccurrence,
    MemoryPart,
    MemoryPayloadRef,
    MemorySourceIdentity,
)
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller


@pytest_asyncio.fixture
async def clean_memory_graph():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_memory_graph_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-memory")
    await store.open()
    installer = SchemaBundleInstaller(store)
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    memory_storage = official_storage_bundle(
        "memory.graph",
        "forest.memory.graph",
        "forest.memory-graph.repository/1",
        "storage/official/memory_graph.sql",
    )
    scheduler_storage = official_storage_bundle(
        "conversation.scheduler",
        "forest.conversation.scheduler",
        "forest.conversation-scheduler.repository/1",
        "storage/official/conversation_scheduler.sql",
    )
    for storage in (payload_storage, scheduler_storage, memory_storage):
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
    accepted_batches: set[tuple[str, str, str, int]] = set()

    async def verify_batch(
        _conn,
        *,
        input_batch_ref,
        conversation_ref,
        event_ref,
        context_revision,
    ):
        identity = (input_batch_ref, conversation_ref, event_ref, context_revision)
        if identity not in accepted_batches:
            raise ForestError(
                "MEMORY_INPUT_BATCH_UNAVAILABLE", "InputBatch occurrence is unavailable", 409
            )
        return {"verified": True}

    memory = MemoryGraphRepository(
        store,
        namespace=memory_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
        deletion_epoch_guard=payloads.epoch_in,
        input_batch_verifier=verify_batch,
    )
    forget = MemoryForgetUnitOfWork(
        store,
        memory=memory,
        payload_forgetter=payloads.forget_in,
    )
    try:
        yield store, memory_storage, payloads, memory, forget, accepted_batches, scheduler
    finally:
        await store.close()
        assert name.startswith("forest_memory_graph_") and len(name) == 52
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _initialize(memory: MemoryGraphRepository):
    return await memory.initialize_domain(
        operation_id="memory/domain/initialize",
        space_id="qiyu-vl-space/v1",
        embedding_model_kind="qiyu-vl",
        model_revision_digest="sha256:" + "a" * 64,
        dimensions=1024,
        normalization="L2",
        fusion_policy_revision="fusion/1",
    )


async def _register(memory: MemoryGraphRepository, suffix: str):
    return await memory.register_source(
        operation_id=f"source/register/{suffix}",
        identity=MemorySourceIdentity(
            source_id=f"source/group/{suffix}",
            source_kind="GROUP",
            platform="onebot11",
            realm_id="qq",
            external_group_id=f"group-{suffix}",
            trunk_id=f"trunk/{suffix}",
        ),
    )


async def _put(
    payloads: KernelPayloadRepository,
    *,
    unit_id: str,
    part_id: str,
    media_type: str,
    body: bytes,
    owner_kind: str,
):
    epoch = await payloads.epoch()
    owner_id = f"{unit_id}/{part_id}"
    saved = await payloads.put_bytes(
        owner_component_id="conversation.scheduler",
        owner_kind=owner_kind,
        owner_id=owner_id,
        media_type=media_type,
        content=body,
        sources=(
            PayloadSource(
                source_component_id="conversation.scheduler",
                source_kind="context.source",
                source_id=unit_id,
                source_revision=1,
                part_id=part_id,
            ),
        ),
        expected_deletion_epoch=epoch,
    )
    return MemoryPayloadRef(
        saved.payload_id,
        saved.content_digest,
        "conversation.scheduler",
        owner_kind,
        owner_id,
    )


async def _unit_values(
    payloads: KernelPayloadRepository,
    *,
    source_suffix: str,
    unit_suffix: str,
    occurrence_suffix: str | None = None,
    occurred_at: datetime | None = None,
    mixed: bool = False,
):
    unit_id = f"unit/{unit_suffix}"
    occurrence_suffix = occurrence_suffix or unit_suffix
    author = await _put(
        payloads,
        unit_id=unit_id,
        part_id="author",
        media_type="application/json",
        body=(f'{{"subject":"user/{unit_suffix}","display":"member"}}').encode(),
        owner_kind="author.snapshot",
    )
    text = await _put(
        payloads,
        unit_id=unit_id,
        part_id="text",
        media_type="text/plain",
        body=f"message {unit_suffix}".encode(),
        owner_kind="context.part",
    )
    parts = [
        MemoryPart(
            "part/text",
            0,
            "TEXT",
            "text/plain",
            text,
            byte_length=len(f"message {unit_suffix}".encode()),
        )
    ]
    if mixed:
        image_body = b"\x89PNG\r\n\x1a\nforest-image"
        image = await _put(
            payloads,
            unit_id=unit_id,
            part_id="image",
            media_type="image/png",
            body=image_body,
            owner_kind="context.part",
        )
        parts.append(
            MemoryPart(
                "part/image",
                1,
                "IMAGE",
                "image/png",
                image,
                width=640,
                height=480,
                byte_length=len(image_body),
            )
        )
    happened = occurred_at or datetime.now(timezone.utc)
    return {
        "source_unit_id": unit_id,
        "source_id": f"source/group/{source_suffix}",
        "source_revision": 1,
        "unit_kind": "GROUP_MESSAGE",
        "author_subject_ref": f"principal/{unit_suffix}",
        "author": author,
        "parts": tuple(parts),
        "occurrence": MemoryOccurrence(
            occurrence_id=f"occurrence/{occurrence_suffix}",
            binding_ref=f"binding/{source_suffix}/{occurrence_suffix}",
            conversation_ref=f"conversation/{source_suffix}",
            event_ref=f"event/{occurrence_suffix}",
            ingress_seq=1,
            observed_at=happened,
        ),
        "occurred_at": happened,
        "expected_deletion_epoch": await payloads.epoch(),
    }


@pytest.mark.postgres
async def test_memory_domain_and_source_identity_are_lifecycle_bound(clean_memory_graph):
    store, storage, _payloads, memory, _forget, _batches, _scheduler = clean_memory_graph
    lowered = storage.baseline.lower()
    for forbidden in (b"jsonb", b"agent_run", b"send_intent", b"onebot", b"child_agent"):
        assert forbidden not in lowered

    with pytest.raises(ForestError) as caught:
        await _register(memory, "early")
    assert caught.value.code == "MEMORY_DOMAIN_MISSING"

    first = await _initialize(memory)
    assert first["embedding_model_kind"] == "qiyu-vl"
    assert (await _initialize(memory))["space_id"] == first["space_id"]
    with pytest.raises(ForestError) as caught:
        await memory.initialize_domain(
            operation_id="memory/domain/change",
            space_id="another-space",
            embedding_model_kind="text-only",
            model_revision_digest="sha256:" + "b" * 64,
            dimensions=768,
            normalization="L2",
            fusion_policy_revision="fusion/2",
        )
    assert caught.value.code == "MEMORY_SPACE_IMMUTABLE"

    left, right = await asyncio.gather(
        memory.register_source(
            operation_id="source/collision/a",
            identity=MemorySourceIdentity(
                "source/collision/a",
                "GROUP",
                platform="onebot11",
                realm_id="qq",
                external_group_id="same-group",
                trunk_id="trunk/collision/a",
            ),
        ),
        memory.register_source(
            operation_id="source/collision/b",
            identity=MemorySourceIdentity(
                "source/collision/b",
                "GROUP",
                platform="onebot11",
                realm_id="qq",
                external_group_id="same-group",
                trunk_id="trunk/collision/b",
            ),
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(item, dict) for item in (left, right)) == 1
    failure = next(item for item in (left, right) if isinstance(item, Exception))
    assert isinstance(failure, ForestError)
    assert failure.code == "MEMORY_SOURCE_CONFLICT"

    stable_identity = MemorySourceIdentity(
        "source/stable",
        "GROUP",
        platform="onebot11",
        realm_id="qq",
        external_group_id="stable-group",
        trunk_id="trunk/stable",
    )
    original = await memory.register_source(
        operation_id="source/stable/first", identity=stable_identity
    )
    duplicate = await memory.register_source(
        operation_id="source/stable/second", identity=stable_identity
    )
    assert original["duplicate_source"] is False
    assert duplicate["duplicate_source"] is True

    async with store.pool.connection() as conn:
        tables = await (
            await conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema=%s",
                (storage.spec.namespace,),
            )
        ).fetchall()
    names = {row["table_name"] for row in tables}
    assert {"memory_domains", "memory_trunks", "source_units", "relation_evidence"} <= names


@pytest.mark.postgres
async def test_multimodal_source_is_hidden_until_committed_input_batch(clean_memory_graph):
    _store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    values = await _unit_values(
        payloads,
        source_suffix="a",
        unit_suffix="mixed",
        mixed=True,
    )
    captured = await memory.capture_source_unit(operation_id="capture/mixed/one", **values)
    assert captured["modality"] == "MIXED"
    assert (await memory.query_sources())["items"] == []

    occurrence = values["occurrence"]
    batches.add(("batch/mixed", occurrence.conversation_ref, occurrence.event_ref, 7))
    await memory.qualify_occurrence(
        operation_id="eligibility/mixed",
        source_unit_id=values["source_unit_id"],
        occurrence_id=occurrence.occurrence_id,
        input_batch_ref="batch/mixed",
        context_revision=7,
        expected_deletion_epoch=await payloads.epoch(),
    )
    page = await memory.query_sources(trunk_id="trunk/a")
    assert len(page["items"]) == 1
    item = page["items"][0]
    assert item["external_group_id"] == "group-a"
    assert item["author_subject_ref"] == "principal/mixed"
    assert [part["part_kind"] for part in item["parts"]] == ["TEXT", "IMAGE"]
    assert [part["part_index"] for part in item["parts"]] == [0, 1]


@pytest.mark.postgres
async def test_duplicate_observations_converge_on_one_source_unit(clean_memory_graph):
    store, _storage, payloads, memory, _forget, _batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    base = await _unit_values(payloads, source_suffix="a", unit_suffix="shared")
    second = dict(base)
    second["occurrence"] = MemoryOccurrence(
        occurrence_id="occurrence/shared/second",
        binding_ref="binding/a/second",
        conversation_ref="conversation/a/other-binding",
        event_ref="event/shared/second",
        ingress_seq=4,
        observed_at=base["occurred_at"],
    )
    first_result, second_result = await asyncio.gather(
        memory.capture_source_unit(operation_id="capture/shared/first", **base),
        memory.capture_source_unit(operation_id="capture/shared/second", **second),
    )
    assert {first_result["duplicate_source"], second_result["duplicate_source"]} == {False, True}
    assert first_result["source_order_seq"] == second_result["source_order_seq"] == 1
    async with store.pool.connection() as conn:
        unit_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS n FROM {}.source_units").format(
                    sql.Identifier(memory.namespace)
                )
            )
        ).fetchone()
        occurrence_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS n FROM {}.source_occurrences").format(
                    sql.Identifier(memory.namespace)
                )
            )
        ).fetchone()
    assert unit_count["n"] == 1
    assert occurrence_count["n"] == 2


@pytest.mark.postgres
async def test_source_keyset_snapshot_excludes_late_qualification(clean_memory_graph):
    _store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    base_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    values = []
    for number in range(3):
        item = await _unit_values(
            payloads,
            source_suffix="a",
            unit_suffix=f"page-{number}",
            occurred_at=base_time + timedelta(minutes=number),
        )
        await memory.capture_source_unit(operation_id=f"capture/page/{number}", **item)
        occurrence = item["occurrence"]
        batches.add((f"batch/page/{number}", occurrence.conversation_ref, occurrence.event_ref, 1))
        await memory.qualify_occurrence(
            operation_id=f"eligibility/page/{number}",
            source_unit_id=item["source_unit_id"],
            occurrence_id=occurrence.occurrence_id,
            input_batch_ref=f"batch/page/{number}",
            context_revision=1,
            expected_deletion_epoch=await payloads.epoch(),
        )
        values.append(item)

    first = await memory.query_sources(limit=1, trunk_id="trunk/a")
    assert first["items"][0]["source_unit_id"] == "unit/page-2"
    late = await _unit_values(
        payloads,
        source_suffix="a",
        unit_suffix="page-late",
        occurred_at=base_time + timedelta(minutes=1, seconds=30),
    )
    await memory.capture_source_unit(operation_id="capture/page/late", **late)
    occurrence = late["occurrence"]
    batches.add(("batch/page/late", occurrence.conversation_ref, occurrence.event_ref, 1))
    await memory.qualify_occurrence(
        operation_id="eligibility/page/late",
        source_unit_id=late["source_unit_id"],
        occurrence_id=occurrence.occurrence_id,
        input_batch_ref="batch/page/late",
        context_revision=1,
        expected_deletion_epoch=await payloads.epoch(),
    )

    cursor = first["next_cursor"]
    second = await memory.query_sources(
        limit=10,
        trunk_id="trunk/a",
        snapshot_accepted_at=first["snapshot_accepted_at"],
        after_occurred_at=cursor["occurred_at"],
        after_source_unit_id=cursor["source_unit_id"],
    )
    assert [item["source_unit_id"] for item in second["items"]] == [
        "unit/page-1",
        "unit/page-0",
    ]


@pytest.mark.postgres
async def test_cross_trunk_relations_use_composite_cursor_and_source_labels(clean_memory_graph):
    store, _storage, payloads, memory, forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    await _register(memory, "b")
    left = await _unit_values(payloads, source_suffix="a", unit_suffix="left")
    right = await _unit_values(payloads, source_suffix="b", unit_suffix="right")
    for item, suffix in ((left, "left"), (right, "right")):
        await memory.capture_source_unit(operation_id=f"capture/{suffix}", **item)
        occurrence = item["occurrence"]
        batches.add((f"batch/{suffix}", occurrence.conversation_ref, occurrence.event_ref, 1))
        await memory.qualify_occurrence(
            operation_id=f"eligibility/{suffix}",
            source_unit_id=item["source_unit_id"],
            occurrence_id=occurrence.occurrence_id,
            input_batch_ref=f"batch/{suffix}",
            context_revision=1,
            expected_deletion_epoch=await payloads.epoch(),
        )
    evidence = (
        MemoryEvidenceRef("unit/left", 1, "part/text"),
        MemoryEvidenceRef("unit/right", 1, "part/text"),
    )
    for number in (1, 2):
        relation = await memory.add_relation(
            operation_id=f"relation/add/{number}",
            contribution_id=f"contribution/{number}",
            relation_kind="ASSOCIATES_WITH",
            left_kind="SOURCE_UNIT",
            left_id="unit/right",
            right_kind="SOURCE_UNIT",
            right_id="unit/left",
            evidence=evidence,
            expected_deletion_epoch=await payloads.epoch(),
            origin_kind="MANUAL",
            weight_micros=900_000 + number,
        )
        assert relation["left_id"] == "unit/left"
        assert relation["right_id"] == "unit/right"

    first = await memory.query_relations(
        endpoint_kind="SOURCE_UNIT",
        endpoint_id="unit/left",
        limit=1,
    )
    assert first["has_more"] is True
    assert {item["external_group_id"] for item in first["items"][0]["evidence"]} == {
        "group-a",
        "group-b",
    }
    second = await memory.query_relations(
        endpoint_kind="SOURCE_UNIT",
        endpoint_id="unit/left",
        limit=1,
        snapshot_created_at=first["snapshot_created_at"],
        after_relation_id=first["next_cursor"]["relation_id"],
        after_contribution_id=first["next_cursor"]["contribution_id"],
    )
    assert len(second["items"]) == 1
    assert second["items"][0]["contribution_id"] != first["items"][0]["contribution_id"]

    forgotten = await forget.apply(
        operation_id="forget/left",
        source_unit_ids=("unit/left",),
        expected_deletion_epoch=0,
        reason_code="ADMIN_REQUEST",
    )
    assert forgotten["deletion_epoch"] == 1
    assert forgotten["payload_selected_count"] >= 2
    assert (
        await memory.query_relations(
            endpoint_kind="SOURCE_UNIT",
            endpoint_id="unit/left",
        )
    )["items"] == []
    administrative = await memory.query_relations(
        endpoint_kind="SOURCE_UNIT",
        endpoint_id="unit/left",
        eligible_only=False,
    )
    assert len(administrative["items"]) == 2
    assert all(
        any(evidence_item["eligibility_state"] == "SUPPRESSED" for evidence_item in item["evidence"])
        for item in administrative["items"]
    )

    replay = await forget.apply(
        operation_id="forget/left",
        source_unit_ids=("unit/left",),
        expected_deletion_epoch=0,
        reason_code="ADMIN_REQUEST",
    )
    assert replay["duplicate"] is True
    with pytest.raises(ForestError) as caught:
        await memory.capture_source_unit(operation_id="capture/left/late-old-epoch", **left)
    assert caught.value.code == "DELETION_CONFLICT"
    late = dict(left)
    late["expected_deletion_epoch"] = 1
    late["occurrence"] = MemoryOccurrence(
        "occurrence/left/late",
        "binding/a/late",
        "conversation/a",
        "event/left/late",
        9,
        datetime.now(timezone.utc),
    )
    with pytest.raises(ForestError) as caught:
        await memory.capture_source_unit(operation_id="capture/left/late-new-epoch", **late)
    assert caught.value.code == "MEMORY_SOURCE_SUPPRESSED"


@pytest.mark.postgres
async def test_eligibility_participant_rolls_back_with_caller_transaction(clean_memory_graph):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    values = await _unit_values(payloads, source_suffix="a", unit_suffix="rollback")
    await memory.capture_source_unit(operation_id="capture/rollback", **values)
    occurrence = values["occurrence"]
    batches.add(("batch/rollback", occurrence.conversation_ref, occurrence.event_ref, 3))

    with pytest.raises(RuntimeError, match="rollback"):
        async with store.pool.connection() as conn, conn.transaction():
            await memory.qualify_occurrence_in(
                conn,
                operation_id="eligibility/rollback",
                source_unit_id=values["source_unit_id"],
                occurrence_id=occurrence.occurrence_id,
                input_batch_ref="batch/rollback",
                context_revision=3,
                expected_deletion_epoch=await payloads.epoch_in(conn),
            )
            raise RuntimeError("rollback")
    assert (await memory.query_sources())["items"] == []
    await memory.qualify_occurrence(
        operation_id="eligibility/rollback",
        source_unit_id=values["source_unit_id"],
        occurrence_id=occurrence.occurrence_id,
        input_batch_ref="batch/rollback",
        context_revision=3,
        expected_deletion_epoch=await payloads.epoch(),
    )
    assert len((await memory.query_sources())["items"]) == 1


@pytest.mark.postgres
async def test_forget_unit_of_work_rolls_back_payload_revocation_on_memory_failure(
    clean_memory_graph,
):
    store, _storage, payloads, memory, forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    values = await _unit_values(payloads, source_suffix="a", unit_suffix="forget-rollback")
    await memory.capture_source_unit(operation_id="capture/forget-rollback", **values)
    occurrence = values["occurrence"]
    batches.add(("batch/forget-rollback", occurrence.conversation_ref, occurrence.event_ref, 1))
    await memory.qualify_occurrence(
        operation_id="eligibility/forget-rollback",
        source_unit_id=values["source_unit_id"],
        occurrence_id=occurrence.occurrence_id,
        input_batch_ref="batch/forget-rollback",
        context_revision=1,
        expected_deletion_epoch=0,
    )
    original_suppress = memory.suppress_sources_in

    async def fail_suppression(*_args, **_values):
        raise ForestError("MEMORY_TEST_FAILURE", "fail after payload participant", 500)

    memory.suppress_sources_in = fail_suppression
    try:
        with pytest.raises(ForestError) as caught:
            await forget.apply(
                operation_id="forget/rollback",
                source_unit_ids=(values["source_unit_id"],),
                expected_deletion_epoch=0,
                reason_code="TEST",
            )
        assert caught.value.code == "MEMORY_TEST_FAILURE"
    finally:
        memory.suppress_sources_in = original_suppress

    assert await payloads.epoch() == 0
    async with store.pool.connection() as conn, conn.transaction():
        body = await payloads.read_bytes_in(
            conn,
            values["author"].payload_id,
            expected_deletion_epoch=0,
        )
    assert b"forget-rollback" in body
    assert len((await memory.query_sources())["items"]) == 1


@pytest.mark.postgres
async def test_conversation_cut_and_memory_eligibility_commit_as_one_unit_of_work(
    clean_memory_graph,
):
    store, _storage, payloads, memory, _forget, _batches, scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "a")
    actual_memory = MemoryGraphRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        deletion_epoch_guard=payloads.epoch_in,
        input_batch_verifier=scheduler.verify_input_batch_occurrence_in,
    )
    unit_of_work = ConversationMemoryInputUnitOfWork(
        store,
        scheduler=scheduler,
        memory=actual_memory,
        deletion_epoch_guard=payloads.epoch_in,
    )

    async def admit(number: int, *, capture: bool):
        suffix = f"uow-{number}"
        values = await _unit_values(
            payloads,
            source_suffix="a",
            unit_suffix=suffix,
            occurrence_suffix=suffix,
        )
        message = await _put(
            payloads,
            unit_id=f"scheduler/{suffix}",
            part_id="content",
            media_type="application/json",
            body=(f'{{"parts":[{{"type":"text","text":"{suffix}"}}]}}').encode(),
            owner_kind="normalized.message",
        )
        author = await _put(
            payloads,
            unit_id=f"scheduler/{suffix}",
            part_id="author",
            media_type="application/json",
            body=(f'{{"subject":"principal/{suffix}"}}').encode(),
            owner_kind="author.snapshot",
        )
        received = datetime.now(timezone.utc)
        await scheduler.admit_message(
            operation_id=f"admit/{suffix}",
            conversation_id="conversation/a",
            source_component_id="adapter.onebot",
            binding_ref="binding/a",
            platform="onebot11",
            channel_ref="group-a",
            thread_ref=None,
            event_id=f"event/{suffix}",
            source_event_ref=f"wire/{suffix}",
            event_kind="group.message",
            platform_message_ref=f"message/{suffix}",
            author_subject_ref=f"principal/{suffix}",
            content=SchedulerPayloadRef(
                message.payload_id,
                message.digest,
                message.owner_component_id,
                message.owner_kind,
                message.owner_id,
            ),
            author=SchedulerPayloadRef(
                author.payload_id,
                author.digest,
                author.owner_component_id,
                author.owner_kind,
                author.owner_id,
            ),
            sent_at=received,
            received_at=received,
            classification_deadline=received + timedelta(minutes=1),
        )
        await scheduler.complete_classification(
            operation_id=f"classify/{suffix}",
            conversation_id="conversation/a",
            event_id=f"event/{suffix}",
            decision="NO_TRIGGER",
            classifier_component_id="conversation.classifier",
            classifier_generation_id="generation/classifier/1",
            rule_revision="rules/1",
            reason_code="ordinary",
        )
        if capture:
            await memory.capture_source_unit(operation_id=f"capture/{suffix}", **values)
        return values

    await admit(1, capture=True)
    actor = await scheduler.acquire_actor("conversation/a", "worker/memory-uow")
    first = await unit_of_work.select_input(actor, "selection/uow/1", boundary="IDLE")
    assert first["kind"] == "SILENT_DRAIN"
    assert first["memory_qualified_count"] == 1
    assert [item["source_unit_id"] for item in (await memory.query_sources())["items"]] == [
        "unit/uow-1"
    ]

    missing = await admit(2, capture=False)
    with pytest.raises(ForestError) as caught:
        await unit_of_work.select_input(actor, "selection/uow/2", boundary="IDLE")
    assert caught.value.code == "MEMORY_OCCURRENCE_MISSING"
    snapshot = await scheduler.context_snapshot(actor)
    assert snapshot["context_applied_seq"] == 1

    await memory.capture_source_unit(operation_id="capture/uow-2", **missing)
    second = await unit_of_work.select_input(actor, "selection/uow/2", boundary="IDLE")
    assert second["memory_qualified_count"] == 1
    assert {item["source_unit_id"] for item in (await memory.query_sources())["items"]} == {
        "unit/uow-1",
        "unit/uow-2",
    }

from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.embeddings import (
    SILICONFLOW_VL_MODEL,
    EmbeddingProfile,
    EmbeddingResult,
    EmbeddingVector,
    prepare_embedding_request,
)
from forest.errors import ForestError
from forest.memory_embedding_worker import (
    EmbeddingBatchItem,
    EmbeddingBudgetClaim,
    MemoryEmbeddingWorker,
)
from forest.runtime_modules import official_storage_bundle
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.conversation_memory_uow import ConversationMemoryInputUnitOfWork
from forest.storage.conversation_scheduler import (
    ConversationSchedulerRepository,
    SchedulerPayloadRef,
)
from forest.storage.memory_embedding_budget import MemoryEmbeddingBudgetRepository
from forest.storage.memory_graph_authority import (
    MemoryEvidenceRef,
    MemoryForgetUnitOfWork,
    MemoryGraphRepository,
    MemoryOccurrence,
    MemoryPart,
    MemoryPayloadRef,
    MemorySourceIdentity,
)
from forest.storage.memory_retrieval_authority import (
    MemoryRetrievalRepository,
    RetrievalQueryPart,
)
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller


class _DirectEmbeddingProvider:
    """Test transport adapter; production projection belongs to the Provider module."""

    def __init__(self, store, profile, client):
        self.client_store = store
        self.profile = profile
        self.client = client

    async def encode(
        self,
        inputs,
        *,
        request_id,
        transport_attempt_id,
        purpose,
        permit_in,
    ):
        prepared = prepare_embedding_request(
            self.profile,
            inputs,
            request_id=request_id,
            transport_attempt_id=transport_attempt_id,
            purpose=purpose,
        )

        async def permitcheck(_prepared):
            async with self.client_store.pool.connection() as conn, conn.transaction():
                await permit_in(conn, request=type("Request", (), {"prepared": prepared})())

        return await self.client.dispatch(prepared, permitcheck=permitcheck)


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
    store = CleanBotStore(uri, "bot-memory")
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
    text_body: bytes | None = None,
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
    text_content = text_body if text_body is not None else f"message {unit_suffix}".encode()
    text = await _put(
        payloads,
        unit_id=unit_id,
        part_id="text",
        media_type="text/plain",
        body=text_content,
        owner_kind="context.part",
    )
    parts = [
        MemoryPart(
            "part/text",
            0,
            "TEXT",
            "text/plain",
            text,
            byte_length=len(text_content),
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
async def test_memory_browse_and_relation_plans_scale_to_100k_sources(clean_memory_graph):
    store, _storage, _payloads, memory, _forget, _batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    for number in range(20):
        await _register(memory, f"bulk-{number:02d}")

    namespace = sql.Identifier(memory.namespace)
    digest = "sha256:" + "b" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.source_units("
                "bot_id,source_unit_id,capture_operation_id,input_digest,source_id,trunk_id,"
                "source_order_seq,source_revision,unit_kind,modality,part_count,"
                "content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,"
                "eligibility_revision,eligible_at,occurred_at,accepted_at) "
                "SELECT %s,'bulk/unit/'||g,'bulk/capture/'||g,%s,"
                "'source/group/bulk-'||lpad(((g-1)%%20)::text,2,'0'),"
                "'trunk/bulk-'||lpad(((g-1)%%20)::text,2,'0'),"
                "((g-1)/20)+1,1,'GROUP_MESSAGE',CASE WHEN g%%10=0 THEN 'IMAGE' ELSE 'TEXT' END,"
                "1,%s,'principal/'||lpad((g%%100)::text,2,'0'),'bulk/author/'||g,%s,"
                "'conversation.scheduler','author.snapshot','bulk/author/'||g,0,'ELIGIBLE',1,"
                "TIMESTAMPTZ '2026-01-01 00:00:00+00',"
                "TIMESTAMPTZ '2025-01-01 00:00:00+00'+g*INTERVAL '1 millisecond',"
                "TIMESTAMPTZ '2026-01-01 00:00:00+00' "
                "FROM generate_series(1,100000) AS series(g)"
            ).format(namespace),
            (store.bot_id, digest, digest, digest),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.source_parts("
                "bot_id,source_unit_id,part_id,part_index,part_kind,media_type,payload_id,"
                "payload_digest,payload_owner_component_id,payload_owner_kind,payload_owner_id,"
                "byte_length) SELECT %s,'bulk/unit/'||g,'part/text',0,'TEXT','text/plain',"
                "'bulk/payload/'||g,%s,'conversation.scheduler','context.part',"
                "'bulk/unit/'||g||'/part/text',16 FROM generate_series(1,100000) AS series(g)"
            ).format(namespace),
            (store.bot_id, digest),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.relation_contributions("
                "bot_id,contribution_id,operation_id,input_digest,relation_id,relation_kind,"
                "left_kind,left_id,right_kind,right_id,origin_kind,weight_micros,"
                "admitted_deletion_epoch,active,created_at) SELECT %s,"
                "'bulk/contribution/'||lpad(g::text,8,'0'),'bulk/relation-op/'||g,%s,"
                "'bulk/relation/'||lpad(g::text,8,'0'),'ASSOCIATES_WITH',"
                "CASE WHEN g%%200=1 THEN 'SOURCE_UNIT' ELSE 'ENTITY' END,"
                "CASE WHEN g%%200=0 THEN 'topic/scale' WHEN g%%200=1 THEN 'bulk/unit/'||g "
                "ELSE 'topic/'||g END,"
                "CASE WHEN g%%200=1 THEN 'ENTITY' ELSE 'SOURCE_UNIT' END,"
                "CASE WHEN g%%200=1 THEN 'topic/scale' ELSE 'bulk/unit/'||g END,"
                "'RULE',500000,0,true,TIMESTAMPTZ '2026-01-01 00:00:00+00' "
                "FROM generate_series(1,20000) AS series(g)"
            ).format(namespace),
            (store.bot_id, digest),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.relation_evidence("
                "bot_id,contribution_id,evidence_index,source_unit_id,source_revision,part_id) "
                "SELECT %s,'bulk/contribution/'||lpad(g::text,8,'0'),0,"
                "'bulk/unit/'||g,1,'part/text' FROM generate_series(1,20000) AS series(g)"
            ).format(namespace),
            (store.bot_id,),
        )
        await conn.execute(sql.SQL("ANALYZE {}.source_units").format(namespace))
        await conn.execute(sql.SQL("ANALYZE {}.relation_contributions").format(namespace))
        await conn.execute(sql.SQL("ANALYZE {}.relation_evidence").format(namespace))

        source_plan_row = await (
            await conn.execute(
                sql.SQL(
                    "EXPLAIN (FORMAT JSON) SELECT u.* FROM {}.source_units u "
                    "JOIN {}.memory_sources s ON s.bot_id=u.bot_id AND s.source_id=u.source_id "
                    "WHERE u.bot_id=%s AND u.eligibility_state='ELIGIBLE' "
                    "AND u.accepted_at<=%s AND u.eligible_at<=%s AND u.trunk_id=%s "
                    "AND u.author_subject_ref=%s AND u.modality=%s AND u.unit_kind=%s "
                    "ORDER BY u.occurred_at DESC,u.source_unit_id DESC LIMIT 51"
                ).format(namespace, namespace),
                (
                    store.bot_id,
                    datetime(2026, 2, 1, tzinfo=timezone.utc),
                    datetime(2026, 2, 1, tzinfo=timezone.utc),
                    "trunk/bulk-02",
                    "principal/43",
                    "TEXT",
                    "GROUP_MESSAGE",
                ),
            )
        ).fetchone()
        relation_plan_row = await (
            await conn.execute(
                sql.SQL(
                    "EXPLAIN (FORMAT JSON) SELECT c.* FROM {}.relation_contributions c "
                    "WHERE c.bot_id=%s AND ((c.left_kind=%s AND c.left_id=%s) OR "
                    "(c.right_kind=%s AND c.right_id=%s)) AND c.active=true "
                    "AND c.created_at<=%s AND NOT EXISTS (SELECT 1 FROM {}.relation_evidence e0 "
                    "JOIN {}.source_units u0 ON u0.bot_id=e0.bot_id "
                    "AND u0.source_unit_id=e0.source_unit_id WHERE e0.bot_id=c.bot_id "
                    "AND e0.contribution_id=c.contribution_id "
                    "AND u0.eligibility_state<>'ELIGIBLE') "
                    "ORDER BY c.relation_id,c.contribution_id LIMIT 41"
                ).format(namespace, namespace, namespace),
                (
                    store.bot_id,
                    "ENTITY",
                    "topic/scale",
                    "ENTITY",
                    "topic/scale",
                    datetime(2026, 2, 1, tzinfo=timezone.utc),
                ),
            )
        ).fetchone()

    def plan_nodes(plan):
        yield plan
        for child in plan.get("Plans", ()):
            yield from plan_nodes(child)

    source_nodes = tuple(plan_nodes(source_plan_row["QUERY PLAN"][0]["Plan"]))
    assert not any(
        node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "source_units"
        for node in source_nodes
    )
    assert any(
        node.get("Index Name")
        in {"source_units_trunk_idx", "source_units_author_idx", "source_units_modality_idx"}
        for node in source_nodes
    )
    relation_nodes = tuple(plan_nodes(relation_plan_row["QUERY PLAN"][0]["Plan"]))
    relation_indexes = {node.get("Index Name") for node in relation_nodes}
    assert {"relation_left_idx", "relation_right_idx"} <= relation_indexes

    first = await memory.query_sources(
        limit=50,
        trunk_id="trunk/bulk-02",
        author_subject_ref="principal/43",
        modality="TEXT",
        unit_kind="GROUP_MESSAGE",
    )
    second = await memory.query_sources(
        limit=50,
        trunk_id="trunk/bulk-02",
        author_subject_ref="principal/43",
        modality="TEXT",
        unit_kind="GROUP_MESSAGE",
        snapshot_accepted_at=first["snapshot_accepted_at"],
        after_occurred_at=first["next_cursor"]["occurred_at"],
        after_source_unit_id=first["next_cursor"]["source_unit_id"],
    )
    first_ids = [item["source_unit_id"] for item in first["items"]]
    second_ids = [item["source_unit_id"] for item in second["items"]]
    assert len(first_ids) == len(second_ids) == 50
    assert set(first_ids).isdisjoint(second_ids)
    assert all(item["external_group_id"] == "group-bulk-02" for item in first["items"])
    assert all(item["author_subject_ref"] == "principal/43" for item in first["items"])
    assert all(item["parts"][0]["part_kind"] == "TEXT" for item in first["items"])

    relations = await memory.query_relations(
        endpoint_kind="ENTITY",
        endpoint_id="topic/scale",
        limit=40,
    )
    assert relations["has_more"] is True
    assert len(relations["items"]) == 40
    assert all(
        item["evidence"][0]["external_group_id"].startswith("group-bulk-")
        and item["evidence"][0]["trunk_id"].startswith("trunk/bulk-")
        for item in relations["items"]
    )
    relation_cursor = relations["next_cursor"]
    later_relations = await memory.query_relations(
        endpoint_kind="ENTITY",
        endpoint_id="topic/scale",
        limit=40,
        snapshot_created_at=relations["snapshot_created_at"],
        after_relation_id=relation_cursor["relation_id"],
        after_contribution_id=relation_cursor["contribution_id"],
    )
    assert len(later_relations["items"]) == 40
    assert {item["contribution_id"] for item in relations["items"]}.isdisjoint(
        item["contribution_id"] for item in later_relations["items"]
    )


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
            channel_kind="GROUP",
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
    second = await unit_of_work.select_input(actor, "selection/uow/2", boundary="IDLE")
    assert second["kind"] == "SILENT_DRAIN"
    assert second["memory_qualified_count"] == 0
    assert second["memory_pending_count"] == 1
    snapshot = await scheduler.context_snapshot(actor)
    assert snapshot["context_applied_seq"] == 2

    late = await actual_memory.capture_source_unit(operation_id="capture/uow-2", **missing)
    assert late["eligibility_state"] == "ELIGIBLE"
    assert {item["source_unit_id"] for item in (await memory.query_sources())["items"]} == {
        "unit/uow-1",
        "unit/uow-2",
    }

    racing = await admit(3, capture=False)
    selection, _capture = await asyncio.gather(
        unit_of_work.select_input(actor, "selection/uow/3", boundary="IDLE"),
        actual_memory.capture_source_unit(operation_id="capture/uow-3", **racing),
    )
    assert selection["memory_qualified_count"] + selection["memory_pending_count"] == 1
    assert {item["source_unit_id"] for item in (await memory.query_sources())["items"]} == {
        "unit/uow-1",
        "unit/uow-2",
        "unit/uow-3",
    }
    async with store.pool.connection() as conn:
        unresolved = await (
            await conn.execute(
                sql.SQL(
                    "SELECT count(*) AS n FROM {}.pending_input_events "
                    "WHERE state='PENDING_CAPTURE'"
                ).format(sql.Identifier(memory.namespace))
            )
        ).fetchone()
    assert unresolved["n"] == 0


async def _capture_and_qualify(
    memory,
    payloads,
    batches,
    values,
    *,
    suffix: str,
    context_revision: int = 1,
):
    await memory.capture_source_unit(operation_id=f"capture/retrieval/{suffix}", **values)
    occurrence = values["occurrence"]
    batch = f"batch/retrieval/{suffix}"
    batches.add((batch, occurrence.conversation_ref, occurrence.event_ref, context_revision))
    await memory.qualify_occurrence(
        operation_id=f"eligibility/retrieval/{suffix}",
        source_unit_id=values["source_unit_id"],
        occurrence_id=occurrence.occurrence_id,
        input_batch_ref=batch,
        context_revision=context_revision,
        expected_deletion_epoch=await payloads.epoch(),
    )
    return batch


def _vector(axis: int):
    value = [0.0] * 1024
    value[axis] = 1.0
    return value


async def _embed(payloads, retrieval, facet, vector, *, suffix: str):
    attempt_id = f"embedding/attempt/{suffix}"
    permit = await retrieval.prepare_attempt(
        operation_id=f"embedding/prepare/{suffix}",
        attempt_id=attempt_id,
        facet_id=facet["facet_id"],
        executor_component_id="embedding.qiyu-vl",
        executor_generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    await retrieval.mark_handoff(permit, handoff_operation_id=f"embedding/handoff/{suffix}")
    body = (f'{{"attempt_id":"{attempt_id}","dimensions":1024}}').encode()
    saved = await payloads.put_bytes(
        owner_component_id="embedding.qiyu-vl",
        owner_kind="embedding.result",
        owner_id=attempt_id,
        media_type="application/json",
        content=body,
        sources=(
            PayloadSource(
                source_component_id="memory.graph",
                source_kind="embedding.facet",
                source_id=facet["facet_id"],
                source_revision=1,
            ),
        ),
        expected_deletion_epoch=permit.accepted_deletion_epoch,
    )
    attempt = await retrieval.settle_attempt(
        permit,
        settlement_operation_id=f"embedding/settle/{suffix}",
        outcome="SUCCEEDED",
        expected_deletion_epoch=permit.accepted_deletion_epoch,
        result=MemoryPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "embedding.qiyu-vl",
            "embedding.result",
            attempt_id,
        ),
        vector=vector,
    )
    await retrieval.publish_index_ref(
        facet_id=facet["facet_id"],
        index_ref_id=facet["facet_id"],
        expected_vector_digest=attempt["vector_digest"],
        expected_deletion_epoch=permit.accepted_deletion_epoch,
    )
    indexed = getattr(retrieval, "_test_index_vectors", None)
    if indexed is None:
        indexed = {}
        retrieval._test_index_vectors = indexed
    indexed[facet["facet_id"]] = (facet["purpose"], tuple(vector))
    return attempt


async def _freeze_bundle(retrieval, **values):
    query = await retrieval.query_view(values["query_id"])
    if query["state"] == "FROZEN":
        return await retrieval.read_bundle(
            query["bundle_id"],
            expected_deletion_epoch=values["expected_deletion_epoch"],
        )
    plan = await retrieval.prepare_vector_search(values["query_id"])
    indexed = getattr(retrieval, "_test_index_vectors", {})
    documents = {
        ref_id: vector for ref_id, (purpose, vector) in indexed.items() if purpose == "DOCUMENT"
    }
    candidate_sets = {}
    for facet in plan["facets"]:
        query_vector = indexed[facet["index_ref_id"]][1]
        ranked = sorted(
            (
                {
                    "ref_id": ref_id,
                    "score": sum(left * right for left, right in zip(query_vector, vector, strict=True)),
                }
                for ref_id, vector in documents.items()
            ),
            key=lambda item: (-item["score"], item["ref_id"]),
        )
        candidate_sets[facet["facet_id"]] = tuple(ranked)
    return await retrieval.freeze_bundle(
        **values,
        index_snapshot_at=plan["snapshot_at"],
        candidate_sets=candidate_sets,
    )


@pytest.mark.postgres
async def test_text_query_retrieves_image_with_explicit_cross_trunk_source(clean_memory_graph):
    _store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "query-a")
    await _register(memory, "evidence-b")
    query_source = await _unit_values(
        payloads,
        source_suffix="query-a",
        unit_suffix="query-text",
    )
    evidence_source = await _unit_values(
        payloads,
        source_suffix="evidence-b",
        unit_suffix="evidence-image",
        mixed=True,
    )
    query_batch = await _capture_and_qualify(
        memory, payloads, batches, query_source, suffix="query-text"
    )
    await _capture_and_qualify(
        memory, payloads, batches, evidence_source, suffix="evidence-image"
    )
    retrieval = MemoryRetrievalRepository(
        memory.store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    documents = await retrieval.prepare_source_facets(
        operation_id="facets/evidence-image",
        source_unit_id="unit/evidence-image",
        expected_deletion_epoch=0,
    )
    query = await retrieval.register_query(
        operation_id="query/register/text-image",
        query_id="query/text-image",
        current_source_id="source/group/query-a",
        input_batch_ref=query_batch,
        context_revision=1,
        parts=(
            RetrievalQueryPart(
                "unit/query-text",
                1,
                query_source["occurrence"].occurrence_id,
                "part/text",
            ),
        ),
        expected_deletion_epoch=0,
    )
    for facet in documents["facets"]:
        await _embed(
            payloads,
            retrieval,
            facet,
            _vector(0 if facet["modality"] == "IMAGE" else 1),
            suffix="document-" + facet["modality"].lower(),
        )
    await _embed(payloads, retrieval, query["facets"][0], _vector(0), suffix="query-text")

    bundle = await _freeze_bundle(retrieval,
        operation_id="bundle/freeze/text-image",
        bundle_id="bundle/text-image",
        query_id="query/text-image",
        expected_deletion_epoch=0,
    )
    assert bundle["query_mode"] == "TEXT"
    assert len(bundle["evidence"]) == 1
    evidence = bundle["evidence"][0]
    assert evidence["external_group_id"] == "group-evidence-b"
    assert evidence["trunk_id"] == "trunk/evidence-b"
    assert evidence["author_subject_ref"] == "principal/evidence-image"
    assert evidence["is_current_group"] is False
    assert [part["part_kind"] for part in evidence["parts"]] == ["TEXT", "IMAGE"]
    assert evidence["matches"][0]["query_modality"] == "TEXT"
    assert evidence["matches"][0]["document_modality"] == "IMAGE"
    query_view = await retrieval.query_view("query/text-image")
    assert query_view["state"] == "FROZEN"
    assert {facet["state"] for facet in query_view["facets"]} == {"SUPPRESSED"}
    deletion = await retrieval.next_index_deletion()
    assert deletion["index_ref_id"] == query["facets"][0]["facet_id"]
    assert deletion["deletion_epoch"] == 0
    await retrieval.settle_index_deletion(deletion["index_ref_id"])


@pytest.mark.postgres
async def test_mixed_query_waits_for_image_and_freezes_late_index_results(clean_memory_graph):
    _store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    for suffix in ("query", "first", "late"):
        await _register(memory, suffix)
    query_source = await _unit_values(
        payloads,
        source_suffix="query",
        unit_suffix="mixed-query",
        mixed=True,
    )
    first_source = await _unit_values(
        payloads,
        source_suffix="first",
        unit_suffix="first-text",
    )
    late_source = await _unit_values(
        payloads,
        source_suffix="late",
        unit_suffix="late-text",
    )
    query_batch = await _capture_and_qualify(
        memory, payloads, batches, query_source, suffix="mixed-query"
    )
    await _capture_and_qualify(memory, payloads, batches, first_source, suffix="first-text")
    await _capture_and_qualify(memory, payloads, batches, late_source, suffix="late-text")
    retrieval = MemoryRetrievalRepository(
        memory.store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    first = await retrieval.prepare_source_facets(
        operation_id="facets/first", source_unit_id="unit/first-text", expected_deletion_epoch=0
    )
    late = await retrieval.prepare_source_facets(
        operation_id="facets/late", source_unit_id="unit/late-text", expected_deletion_epoch=0
    )
    query = await retrieval.register_query(
        operation_id="query/register/mixed",
        query_id="query/mixed",
        current_source_id="source/group/query",
        input_batch_ref=query_batch,
        context_revision=1,
        parts=(
            RetrievalQueryPart(
                "unit/mixed-query", 1, query_source["occurrence"].occurrence_id, "part/text"
            ),
            RetrievalQueryPart(
                "unit/mixed-query", 1, query_source["occurrence"].occurrence_id, "part/image"
            ),
        ),
        expected_deletion_epoch=0,
    )
    await _embed(payloads, retrieval, first["facets"][0], _vector(0), suffix="first")
    query_by_modality = {facet["modality"]: facet for facet in query["facets"]}
    await _embed(
        payloads, retrieval, query_by_modality["TEXT"], _vector(1), suffix="mixed-query-text"
    )
    with pytest.raises(ForestError) as caught:
        await _freeze_bundle(retrieval,
            operation_id="bundle/freeze/mixed",
            bundle_id="bundle/mixed",
            query_id="query/mixed",
            expected_deletion_epoch=0,
        )
    assert caught.value.code == "MEMORY_QUERY_NOT_READY"
    await _embed(
        payloads, retrieval, query_by_modality["IMAGE"], _vector(0), suffix="mixed-query-image"
    )
    bundle = await _freeze_bundle(retrieval,
        operation_id="bundle/freeze/mixed",
        bundle_id="bundle/mixed",
        query_id="query/mixed",
        expected_deletion_epoch=0,
    )
    assert bundle["query_mode"] == "MIXED"
    assert bundle["evidence"][0]["source_unit_id"] == "unit/first-text"
    assert any(
        match["query_modality"] == "IMAGE" and match["document_modality"] == "TEXT"
        for match in bundle["evidence"][0]["matches"]
    )

    await _embed(payloads, retrieval, late["facets"][0], _vector(0), suffix="late")
    replay = await _freeze_bundle(retrieval,
        operation_id="bundle/freeze/mixed",
        bundle_id="bundle/mixed",
        query_id="query/mixed",
        expected_deletion_epoch=0,
    )
    assert [item["source_unit_id"] for item in replay["evidence"]] == ["unit/first-text"]


@pytest.mark.postgres
async def test_forget_revokes_bundle_and_scrubs_derived_vectors(clean_memory_graph):
    store, _storage, payloads, memory, forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "query")
    await _register(memory, "evidence")
    query_source = await _unit_values(
        payloads, source_suffix="query", unit_suffix="forget-query"
    )
    evidence_source = await _unit_values(
        payloads, source_suffix="evidence", unit_suffix="forget-evidence"
    )
    query_batch = await _capture_and_qualify(
        memory, payloads, batches, query_source, suffix="forget-query"
    )
    await _capture_and_qualify(
        memory, payloads, batches, evidence_source, suffix="forget-evidence"
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    document = await retrieval.prepare_source_facets(
        operation_id="facets/forget-evidence",
        source_unit_id="unit/forget-evidence",
        expected_deletion_epoch=0,
    )
    query = await retrieval.register_query(
        operation_id="query/register/forget",
        query_id="query/forget",
        current_source_id="source/group/query",
        input_batch_ref=query_batch,
        context_revision=1,
        parts=(
            RetrievalQueryPart(
                "unit/forget-query",
                1,
                query_source["occurrence"].occurrence_id,
                "part/text",
            ),
        ),
        expected_deletion_epoch=0,
    )
    document_attempt = await _embed(
        payloads, retrieval, document["facets"][0], _vector(0), suffix="forget-doc"
    )
    await _embed(payloads, retrieval, query["facets"][0], _vector(0), suffix="forget-query")
    await _freeze_bundle(retrieval,
        operation_id="bundle/freeze/forget",
        bundle_id="bundle/forget",
        query_id="query/forget",
        expected_deletion_epoch=0,
    )
    await forget.apply(
        operation_id="forget/retrieval/evidence",
        source_unit_ids=("unit/forget-evidence",),
        expected_deletion_epoch=0,
        reason_code="ADMIN_REQUEST",
    )
    with pytest.raises(ForestError) as caught:
        await retrieval.read_bundle("bundle/forget", expected_deletion_epoch=1)
    assert caught.value.code == "MEMORY_EVIDENCE_REVOKED"
    async with store.pool.connection() as conn:
        facet = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,index_ref_id,result_payload_id FROM {}.embedding_facets "
                    "WHERE bot_id=%s AND source_unit_id=%s"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id, "unit/forget-evidence"),
            )
        ).fetchone()
    assert facet["state"] == "SUPPRESSED"
    assert facet["index_ref_id"] is None
    assert facet["result_payload_id"] is None
    async with store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as caught:
            await payloads.read_bytes_in(
                conn,
                document_attempt["result_payload_id"],
                expected_deletion_epoch=1,
            )
    assert caught.value.code == "PAYLOAD_REVOKED"


@pytest.mark.postgres
async def test_embedding_attempt_replays_converge_without_duplicate_dispatch(clean_memory_graph):
    _store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "converge")
    source = await _unit_values(
        payloads, source_suffix="converge", unit_suffix="converge"
    )
    await _capture_and_qualify(memory, payloads, batches, source, suffix="converge")
    retrieval = MemoryRetrievalRepository(
        memory.store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    first, replay = await asyncio.gather(
        retrieval.prepare_source_facets(
            operation_id="facets/converge",
            source_unit_id="unit/converge",
            expected_deletion_epoch=0,
        ),
        retrieval.prepare_source_facets(
            operation_id="facets/converge",
            source_unit_id="unit/converge",
            expected_deletion_epoch=0,
        ),
    )
    assert {first["duplicate"], replay["duplicate"]} == {False, True}
    facet = first["facets"][0]
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)

    async def prepare_same():
        return await retrieval.prepare_attempt(
            operation_id="embedding/prepare/converge",
            attempt_id="embedding/attempt/converge",
            facet_id=facet["facet_id"],
            executor_component_id="embedding.qiyu-vl",
            executor_generation_id="generation/qiyu-vl/1",
            route_revision="route/qiyu-vl/1",
            deadline_at=deadline,
        )

    permits = await asyncio.gather(prepare_same(), prepare_same())
    assert permits[0] == permits[1]
    handoffs = await asyncio.gather(
        retrieval.mark_handoff(
            permits[0], handoff_operation_id="embedding/handoff/converge"
        ),
        retrieval.mark_handoff(
            permits[1], handoff_operation_id="embedding/handoff/converge"
        ),
    )
    assert {row["state"] for row in handoffs} == {"DISPATCHED"}
    with pytest.raises(ForestError) as caught:
        await retrieval.prepare_attempt(
            operation_id="embedding/prepare/converge/second",
            attempt_id="embedding/attempt/converge/second",
            facet_id=facet["facet_id"],
            executor_component_id="embedding.qiyu-vl",
            executor_generation_id="generation/qiyu-vl/1",
            route_revision="route/qiyu-vl/1",
            deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
        )
    assert caught.value.code == "MEMORY_FACET_NOT_DISPATCHABLE"


@pytest.mark.postgres
async def test_clean_vl_worker_uses_handoff_fence_and_payload_lineage(clean_memory_graph):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    profile = EmbeddingProfile(
        deployment_id="qiyu-vl",
        model_revision="qiyu-docker-v1",
        model=SILICONFLOW_VL_MODEL,
        dimensions=1024,
    )
    await memory.initialize_domain(
        operation_id="memory/domain/initialize/worker",
        space_id=profile.space_id,
        embedding_model_kind=profile.model,
        model_revision_digest="sha256:" + "c" * 64,
        dimensions=profile.dimensions,
        normalization="L2",
        fusion_policy_revision="fusion/1",
    )
    await _register(memory, "worker")
    source = await _unit_values(
        payloads,
        source_suffix="worker",
        unit_suffix="worker-image",
        mixed=True,
    )
    await _capture_and_qualify(memory, payloads, batches, source, suffix="worker-image")
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    prepared = await retrieval.prepare_source_facets(
        operation_id="facets/worker-image",
        source_unit_id="unit/worker-image",
        expected_deletion_epoch=0,
    )
    image_facet = next(facet for facet in prepared["facets"] if facet["modality"] == "IMAGE")

    class FakeClient:
        def __init__(self, status):
            self.status = status

        async def dispatch(self, request, *, permitcheck):
            await permitcheck(request)
            if self.status == "NOT_SENT":
                return EmbeddingResult(
                    "NOT_SENT",
                    request.request_id,
                    request.transport_attempt_id,
                    request.payload_digest,
                    request.space_id,
                    error_code="EMBEDDING_CONNECTION_UNAVAILABLE",
                )
            return EmbeddingResult(
                "RESPONDED",
                request.request_id,
                request.transport_attempt_id,
                request.payload_digest,
                request.space_id,
                vectors=(EmbeddingVector(image_facet["facet_id"], 0, tuple(_vector(0))),),
                usage_json='{"total_tokens":1}',
                http_status=200,
            )

    def worker(status):
        return MemoryEmbeddingWorker(
            retrieval=retrieval,
            payloads=payloads,
            provider=_DirectEmbeddingProvider(store, profile, FakeClient(status)),
            vector_index=object(),
            profile=profile,
            component_id="embedding.qiyu-vl",
            generation_id="generation/qiyu-vl/1",
            route_revision="route/qiyu-vl/1",
        )

    first = await worker("NOT_SENT").execute(
        facet_id=image_facet["facet_id"],
        attempt_id="embedding/worker/not-sent",
        prepare_operation_id="embedding/worker/prepare/not-sent",
        expected_deletion_epoch=0,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert first["state"] == "NOT_SENT"
    succeeded = await worker("RESPONDED").execute(
        facet_id=image_facet["facet_id"],
        attempt_id="embedding/worker/success",
        prepare_operation_id="embedding/worker/prepare/success",
        expected_deletion_epoch=0,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert succeeded["state"] == "SUCCEEDED"
    async with store.pool.connection() as conn:
        lineage = await (
            await conn.execute(
                sql.SQL(
                    "SELECT parent_payload_id FROM {}.payload_derivations "
                    "WHERE bot_id=%s AND payload_id=%s"
                ).format(sql.Identifier(payloads.namespace)),
                (store.bot_id, succeeded["result_payload_id"]),
            )
        ).fetchone()
    assert lineage["parent_payload_id"] == image_facet["input_payload_id"]


@pytest.mark.postgres
async def test_clean_vl_worker_batches_text_and_image_in_one_dispatch(clean_memory_graph):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    profile = EmbeddingProfile(
        deployment_id="qiyu-vl",
        model_revision="qiyu-docker-v1",
        model=SILICONFLOW_VL_MODEL,
        dimensions=1024,
    )
    await memory.initialize_domain(
        operation_id="memory/domain/initialize/batch",
        space_id=profile.space_id,
        embedding_model_kind=profile.model,
        model_revision_digest="sha256:" + "d" * 64,
        dimensions=profile.dimensions,
        normalization="L2",
        fusion_policy_revision="fusion/1",
    )
    await _register(memory, "batch")
    source = await _unit_values(
        payloads, source_suffix="batch", unit_suffix="batch-mixed", mixed=True
    )
    await _capture_and_qualify(memory, payloads, batches, source, suffix="batch-mixed")
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    prepared = await retrieval.prepare_source_facets(
        operation_id="facets/batch-mixed",
        source_unit_id="unit/batch-mixed",
        expected_deletion_epoch=0,
    )

    class BatchClient:
        calls = 0

        async def dispatch(self, request, *, permitcheck):
            self.calls += 1
            await permitcheck(request)
            facets = request.manifest["facets"]
            return EmbeddingResult(
                "RESPONDED",
                request.request_id,
                request.transport_attempt_id,
                request.payload_digest,
                request.space_id,
                vectors=tuple(
                    EmbeddingVector(facet["facet_id"], index, tuple(_vector(index)))
                    for index, facet in enumerate(facets)
                ),
                usage_json='{"total_tokens":2}',
                http_status=200,
            )

    client = BatchClient()
    budget = MemoryEmbeddingBudgetRepository(store, namespace=memory.namespace)
    worker = MemoryEmbeddingWorker(
        retrieval=retrieval,
        payloads=payloads,
        provider=_DirectEmbeddingProvider(store, profile, client),
        vector_index=object(),
        profile=profile,
        component_id="embedding.qiyu-vl",
        generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        budget=budget,
    )
    claim = await retrieval.claim_pending_batch(
        operation_id="embedding/batch/claim/1",
        claim_id="embedding/batch/transport/1",
        purpose="DOCUMENT",
        executor_component_id="embedding.qiyu-vl",
        executor_generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert len(claim.permits) == len(prepared["facets"])
    results = await worker.execute_claim(
        claim,
        budget_claim=EmbeddingBudgetClaim(
            reservation_id="budget/batch-worker/1",
            operation_id="budget/batch-worker/reserve/1",
            policy_revision="budget-policy/1",
            budget_day=date(2026, 9, 18),
            request_limit=10,
            token_limit=1000,
            reserved_tokens=100,
        ),
    )
    assert client.calls == 1
    assert len(results) == 2
    assert {result["state"] for result in results} == {"SUCCEEDED"}
    budget_state = await budget.snapshot(date(2026, 9, 18))
    assert budget_state["settled_requests"] == 1
    assert budget_state["charged_tokens"] == 2
    assert budget_state["reserved_requests"] == 0
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT modality,state,result_payload_id FROM {}.embedding_facets "
                    "WHERE bot_id=%s "
                    "AND source_unit_id=%s ORDER BY modality"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id, "unit/batch-mixed"),
            )
        ).fetchall()
    assert [(row["modality"], row["state"]) for row in rows] == [
        ("IMAGE", "EMBEDDED"),
        ("TEXT", "EMBEDDED"),
    ]
    assert len({row["result_payload_id"] for row in rows}) == 1


@pytest.mark.postgres
async def test_whitespace_text_is_suppressed_without_poisoning_image_batch(clean_memory_graph):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "whitespace-mixed")
    source = await _unit_values(
        payloads,
        source_suffix="whitespace-mixed",
        unit_suffix="whitespace-mixed",
        mixed=True,
        text_body=b" \n\t ",
    )
    await _capture_and_qualify(
        memory,
        payloads,
        batches,
        source,
        suffix="whitespace-mixed",
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    prepared = await retrieval.prepare_source_facets(
        operation_id="facets/whitespace-mixed",
        source_unit_id="unit/whitespace-mixed",
        expected_deletion_epoch=0,
    )
    assert {
        facet["modality"]: facet["state"] for facet in prepared["facets"]
    } == {"TEXT": "SUPPRESSED", "IMAGE": "PENDING"}
    claim = await retrieval.claim_pending_batch(
        operation_id="embedding/whitespace-mixed/claim",
        claim_id="embedding/whitespace-mixed/transport",
        purpose="DOCUMENT",
        executor_component_id="embedding.qiyu-vl",
        executor_generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert len(claim.permits) == 1
    assert claim.permits[0].facet_id == next(
        facet["facet_id"] for facet in prepared["facets"] if facet["modality"] == "IMAGE"
    )


@pytest.mark.postgres
@pytest.mark.parametrize("invalid_kind", ["vector", "usage"])
async def test_invalid_batch_response_settles_cost_and_fails_all_facets(
    clean_memory_graph, invalid_kind
):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    profile = EmbeddingProfile(
        deployment_id="qiyu-vl",
        model_revision="qiyu-docker-v1",
        model=SILICONFLOW_VL_MODEL,
        dimensions=1024,
    )
    await memory.initialize_domain(
        operation_id="memory/domain/initialize/atomic-batch",
        space_id=profile.space_id,
        embedding_model_kind=profile.model,
        model_revision_digest="sha256:" + "e" * 64,
        dimensions=profile.dimensions,
        normalization="L2",
        fusion_policy_revision="fusion/1",
    )
    await _register(memory, "atomic-batch")
    source = await _unit_values(
        payloads,
        source_suffix="atomic-batch",
        unit_suffix="atomic-batch",
        mixed=True,
    )
    await _capture_and_qualify(
        memory, payloads, batches, source, suffix="atomic-batch"
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    facets = (
        await retrieval.prepare_source_facets(
            operation_id="facets/atomic-batch",
            source_unit_id="unit/atomic-batch",
            expected_deletion_epoch=0,
        )
    )["facets"]

    class InvalidOnceClient:
        calls = 0

        async def dispatch(self, request, *, permitcheck):
            self.calls += 1
            await permitcheck(request)
            request_facets = request.manifest["facets"]
            vectors = []
            for index, facet in enumerate(request_facets):
                value = tuple(_vector(index))
                if invalid_kind == "vector" and index == 1:
                    value = (1.0,)
                vectors.append(EmbeddingVector(facet["facet_id"], index, value))
            return EmbeddingResult(
                "RESPONDED",
                request.request_id,
                request.transport_attempt_id,
                request.payload_digest,
                request.space_id,
                vectors=tuple(vectors),
                usage_json=(
                    '{"total_tokens":2}'
                    if invalid_kind == "vector"
                    else '{"total_tokens":"unknown"}'
                ),
                http_status=200,
            )

    client = InvalidOnceClient()
    budget = MemoryEmbeddingBudgetRepository(store, namespace=memory.namespace)
    worker = MemoryEmbeddingWorker(
        retrieval=retrieval,
        payloads=payloads,
        provider=_DirectEmbeddingProvider(store, profile, client),
        vector_index=object(),
        profile=profile,
        component_id="embedding.qiyu-vl",
        generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        budget=budget,
    )
    items = tuple(
        EmbeddingBatchItem(
            facet["facet_id"],
            f"embedding/atomic-batch/attempt/{index}",
            f"embedding/atomic-batch/prepare/{index}",
        )
        for index, facet in enumerate(facets)
    )
    claim = EmbeddingBudgetClaim(
        reservation_id="budget/atomic-batch/1",
        operation_id="budget/atomic-batch/reserve/1",
        policy_revision="budget-policy/atomic-batch",
        budget_day=date(2026, 9, 18),
        request_limit=10,
        token_limit=1000,
        reserved_tokens=100,
    )
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    with pytest.raises(ForestError) as caught:
        await worker.execute_batch(
            items=items,
            batch_attempt_id="embedding/atomic-batch/transport/1",
            expected_deletion_epoch=0,
            deadline_at=deadline,
            budget_claim=claim,
        )
    assert caught.value.code == (
        "EMBEDDING_RESPONSE_INVALID" if invalid_kind == "vector" else "MEMORY_USAGE_INVALID"
    )
    charged = await budget.snapshot(date(2026, 9, 18))
    assert charged["reserved_requests"] == 0
    if invalid_kind == "vector":
        assert charged["settled_requests"] == 1
        assert charged["charged_tokens"] == 2
    else:
        assert charged["unknown_requests"] == 1
        assert charged["unknown_tokens"] == 100
    async with store.pool.connection() as conn:
        attempt_states = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state FROM {}.embedding_attempts WHERE bot_id=%s "
                    "ORDER BY attempt_id"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id,),
            )
        ).fetchall()
        response_count = await (
            await conn.execute(
                sql.SQL(
                    "SELECT count(*) AS value FROM {}.payload_headers WHERE bot_id=%s "
                    "AND owner_kind='embedding.batch-response'"
                ).format(sql.Identifier(payloads.namespace)),
                (store.bot_id,),
            )
        ).fetchone()
    assert [row["state"] for row in attempt_states] == ["FAILED", "FAILED"]
    assert response_count["value"] == 0
    async with store.pool.connection() as conn:
        published = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,result_payload_id FROM {}.embedding_facets "
                    "WHERE bot_id=%s ORDER BY facet_id"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id,),
            )
        ).fetchall()
    assert {row["state"] for row in published} == {"FAILED"}
    assert {row["result_payload_id"] for row in published} == {None}


@pytest.mark.postgres
async def test_unrelated_forget_during_provider_call_keeps_valid_batch_publishable(
    clean_memory_graph,
):
    store, _storage, payloads, memory, forget, batches, _scheduler = clean_memory_graph
    profile = EmbeddingProfile(
        deployment_id="qiyu-vl",
        model_revision="qiyu-docker-v1",
        model=SILICONFLOW_VL_MODEL,
        dimensions=1024,
    )
    await memory.initialize_domain(
        operation_id="memory/domain/initialize/inflight-forget",
        space_id=profile.space_id,
        embedding_model_kind=profile.model,
        model_revision_digest="sha256:" + "f" * 64,
        dimensions=profile.dimensions,
        normalization="L2",
        fusion_policy_revision="fusion/1",
    )
    await _register(memory, "inflight-removed")
    await _register(memory, "inflight-survivor")
    await _register(memory, "inflight-forgotten")
    removed = await _unit_values(
        payloads,
        source_suffix="inflight-removed",
        unit_suffix="inflight-removed",
    )
    survivor = await _unit_values(
        payloads,
        source_suffix="inflight-survivor",
        unit_suffix="inflight-survivor",
    )
    forgotten = await _unit_values(
        payloads,
        source_suffix="inflight-forgotten",
        unit_suffix="inflight-forgotten",
    )
    await _capture_and_qualify(
        memory, payloads, batches, removed, suffix="inflight-removed"
    )
    await _capture_and_qualify(
        memory, payloads, batches, survivor, suffix="inflight-survivor"
    )
    await _capture_and_qualify(
        memory, payloads, batches, forgotten, suffix="inflight-forgotten"
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    facet = (
        await retrieval.prepare_source_facets(
            operation_id="facets/inflight-survivor",
            source_unit_id="unit/inflight-survivor",
            expected_deletion_epoch=0,
        )
    )["facets"][0]

    class ForgetDuringDispatch:
        async def dispatch(self, request, *, permitcheck):
            await permitcheck(request)
            await forget.apply(
                operation_id="forget/inflight-unrelated",
                source_unit_ids=("unit/inflight-removed",),
                expected_deletion_epoch=0,
                reason_code="ADMIN_REQUEST",
            )
            return EmbeddingResult(
                "RESPONDED",
                request.request_id,
                request.transport_attempt_id,
                request.payload_digest,
                request.space_id,
                vectors=(EmbeddingVector(facet["facet_id"], 0, tuple(_vector(0))),),
                usage_json='{"total_tokens":1}',
                http_status=200,
            )

    worker = MemoryEmbeddingWorker(
        retrieval=retrieval,
        payloads=payloads,
        provider=_DirectEmbeddingProvider(store, profile, ForgetDuringDispatch()),
        vector_index=object(),
        profile=profile,
        component_id="embedding.qiyu-vl",
        generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
    )
    result = await worker.execute(
        facet_id=facet["facet_id"],
        attempt_id="embedding/inflight-survivor/attempt",
        prepare_operation_id="embedding/inflight-survivor/prepare",
        expected_deletion_epoch=0,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert result["state"] == "SUCCEEDED"
    assert result["accepted_deletion_epoch"] == 0
    async with store.pool.connection() as conn:
        saved = await (
            await conn.execute(
                sql.SQL(
                    "SELECT accepted_deletion_epoch FROM {}.payload_headers "
                    "WHERE bot_id=%s AND payload_id=%s"
                ).format(sql.Identifier(payloads.namespace)),
                (store.bot_id, result["result_payload_id"]),
            )
        ).fetchone()
    assert saved["accepted_deletion_epoch"] == 1

    forgotten_facet = (
        await retrieval.prepare_source_facets(
            operation_id="facets/inflight-forgotten",
            source_unit_id="unit/inflight-forgotten",
            expected_deletion_epoch=1,
        )
    )["facets"][0]

    class ForgetOwnSourceDuringDispatch:
        async def dispatch(self, request, *, permitcheck):
            await permitcheck(request)
            await forget.apply(
                operation_id="forget/inflight-own-source",
                source_unit_ids=("unit/inflight-forgotten",),
                expected_deletion_epoch=1,
                reason_code="ADMIN_REQUEST",
            )
            return EmbeddingResult(
                "RESPONDED",
                request.request_id,
                request.transport_attempt_id,
                request.payload_digest,
                request.space_id,
                vectors=(
                    EmbeddingVector(forgotten_facet["facet_id"], 0, tuple(_vector(0))),
                ),
                usage_json='{"total_tokens":3}',
                http_status=200,
            )

    budget = MemoryEmbeddingBudgetRepository(store, namespace=memory.namespace)
    forgotten_worker = MemoryEmbeddingWorker(
        retrieval=retrieval,
        payloads=payloads,
        provider=_DirectEmbeddingProvider(store, profile, ForgetOwnSourceDuringDispatch()),
        vector_index=object(),
        profile=profile,
        component_id="embedding.qiyu-vl",
        generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        budget=budget,
    )
    with pytest.raises(ForestError) as caught:
        await forgotten_worker.execute(
            facet_id=forgotten_facet["facet_id"],
            attempt_id="embedding/inflight-forgotten/attempt",
            prepare_operation_id="embedding/inflight-forgotten/prepare",
            expected_deletion_epoch=1,
            deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
            budget_claim=EmbeddingBudgetClaim(
                reservation_id="budget/inflight-forgotten/1",
                operation_id="budget/inflight-forgotten/reserve/1",
                policy_revision="budget-policy/inflight-forgotten",
                budget_day=date(2026, 9, 18),
                request_limit=10,
                token_limit=1000,
                reserved_tokens=100,
            ),
        )
    assert caught.value.code in {"SOURCE_SUPPRESSED", "PAYLOAD_PARENT_UNAVAILABLE"}
    charged = await budget.snapshot(date(2026, 9, 18))
    assert charged["settled_requests"] == 1
    assert charged["charged_tokens"] == 3
    async with store.pool.connection() as conn:
        attempt = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,error_code FROM {}.embedding_attempts "
                    "WHERE bot_id=%s AND attempt_id=%s"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id, "embedding/inflight-forgotten/attempt"),
            )
        ).fetchone()
        forgotten_state = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,result_payload_id FROM {}.embedding_facets "
                    "WHERE bot_id=%s AND facet_id=%s"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id, forgotten_facet["facet_id"]),
            )
        ).fetchone()
    assert attempt["state"] == "UNKNOWN"
    assert attempt["error_code"] == "SOURCE_SUPPRESSED"
    assert forgotten_state["state"] == "SUPPRESSED"
    assert forgotten_state["result_payload_id"] is None


@pytest.mark.postgres
async def test_batch_handoff_rolls_back_when_one_facet_is_no_longer_dispatchable(
    clean_memory_graph,
):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "batch-rollback")
    source = await _unit_values(
        payloads,
        source_suffix="batch-rollback",
        unit_suffix="batch-rollback",
        mixed=True,
    )
    await _capture_and_qualify(
        memory, payloads, batches, source, suffix="batch-rollback"
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    facets = (
        await retrieval.prepare_source_facets(
            operation_id="facets/batch-rollback",
            source_unit_id="unit/batch-rollback",
            expected_deletion_epoch=0,
        )
    )["facets"]
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    permits = []
    for index, facet in enumerate(facets):
        permits.append(
            await retrieval.prepare_attempt(
                operation_id=f"embedding/batch-rollback/prepare/{index}",
                attempt_id=f"embedding/batch-rollback/attempt/{index}",
                facet_id=facet["facet_id"],
                executor_component_id="embedding.qiyu-vl",
                executor_generation_id="generation/qiyu-vl/1",
                route_revision="route/qiyu-vl/1",
                deadline_at=deadline,
            )
        )
    await retrieval.settle_attempt(
        permits[1],
        settlement_operation_id="embedding/batch-rollback/settle/1",
        outcome="NOT_SENT",
        expected_deletion_epoch=0,
        error_code="EMBEDDING_CONNECTION_UNAVAILABLE",
    )
    with pytest.raises(ForestError) as caught:
        await retrieval.mark_batch_handoff(
            permits,
            batch_operation_id="embedding/batch-rollback/handoff",
        )
    assert caught.value.code == "MEMORY_EMBEDDING_NOT_DISPATCHABLE"
    async with store.pool.connection() as conn:
        first = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,handoff_operation_id FROM {}.embedding_attempts "
                    "WHERE bot_id=%s AND attempt_id=%s"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id, permits[0].attempt_id),
            )
        ).fetchone()
    assert first["state"] == "PREPARED"
    assert first["handoff_operation_id"] is None


@pytest.mark.postgres
async def test_embedding_budget_converges_and_keeps_unknown_exposure(clean_memory_graph):
    store, storage, _payloads, _memory, _forget, _batches, _scheduler = clean_memory_graph
    budget = MemoryEmbeddingBudgetRepository(store, namespace=storage.spec.namespace)
    budget_day = date(2026, 9, 18)
    replay_deadline = datetime.now(timezone.utc) + timedelta(minutes=2)

    async def reserve_same():
        return await budget.reserve(
            operation_id="budget/reserve/one",
            reservation_id="budget/one",
            batch_ref="embedding/batch/one",
            policy_revision="budget-policy/1",
            budget_day=budget_day,
            request_limit=1,
            token_limit=1000,
            reserved_tokens=100,
            deadline_at=replay_deadline,
        )

    first, replay = await asyncio.gather(reserve_same(), reserve_same())
    assert {first["duplicate"], replay["duplicate"]} == {False, True}
    with pytest.raises(ForestError) as caught:
        await budget.reserve(
            operation_id="budget/reserve/two",
            reservation_id="budget/two",
            batch_ref="embedding/batch/two",
            policy_revision="budget-policy/1",
            budget_day=budget_day,
            request_limit=1,
            token_limit=1000,
            reserved_tokens=100,
            deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
        )
    assert caught.value.code == "MEMORY_EMBEDDING_REQUEST_BUDGET_EXHAUSTED"

    unknown = await budget.settle(
        reservation_id="budget/one",
        settlement_operation_id="budget/settle/one",
        outcome="UNKNOWN",
        error_code="EMBEDDING_TIMEOUT",
    )
    assert unknown["day"]["reserved_requests"] == 0
    assert unknown["day"]["unknown_requests"] == 1
    assert unknown["day"]["unknown_tokens"] == 100
    duplicate = await budget.settle(
        reservation_id="budget/one",
        settlement_operation_id="budget/settle/one",
        outcome="UNKNOWN",
        error_code="EMBEDDING_TIMEOUT",
    )
    assert duplicate["duplicate"] is True

    released = await budget.reserve(
        operation_id="budget/reserve/released",
        reservation_id="budget/released",
        batch_ref="embedding/batch/released",
        policy_revision="budget-policy/2",
        budget_day=budget_day,
        request_limit=3,
        token_limit=200,
        reserved_tokens=50,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert released["state"] == "RESERVED"
    await budget.settle(
        reservation_id="budget/released",
        settlement_operation_id="budget/settle/released",
        outcome="RELEASED",
        error_code="EMBEDDING_CONNECTION_UNAVAILABLE",
    )
    charged = await budget.reserve(
        operation_id="budget/reserve/charged",
        reservation_id="budget/charged",
        batch_ref="embedding/batch/charged",
        policy_revision="budget-policy/2",
        budget_day=budget_day,
        request_limit=3,
        token_limit=200,
        reserved_tokens=50,
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert charged["state"] == "RESERVED"
    receipt = await budget.settle(
        reservation_id="budget/charged",
        settlement_operation_id="budget/settle/charged",
        outcome="CHARGED",
        charged_tokens=70,
    )
    assert receipt["day"]["charged_tokens"] == 70
    assert receipt["day"]["unknown_tokens"] == 100
    assert receipt["day"]["reserved_tokens"] == 0

    recovery_day = date(2026, 9, 19)
    await budget.reserve(
        operation_id="budget/reserve/recovery",
        reservation_id="budget/recovery",
        batch_ref="embedding/batch/recovery",
        policy_revision="budget-policy/3",
        budget_day=recovery_day,
        request_limit=2,
        token_limit=200,
        reserved_tokens=40,
        deadline_at=datetime.now(timezone.utc) + timedelta(milliseconds=100),
    )
    await asyncio.sleep(0.2)
    assert await budget.recover_expired() == 1
    recovered = await budget.snapshot(recovery_day)
    assert recovered["unknown_requests"] == 1
    assert recovered["unknown_tokens"] == 40


@pytest.mark.postgres
async def test_unrelated_forget_does_not_strand_pending_embedding_facet(clean_memory_graph):
    _store, _storage, payloads, memory, forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "removed")
    await _register(memory, "survivor")
    removed = await _unit_values(
        payloads, source_suffix="removed", unit_suffix="removed-before-embedding"
    )
    survivor = await _unit_values(
        payloads, source_suffix="survivor", unit_suffix="survivor-after-epoch"
    )
    await _capture_and_qualify(memory, payloads, batches, removed, suffix="removed-epoch")
    await _capture_and_qualify(memory, payloads, batches, survivor, suffix="survivor-epoch")
    retrieval = MemoryRetrievalRepository(
        memory.store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    facet = (
        await retrieval.prepare_source_facets(
            operation_id="facets/survivor-before-epoch",
            source_unit_id="unit/survivor-after-epoch",
            expected_deletion_epoch=0,
        )
    )["facets"][0]
    await forget.apply(
        operation_id="forget/unrelated-before-embedding",
        source_unit_ids=("unit/removed-before-embedding",),
        expected_deletion_epoch=0,
        reason_code="ADMIN_REQUEST",
    )
    materialized = await retrieval.materialize_facet(
        facet["facet_id"], expected_deletion_epoch=1
    )
    assert materialized["source"]["source_unit_id"] == "unit/survivor-after-epoch"
    permit = await retrieval.prepare_attempt(
        operation_id="embedding/survivor/prepare",
        attempt_id="embedding/survivor/attempt",
        facet_id=facet["facet_id"],
        executor_component_id="embedding.qiyu-vl",
        executor_generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        deadline_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    assert permit.accepted_deletion_epoch == 1


@pytest.mark.postgres
async def test_embedding_claim_replay_and_expiry_recovery_converge(clean_memory_graph):
    store, _storage, payloads, memory, _forget, batches, _scheduler = clean_memory_graph
    await _initialize(memory)
    await _register(memory, "claim-recovery")
    source = await _unit_values(
        payloads,
        source_suffix="claim-recovery",
        unit_suffix="claim-recovery",
        mixed=True,
    )
    await _capture_and_qualify(
        memory, payloads, batches, source, suffix="claim-recovery"
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=memory.namespace,
        payload_verifier=payloads.verify_in,
        payload_reader=payloads.read_bytes_in,
        deletion_epoch_guard=payloads.epoch_in,
    )
    await retrieval.prepare_source_facets(
        operation_id="facets/claim-recovery",
        source_unit_id="unit/claim-recovery",
        expected_deletion_epoch=0,
    )
    first_deadline = datetime.now(timezone.utc) + timedelta(seconds=1)

    async def claim_same():
        return await retrieval.claim_pending_batch(
            operation_id="embedding/claim/one",
            claim_id="embedding/claim/one",
            purpose="DOCUMENT",
            executor_component_id="embedding.qiyu-vl",
            executor_generation_id="generation/qiyu-vl/1",
            route_revision="route/qiyu-vl/1",
            deadline_at=first_deadline,
        )

    first, replay = await asyncio.gather(claim_same(), claim_same())
    assert first == replay
    assert len(first.permits) == 2
    assert {permit.attempt_no for permit in first.permits} == {1}
    await asyncio.sleep(1.1)
    recovered = await retrieval.recover_expired_attempts()
    assert recovered == {"recovered": 2, "not_sent": 2, "unknown": 0}

    second = await retrieval.claim_pending_batch(
        operation_id="embedding/claim/two",
        claim_id="embedding/claim/two",
        purpose="DOCUMENT",
        executor_component_id="embedding.qiyu-vl",
        executor_generation_id="generation/qiyu-vl/1",
        route_revision="route/qiyu-vl/1",
        deadline_at=datetime.now(timezone.utc) + timedelta(seconds=1),
    )
    assert len(second.permits) == 2
    assert {permit.attempt_no for permit in second.permits} == {2}
    await retrieval.mark_batch_handoff(
        second.permits,
        batch_operation_id="embedding/claim/two/handoff",
    )
    await asyncio.sleep(1.1)
    recovered = await retrieval.recover_expired_attempts()
    assert recovered == {"recovered": 2, "not_sent": 0, "unknown": 2}
    assert await retrieval.recover_expired_attempts() == {
        "recovered": 0,
        "not_sent": 0,
        "unknown": 0,
    }
    async with store.pool.connection() as conn:
        attempts = await (
            await conn.execute(
                sql.SQL(
                    "SELECT attempt_no,state,error_code FROM {}.embedding_attempts "
                    "WHERE bot_id=%s ORDER BY attempt_no,state"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id,),
            )
        ).fetchall()
        facets = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,current_attempt_id FROM {}.embedding_facets "
                    "WHERE bot_id=%s ORDER BY facet_id"
                ).format(sql.Identifier(memory.namespace)),
                (store.bot_id,),
            )
        ).fetchall()
    assert [(row["attempt_no"], row["state"]) for row in attempts] == [
        (1, "NOT_SENT"),
        (1, "NOT_SENT"),
        (2, "UNKNOWN"),
        (2, "UNKNOWN"),
    ]
    assert all(
        row["error_code"].startswith("EMBEDDING_ATTEMPT_EXPIRED_") for row in attempts
    )
    assert [(row["state"], row["current_attempt_id"]) for row in facets] == [
        ("UNKNOWN", None),
        ("UNKNOWN", None),
    ]

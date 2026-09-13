"""Real PostgreSQL query/evidence boundaries; vectors test ranking, not semantics."""

import asyncio
import base64
import json

import pytest
from test_embeddings import send, server
from test_media import media_server, picture, respond
from test_memory_config import environment as environment
from test_memory_index_store import outcome, rows
from test_memory_media import configure
from test_memory_media import media as media
from test_messages import event, owner

from forest.embeddings import EmbeddingClient, EmbeddingResult, EmbeddingVector
from forest.errors import ForestError
from forest.memory_index import profile
from forest.memory_types import SourceRef
from forest.storage.memory_associations import MemoryAssociationAdministration
from forest.storage.memory_index import MemoryIndexStore
from forest.storage.memory_queries import MemoryQueryStore

pytestmark = pytest.mark.postgres


async def source(
    media,
    number,
    *,
    group="query-group",
    thread="",
    user="user-1",
    text="native source",
    url=None,
    image_only=False,
    private=False,
    drain=True,
):
    env, images, _ = media
    message = event(number, group=group, user=user, text=text)
    message["thread_id"] = thread
    if private:
        message["channel_kind"] = "private"
    if url:
        message["segments"] = ([] if image_only else message["segments"]) + [
            {"type": "image", "media_ref": {"kind": "onebot-source", "uri": url}}
        ]
    receipt = await images.messages.ingest(env.lease, "binding-a", message, decision="NO_TRIGGER")
    if drain:
        actor = await owner(images.messages, env.lease, receipt["conversation_id"])
        await images.messages.select_input(actor, "drain:" + receipt["event_id"])
    unit = (
        await rows(
            media,
            "SELECT source_unit_id FROM public.bot_memory_source_occurrences WHERE event_id=%s",
            (receipt["event_id"],),
        )
    )[0]["source_unit_id"]
    return {**receipt, "source_unit_id": unit}


async def query(media, store, receipt, *, query_id="query", cut=None, ids=None, revision=1):
    return await store.prepare(
        media[0].lease,
        revision,
        query_id=query_id,
        conversation_id=receipt["conversation_id"],
        input_cut=receipt["ingress_seq"] if cut is None else cut,
        source_unit_ids=ids or [receipt["source_unit_id"]],
    )


async def embed(
    media,
    index,
    ids,
    *,
    request_id=None,
    query_receipt=None,
    axes=None,
    status="RESPONDED",
    revision=1,
):
    kwargs = {"request_id": request_id}
    if query_receipt:
        kwargs.update(
            request_id=query_receipt["query_id"],
            purpose="query",
            conversation_id=query_receipt["conversation_id"],
            input_cut=query_receipt["input_cut"],
        )
    prepared = (await index.prepare(media[0].lease, revision, ids, **kwargs))["prepared"]
    token = "transport:" + prepared.transport_attempt_id
    if status != "NOT_SENT":
        await index.check_permit(media[0].lease, prepared, dispatch_token=token)
    result = outcome(prepared, status)
    if status == "RESPONDED" and axes is not None:
        vectors = []
        for ordinal, facet in enumerate(prepared.manifest["facets"]):
            axis = axes.get(facet["facet_id"], 0)
            values = [0.0] * 512
            values[axis] = 1.0
            vectors.append(EmbeddingVector(facet["facet_id"], ordinal, tuple(values)))
        result = EmbeddingResult(
            result.status,
            result.request_id,
            result.transport_attempt_id,
            result.payload_digest,
            result.space_id,
            tuple(vectors),
            result.usage_json,
        )
    await index.complete(
        media[0].lease,
        prepared,
        result,
        **({"dispatch_token": token} if status != "NOT_SENT" else {}),
    )
    return prepared


async def documents(media, index, source_ids):
    await index.enroll(media[0].lease, 1, limit=64)
    pending = await index.pending(media[0].lease, 1, limit=32)
    ids = [row["facet_id"] for row in pending if row["source_unit_id"] in source_ids]
    await embed(media, index, ids)
    return ids


async def read_frozen(media, store, receipt):
    async with media[0].store.pool.connection() as conn, conn.transaction():
        _, _, conversation, binding = await store.index._guard(
            conn, media[0].lease, 1, receipt["conversation_id"]
        )
        result = await store.read_in(
            conn, receipt["query_id"], conversation=conversation, input_cut=receipt["input_cut"]
        )
        await store.index._final(conn, media[0].lease, binding)
        return result


async def test_query_registers_native_facets_inline_and_identity_rejects_changed_input(media):
    env, images, _ = media
    await configure(env, [])
    first = await source(media, 1)
    other = await source(media, 2, group="other")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    planned = await query(media, store, first)
    assert planned == await query(media, store, first)
    assert planned["query_modalities"] == ["text"]
    assert len(await rows(media, "SELECT * FROM public.bot_memory_index_facets")) == 1
    with pytest.raises(ForestError) as conflict:
        await query(media, store, first, ids=[other["source_unit_id"]])
    assert conflict.value.code == "MEMORY_QUERY_CONFLICT"
    assert not await rows(media, "SELECT * FROM public.bot_memory_embedding_requests")


async def test_pg_vectors_recall_cross_group_sources_with_distinct_authors_and_freeze(media):
    env, images, _ = media
    await configure(env, [], max_result_tokens=16000)
    asking = await source(media, 1)
    left = await source(media, 2, group="left", user="alice", text="left original")
    right = await source(media, 3, group="right", user="bob", text="right original")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [left["source_unit_id"], right["source_unit_id"]])
    planned = await query(media, store, asking)
    await embed(media, index, planned["facet_ids"], query_receipt=planned)
    selected = await asyncio.gather(*(store.retrieve(env.lease, "query") for _ in range(5)))
    assert all(item == selected[0] for item in selected)
    bundle = selected[0]
    assert bundle["status"] == "READY" and len(bundle["evidence"]) == 2
    assert {item["source"]["channel_id"] for item in bundle["evidence"]} == {"left", "right"}
    assert {item["author"]["external_user_id"] for item in bundle["evidence"]} == {"alice", "bob"}
    assert all(
        item["source"]["source_kind"] == "GROUP"
        and item["source"]["identity_schema"] == 1
        and item["source"]["realm_id"]
        and item["matched_facets"]
        for item in bundle["evidence"]
    )
    assert await read_frozen(media, store, planned) == bundle
    later = await source(media, 4, group="late")
    await documents(media, index, [later["source_unit_id"]])
    restarted = MemoryQueryStore(index)
    assert await restarted.retrieve(env.lease, "query") == bundle
    assert len(await rows(media, "SELECT * FROM public.bot_memory_evidence_bundles")) == 1


async def test_vector_seed_expands_one_published_cross_trunk_association(media):
    env, images, _ = media
    await configure(env, [], max_result_tokens=16000)
    asking = await source(media, 1, group="asking", user="questioner", text="find alpha")
    seed = await source(media, 2, group="seed-group", user="alice", text="alpha seed")
    neighbor = await source(
        media, 3, group="neighbor-group", user="bob", text="associated beta"
    )
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [seed["source_unit_id"]])

    ticket = env.ticket
    associations = MemoryAssociationAdministration(images.messages)
    proposal = {
        "expected_deletion_epoch": 0,
        "expected_revision": 0,
        "active": True,
        "source_unit_ids": [seed["source_unit_id"], neighbor["source_unit_id"]],
    }
    plan_operation = "query-association-plan"
    plan = await associations.plan(
        await ticket("memory.association.plan", plan_operation, proposal),
        plan_operation,
        proposal,
    )
    apply_payload = {
        key: plan[key]
        for key in ("expected_deletion_epoch", "plan_id", "plan_digest")
    }
    apply_operation = "query-association-apply"
    await associations.apply(
        await ticket(
            "memory.association.apply",
            apply_operation,
            apply_payload,
            plan_digest=plan["plan_digest"],
        ),
        apply_operation,
        apply_payload,
        lease=env.lease,
    )
    pending = await associations.graph.pending(env.lease)
    assert len(pending["scopes"]) == 1
    published = await associations.graph.refresh(env.lease, pending["scopes"][0])

    planned = await query(media, store, asking)
    await embed(media, index, planned["facet_ids"], query_receipt=planned)
    result = await store.retrieve(env.lease, "query")
    assert [item["source_unit_id"] for item in result["evidence"]] == [
        seed["source_unit_id"], neighbor["source_unit_id"]
    ]
    associated = result["evidence"][1]
    assert associated["matched_facets"] == []
    assert associated["source"]["channel_id"] == "neighbor-group"
    assert associated["author"]["external_user_id"] == "bob"
    assert associated["retrieval"] == {
        "kind": "graph_association",
        "association_id": plan["association_id"],
        "association_origin": "reviewed",
        "score_ppm": None,
        "edge_id": associated["retrieval"]["edge_id"],
        "seed_source_unit_id": seed["source_unit_id"],
        "graph_revision": published["graph_revision"],
    }
    assert result["completeness"]["graph"] == "CURRENT"
    assert result["completeness"]["association_edges_considered"] == 1
    assert result["completeness"]["association_sources_added"] == 1
    assert await read_frozen(media, store, planned) == result


async def test_association_cannot_pull_a_later_same_conversation_source_across_query_cut(media):
    env, images, _ = media
    await configure(env, [], max_result_tokens=16000)
    asking = await source(media, 1, group="asking", text="frozen question")
    seed = await source(media, 2, group="seed-group", text="visible seed")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [seed["source_unit_id"]])
    planned = await query(media, store, asking, query_id="cut-query")
    await embed(media, index, planned["facet_ids"], query_receipt=planned)

    future = await source(media, 3, group="asking", text="must remain beyond cut")
    associations = MemoryAssociationAdministration(images.messages)
    proposal = {
        "expected_deletion_epoch": 0,
        "expected_revision": 0,
        "active": True,
        "source_unit_ids": [seed["source_unit_id"], future["source_unit_id"]],
    }
    plan_operation = "cut-association-plan"
    plan = await associations.plan(
        await env.ticket("memory.association.plan", plan_operation, proposal),
        plan_operation,
        proposal,
    )
    apply_payload = {
        key: plan[key]
        for key in ("expected_deletion_epoch", "plan_id", "plan_digest")
    }
    apply_operation = "cut-association-apply"
    await associations.apply(
        await env.ticket(
            "memory.association.apply",
            apply_operation,
            apply_payload,
            plan_digest=plan["plan_digest"],
        ),
        apply_operation,
        apply_payload,
        lease=env.lease,
    )
    pending = await associations.graph.pending(env.lease)
    await associations.graph.refresh(env.lease, pending["scopes"][0])

    result = await store.retrieve(env.lease, "cut-query")
    assert [item["source_unit_id"] for item in result["evidence"]] == [
        seed["source_unit_id"]
    ]
    assert result["completeness"]["graph"] == "CURRENT"
    assert result["completeness"]["association_edges_considered"] == 0
    assert result["completeness"]["association_sources_added"] == 0
    assert "must remain beyond cut" not in json.dumps(result)


async def test_unread_current_occurrence_and_private_source_cannot_bypass_query_cut(media):
    env, images, _ = media
    await configure(env, [], max_result_tokens=16000)
    asking = await source(media, 1, thread="current")
    shared = await source(media, 2, thread="elsewhere")
    tail = await source(media, 2, thread="current", drain=False)
    private = await source(media, 3, group="private-user", private=True)
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [shared["source_unit_id"], private["source_unit_id"]])
    with pytest.raises(ForestError) as denied:
        await query(media, store, asking, ids=[shared["source_unit_id"]])
    assert denied.value.code == "MEMORY_QUERY_SOURCE_UNAVAILABLE"
    planned = await query(media, store, asking)
    await embed(media, index, planned["facet_ids"], query_receipt=planned)
    assert (await store.retrieve(env.lease, "query"))["evidence"] == []
    actor = await owner(images.messages, env.lease, tail["conversation_id"])
    await images.messages.select_input(actor, "accept-tail")
    next_query = await query(media, store, tail, query_id="next-query")
    await embed(media, index, next_query["facet_ids"], query_receipt=next_query)
    assert (await store.retrieve(env.lease, "next-query"))["evidence"] == []


async def test_real_tcp_image_query_keeps_original_pixels_and_cross_group_evidence(media):
    env, images, _ = media
    content = picture()

    async def image_handler(writer, header):
        await respond(writer, content)

    async def embedding_handler(writer, request):
        body = json.loads(request["body"])
        await send(
            writer,
            {
                "model": "jina-clip-v2",
                "data": [
                    {"index": index, "embedding": [1.0] + [0.0] * 511}
                    for index in range(len(body["input"]))
                ],
                "usage": {"total_tokens": 9},
            },
        )

    async with (
        media_server(image_handler) as (origin, _),
        server(embedding_handler) as (endpoint, requests, _),
    ):
        await configure(env, [origin], endpoint=endpoint, max_result_tokens=16000)
        # Both capture and image retrieval run without any Agent model or vision config.
        asking = await source(media, 1, url=origin + "/query", image_only=True)
        found = await source(
            media, 2, group="pictures", user="photographer", url=origin + "/memory"
        )
        for item in (asking, found):
            pending = await images.ensure(env.lease, [item["source_unit_id"]])
            assert len(pending) == 1 and pending[0]["source_unit_id"] == item["source_unit_id"]
            await images.capture_once(env.lease, pending[0]["capture_id"])
        index = MemoryIndexStore(images.messages, images)
        store = MemoryQueryStore(index)
        await documents(media, index, [found["source_unit_id"]])
        planned = await query(media, store, asking)
        assert planned["query_modalities"] == ["image"]
        prepared = (
            await index.prepare(
                env.lease,
                1,
                planned["facet_ids"],
                request_id="query",
                purpose="query",
                conversation_id=asking["conversation_id"],
                input_cut=1,
            )
        )["prepared"]
        cfg = await images.config.runtime(env.lease)
        client = EmbeddingClient(endpoint, "test-key", profile(cfg["values"]))
        try:

            async def permit(candidate):
                await index.check_permit(env.lease, candidate, dispatch_token="tcp")

            result = await client.dispatch(prepared, permitcheck=permit)
            await index.complete(env.lease, prepared, result, dispatch_token="tcp")
        finally:
            await client.aclose()
        assert len(requests) == 1
        wire = json.loads(requests[0]["body"])
        assert wire["task"] == "retrieval.query"
        assert len(wire["input"]) == 1 and "text" not in wire["input"][0]
        assert base64.b64decode(wire["input"][0]["image"]) == content
        bundle = await store.retrieve(env.lease, "query")
        assert bundle["status"] == "READY" and len(bundle["evidence"]) == 1
        evidence = bundle["evidence"][0]
        assert evidence["source_unit_id"] == found["source_unit_id"]
        assert [part["type"] for part in evidence["parts"]] == ["text", "image"]
        assert evidence["parts"][1]["availability"] == "AVAILABLE"
        async with env.store.pool.connection() as conn, conn.transaction():
            _, clock, conversation, _ = await index._guard(
                conn, env.lease, 1, asking["conversation_id"]
            )
            await store.read_in(conn, "query", conversation=conversation, input_cut=1)
            pixels = await images.materialize_in(
                conn,
                [found["source_unit_id"]],
                expected_deletion_epoch=clock["deletion_epoch"],
                part_refs=[(found["source_unit_id"], "1")],
            )
        assert pixels[(found["source_unit_id"], "1")].content == content
        assert not await rows(media, "SELECT * FROM public.bot_agent_config_revisions")


@pytest.mark.parametrize("status", ["UNKNOWN", "NOT_SENT"])
async def test_ambiguous_or_unstarted_embedding_freezes_explicit_degraded_evidence(media, status):
    env, images, _ = media
    await configure(env, [])
    asking = await source(media, 1)
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    planned = await query(media, store, asking)
    await embed(media, index, planned["facet_ids"], query_receipt=planned, status=status)
    bundle = await store.retrieve(env.lease, "query")
    assert bundle["status"] == "DEGRADED" and bundle["evidence"] == []
    assert bundle["reason_code"] == "MEMORY_QUERY_EMBEDDING_" + status
    assert bundle["completeness"]["query_facets_available"] == 0
    assert await read_frozen(media, store, planned) == bundle
    if status == "UNKNOWN":
        assert await index.retryable(env.lease, 1) is None


async def test_image_facet_limit_rejects_whole_query_and_missing_image_never_becomes_text(media):
    env, images, _ = media
    await configure(env, [], query_max_images=0)
    asking = await source(media, 1, url="https://untrusted.invalid/a", image_only=True)
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    with pytest.raises(ForestError) as budget:
        await query(media, store, asking)
    assert budget.value.code == "MEMORY_QUERY_BUDGET"
    assert not await rows(media, "SELECT * FROM public.bot_memory_queries")
    await configure(env, [], revision=1, query_max_images=1)
    planned = await query(media, store, asking, revision=2)
    with pytest.raises(ForestError) as unavailable:
        await index.prepare(
            env.lease,
            2,
            planned["facet_ids"],
            request_id="query",
            purpose="query",
            conversation_id=asking["conversation_id"],
            input_cut=1,
        )
    assert unavailable.value.code == "MEMORY_MEDIA_PENDING"
    bundle = await store.retrieve(env.lease, "query", error_code=unavailable.value.code)
    assert bundle["status"] == "DEGRADED" and bundle["evidence"] == []
    assert bundle["completeness"]["query_modalities"] == ["image"]
    assert not await rows(media, "SELECT * FROM public.bot_memory_embedding_requests")


async def test_request_budget_exhaustion_is_frozen_and_never_fakes_a_query_vector(media):
    env, images, _ = media
    await configure(env, [], daily_request_budget=1)
    asking = await source(media, 1)
    found = await source(media, 2, group="other")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [found["source_unit_id"]])
    planned = await query(media, store, asking)
    with pytest.raises(ForestError) as budget:
        await embed(media, index, planned["facet_ids"], query_receipt=planned)
    assert budget.value.code == "MEMORY_BUDGET_EXCEEDED"
    bundle = await store.retrieve(env.lease, "query", error_code=budget.value.code)
    assert bundle["status"] == "DEGRADED" and not bundle["evidence"]
    assert bundle["reason_code"] == "MEMORY_BUDGET_EXCEEDED"


@pytest.mark.parametrize("change", ["delete", "config"])
async def test_late_search_results_cannot_publish_after_deletion_or_config_revision(
    media, monkeypatch, change
):
    env, images, _ = media
    await configure(env, [])
    asking = await source(media, 1)
    found = await source(media, 2, group="other")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [found["source_unit_id"]])
    planned = await query(media, store, asking)
    await embed(media, index, planned["facet_ids"], query_receipt=planned)
    searching, release = asyncio.Event(), asyncio.Event()
    original = store._search

    async def delayed(*args):
        result = await original(*args)
        searching.set()
        await release.wait()
        return result

    monkeypatch.setattr(store, "_search", delayed)
    task = asyncio.create_task(store.retrieve(env.lease, "query"))
    await asyncio.wait_for(searching.wait(), 10)
    try:
        if change == "delete":
            await index.payloads.forget_sources(
                env.lease, "forget", [SourceRef(found["source_unit_id"])], expected_deletion_epoch=0
            )
        else:
            await configure(env, [], revision=1, enabled=False)
    finally:
        release.set()
    bundle = await task
    assert bundle["status"] == "DEGRADED" and bundle["evidence"] == []
    assert bundle["payload_id"] is None
    assert not await rows(
        media, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='memory.evidence'"
    )
    assert await store.retrieve(env.lease, "query") == bundle


async def test_frozen_evidence_body_is_physically_scrubbable_and_never_reselected(media):
    env, images, _ = media
    await configure(env, [])
    asking = await source(media, 1)
    found = await source(media, 2, group="other")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [found["source_unit_id"]])
    planned = await query(media, store, asking)
    await embed(media, index, planned["facet_ids"], query_receipt=planned)
    frozen = await store.retrieve(env.lease, "query")
    assert frozen["evidence"]
    await index.payloads.forget_sources(
        env.lease, "forget", [SourceRef(found["source_unit_id"])], expected_deletion_epoch=0
    )
    await index.payloads.scrub(env.lease)
    assert not await rows(
        media,
        "SELECT * FROM public.bot_payload_bodies WHERE payload_id=%s",
        (frozen["payload_id"],),
    )
    assert len(await rows(media, "SELECT * FROM public.bot_memory_evidence_bundles")) == 1
    with pytest.raises(ForestError) as revoked:
        await store.retrieve(env.lease, "query")
    assert revoked.value.code == "MEMORY_QUERY_REVOKED"


async def test_exact_dot_rank_uses_vector_coordinates_before_rrf_and_result_limit(media):
    env, images, _ = media
    await configure(env, [], query_limit=1)
    asking = await source(media, 1)
    irrelevant = await source(media, 2, group="orthogonal")
    relevant = await source(media, 3, group="same-axis")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await index.enroll(env.lease, 1)
    pending = [
        item
        for item in await index.pending(env.lease, 1)
        if item["source_unit_id"] != asking["source_unit_id"]
    ]
    assert {item["source_unit_id"] for item in pending} == {
        irrelevant["source_unit_id"],
        relevant["source_unit_id"],
    }
    await embed(
        media,
        index,
        [item["facet_id"] for item in pending],
        axes={
            item["facet_id"]: int(item["source_unit_id"] == relevant["source_unit_id"])
            for item in pending
        },
    )
    planned = await query(media, store, asking)
    await embed(
        media,
        index,
        planned["facet_ids"],
        query_receipt=planned,
        axes={key: 1 for key in planned["facet_ids"]},
    )
    result = await store.retrieve(env.lease, "query")
    assert [item["source_unit_id"] for item in result["evidence"]] == [relevant["source_unit_id"]]
    assert result["completeness"]["candidates_considered"] == 2


async def test_same_dimensions_from_another_embedding_space_are_never_compared(media):
    env, images, _ = media
    await configure(env, [])
    asking = await source(media, 1)
    found = await source(media, 2, group="other")
    index = MemoryIndexStore(images.messages, images)
    store = MemoryQueryStore(index)
    await documents(media, index, [found["source_unit_id"]])
    old_space = (await rows(media, "SELECT space_id FROM public.bot_memory_vectors"))[0]["space_id"]
    await configure(env, [], revision=1, deployment_id="different-embedding-installation")
    planned = await query(media, store, asking, revision=2)
    assert planned["space_id"] != old_space
    await embed(media, index, planned["facet_ids"], query_receipt=planned, revision=2)
    result = await store.retrieve(env.lease, "query")
    assert result["status"] == "READY" and result["evidence"] == []
    assert result["completeness"]["candidates_considered"] == 0

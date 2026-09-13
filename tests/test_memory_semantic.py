"""Convergent semantic associations derived from already-paid embeddings."""

import asyncio

import pytest
from test_memory_config import environment as environment
from test_memory_media import configure
from test_memory_media import media as media
from test_memory_queries import documents, source

from forest.memory_types import SourceRef
from forest.storage.memory_index import MemoryIndexStore
from forest.storage.memory_queries import MemoryQueryStore
from forest.storage.memory_semantic import MemorySemanticAssociationStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def rows(media, query, parameters=()):
    async with media[0].store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


async def settle(store, lease, *, limit=32):
    results = []
    for _ in range(limit):
        result = await store.process_one(lease)
        if result is None:
            return results
        results.append(result)
        if result["affected"]:
            await store.enqueue_sources(lease, result["space_id"], result["affected"])
    pytest.fail("Semantic association tasks did not converge")


async def test_cross_trunk_vector_votes_converge_under_parallel_retries(media):
    env, images, _ = media
    await configure(env, [])
    left = await source(media, 1, group="semantic-left", text="same subject")
    right = await source(media, 2, group="semantic-right", text="same subject")
    index = MemoryIndexStore(images.messages, images)
    await documents(media, index, [left["source_unit_id"], right["source_unit_id"]])
    semantic = MemorySemanticAssociationStore(images.messages)

    first = await asyncio.gather(
        semantic.process_one(env.lease), semantic.process_one(env.lease)
    )
    for result in first:
        assert result and result["outcome"] == "CURRENT"
        if result["affected"]:
            await semantic.enqueue_sources(
                env.lease, result["space_id"], result["affected"]
            )
    await settle(semantic, env.lease)

    votes = await rows(
        media,
        "SELECT association_id,origin_source_unit_id,neighbor_source_unit_id,revision,"
        "score_ppm,current_contribution_id FROM public.bot_memory_semantic_votes "
        "WHERE active ORDER BY origin_source_unit_id",
    )
    assert len(votes) == 2
    assert len({row["association_id"] for row in votes}) == 1
    assert {row["origin_source_unit_id"] for row in votes} == {
        left["source_unit_id"], right["source_unit_id"]
    }
    assert all(row["revision"] == 1 and row["score_ppm"] == 1_000_000 for row in votes)
    assert len({row["current_contribution_id"] for row in votes}) == 2
    assert len(await rows(media, "SELECT * FROM public.bot_memory_semantic_vote_changes")) == 2

    pending = await semantic.graph.pending(env.lease)
    assert len(pending["scopes"]) == 1
    await semantic.graph.refresh(env.lease, pending["scopes"][0])
    projection = await semantic.graph.read(env.lease, pending["scopes"][0].scope_id)
    edges = [edge for edge in projection["edges"] if edge["kind"] == "ASSOCIATES_WITH"]
    assert len(edges) == 1
    assert edges[0]["contribution_ids"] == sorted(
        row["current_contribution_id"] for row in votes
    )
    space_id = (
        await rows(media, "SELECT space_id FROM public.bot_memory_semantic_votes LIMIT 1")
    )[0]["space_id"]
    query_store = MemoryQueryStore(index)
    async with env.store.pool.connection() as conn, conn.transaction():
        conversation = await images.messages._conversation(conn, left["conversation_id"])
        expanded, detail = await query_store._expand_associations_in(
            conn,
            {
                "input_cut": left["ingress_seq"],
                "source_unit_ids": [left["source_unit_id"]],
                "space_id": space_id,
            },
            conversation,
            [{"source_unit_id": left["source_unit_id"], "rank_score": 1.0, "matches": []}],
        )
    assert [row["source_unit_id"] for row in expanded] == [
        left["source_unit_id"], right["source_unit_id"]
    ]
    assert expanded[1]["association"]["association_origin"] == "semantic"
    assert expanded[1]["association"]["score_ppm"] == 1_000_000
    assert detail["association_sources_added"] == 1
    async with env.store.pool.connection() as conn, conn.transaction():
        conversation = await images.messages._conversation(conn, left["conversation_id"])
        disabled, _ = await query_store._expand_associations_in(
            conn,
            {
                "input_cut": left["ingress_seq"],
                "source_unit_ids": [left["source_unit_id"]],
                "space_id": space_id,
            },
            conversation,
            [{"source_unit_id": left["source_unit_id"], "rank_score": 1.0, "matches": []}],
            semantic_enabled=False,
        )
    assert [row["source_unit_id"] for row in disabled] == [left["source_unit_id"]]

    # Re-queueing and racing the same immutable vectors changes only task watermarks.
    await semantic.enqueue_sources(
        env.lease, space_id, [left["source_unit_id"], right["source_unit_id"]]
    )
    await asyncio.gather(
        semantic.process_one(env.lease), semantic.process_one(env.lease)
    )
    await settle(semantic, env.lease)
    assert len(await rows(media, "SELECT * FROM public.bot_memory_semantic_vote_changes")) == 2
    tasks = await rows(
        media,
        "SELECT requested_revision,processed_revision,outcome FROM public.bot_memory_semantic_tasks",
    )
    assert tasks and all(
        row["requested_revision"] == row["processed_revision"]
        and row["outcome"] == "CURRENT"
        for row in tasks
    )

    await PayloadStore(env.store).forget_sources(
        env.lease,
        "forget-semantic-right",
        (SourceRef(right["source_unit_id"]),),
        expected_deletion_epoch=0,
    )
    assert await semantic.repair_stale(env.lease) == 2
    await settle(semantic, env.lease)
    assert await rows(
        media, "SELECT * FROM public.bot_memory_semantic_votes WHERE active"
    ) == []
    assert len(await rows(media, "SELECT * FROM public.bot_memory_semantic_vote_changes")) == 4
    pending = await semantic.graph.pending(env.lease)
    assert len(pending["scopes"]) == 1
    await semantic.graph.refresh(env.lease, pending["scopes"][0])
    projection = await semantic.graph.read(env.lease, pending["scopes"][0].scope_id)
    assert not [edge for edge in projection["edges"] if edge["kind"] == "ASSOCIATES_WITH"]


async def test_semantic_maintenance_never_links_two_units_on_one_trunk(media):
    env, images, _ = media
    await configure(env, [])
    first = await source(media, 1, group="one-trunk", text="same")
    second = await source(media, 2, group="one-trunk", text="same")
    index = MemoryIndexStore(images.messages, images)
    await documents(media, index, [first["source_unit_id"], second["source_unit_id"]])
    semantic = MemorySemanticAssociationStore(images.messages)
    await settle(semantic, env.lease)
    assert await rows(media, "SELECT * FROM public.bot_memory_semantic_votes") == []


async def test_candidate_capacity_retracts_the_origins_previous_partial_view(
    media, monkeypatch
):
    env, images, _ = media
    await configure(env, [])
    origin = await source(media, 1, group="capacity-origin", text="same")
    neighbor = await source(media, 2, group="capacity-neighbor", text="same")
    index = MemoryIndexStore(images.messages, images)
    await documents(media, index, [origin["source_unit_id"], neighbor["source_unit_id"]])
    semantic = MemorySemanticAssociationStore(images.messages)
    await settle(semantic, env.lease)

    space_id = (
        await rows(media, "SELECT space_id FROM public.bot_memory_semantic_votes LIMIT 1")
    )[0]["space_id"]
    await semantic.enqueue_sources(env.lease, space_id, [origin["source_unit_id"]])

    async def over_limit(_conn, _task):
        return [None] * 4097

    monkeypatch.setattr(semantic, "_candidates_in", over_limit)
    result = await semantic.process_one(env.lease)
    assert result["outcome"] == "CAPACITY_EXCEEDED"
    assert result["affected"] == []
    task = (
        await rows(
            media,
            "SELECT requested_revision,processed_revision,outcome,association_count "
            "FROM public.bot_memory_semantic_tasks WHERE source_unit_id=%s",
            (origin["source_unit_id"],),
        )
    )[0]
    assert task["requested_revision"] == task["processed_revision"]
    assert task["outcome"] == "CAPACITY_EXCEEDED"
    assert task["association_count"] == 0
    assert await rows(
        media,
        "SELECT * FROM public.bot_memory_semantic_votes "
        "WHERE origin_source_unit_id=%s AND active",
        (origin["source_unit_id"],),
    ) == []


async def test_equal_score_top_k_uses_stable_source_identity_after_async_fan_in(media):
    env, images, _ = media
    await configure(env, [])
    sources = [
        await source(media, number, group=f"fan-in-{number}", text="same vector")
        for number in range(1, 7)
    ]
    index = MemoryIndexStore(images.messages, images)
    await documents(media, index, [row["source_unit_id"] for row in sources])
    semantic = MemorySemanticAssociationStore(images.messages)
    await settle(semantic, env.lease)

    votes = await rows(
        media,
        "SELECT origin_source_unit_id,neighbor_source_unit_id FROM "
        "public.bot_memory_semantic_votes WHERE active "
        "ORDER BY origin_source_unit_id,neighbor_source_unit_id",
    )
    identities = sorted(row["source_unit_id"] for row in sources)
    actual = {}
    for row in votes:
        actual.setdefault(row["origin_source_unit_id"], []).append(
            row["neighbor_source_unit_id"]
        )
    assert actual == {
        origin: sorted(identity for identity in identities if identity != origin)[:4]
        for origin in identities
    }
    tasks = await rows(
        media,
        "SELECT requested_revision,processed_revision FROM public.bot_memory_semantic_tasks",
    )
    assert len(tasks) == 6
    assert all(row["requested_revision"] == row["processed_revision"] for row in tasks)

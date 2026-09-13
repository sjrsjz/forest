"""Real PostgreSQL embedding ownership, cost and immutable source projections."""

import asyncio
import json
from dataclasses import replace

import pytest
from test_memory_config import environment as environment
from test_memory_media import configure
from test_memory_media import media as media
from test_messages import event, owner

from forest.embeddings import EmbeddingResult, EmbeddingVector
from forest.errors import ForestError
from forest.memory_index import facets, profile, text_chunks
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS
from forest.memory_types import SourceRef
from forest.storage.memory_index import MemoryIndexStore

pytestmark = pytest.mark.postgres


async def text_source(media, number=1, *, group="group-1", thread="", text="原始来源", drain=True):
    env, images, _ = media
    message = event(number, group=group)
    message.update(thread_id=thread, segments=[{"type": "text", "text": text}])
    receipt = await images.messages.ingest(env.lease, "binding-a", message, decision="NO_TRIGGER")
    if drain:
        actor = await owner(images.messages, env.lease, receipt["conversation_id"])
        await images.messages.select_input(actor, "drain-" + str(number))
    return receipt


async def rows(media, sql, parameters=()):
    async with media[0].store.pool.connection() as conn:
        return await (await conn.execute(sql, parameters)).fetchall()


async def ready(media, *, config=None):
    env, images, _ = media
    await configure(env, [], **(config or {}))
    await text_source(media)
    store = MemoryIndexStore(images.messages, images)
    await store.enroll(env.lease, 1)
    pending = await store.pending(env.lease, 1)
    return store, [row["facet_id"] for row in pending]


def outcome(prepared, status="RESPONDED", *, tokens=7):
    vectors = (
        tuple(
            EmbeddingVector(facet["facet_id"], index, tuple([1.0] + [0.0] * 511))
            for index, facet in enumerate(prepared.manifest["facets"])
        )
        if status == "RESPONDED"
        else ()
    )
    return EmbeddingResult(
        status,
        prepared.request_id,
        prepared.transport_attempt_id,
        prepared.payload_digest,
        prepared.space_id,
        vectors,
        json.dumps({"total_tokens": tokens}) if vectors else None,
    )


async def test_only_eligible_inputs_enroll_and_each_same_text_source_keeps_identity(media):
    env, images, _ = media
    await configure(env, [])
    receipt = await text_source(media, drain=False)
    store = MemoryIndexStore(images.messages, images)
    assert (await store.enroll(env.lease, 1))["added"] == 0
    actor = await owner(images.messages, env.lease, receipt["conversation_id"])
    await images.messages.select_input(actor, "drain")
    await text_source(media, 2, group="other")
    assert (await store.enroll(env.lease, 1))["added"] == 2
    assert (await store.enroll(env.lease, 1))["added"] == 0
    assert len(await store.pending(env.lease, 1)) == 2


async def test_concurrent_workers_select_only_one_request_and_vector_is_fixed(media):
    store, ids = await ready(media)
    lease = media[0].lease
    results = await asyncio.gather(
        *(store.prepare(lease, 1, ids) for _ in range(8)), return_exceptions=True
    )
    admitted = [r for r in results if isinstance(r, dict)]
    assert len(admitted) == 1
    assert all(r.code == "MEMORY_FACET_PENDING" for r in results if isinstance(r, ForestError))
    prepared = admitted[0]["prepared"]
    await store.check_permit(lease, prepared, dispatch_token="wire-owner")
    result = outcome(prepared)
    completed = await store.complete(lease, prepared, result, dispatch_token="wire-owner")
    assert completed["published"]
    assert (await store.complete(lease, prepared, result, dispatch_token="wire-owner"))[
        "status"
    ] == "RESPONDED"
    with pytest.raises(ForestError, match="another observation"):
        await store.complete(
            lease, prepared, outcome(prepared, tokens=9), dispatch_token="wire-owner"
        )
    vectors = await rows(media, "SELECT * FROM public.bot_memory_vectors")
    assert len(vectors) == 1 and vectors[0]["facet_id"] == ids[0]
    budget = (await rows(media, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0]
    assert (
        budget["charged_tokens"] == 7 and budget["reserved_tokens"] == 0 and budget["requests"] == 1
    )


async def test_not_sent_preserves_exact_bytes_and_unknown_never_enters_retry_queue(media):
    store, ids = await ready(media)
    lease = media[0].lease
    first = await store.prepare(lease, 1, ids, request_id="stable")
    original = first["prepared"]
    await store.complete(lease, original, outcome(original, "NOT_SENT"))
    assert (await store.retryable(lease, 1))["request_id"] == "stable"
    second = await store.prepare(lease, 1, ids, request_id="stable")
    retry = second["prepared"]
    assert retry.body == original.body and retry.manifest == original.manifest
    assert retry.transport_attempt_id != original.transport_attempt_id
    await store.check_permit(lease, retry, dispatch_token="second")
    await store.complete(lease, retry, outcome(retry, "UNKNOWN"), dispatch_token="second")
    assert await store.retryable(lease, 1) is None
    assert not await store.pending(lease, 1)
    budget = (await rows(media, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0]
    assert budget["reserved_tokens"] == DEFAULT_MEMORY_SETTINGS["request_token_reservation"]
    assert budget["charged_tokens"] == 0


async def test_forgotten_source_blocks_late_result_and_scrubs_vector_projection(media):
    store, ids = await ready(media)
    lease = media[0].lease
    prepared = (await store.prepare(lease, 1, ids))["prepared"]
    await store.check_permit(lease, prepared, dispatch_token="owner")
    ref = SourceRef(prepared.manifest["facets"][0]["source"]["source_unit_id"])
    await store.payloads.forget_sources(lease, "erase", [ref], expected_deletion_epoch=0)
    result = await store.complete(lease, prepared, outcome(prepared), dispatch_token="owner")
    assert not result["published"]
    assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
    budget = (await rows(media, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0]
    assert budget["charged_tokens"] == 7 and budget["reserved_tokens"] == 0
    await store.payloads.scrub(lease)
    assert (await store.enroll(lease, 1))["scanned"] == 0


async def test_actual_vector_body_deletion_removes_search_projection(media):
    store, ids = await ready(media)
    lease = media[0].lease
    prepared = (await store.prepare(lease, 1, ids))["prepared"]
    await store.check_permit(lease, prepared, dispatch_token="owner")
    await store.complete(lease, prepared, outcome(prepared), dispatch_token="owner")
    assert len(await rows(media, "SELECT * FROM public.bot_memory_vectors")) == 1
    ref = SourceRef(prepared.manifest["facets"][0]["source"]["source_unit_id"])
    await store.payloads.forget_sources(lease, "erase", [ref], expected_deletion_epoch=0)
    await store.payloads.scrub(lease)
    assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")


async def test_budget_reservation_rolls_back_second_group_without_orphan_request(media):
    store, ids = await ready(media, config={"daily_request_budget": 1})
    await text_source(media, 2, group="other")
    await store.enroll(media[0].lease, 1)
    pending = [row["facet_id"] for row in await store.pending(media[0].lease, 1)]
    other = next(key for key in pending if key not in ids)
    await store.prepare(media[0].lease, 1, ids)
    with pytest.raises(ForestError) as rejected:
        await store.prepare(media[0].lease, 1, [other])
    assert rejected.value.code == "MEMORY_BUDGET_EXCEEDED"
    assert len(await rows(media, "SELECT * FROM public.bot_memory_embedding_requests")) == 1
    assert len(await rows(media, "SELECT * FROM public.bot_memory_embedding_attempts")) == 1


async def test_cut_and_transport_owner_cannot_be_bypassed(media):
    store, ids = await ready(media)
    lease = media[0].lease
    second = await text_source(media, 1, thread="second", drain=False)
    with pytest.raises(ForestError) as cut:
        await store.prepare(
            lease, 1, ids, purpose="query", conversation_id=second["conversation_id"], input_cut=0
        )
    assert cut.value.code == "MEMORY_SOURCE_UNAVAILABLE"
    prepared = (await store.prepare(lease, 1, ids))["prepared"]
    await store.check_permit(lease, prepared, dispatch_token="actual")
    with pytest.raises(ForestError) as owner:
        await store.complete(lease, prepared, outcome(prepared, "NOT_SENT"), dispatch_token="loser")
    assert owner.value.code == "MEMORY_ATTEMPT_REVOKED"
    assert (await rows(media, "SELECT state FROM public.bot_memory_embedding_attempts"))[0][
        "state"
    ] == "DISPATCHING"
    with pytest.raises(ForestError):
        await store.check_permit(
            replace(lease, owner_epoch=lease.owner_epoch + 1), prepared, dispatch_token="actual"
        )


def test_unicode_chunk_spans_cover_source_without_whitespace_loss():
    text = " 你好🌳\n" * 4096
    chunks = list(text_chunks(text))
    assert "".join(part for _, _, part in chunks) == text
    assert all(
        len(part.encode()) <= 4096 and text[start:end] == part for start, end, part in chunks
    )
    source = {
        "source_unit_id": "unit",
        "source_revision": 1,
        "content_digest": "digest",
        "parts": [{"type": "text", "part_id": "0", "text": text}],
    }
    entries = facets(source, profile(DEFAULT_MEMORY_SETTINGS).space_id)
    assert len({row["facet_id"] for row in entries}) == len(chunks)

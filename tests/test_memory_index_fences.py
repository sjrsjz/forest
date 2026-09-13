"""PostgreSQL acceptance fences shared by document and foreground query work."""

import asyncio
import json
from dataclasses import replace

import psycopg
import pytest
from test_memory_config import environment as environment
from test_memory_index_store import outcome, ready, rows, text_source
from test_memory_media import configure
from test_memory_media import media as media

from forest.embeddings import EmbeddingInput, prepare_embedding_request
from forest.errors import ForestError
from forest.memory_index import profile
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS

pytestmark = pytest.mark.postgres


async def query_scope(media):
    return (
        await rows(
            media,
            "SELECT conversation_id,context_applied_seq FROM public.bot_conversations ORDER BY conversation_id",
        )
    )[0]


async def budget(media):
    values = await rows(
        media,
        "SELECT reserved_tokens,charged_tokens,requests FROM public.bot_memory_embedding_budget_days",
    )
    assert len(values) == 1
    return values[0]


async def test_bot_concurrency_counts_foreground_queries_and_document_reservations(media):
    index, ids = await ready(media, config={"index_concurrency": 1})
    env = media[0]
    scope = await query_scope(media)
    barrier = asyncio.Event()

    async def enter(number):
        await barrier.wait()
        query = (
            {
                "purpose": "query",
                "conversation_id": scope["conversation_id"],
                "input_cut": scope["context_applied_seq"],
            }
            if number
            else {}
        )
        return await index.prepare(env.lease, 1, ids, request_id=f"concurrent-{number}", **query)

    tasks = [asyncio.create_task(enter(number)) for number in range(5)]
    barrier.set()
    answers = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(item, (dict, ForestError)) for item in answers), answers
    admitted = [item for item in answers if isinstance(item, dict)]
    assert len(admitted) == 1, answers
    assert len(await rows(media, "SELECT * FROM public.bot_memory_embedding_attempts")) == 1
    assert len(await rows(media, "SELECT * FROM public.bot_memory_embedding_requests")) == 1
    assert (await budget(media))["requests"] == 1
    prepared = admitted[0]["prepared"]
    await index.check_permit(env.lease, prepared, dispatch_token="one-slot")
    with pytest.raises(ForestError):
        await index.prepare(
            env.lease,
            1,
            ids,
            request_id="while-dispatching",
            purpose="query",
            conversation_id=scope["conversation_id"],
            input_cut=scope["context_applied_seq"],
        )
    await index.complete(
        env.lease, prepared, outcome(prepared, "NOT_SENT"), dispatch_token="one-slot"
    )
    next_request = await index.prepare(
        env.lease,
        1,
        ids,
        request_id="released-slot",
        purpose="query",
        conversation_id=scope["conversation_id"],
        input_cut=scope["context_applied_seq"],
    )
    assert next_request["status"] == "PREPARED"
    current = await budget(media)
    assert (
        current["requests"] == 2
        and current["reserved_tokens"] == DEFAULT_MEMORY_SETTINGS["request_token_reservation"]
    )


@pytest.mark.parametrize("invalid_index", [-1, 1, True])
async def test_forged_vector_position_cannot_commit_or_charge(media, invalid_index):
    index, ids = await ready(media)
    lease = media[0].lease
    prepared = (await index.prepare(lease, 1, ids))["prepared"]
    await index.check_permit(lease, prepared, dispatch_token="owner")
    original = outcome(prepared)
    altered = replace(original, vectors=(replace(original.vectors[0], index=invalid_index),))
    before = await budget(media)
    with pytest.raises(ForestError):
        await index.complete(lease, prepared, altered, dispatch_token="owner")
    assert await budget(media) == before
    assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
    assert (
        await rows(media, "SELECT state,result_digest FROM public.bot_memory_embedding_attempts")
    )[0] == {"state": "DISPATCHING", "result_digest": None}
    assert (await index.complete(lease, prepared, original, dispatch_token="owner"))["published"]


async def test_two_facets_must_retain_exact_frozen_index_to_source_mapping(media):
    index, _ = await ready(media)
    lease = media[0].lease
    await text_source(media, 2, group="separate-group")
    await index.enroll(lease, 1)
    ids = [row["facet_id"] for row in await index.pending(lease, 1)]
    assert len(ids) == 2
    prepared = (await index.prepare(lease, 1, ids))["prepared"]
    await index.check_permit(lease, prepared, dispatch_token="two")
    original = outcome(prepared)
    swapped = replace(
        original,
        vectors=tuple(replace(vector, index=1 - vector.index) for vector in original.vectors),
    )
    with pytest.raises(ForestError):
        await index.complete(lease, prepared, swapped, dispatch_token="two")
    duplicate = replace(
        original, vectors=tuple(replace(vector, index=0) for vector in original.vectors)
    )
    with pytest.raises(ForestError):
        await index.complete(lease, prepared, duplicate, dispatch_token="two")
    assert (await index.complete(lease, prepared, original, dispatch_token="two"))["published"]
    assert len(await rows(media, "SELECT * FROM public.bot_memory_vectors")) == 2


async def test_observation_cannot_supply_altered_wire_with_original_declared_digest(media):
    index, ids = await ready(media)
    lease = media[0].lease
    prepared = (await index.prepare(lease, 1, ids))["prepared"]
    await index.check_permit(lease, prepared, dispatch_token="owner")
    altered = replace(prepared, body=prepared.body + b" ")
    with pytest.raises(ForestError):
        await index.complete(lease, altered, outcome(prepared), dispatch_token="owner")
    assert (await budget(media))["charged_tokens"] == 0
    assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
    assert (await index.complete(lease, prepared, outcome(prepared), dispatch_token="owner"))[
        "published"
    ]


async def test_valid_request_with_same_wire_cannot_rebind_observation_provenance(media):
    index, ids = await ready(media)
    lease = media[0].lease
    prepared = (await index.prepare(lease, 1, ids))["prepared"]
    await index.check_permit(lease, prepared, dispatch_token="owner")
    original_source = prepared.manifest["facets"][0]["source"]
    changed = prepare_embedding_request(
        profile(DEFAULT_MEMORY_SETTINGS),
        [
            EmbeddingInput(
                ids[0],
                text=json.loads(prepared.body)["input"][0]["text"],
                source_json=json.dumps({**original_source, "principal_ref": "other-author"}),
            )
        ],
        request_id=prepared.request_id,
        transport_attempt_id=prepared.transport_attempt_id,
    )
    changed.verify()
    assert changed.body == prepared.body and changed.payload_digest == prepared.payload_digest
    with pytest.raises(ForestError) as rejected:
        await index.complete(lease, changed, outcome(changed), dispatch_token="owner")
    assert rejected.value.code == "MEMORY_RESULT_MISMATCH"
    assert (await budget(media))["charged_tokens"] == 0
    assert (await index.complete(lease, prepared, outcome(prepared), dispatch_token="owner"))[
        "published"
    ]


async def test_old_configuration_reservation_still_occupies_new_configuration_pool(media):
    index, ids = await ready(media, config={"index_concurrency": 1})
    env = media[0]
    scope = await query_scope(media)
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    await configure(env, [], revision=1, index_concurrency=1)
    with pytest.raises(ForestError) as revoked:
        await index.check_permit(env.lease, prepared, dispatch_token="stale")
    assert revoked.value.code == "MEMORY_CONFIGURATION_REVOKED"
    with pytest.raises(ForestError) as occupied:
        await index.prepare(
            env.lease,
            2,
            ids,
            request_id="current-query",
            purpose="query",
            conversation_id=scope["conversation_id"],
            input_cut=scope["context_applied_seq"],
        )
    assert occupied.value.code == "MEMORY_CONCURRENCY_EXCEEDED"
    assert (await budget(media))["requests"] == 1
    await index.complete(env.lease, prepared, outcome(prepared, "NOT_SENT"))
    accepted = await index.prepare(
        env.lease,
        2,
        ids,
        request_id="current-query",
        purpose="query",
        conversation_id=scope["conversation_id"],
        input_cut=scope["context_applied_seq"],
    )
    assert accepted["status"] == "PREPARED"
    assert (await budget(media))["requests"] == 2


async def test_config_switch_settles_old_known_cost_once_without_old_publication(media):
    index, ids = await ready(
        media, config={"request_token_reservation": 10, "daily_token_budget": 20}
    )
    env = media[0]
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    await index.check_permit(env.lease, prepared, dispatch_token="old")
    await configure(
        env, [], revision=1, enabled=False, request_token_reservation=10, daily_token_budget=20
    )
    completed = await index.complete(
        env.lease, prepared, outcome(prepared, tokens=7), dispatch_token="old"
    )
    assert completed["status"] == "RESPONDED" and not completed["published"]
    charged = await budget(media)
    assert charged == {"reserved_tokens": 0, "charged_tokens": 7, "requests": 1}
    await index.complete(env.lease, prepared, outcome(prepared, tokens=7), dispatch_token="old")
    assert await budget(media) == charged
    with pytest.raises(ForestError) as conflict:
        await index.complete(env.lease, prepared, outcome(prepared, tokens=8), dispatch_token="old")
    assert conflict.value.code == "MEMORY_RESULT_CONFLICT"
    assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
    await configure(env, [], revision=2, request_token_reservation=10, daily_token_budget=16)
    await index.enroll(env.lease, 3)
    with pytest.raises(ForestError) as exhausted:
        await index.prepare(env.lease, 3, ids)
    assert exhausted.value.code == "MEMORY_BUDGET_EXCEEDED"
    assert await budget(media) == charged
    assert len(await rows(media, "SELECT * FROM public.bot_memory_embedding_requests")) == 1
    await configure(env, [], revision=3, request_token_reservation=10, daily_token_budget=17)
    assert (await index.prepare(env.lease, 4, ids))["status"] == "PREPARED"
    assert await budget(media) == {"reserved_tokens": 10, "charged_tokens": 7, "requests": 2}


async def test_configuration_cannot_reset_unknown_cost_reservation(media):
    index, ids = await ready(
        media, config={"request_token_reservation": 10, "daily_token_budget": 20}
    )
    env = media[0]
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    await index.check_permit(env.lease, prepared, dispatch_token="unknown")
    await index.complete(
        env.lease, prepared, outcome(prepared, "UNKNOWN"), dispatch_token="unknown"
    )
    await configure(env, [], revision=1, request_token_reservation=10, daily_token_budget=19)
    await text_source(media, 2, group="new-group")
    await index.enroll(env.lease, 2)
    other = [row["facet_id"] for row in await index.pending(env.lease, 2)]
    assert len(other) == 1 and other != ids
    with pytest.raises(ForestError) as exhausted:
        await index.prepare(env.lease, 2, other)
    assert exhausted.value.code == "MEMORY_BUDGET_EXCEEDED"
    assert await budget(media) == {"reserved_tokens": 10, "charged_tokens": 0, "requests": 1}
    assert await index.retryable(env.lease, 2) is None


async def test_database_attempt_identity_and_dispatch_owner_are_immutable(media):
    index, ids = await ready(media)
    env = media[0]
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    statements = [
        "UPDATE public.bot_memory_embedding_attempts SET attempt_id='replacement'",
        "UPDATE public.bot_memory_embedding_attempts SET host_owner_epoch=host_owner_epoch+1",
        "UPDATE public.bot_memory_embedding_attempts SET budget_day=budget_day+1",
        "UPDATE public.bot_memory_embedding_attempts SET reserved_tokens=reserved_tokens+1",
        "UPDATE public.bot_memory_embedding_attempts SET deadline_at=deadline_at+interval '1 second'",
    ]
    for statement in statements:
        async with env.store.pool.connection() as conn:
            with pytest.raises(psycopg.Error):
                async with conn.transaction():
                    await conn.execute(statement)
    await index.check_permit(env.lease, prepared, dispatch_token="fixed-owner")
    async with env.store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE public.bot_memory_embedding_attempts SET dispatch_token='replacement'"
                )
    assert (
        await index.complete(env.lease, prepared, outcome(prepared), dispatch_token="fixed-owner")
    )["published"]


async def test_database_terminal_observation_and_budget_receipt_cannot_be_rewritten(media):
    index, ids = await ready(media)
    env = media[0]
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    await index.check_permit(env.lease, prepared, dispatch_token="fixed")
    result = outcome(prepared)
    await index.complete(env.lease, prepared, result, dispatch_token="fixed")
    before = (await rows(media, "SELECT * FROM public.bot_memory_embedding_attempts"))[0]
    for statement in (
        "UPDATE public.bot_memory_embedding_attempts SET state='PREPARED'",
        "UPDATE public.bot_memory_embedding_attempts SET result_digest='forged'",
        "UPDATE public.bot_memory_embedding_attempts SET charged_tokens=9",
        "DELETE FROM public.bot_memory_embedding_attempts",
    ):
        async with env.store.pool.connection() as conn:
            with pytest.raises(psycopg.Error):
                async with conn.transaction():
                    await conn.execute(statement)
    assert (await rows(media, "SELECT * FROM public.bot_memory_embedding_attempts"))[0] == before
    assert (await index.complete(env.lease, prepared, result, dispatch_token="fixed"))[
        "status"
    ] == "RESPONDED"
    assert (await budget(media))["charged_tokens"] == 7

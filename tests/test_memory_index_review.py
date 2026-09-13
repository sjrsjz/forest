"""Deterministic runtime ownership faults; repository security is tested on PostgreSQL elsewhere."""

import asyncio
from types import SimpleNamespace

import pytest
from test_embeddings import response, send, server
from test_memory_config import environment as environment
from test_memory_index_store import outcome, ready, rows
from test_memory_media import media as media

from forest.embeddings import EmbeddingCancelled, EmbeddingInput, prepare_embedding_request
from forest.errors import ForestError
from forest.memory_index import profile
from forest.memory_runtime import MemoryRuntime
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS


async def empty(*args, **kwargs):
    return []


def runtime_fixture():
    store = SimpleNamespace(bot_id="review-bot")
    runtime = MemoryRuntime(
        SimpleNamespace(
            bot=SimpleNamespace(store=store),
            messages=SimpleNamespace(store=store),
            kernel=SimpleNamespace(_authority=lambda: "host"),
            vault=SimpleNamespace(decrypt=lambda value: "test-key"),
        )
    )
    cfg = {
        "revision": 1,
        "values": {**DEFAULT_MEMORY_SETTINGS, "enabled": True},
        "secret_ciphertext": "host-only",
    }

    async def config(host):
        return cfg

    runtime.config = SimpleNamespace(runtime=config)
    runtime.index = SimpleNamespace(recover=empty, enroll=empty, retryable=empty, pending=empty)
    runtime.media = SimpleNamespace(recover=empty, scan=empty)
    return runtime, cfg


async def test_empty_graph_completion_does_not_self_wake_another_scan():
    runtime, _ = runtime_fixture()
    entered = asyncio.Event()
    scans, graphs = [], []

    async def enroll(*args, **kwargs):
        scans.append("scan")

    async def graph(host):
        graphs.append("graph")
        entered.set()

    runtime.index.enroll = enroll
    runtime._graph = graph
    try:
        await runtime.start()
        await asyncio.wait_for(entered.wait(), 2)
        # Well below the configured 2-second idle poll. Finished graph work must
        # not perpetually wake its own next scan when there are no new facts.
        await asyncio.sleep(0.15)
        assert scans == ["scan"] and graphs == ["graph"]
        assert runtime.errors == {}
    finally:
        await runtime.close()


async def test_close_fences_pending_result_after_scan_dependency_consumes_cancel():
    runtime, _ = runtime_fixture()
    entered = asyncio.Event()
    executed = []

    async def pending(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return [{"facet_id": "late-facet", "modality": "text"}]

    async def embed(*args, **kwargs):
        executed.append("embed")
        await asyncio.Event().wait()

    runtime.index.pending = pending
    runtime.embed = embed
    try:
        await runtime.start()
        await asyncio.wait_for(entered.wait(), 2)
        async with asyncio.timeout(1):
            await runtime.close()
        assert not executed and runtime.task.done()
        assert not runtime.workers and not runtime.clients
    finally:
        await runtime.close()


@pytest.mark.postgres
@pytest.mark.parametrize("phase", ["permit", "publication"])
async def test_real_row_lock_wait_crossing_deadline_rolls_back_final_effect(
    media, monkeypatch, phase
):
    index, ids = await ready(media)
    env = media[0]
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    if phase == "publication":
        await index.check_permit(env.lease, prepared, dispatch_token="owner")
    entered = asyncio.Event()
    original = index._inputs_in

    async def wait_on_lock(conn, *args, **kwargs):
        result = await original(conn, *args, **kwargs)
        entered.set()
        await conn.execute("SELECT gate_id FROM public.memory_review_gate FOR UPDATE")
        return result

    original_put = index.payloads.put_json_in

    async def wait_after_vector_write(conn, **kwargs):
        result = await original_put(conn, **kwargs)
        if kwargs["owner_kind"] == "memory.vector":
            entered.set()
            await conn.execute("SELECT gate_id FROM public.memory_review_gate FOR UPDATE")
        return result

    if phase == "permit":
        monkeypatch.setattr(index, "_inputs_in", wait_on_lock)
    else:
        monkeypatch.setattr(index.payloads, "put_json_in", wait_after_vector_write)
    work = None
    try:
        async with env.store.pool.connection() as blocking:
            await blocking.execute(
                "CREATE TABLE public.memory_review_gate(gate_id int PRIMARY KEY)"
            )
            await blocking.execute("INSERT INTO public.memory_review_gate VALUES (1)")
            await blocking.execute(
                "UPDATE public.bot_memory_embedding_attempts SET deadline_at=clock_timestamp()+interval '1 second' WHERE attempt_id=%s",
                (prepared.transport_attempt_id,),
            )
            await blocking.commit()
            async with blocking.transaction():
                await blocking.execute("SELECT gate_id FROM public.memory_review_gate FOR UPDATE")
                work = asyncio.create_task(
                    index.check_permit(env.lease, prepared, dispatch_token="owner")
                    if phase == "permit"
                    else index.complete(
                        env.lease, prepared, outcome(prepared), dispatch_token="owner"
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
                # A genuine second PostgreSQL transaction holds the final read
                # until the attempt's server-side deadline has elapsed.
                await blocking.execute(
                    "SELECT pg_sleep_until(deadline_at+interval '0.02 second') FROM public.bot_memory_embedding_attempts WHERE attempt_id=%s",
                    (prepared.transport_attempt_id,),
                )
                assert not work.done()
            with pytest.raises(ForestError) as expired:
                await asyncio.wait_for(work, 3)
        assert expired.value.code == (
            "MEMORY_PERMIT_REVOKED" if phase == "permit" else "MEMORY_PUBLICATION_EXPIRED"
        )
        attempt = (await rows(media, "SELECT * FROM public.bot_memory_embedding_attempts"))[0]
        assert attempt["state"] == ("PREPARED" if phase == "permit" else "DISPATCHING")
        assert attempt["result_payload_id"] is None and attempt["result_digest"] is None
        assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
        budget = (await rows(media, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0]
        assert (
            budget["charged_tokens"] == 0
            and budget["reserved_tokens"] == attempt["reserved_tokens"]
        )
        await index.recover(env.lease)
        recovered = (await rows(media, "SELECT state FROM public.bot_memory_embedding_attempts"))[0]
        assert recovered["state"] == ("NOT_SENT" if phase == "permit" else "UNKNOWN")
    finally:
        if work and not work.done():
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)


@pytest.mark.postgres
async def test_late_known_response_settles_cost_once_without_publishing(media):
    index, ids = await ready(media)
    env = media[0]
    prepared = (await index.prepare(env.lease, 1, ids))["prepared"]
    await index.check_permit(env.lease, prepared, dispatch_token="owner")
    async with env.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_memory_embedding_attempts SET deadline_at=clock_timestamp()-interval '1 second'"
        )
    result = await index.complete(env.lease, prepared, outcome(prepared), dispatch_token="owner")
    assert result["status"] == "RESPONDED" and not result["published"]
    assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
    before = (await rows(media, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0]
    assert before["charged_tokens"] == 7 and before["reserved_tokens"] == 0
    replay = await index.complete(env.lease, prepared, outcome(prepared), dispatch_token="owner")
    assert replay["status"] == "RESPONDED"
    assert (await rows(media, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0] == before


async def test_close_prevents_unregistered_query_from_starting_late_http_after_prepare():
    runtime, cfg = runtime_fixture()
    entered, release = asyncio.Event(), asyncio.Event()
    permits, observations = [], []

    async def handler(writer, record):
        body = response()
        body["data"][0]["embedding"] = [1.0] + [0.0] * 511
        await send(writer, body)

    async with server(handler) as (endpoint, requests, _):
        cfg["values"] = {**cfg["values"], "endpoint": endpoint, "deployment_id": "review-local"}
        prepared = prepare_embedding_request(
            profile(cfg["values"]),
            [EmbeddingInput("query-facet", text="private evidence")],
            request_id="query-request",
            transport_attempt_id="query-attempt",
            purpose="query",
        )

        async def prepare(*args, **kwargs):
            entered.set()
            await release.wait()
            return {"prepared": prepared}

        async def permit(*args, **kwargs):
            permits.append("network-permit")

        async def complete(lease, request, result, **kwargs):
            observations.append(result.status)
            return {"status": result.status}

        runtime.index = SimpleNamespace(prepare=prepare, check_permit=permit, complete=complete)
        work = asyncio.create_task(
            runtime.embed(
                "host",
                cfg,
                ["query-facet"],
                request_id="query-request",
                conversation_id="private-conversation",
                input_cut=1,
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            async with asyncio.timeout(1):
                await runtime.close()
            release.set()
            try:
                result = await asyncio.wait_for(work, 2)
                assert result["status"] in {"NOT_SENT", "CANCELLED"}
            except EmbeddingCancelled as exc:
                assert exc.result.status == "NOT_SENT"
            except asyncio.CancelledError:
                # A runtime that owns the whole query task can cancel it while
                # preparation is still pending, before a transport exists.
                pass
            except ForestError as exc:
                assert exc.code == "MEMORY_RUNTIME_CLOSED"
            assert not permits and not requests
            assert observations in ([], ["NOT_SENT"])
            assert not runtime.clients
        finally:
            release.set()
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            await runtime.close()

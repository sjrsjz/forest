"""Native Worker ingress and supervised memory-source recovery on real PostgreSQL."""

import asyncio

import pytest
from test_platform import platform, receive, wait_for  # noqa: F401
from test_web_authority import environment  # noqa: F401

from forest.embeddings import EmbeddingClient
from forest.models import ChatCompletionsClient

pytestmark = pytest.mark.postgres


async def test_runtime_silent_drain_shares_group_sources_with_independent_cuts(
    platform,  # noqa: F811
    monkeypatch,
):
    p = platform
    provider_calls = []

    async def unexpected_provider(*args, **kwargs):
        provider_calls.append((args, kwargs))
        raise AssertionError("Memory source registration must not call a model or embedder")

    monkeypatch.setattr(ChatCompletionsClient, "dispatch", unexpected_provider)
    monkeypatch.setattr(EmbeddingClient, "dispatch", unexpected_provider)
    await p.runtime._replace(p.desired)
    await receive(p, 1, group_id=30001, message="A-first")
    await receive(
        p,
        2,
        group_id=30002,
        user_id=20002,
        sender={"nickname": "Another author"},
        message="B-first",
    )
    await receive(p, 3, group_id=30001, message="A-second")

    async def conversations():
        async with p.env.store.pool.connection() as conn:
            rows = await (
                await conn.execute("SELECT * FROM public.bot_conversations ORDER BY channel_id")
            ).fetchall()
            return {row["channel_id"]: row for row in rows}

    before = await conversations()
    assert len(before) == 2 and all(row["context_applied_seq"] == 0 for row in before.values())
    source_store = p.runtime.module("memory.graph").sources
    unread = await source_store.read(
        p.kernel._authority(),
        before["30001"]["conversation_id"],
        input_cut=0,
    )
    assert unread["sources"] == []

    # Start the actual scheduler: no manual select_input/accept_batch calls.
    await p.runtime.start()

    async def drained():
        rows = await conversations()
        return (
            rows
            if {key: row["context_applied_seq"] for key, row in rows.items()}
            == {
                "30001": 2,
                "30002": 1,
            }
            else None
        )

    after = await wait_for(drained)

    async def read(group, cut):
        result = await source_store.read(
            p.kernel._authority(),
            after[group]["conversation_id"],
            input_cut=cut,
        )
        return {row["parts"][0]["text"]: row for row in result["sources"]}

    a_cut_one = await read("30001", 1)
    assert set(a_cut_one) == {"A-first", "B-first"}
    assert a_cut_one["B-first"]["source"]["channel_id"] == "30002"
    assert a_cut_one["B-first"]["author"]["external_user_id"] == "20002"
    assert (
        a_cut_one["A-first"]["author"]["principal_ref"]
        != a_cut_one["B-first"]["author"]["principal_ref"]
    )
    assert set(await read("30002", 0)) == {"A-first", "A-second"}
    assert set(await read("30001", 2)) == {"A-first", "A-second", "B-first"}
    async with p.env.store.pool.connection() as conn:
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_conversation_runs) AS runs, "
                "(SELECT count(*) FROM public.bot_send_intents) AS sends, "
                "(SELECT count(*) FROM public.bot_memory_source_eligibility) AS eligible"
            )
        ).fetchone()
    assert counts == {"runs": 0, "sends": 0, "eligible": 3}
    assert not provider_calls
    assert not any("send_group_msg" in request[0] for request in p.requests)
    assert p.runtime.failure is None


async def test_runtime_owns_bounded_backfill_and_reclaims_its_supervised_task(platform):  # noqa: F811
    p = platform
    await p.runtime._replace(p.desired)
    connection = p.runtime.connection(p.binding.binding_id)
    await p.runtime.start()
    tasks = list(p.runtime._tasks)
    memory_tasks = [task for task in tasks if task.get_name() == "memory-source-backfill"]
    assert len(memory_tasks) == 1
    await p.runtime.start()
    assert p.runtime._tasks == tasks

    async def scanned():
        async with p.env.store.pool.connection() as conn:
            rows = await (
                await conn.execute("SELECT * FROM public.bot_memory_backfill_heads ORDER BY kind")
            ).fetchall()
        return rows if len(rows) == 2 and all(row["completed"] for row in rows) else None

    heads = await wait_for(scanned)
    assert [row["kind"] for row in heads] == ["INPUT", "REPLY"]
    assert all(row["bot_id"] == p.env.store.bot_id for row in heads)
    memory = p.runtime.module("memory.graph")
    assert memory_tasks == [memory.backfill_task]
    assert not memory_tasks[0].done() and "memory_sources" not in memory.errors
    async with asyncio.timeout(10):
        await p.runtime.close()
    assert all(task.done() for task in tasks)
    assert not p.runtime._tasks and not p.runtime._clients and not p.runtime.connections
    assert connection.client._client.is_closed
    assert p.runtime.failure is None
    assert not hasattr(p.runtime, "memory_sources")

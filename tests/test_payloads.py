"""Actual PostgreSQL deletion/content admission races and retained identities."""

import asyncio
import hashlib
import multiprocessing
import sys
from dataclasses import replace

import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb
from test_bot_store import database as database
from test_bot_store import store as store

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.bot import BotStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def payloads(store):
    lease = await store.acquire_host("host", "incarnation", ttl_seconds=120)
    yield PayloadStore(store), lease


async def put(payloads, owner, value, *, sources=(), parents=(), epoch=0):
    repo, lease = payloads
    return await repo.put_json(lease, owner_kind="memory.source", owner_id=owner, value=value,
                               sources=sources, parents=parents, expected_deletion_epoch=epoch)


async def fails(code, awaitable):
    with pytest.raises(ForestError) as exc:
        await awaitable
    assert exc.value.code == code


async def test_removal_keeps_identity_and_erases_entire_transitive_body_closure(payloads):
    repo, lease = payloads
    original = await put(payloads, "source-a", {"text": "CANARY-A-unique"}, sources=(SourceRef("a"),))
    mixed = await put(payloads, "mixed", {"answer": "CANARY-A-and-B"},
                      sources=(SourceRef("b"),), parents=(original.payload_id,))
    child = await put(payloads, "child", {"analysis": "CANARY-A-derived"}, parents=(mixed.payload_id,))
    survivor = await put(payloads, "source-b", {"text": "B"}, sources=(SourceRef("b"),))
    result = await repo.forget_sources(lease, "forget-a", (SourceRef("a"),), expected_deletion_epoch=0)
    assert result["deletion_epoch"] == 1
    for ref in (original, mixed, child):
        await fails("PAYLOAD_ERASED", repo.read_json(lease, ref.payload_id, expected_deletion_epoch=1))
    assert await repo.read_json(lease, survivor.payload_id, expected_deletion_epoch=1) == {"text": "B"}
    assert (await repo.scrub(lease, limit=2))["removed"] == 2
    assert (await repo.scrub(lease, limit=2))["removed"] == 1
    assert (await repo.scrub(lease))["removed"] == 0
    repeated = await put(payloads, "source-a", {"text": "CANARY-A-unique"}, sources=(SourceRef("a"),))
    assert repeated.state == "SCRUBBED" and repeated.digest == original.digest
    assert repeated.payload_id == original.payload_id
    await fails("PAYLOAD_CONFLICT", put(payloads, "source-a", {"text": "replacement"}))
    assert result == await repo.forget_sources(lease, "forget-a", (SourceRef("a"),), expected_deletion_epoch=0)
    async with repo.store.pool.connection() as conn:
        bodies = await (await conn.execute("SELECT content FROM public.bot_payload_bodies")).fetchall()
        heads = await (await conn.execute("SELECT state FROM public.bot_payload_headers")).fetchall()
    assert len(bodies) == 1 and b"CANARY" not in bytes(bodies[0]["content"])
    assert sorted(h["state"] for h in heads) == ["ACTIVE", "SCRUBBED", "SCRUBBED", "SCRUBBED"]


async def test_partial_image_removal_retains_independent_text_and_other_group_bytes(payloads):
    repo, lease = payloads
    raw = b"\x89PNG\r\nunique-pixel-canary"
    image = await repo.put_bytes(lease, owner_kind="media.capture", owner_id="a-image", value=raw,
                                 sources=(SourceRef("a", part_id="part:1"),), expected_deletion_epoch=0)
    other = await repo.put_bytes(lease, owner_kind="media.capture", owner_id="b-image", value=raw,
                                 sources=(SourceRef("b", part_id="part:1"),), expected_deletion_epoch=0)
    text = await put(payloads, "a-text", "still visible", sources=(SourceRef("a", part_id="part:0"),))
    whole = await put(payloads, "a-whole", {"parts": ["text", "image"]}, sources=(SourceRef("a"),))
    await repo.forget_sources(lease, "forget-image", (SourceRef("a", part_id="part:1"),), expected_deletion_epoch=0)
    await repo.scrub(lease)
    for ref in (image, whole):
        await fails("PAYLOAD_ERASED", repo.read_json(lease, ref.payload_id, expected_deletion_epoch=1))
    assert await repo.read_json(lease, text.payload_id, expected_deletion_epoch=1) == "still visible"
    async with repo.store.pool.connection() as conn, conn.transaction():
        assert await repo.read_bytes_in(conn, other.payload_id, expected_deletion_epoch=1) == raw
    # The source identity, not a temporary revision/epoch, is suppressed forever.
    await fails("SOURCE_SUPPRESSED", put(payloads, "new-image", "new", epoch=1,
                                         sources=(SourceRef("a", 99, "part:1"),)))


async def test_delete_before_source_registration_and_forged_current_epoch_do_not_revive(payloads):
    repo, lease = payloads
    await repo.forget_sources(lease, "preempt", (SourceRef("future"),), expected_deletion_epoch=0)
    await fails("DELETION_CONFLICT", put(payloads, "late", "old", sources=(SourceRef("future"),)))
    await fails("SOURCE_SUPPRESSED", put(payloads, "changed-id", "old", epoch=1,
                                         sources=(SourceRef("future", 500),)))
    # Unknown source identifiers are allowed here only for trusted migration;
    # source eligibility/authorization belongs to MemorySourceStore.
    good = await put(payloads, "other", "ok", epoch=1, sources=(SourceRef("other"),))
    assert good.state == "ACTIVE"


async def test_concurrent_duplicate_writes_and_conflicts_share_single_body(payloads):
    repo, lease = payloads
    results = await asyncio.gather(*(put(payloads, "owner", {"v": 7}, sources=(SourceRef("s"),))
                                     for _ in range(8)))
    assert len({r.payload_id for r in results}) == 1
    outcomes = await asyncio.gather(*(put(payloads, "racer", value) for value in range(8)),
                                    return_exceptions=True)
    assert sum(not isinstance(r, Exception) for r in outcomes) == 1
    assert all(not isinstance(r, Exception) or r.code == "PAYLOAD_CONFLICT" for r in outcomes)
    async with repo.store.pool.connection() as conn:
        row = await (await conn.execute("SELECT count(*) AS n FROM public.bot_payload_bodies")).fetchone()
    assert row["n"] == 2


async def test_competing_forget_cas_and_stale_host_have_no_partial_writes(payloads):
    repo, lease = payloads
    await put(payloads, "original", "a", sources=(SourceRef("a"),))
    outcomes = await asyncio.gather(*(
        repo.forget_sources(lease, f"forget-{index}", (SourceRef("a"),), expected_deletion_epoch=0)
        for index in range(2)), return_exceptions=True)
    assert sum(isinstance(r, dict) for r in outcomes) == 1
    assert [r.code for r in outcomes if isinstance(r, ForestError)] == ["DELETION_CONFLICT"]
    stale = replace(lease, incarnation="old-host")
    await fails("STALE_HOST", repo.scrub(stale))
    async with repo.store.pool.connection() as conn:
        row = await (await conn.execute("SELECT count(*) AS n FROM public.bot_payload_bodies")).fetchone()
    assert row["n"] == 1


async def test_delete_vs_late_content_waits_for_actual_database_clock(payloads):
    repo, lease = payloads
    # Separate pooled connections; late publication is already waiting on the
    # database lock when suppression commits, not merely called afterwards.
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.lock_in(conn)
        pending = asyncio.create_task(put(payloads, "late-publication", "canary", sources=(SourceRef("s"),)))
        try:
            for _ in range(100):
                await conn.execute("SELECT pg_stat_clear_snapshot()")
                row = await (await conn.execute(
                    "SELECT count(*) AS n FROM pg_stat_activity WHERE datname=current_database() "
                    "AND pid<>pg_backend_pid() AND wait_event_type='Lock'"
                )).fetchone()
                if row["n"]:
                    break
                await asyncio.sleep(0.01)
            assert row["n"] > 0
            # Use another transaction to commit suppression after releasing
            # this artificial barrier; race winner may legally be either side.
        except BaseException:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            raise
    forgotten = asyncio.create_task(repo.forget_sources(lease, "delete-racing", (SourceRef("s"),),
                                                         expected_deletion_epoch=0))
    result, _ = await asyncio.gather(pending, forgotten, return_exceptions=True)
    assert isinstance(result, ForestError) or result.state == "ACTIVE"
    if isinstance(result, ForestError):
        assert result.code == "DELETION_CONFLICT"
    await repo.scrub(lease)
    async with repo.store.pool.connection() as conn:
        row = await (await conn.execute("SELECT count(*) AS n FROM public.bot_payload_bodies")).fetchone()
    assert row["n"] == 0


async def test_database_rejects_body_replacement_unrequested_removal_and_resurrection(payloads):
    repo, lease = payloads
    ref = await put(payloads, "fixed", "canary", sources=(SourceRef("s"),))
    async with repo.store.pool.connection() as conn:
        for statement in (
            "UPDATE public.bot_payload_bodies SET content='replacement'::bytea",
            "DELETE FROM public.bot_payload_bodies",
            "UPDATE public.bot_payload_headers SET state='SCRUBBED'",
            "DELETE FROM public.bot_payload_headers",
            "UPDATE public.bot_payload_sources SET source_unit_id='other'",
        ):
            with pytest.raises(psycopg.Error):
                await conn.execute(statement)
    await repo.forget_sources(lease, "forget", (SourceRef("s"),), expected_deletion_epoch=0)
    await repo.scrub(lease)
    async with repo.store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute("INSERT INTO public.bot_payload_bodies(bot_id,payload_id,content) VALUES (%s,%s,%s)",
                               (repo.store.bot_id, ref.payload_id, b'"canary"'))
        with pytest.raises(psycopg.Error):
            await conn.execute("UPDATE public.bot_payload_headers SET state='ACTIVE',forget_operation_id=NULL")


async def test_database_rejects_parent_lineage_omission_and_incomplete_manifests(payloads):
    repo, lease = payloads
    parent = await put(payloads, "parent", "canary", sources=(SourceRef("a"),))
    async with repo.store.pool.connection() as conn:
        for manifest in ({}, {"sources": None, "parents": []}, {"sources": [], "parents": None}):
            with pytest.raises(psycopg.Error):
                await conn.execute(
                    "INSERT INTO public.bot_payload_headers(bot_id,payload_id,owner_kind,owner_id,encoding,"
                    "digest,byte_length,input_digest,manifest,accepted_deletion_epoch) "
                    "VALUES ('bot-a','invalid','memory.source','invalid','json','digest',1,'digest',%s,0)",
                    (Jsonb(manifest),),
                )
        with pytest.raises(psycopg.Error):
            async with conn.transaction():
                content = b'"copied-with-no-sources"'
                await conn.execute(
                    "INSERT INTO public.bot_payload_headers(bot_id,payload_id,owner_kind,owner_id,encoding,"
                    "digest,byte_length,input_digest,manifest,accepted_deletion_epoch) "
                    "VALUES ('bot-a','bypass','memory.source','bypass','json',%s,%s,'test',%s,0)",
                    ("sha256:" + hashlib.sha256(content).hexdigest(), len(content),
                     Jsonb({"sources": [], "parents": [parent.payload_id]})),
                )
                await conn.execute(
                    "INSERT INTO public.bot_payload_derivations(bot_id,payload_id,parent_payload_id) "
                    "VALUES ('bot-a','bypass',%s)", (parent.payload_id,),
                )
                # Counts, content hash and parent ACTIVE state all match. The
                # database must still reject omission of the parent's source A.
                await conn.execute("INSERT INTO public.bot_payload_bodies(bot_id,payload_id,content) "
                                   "VALUES ('bot-a','bypass',%s)", (content,))


async def test_payload_ids_and_lease_cannot_cross_bot_authorities(payloads, database):
    repo, lease = payloads
    ref = await put(payloads, "same-owner", "private", sources=(SourceRef("s"),))
    wrong = PayloadStore(BotStore(database, "bot-b"))
    await wrong.store.open()
    try:
        await fails("BOT_SCOPE_MISMATCH", wrong.read_json(lease, ref.payload_id, expected_deletion_epoch=0))
    finally:
        await wrong.store.close()


async def test_atomic_failure_leaves_no_header_or_lineage(payloads):
    repo, lease = payloads
    with pytest.raises(RuntimeError):
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._host(conn, lease)
            await repo.put_json_in(conn, owner_kind="memory.source", owner_id="rolled-back", value="canary",
                                   sources=(SourceRef("s"),), expected_deletion_epoch=0)
            raise RuntimeError("transaction fails after body admission")
    async with repo.store.pool.connection() as conn:
        for table in ("bot_payload_headers", "bot_payload_sources", "bot_payload_bodies"):
            row = await (await conn.execute("SELECT count(*) AS n FROM public." + table)).fetchone()
            assert row["n"] == 0


async def test_invalid_json_and_encoding_fail_without_side_effects(payloads):
    repo, lease = payloads
    await fails("INVALID_PAYLOAD", put(payloads, "bad", {"number": float("nan")}))
    await fails("INVALID_PAYLOAD", put(payloads, "bad", {"not-json": object()}))
    await fails("INVALID_PAYLOAD_OWNER", repo.put_json(lease, owner_kind="user-table", owner_id="bad",
                                                       value={}, expected_deletion_epoch=0))
    await fails("PAYLOAD_TOO_LARGE", repo.put_bytes(lease, owner_kind="media.capture", owner_id="bad",
                                                   value=b"x" * (32 * 1024 * 1024 + 1),
                                                   expected_deletion_epoch=0))
    ref = await repo.put_bytes(lease, owner_kind="media.capture", owner_id="bytes", value=b"{}",
                               expected_deletion_epoch=0)
    await fails("PAYLOAD_ENCODING_MISMATCH", repo.read_json(lease, ref.payload_id, expected_deletion_epoch=0))


async def test_host_expiring_while_waiting_cannot_publish_a_body(store):
    repo = PayloadStore(store)
    lease = await store.acquire_host("brief", "brief-incarnation", ttl_seconds=0.5)
    async with store.pool.connection() as conn, conn.transaction():
        await repo.lock_in(conn)
        pending = asyncio.create_task(put((repo, lease), "expires", "canary"))
        try:
            for _ in range(100):
                await conn.execute("SELECT pg_stat_clear_snapshot()")
                waiting = await (await conn.execute(
                    "SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                    "AND pid<>pg_backend_pid() AND wait_event_type='Lock'"
                )).fetchone()
                if waiting:
                    break
                await asyncio.sleep(0.01)
            assert waiting
            await asyncio.sleep(0.6)
        except BaseException:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            raise
    await fails("STALE_HOST", pending)
    async with store.pool.connection() as conn:
        row = await (await conn.execute("SELECT count(*) AS n FROM public.bot_payload_headers")).fetchone()
    assert row["n"] == 0


def _payload_process(dsn, lease, barrier, output, value):
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    async def run():
        instance = BotStore(dsn, lease.bot_id)
        await instance.open()
        try:
            await asyncio.to_thread(barrier.wait, 10)
            ref = await put((PayloadStore(instance), lease), "process-owner", value, sources=(SourceRef("s"),))
            output.put(("ok", ref.digest))
        except ForestError as exc:
            output.put(("rejected", exc.code))
        finally:
            await instance.close()
    asyncio.run(run())


async def test_different_processes_cannot_choose_two_payloads_for_one_owner(payloads, database):
    repo, lease = payloads
    context = multiprocessing.get_context("spawn")
    barrier, output = context.Barrier(2), context.Queue()
    workers = [context.Process(target=_payload_process, args=(database, lease, barrier, output, value))
               for value in ("left-content", "right-content")]
    try:
        for worker in workers:
            worker.start()
        results = await asyncio.gather(*(asyncio.to_thread(output.get, True, 20) for _ in workers))
        assert sum(result[0] == "ok" for result in results) == 1
        assert ("rejected", "PAYLOAD_CONFLICT") in results
        for worker in workers:
            await asyncio.to_thread(worker.join, 10)
            assert worker.exitcode == 0
        async with repo.store.pool.connection() as conn:
            row = await (await conn.execute("SELECT count(*) AS n FROM public.bot_payload_bodies")).fetchone()
        assert row["n"] == 1
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            await asyncio.to_thread(worker.join, 10)
        output.close()
        output.join_thread()

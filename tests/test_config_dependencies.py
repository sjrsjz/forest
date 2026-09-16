"""Real PostgreSQL: immutable old copies gain precise deletion prerequisites."""

import json

import psycopg
import pytest
import pytest_asyncio
from test_bot_store import database as database
from test_config_writer_fence import resources

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage import bot as bot_module
from forest.storage.bot import BotStore
from forest.storage.data_migrations.config_dependencies_v2 import import_dependency_in
from forest.storage.migration_v1 import _migration_context
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def content(database, tmp_path, monkeypatch):
    path = resources(tmp_path / "schema", cutoff="044")
    monkeypatch.setattr(bot_module, "files", lambda _: path)
    store = BotStore(database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("host", "dependencies", ttl_seconds=300)
        yield PayloadStore(store), lease
    finally:
        await store.close()


async def put(content, owner, *, sources=(), parents=(), epoch=0):
    return await content[0].put_json(content[1], owner_kind="model.snapshot", owner_id=owner,
        value={"owner": owner, "text": "frozen config text"}, sources=sources, parents=parents,
        expected_deletion_epoch=epoch)


async def bridge(content, child, parent, proof="run-old"):
    payloads, lease = content
    async with payloads.store.pool.connection() as conn, conn.transaction():
        await payloads.store._host(conn, lease)
        with _migration_context(conn, payloads.store, "044_config_dependencies.sql"):
            await import_dependency_in(conn, payloads.store, child.payload_id, parent.payload_id,
                proof_kind="agent.spec", proof_id=proof)


async def rows(content, statement, params=()):
    async with content[0].store.pool.connection() as conn:
        return await (await conn.execute(statement, params)).fetchall()


async def available(content, ref, epoch=0):
    return await content[0].read_json(content[1], ref.payload_id, expected_deletion_epoch=epoch)


async def denied(content, ref, epoch):
    with pytest.raises(ForestError) as exc:
        await available(content, ref, epoch)
    assert exc.value.code in {"PAYLOAD_ERASED", "SOURCE_SUPPRESSED"}


async def test_old_and_future_copies_follow_config_without_revoking_original_chat(content):
    chat = await put(content, "chat", sources=(SourceRef("chat-source"),))
    cfg = await put(content, "config", sources=(SourceRef("configuration-source"),))
    old = await put(content, "old-snapshot", parents=(chat.payload_id,))
    old_output = await put(content, "old-result", parents=(old.payload_id,))
    before = await rows(content, "SELECT payload_id,input_digest,manifest,digest FROM public.bot_payload_headers ORDER BY payload_id")
    await bridge(content, old, cfg)
    assert before == await rows(content, "SELECT payload_id,input_digest,manifest,digest FROM public.bot_payload_headers ORDER BY payload_id")
    future = await put(content, "future", parents=(old_output.payload_id,))
    sources = await rows(content, "SELECT source_unit_id FROM public.bot_payload_sources WHERE payload_id=%s", (future.payload_id,))
    assert {r["source_unit_id"] for r in sources} == {"chat-source", "configuration-source"}
    forgotten = await content[0].forget_sources(content[1], "forget-config", (SourceRef("configuration-source"),), expected_deletion_epoch=0)
    assert not forgotten["legacy_conservative"]
    for ref in (cfg, old, old_output, future):
        await denied(content, ref, 1)
    assert (await available(content, chat, 1))["owner"] == "chat"
    result = await content[0].scrub(content[1])
    assert result["removed"] == 4 and result["remaining"] == 0
    assert (await available(content, chat, 1))["owner"] == "chat"


async def test_deleted_before_proof_erases_existing_copies_without_changing_receipt(content):
    cfg = await put(content, "configuration", sources=(SourceRef("cfg"),))
    old = await put(content, "old")
    child = await put(content, "child", parents=(old.payload_id,))
    await content[0].forget_sources(content[1], "delete-first", (SourceRef("cfg"),), expected_deletion_epoch=0)
    before = await rows(content, "SELECT * FROM public.bot_payload_forget_requests")
    await bridge(content, old, cfg)
    assert before == await rows(content, "SELECT * FROM public.bot_payload_forget_requests")
    for ref in (old, child):
        await denied(content, ref, 1)
    assert (await content[0].scrub(content[1]))["removed"] == 3
    with pytest.raises(ForestError):
        await put(content, "new-copy", parents=(old.payload_id,), epoch=1)


async def test_later_prerequisite_updates_earlier_overlay_and_actual_descendants(content):
    cfg = await put(content, "configuration", sources=(SourceRef("cfg"),))
    spec = await put(content, "old-spec")
    old = await put(content, "old-snapshot")
    await bridge(content, old, spec)
    future = await put(content, "new-snapshot", parents=(old.payload_id,))
    await bridge(content, spec, cfg)
    await content[0].forget_sources(content[1], "forget", (SourceRef("cfg"),), expected_deletion_epoch=0)
    for ref in (spec, old, future):
        await denied(content, ref, 1)


async def test_cycles_conflicting_proofs_and_runtime_imports_are_rejected(content):
    first = await put(content, "first")
    second = await put(content, "second", parents=(first.payload_id,))
    with pytest.raises(ForestError, match="cycle"):
        await bridge(content, first, second)
    cfg = await put(content, "config")
    await bridge(content, first, cfg)
    await bridge(content, first, cfg)
    with pytest.raises(ForestError) as exc:
        await bridge(content, first, cfg, "different")
    assert exc.value.code == "LEGACY_DEPENDENCY_CONFLICT"
    async with content[0].store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as exc:
            await import_dependency_in(conn, content[0].store, second.payload_id, cfg.payload_id,
                proof_kind="agent.spec", proof_id="runtime")
        assert exc.value.code == "MIGRATION_REQUIRED"
    async with content[0].store.pool.connection() as conn, conn.transaction():
        with pytest.raises(psycopg.Error) as exc:
            await conn.execute("INSERT INTO public.bot_payload_legacy_dependencies VALUES (%s,%s,%s,'agent.spec','forged')",
                (content[0].store.bot_id, second.payload_id, cfg.payload_id))
        assert exc.value.sqlstate == "55000"


async def denied_read(content, ref, epoch):
    with pytest.raises(ForestError) as caught:
        await available(content, ref, epoch)
    assert caught.value.code in {"PAYLOAD_ERASED", "SOURCE_SUPPRESSED"}


async def test_active_receipt_with_incomplete_proven_parent_does_not_authorize_body_read(content):
    p, lease = content
    parent = await put(content, "parent", sources=(SourceRef("parent-source"),))
    child = await put(content, "child", parents=(parent.payload_id,))
    value = {"missing": True}
    manifest = {"sources": [], "parents": []}
    owner, digest = "incomplete-prerequisite", digest_json(value)
    payload_id = "payload:" + digest_json([p.store.bot_id, "model.snapshot", owner])
    async with p.store.pool.connection() as conn, conn.transaction():
        await p.store._host(conn, lease)
        await p.lock_in(conn)
        await conn.execute("INSERT INTO public.bot_payload_headers(bot_id,payload_id,owner_kind,owner_id,encoding,digest,"
            "byte_length,input_digest,manifest,accepted_deletion_epoch) VALUES (%s,%s,'model.snapshot',%s,'json',%s,%s,%s,%s::jsonb,0)",
            (p.store.bot_id, payload_id, owner, digest, len(json.dumps(value, separators=(",", ":")).encode()),
             digest_json(["model.snapshot", owner, "json", digest, manifest]), json.dumps(manifest)))
    from types import SimpleNamespace

    await bridge(content, parent, SimpleNamespace(payload_id=payload_id))
    receipt = await put(content, "child", parents=(parent.payload_id,))
    assert receipt == child and receipt.state == "ACTIVE"
    with pytest.raises(ForestError) as denied:
        await available(content, child)
    assert denied.value.code == "PAYLOAD_ERASED"
    with pytest.raises(ForestError) as denied:
        await put(content, "new-body", parents=(parent.payload_id,))
    assert denied.value.code == "PAYLOAD_ERASED"

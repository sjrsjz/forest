"""Metadata candidate authorization never loads source text or image payload bytes."""

import pytest
from test_memory_sources import drain, rows, source_read
from test_messages import event, receive
from test_messages import messages as messages

from forest.memory_types import SourceRef
from forest.storage.delivery import DeliveryStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


def forbid_content_reads(monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("Metadata authorization attempted to load payload content")

    monkeypatch.setattr(PayloadStore, "read_json_in", forbidden)
    monkeypatch.setattr(PayloadStore, "read_bytes_in", forbidden)


async def global_metadata(sources, lease, **kwargs):
    async with sources.store.pool.connection() as conn, conn.transaction():
        await sources.store._host(conn, lease)
        result = await sources.authorize_sources_in(conn, include_payload=False, **kwargs)
        await sources.store._host(conn, lease)
        return result["sources"]


async def test_large_source_metadata_keeps_verified_author_group_scope_without_reading_bodies(
    messages, monkeypatch
):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    left = await receive(repository, lease, 1, user="alice", text="a" * 98304)
    right = await receive(repository, lease, 2, user="bob", group="other", text="b" * 98304)
    private = await repository.ingest(
        lease,
        "binding-a",
        {**event(3, group="private-user", user="carol"), "channel_kind": "private"},
        decision="NO_TRIGGER",
    )
    for receipt in (left, right, private):
        await drain(repository, lease, receipt)
    # A repeated source in another thread is still unaccepted at that thread's cut.
    tail = await repository.ingest(
        lease,
        "binding-a",
        {**event(1, user="alice", text="a" * 98304), "thread_id": "unread"},
        decision="NO_TRIGGER",
    )
    expected_authors = {
        row["principal_ref"]
        for row in await rows(
            repository,
            "SELECT u.principal_ref FROM public.bot_memory_source_units u JOIN public.bot_memory_sources s USING(bot_id,source_id) WHERE s.source_kind='GROUP'",
        )
    }
    headers = await rows(
        repository,
        "SELECT byte_length FROM public.bot_payload_headers WHERE owner_kind='memory.source'",
    )
    assert sum(row["byte_length"] >= 98304 for row in headers) == 2
    forbid_content_reads(monkeypatch)
    readable = await source_read(sources, lease, left, 1, include_payload=False)
    assert {row["principal_ref"] for row in readable} == expected_authors
    assert {row["identity_origin"] for row in readable} == {"adapter_verified"}
    assert {row["source"]["channel_id"] for row in readable} == {"group-1", "other"}
    assert all(
        row["source"]["realm_id"] == "qq-global" and row["source"]["identity_schema"] == 1
        for row in readable
    )
    assert all("author" not in row and "parts" not in row and row["payload_id"] for row in readable)
    assert {
        row["source"]["channel_id"]
        for row in await source_read(sources, lease, tail, 0, include_payload=False)
    } == {"other"}
    assert {row["source_kind"] for row in await global_metadata(sources, lease)} == {
        "GROUP",
        "DIRECT",
    }


async def test_header_only_source_and_its_complete_child_are_excluded_before_limit(
    messages, monkeypatch
):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    healthy = await receive(repository, lease, 1)
    actor, _ = await drain(repository, lease, healthy)
    original_put = PayloadStore._put_in

    class HeaderOnlyConnection:
        """Fault fixture: persist the admission prefix, omit only its body INSERT.

        All real PostgreSQL FKs and immutable triggers remain enabled. This
        represents an incomplete imported payload, not a normal atomic crash.
        """

        def __init__(self, connection):
            self.connection = connection

        async def execute(self, statement, parameters=None):
            if str(statement).startswith("INSERT INTO public.bot_payload_bodies"):
                return None
            return await self.connection.execute(statement, parameters)

    async def missing_body(self, conn, **kwargs):
        assert kwargs["owner_kind"] == "memory.source"
        return await original_put(self, HeaderOnlyConnection(conn), **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(PayloadStore, "_put_in", missing_body)
        incomplete = await receive(repository, lease, 2)
    parent = (
        await rows(
            repository,
            "SELECT u.* FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) WHERE o.event_id=%s",
            (incomplete["event_id"],),
        )
    )[0]
    assert parent["lineage_complete"]
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
        (parent["payload_id"],),
    )
    assert (
        await rows(
            repository,
            "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
            (parent["payload_id"],),
        )
    )[0]["state"] == "ACTIVE"

    original_register = MemorySourceStore._register_in

    async def dependent(self, conn, conversation, **kwargs):
        return await original_register(
            self,
            conn,
            conversation,
            **{**kwargs, "dependencies": (SourceRef(parent["source_unit_id"]),)},
        )

    with monkeypatch.context() as patch:
        patch.setattr(MemorySourceStore, "_register_in", dependent)
        child = await receive(repository, lease, 3)
    await repository.select_input(actor, "drain-incomplete-import")
    child_unit = (
        await rows(
            repository,
            "SELECT u.* FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) WHERE o.event_id=%s",
            (child["event_id"],),
        )
    )[0]
    assert child_unit["lineage_complete"]
    assert await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
        (child_unit["payload_id"],),
    )
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_source_eligibility")) == 3
    forbid_content_reads(monkeypatch)
    # The two newest candidates have missing content in their dependency closure.
    # LIMIT 1 must still return the older complete source without reading its text.
    result = await source_read(sources, lease, healthy, 3, include_payload=False, limit=1)
    assert len(result) == 1 and result[0]["source_order_seq"] == 1
    assert {row["source_order_seq"] for row in await global_metadata(sources, lease)} == {1}


async def test_metadata_still_excludes_deleted_and_incomplete_reply_sources(messages, monkeypatch):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    first = await receive(repository, lease, 1, trigger=True)
    actor, selection = await drain(repository, lease, first)
    await repository.finalize_reply(
        actor,
        "final",
        run_id=selection["run_id"],
        expected_input_revision=1,
        reply_parts=[
            [{"type": "text", "text": "An official reply retains incomplete legacy lineage"}]
        ],
        delivery=DeliveryStore(repository.store),
    )
    second = await receive(repository, lease, 2)
    await repository.select_input(actor, "drain-second")
    unit = (
        await rows(
            repository,
            "SELECT u.* FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) WHERE o.event_id=%s",
            (second["event_id"],),
        )
    )[0]
    await sources.payloads.forget_sources(
        lease, "erase-source", [SourceRef(unit["source_unit_id"])], expected_deletion_epoch=0
    )
    await sources.payloads.scrub(lease)
    forbid_content_reads(monkeypatch)
    result = await source_read(sources, lease, first, 2, include_payload=False)
    assert len(result) == 1 and result[0]["kind"] == "INPUT"
    assert result[0]["source_unit_id"] != unit["source_unit_id"]
    assert len(await global_metadata(sources, lease)) == 1

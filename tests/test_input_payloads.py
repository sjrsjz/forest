"""Actual input/capture payload migration, physical erasure and late-writer fences."""

import asyncio
import hashlib
import json
from importlib.resources import files

import psycopg
import pytest
from legacy_022 import FrozenPayloadStore
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_media import configure, context, media_server, picture, respond
from test_messages import event, owner, receive
from test_messages import messages as messages
from test_p3_upgrade import p2_authority
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.input_payloads import InputContentStore
from forest.storage.media import MediaStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


async def rows(repository, statement, parameters=()):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute(statement, parameters)).fetchall()


async def input_read(repository, lease, event_id, *, required=False):
    contents = InputContentStore(repository.store)
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        clock = await contents.payloads.lock_in(conn)
        result = await contents.read_input_in(
            conn, event_id, expected_deletion_epoch=clock["deletion_epoch"], required=required
        )
        await repository.store._host(conn, lease)
        return result


async def erase(repository, lease, receipt):
    contents = InputContentStore(repository.store)
    stored = (
        await rows(
            repository,
            "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
            (receipt["event_id"],),
        )
    )[0]
    await contents.payloads.forget_sources(
        lease, "forget", [contents.source_ref(stored["source_event_id"])], expected_deletion_epoch=0
    )
    while (await contents.payloads.scrub(lease))["remaining"]:
        pass


async def test_author_and_segments_are_payload_only_and_silent_drain_retains_order(messages):
    repository, lease, _ = messages
    original = event(1, text="PRIVATE-TEXT-CANARY")
    original["author"]["display_name"] = "PRIVATE-NAME-CANARY"
    first = await repository.ingest(lease, "binding-a", original, decision="NO_TRIGGER")
    second = await receive(repository, lease, 2, text="second")
    stored = await rows(
        repository,
        "SELECT event_id,author,segments,payload_id FROM public.bot_message_inbox ORDER BY ingress_seq",
    )
    assert all(
        row["segments"] is None and row["payload_id"] and "display_name" not in row["author"]
        for row in stored
    )
    assert "PRIVATE" not in json.dumps(stored)
    hydrated = await input_read(repository, lease, first["event_id"])
    assert hydrated["payload_state"] == "ACTIVE"
    assert hydrated["segments"] == original["segments"]
    assert hydrated["author"]["display_name"] == "PRIVATE-NAME-CANARY"
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "silent")
    assert batch["kind"] == "SILENT_DRAIN" and batch["end_seq"] == 2
    view = await repository.context(actor)
    assert [unit["event_id"] for unit in view["units"]] == [first["event_id"], second["event_id"]]
    assert "PRIVATE-TEXT-CANARY" in str(view)
    await erase(repository, lease, first)
    erased = await input_read(repository, lease, first["event_id"])
    assert erased["payload_state"] == "UNAVAILABLE"
    assert erased["segments"][0]["type"] == "unavailable"
    assert erased["author"]["principal_ref"] == hydrated["author"]["principal_ref"]
    assert "PRIVATE" not in json.dumps(erased, default=str)
    with pytest.raises(ForestError):
        await input_read(repository, lease, first["event_id"], required=True)
    assert (await repository.ingest(lease, "binding-a", original, decision="NO_TRIGGER"))[
        "duplicate"
    ]
    assert (await repository.select_input(actor, "after-erase"))["status"] == "IDLE"
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
        (b"PRIVATE",),
    )


async def test_real_tcp_capture_uses_payload_bytes_and_manifest_then_erases_every_copy(messages):
    repository, lease, _ = messages
    content = picture()

    async def handler(writer, request):
        await respond(writer, content)

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image?token=PRIVATE-URI")
        store = MediaStore(repository)
        first = await store.materialize(actor, snapshot, allowed_origins=[origin])
        assert next(iter(first.values())).content == content
        assert (
            await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[]) == first
        )
        assert len(requests) == 1
        capture = (await rows(repository, "SELECT * FROM public.bot_media_captures"))[0]
        inbound = (await rows(repository, "SELECT * FROM public.bot_inbound_media_refs"))[0]
        assert capture["content"] is None and capture["source_manifest"] is None
        assert capture["payload_id"] and capture["manifest_payload_id"]
        assert inbound["source"] is None and inbound["source_payload_id"]
        contents = InputContentStore(repository.store)
        async with repository.store.pool.connection() as conn, conn.transaction():
            await repository.store._host(conn, lease)
            saved = await contents.read_capture_in(
                conn, capture["media_ref"], expected_deletion_epoch=0
            )
            assert (
                saved["content"] == content
                and saved["source_manifest"]["author"]["external_user_id"] == "user-1"
            )
            payload_refs = await (
                await conn.execute(
                    "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
                    (capture["payload_id"],),
                )
            ).fetchone()
            assert payload_refs["manifest"]["sources"][0]["part_id"] == "0"
        await erase(repository, lease, {"event_id": capture["event_id"]})
        async with repository.store.pool.connection() as conn, conn.transaction():
            await repository.store._host(conn, lease)
            assert (
                await contents.read_capture_in(
                    conn, capture["media_ref"], expected_deletion_epoch=1
                )
                is None
            )
            with pytest.raises(ForestError):
                await contents.read_media_source_in(
                    conn, capture["media_ref"], expected_deletion_epoch=1
                )
        assert not await rows(
            repository,
            "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
            (b"PRIVATE-URI",),
        )
        assert not await rows(
            repository, "SELECT 1 FROM public.bot_payload_bodies WHERE content=%s", (content,)
        )
        with pytest.raises(ForestError):
            await store.materialize(actor, snapshot, allowed_origins=[origin])
        assert len(requests) == 1


@pytest.mark.parametrize("erase_same_source", [False, True])
async def test_late_original_image_cannot_publish_across_deletion_epoch(
    messages, erase_same_source
):
    repository, lease, _ = messages
    arrived, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, request):
        arrived.set()
        await release.wait()
        await respond(writer, picture())

    async with media_server(handler) as (origin, _):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/slow")
        target = {"event_id": snapshot["units"][0]["unit_id"]}
        if not erase_same_source:
            target = await receive(repository, lease, 2, group="another-group")
        task = asyncio.create_task(
            MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin])
        )
        await asyncio.wait_for(arrived.wait(), 5)
        try:
            await erase(repository, lease, target)
        finally:
            release.set()
        with pytest.raises(ForestError):
            await task
        assert not await rows(repository, "SELECT * FROM public.bot_media_captures")
        assert not await rows(
            repository,
            "SELECT * FROM public.bot_payload_headers WHERE owner_id LIKE %s",
            ("input-capture:%",),
        )


async def test_legacy_plaintext_columns_cannot_be_reintroduced(messages):
    repository, lease, _ = messages
    value = event(1)
    value["segments"] = [
        {
            "type": "image",
            "media_ref": {"kind": "onebot-source", "uri": "https://example.invalid/x"},
        }
    ]
    await repository.ingest(lease, "binding-a", value, decision="NO_TRIGGER")
    for statement in (
        "UPDATE public.bot_message_inbox SET segments='[]'::jsonb",
        'UPDATE public.bot_message_inbox SET author=author || \'{"display_name":"restored"}\'::jsonb',
        "UPDATE public.bot_inbound_media_refs SET source='{}'::jsonb",
        "UPDATE public.bot_message_inbox SET payload_id='changed'",
    ):
        async with repository.store.pool.connection() as conn:
            with pytest.raises(psycopg.Error):
                await conn.execute(statement)


async def test_scrub_reclaims_actual_capture_storage_but_revocation_alone_does_not(
    messages, monkeypatch
):
    repository, lease, _ = messages
    content = picture()
    monkeypatch.setattr("forest.storage.media.MAX_BOT_MEDIA_BYTES", len(content))

    async def handler(writer, request):
        await respond(writer, content)

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        media = MediaStore(repository)
        first_actor, first = await context(repository, lease, origin + "/first", group="first")
        assert len(await media.materialize(first_actor, first, allowed_origins=[origin])) == 1
        source = (
            await rows(
                repository,
                "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
                (first["units"][0]["unit_id"],),
            )
        )[0]
        contents = InputContentStore(repository.store)
        await contents.payloads.forget_sources(
            lease,
            "revoke",
            [contents.source_ref(source["source_event_id"])],
            expected_deletion_epoch=0,
        )
        second_actor, second = await context(
            repository, lease, origin + "/second", number=2, group="second"
        )
        assert await media.materialize(second_actor, second, allowed_origins=[origin]) == {}
        assert (
            await rows(
                repository,
                "SELECT error_code FROM public.bot_inbound_media_refs WHERE error_code IS NOT NULL",
            )
        ) == [{"error_code": "MEDIA_BOT_QUOTA_EXCEEDED"}]
        while (await contents.payloads.scrub(lease))["remaining"]:
            pass
        third_actor, third = await context(
            repository, lease, origin + "/third", number=3, group="third"
        )
        assert len(await media.materialize(third_actor, third, allowed_origins=[origin])) == 1
        assert len(requests) == 3
        assert (
            await rows(repository, "SELECT retained_bytes FROM public.bot_media_quota_heads")
        ) == [{"retained_bytes": len(content)}]


@pytest.mark.parametrize("unknown_dependency", [False, True])
async def test_source_store_complete_reply_inherits_precise_parent_payloads(
    messages, monkeypatch, unknown_dependency
):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "start")
    from forest.storage.delivery import DeliveryStore

    original_register = MemorySourceStore.register_reply_in

    async def register_with_exact_dependencies(self, conn, conversation, reply, **kwargs):
        # Exercise the new explicit-parent registrar contract with the actual
        # reply body parent. Ordinary component replies deliberately do not
        # claim complete execution lineage in MessageStore itself.
        parent = await conn.execute(
            "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s",
            (first["event_id"],),
        )
        parents = [(await parent.fetchone())["payload_id"]]
        if unknown_dependency:
            payload = await self.payloads.put_json_in(
                conn,
                owner_kind="model.snapshot",
                owner_id="untracked-history",
                value={"history": "not registered"},
                sources=(SourceRef("untracked-source"),),
                expected_deletion_epoch=0,
            )
            parents.append(payload.payload_id)
        return await original_register(
            self, conn, conversation, reply, dependency_payload_ids=parents
        )

    monkeypatch.setattr(MemorySourceStore, "register_reply_in", register_with_exact_dependencies)
    reply = await repository.finalize_reply(
        actor,
        "reply",
        run_id=batch["run_id"],
        expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "derived"}]],
        delivery=DeliveryStore(repository.store),
    )
    sources = MemorySourceStore(repository)
    registered = (
        await rows(repository, "SELECT * FROM public.bot_memory_source_units WHERE kind='REPLY'")
    )[0]
    assert registered["lineage_complete"] is not unknown_dependency
    assert registered["payload_id"] != reply["payload_id"]
    await erase(repository, lease, first)
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        with pytest.raises(ForestError):
            await sources.payloads.read_json_in(
                conn, registered["payload_id"], expected_deletion_epoch=1
            )


@pytest.mark.parametrize("previously_suppressed", [False, True])
async def test_upgrade_moves_original_input_uri_and_image_once_without_reviving_suppression(
    upgrade_database, previously_suppressed
):
    await p2_authority(upgrade_database)
    content = picture()
    manifest = {
        "bot_id": "bot-a",
        "unit_id": "event-3",
        "part_id": "0",
        "author": {"display_name": "old private author"},
        "source": {"group_id": "group-a"},
    }
    # Build the released pre-cutover schema; all data below is written through
    # its original bytea/JSON columns, before the new handler is invoked.
    async with (
        await psycopg.AsyncConnection.connect(
            upgrade_database, autocommit=True, row_factory=dict_row
        ) as conn,
        conn.transaction(),
    ):
        for path in sorted(
            files("forest.storage").joinpath("bot_migrations").iterdir(), key=lambda item: item.name
        ):
            if not path.name.endswith(".sql") or not 13 <= int(path.name[:3]) <= 28:
                continue
            source = path.read_text(encoding="utf-8")
            await conn.execute(source)
            await conn.execute(
                "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                (path.name, hashlib.sha256(source.encode()).hexdigest()),
            )
        original = await (
            await conn.execute("SELECT * FROM public.bot_message_inbox WHERE event_id='event-3'")
        ).fetchone()
        media = await (
            await conn.execute(
                "SELECT * FROM public.bot_inbound_media_refs WHERE media_ref='old-media'"
            )
        ).fetchone()
        await conn.execute(
            "INSERT INTO public.bot_media_captures(bot_id,media_ref,media_revision,event_id,part_index,content,"
            "blob_digest,mime,width,height,frame_count,decoded_pixels,source_manifest,source_digest,capture_id,capture_epoch) "
            "VALUES ('bot-a','old-media',1,'event-3',0,%s,%s,'image/png',2,2,1,4,%s,%s,'old-capture',1)",
            (
                content,
                "sha256:" + hashlib.sha256(content).hexdigest(),
                Jsonb(manifest),
                digest_json(manifest),
            ),
        )
        await conn.execute(
            "INSERT INTO public.bot_media_quota_heads(bot_id,retained_bytes) VALUES ('bot-a',%s)",
            (len(content),),
        )
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    repository = MessageStore(store)
    contents = InputContentStore(store)
    try:
        if previously_suppressed:
            lease = await store.acquire_host("old", "old", ttl_seconds=30)
            await FrozenPayloadStore(store).forget_sources(
                lease,
                "forget-before-migration",
                [contents.source_ref(original["source_event_id"])],
                expected_deletion_epoch=0,
            )
            await store.release_host(lease)
        await store.migrate()
        lease = await store.acquire_host("new", "new", ttl_seconds=120)
        raw = (
            await rows(
                repository, "SELECT * FROM public.bot_message_inbox WHERE event_id='event-3'"
            )
        )[0]
        raw_media = (
            await rows(
                repository,
                "SELECT * FROM public.bot_inbound_media_refs WHERE media_ref='old-media'",
            )
        )[0]
        raw_capture = (await rows(repository, "SELECT * FROM public.bot_media_captures"))[0]
        assert raw["segments"] is None and "display_name" not in raw["author"]
        assert raw_media["source"] is None
        assert raw_capture["content"] is None and raw_capture["source_manifest"] is None
        assert raw["input_digest"] == original["input_digest"]
        assert raw_capture["source_digest"] == digest_json(manifest)
        async with store.pool.connection() as conn, conn.transaction():
            clock = await contents.payloads.lock_in(conn)
            hydrated = await contents.read_input_in(
                conn, "event-3", expected_deletion_epoch=clock["deletion_epoch"]
            )
            captured = await contents.read_capture_in(
                conn, "old-media", expected_deletion_epoch=clock["deletion_epoch"]
            )
            if previously_suppressed:
                assert hydrated["payload_state"] == "UNAVAILABLE" and captured is None
            else:
                assert hydrated["author"] == original["author"]
                assert hydrated["segments"] == original["segments"]
                assert captured["content"] == content and captured["source_manifest"] == manifest
                source = await contents.read_media_source_in(
                    conn, "old-media", expected_deletion_epoch=0
                )
                assert source["source"] == media["source"]
            source = await (
                await conn.execute(
                    "SELECT u.*,e.input_batch_id FROM public.bot_memory_source_units u "
                    "JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) "
                    "JOIN public.bot_memory_source_eligibility e USING(bot_id,occurrence_id) WHERE o.event_id='event-3'"
                )
            ).fetchone()
            assert (
                source["order_origin"] == "RECONSTRUCTED"
                and source["input_batch_id"] == "open-batch"
            )
            before = await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_payload_headers")
            ).fetchone()
        await store.migrate()
        assert (await rows(repository, "SELECT count(*) AS n FROM public.bot_payload_headers"))[
            0
        ] == before
        if not previously_suppressed:
            # Backfilling a readable legacy reply creates a new copy only if
            # it also inherits the formal reply's conservative deletion scope.
            await MemorySourceStore(repository).backfill(lease, limit=8)
            copied = (
                await rows(
                    repository,
                    "SELECT u.payload_id AS source_payload_id,r.payload_id AS reply_payload_id "
                    "FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) "
                    "JOIN public.bot_conversation_replies r USING(bot_id,reply_id) WHERE u.kind='REPLY'",
                )
            )[0]
            assert await rows(
                repository,
                "SELECT 1 FROM public.bot_payload_derivations WHERE payload_id=%s AND parent_payload_id=%s",
                (copied["source_payload_id"], copied["reply_payload_id"]),
            )
            await erase(repository, lease, {"event_id": "event-3"})
        while (await contents.payloads.scrub(lease))["remaining"]:
            pass
        assert not await rows(
            repository, "SELECT 1 FROM public.bot_payload_bodies WHERE content=%s", (content,)
        )
        assert not await rows(
            repository,
            "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
            (b"old-signed-token",),
        )
        # Missing/erased old replies cannot poison the durable backfill cursor
        # or recreate source bodies from the retired plaintext column.
        from forest.storage.context_payloads import ContextPayloadStore

        current_epoch = (
            await rows(repository, "SELECT deletion_epoch FROM public.bot_memory_clock")
        )[0]["deletion_epoch"]
        await contents.payloads.forget_sources(
            lease,
            "erase-reply",
            [ContextPayloadStore(store).reply_source("old-reply")],
            expected_deletion_epoch=current_epoch,
        )
        while (await contents.payloads.scrub(lease))["remaining"]:
            pass
        async with store.pool.connection() as conn, conn.transaction():
            clock = await contents.payloads.lock_in(conn)
            old_reply = await (
                await conn.execute(
                    "SELECT * FROM public.bot_conversation_replies WHERE reply_id='old-reply'"
                )
            ).fetchone()
            old_reply = await ContextPayloadStore(store).read_reply_in(
                conn, old_reply, expected_deletion_epoch=clock["deletion_epoch"]
            )
            assert old_reply["payload_state"] != "ACTIVE"
        sources = MemorySourceStore(repository)
        while (await sources.backfill(lease, limit=8))["more_possible"]:
            pass
        assert not await rows(
            repository,
            "SELECT 1 FROM public.bot_memory_source_units u JOIN public.bot_payload_bodies b USING(bot_id,payload_id) WHERE u.kind='REPLY'",
        )
        assert (await sources.backfill(lease, limit=8)) == {
            "processed": 0,
            "scanned": 0,
            "more_possible": False,
        }
    finally:
        await store.close()

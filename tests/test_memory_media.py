"""Actual PostgreSQL and TCP tests for independent original-image capture."""

import asyncio
import threading
from dataclasses import replace
from datetime import timedelta

import pytest
import pytest_asyncio
from binding_seed import create_online_binding
from test_media import media_server, picture, respond
from test_memory_config import apply, plan
from test_memory_config import environment as environment
from test_messages import event, owner

from forest.errors import ForestError
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS
from forest.memory_types import SourceRef
from forest.storage.memory_media import MemoryMediaStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def media(environment):
    env = environment
    await create_online_binding(env.store, env.lease, "binding-a", "1000", ticket=env.ticket)
    messages = MessageStore(env.store)
    yield env, MemoryMediaStore(messages), MemorySourceStore(messages)


async def configure(env, origins, *, revision=0, **values):
    settings = {
        **DEFAULT_MEMORY_SETTINGS,
        "enabled": True,
        "media_allowed_origins": origins,
        "media_max_bytes": 512,
        **values,
    }
    return await apply(env, await plan(env, revision=revision, values=settings))


async def incoming(repo, lease, url, number=1, *, group="group-1", thread="", private=False):
    value = event(number, group=group)
    value["thread_id"] = thread
    if private:
        value["channel_kind"] = "private"
    value["segments"] = [
        {"type": "text", "text": "图片来源"},
        {"type": "image", "media_ref": {"kind": "onebot-source", "uri": url}},
    ]
    return await repo.messages.ingest(lease, "binding-a", value, decision="NO_TRIGGER")


async def rows(repo, query, params=()):
    async with repo.store.pool.connection() as conn:
        return await (await conn.execute(query, params)).fetchall()


async def global_sources(repo, sources, lease, **kwargs):
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        result = await sources.authorize_sources_in(conn, **kwargs)
        await repo.store._host(conn, lease)
        return result


async def test_silent_capture_needs_no_agent_or_actor_and_preserves_original_pixels(media):
    env, repo, sources = media
    content = picture()

    async def handler(writer, header):
        await respond(writer, content)

    async with media_server(handler) as (origin, requests):
        await configure(env, [origin])
        receipt = await incoming(repo, env.lease, origin + "/original.png")
        candidates = await repo.scan(env.lease)
        assert len(candidates) == 1
        result = await repo.capture_once(env.lease, candidates[0]["capture_id"])
        assert result["status"] == "CAPTURED" and len(requests) == 1
        assert not await rows(repo, "SELECT * FROM public.bot_agent_config_revisions")
        assert not await rows(repo, "SELECT * FROM public.bot_conversation_runs")
        assert (await global_sources(repo, sources, env.lease))["sources"] == []
        actor = await owner(repo.messages, env.lease, receipt["conversation_id"])
        batch = await repo.messages.select_input(actor, "silent")
        assert batch["kind"] == "SILENT_DRAIN"
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._host(conn, env.lease)
            authorized = await sources.authorize_sources_in(conn)
            pixels = await repo.materialize_in(
                conn,
                [row["source_unit_id"] for row in authorized["sources"]],
                expected_deletion_epoch=authorized["deletion_epoch"],
            )
        image = next(iter(pixels.values()))
        assert image.content == content and image.media_ref.startswith("source-media:")
        assert image.source_unit_id == candidates[0]["source_unit_id"] and image.part_id == "1"
        assert not await rows(repo, "SELECT * FROM public.bot_media_captures")
        head = (
            await rows(
                repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
            )
        )[0]
        assert head["manifest"]["sources"] == [
            {"source_unit_id": image.source_unit_id, "source_revision": 1, "part_id": "1"}
        ]
        assert (await rows(repo, "SELECT charged_bytes FROM public.bot_memory_media_daily"))[0][
            "charged_bytes"
        ] == len(content)
        assert (await rows(repo, "SELECT * FROM public.bot_memory_media_quota"))[0] == {
            "bot_id": env.store.bot_id,
            "retained_bytes": len(content),
            "reserved_bytes": 0,
        }


async def test_competing_workers_and_duplicate_occurrences_publish_once_then_restart_reads(media):
    env, repo, _ = media
    content = picture()

    async def handler(writer, header):
        await asyncio.sleep(0.03)
        await respond(writer, content)

    async with media_server(handler) as (origin, requests):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/same")
        await incoming(repo, env.lease, origin + "/same", thread="second")
        candidates = await repo.scan(env.lease)
        assert len(candidates) == 1
        capture_id = candidates[0]["capture_id"]
        outcomes = await asyncio.gather(
            *(repo.capture_once(env.lease, capture_id, "worker-" + str(i)) for i in range(8))
        )
        assert sum(row is not None for row in outcomes) == 1
        assert len(requests) == 1
        assert len(await rows(repo, "SELECT * FROM public.bot_memory_media_attempts")) == 1
        restarted = MemoryMediaStore(repo.messages)
        assert await restarted.scan(env.lease) == []
        assert await restarted.capture_once(env.lease, capture_id) is None
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._host(conn, env.lease)
            pixels = await restarted.materialize_in(
                conn, [candidates[0]["source_unit_id"]], expected_deletion_epoch=0
            )
        assert next(iter(pixels.values())).content == content


async def test_same_pixels_keep_independent_sources_and_frozen_occurrence_cut(media):
    env, repo, sources = media

    async def handler(writer, header):
        await respond(writer, picture())

    async with media_server(handler) as (origin, _):
        await configure(env, [origin])
        first = await incoming(repo, env.lease, origin + "/x")
        private = await incoming(repo, env.lease, origin + "/x", 2, group="user-1", private=True)
        thread = await incoming(repo, env.lease, origin + "/x", thread="second")
        candidates = await repo.scan(env.lease)
        assert len(candidates) == 2
        for candidate in candidates:
            await repo.capture_once(env.lease, candidate["capture_id"])
        for receipt in (first, private):
            actor = await owner(repo.messages, env.lease, receipt["conversation_id"])
            await repo.messages.select_input(actor, "drain")
        # Global indexing can see both sources; this thread has an unread
        # occurrence of the group source, and has no right to the direct source.
        assert len((await global_sources(repo, sources, env.lease))["sources"]) == 2
        assert (await sources.read(env.lease, thread["conversation_id"], input_cut=0))[
            "sources"
        ] == []
        group = (await sources.read(env.lease, first["conversation_id"], input_cut=1))["sources"]
        assert len(group) == 1 and group[0]["source_kind"] == "GROUP"
        page = await global_sources(repo, sources, env.lease, limit=1, after_source_unit_id="")
        following = await global_sources(
            repo,
            sources,
            env.lease,
            limit=1,
            after_source_unit_id=page["sources"][0]["source_unit_id"],
        )
        assert len(following["sources"]) == 1
        assert following["sources"][0]["source_unit_id"] > page["sources"][0]["source_unit_id"]
        direct = next(
            row
            for row in (await global_sources(repo, sources, env.lease))["sources"]
            if row["source_kind"] == "DIRECT"
        )
        await sources.payloads.forget_sources(
            env.lease,
            "forget-direct",
            (SourceRef(direct["source_unit_id"], part_id="1"),),
            expected_deletion_epoch=0,
        )
        await sources.payloads.scrub(env.lease)
        await repo.scan(env.lease)
        await repo.scan(env.lease)  # Never release another source's retained bytes.
        quota = (await rows(repo, "SELECT * FROM public.bot_memory_media_quota"))[0]
        assert quota["retained_bytes"] == len(picture()) and quota["reserved_bytes"] == 0
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._host(conn, env.lease)
            pixels = await repo.materialize_in(
                conn, [row["source_unit_id"] for row in candidates], expected_deletion_epoch=1
            )
        assert len(pixels) == 1 and next(iter(pixels))[0] != direct["source_unit_id"]


async def test_source_deletion_wins_against_late_capture_and_keeps_cost_accounting(media):
    env, repo, sources = media
    entered, release = asyncio.Event(), asyncio.Event()
    content = picture()

    async def handler(writer, header):
        entered.set()
        await release.wait()
        await respond(writer, content)

    async with media_server(handler) as (origin, _):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/late")
        candidate = (await repo.scan(env.lease))[0]
        pending = asyncio.create_task(repo.capture_once(env.lease, candidate["capture_id"]))
        await asyncio.wait_for(entered.wait(), 3)
        await sources.payloads.forget_sources(
            env.lease,
            "delete-image",
            (SourceRef(candidate["source_unit_id"], part_id="1"),),
            expected_deletion_epoch=0,
        )
        release.set()
        result = await pending
        assert result["status"] == "REVOKED"
        assert not await rows(
            repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
        )
        assert (await rows(repo, "SELECT * FROM public.bot_memory_media_quota"))[0][
            "reserved_bytes"
        ] == 0
        assert (await rows(repo, "SELECT charged_bytes FROM public.bot_memory_media_daily"))[0][
            "charged_bytes"
        ] == len(content)
        assert await repo.scan(env.lease) == []


async def test_config_revocation_while_downloading_prevents_publication(media):
    env, repo, _ = media
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, header):
        entered.set()
        await release.wait()
        await respond(writer, picture())

    async with media_server(handler) as (origin, _):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/late")
        candidate = (await repo.scan(env.lease))[0]
        pending = asyncio.create_task(repo.capture_once(env.lease, candidate["capture_id"]))
        await asyncio.wait_for(entered.wait(), 3)
        await configure(env, [origin], revision=1, enabled=False)
        release.set()
        result = await pending
        assert (
            result["status"] == "REJECTED"
            and result["error_code"] == "MEMORY_CONFIGURATION_REVOKED"
        )
        assert not await rows(
            repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
        )
        assert await repo.scan(env.lease) == []


async def test_cancelled_network_keeps_unknown_cost_and_releases_only_its_storage_reservation(
    media,
):
    env, repo, _ = media
    entered = asyncio.Event()

    async def handler(writer, header):
        entered.set()
        await asyncio.Event().wait()

    async with media_server(handler) as (origin, _):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/first")
        await incoming(repo, env.lease, origin + "/second", 2)
        a, b = await repo.scan(env.lease)
        other = await repo.claim(env.lease, b["capture_id"], "other")
        pending = asyncio.create_task(repo.capture_once(env.lease, a["capture_id"]))
        await asyncio.wait_for(entered.wait(), 3)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        heads = {
            row["capture_id"]: row
            for row in await rows(repo, "SELECT * FROM public.bot_memory_media_heads")
        }
        assert heads[a["capture_id"]]["status"] == "RETRYABLE"
        assert heads[b["capture_id"]]["status"] == "CLAIMED"
        quota = (await rows(repo, "SELECT * FROM public.bot_memory_media_quota"))[0]
        assert quota["reserved_bytes"] == 512 and quota["retained_bytes"] == 0
        assert (await rows(repo, "SELECT charged_bytes FROM public.bot_memory_media_daily"))[0][
            "charged_bytes"
        ] == 2048
        await repo.finish(env.lease, other, error="MEDIA_CANCELLED", actual_bytes=0)
        assert (await rows(repo, "SELECT charged_bytes FROM public.bot_memory_media_daily"))[0][
            "charged_bytes"
        ] == 1024


@pytest.mark.parametrize(
    "budget,code",
    [
        ("capture_daily_bytes", "MEDIA_DAILY_QUOTA_EXCEEDED"),
        ("capture_storage_bytes", "MEDIA_STORAGE_QUOTA_EXCEEDED"),
    ],
)
async def test_parallel_reservations_respect_daily_and_storage_quotas(media, budget, code):
    env, repo, _ = media
    await configure(
        env, ["http://127.0.0.1:9"], **{budget: 1024 if budget == "capture_daily_bytes" else 512}
    )
    await incoming(repo, env.lease, "http://127.0.0.1:9/a")
    await incoming(repo, env.lease, "http://127.0.0.1:9/b", 2)
    candidates = await repo.scan(env.lease)
    claims = await asyncio.gather(
        *(repo.claim(env.lease, row["capture_id"], "worker") for row in candidates)
    )
    assert sum(claim is not None for claim in claims) == 1
    blocked = [
        row
        for row in await rows(repo, "SELECT * FROM public.bot_memory_media_heads")
        if row["status"] == "QUOTA_EXCEEDED"
    ]
    assert len(blocked) == 1 and blocked[0]["error_code"] == code
    claim = next(claim for claim in claims if claim)
    await repo.finish(env.lease, claim, error="MEDIA_CANCELLED", actual_bytes=0)
    assert (await rows(repo, "SELECT * FROM public.bot_memory_media_quota"))[0][
        "reserved_bytes"
    ] == 0


async def test_new_host_recovers_pre_dispatch_claim_without_spending_or_reusing_old_permit(media):
    env, repo, _ = media

    async def handler(writer, header):
        await respond(writer, picture())

    async with media_server(handler) as (origin, requests):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/restart")
        candidate = (await repo.scan(env.lease))[0]
        old = await repo.claim(env.lease, candidate["capture_id"], "old-owner")
        old_lease = env.lease
        await repo.store.release_host(old_lease)
        env.lease = await repo.store.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
        restarted = MemoryMediaStore(repo.messages)
        await restarted.scan(env.lease)
        assert (await rows(repo, "SELECT charged_bytes FROM public.bot_memory_media_daily"))[0][
            "charged_bytes"
        ] == 0
        with pytest.raises(ForestError) as caught:
            await repo.execute(old_lease, old)
        assert caught.value.code == "STALE_HOST" and requests == []
        await asyncio.sleep(1.05)
        result = await restarted.capture_once(env.lease, candidate["capture_id"])
        assert (
            result["status"] == "CAPTURED" and result["attempt_count"] == 2 and len(requests) == 1
        )


@pytest.mark.parametrize(
    "failure,expected",
    [
        ("redirect", "MEDIA_HTTP_REJECTED"),
        ("large", "MEDIA_SIZE_EXCEEDED"),
        ("invalid", "MEDIA_IMAGE_INVALID"),
        ("disconnect", "MEDIA_TRANSPORT_FAILED"),
    ],
)
async def test_real_transport_rejects_redirect_oversize_invalid_and_bounds_retries(
    media, failure, expected
):
    env, repo, _ = media

    async def handler(writer, header):
        if failure == "redirect":
            await respond(
                writer, b"", status=302, extra="Location: http://127.0.0.1:9/forbidden\r\n"
            )
        elif failure == "large":
            await respond(writer, b"x" * 513)
        elif failure == "invalid":
            await respond(writer, b"not pixels")

    async with media_server(handler) as (origin, requests):
        await configure(env, [origin], capture_max_attempts=1)
        await incoming(repo, env.lease, origin + "/bad")
        candidate = (await repo.scan(env.lease))[0]
        result = await repo.capture_once(env.lease, candidate["capture_id"])
        assert result["error_code"] == expected
        assert result["status"] == ("UNAVAILABLE" if failure == "disconnect" else "REJECTED")
        assert len(requests) == 1 and await repo.scan(env.lease) == []
        assert not await rows(
            repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
        )


async def test_only_authorized_fixed_origin_and_claim_url_can_start_http(media):
    env, repo, _ = media
    async with media_server(lambda writer, header: respond(writer, picture())) as (
        origin,
        requests,
    ):
        await configure(env, [])
        await incoming(repo, env.lease, origin + "/source")
        candidate = (await repo.scan(env.lease))[0]
        assert await repo.capture_once(env.lease, candidate["capture_id"]) is None
        assert requests == []
        await configure(env, [origin], revision=1)
        candidate = (await repo.scan(env.lease))[0]
        claim = await repo.claim(env.lease, candidate["capture_id"], "owner")
        result = await repo.execute(env.lease, replace(claim, url=origin + "/substituted"))
        assert result["status"] == "REJECTED" and result["error_code"] == "MEDIA_SOURCE_MISMATCH"
        assert requests == []


@pytest.mark.parametrize("during_download", [False, True])
async def test_pause_resume_never_revives_an_old_capture_permission(media, during_download):
    env, repo, _ = media
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, header):
        entered.set()
        await release.wait()
        await respond(writer, picture())

    async with media_server(handler) as (origin, requests):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/fenced")
        candidate = (await repo.scan(env.lease))[0]
        claim = await repo.claim(env.lease, candidate["capture_id"], "owner")
        if during_download:
            pending = asyncio.create_task(repo.execute(env.lease, claim))
            await asyncio.wait_for(entered.wait(), 3)
        runtime = await repo.store.runtime()
        paused = await repo.store.set_maintenance(env.lease, runtime["maintenance_epoch"], False)
        await repo.store.set_maintenance(env.lease, paused["maintenance_epoch"], True)
        release.set()
        result = await pending if during_download else await repo.execute(env.lease, claim)
        assert result["status"] == "RETRYABLE"
        assert result["error_code"] == "MEDIA_MAINTENANCE_REVOKED"
        assert len(requests) == int(during_download)
        assert not await rows(
            repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
        )
        assert (await rows(repo, "SELECT reserved_bytes FROM public.bot_memory_media_quota"))[0][
            "reserved_bytes"
        ] == 0


async def test_repeated_cancellation_waits_for_owned_decoder_thread(media, monkeypatch):
    env, repo, _ = media
    entered, release = threading.Event(), threading.Event()
    from forest.storage.media import _validate_image

    def blocked_decoder(content):
        entered.set()
        assert release.wait(5)
        return _validate_image(content)

    monkeypatch.setattr("forest.storage.media._validate_image", blocked_decoder)
    async with media_server(lambda writer, header: respond(writer, picture())) as (origin, _):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/decoder")
        candidate = (await repo.scan(env.lease))[0]
        task = asyncio.create_task(repo.capture_once(env.lease, candidate["capture_id"]))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not await rows(
            repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
        )
        assert (await rows(repo, "SELECT reserved_bytes FROM public.bot_memory_media_quota"))[0][
            "reserved_bytes"
        ] == 0


async def test_expiry_after_body_write_rolls_back_pixels_and_quota_settlement(media, monkeypatch):
    env, repo, _ = media
    original = repo.payloads.put_bytes_in

    async def delayed_body(*args, **kwargs):
        result = await original(*args, **kwargs)
        await asyncio.sleep(1.05)
        return result

    monkeypatch.setattr(
        "forest.storage.memory_media.timedelta", lambda **kwargs: timedelta(seconds=1)
    )
    monkeypatch.setattr(repo.payloads, "put_bytes_in", delayed_body)
    async with media_server(lambda writer, header: respond(writer, picture())) as (origin, _):
        await configure(env, [origin])
        await incoming(repo, env.lease, origin + "/expires-at-commit")
        candidate = (await repo.scan(env.lease))[0]
        with pytest.raises(ForestError) as caught:
            await repo.capture_once(env.lease, candidate["capture_id"])
        assert caught.value.code == "MEDIA_CAPTURE_STALE"
        assert not await rows(
            repo, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='media.capture'"
        )
        quota = (await rows(repo, "SELECT * FROM public.bot_memory_media_quota"))[0]
        assert quota["retained_bytes"] == 0 and quota["reserved_bytes"] == 512
        await repo.scan(env.lease)
        assert (await rows(repo, "SELECT reserved_bytes FROM public.bot_memory_media_quota"))[0][
            "reserved_bytes"
        ] == 0
        assert (await rows(repo, "SELECT charged_bytes FROM public.bot_memory_media_daily"))[0][
            "charged_bytes"
        ] == 1024


async def test_materialization_reads_only_requested_parts_and_bounds_total_pixels(
    media, monkeypatch
):
    env, repo, _ = media
    content = picture()
    async with media_server(lambda writer, header: respond(writer, content)) as (origin, _):
        await configure(env, [origin])
        value = event(1)
        value["segments"] = [
            {"type": "image", "media_ref": {"uri": origin + "/" + str(index)}} for index in range(2)
        ]
        await repo.messages.ingest(env.lease, "binding-a", value, decision="NO_TRIGGER")
        candidates = await repo.scan(env.lease)
        assert len(candidates) == 2
        for candidate in candidates:
            await repo.capture_once(env.lease, candidate["capture_id"])
        unit_id = candidates[0]["source_unit_id"]
        loaded = []
        original = repo.payloads.read_bytes_in

        async def observe_read(conn, payload_id, **kwargs):
            loaded.append(payload_id)
            return await original(conn, payload_id, **kwargs)

        monkeypatch.setattr(repo.payloads, "read_bytes_in", observe_read)
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._host(conn, env.lease)
            assert (
                await repo.materialize_in(conn, [unit_id], expected_deletion_epoch=0, part_refs=[])
                == {}
            )
            assert loaded == []
            selected = await repo.materialize_in(
                conn, [unit_id], expected_deletion_epoch=0, part_refs=[(unit_id, "1")]
            )
            assert list(selected) == [(unit_id, "1")] and len(loaded) == 1
            loaded.clear()
            with pytest.raises(ForestError) as caught:
                await repo.materialize_in(
                    conn,
                    [unit_id],
                    expected_deletion_epoch=0,
                    part_refs=[(unit_id, "0"), (unit_id, "1")],
                    byte_limit=len(content),
                )
            assert caught.value.code == "MEDIA_MATERIALIZATION_BUDGET"
            assert len(loaded) == 1

"""Real HTTP and PostgreSQL image capture, source fencing and immutable pixels."""

import asyncio
import copy
import io
from contextlib import asynccontextmanager
from dataclasses import replace

import psycopg
import pytest
from PIL import Image
from psycopg.types.json import Jsonb
from test_messages import event, owner
from test_messages import messages as messages

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.errors import ForestError
from forest.models import ModelCapabilities, prepare_model_request
from forest.storage.media import MAX_MEDIA_BYTES, MediaStore, _url, _validate_image


def picture(fmt="PNG", size=(3, 2)):
    stream = io.BytesIO()
    Image.new("RGB", size, "red").save(stream, format=fmt)
    return stream.getvalue()


@asynccontextmanager
async def media_server(handler):
    requests, active = [], set()

    async def connection(reader, writer):
        task = asyncio.current_task()
        active.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            requests.append(header)
            await handler(writer, header)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            active.discard(task)

    listener = await asyncio.start_server(connection, "127.0.0.1", 0)
    url = "http://127.0.0.1:" + str(listener.sockets[0].getsockname()[1])
    try:
        yield url, requests
    finally:
        listener.close()
        await listener.wait_closed()
        for task in tuple(active):
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)


async def respond(writer, body, *, status=200, extra=""):
    writer.write(f"HTTP/1.1 {status} Test\r\nContent-Length: {len(body)}\r\n{extra}Connection: close\r\n\r\n".encode())
    writer.write(body)
    await writer.drain()


async def configure(repository, origins, *, enabled=True):
    """Seed immutable config facts in this test's private disposable database."""
    values = {**DEFAULT_AGENT_SETTINGS, "enabled": enabled, "model": "test", "media_allowed_origins": origins}
    async with repository.store.pool.connection() as conn, conn.transaction():
        await conn.execute("INSERT INTO public.bot_agent_config_heads(bot_id) VALUES ('bot-a') ON CONFLICT DO NOTHING")
        row = await (await conn.execute("SELECT revision FROM public.bot_agent_config_heads WHERE bot_id='bot-a' FOR UPDATE")).fetchone()
        revision = row["revision"] + 1
        identifier = "media-test-" + str(revision)
        await conn.execute(
            "INSERT INTO public.bot_web_operations(bot_id,operation_id,action,input_digest,subject_id,issuer_id,ticket_basis,result) "
            "VALUES ('bot-a',%s,'agent.config.apply','test','admin','test','{}','{}')", (identifier,),
        )
        await conn.execute(
            "INSERT INTO public.bot_agent_config_plans(bot_id,plan_id,subject_id,issuer_id,expected_revision,values,"
            "secret_ciphertext,credentials_changed,plan_digest,expires_at) "
            "VALUES ('bot-a',%s,'admin','test',%s,%s,'cipher',false,'test',clock_timestamp()+interval '1 minute')",
            (identifier, revision - 1, Jsonb(values)),
        )
        await conn.execute(
            "INSERT INTO public.bot_agent_config_revisions(bot_id,revision,previous_revision,values,secret_revision,"
            "secret_ciphertext,policy_revision,plan_id,operation_id) VALUES ('bot-a',%s,%s,%s,1,'cipher',1,%s,%s)",
            (revision, revision - 1, Jsonb(values), identifier, identifier),
        )
        await conn.execute("UPDATE public.bot_agent_config_heads SET revision=%s,secret_revision=1 WHERE bot_id='bot-a'", (revision,))


async def context(repository, lease, uri, *, number=1, group="group-1"):
    incoming = event(number, group=group)
    incoming["segments"] = [{"type": "image", "media_ref": {"kind": "onebot-source", "uri": uri,
                                                               "state": "PENDING_CAPTURE"}}]
    received = await repository.ingest(lease, "binding-a", incoming, decision="TRIGGER")
    actor = await owner(repository, lease, received["conversation_id"])
    batch = await repository.select_input(actor, "select-" + str(number))
    canonical = await repository.context(actor, run_id=batch["run_id"])
    snapshot = {"bot_id": "bot-a", "run_id": batch["run_id"], "logical_step_id": "step",
                "input_revision": batch["input_revision"], "context_revision": canonical["context_revision"],
                "context_view_revision": canonical["context_view_revision"], "selected_input_end_seq": batch["end_seq"],
                "trigger_ids": batch["trigger_ids"], "input_batch_ids": [batch["input_batch_id"]],
                "units": [{"unit_id": u["unit_id"], "role": u["context_role"], "source": u["source"],
                           "author": u["author"], "parts": u["segments"]} for u in canonical["units"]]}
    return actor, snapshot


@pytest.mark.parametrize("fmt,mime", [("PNG", "image/png"), ("JPEG", "image/jpeg"),
                                      ("WEBP", "image/webp"), ("GIF", "image/gif")])
def test_real_decoding_detects_format_dimensions_and_retains_original(fmt, mime):
    content = picture(fmt)
    properties = _validate_image(content)
    assert properties == {"mime": mime, "width": 3, "height": 2, "frame_count": 1, "decoded_pixels": 6}


@pytest.mark.parametrize("url", ["file:///secret", "data:image/png;base64,a", "http://user:pass@host/a",
                                     "http://host/a#secret", "http://host\\@evil/a", "http://host/%0a\n",
                                     "http://host:99999/a", "http://%68ost/a"])
def test_url_rejects_local_and_credential_confusions(url):
    with pytest.raises(ForestError):
        _url(url)


def test_origin_is_exact_and_normalizes_only_equivalent_http_origins():
    assert _url("https://Example.com/image?q=1")[1] == "https://example.com:443"
    assert _url("https://example.com.evil/image")[1] != _url("https://example.com")[1]
    assert _url("https://example.com:444/image")[1] != _url("https://example.com")[1]
    with pytest.raises(ForestError):
        _url("https://example.com/path", origin_only=True)


@pytest.mark.parametrize("content", [b"<svg>bad</svg>", picture()[:-8], b"x" * (MAX_MEDIA_BYTES + 1)],
                         ids=["unsupported-svg", "truncated-png", "oversized"])
def test_invalid_truncated_and_oversized_images_are_not_snapshots(content):
    with pytest.raises(ForestError):
        _validate_image(content)


def test_pixel_and_animation_bombs_are_bounded(monkeypatch):
    monkeypatch.setattr("forest.storage.media.MAX_MEDIA_PIXELS", 4)
    with pytest.raises(ForestError) as caught:
        _validate_image(picture())
    assert caught.value.code == "MEDIA_PIXELS_EXCEEDED"
    monkeypatch.setattr("forest.storage.media.MAX_MEDIA_PIXELS", 1000)
    monkeypatch.setattr("forest.storage.media.MAX_MEDIA_FRAMES", 1)
    stream = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(stream, format="GIF", save_all=True,
                                         append_images=[Image.new("RGB", (2, 2), "blue")])
    with pytest.raises(ForestError) as caught:
        _validate_image(stream.getvalue())
    assert caught.value.code == "MEDIA_PIXELS_EXCEEDED"


@pytest.mark.postgres
async def test_capture_is_bot_bound_immutable_and_restart_reuses_original_bytes(messages, monkeypatch):
    repository, lease, _ = messages
    content = picture()
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")

    async def handler(writer, request):
        assert b"Authorization:" not in request and b"Cookie:" not in request
        await respond(writer, content, extra="Content-Type: application/octet-stream\r\nSet-Cookie: private=secret\r\n")

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image?platform=signed")
        before = copy.deepcopy(snapshot)
        result = await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin])
        assert len(result) == 1 and len(requests) == 1
        image = next(iter(result.values()))
        assert image.content == content and image.media_revision == 1
        restored = await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[])
        assert restored == result and len(requests) == 1
        assert snapshot == before
        visual = prepare_model_request(snapshot, ModelCapabilities("model", "vision", "1", input_modalities=("text", "image")),
                                       request_id="request", transport_attempt_id="attempt", media=result)
        assert b"data:image/png;base64," in visual.body
        assert "platform=signed" not in visual.manifest_json and origin not in visual.manifest_json
        async with repository.store.pool.connection() as conn:
            saved = await (await conn.execute("SELECT * FROM public.bot_media_captures")).fetchone()
            assert saved["source_manifest"]["author"]["external_user_id"] == "user-1"
            assert saved["source_manifest"]["channel_id"] == "group-1"
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                async with conn.transaction():
                    await conn.execute("UPDATE public.bot_media_captures SET content='changed'")
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                async with conn.transaction():
                    await conn.execute("UPDATE public.bot_inbound_media_refs SET source='{}'")


@pytest.mark.postgres
async def test_origin_requires_both_frozen_and_current_administrator_allowlist(messages):
    repository, lease, _ = messages

    async def handler(writer, request):
        await respond(writer, picture())

    async with media_server(handler) as (origin, requests):
        actor, snapshot = await context(repository, lease, origin + "/image")
        store = MediaStore(repository)
        assert await store.materialize(actor, snapshot, allowed_origins=[origin]) == {}
        await configure(repository, [origin])
        assert await store.materialize(actor, snapshot, allowed_origins=[]) == {}
        await configure(repository, [], enabled=True)
        assert await store.materialize(actor, snapshot, allowed_origins=[origin]) == {}
        assert requests == []


@pytest.mark.postgres
async def test_concurrent_capture_has_single_download_and_stable_revision(messages):
    repository, lease, _ = messages
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, request):
        entered.set()
        await release.wait()
        await respond(writer, picture())

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image")
        first = asyncio.create_task(MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin]))
        await asyncio.wait_for(entered.wait(), 5)
        assert await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin]) == {}
        release.set()
        result = await first
        assert len(result) == 1 and len(requests) == 1
        assert await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin]) == result


@pytest.mark.postgres
async def test_bot_quota_serializes_concurrent_groups_and_counts_only_committed_bytes(messages, monkeypatch):
    repository, lease, _ = messages
    content = picture()
    monkeypatch.setattr("forest.storage.media.MAX_BOT_MEDIA_BYTES", len(content))

    async def handler(writer, request):
        await respond(writer, content)

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        left = await context(repository, lease, origin + "/a", group="group-a")
        right = await context(repository, lease, origin + "/b", group="group-b")
        outputs = await asyncio.gather(*[MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin])
                                        for actor, snapshot in (left, right)])
        assert sum(len(output) for output in outputs) == 1 and len(requests) == 2
        async with repository.store.pool.connection() as conn:
            quota = await (await conn.execute("SELECT retained_bytes FROM public.bot_media_quota_heads")).fetchone()
            count = await (await conn.execute("SELECT count(*) AS n FROM public.bot_media_captures")).fetchone()
            assert quota["retained_bytes"] == len(content) and count["n"] == 1
            failed = await (await conn.execute("SELECT error_code FROM public.bot_inbound_media_refs WHERE status='FETCH_FAILED'")).fetchone()
            assert failed["error_code"] == "MEDIA_BOT_QUOTA_EXCEEDED"


@pytest.mark.postgres
async def test_capture_time_is_clipped_to_remaining_persistent_run_deadline(messages):
    repository, lease, _ = messages

    async def handler(writer, request):
        await asyncio.Event().wait()

    async with media_server(handler) as (origin, _):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image")
        async with repository.store.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_agent_runs(bot_id,run_id,conversation_id,session_id,session_epoch,spec,spec_digest,"
                "actor_epoch,actor_host_incarnation,input_revision,selected_input_end_seq,deadline_at) "
                "SELECT bot_id,run_id,conversation_id,session_id,1,'{}','test',%s,%s,input_revision,selected_input_end_seq,"
                "clock_timestamp()+interval '0.3 seconds' FROM public.bot_conversation_runs WHERE run_id=%s",
                (actor.actor_epoch, actor.host.incarnation, snapshot["run_id"]),
            )
        result = await asyncio.wait_for(MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin]), 2)
        assert result == {}


@pytest.mark.postgres
@pytest.mark.parametrize("revocation", ["origin", "actor", "readability"])
async def test_inflight_revocation_prevents_publishing_pixels(messages, revocation):
    repository, lease, _ = messages
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, request):
        entered.set()
        await release.wait()
        await respond(writer, picture())

    async with media_server(handler) as (origin, _):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image")
        task = asyncio.create_task(MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin]))
        await asyncio.wait_for(entered.wait(), 5)
        if revocation == "origin":
            await configure(repository, [])
        else:
            async with repository.store.pool.connection() as conn:
                if revocation == "actor":
                    await conn.execute("UPDATE public.bot_conversations SET actor_epoch=actor_epoch+1")
                else:
                    await conn.execute("UPDATE public.bot_message_inbox SET context_disposition='RESTRICTED'")
        release.set()
        if revocation == "actor":
            with pytest.raises(ForestError, match="ownership"):
                await task
        else:
            assert await task == {}
        async with repository.store.pool.connection() as conn:
            assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_media_captures")).fetchone())["n"] == 0


@pytest.mark.postgres
@pytest.mark.parametrize("change", ["bot", "unit", "part", "cut", "actor_bot"])
async def test_forged_media_source_never_starts_http(messages, change):
    repository, lease, _ = messages

    async def handler(writer, request):
        await respond(writer, picture())

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image")
        if change == "bot":
            snapshot["bot_id"] = "other"
        elif change == "unit":
            snapshot["units"][0]["unit_id"] = "forged"
        elif change == "part":
            snapshot["units"][0]["parts"][0]["part_id"] = "1"
        elif change == "cut":
            snapshot["selected_input_end_seq"] = 0
        else:
            actor = replace(actor, host=replace(actor.host, bot_id="other"))
        with pytest.raises(ForestError):
            await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[origin])
        assert requests == []


@pytest.mark.postgres
@pytest.mark.parametrize("failure", ["redirect", "too_large", "gzip", "invalid", "timeout", "cancel"])
async def test_failed_transport_and_cancel_never_publish_partial_image(messages, failure):
    repository, lease, _ = messages
    entered = asyncio.Event()

    async def handler(writer, request):
        entered.set()
        if failure in {"timeout", "cancel"}:
            await asyncio.Event().wait()
        elif failure == "redirect":
            await respond(writer, b"", status=302, extra="Location: http://127.0.0.1:1/private\r\n")
        elif failure == "too_large":
            writer.write(f"HTTP/1.1 200 OK\r\nContent-Length: {MAX_MEDIA_BYTES + 1}\r\n\r\n".encode())
            await writer.drain()
        elif failure == "gzip":
            await respond(writer, b"compressed", extra="Content-Encoding: gzip\r\n")
        else:
            await respond(writer, b"not an image")

    async with media_server(handler) as (origin, requests):
        await configure(repository, [origin])
        actor, snapshot = await context(repository, lease, origin + "/image")
        store = MediaStore(repository, timeout_seconds=0.5 if failure == "timeout" else 15)
        task = asyncio.create_task(store.materialize(actor, snapshot, allowed_origins=[origin]))
        await asyncio.wait_for(entered.wait(), 5)
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert await task == {}
        assert await store.materialize(actor, snapshot, allowed_origins=[origin]) == {}
        assert len(requests) == 1
        async with repository.store.pool.connection() as conn:
            assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_media_captures")).fetchone())["n"] == 0

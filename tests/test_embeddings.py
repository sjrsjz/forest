"""Protocol tests against real local TCP, without paid Provider inference."""

import asyncio
import base64
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from forest.embeddings import (
    MAX_BATCH_IMAGE_BYTES,
    MAX_IMAGE_BYTES,
    MAX_RESPONSE_BYTES,
    MAX_TEXT_BYTES,
    EmbeddingCancelled,
    EmbeddingClient,
    EmbeddingInput,
    EmbeddingProfile,
    prepare_embedding_request,
)
from forest.errors import ForestError
from forest.models import MediaSnapshot

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jD1sAAAAASUVORK5CYII="
)


def profile(**changes):
    return EmbeddingProfile(
        "jina-test-installation", "weights-and-processor-v1", dimensions=4, **changes
    )


def capture(content=PNG):
    return MediaSnapshot(
        "bot-a",
        "media-a",
        1,
        "unit-a",
        "part-a",
        "image/png",
        content,
        hashlib.sha256(content).hexdigest(),
        1,
        1,
    )


def prepared(*inputs, purpose="document", configuration=None):
    return prepare_embedding_request(
        configuration or profile(),
        inputs or (EmbeddingInput("text-a", text="红色拱桥"),),
        request_id="request-a",
        transport_attempt_id="attempt-a",
        purpose=purpose,
    )


async def permit(request):
    return None


def response(count=1):
    return {
        "model": "jina-clip-v2",
        "object": "list",
        "usage": {"total_tokens": 7},
        "data": [
            {"object": "embedding", "index": i, "embedding": [1.0, 0.0, 0.0, 0.0]}
            for i in range(count)
        ],
    }


async def send(writer, value, *, status=200, content_type="application/json", extra=""):
    body = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False).encode()
    writer.write(
        f"HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n{extra}\r\n".encode()
        + body
    )
    await writer.drain()


@asynccontextmanager
async def server(handler):
    requests, active = [], set()

    async def connection(reader, writer):
        task = asyncio.current_task()
        active.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            headers = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in header.split(b"\r\n")[1:]
                if b":" in line
            }
            body = await reader.readexactly(int(headers[b"content-length"]))
            record = {"start": header.split(b"\r\n", 1)[0], "headers": headers, "body": body}
            requests.append(record)
            await handler(writer, record)
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
    endpoint = f"http://127.0.0.1:{listener.sockets[0].getsockname()[1]}/v1/embeddings"
    try:
        yield endpoint, requests, active
    finally:
        listener.close()
        await listener.wait_closed()
        for task in tuple(active):
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)


def test_frozen_request_preserves_capture_and_task_pair_without_fusing():
    image = capture()
    request = prepared(
        EmbeddingInput("text", text="红色拱桥", source_json='{"unit_id":"unit-a"}'),
        EmbeddingInput("image", image=image),
    )
    request.verify()
    body = json.loads(request.body)
    assert body["input"][0] == {"text": "红色拱桥"}
    assert base64.b64decode(body["input"][1]["image"], validate=True) == PNG
    assert "task" not in body and "source" not in body
    assert request.input_count == 2 and image.content == PNG
    assert b"unit-a" not in request.body
    assert request.payload_digest == "sha256:" + hashlib.sha256(request.body).hexdigest()
    retry = request.replay("attempt-b")
    assert retry.body == request.body and retry.manifest_json == request.manifest_json
    assert (
        retry.request_id == request.request_id
        and retry.transport_attempt_id != request.transport_attempt_id
    )
    copy = request.manifest
    copy["facets"][0]["source"]["unit_id"] = "changed"
    assert request.manifest["facets"][0]["source"]["unit_id"] == "unit-a"
    query = prepared(EmbeddingInput("image-query", image=image), purpose="query")
    assert json.loads(query.body)["task"] == "retrieval.query" and query.input_count == 1
    assert query.space_id == request.space_id


def test_revision_dimension_and_deployment_each_create_a_distinct_space():
    original = profile()
    assert (
        len(
            {
                original.space_id,
                replace(original, dimensions=8).space_id,
                replace(original, deployment_id="another-installation").space_id,
                replace(original, model_revision="revision-2").space_id,
            }
        )
        == 4
    )


@pytest.mark.parametrize(
    "change",
    [
        lambda r: replace(r, body=r.body + b" "),
        lambda r: replace(r, payload_digest="sha256:" + "0" * 64),
        lambda r: replace(r, request_id="another"),
        lambda r: replace(r, space_id="another"),
    ],
)
def test_changed_prepared_bytes_or_identity_are_rejected(change):
    with pytest.raises(ForestError, match="fixed bytes"):
        change(prepared()).verify()


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"text": " "},
        {"text": "a" * (MAX_TEXT_BYTES + 1)},
        {"text": "\ud800"},
        {"image": "https://example.com/image.png"},
        {"text": "caption", "image": capture()},
        {"text": "ok", "source_json": '{"text":"free-form prompt"}'},
        {"text": "ok", "source_json": '{"unit_id":"a","unit_id":"b"}'},
        {"text": "ok", "source_json": '{"unit_id":"\\ud800"}'},
        {"image": capture(), "source_json": '{"bot_id":"another-bot"}'},
    ],
)
def test_inputs_reject_urls_ambiguous_modalities_and_unbounded_or_forged_source(kwargs):
    with pytest.raises(ForestError):
        EmbeddingInput("facet", **kwargs)


def test_batch_rejects_duplicates_generators_and_excess_image_bytes():
    with pytest.raises(ForestError):
        prepared(*[EmbeddingInput("same", text="a") for _ in range(2)])
    with pytest.raises(ForestError):
        prepare_embedding_request(
            profile(),
            (EmbeddingInput(str(i), text="a") for i in range(100)),
            request_id="r",
            transport_attempt_id="a",
        )
    with pytest.raises(ForestError):
        prepared(*[EmbeddingInput(str(i), text="a") for i in range(33)])
    with pytest.raises(ForestError):
        EmbeddingInput("too-large", image=capture(b"x" * (MAX_IMAGE_BYTES + 1)))
    count = MAX_BATCH_IMAGE_BYTES // MAX_IMAGE_BYTES + 1
    with pytest.raises(ForestError):
        prepared(
            *[EmbeddingInput(str(i), image=capture(b"x" * MAX_IMAGE_BYTES)) for i in range(count)]
        )


async def test_real_tcp_sends_exact_bytes_and_reorders_attributed_facets(monkeypatch):
    async def handler(writer, record):
        body = response(2)
        body["data"] = [
            {"index": 1, "embedding": [0, 1, 0, 0]},
            {"index": 0, "embedding": [1, 0, 0, 0]},
        ]
        body["usage"] = {
            "total_tokens": 11,
            "prompt_tokens": 5,
            "image_tokens": 6,
            "audio_tokens": None,
        }
        await send(writer, body)

    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "secret-api-key", profile())
        request = prepared(
            EmbeddingInput("text", text="bridge"), EmbeddingInput("picture", image=capture())
        )
        try:
            result = await client.dispatch(request, permitcheck=permit)
            assert result.status == "RESPONDED"
            assert [(v.facet_id, v.index, v.values) for v in result.vectors] == [
                ("text", 0, (1.0, 0.0, 0.0, 0.0)),
                ("picture", 1, (0.0, 1.0, 0.0, 0.0)),
            ]
            assert result.usage == {"total_tokens": 11, "prompt_tokens": 5, "image_tokens": 6}
            assert requests[0]["body"] == request.body
            assert requests[0]["start"] == b"POST /v1/embeddings HTTP/1.1"
            assert requests[0]["headers"][b"authorization"] == b"Bearer secret-api-key"
            assert b"secret-api-key" not in request.body and "secret-api-key" not in repr(client)
            assert "secret-api-key" not in json.dumps(result.as_dict())
        finally:
            await client.aclose()
        assert client._client.is_closed and not client._tasks


@pytest.mark.parametrize(
    "status,expected",
    [
        (301, "REJECTED"),
        (400, "REJECTED"),
        (401, "REJECTED"),
        (403, "REJECTED"),
        (404, "REJECTED"),
        (409, "REJECTED"),
        (413, "REJECTED"),
        (422, "REJECTED"),
        (429, "REJECTED"),
        (500, "UNKNOWN"),
        (503, "UNKNOWN"),
        (504, "UNKNOWN"),
    ],
)
async def test_http_errors_do_not_assume_free_billing_retry_or_echo_remote_text(status, expected):
    async def handler(writer, record):
        await send(
            writer,
            b"remote-secret-body-and-private-input",
            status=status,
            extra="Retry-After: 0\r\nLocation: http://127.0.0.1:1/v1/embeddings\r\n",
        )

    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "key", profile())
        try:
            result = await client.dispatch(prepared(), permitcheck=permit)
            assert result.status == expected and result.http_status == status
            assert result.usage is None and not result.vectors and len(requests) == 1
            assert "remote-secret" not in json.dumps(result.as_dict())
        finally:
            await client.aclose()


@pytest.mark.parametrize(
    "change,code",
    [
        (lambda r: r.update(model="another-model"), "EMBEDDING_RESPONSE_INVALID"),
        (lambda r: r.update(data=[]), "EMBEDDING_RESPONSE_INVALID"),
        (lambda r: r["data"][0].update(index=True), "EMBEDDING_INDEX_INVALID"),
        (lambda r: r["data"][0].update(index=-1), "EMBEDDING_INDEX_INVALID"),
        (lambda r: r["data"][0].update(index=1), "EMBEDDING_INDEX_INVALID"),
        (lambda r: r["data"][0].update(embedding=[1, 0]), "EMBEDDING_DIMENSION_INVALID"),
        (lambda r: r["data"][0].update(embedding=[True, 0, 0, 0]), "EMBEDDING_VECTOR_INVALID"),
        (lambda r: r["data"][0].update(embedding=[0, 0, 0, 0]), "EMBEDDING_VECTOR_INVALID"),
        (lambda r: r["data"][0].update(embedding=[2, 0, 0, 0]), "EMBEDDING_VECTOR_INVALID"),
        (lambda r: r["data"][0].update(embedding=[10**400, 0, 0, 0]), "EMBEDDING_VECTOR_INVALID"),
        (
            lambda r: r["data"][0].update(embedding=[float("nan"), 0, 0, 0]),
            "EMBEDDING_JSON_INVALID",
        ),
        (
            lambda r: r["data"][0].update(embedding=[float("inf"), 0, 0, 0]),
            "EMBEDDING_JSON_INVALID",
        ),
        (lambda r: r.update(usage={"total_tokens": True}), "EMBEDDING_USAGE_INVALID"),
        (lambda r: r.update(usage={"total_tokens": -1}), "EMBEDDING_USAGE_INVALID"),
        (
            lambda r: r.update(usage={"total_tokens": 7, "prompt_tokens": 8}),
            "EMBEDDING_USAGE_INVALID",
        ),
        (
            lambda r: r.update(usage={"total_tokens": 7, "credential": "no"}),
            "EMBEDDING_USAGE_INVALID",
        ),
    ],
)
async def test_malformed_success_responses_are_unknown_and_cannot_publish_vectors(change, code):
    payload = response()
    change(payload)

    async def handler(writer, record):
        await send(writer, payload)

    async with server(handler) as (endpoint, _, _):
        client = EmbeddingClient(endpoint, "key", profile())
        try:
            result = await client.dispatch(prepared(), permitcheck=permit)
            assert result.status == "UNKNOWN" and result.error_code == code
            assert result.vectors == () and result.usage is None
        finally:
            await client.aclose()


@pytest.mark.parametrize(
    "body,content_type,extra,code",
    [
        (
            b'{"model":"jina-clip-v2","model":"another"}',
            "application/json",
            "",
            "EMBEDDING_JSON_INVALID",
        ),
        (b"{", "application/json", "", "EMBEDDING_JSON_INVALID"),
        (b"not-json", "text/html", "", "EMBEDDING_RESPONSE_ENCODING"),
        (
            b"gzip-data",
            "application/json",
            "Content-Encoding: gzip\r\n",
            "EMBEDDING_RESPONSE_ENCODING",
        ),
        (b"x" * (MAX_RESPONSE_BYTES + 1), "application/json", "", "EMBEDDING_RESPONSE_TOO_LARGE"),
    ],
    ids=["duplicate-json-key", "incomplete-json", "wrong-content-type", "compressed", "oversized"],
)
async def test_response_encoding_json_and_size_limits(body, content_type, extra, code):
    async def handler(writer, record):
        await send(writer, body, content_type=content_type, extra=extra)

    async with server(handler) as (endpoint, _, _):
        client = EmbeddingClient(endpoint, "key", profile())
        try:
            result = await client.dispatch(prepared(), permitcheck=permit)
            assert result.status == "UNKNOWN" and result.error_code == code
        finally:
            await client.aclose()


async def test_duplicate_response_indexes_cannot_swap_two_sources():
    async def handler(writer, record):
        value = response(2)
        value["data"][1]["index"] = 0
        await send(writer, value)

    async with server(handler) as (endpoint, _, _):
        client = EmbeddingClient(endpoint, "key", profile())
        try:
            result = await client.dispatch(
                prepared(EmbeddingInput("one", text="one"), EmbeddingInput("two", text="two")),
                permitcheck=permit,
            )
            assert result.status == "UNKNOWN" and result.error_code == "EMBEDDING_INDEX_INVALID"
        finally:
            await client.aclose()


async def test_capacity_timeout_and_cancellation_distinguish_unsent_and_inflight():
    received = asyncio.Event()

    async def handler(writer, record):
        received.set()
        await asyncio.Event().wait()

    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "key", profile(), max_connections=1)
        invoked = []

        async def check(request):
            invoked.append(request.transport_attempt_id)

        first = asyncio.create_task(client.dispatch(prepared(), permitcheck=check))
        try:
            await asyncio.wait_for(received.wait(), 2)
            timed_out = await client.dispatch(
                prepared().replay("queued-timeout"), permitcheck=check, timeout=0.03
            )
            assert timed_out.status == "NOT_SENT" and timed_out.error_code == "EMBEDDING_TIMEOUT"
            queued = asyncio.create_task(
                client.dispatch(prepared().replay("queued-cancel"), permitcheck=check)
            )
            await asyncio.sleep(0)
            queued.cancel()
            with pytest.raises(EmbeddingCancelled) as cancelled:
                await queued
            assert cancelled.value.result.status == "NOT_SENT"
            first.cancel()
            with pytest.raises(EmbeddingCancelled) as cancelled:
                await first
            assert cancelled.value.result.status == "UNKNOWN"
            assert invoked == ["attempt-a"] and len(requests) == 1
        finally:
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)
            await client.aclose()
            await client.aclose()
        assert not client._tasks and client._client.is_closed


async def test_permit_rejection_and_foreign_profile_never_reach_network():
    async def handler(writer, record):
        await send(writer, response())

    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "key", profile())

        async def reject(request):
            raise ForestError("SOURCE_REVOKED", "Source was revoked", 403)

        try:
            with pytest.raises(ForestError) as caught:
                await client.dispatch(prepared(), permitcheck=reject)
            assert caught.value.code == "SOURCE_REVOKED"
            with pytest.raises(ForestError) as caught:
                await client.dispatch(
                    prepared(configuration=replace(profile(), model_revision="v2")),
                    permitcheck=permit,
                )
            assert caught.value.code == "EMBEDDING_ROUTE_MISMATCH" and not requests
        finally:
            await client.aclose()
        result = await client.dispatch(prepared(), permitcheck=permit)
        assert result.status == "NOT_SENT" and not requests


async def test_connection_failure_is_unsent_but_received_then_disconnected_is_unknown():
    listener = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    endpoint = f"http://127.0.0.1:{listener.sockets[0].getsockname()[1]}/v1/embeddings"
    listener.close()
    await listener.wait_closed()
    client = EmbeddingClient(endpoint, "", profile())
    try:
        result = await client.dispatch(prepared(), permitcheck=permit, timeout=2)
        assert (
            result.status == "NOT_SENT" and result.error_code == "EMBEDDING_CONNECTION_UNAVAILABLE"
        )
    finally:
        await client.aclose()

    async def disconnect(writer, record):
        return

    async with server(disconnect) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "", profile())
        try:
            result = await client.dispatch(prepared(), permitcheck=permit)
            assert result.status == "UNKNOWN" and len(requests) == 1
        finally:
            await client.aclose()


async def test_external_close_joins_inflight_owner_even_when_owner_finally_closes_client():
    received = asyncio.Event()

    async def handler(writer, record):
        received.set()
        await asyncio.Event().wait()

    async with server(handler) as (endpoint, _, _):
        client = EmbeddingClient(endpoint, "", profile())

        async def owner():
            try:
                return await client.dispatch(prepared(), permitcheck=permit)
            finally:
                await client.aclose()

        task = asyncio.create_task(owner())
        await asyncio.wait_for(received.wait(), 2)
        async with asyncio.timeout(2):
            await client.aclose()
        with pytest.raises(EmbeddingCancelled) as cancelled:
            await task
        assert cancelled.value.result.status == "UNKNOWN"
        assert client._client.is_closed and not client._tasks and not client._closing_tasks


async def test_close_does_not_wait_for_callers_unrelated_work_after_cancellation():
    received, continued, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def handler(writer, record):
        received.set()
        await asyncio.Event().wait()

    async with server(handler) as (endpoint, _, _):
        client = EmbeddingClient(endpoint, "", profile())

        async def caller():
            try:
                await client.dispatch(prepared(), permitcheck=permit)
            except EmbeddingCancelled as cancelled:
                observation = cancelled.result
                continued.set()
                await release.wait()
                return observation

        task = asyncio.create_task(caller())
        try:
            await asyncio.wait_for(received.wait(), 2)
            async with asyncio.timeout(2):
                await client.aclose()
                await continued.wait()
            assert not task.done()
            assert client._client.is_closed and not client._tasks
        finally:
            release.set()
            result = await asyncio.wait_for(task, 2)
            await client.aclose()
        assert result.status == "UNKNOWN"


async def test_close_bounds_uncooperative_permit_and_retains_cleanup_failure():
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(writer, record):
        await send(writer, response())

    async def slow_permit(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # Host callback bug: cancellation is suppressed during unrelated IO.
            await release.wait()

    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "", profile())
        task = asyncio.create_task(client.dispatch(prepared(), permitcheck=slow_permit))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            async with asyncio.timeout(2):
                with pytest.raises(ForestError) as failed:
                    await client.aclose()
            assert failed.value.code == "EMBEDDING_CLEANUP_FAILED"
            assert client._client.is_closed and len(client._tasks) == 1
            assert task not in client._tasks and not requests
        finally:
            release.set()
            result = await asyncio.wait_for(task, 2)
            await client.aclose()
        assert result.status == "NOT_SENT" and not requests
        assert not client._tasks and not client._closing_tasks


async def test_clip_usage_preserves_overlapping_counters_without_invented_sum():
    async def handler(writer, record):
        body = response()
        body["usage"] = {"total_tokens": 11, "prompt_tokens": 11, "image_tokens": 6}
        await send(writer, body)

    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "", profile())
        try:
            result = await client.dispatch(prepared(), permitcheck=permit)
            assert result.status == "RESPONDED" and len(requests) == 1
            assert result.usage == {"total_tokens": 11, "prompt_tokens": 11, "image_tokens": 6}
        finally:
            await client.aclose()


@pytest.mark.parametrize("cancel_caller", [True, False], ids=["caller-cancel", "timeout"])
async def test_permit_cannot_swallow_cancellation_and_then_send(cancel_caller):
    entered = asyncio.Event()
    cancellations = []

    async def handler(writer, record):
        await send(writer, response())

    async def swallowing_permit(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellations.append("swallowed")
        # Deliberately return normally, without closing the client.

    async with server(handler) as (endpoint, requests, _):
        client = EmbeddingClient(endpoint, "", profile())
        task = asyncio.create_task(
            client.dispatch(
                prepared(),
                permitcheck=swallowing_permit,
                timeout=2 if cancel_caller else 0.05,
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            if cancel_caller:
                task.cancel()
                with pytest.raises(EmbeddingCancelled) as cancelled:
                    await asyncio.wait_for(task, 2)
                result = cancelled.value.result
                assert result.error_code == "EMBEDDING_CANCELLED"
            else:
                result = await asyncio.wait_for(task, 2)
                assert result.error_code == "EMBEDDING_TIMEOUT"
            assert result.status == "NOT_SENT" and cancellations == ["swallowed"]
            assert not requests and not client._client.is_closed and not client._tasks
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client.aclose()

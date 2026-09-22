"""Protocol normalization and real loopback HTTP failure injection (no QQ account)."""

import asyncio
import hashlib
import hmac
import json
from contextlib import asynccontextmanager

import pytest

from forest.adapters.onebot import (
    CAPABILITIES,
    MAX_EVENT_BYTES,
    MAX_RESPONSE_BYTES,
    OneBotHTTPClient,
    PreparedSend,
    verify_signature,
)
from forest.errors import ForestError


def event(**changes):
    return {
        "self_id": 10001,
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "time": 1720000000,
        "message_id": -42,
        "user_id": 20001,
        "group_id": 30001,
        "sender": {"user_id": 20001, "nickname": "小林", "card": "群名片", "role": "admin"},
        "message": [{"type": "text", "data": {"text": "群消息"}}],
        **changes,
    }


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def signature(body, secret="事件签名密钥"):
    return "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()


def test_exact_body_signature_required():
    body = encoded(event())
    valid = signature(body, secret="event-secret")
    verify_signature(body, "event-secret", valid)
    with pytest.raises(ForestError) as error:
        verify_signature(body + b" ", "event-secret", valid)
    assert error.value.code == "ONEBOT_SIGNATURE_INVALID"


def test_unsigned_configuration_and_oversized_event_are_rejected():
    with pytest.raises(ForestError) as error:
        verify_signature(b"{}", "", None)
    assert error.value.code == "ONEBOT_CONFIG_INVALID"
    with pytest.raises(ForestError) as error:
        verify_signature(b" " * (MAX_EVENT_BYTES + 1), "secret", None)
    assert error.value.status == 413


def prepare_send(channel_kind, channel_id, parts):
    action = "send_group_msg" if channel_kind == "group" else "send_private_msg"
    target = "group_id" if channel_kind == "group" else "user_id"
    message = []
    for part in parts:
        if part["type"] == "text":
            message.append({"type": "text", "data": {"text": part["text"]}})
        elif part["type"] == "mention":
            message.append({"type": "at", "data": {"qq": part["external_user_id"]}})
        elif part["type"] == "reply":
            message.append({"type": "reply", "data": {"id": part["platform_message_id"]}})
        else:
            raise AssertionError("test prepared-send helper received unsupported content")
    body = json.dumps(
        {target: int(channel_id), "message": message, "auto_escape": True},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return PreparedSend(action, body, hashlib.sha256(body).hexdigest())


def response(body=None, *, status=200, headers=(), raw_body=None):
    raw = encoded(body) if raw_body is None else raw_body
    return (
        f"HTTP/1.1 {status} Test\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n".encode()
        + b"".join(f"{name}: {value}\r\n".encode() for name, value in headers)
        + b"\r\n"
        + raw
    )


@asynccontextmanager
async def socket_api(reply):
    requests = []
    entered = asyncio.Event()
    tasks = set()

    async def handle(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            lines = header.decode("latin-1").split("\r\n")
            headers = dict(line.lower().split(": ", 1) for line in lines[1:] if ": " in line)
            body = await reader.readexactly(int(headers["content-length"]))
            requests.append((lines[0], headers, body))
            entered.set()
            if callable(reply):
                await reply(writer)
            else:
                writer.write(reply)
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}", requests, entered
    finally:
        server.close()
        await server.wait_closed()
        running = list(tasks)
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)


async def test_real_socket_sends_exact_frozen_body_and_token_once(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    reply = response({"status": "ok", "retcode": 0, "data": {"message_id": -55}})
    async with socket_api(reply) as (origin, requests, entered):
        client = OneBotHTTPClient(origin, "private-api-token")
        try:
            prepared = prepare_send(
                "group",
                "30001",
                [{"type": "text", "text": "实际 HTTP 字节"}],
            )
            result = await client.send(prepared)
        finally:
            await client.aclose()
    assert result.as_dict() == {"status": "SENT", "message_id": "-55", "error_code": None}
    assert len(requests) == 1 and entered.is_set()
    first, headers, body = requests[0]
    assert first == "POST /send_group_msg HTTP/1.1"
    assert headers["authorization"] == "bearer private-api-token"
    assert headers["content-type"] == "application/json"
    assert body == prepared.wire_body
    assert b"stable-not-on-wire" not in body


@pytest.mark.parametrize(
    "body,expected",
    [
        ({"status": "failed", "retcode": 100, "data": None}, "NOT_SENT"),
        ({"status": "async", "retcode": 1, "data": None}, "UNKNOWN"),
        ({"status": "ok", "retcode": 0, "data": {}}, "UNKNOWN"),
        ({"status": "ok", "retcode": False, "data": {"message_id": 1}}, "UNKNOWN"),
        ({"status": "failed", "retcode": 0, "data": None}, "UNKNOWN"),
        ({"status": "ok", "retcode": 0, "data": {"message_id": True}}, "UNKNOWN"),
    ],
)
async def test_socket_platform_results_preserve_uncertainty(body, expected):
    async with socket_api(response(body)) as (origin, requests, _):
        client = OneBotHTTPClient(origin, "token")
        try:
            result = await client.send(
                prepare_send("private", "20001", [{"type": "text", "text": "x"}])
            )
        finally:
            await client.aclose()
    assert result.status == expected and len(requests) == 1


@pytest.mark.parametrize(
    "status,expected",
    [
        (400, "NOT_SENT"),
        (401, "NOT_SENT"),
        (403, "NOT_SENT"),
        (404, "NOT_SENT"),
        (406, "NOT_SENT"),
        (429, "UNKNOWN"),
        (500, "UNKNOWN"),
        (503, "UNKNOWN"),
    ],
)
async def test_socket_http_statuses_do_not_create_automatic_retries(status, expected):
    async with socket_api(response({}, status=status)) as (origin, requests, _):
        client = OneBotHTTPClient(origin, "token")
        try:
            result = await client.send(
                prepare_send("group", "30001", [{"type": "text", "text": "x"}])
            )
        finally:
            await client.aclose()
    assert result.status == expected and len(requests) == 1


async def test_socket_redirect_is_never_followed_or_leaks_authorization():
    async with socket_api(response({})) as (other, other_requests, _):
        redirect = response({}, status=307, headers=[("Location", other + "/send_group_msg")])
        async with socket_api(redirect) as (origin, requests, _):
            client = OneBotHTTPClient(origin, "secret")
            try:
                result = await client.send(
                    prepare_send("group", "30001", [{"type": "text", "text": "x"}])
                )
            finally:
                await client.aclose()
    assert result.status == "UNKNOWN" and len(requests) == 1 and other_requests == []


@pytest.mark.parametrize("mode", ["disconnect", "timeout", "slow_body"])
async def test_socket_after_receiving_body_loss_is_unknown_and_one_attempt(mode):
    async def uncertain(writer):
        if mode == "disconnect":
            return
        if mode == "slow_body":
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
            await writer.drain()
            while True:
                writer.write(b" ")
                await writer.drain()
                await asyncio.sleep(0.01)
        await asyncio.Event().wait()

    async with socket_api(uncertain) as (origin, requests, _):
        client = OneBotHTTPClient(origin, "token", timeout_seconds=0.1)
        try:
            result = await client.send(
                prepare_send("group", "30001", [{"type": "text", "text": "x"}])
            )
        finally:
            await client.aclose()
    assert result.status == "UNKNOWN" and len(requests) == 1


@pytest.mark.parametrize(
    "reply",
    [
        response(raw_body=b'{"status":"ok","status":"failed","retcode":0}'),
        response(raw_body=b" " * (MAX_RESPONSE_BYTES + 1)),
        response(raw_body=b"compressed", headers=[("Content-Encoding", "gzip")]),
        response(raw_body=b"not-json"),
    ],
    ids=["duplicate-json-key", "oversized", "compressed", "malformed-json"],
)
async def test_socket_malformed_or_unbounded_response_is_unknown(reply):
    async with socket_api(reply) as (origin, requests, _):
        client = OneBotHTTPClient(origin, "token")
        try:
            result = await client.send(
                prepare_send("group", "30001", [{"type": "text", "text": "x"}])
            )
        finally:
            await client.aclose()
    assert result.status == "UNKNOWN" and len(requests) == 1


async def test_cancellation_propagates_to_durable_effect_owner_without_retry():
    async def hanging(writer):
        await asyncio.Event().wait()

    async with socket_api(hanging) as (origin, requests, entered):
        client = OneBotHTTPClient(origin, "token")
        try:
            task = asyncio.create_task(
                client.send(prepare_send("group", "30001", [{"type": "text", "text": "x"}]))
            )
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            await client.aclose()
    assert len(requests) == 1


async def test_client_close_after_socket_write_does_not_prove_message_was_not_sent():
    async def hanging(writer):
        await asyncio.Event().wait()

    async with socket_api(hanging) as (origin, requests, entered):
        client = OneBotHTTPClient(origin, "token")
        prepared = prepare_send("group", "30001", [{"type": "text", "text": "x"}])
        task = asyncio.create_task(client.send(prepared))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            await client.aclose()
            result = await asyncio.wait_for(task, 2)
            assert result.status == "UNKNOWN"
            assert (await client.send(prepared)).status == "UNKNOWN"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client.aclose()
    assert len(requests) == 1


async def test_binding_identity_probe_is_read_only_real_http():
    reply = response({"status": "ok", "retcode": 0, "data": {"user_id": 10001, "nickname": "Bot"}})
    async with socket_api(reply) as (origin, requests, _):
        client = OneBotHTTPClient(origin, "token")
        try:
            assert await client.get_login_info() == {"self_id": "10001", "nickname": "Bot"}
        finally:
            await client.aclose()
    assert requests[0][0] == "POST /get_login_info HTTP/1.1"
    assert requests[0][2] == b"{}"


async def test_identity_probe_safe_error_has_no_server_body_or_credentials():
    async with socket_api(response({"error": "private-server-data"}, status=401)) as (origin, _, _):
        client = OneBotHTTPClient(origin, "private-api-token")
        try:
            with pytest.raises(ForestError) as exc:
                await client.get_login_info()
        finally:
            await client.aclose()
    assert exc.value.code == "ONEBOT_API_UNAVAILABLE"
    assert "private" not in str(exc.value)


@pytest.mark.parametrize(
    "origin",
    [
        "file:///secret",
        "http://user:password@127.0.0.1",
        "http://127.0.0.1?token=x",
        "http://127.0.0.1/action",
        "http://127.0.0.1/#fragment",
        "http://127.0.0.1\\@evil.test",
    ],
)
def test_fixed_api_origin_rejects_credentials_paths_and_nonhttp(origin):
    with pytest.raises(ForestError):
        OneBotHTTPClient(origin, "token")


def test_capabilities_do_not_claim_platform_dedup_or_replay():
    assert CAPABILITIES == {
        "event_transport": ["http-post", "forward-websocket"],
        "api_transport": ["http", "forward-websocket"],
        "event_replay": False,
        "send_idempotency": False,
        "send_result_reconciliation": False,
        "unknown_result_retry": False,
    }

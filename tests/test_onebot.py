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
    AuthorizedImage,
    OneBotHTTPClient,
    PreparedSend,
    normalize_event,
    parse_event,
    prepare_send,
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


def parse(value):
    result = parse_event(encoded(value), self_id="10001")
    return result.as_dict() if result else None


def signature(body, secret="事件签名密钥"):
    return "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()


def test_signed_event_preserves_author_origin_and_order_without_granting_identity():
    body = encoded(
        event(
            principal_ref="forged-admin",
            message=[
                {"type": "text", "data": {"text": "这是谁的照片？"}},
                {"type": "image", "data": {"file": "a.image", "url": "https://media.test/a?key=x"}},
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "reply", "data": {"id": "-41"}},
                {"type": "face", "data": {"id": "178"}},
            ],
        )
    )
    result = normalize_event(
        body, self_id="10001", secret="事件签名密钥", signature=signature(body)
    ).as_dict()
    assert result["platform"] == "onebot-v11"
    assert (result["channel_kind"], result["channel_id"], result["thread_id"]) == (
        "group",
        "30001",
        "",
    )
    assert result["sent_at"] == "2024-07-03T09:46:40Z"
    assert result["author"] == {
        "external_user_id": "20001",
        "display_name": "群名片",
        "group_card": "群名片",
        "is_self": False,
        "role": "admin",
    }
    assert "principal_ref" not in result
    assert [part["type"] for part in result["segments"]] == [
        "text",
        "image",
        "mention",
        "reply",
        "unsupported",
    ]
    assert result["segments"][1]["media_ref"] == {
        "kind": "onebot-source",
        "uri": "https://media.test/a?key=x",
        "platform_file": "a.image",
        "state": "PENDING_CAPTURE",
    }
    assert result["segments"][2] == {"type": "mention", "external_user_id": "10001"}
    assert result["segments"][3] == {"type": "reply", "platform_message_id": "-41"}


@pytest.mark.parametrize("bad_signature", [None, "", "sha256=" + "a" * 64, "sha1=" + "0" * 40])
def test_exact_body_signature_required(bad_signature):
    body = encoded(event())
    with pytest.raises(ForestError, match="signature"):
        verify_signature(body, "事件签名密钥", bad_signature)
    with pytest.raises(ForestError, match="signature"):
        verify_signature(body + b" ", "事件签名密钥", signature(body))
    verify_signature(body, "事件签名密钥", signature(body).upper().replace("SHA1", "sha1"))


def test_unsigned_configuration_and_oversized_event_are_rejected():
    with pytest.raises(ForestError) as exc:
        verify_signature(b"{}", "", None)
    assert exc.value.code == "ONEBOT_CONFIG_INVALID"
    with pytest.raises(ForestError) as exc:
        verify_signature(b" " * (MAX_EVENT_BYTES + 1), "secret", None)
    assert exc.value.status == 413


@pytest.mark.parametrize("post_type", ["message", "notice", "request", "meta_event"])
def test_wrong_binding_identity_never_reaches_even_nonmessage_processing(post_type):
    with pytest.raises(ForestError) as exc:
        parse(event(self_id=10002, post_type=post_type))
    assert exc.value.code == "ONEBOT_IDENTITY_MISMATCH"
    if post_type != "message":
        assert parse(event(post_type=post_type)) is None


@pytest.mark.parametrize(
    "message",
    [
        "&#91;CQ:at,qq=10001&#93;",
        "&amp;#91;CQ:at,qq=10001&amp;#93;",
        "[CQ:at,qq=10001,qq=20001]",
        "[CQ:bad,[CQ:at,qq=10001]]",
        "[CQ:at, qq=10001]",
        [{"type": "text", "data": {"text": "[CQ:at,qq=10001]"}}],
    ],
)
def test_escaped_malformed_and_array_text_cannot_forge_a_mention(message):
    result = parse(event(message=message))
    assert all(part["type"] == "text" for part in result["segments"])


def test_cq_entities_are_decoded_once_in_correct_context():
    result = parse(
        event(
            message=(
                "&amp;#91;literal&#44;[CQ:at,qq=10001]"
                "[CQ:share,title=a&#44;b&amp;#91;c,url=https://x.test/?a=b=c&amp;d=e]"
            )
        )
    )
    assert result["segments"][0] == {"type": "text", "text": "&#91;literal&#44;"}
    assert result["segments"][1] == {"type": "mention", "external_user_id": "10001"}
    assert result["segments"][2]["data"] == {
        "title": "a,b&#91;c",
        "url": "https://x.test/?a=b=c&d=e",
    }
    raw = "&#91;CQ:at,qq=10001&#93;"
    assert (
        parse(event(message=[{"type": "text", "data": {"text": raw}}]))["segments"][0]["text"]
        == raw
    )


def test_private_all_messages_and_anonymous_snapshot_do_not_depend_on_trigger():
    private = parse(event(message_type="private", message="", sender={}, group_id=None))
    assert private["channel_id"] == "20001" and private["segments"] == []
    assert private["author"]["display_name"] == "20001"
    anonymous = parse(event(anonymous={"id": 55, "name": "匿名者", "flag": "not-context-secret"}))
    assert anonymous["author"]["anonymous"] == {"id": "55", "name": "匿名者"}
    assert "not-context-secret" not in json.dumps(anonymous)
    assert parse(event(sub_type="anonymous"))["author"]["anonymous"]["id"] is None
    assert parse(event(user_id=10001, sender={"user_id": 10001}))["author"]["is_self"]


@pytest.mark.parametrize(
    "changes",
    [
        {"self_id": True},
        {"self_id": []},
        {"self_id": "010001"},
        {"post_type": []},
        {"message_type": {}},
        {"time": True},
        {"time": 2**63 - 1},
        {"message_id": 2**31},
        {"user_id": -1},
        {"sender": {"user_id": 20002}},
        {"message": [{"type": "text", "data": {"text": 10}}]},
        {"message": [{"type": "text", "data": {"text": "x"}}] * 257},
    ],
)
def test_invalid_protocol_fields_have_safe_bounded_errors(changes):
    with pytest.raises(ForestError):
        parse(event(**changes))


@pytest.mark.parametrize(
    "body",
    [
        b'{"self_id":10001,"self_id":10002}',
        b'{"self_id":10001,"x":NaN}',
        b"[" * 40 + b"0" + b"]" * 40,
        b'{"self_id":10001,"x":"\\ud800"}',
        '{"self_id":10001}'.encode("utf-16"),
    ],
)
def test_json_duplicate_depth_nonfinite_unicode_are_rejected(body):
    with pytest.raises(ForestError):
        parse_event(body, self_id="10001")


def test_normalized_message_cannot_be_changed_through_returned_dict():
    result = parse_event(encoded(event()), self_id="10001")
    result.as_dict()["author"]["external_user_id"] = "attacker"
    assert result.as_dict()["author"]["external_user_id"] == "20001"


def test_prepared_bytes_are_stable_literal_and_bound_to_action():
    parts = [
        {"type": "text", "text": "[CQ:image,file=file:///secret] 中文"},
        {"type": "mention", "external_user_id": "20001"},
        {"type": "reply", "platform_message_id": "-42"},
    ]
    prepared = prepare_send("group", "30001", parts)
    parts[0]["text"] = "changed"
    assert prepared.action == "send_group_msg"
    assert hashlib.sha256(prepared.wire_body).hexdigest() == prepared.wire_digest
    body = json.loads(prepared.wire_body)
    assert body["auto_escape"] is True and body["group_id"] == 30001
    assert body["message"][0] == {
        "type": "text",
        "data": {"text": "[CQ:image,file=file:///secret] 中文"},
    }
    with pytest.raises(ForestError):
        PreparedSend("delete_msg", prepared.wire_body, prepared.wire_digest)
    with pytest.raises(ForestError):
        PreparedSend("send_group_msg", prepared.wire_body + b" ", prepared.wire_digest)
    with pytest.raises(ForestError):
        PreparedSend("send_private_msg", prepared.wire_body, prepared.wire_digest)


@pytest.mark.parametrize(
    "uri",
    [
        "file:///C:/secret",
        "C:\\secret",
        "ftp://x.test/a",
        "https://u:pw@x.test/a",
        "https://x.test/a\r\nAuthorization: secret",
        "//x.test/a",
    ],
)
def test_outgoing_media_requires_approved_http_or_bytes_and_never_host_paths(uri):
    with pytest.raises(ForestError):
        prepare_send("group", "30001", [AuthorizedImage(uri=uri)])
    with pytest.raises(ForestError):
        prepare_send("group", "30001", [{"type": "image", "media_ref": {"uri": uri}}])


def test_authorized_image_serializes_bytes_without_opening_a_file_or_url():
    prepared = prepare_send("private", "20001", [AuthorizedImage(content=b"image bytes")])
    data = json.loads(prepared.wire_body)
    assert data["message"][0]["data"] == {
        "file": "base64://aW1hZ2UgYnl0ZXM=",
        "proxy": "0",
        "timeout": "10",
    }
    with pytest.raises(ForestError):
        prepare_send("private", "20001", [{"type": "mention", "external_user_id": "all"}])


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
            prepared = client.prepare(
                {"channel_kind": "group", "channel_id": "30001"},
                [{"type": "text", "text": "实际 HTTP 字节"}],
                operation_id="stable-not-on-wire",
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

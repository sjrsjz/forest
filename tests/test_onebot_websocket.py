from __future__ import annotations

import asyncio
import hashlib
import json

import pytest
from websockets.asyncio.server import serve

from forest.adapters import onebot

_END = object()


def prepared_text(text: str) -> onebot.PreparedSend:
    body = json.dumps(
        {
            "group_id": 933179609,
            "message": [{"type": "text", "data": {"text": text}}],
            "auto_escape": True,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return onebot.PreparedSend("send_group_msg", body, hashlib.sha256(body).hexdigest())


class FakeSocket:
    def __init__(self) -> None:
        self.incoming = asyncio.Queue()
        self.sent = []
        self.sent_event = asyncio.Event()
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.incoming.get()
        if value is _END:
            raise StopAsyncIteration
        return value

    async def send(self, wire):
        value = json.loads(wire)
        self.sent.append(value)
        self.sent_event.set()
        if value["action"] == "get_login_info":
            await self.incoming.put(
                json.dumps(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"user_id": 123456789, "nickname": "Forest"},
                        "echo": value["echo"],
                    },
                    separators=(",", ":"),
                )
            )
        elif value["action"] == "get_status":
            await self.incoming.put(
                json.dumps(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"online": True, "good": True},
                        "echo": value["echo"],
                    },
                    separators=(",", ":"),
                )
            )

    async def close(self, **_kwargs):
        if not self.closed:
            self.closed = True
            await self.incoming.put(_END)

    async def remote_disconnect(self):
        await self.incoming.put(_END)


async def create_client(monkeypatch, *, event_capacity=256):
    socket = FakeSocket()

    async def connect(*_args, **_kwargs):
        return socket

    monkeypatch.setattr(onebot, "_FixedEndpointConnect", connect)
    client = await onebot.OneBotWebSocketClient.create(
        "ws://onebot.invalid/",
        "test-token",
        event_capacity=event_capacity,
    )
    identity = await client.get_login_info()
    assert identity == {"self_id": "123456789", "nickname": "Forest"}
    assert await client.get_status() == {"online": True, "good": True}
    socket.sent_event.clear()
    return client, socket


async def eventually(predicate, *, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


async def test_group_discovery_uses_existing_socket_and_its_own_response_bound(monkeypatch):
    client, socket = await create_client(monkeypatch)
    rows = [{"group_id": 100000 + i, "group_name": "群" * 60} for i in range(600)]

    async def respond(wire):
        request = json.loads(wire)
        assert request["action"] == "get_group_list" and request["params"] == {}
        await socket.incoming.put(json.dumps({
            "status": "ok", "retcode": 0, "data": rows, "echo": request["echo"],
        }, ensure_ascii=False))

    socket.send = respond
    try:
        groups = await client.get_group_list()
        assert len(groups) == 600 and groups[0] == {"group_id": "100000", "group_name": "群" * 60}
        assert not client.is_closed
        rows.append(rows[0])
        with pytest.raises(onebot.ForestError, match="group list"):
            await client.get_group_list()
        assert not client.is_closed
        assert not client._pending and not client._response_limits
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_sustained_event_stream_is_buffered_then_consumed_once_in_order(monkeypatch):
    client, socket = await create_client(monkeypatch, event_capacity=1200)
    received = []
    complete = asyncio.Event()
    total = 1000
    try:
        for index in range(total):
            await socket.incoming.put(
                json.dumps(
                    {"post_type": "message", "message_id": index},
                    separators=(",", ":"),
                )
            )
        await eventually(lambda: client._events.qsize() == total)

        async def consume(body):
            received.append(json.loads(body)["message_id"])
            if len(received) == total:
                complete.set()

        client.activate_events(consume)
        await asyncio.wait_for(complete.wait(), timeout=3)
        assert received == list(range(total))
        assert client.failure_code is None
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_event_capacity_failure_is_explicit_and_finishes_buffer(monkeypatch):
    client, socket = await create_client(monkeypatch, event_capacity=2)
    received = []
    try:
        for index in range(3):
            await socket.incoming.put(
                json.dumps(
                    {"post_type": "message", "message_id": index},
                    separators=(",", ":"),
                )
            )
        await eventually(lambda: client.failure_code is not None)
        assert client.failure_code == "ONEBOT_WEBSOCKET_BACKPRESSURE"
        assert client.is_closed

        client.activate_events(lambda body: _record(received, body))
        await asyncio.wait_for(client._consumer_task, timeout=3)
        assert received == [0, 1]
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_event_admission_freezes_at_handler_boundary_and_resumes_in_order(monkeypatch):
    client, socket = await create_client(monkeypatch)
    received = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def consume(body):
        value = json.loads(body)["message_id"]
        if value == 1:
            first_started.set()
            await release_first.wait()
        received.append(value)

    try:
        client.activate_events(consume)
        await socket.incoming.put(json.dumps({"post_type": "message", "message_id": 1}))
        await asyncio.wait_for(first_started.wait(), timeout=3)
        freezing = asyncio.create_task(client.freeze_events())
        await asyncio.sleep(0)
        assert not freezing.done()
        release_first.set()
        await asyncio.wait_for(freezing, timeout=3)
        assert received == [1]

        await socket.incoming.put(json.dumps({"post_type": "message", "message_id": 2}))
        await eventually(lambda: client._events.qsize() == 0)
        await asyncio.sleep(0.01)
        assert received == [1]
        client.resume_events()
        await eventually(lambda: received == [1, 2])
    finally:
        await client.aclose()


async def _record(received, body):
    received.append(json.loads(body)["message_id"])


@pytest.mark.asyncio
async def test_disconnect_after_action_handoff_returns_unknown_and_exposes_owned_failure(
    monkeypatch,
):
    client, socket = await create_client(monkeypatch)
    try:
        prepared = prepared_text("test")
        sending = asyncio.create_task(client.send(prepared, timeout=3))
        await asyncio.wait_for(socket.sent_event.wait(), timeout=3)
        await socket.remote_disconnect()
        result = await asyncio.wait_for(sending, timeout=3)
        assert result.status == "UNKNOWN"
        assert result.error_code == "ONEBOT_WEBSOCKET_CLOSED"
        await asyncio.wait_for(client._reader_task, timeout=3)
        assert client.failure_code == "ONEBOT_WEBSOCKET_CLOSED"
        assert client._reader_task in client.owned_tasks()
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_real_websocket_disconnect_becomes_generation_owned_failure():
    connected = asyncio.get_running_loop().create_future()

    async def handler(socket):
        connected.set_result(socket)
        async for wire in socket:
            value = json.loads(wire)
            if value["action"] == "get_login_info":
                await socket.send(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {"user_id": 123456789, "nickname": "Forest"},
                            "echo": value["echo"],
                        },
                        separators=(",", ":"),
                    )
                )

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = await onebot.OneBotWebSocketClient.create(
            f"ws://127.0.0.1:{port}/", "test-token"
        )
        try:
            identity = await client.get_login_info()
            assert identity["self_id"] == "123456789"
            client.activate_events(lambda _body: asyncio.sleep(0))
            socket = await asyncio.wait_for(connected, timeout=3)
            await socket.close(code=1012, reason="fixture restart")
            await asyncio.wait_for(client._reader_task, timeout=3)
            assert client.failure_code == "ONEBOT_WEBSOCKET_CLOSED"
            assert client.is_closed
        finally:
            await client.aclose()

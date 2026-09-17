import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from websockets.asyncio.server import serve

from forest.adapters.onebot import OneBotWebSocketClient, prepare_send
from forest.errors import ForestError
from forest.management.onebot_contract import BindingValues


def event(message_id: int, text: str = "hello") -> dict:
    return {
        "time": 1_700_000_000 + message_id,
        "self_id": 10001,
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "message_id": message_id,
        "user_id": 20001,
        "group_id": 933179609,
        "message": [{"type": "text", "data": {"text": text}}],
        "sender": {"user_id": 20001, "nickname": "member", "role": "member"},
    }


@asynccontextmanager
async def onebot_ws(*, delay_send: float = 0):
    state = {"connection": None, "actions": [], "headers": None, "path": None}
    connected = asyncio.Event()

    async def handler(connection):
        state["connection"] = connection
        state["headers"] = connection.request.headers
        state["path"] = connection.request.path
        connected.set()
        async for raw in connection:
            request = json.loads(raw)
            state["actions"].append(request)
            if request["action"] == "get_login_info":
                data = {"user_id": 10001, "nickname": "Live-compatible"}
            elif request["action"] in {"send_group_msg", "send_private_msg"}:
                if delay_send:
                    await asyncio.sleep(delay_send)
                data = {"message_id": -42}
            else:
                await connection.send(
                    json.dumps(
                        {
                            "status": "failed",
                            "retcode": 1404,
                            "data": None,
                            "echo": request["echo"],
                        }
                    )
                )
                continue
            await connection.send(
                json.dumps(
                    {"status": "ok", "retcode": 0, "data": data, "echo": request["echo"]}
                )
            )

    async with serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        yield "ws://127.0.0.1:" + str(port) + "/", state, connected


async def wait_until(read, timeout=2):
    async with asyncio.timeout(timeout):
        while not read():
            await asyncio.sleep(0.01)


async def test_forward_websocket_auth_identity_buffered_events_and_send_once():
    async with onebot_ws() as (url, state, connected):
        client = await OneBotWebSocketClient.create(url, "private-test-token")
        received = []
        try:
            await connected.wait()
            assert await client.get_login_info() == {
                "self_id": "10001",
                "nickname": "Live-compatible",
            }
            await state["connection"].send(json.dumps(event(1, "first")))
            await state["connection"].send(json.dumps(event(2, "second")))
            await asyncio.sleep(0.05)
            assert received == []

            async def consume(body):
                received.append(json.loads(body))

            client.activate_events(consume)
            await wait_until(lambda: len(received) == 2)
            prepared = prepare_send(
                "group", "933179609", [{"type": "text", "text": "single test"}]
            )
            assert (await client.send(prepared)).as_dict() == {
                "status": "SENT",
                "message_id": "-42",
                "error_code": None,
            }
        finally:
            await client.aclose()

    assert state["path"] == "/"
    assert state["headers"]["authorization"] == "Bearer private-test-token"
    assert [item["message_id"] for item in received] == [1, 2]
    assert [item["action"] for item in state["actions"]] == [
        "get_login_info",
        "send_group_msg",
    ]
    assert state["actions"][1]["params"] == json.loads(prepared.wire_body)


async def test_late_echo_after_timeout_is_ignored_without_retrying_action():
    async with onebot_ws(delay_send=0.08) as (url, state, _):
        client = await OneBotWebSocketClient.create(url, "token", timeout_seconds=0.02)
        try:
            result = await client.send(
                prepare_send("group", "933179609", [{"type": "text", "text": "once"}])
            )
            assert result.status == "UNKNOWN"
            assert result.error_code == "ONEBOT_WEBSOCKET_TIMEOUT"
            await asyncio.sleep(0.12)
            assert not client.is_closed
            assert await client.get_login_info(timeout=0.5) == {
                "self_id": "10001",
                "nickname": "Live-compatible",
            }
        finally:
            await client.aclose()
    assert [item["action"] for item in state["actions"]].count("send_group_msg") == 1


async def test_event_queue_overflow_fails_candidate_before_activation():
    async with onebot_ws() as (url, state, connected):
        client = await OneBotWebSocketClient.create(url, "token", event_capacity=1)
        try:
            await connected.wait()
            await state["connection"].send(json.dumps(event(1)))
            await state["connection"].send(json.dumps(event(2)))
            await wait_until(lambda: client.is_closed)
            assert client.failure_code == "ONEBOT_WEBSOCKET_BACKPRESSURE"
        finally:
            await client.aclose()


async def test_closed_before_dispatch_is_not_sent():
    async with onebot_ws() as (url, state, _):
        client = await OneBotWebSocketClient.create(url, "token")
        await client.aclose()
        result = await client.send(
            prepare_send("group", "933179609", [{"type": "text", "text": "never"}])
        )
    assert result.status == "NOT_SENT"
    assert state["actions"] == []


async def test_interleaved_event_and_out_of_order_action_responses_use_single_reader():
    state = {"connection": None, "requests": []}
    connected = asyncio.Event()

    async def handler(connection):
        state["connection"] = connection
        connected.set()
        first = json.loads(await connection.recv())
        second = json.loads(await connection.recv())
        state["requests"] = [first, second]
        await connection.send(json.dumps(event(9, "between responses")))
        for request, message_id in ((second, -82), (first, -81)):
            await connection.send(
                json.dumps(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": message_id},
                        "echo": request["echo"],
                    }
                )
            )
        await connection.wait_closed()

    async with serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        client = await OneBotWebSocketClient.create(f"ws://127.0.0.1:{port}/", "token")
        received = []

        async def consume(body):
            received.append(json.loads(body)["message_id"])

        client.activate_events(consume)
        try:
            await connected.wait()

            async def call(index):
                text = f"message {index}"
                result = await client.send(
                    prepare_send(
                        "group",
                        "933179609",
                        [{"type": "text", "text": text}],
                    )
                )
                return text, result

            calls = [
                asyncio.create_task(call(index))
                for index in range(2)
            ]
            results = await asyncio.gather(*calls)
            await wait_until(lambda: received == [9])
        finally:
            await client.aclose()
    by_text = {text: result.message_id for text, result in results}
    first_text = state["requests"][0]["params"]["message"][0]["data"]["text"]
    second_text = state["requests"][1]["params"]["message"][0]["data"]["text"]
    assert by_text[first_text] == "-81"
    assert by_text[second_text] == "-82"


async def test_close_settles_dispatched_pending_action_as_unknown():
    state = {"requests": []}
    dispatched = asyncio.Event()

    async def handler(connection):
        request = json.loads(await connection.recv())
        state["requests"].append(request)
        dispatched.set()
        await connection.wait_closed()

    async with serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        client = await OneBotWebSocketClient.create(f"ws://127.0.0.1:{port}/", "token")
        task = asyncio.create_task(
            client.send(
                prepare_send("group", "933179609", [{"type": "text", "text": "once"}])
            )
        )
        await dispatched.wait()
        await client.aclose()
        result = await task
    assert result.status == "UNKNOWN"
    assert len(state["requests"]) == 1


async def test_websocket_redirect_is_not_followed_or_given_authorization():
    async with onebot_ws() as (target_url, target, _):
        requests = []

        async def redirect(reader, writer):
            header = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            requests.append(header)
            response = (
                "HTTP/1.1 307 Temporary Redirect\r\n"
                + "Location: "
                + target_url
                + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            writer.write(response.encode("ascii"))
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(redirect, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            with pytest.raises(ForestError, match="Could not connect"):
                await OneBotWebSocketClient.create(
                    f"ws://127.0.0.1:{port}/", "private-redirect-token"
                )
            await asyncio.sleep(0.05)
        finally:
            server.close()
            await server.wait_closed()
    assert len(requests) == 1
    assert "Authorization: Bearer private-redirect-token\r\n" in requests[0]
    assert target["connection"] is None


def test_binding_values_keep_http_compatibility_and_canonicalize_websocket():
    old = BindingValues(enabled=True, api_base_url="http://127.0.0.1:5700")
    assert old.transport == "http-post"
    assert old.websocket_url == "ws://127.0.0.1:8080/"
    websocket = BindingValues(
        enabled=True,
        transport="forward-websocket",
        websocket_url="wss://onebot.example",
    )
    assert websocket.websocket_url == "wss://onebot.example/"


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8080/",
        "ws://user:secret@127.0.0.1:8080/",
        "ws://127.0.0.1:8080/api",
        "ws://127.0.0.1:8080/?token=secret",
        "ws://127.0.0.1:8080/#fragment",
    ],
)
def test_forward_websocket_requires_fixed_universal_endpoint(url):
    with pytest.raises(ForestError):
        OneBotWebSocketClient(url, "token")

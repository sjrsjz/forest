"""Host-only OneBot transports; the pure codec ships in the immutable plugin."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from forest.builtin.onebot.python.onebot_codec import CAPABILITIES as CAPABILITIES
from forest.builtin.onebot.python.onebot_codec import MAX_EVENT_BYTES as MAX_EVENT_BYTES
from forest.builtin.onebot.python.onebot_codec import (
    MAX_RESPONSE_BYTES,
    _encode,
    _http_url,
    _id,
    _invalid,
    _json,
    _string,
)
from forest.builtin.onebot.python.onebot_codec import MAX_SEGMENTS as MAX_SEGMENTS
from forest.builtin.onebot.python.onebot_codec import MAX_SEND_BYTES as MAX_SEND_BYTES
from forest.builtin.onebot.python.onebot_codec import PLATFORM as PLATFORM
from forest.builtin.onebot.python.onebot_codec import AuthorizedImage as AuthorizedImage
from forest.builtin.onebot.python.onebot_codec import NormalizedMessage as NormalizedMessage
from forest.builtin.onebot.python.onebot_codec import PreparedSend as PreparedSend
from forest.builtin.onebot.python.onebot_codec import normalize_event as normalize_event
from forest.builtin.onebot.python.onebot_codec import parse_event as parse_event
from forest.builtin.onebot.python.onebot_codec import prepare_send as prepare_send
from forest.builtin.onebot.python.onebot_codec import verify_signature as verify_signature
from forest.errors import ForestError

MAX_WS_FRAME_BYTES = MAX_EVENT_BYTES + 4096
MAX_WS_EVENTS = 256
MAX_WS_PENDING = 64


class _FixedEndpointConnect(connect):
    """Disable library redirects so the Bearer credential stays on one origin."""

    def process_redirect(self, exc: Exception) -> Exception | str:
        return exc


@dataclass(frozen=True)
class SendResult:
    status: Literal["SENT", "NOT_SENT", "UNKNOWN"]
    message_id: str | None = None
    error_code: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.status, "message_id": self.message_id, "error_code": self.error_code}


class OneBotHTTPClient:
    """A host-owned fixed-origin HTTP API client, with no automatic retries.

    Cancellation propagates: the durable effect owner must settle it as UNKNOWN
    whenever a dispatch permit was already issued. A network exception also
    cannot prove absence of a platform side effect, even at connect time.
    """

    def __init__(
        self,
        api_base_url: str,
        access_token: str,
        *,
        timeout_seconds: float = 15,
        max_connections: int = 16,
    ) -> None:
        try:
            self.api_base_url = str(httpx.URL(_http_url(api_base_url, origin_only=True))).rstrip("/")
        except httpx.InvalidURL:
            raise _invalid("Invalid HTTP endpoint") from None
        if (
            not isinstance(access_token, str)
            or not 1 <= len(access_token) <= 4096
            or any(ord(char) < 33 or ord(char) > 126 for char in access_token)
        ):
            raise ForestError("ONEBOT_CONFIG_INVALID", "A valid API access token is required", 400)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds <= 120
            or isinstance(max_connections, bool)
            or not isinstance(max_connections, int)
            or not 1 <= max_connections <= 64
        ):
            raise _invalid("Invalid transport limits")
        self.timeout_seconds = timeout_seconds
        limits = httpx.Limits(
            max_connections=max_connections, max_keepalive_connections=max_connections
        )
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False),
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(timeout_seconds),
            headers={"Authorization": "Bearer " + access_token, "Accept-Encoding": "identity"},
        )

    @property
    def is_closed(self) -> bool:
        return self._client.is_closed

    def prepare(
        self,
        route: dict[str, Any],
        parts: list[dict[str, Any] | AuthorizedImage],
        operation_id: str | None = None,
    ) -> PreparedSend:
        return prepare_send(route["channel_kind"], route["channel_id"], parts)

    async def send(self, prepared: PreparedSend, *, timeout: float | None = None) -> SendResult:
        if not isinstance(prepared, PreparedSend):
            raise _invalid("A frozen prepared send is required")
        timeout = self._timeout(timeout)
        # The outer deadline bounds pool wait, connect, write, and all body chunks
        # together, including a slow response that never trips an idle timeout.
        try:
            status_code, body = await self._post(prepared.action, prepared.wire_body, timeout)
            if status_code in {400, 401, 403, 404, 406}:
                return SendResult("NOT_SENT", error_code="HTTP_REJECTED")
            if status_code != 200:
                return SendResult("UNKNOWN", error_code="HTTP_UNCERTAIN")
            return _send_result(body)
        except (TimeoutError, httpx.HTTPError, OSError):
            return SendResult("UNKNOWN", error_code="TRANSPORT_UNCERTAIN")
        except RuntimeError:
            if self._client.is_closed:
                return SendResult("UNKNOWN", error_code="TRANSPORT_CLOSED")
            raise
        except ForestError as exc:
            return SendResult("UNKNOWN", error_code=exc.code)

    def _timeout(self, timeout: float | None) -> float:
        value = self.timeout_seconds if timeout is None else timeout
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value <= 120:
            raise _invalid("Invalid effect timeout")
        return value

    async def _post(self, action: str, body: bytes, timeout: float) -> tuple[int, bytes]:
        async with asyncio.timeout(timeout):
            async with self._client.stream(
                "POST",
                self.api_base_url + "/" + action,
                content=body,
                headers={"Content-Type": "application/json"},
                timeout=timeout,
            ) as response:
                if response.status_code != 200:
                    return response.status_code, b""
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ForestError("RESPONSE_ENCODING", "Unsupported API response encoding", 502)
                result = bytearray()
                async for chunk in response.aiter_raw(chunk_size=8192):
                    result.extend(chunk)
                    if len(result) > MAX_RESPONSE_BYTES:
                        raise ForestError(
                            "RESPONSE_TOO_LARGE", "API response exceeds byte limit", 502
                        )
                return response.status_code, bytes(result)

    async def get_login_info(self, *, timeout: float | None = None) -> dict[str, str]:
        """Read the actual API account before publishing a binding candidate."""
        timeout = self._timeout(timeout)
        try:
            status, body = await self._post("get_login_info", b"{}", timeout)
            if status != 200:
                raise _invalid()
            result = _json(body, MAX_RESPONSE_BYTES)
            if (
                not isinstance(result, dict)
                or result.get("status") != "ok"
                or type(result.get("retcode")) is not int
                or result["retcode"] != 0
                or not isinstance(result.get("data"), dict)
            ):
                raise _invalid()
            data = result["data"]
            return {"self_id": _id(data.get("user_id")), "nickname": _string(data.get("nickname"))}
        except (TimeoutError, httpx.HTTPError, OSError, ForestError, RuntimeError):
            raise ForestError(
                "ONEBOT_API_UNAVAILABLE", "Could not verify the OneBot API account", 502
            ) from None

    async def aclose(self) -> None:
        await self._client.aclose()


class _ActionError(Exception):
    def __init__(self, code: str, *, uncertain: bool) -> None:
        super().__init__(code)
        self.code = code
        self.uncertain = uncertain


def _websocket_url(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 8192:
        raise _invalid("Invalid WebSocket endpoint")
    if any(ord(char) <= 32 or ord(char) == 127 for char in value) or "\\" in value:
        raise _invalid("Invalid WebSocket endpoint")
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"ws", "wss"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or (parsed.port is not None and not 1 <= parsed.port <= 65535)
        ):
            raise ValueError("Invalid universal endpoint")
        parsed.hostname.encode("idna")
    except (ValueError, UnicodeError):
        raise _invalid("Invalid WebSocket endpoint") from None
    return value.rstrip("/") + "/"


class OneBotWebSocketClient:
    """OneBot v11 forward Universal WebSocket owned by the host.

    Exactly one task reads the socket. It routes action responses by an opaque
    echo and feeds raw event bytes to one ordered consumer after activation.
    No action is retried automatically.
    """

    def __init__(
        self,
        websocket_url: str,
        access_token: str,
        *,
        timeout_seconds: float = 15,
        event_capacity: int = MAX_WS_EVENTS,
        pending_capacity: int = MAX_WS_PENDING,
    ) -> None:
        self.websocket_url = _websocket_url(websocket_url)
        if (
            not isinstance(access_token, str)
            or not 1 <= len(access_token) <= 4096
            or any(ord(char) < 33 or ord(char) > 126 for char in access_token)
        ):
            raise ForestError("ONEBOT_CONFIG_INVALID", "A valid API access token is required", 400)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds <= 120
            or isinstance(event_capacity, bool)
            or not isinstance(event_capacity, int)
            or not 1 <= event_capacity <= 4096
            or isinstance(pending_capacity, bool)
            or not isinstance(pending_capacity, int)
            or not 1 <= pending_capacity <= 256
        ):
            raise _invalid("Invalid transport limits")
        self.timeout_seconds = float(timeout_seconds)
        self._headers = {"Authorization": "Bearer " + access_token}
        self._event_capacity = event_capacity
        # One additional slot is reserved for the terminal sentinel.
        self._events: asyncio.Queue[bytes | None] = asyncio.Queue(event_capacity + 1)
        self._pending_slots = asyncio.Semaphore(pending_capacity)
        self._pending: dict[str, asyncio.Future[bytes]] = {}
        self._expired: set[str] = set()
        self._expired_order: list[str] = []
        self._event_handler: Callable[[bytes], Awaitable[None]] | None = None
        self._socket: ClientConnection | None = None
        self._reader_task: asyncio.Task | None = None
        self._consumer_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._activation = asyncio.Event()
        self._closed = asyncio.Event()
        self._events_finished = False
        self._closing = False
        self._failure_code: str | None = None
        self._close_task: asyncio.Task | None = None

    @classmethod
    async def create(cls, websocket_url: str, access_token: str, **kwargs):
        client = cls(websocket_url, access_token, **kwargs)
        await client._open()
        return client

    @property
    def is_closed(self) -> bool:
        return self._closed.is_set()

    @property
    def failure_code(self) -> str | None:
        return self._failure_code

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        """Expose transport tasks to the owning runtime Generation."""

        return tuple(
            task
            for task in (self._reader_task, self._consumer_task, self._close_task)
            if task is not None
        )

    async def _open(self) -> None:
        try:
            self._socket = await _FixedEndpointConnect(
                self.websocket_url,
                additional_headers=self._headers,
                proxy=None,
                compression=None,
                open_timeout=min(max(self.timeout_seconds, 1), 15),
                close_timeout=3,
                ping_interval=20,
                ping_timeout=10,
                max_size=MAX_WS_FRAME_BYTES,
                max_queue=16,
                write_limit=64 * 1024,
            )
        except (TimeoutError, OSError, InvalidHandshake, ConnectionClosed):
            self._terminate("ONEBOT_WEBSOCKET_UNAVAILABLE")
            raise ForestError(
                "ONEBOT_API_UNAVAILABLE", "Could not connect to the OneBot WebSocket", 502
            ) from None
        self._reader_task = asyncio.create_task(self._read(), name="onebot-ws-reader")
        self._consumer_task = asyncio.create_task(self._consume(), name="onebot-ws-events")

    def activate_events(self, handler: Callable[[bytes], Awaitable[None]]) -> None:
        if self._event_handler is not None or self._activation.is_set():
            raise ForestError("ONEBOT_WEBSOCKET_STATE", "WebSocket events are already active", 409)
        if not callable(handler):
            raise ForestError("ONEBOT_WEBSOCKET_STATE", "WebSocket cannot accept events", 409)
        self._event_handler = handler
        self._activation.set()
        if self.is_closed:
            self._finish_events()

    def _finish_events(self) -> None:
        if not self._events_finished:
            self._events_finished = True
            self._events.put_nowait(None)

    def _terminate(self, code: str) -> None:
        if self._failure_code is None and not self._closing:
            self._failure_code = code
        self._closed.set()
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(_ActionError(code, uncertain=True))

    async def _read(self) -> None:
        assert self._socket is not None
        code = "ONEBOT_WEBSOCKET_CLOSED"
        try:
            async for frame in self._socket:
                if not isinstance(frame, str):
                    raise ForestError("ONEBOT_WEBSOCKET_PROTOCOL", "Binary frame is unsupported", 502)
                try:
                    body = frame.encode("utf-8")
                except UnicodeError:
                    raise ForestError(
                        "ONEBOT_WEBSOCKET_PROTOCOL", "Frame is not valid UTF-8", 502
                    ) from None
                value = _json(body, MAX_WS_FRAME_BYTES)
                if not isinstance(value, dict):
                    raise ForestError(
                        "ONEBOT_WEBSOCKET_PROTOCOL", "Frame must be a JSON object", 502
                    )
                if "echo" in value:
                    echo = value["echo"]
                    if not isinstance(echo, str):
                        raise ForestError(
                            "ONEBOT_WEBSOCKET_PROTOCOL", "Action echo must be a string", 502
                        )
                    future = self._pending.get(echo)
                    if future is not None:
                        if future.done() or len(body) > MAX_RESPONSE_BYTES:
                            raise ForestError(
                                "ONEBOT_WEBSOCKET_PROTOCOL", "Invalid action response", 502
                            )
                        future.set_result(body)
                        continue
                    if echo in self._expired:
                        self._expired.remove(echo)
                        continue
                    raise ForestError(
                        "ONEBOT_WEBSOCKET_PROTOCOL", "Unknown action echo", 502
                    )
                if "post_type" not in value or len(body) > MAX_EVENT_BYTES:
                    raise ForestError(
                        "ONEBOT_WEBSOCKET_PROTOCOL", "Unknown or oversized event frame", 502
                    )
                if self._events.qsize() >= self._event_capacity:
                    raise ForestError(
                        "ONEBOT_WEBSOCKET_BACKPRESSURE", "Event queue capacity was exceeded", 503
                    )
                self._events.put_nowait(body)
        except asyncio.CancelledError:
            code = "ONEBOT_WEBSOCKET_CLOSED"
            raise
        except ForestError as exc:
            code = exc.code
        except (ConnectionClosed, OSError, RuntimeError):
            code = "ONEBOT_WEBSOCKET_CLOSED"
        except Exception:
            code = "ONEBOT_WEBSOCKET_FAILED"
        finally:
            self._terminate(code)
            if not self._closing and self._socket is not None:
                await self._socket.close(code=1002, reason="transport stopped")
            if self._activation.is_set():
                self._finish_events()

    async def _consume(self) -> None:
        try:
            await self._activation.wait()
            handler = self._event_handler
            if handler is None:
                return
            while True:
                body = await self._events.get()
                try:
                    if body is None:
                        return
                    await handler(body)
                finally:
                    self._events.task_done()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._terminate("ONEBOT_WEBSOCKET_INGRESS_FAILED")
            if self._socket is not None:
                await self._socket.close(code=1011, reason="ingress failed")

    def _remember_expired(self, echo: str) -> None:
        self._expired.add(echo)
        self._expired_order.append(echo)
        if len(self._expired_order) > MAX_WS_PENDING * 4:
            old = self._expired_order.pop(0)
            self._expired.discard(old)

    async def _request(self, action: str, params: dict[str, Any], timeout: float) -> bytes:
        if self.is_closed or self._socket is None:
            raise _ActionError("ONEBOT_WEBSOCKET_CLOSED", uncertain=False)
        echo = "forest:" + str(uuid4())
        future: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        dispatched = False
        acquired = False
        try:
            async with asyncio.timeout(timeout):
                await self._pending_slots.acquire()
                acquired = True
                if self.is_closed or self._socket is None:
                    raise _ActionError("ONEBOT_WEBSOCKET_CLOSED", uncertain=False)
                self._pending[echo] = future
                wire = _encode(
                    {"action": action, "params": params, "echo": echo},
                    MAX_SEND_BYTES + 4096,
                ).decode("utf-8")
                async with self._send_lock:
                    if self.is_closed or self._socket is None:
                        raise _ActionError("ONEBOT_WEBSOCKET_CLOSED", uncertain=False)
                    dispatched = True
                    await self._socket.send(wire)
                return await future
        except asyncio.CancelledError:
            if dispatched:
                self._remember_expired(echo)
            raise
        except TimeoutError:
            if dispatched:
                self._remember_expired(echo)
            raise _ActionError("ONEBOT_WEBSOCKET_TIMEOUT", uncertain=dispatched) from None
        except _ActionError:
            raise
        except (ConnectionClosed, OSError, RuntimeError):
            raise _ActionError("ONEBOT_WEBSOCKET_CLOSED", uncertain=dispatched) from None
        finally:
            if self._pending.pop(echo, None) is not None and not future.done():
                future.cancel()
            if acquired:
                self._pending_slots.release()

    def _timeout(self, timeout: float | None) -> float:
        value = self.timeout_seconds if timeout is None else timeout
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value <= 120:
            raise _invalid("Invalid effect timeout")
        return float(value)

    async def get_login_info(self, *, timeout: float | None = None) -> dict[str, str]:
        try:
            body = await self._request("get_login_info", {}, self._timeout(timeout))
            result = _json(body, MAX_RESPONSE_BYTES)
            if (
                not isinstance(result, dict)
                or result.get("status") != "ok"
                or type(result.get("retcode")) is not int
                or result["retcode"] != 0
                or not isinstance(result.get("data"), dict)
            ):
                raise _invalid()
            data = result["data"]
            return {"self_id": _id(data.get("user_id")), "nickname": _string(data.get("nickname"))}
        except (ForestError, _ActionError, ValueError, TypeError):
            raise ForestError(
                "ONEBOT_API_UNAVAILABLE", "Could not verify the OneBot API account", 502
            ) from None

    def prepare(
        self,
        route: dict[str, Any],
        parts: list[dict[str, Any] | AuthorizedImage],
        operation_id: str | None = None,
    ) -> PreparedSend:
        return prepare_send(route["channel_kind"], route["channel_id"], parts)

    async def send(self, prepared: PreparedSend, *, timeout: float | None = None) -> SendResult:
        if not isinstance(prepared, PreparedSend):
            raise _invalid("A frozen prepared send is required")
        try:
            params = _json(prepared.wire_body, MAX_SEND_BYTES)
            body = await self._request(prepared.action, params, self._timeout(timeout))
            return _send_result(body)
        except _ActionError as exc:
            return SendResult(
                "UNKNOWN" if exc.uncertain else "NOT_SENT",
                error_code=exc.code,
            )
        except ForestError as exc:
            return SendResult("UNKNOWN", error_code=exc.code)

    async def aclose(self) -> None:
        async with self._close_lock:
            if self._close_task is None or (
                self._close_task.done()
                and (self._close_task.cancelled() or self._close_task.exception())
            ):
                self._close_task = asyncio.create_task(self._close(), name="close:onebot-websocket")
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        self._closing = True
        self._closed.set()
        self._activation.set()
        if self._socket is not None:
            await self._socket.close()
        current = asyncio.current_task()
        if self._reader_task is not None and self._reader_task is not current:
            await asyncio.gather(self._reader_task, return_exceptions=True)
        if self._consumer_task is not None and self._consumer_task is not current:
            self._consumer_task.cancel()
            await asyncio.gather(self._consumer_task, return_exceptions=True)
        self._terminate("ONEBOT_WEBSOCKET_CLOSED")


def _send_result(body: bytes) -> SendResult:
    try:
        result = _json(body, MAX_RESPONSE_BYTES)
        if not isinstance(result, dict) or type(result.get("retcode")) is not int:
            raise _invalid()
        status, code = result.get("status"), result["retcode"]
        if status == "failed" and code not in {0, 1}:
            return SendResult("NOT_SENT", error_code="PLATFORM_REJECTED")
        if status == "ok" and code == 0 and isinstance(result.get("data"), dict):
            return SendResult(
                "SENT", message_id=_id(result["data"].get("message_id"), message=True)
            )
        return SendResult("UNKNOWN", error_code="PLATFORM_UNCERTAIN")
    except ForestError:
        return SendResult("UNKNOWN", error_code="INVALID_RESPONSE")

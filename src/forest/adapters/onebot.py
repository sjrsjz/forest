"""Host-only OneBot transports; the pure codec ships in the immutable plugin."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import math
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from forest.errors import ForestError

CAPABILITIES = {
    "event_transport": ["http-post", "forward-websocket"],
    "api_transport": ["http", "forward-websocket"],
    "event_replay": False,
    "send_idempotency": False,
    "send_result_reconciliation": False,
    "unknown_result_retry": False,
}
MAX_EVENT_BYTES = 128 * 1024
MAX_SEND_BYTES = 12 * 1024 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_GROUP_LIST_BYTES = 4 * 1024 * 1024
MAX_JOINED_GROUPS = 4096
MAX_SEGMENTS = 256
MAX_WS_FRAME_BYTES = MAX_GROUP_LIST_BYTES
MAX_WS_EVENTS = 256
MAX_WS_PENDING = 64
_DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]{0,18})\Z")
_ACTIONS = {"send_group_msg", "send_private_msg"}


def _invalid(message: str = "Invalid OneBot transport payload") -> ForestError:
    return ForestError("ONEBOT_INVALID", message, 400)


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate key")
        result[key] = value
    return result


def _json(body: bytes, limit: int) -> Any:
    if not isinstance(body, bytes) or not 0 < len(body) <= limit:
        raise _invalid("OneBot body exceeds its byte limit")
    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_unique)
        pending = [(value, 0)]
        count = 0
        while pending:
            item, depth = pending.pop()
            count += 1
            if count > 8192 or depth > 16:
                raise ValueError("Structure limit")
            if isinstance(item, dict):
                if any(not isinstance(key, str) or len(key) > 128 for key in item):
                    raise ValueError("Key limit")
                pending.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                pending.extend((child, depth + 1) for child in item)
            elif isinstance(item, str):
                item.encode("utf-8")
            elif isinstance(item, float) and not math.isfinite(item):
                raise ValueError("Non-finite number")
            elif isinstance(item, int) and not -(2**63) <= item < 2**63:
                raise ValueError("Integer limit")
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise _invalid("Invalid bounded UTF-8 JSON body") from None


def _encode(value: Any, limit: int) -> bytes:
    try:
        body = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise _invalid() from None
    _json(body, limit)
    return body


def _id(value: Any, *, message: bool = False) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise _invalid("Invalid platform identity")
    if not _DECIMAL.fullmatch(str(value)):
        raise _invalid("Invalid platform identity")
    number = int(value)
    lower, upper = (-(2**31), 2**31) if message else (1, 2**63)
    if not lower <= number < upper:
        raise _invalid("Platform identity is outside its range")
    return str(number)


def _string(value: Any, default: str = "", *, limit: int = 1024) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or len(value) > limit:
        raise _invalid("Invalid bounded text field")
    return value


def _http_url(value: str, *, origin_only: bool = False) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 8192:
        raise _invalid("Invalid HTTP endpoint")
    if any(ord(char) <= 32 or ord(char) == 127 for char in value) or "\\" in value:
        raise _invalid("Invalid HTTP endpoint")
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme in {"http", "https"}
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and (parsed.port is None or 1 <= parsed.port <= 65535)
        )
        if origin_only:
            valid = valid and not parsed.query and parsed.path in {"", "/"}
        if not valid:
            raise ValueError("Invalid origin")
        parsed.hostname.encode("idna")
    except (ValueError, UnicodeError):
        raise _invalid("Invalid HTTP endpoint") from None
    return value.rstrip("/") if origin_only else value


def verify_signature(body: bytes, secret: str, signature: str | None) -> None:
    """Authenticate exact HTTP ingress bytes inside the Adapter boundary."""

    if not isinstance(body, bytes) or len(body) > MAX_EVENT_BYTES:
        raise ForestError("ONEBOT_TOO_LARGE", "OneBot body exceeds its byte limit", 413)
    if not isinstance(secret, str) or not 1 <= len(secret) <= 4096:
        raise ForestError("ONEBOT_CONFIG_INVALID", "A binding signature secret is required", 503)
    try:
        expected = "sha1=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha1).hexdigest()
    except UnicodeError:
        raise ForestError("ONEBOT_CONFIG_INVALID", "Invalid binding signature secret", 503) from None
    if (
        not isinstance(signature, str)
        or re.fullmatch(r"sha1=[0-9a-fA-F]{40}", signature) is None
        or not hmac.compare_digest(expected, signature.lower())
    ):
        raise ForestError("ONEBOT_SIGNATURE_INVALID", "Event signature is invalid", 401)


def _validate_prepared_body(action: str, body: bytes) -> None:
    if action not in _ACTIONS:
        raise _invalid("Unsupported OneBot send action")
    value = _json(body, MAX_SEND_BYTES)
    target = "group_id" if action == "send_group_msg" else "user_id"
    if not isinstance(value, dict) or set(value) != {target, "message", "auto_escape"}:
        raise _invalid("Prepared send envelope does not match its action")
    _id(value[target])
    parts = value["message"]
    if value["auto_escape"] is not True or not isinstance(parts, list) or not 1 <= len(parts) <= MAX_SEGMENTS:
        raise _invalid("Prepared send message is invalid")
    for part in parts:
        if not isinstance(part, dict) or set(part) != {"type", "data"}:
            raise _invalid("Prepared send segment is invalid")
        kind, data = part["type"], part["data"]
        if kind == "text" and isinstance(data, dict) and set(data) == {"text"}:
            if not isinstance(data["text"], str):
                raise _invalid("Prepared text segment is invalid")
        elif kind == "at" and isinstance(data, dict) and set(data) == {"qq"}:
            if action != "send_group_msg":
                raise _invalid("Prepared mention segment is invalid")
            if data["qq"] != "all":
                _id(data["qq"])
        elif kind == "reply" and isinstance(data, dict) and set(data) == {"id"}:
            _id(data["id"], message=True)
        elif kind == "face" and isinstance(data, dict) and set(data) == {"id"}:
            if not isinstance(data["id"], str) or re.fullmatch(r"[0-9]{1,5}", data["id"]) is None:
                raise _invalid("Prepared face segment is invalid")
        elif kind == "image" and isinstance(data, dict) and set(data) == {"file", "proxy", "timeout"}:
            if (data["proxy"] != "0" or data["timeout"] != "10"
                    or not isinstance(data["file"], str) or not data["file"].startswith("base64://")):
                raise _invalid("Prepared image must contain managed image bytes")
            try:
                image = base64.b64decode(data["file"][9:], validate=True)
            except ValueError:
                raise _invalid("Prepared image encoding is invalid") from None
            if not 0 < len(image) <= 8 * 1024 * 1024:
                raise _invalid("Prepared image exceeds its byte limit")
        else:
            raise _invalid("Prepared send contains an unsupported segment")


@dataclass(frozen=True)
class PreparedSend:
    action: str
    wire_body: bytes
    wire_digest: str

    def __post_init__(self) -> None:
        _validate_prepared_body(self.action, self.wire_body)
        if (
            not isinstance(self.wire_digest, str)
            or re.fullmatch(r"[a-f0-9]{64}", self.wire_digest) is None
            or not hmac.compare_digest(hashlib.sha256(self.wire_body).hexdigest(), self.wire_digest)
        ):
            raise _invalid("Prepared send digest does not match its exact bytes")



class _FixedEndpointConnect(connect):
    """Disable library redirects so the Bearer credential stays on one origin."""

    def process_redirect(self, exc: Exception) -> Exception | str:
        return exc


@dataclass(frozen=True)
class SendResult:
    status: Literal["SENT", "NOT_SENT", "REJECTED", "UNKNOWN"]
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

    async def send(self, prepared: PreparedSend, *, timeout: float | None = None) -> SendResult:
        if not isinstance(prepared, PreparedSend):
            raise _invalid("A frozen prepared send is required")
        timeout = self._timeout(timeout)
        # The outer deadline bounds pool wait, connect, write, and all body chunks
        # together, including a slow response that never trips an idle timeout.
        try:
            status_code, body = await self._post(prepared.action, prepared.wire_body, timeout)
            if status_code in {400, 401, 403, 404, 406}:
                return SendResult("REJECTED", error_code="HTTP_REJECTED")
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
        self._response_limits: dict[str, int] = {}
        self._expired: set[str] = set()
        self._expired_order: list[str] = []
        self._event_handler: Callable[[bytes], Awaitable[None]] | None = None
        self._event_admission = asyncio.Event()
        self._event_dispatch_lock = asyncio.Lock()
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
        self._event_admission.set()
        if self.is_closed:
            self._finish_events()

    async def freeze_events(self) -> None:
        """Close event admission and join the handler currently crossing the gate."""

        self._event_admission.clear()
        async with self._event_dispatch_lock:
            pass

    def resume_events(self) -> None:
        if self._event_handler is None or not self._activation.is_set() or self.is_closed:
            raise ForestError("ONEBOT_WEBSOCKET_STATE", "WebSocket events cannot resume", 409)
        self._event_admission.set()

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
                        if future.done():
                            raise ForestError(
                                "ONEBOT_WEBSOCKET_PROTOCOL", "Invalid action response", 502
                            )
                        if len(body) > self._response_limits[echo]:
                            future.set_exception(_ActionError("ONEBOT_RESPONSE_TOO_LARGE", uncertain=True))
                            continue
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
                    await self._event_admission.wait()
                    async with self._event_dispatch_lock:
                        await self._event_admission.wait()
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

    async def _request(
        self, action: str, params: dict[str, Any], timeout: float,
        *, response_limit: int = MAX_RESPONSE_BYTES,
    ) -> bytes:
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
                self._response_limits[echo] = response_limit
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
            self._response_limits.pop(echo, None)
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

    async def get_group_list(self, *, timeout: float | None = None) -> list[dict[str, str]]:
        try:
            body = await self._request(
                "get_group_list", {}, self._timeout(timeout), response_limit=MAX_GROUP_LIST_BYTES,
            )
            result = _json(body, MAX_GROUP_LIST_BYTES)
            if (not isinstance(result, dict) or result.get("status") != "ok"
                    or type(result.get("retcode")) is not int or result["retcode"] != 0
                    or not isinstance(result.get("data"), list)
                    or len(result["data"]) > MAX_JOINED_GROUPS):
                raise _invalid()
            groups, seen = [], set()
            for row in result["data"]:
                if not isinstance(row, dict):
                    raise _invalid()
                group_id = _id(row.get("group_id"))
                name = row.get("group_name")
                if group_id in seen or not isinstance(name, str):
                    raise _invalid()
                seen.add(group_id)
                groups.append({"group_id": group_id, "group_name": _string(name, limit=256)})
            return groups
        except (ForestError, _ActionError, ValueError, TypeError):
            raise ForestError(
                "ONEBOT_GROUP_LIST_UNAVAILABLE", "Could not read the OneBot group list", 502
            ) from None

    async def query_api(self, action: str, arguments: dict, *, timeout=10) -> dict:
        if action not in {"get_group_info", "get_group_member_info", "get_group_member_list"}:
            raise _invalid("Unsupported OneBot query")
        params = {key: int(value) for key, value in arguments.items() if key in {"group_id", "user_id"}}
        if action in {"get_group_info", "get_group_member_info"}:
            params["no_cache"] = True
        try:
            body = await self._request(action, params, self._timeout(timeout),
                                       response_limit=MAX_GROUP_LIST_BYTES)
            result = _json(body, MAX_GROUP_LIST_BYTES)
            if (not isinstance(result, dict) or result.get("status") != "ok"
                    or type(result.get("retcode")) is not int or result["retcode"] != 0):
                raise _invalid()
            data = result.get("data")
            if action == "get_group_info":
                if not isinstance(data, dict) or _id(data.get("group_id")) != str(params["group_id"]):
                    raise _invalid()
                counts = {key: data.get(key) for key in ("member_count", "max_member_count")}
                if any(type(value) is not int or value < 0 for value in counts.values()):
                    raise _invalid()
                return {"group_id": _id(data["group_id"]),
                        "group_name": _string(data.get("group_name"), limit=256), **counts}
            rows = data if action == "get_group_member_list" else [data]
            if not isinstance(rows, list) or len(rows) > 10000:
                raise _invalid()
            members, seen = [], set()
            for row in rows:
                if (not isinstance(row, dict) or _id(row.get("group_id")) != str(params["group_id"])
                        or row.get("role") not in {"owner", "admin", "member"}):
                    raise _invalid()
                user_id = _id(row.get("user_id"))
                if user_id in seen or ("user_id" in params and user_id != str(params["user_id"])):
                    raise _invalid()
                seen.add(user_id)
                members.append({"group_id": str(params["group_id"]), "user_id": user_id,
                                "nickname": _string(row.get("nickname"), limit=256),
                                "card": _string(row.get("card"), limit=256), "role": row["role"]})
            if action == "get_group_member_info":
                return members[0]
            members.sort(key=lambda row: int(row["user_id"]))
            after = arguments["after_user_id"]
            remaining = [row for row in members if after is None or int(row["user_id"]) > int(after)]
            items = remaining[:25]
            return {"items": items, "total": len(members),
                    "next_after_user_id": items[-1]["user_id"] if len(remaining) > 25 else None}
        except (ForestError, _ActionError, ValueError, TypeError):
            raise ForestError("ONEBOT_QUERY_FAILED", "OneBot query did not return valid data", 502) from None

    async def command_api(self, action: str, arguments: dict, *, timeout=10) -> dict:
        if action not in {"set_group_ban", "set_group_whole_ban", "set_group_kick",
                          "set_group_card", "set_group_name"}:
            raise _invalid("Unsupported OneBot command")
        params = {key: int(value) if key in {"group_id", "user_id"} else value
                  for key, value in arguments.items()}
        try:
            body = await self._request(action, params, self._timeout(timeout))
            result = _json(body, MAX_RESPONSE_BYTES)
            if not isinstance(result, dict) or type(result.get("retcode")) is not int:
                raise _invalid()
            if result.get("status") == "ok" and result["retcode"] == 0:
                return {"outcome": "SUCCEEDED", "error_code": None}
            if result.get("status") == "failed" and result["retcode"] != 0:
                return {"outcome": "FAILED", "error_code": "ONEBOT_RETCODE_" + str(result["retcode"])}
            return {"outcome": "UNKNOWN", "error_code": "ONEBOT_COMMAND_RESULT_INVALID"}
        except _ActionError as error:
            return {"outcome": "UNKNOWN" if error.uncertain else "FAILED", "error_code": error.code}
        except (ForestError, ValueError, TypeError):
            return {"outcome": "UNKNOWN", "error_code": "ONEBOT_COMMAND_RESULT_INVALID"}

    async def get_status(self, *, timeout: float | None = None) -> dict[str, bool]:
        try:
            body = await self._request("get_status", {}, self._timeout(timeout))
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
            if type(data.get("online")) is not bool or type(data.get("good")) is not bool:
                raise _invalid()
            return {"online": data["online"], "good": data["good"]}
        except (ForestError, _ActionError, ValueError, TypeError):
            raise ForestError(
                "ONEBOT_API_UNAVAILABLE", "Could not verify the OneBot account status", 502
            ) from None

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
            return SendResult("REJECTED", error_code="PLATFORM_REJECTED")
        if status == "ok" and code == 0 and isinstance(result.get("data"), dict):
            return SendResult(
                "SENT", message_id=_id(result["data"].get("message_id"), message=True)
            )
        return SendResult("UNKNOWN", error_code="PLATFORM_UNCERTAIN")
    except ForestError:
        return SendResult("UNKNOWN", error_code="INVALID_RESPONSE")

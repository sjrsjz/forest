"""Host-only OneBot HTTP effect transport; codec ships in the immutable plugin."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from forest.builtin.onebot.python.onebot_codec import CAPABILITIES as CAPABILITIES
from forest.builtin.onebot.python.onebot_codec import MAX_EVENT_BYTES as MAX_EVENT_BYTES
from forest.builtin.onebot.python.onebot_codec import (
    MAX_RESPONSE_BYTES,
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

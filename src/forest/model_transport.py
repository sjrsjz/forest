"""One-attempt Chat Completions HTTP transport for a frozen Agent request."""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlsplit

import httpx

from forest.errors import ForestError
from forest.model_contract import (
    MAX_EVENT_BYTES,
    MAX_RESPONSE_BYTES,
    MAX_TOOL_ARGUMENT_BYTES,
    MAX_TOOL_CALLS,
    ModelResult,
    PermitCheck,
    PreparedModelRequest,
    _calls,
    _decode,
    _encode,
    _error,
    _identifier,
)


def _usage(value: Any) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise _error("MODEL_USAGE_INVALID", "Provider usage is malformed")
    result = {}
    for name in ("prompt_tokens", "completion_tokens", "total_tokens"):
        amount = value.get(name)
        if type(amount) is not int or not 0 <= amount <= 1000000000:
            raise _error("MODEL_USAGE_INVALID", "Provider usage must contain bounded token counts")
        result[name] = amount
    if result["total_tokens"] < result["prompt_tokens"] + result["completion_tokens"]:
        raise _error("MODEL_USAGE_INVALID", "Provider total usage is inconsistent")
    return result


def _completion(prepared: PreparedModelRequest, value: Any) -> ModelResult:
    if (not isinstance(value, dict) or not isinstance(value.get("choices"), list)
            or len(value["choices"]) != 1):
        raise _error("MODEL_RESPONSE_INVALID", "Expected one complete chat choice")
    response_id = _identifier(value.get("id"), "provider response ID")
    provider_model = _identifier(value.get("model"), "provider model")
    choice = value["choices"][0]
    if (not isinstance(choice, dict) or type(choice.get("index")) is not int or choice["index"] != 0
            or choice.get("finish_reason") not in {"stop", "tool_calls", "length", "content_filter"}
            or not isinstance(choice.get("message"), dict)
            or choice["message"].get("role") != "assistant"):
        raise _error("MODEL_RESPONSE_INVALID", "Chat choice is incomplete or malformed")
    message = choice["message"]
    text = message.get("content")
    if text is None:
        text = message.get("refusal") or ""
    if not isinstance(text, str) or len(text.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise _error("MODEL_RESPONSE_INVALID", "Invalid assistant content")
    error_code, usage, calls = None, None, []
    try:
        usage = _usage(value.get("usage"))
    except ForestError as exc:
        error_code = exc.code
    try:
        calls = _calls(message.get("tool_calls") or [])
        names = set(prepared.manifest["tool_names"])
        if any(call["name"] not in names for call in calls):
            raise _error("MODEL_TOOL_UNDECLARED", "Provider proposed a tool outside the request")
        if bool(calls) != (choice["finish_reason"] == "tool_calls"):
            raise _error("MODEL_TOOL_SEQUENCE", "Tool proposal does not match the finish reason")
    except ForestError as exc:
        calls = []
        error_code = exc.code
    if choice["finish_reason"] == "stop" and not text:
        error_code = "MODEL_EMPTY_OUTPUT"
    elif choice["finish_reason"] in {"length", "content_filter"}:
        error_code = "MODEL_OUTPUT_INCOMPLETE"
    return ModelResult(
        "RESPONDED", prepared.request_id, prepared.transport_attempt_id, prepared.payload_digest,
        text=text, tool_calls_json=_encode(calls).decode(),
        usage_json=_encode(usage).decode() if usage is not None else None,
        finish_reason=choice["finish_reason"], provider_response_id=response_id,
        provider_model=provider_model, error_code=error_code,
    )


class _StreamCollector:
    def __init__(self):
        self.response_id = self.model = self.finish = None
        self.text, self.refusal, self.tools = [], [], {}
        self.usage = None
        self.content_bytes = 0
        self.done = False

    def consume(self, data: bytes) -> None:
        if self.done:
            raise _error("MODEL_RESPONSE_INVALID", "SSE data follows the terminal marker")
        if data == b"[DONE]":
            if self.finish is None:
                raise _error("MODEL_STREAM_INCOMPLETE", "Stream ended before a completed choice")
            self.done = True
            return
        value = _decode(data, limit=MAX_EVENT_BYTES)
        if not isinstance(value, dict) or "error" in value or not isinstance(value.get("choices"), list):
            raise _error("MODEL_RESPONSE_INVALID", "Invalid chat stream event")
        response_id = _identifier(value.get("id"), "provider response ID")
        model = _identifier(value.get("model"), "provider model")
        if self.response_id is None:
            self.response_id, self.model = response_id, model
        if response_id != self.response_id or model != self.model:
            raise _error("MODEL_RESPONSE_INVALID", "Stream changed its response identity")
        if value.get("usage") is not None:
            if self.usage is not None:
                raise _error("MODEL_RESPONSE_INVALID", "Duplicate final usage event")
            self.usage = value["usage"]
        choices = value["choices"]
        if not choices:
            if self.finish is None or value.get("usage") is None:
                raise _error("MODEL_RESPONSE_INVALID", "Unexpected empty stream choice")
            return
        if len(choices) != 1:
            raise _error("MODEL_RESPONSE_INVALID", "Stream must contain one choice")
        choice = choices[0]
        if (not isinstance(choice, dict) or type(choice.get("index")) is not int or choice["index"] != 0
                or not isinstance(choice.get("delta"), dict) or self.finish is not None):
            raise _error("MODEL_RESPONSE_INVALID", "Invalid or late chat stream delta")
        delta = choice["delta"]
        if delta.get("role", "assistant") != "assistant":
            raise _error("MODEL_RESPONSE_INVALID", "Stream changed its assistant role")
        if delta.get("function_call") is not None:
            raise _error("MODEL_RESPONSE_INVALID", "Deprecated function_call stream is unsupported")
        for name, target in (("content", self.text), ("refusal", self.refusal)):
            fragment = delta.get(name)
            if fragment is not None:
                if not isinstance(fragment, str):
                    raise _error("MODEL_RESPONSE_INVALID", "Stream text delta must be text")
                target.append(fragment)
                self.content_bytes += len(fragment.encode("utf-8"))
        if self.content_bytes > MAX_RESPONSE_BYTES:
            raise _error("MODEL_RESPONSE_TOO_LARGE", "Stream exceeds bounded output size")
        additions = delta.get("tool_calls")
        if additions is not None:
            if not isinstance(additions, list) or len(additions) > MAX_TOOL_CALLS:
                raise _error("MODEL_RESPONSE_INVALID", "Invalid streamed tool calls")
            for addition in additions:
                if (not isinstance(addition, dict) or type(addition.get("index")) is not int
                        or not 0 <= addition["index"] < MAX_TOOL_CALLS
                        or addition.get("type", "function") != "function"
                        or not isinstance(addition.get("function", {}), dict)):
                    raise _error("MODEL_RESPONSE_INVALID", "Invalid streamed tool call identity")
                tool = self.tools.setdefault(addition["index"], {"id": "", "name": "", "arguments": ""})
                for name, fragment in (
                    ("id", addition.get("id")),
                    ("name", addition.get("function", {}).get("name")),
                    ("arguments", addition.get("function", {}).get("arguments")),
                ):
                    if fragment is not None:
                        if not isinstance(fragment, str):
                            raise _error("MODEL_RESPONSE_INVALID", "Tool delta field must be text")
                        tool[name] += fragment
                        limit = MAX_TOOL_ARGUMENT_BYTES if name == "arguments" else 128
                        if len(tool[name].encode("utf-8")) > limit:
                            raise _error("MODEL_RESPONSE_TOO_LARGE", "Tool delta exceeds field limit")
        finish = choice.get("finish_reason")
        if finish is not None:
            if finish not in {"stop", "tool_calls", "length", "content_filter"}:
                raise _error("MODEL_RESPONSE_INVALID", "Unsupported stream finish reason")
            self.finish = finish

    def result(self, prepared: PreparedModelRequest) -> ModelResult:
        if not self.done or self.finish is None:
            raise _error("MODEL_STREAM_INCOMPLETE", "Stream did not finish completely")
        if sorted(self.tools) != list(range(len(self.tools))):
            raise _error("MODEL_RESPONSE_INVALID", "Stream tool indices contain a gap")
        message = {"role": "assistant", "content": "".join(self.text), "refusal": "".join(self.refusal),
                   "tool_calls": [self.tools[index] for index in sorted(self.tools)]}
        if not message["content"] and message["refusal"]:
            message["content"] = message["refusal"]
        return _completion(prepared, {"id": self.response_id, "model": self.model,
                                     "choices": [{"index": 0, "finish_reason": self.finish,
                                                  "message": message}], "usage": self.usage})


async def _stream(response: httpx.Response, prepared: PreparedModelRequest) -> ModelResult:
    collector = _StreamCollector()
    buffer = bytearray()
    data_lines: list[bytes] = []
    total = event_bytes = 0
    async for chunk in response.aiter_raw():
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise _error("MODEL_RESPONSE_TOO_LARGE", "Stream exceeds its wire limit")
        buffer.extend(chunk)
        if len(buffer) > MAX_EVENT_BYTES:
            raise _error("MODEL_RESPONSE_TOO_LARGE", "Stream line exceeds its limit")
        while b"\n" in buffer:
            line, _, rest = buffer.partition(b"\n")
            buffer = bytearray(rest)
            line = line.removesuffix(b"\r")
            if not line:
                if data_lines:
                    collector.consume(b"\n".join(data_lines))
                data_lines, event_bytes = [], 0
                continue
            if line.startswith(b":"):
                continue
            field_name, separator, value = line.partition(b":")
            if value.startswith(b" "):
                value = value[1:]
            if field_name == b"data":
                if not separator:
                    value = b""
                event_bytes += len(value) + 1
                if event_bytes > MAX_EVENT_BYTES:
                    raise _error("MODEL_RESPONSE_TOO_LARGE", "Stream event exceeds its limit")
                data_lines.append(bytes(value))
    if buffer or data_lines:
        raise _error("MODEL_STREAM_INCOMPLETE", "Stream ended inside an SSE event")
    return collector.result(prepared)


class ChatCompletionsClient:
    """Host-owned fixed endpoint, no redirects/proxies/retries or hidden API calls."""

    def __init__(self, base_url: str, api_key: str, *, timeout_seconds: float = 120,
                 max_connections: int = 8, deployment_id: str | None = None):
        try:
            if (not isinstance(base_url, str) or len(base_url) > 2048
                    or any(ord(char) <= 32 or ord(char) == 127 for char in base_url)
                    or "\\" in base_url):
                raise ValueError("URL")
            parsed = urlsplit(base_url)
            if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                    or parsed.username is not None or parsed.password is not None
                    or parsed.query or parsed.fragment
                    or (parsed.port is not None and not 1 <= parsed.port <= 65535)):
                raise ValueError("URL")
            self._endpoint = str(httpx.URL(base_url)).rstrip("/") + "/chat/completions"
        except (ValueError, httpx.InvalidURL):
            raise _error("MODEL_CONFIG_INVALID", "Invalid fixed Provider base URL") from None
        if (not isinstance(api_key, str) or len(api_key) > 4096
                or any(ord(char) < 33 or ord(char) > 126 for char in api_key)
                or type(max_connections) is not int or not 1 <= max_connections <= 64
                or isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not 0 < timeout_seconds <= 3600):
            raise _error("MODEL_CONFIG_INVALID", "Invalid Provider credential/transport bounds")
        self.deployment_id, self.timeout_seconds = deployment_id, timeout_seconds
        self._slots = asyncio.Semaphore(max_connections)
        self._tasks: set[asyncio.Task] = set()
        self._closing_tasks: set[asyncio.Task] = set()
        self._closed = False
        self._close_task: asyncio.Task | None = None
        limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections)
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False),
            trust_env=False, follow_redirects=False, timeout=httpx.Timeout(timeout_seconds),
            headers={**({"Authorization": "Bearer " + api_key} if api_key else {}),
                     "Accept-Encoding": "identity"},
        )

    async def dispatch(self, prepared: PreparedModelRequest, *, permitcheck: PermitCheck,
                       timeout: float | None = None) -> ModelResult:
        prepared.verify()
        if self.deployment_id is not None and prepared.manifest["actual_deployment_id"] != self.deployment_id:
            raise _error("MODEL_ROUTE_MISMATCH", "Prepared request belongs to another Provider deployment")
        duration = self.timeout_seconds if timeout is None else timeout
        if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not 0 < duration <= 3600:
            raise _error("MODEL_CONFIG_INVALID", "Invalid model attempt timeout")
        task = asyncio.current_task()
        self._tasks.add(task)
        try:
            if self._closed:
                return self._failure(prepared, "NOT_SENT", "MODEL_CLIENT_CLOSED")
            # Capacity is acquired before issuing a short-lived dispatch permit.
            async with asyncio.timeout(duration):
                async with self._slots:
                    if self._closed:
                        return self._failure(prepared, "NOT_SENT", "MODEL_CLIENT_CLOSED")
                    await permitcheck(prepared)
                    if self._closed:
                        return self._failure(prepared, "NOT_SENT", "MODEL_CLIENT_CLOSED")
                    try:
                        async with self._client.stream(
                            "POST", self._endpoint, content=prepared.body,
                            headers={"Content-Type": "application/json",
                                     "Accept": "text/event-stream" if prepared.stream else "application/json"},
                            timeout=duration,
                        ) as response:
                            if response.status_code != 200:
                                state = "NOT_SENT" if response.status_code in {
                                    400, 401, 403, 404, 405, 413, 415, 422, 429,
                                } else "UNKNOWN"
                                code = (
                                    "MODEL_HTTP_REJECTED_"
                                    if state == "NOT_SENT"
                                    else "MODEL_HTTP_UNCERTAIN_"
                                ) + str(response.status_code)
                                return self._failure(prepared, state, code)
                            if response.headers.get("content-encoding", "identity").lower() != "identity":
                                raise _error("MODEL_RESPONSE_ENCODING", "Provider response encoding is unsupported")
                            content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
                            if prepared.stream:
                                if content_type != "text/event-stream":
                                    raise _error("MODEL_RESPONSE_INVALID", "Expected an SSE model stream")
                                return await _stream(response, prepared)
                            if content_type != "application/json":
                                raise _error("MODEL_RESPONSE_INVALID", "Expected a JSON model response")
                            content = bytearray()
                            async for chunk in response.aiter_raw():
                                content.extend(chunk)
                                if len(content) > MAX_RESPONSE_BYTES:
                                    raise _error("MODEL_RESPONSE_TOO_LARGE", "Provider response exceeds its byte limit")
                            return _completion(prepared, _decode(bytes(content)))
                    except (httpx.HTTPError, OSError, RuntimeError):
                        return self._failure(prepared, "UNKNOWN", "MODEL_TRANSPORT_UNCERTAIN")
                    except ForestError as exc:
                        return self._failure(prepared, "UNKNOWN", exc.code)
        except TimeoutError:
            return self._failure(prepared, "UNKNOWN", "MODEL_TIMEOUT")
        # Cancellation propagates. The durable owner records UNKNOWN for an
        # already permitted attempt and retains any unknown usage reservation.
        finally:
            self._tasks.discard(task)

    @staticmethod
    def _failure(prepared: PreparedModelRequest, status: str, code: str) -> ModelResult:
        return ModelResult(status, prepared.request_id, prepared.transport_attempt_id,
                           prepared.payload_digest, error_code=code)

    async def aclose(self) -> None:
        self._closed = True
        # An externally initiated close already owns and joins these Tasks.
        # Their finally blocks may close their scoped client too; waiting on
        # our closer from one of those Tasks would create a join cycle.
        if asyncio.current_task() in self._closing_tasks:
            return
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close(), name="close:model-client")
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        tasks = [task for task in self._tasks if task is not asyncio.current_task()]
        self._closing_tasks.update(tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._client.aclose()
        self._closing_tasks.clear()

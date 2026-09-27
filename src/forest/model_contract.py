"""Pure model request and result values shared by Agent and Provider ports."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from forest.errors import ForestError

SERIALIZER_REVISION = "chat-completions-agent-protocol-v1"
SUPPORTED_SERIALIZER_REVISIONS = frozenset(
    {SERIALIZER_REVISION, "chat-completions-json-v1"}
)
MAX_REQUEST_BYTES = 24 * 1024 * 1024
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_EVENT_BYTES = 512 * 1024
MAX_TOOL_CALLS = 16
MAX_TOOL_ARGUMENT_BYTES = 64 * 1024
_TOOL_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")


def _error(code: str, message: str) -> ForestError:
    return ForestError(code, message, 422)


def _encode(value: Any, *, limit: int = MAX_REQUEST_BYTES) -> bytes:
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                             allow_nan=False).encode("utf-8")
        if len(encoded) > limit:
            raise ValueError("size")
        return encoded
    except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
        raise _error("MODEL_INPUT_INVALID", "Model data exceeds finite JSON bounds") from exc


def _decode(data: bytes | str, *, limit: int = MAX_RESPONSE_BYTES) -> Any:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate")
            result[key] = value
        return result

    try:
        if isinstance(data, bytes):
            if len(data) > limit:
                raise ValueError("size")
            data = data.decode("utf-8")
        elif not isinstance(data, str) or len(data.encode("utf-8")) > limit:
            raise ValueError("size")
        value = json.loads(data, object_pairs_hook=unique)
        _encode(value, limit=limit)
        return value
    except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
        raise _error("MODEL_RESPONSE_INVALID", "Model response is not bounded finite JSON") from exc


def _identifier(value: Any, name: str, maximum: int = 256) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise _error("MODEL_INPUT_INVALID", f"Invalid {name}")
    return value


@dataclass(frozen=True)
class ModelCapabilities:
    deployment_id: str
    model: str
    capability_revision: str
    model_revision: str = "configured"
    input_modalities: tuple[str, ...] = ("text",)
    supports_tools: bool = True
    supports_streaming: bool = True
    stream_usage: bool = True
    max_output_tokens: int = 4096
    image_roles: tuple[str, ...] = ("user",)
    image_detail: str = "auto"
    system_role: str = "system"
    output_token_parameter: str = "max_completion_tokens"
    tool_image_mapping: str = "omit"

    def __post_init__(self):
        for name in ("deployment_id", "model", "capability_revision", "model_revision"):
            _identifier(getattr(self, name), name)
        for name in ("input_modalities", "image_roles"):
            value = getattr(self, name)
            if not isinstance(value, (tuple, list)) or any(not isinstance(v, str) for v in value):
                raise _error("MODEL_CAPABILITIES_INVALID", "Capability modalities must be string arrays")
        object.__setattr__(self, "input_modalities", tuple(self.input_modalities))
        object.__setattr__(self, "image_roles", tuple(self.image_roles))
        if ("text" not in self.input_modalities or set(self.input_modalities) - {"text", "image"}
                or len(set(self.input_modalities)) != len(self.input_modalities)
                or set(self.image_roles) - {"user"}
                or self.image_detail not in {"auto", "low", "high"}
                or self.system_role not in {"system", "developer"}
                or self.output_token_parameter not in {"max_tokens", "max_completion_tokens"}
                or self.tool_image_mapping not in {"omit", "user_after_tool_group_v1"}):
            raise _error("MODEL_CAPABILITIES_INVALID", "Unsupported model capability mapping")
        for name, minimum, maximum in (("max_output_tokens", 1, 262144),):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise _error("MODEL_CAPABILITIES_INVALID", f"Invalid {name}")
        if any(type(getattr(self, name)) is not bool for name in (
            "supports_tools", "supports_streaming", "stream_usage",
        )):
            raise _error("MODEL_CAPABILITIES_INVALID", "Capability switches must be Boolean")

    def as_dict(self) -> dict:
        return _decode(_encode(asdict(self)))


@dataclass(frozen=True)
class MediaSnapshot:
    """Host-authorized immutable pixels; acquisition credentials never enter here."""
    bot_id: str
    media_ref: str
    media_revision: int
    source_unit_id: str
    part_id: str
    mime: str
    content: bytes = field(repr=False)
    blob_digest: str
    width: int
    height: int
    transform_ref: str | None = None

    def __post_init__(self):
        for name in ("bot_id", "media_ref", "source_unit_id", "part_id"):
            _identifier(getattr(self, name), name)
        if (self.mime not in {"image/png", "image/jpeg", "image/webp", "image/gif"}
                or not isinstance(self.content, bytes) or not 0 < len(self.content) <= 16 * 1024 * 1024
                or type(self.media_revision) is not int or self.media_revision < 1
                or type(self.width) is not int or type(self.height) is not int
                or not 0 < self.width <= 100000 or not 0 < self.height <= 100000):
            raise _error("MODEL_MEDIA_INVALID", "Invalid managed image snapshot")
        digest = "sha256:" + hashlib.sha256(self.content).hexdigest()
        if self.blob_digest not in {digest, digest.removeprefix("sha256:")}:
            raise _error("MODEL_MEDIA_INVALID", "Managed image bytes do not match their digest")
        object.__setattr__(self, "blob_digest", digest)

    def manifest(self) -> dict:
        return {name: getattr(self, name) for name in (
            "bot_id", "media_ref", "media_revision", "source_unit_id", "part_id", "mime",
            "blob_digest", "width", "height", "transform_ref",
        )}


@dataclass(frozen=True)
class PreparedModelRequest:
    request_id: str
    transport_attempt_id: str
    body: bytes = field(repr=False)
    manifest_json: str = field(repr=False)
    payload_digest: str
    max_output_tokens: int
    stream: bool
    serializer_revision: str = SERIALIZER_REVISION

    @property
    def manifest(self) -> dict:
        return _decode(self.manifest_json, limit=MAX_REQUEST_BYTES)

    def transport_record(self) -> dict:
        return {"request_id": self.request_id, "transport_attempt_id": self.transport_attempt_id,
                "serializer_revision": self.serializer_revision, "payload_digest": self.payload_digest}

    def verify(self) -> None:
        manifest = self.manifest
        body = _decode(self.body, limit=MAX_REQUEST_BYTES)
        if not isinstance(manifest, dict) or not isinstance(body, dict):
            raise _error("MODEL_REQUEST_TAMPERED", "Prepared request must retain its object schema")
        capabilities = manifest.get("capability_snapshot", {})
        if (not isinstance(self.body, bytes) or len(self.body) > MAX_REQUEST_BYTES
                or self.payload_digest != "sha256:" + hashlib.sha256(self.body).hexdigest()
                or manifest.get("request_id") != self.request_id
                or manifest.get("serializer_revision") != self.serializer_revision
                or self.serializer_revision not in SUPPORTED_SERIALIZER_REVISIONS
                or not isinstance(capabilities, dict)
                or body.get("model") != manifest.get("model")
                or body.get("stream") is not self.stream
                or body.get(capabilities.get("output_token_parameter", "")) != self.max_output_tokens):
            raise _error("MODEL_REQUEST_TAMPERED", "Prepared model bytes/identity changed")


@dataclass(frozen=True)
class ModelResult:
    status: Literal["RESPONDED", "NOT_SENT", "UNKNOWN"]
    request_id: str
    transport_attempt_id: str
    payload_digest: str
    text: str = field(default="", repr=False)
    tool_calls_json: str = field(default="[]", repr=False)
    usage_json: str | None = field(default=None, repr=False)
    finish_reason: str | None = None
    provider_response_id: str | None = None
    provider_model: str | None = None
    error_code: str | None = None

    @property
    def tool_calls(self) -> list[dict]:
        """Ordered {id, name, arguments:object}; these are proposals, never authorization."""
        return _decode(self.tool_calls_json)

    @property
    def usage(self) -> dict | None:
        return _decode(self.usage_json) if self.usage_json is not None else None

    def as_dict(self) -> dict:
        return {"status": self.status, "request_id": self.request_id,
                "transport_attempt_id": self.transport_attempt_id, "payload_digest": self.payload_digest,
                "text": self.text, "tool_calls": self.tool_calls, "usage": self.usage,
                "finish_reason": self.finish_reason, "provider_response_id": self.provider_response_id,
                "provider_model": self.provider_model, "error_code": self.error_code}


PermitCheck = Callable[[PreparedModelRequest], Awaitable[Any]]


def _calls(value: Any) -> list[dict]:
    if not isinstance(value, list) or len(value) > MAX_TOOL_CALLS:
        raise _error("MODEL_TOOL_SEQUENCE", "Invalid bounded tool call list")
    calls, seen = [], set()
    for item in value:
        if not isinstance(item, dict):
            raise _error("MODEL_TOOL_SEQUENCE", "Invalid tool call")
        identifier = _identifier(item.get("id"), "tool call ID", 128)
        function = item.get("function", item)
        if not isinstance(function, dict):
            raise _error("MODEL_TOOL_SEQUENCE", "Tool function must be an object")
        name = function.get("name")
        if identifier in seen or not isinstance(name, str) or not _TOOL_NAME.fullmatch(name):
            raise _error("MODEL_TOOL_SEQUENCE", "Duplicate tool call ID or invalid function name")
        arguments = function.get("arguments")
        if isinstance(arguments, str):
            arguments = _decode(arguments, limit=MAX_TOOL_ARGUMENT_BYTES)
        if not isinstance(arguments, dict):
            raise _error("MODEL_TOOL_ARGUMENTS", "Tool arguments must be a finite JSON object")
        _encode(arguments, limit=MAX_TOOL_ARGUMENT_BYTES)
        calls.append({"id": identifier, "name": name, "arguments": arguments})
        seen.add(identifier)
    return calls

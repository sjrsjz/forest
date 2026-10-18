"""OneBot v11 codec shared by bounded, single-attempt host transports.

The codec does not open sockets or grant user permissions. Its media locators
must be captured into host-managed references before becoming Context content.
Only the host-side client receives API credentials. The binding fixes its URL;
neither an incoming message nor a prepared request selects a network origin.

Protocol sources: https://github.com/botuniverse/onebot-11/tree/master
communication/{http,http-post,authorization}.md and message/{string,segment}.md.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from forest.errors import ForestError

PLATFORM = "onebot-v11"
CAPABILITIES = {
    "event_transport": ["http-post", "forward-websocket"],
    "api_transport": ["http", "forward-websocket"],
    "event_replay": False,
    "send_idempotency": False,
    "send_result_reconciliation": False,
    "unknown_result_retry": False,
}
MAX_EVENT_BYTES = 128 * 1024
MAX_SEND_BYTES = 192 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_SEGMENTS = 256
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
_CQ = re.compile(r"\[CQ:([^\]]*)\]")
_ENTITY = re.compile(r"&(?:amp|#91|#93|#44);")
_DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]{0,18})\Z")
_ACTIONS = {"group": "send_group_msg", "private": "send_private_msg"}


def _invalid(message: str = "Invalid OneBot message") -> ForestError:
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
        raise ForestError("ONEBOT_TOO_LARGE", "OneBot body exceeds its byte limit", 413)
    try:
        # Decode UTF-8 explicitly: json.loads(bytes) also accepts UTF-16/32.
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_unique)
        pending = [(value, 0)]
        count = 0
        while pending:
            item, depth = pending.pop()
            count += 1
            if count > 8192 or depth > 16:
                raise ValueError("Structure limit")
            if isinstance(item, dict):
                if any(len(key) > 128 for key in item):
                    raise ValueError("Key limit")
                pending.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                pending.extend((child, depth + 1) for child in item)
            elif isinstance(item, str):
                item.encode("utf-8")  # Reject lone surrogate code points.
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
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
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


def verify_signature(body: bytes, secret: str, signature: str | None) -> None:
    """Authenticate the exact received bytes, before any parsing or dispatch.

    Forest requires a nonempty configured secret even though OneBot permits
    unsigned deployments. A failed check never echoes the signature or secret.
    The gateway must additionally bound its streaming read before this call.
    """
    if not isinstance(body, bytes) or len(body) > MAX_EVENT_BYTES:
        raise ForestError("ONEBOT_TOO_LARGE", "OneBot body exceeds its byte limit", 413)
    if not isinstance(secret, str) or not 1 <= len(secret) <= 4096:
        raise ForestError("ONEBOT_CONFIG_INVALID", "A binding signature secret is required", 503)
    try:
        key = secret.encode("utf-8")
    except UnicodeError:
        raise ForestError("ONEBOT_CONFIG_INVALID", "Invalid binding signature secret", 503) from None
    expected = "sha1=" + hmac.new(key, body, hashlib.sha1).hexdigest()
    if (
        not isinstance(signature, str)
        or not re.fullmatch(r"sha1=[0-9a-fA-F]{40}", signature)
        or not hmac.compare_digest(expected, signature.lower())
    ):
        raise ForestError("ONEBOT_SIGNATURE_INVALID", "Event signature is invalid", 401)


def _unescape(text: str, *, parameter: bool = False) -> str:
    # One substitution pass. '&amp;#91;' must remain '&#91;', not '['.
    entities = {"&amp;": "&", "&#91;": "[", "&#93;": "]"}
    if parameter:
        entities["&#44;"] = ","
    return _ENTITY.sub(lambda match: entities.get(match[0], match[0]), text)


def _cq_segments(text: str, *, keep_source: bool = False) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    start = 0

    def append_text(value: str) -> None:
        if value:
            segments.append({"type": "text", "data": {"text": _unescape(value)}})

    for match in _CQ.finditer(text):
        append_text(text[start : match.start()])
        parts = match[1].split(",")
        valid = bool(_IDENTIFIER.fullmatch(parts[0]))
        data: dict[str, str] = {}
        for part in parts[1:]:
            key, sep, value = part.partition("=")
            if not sep or not _IDENTIFIER.fullmatch(key) or key in data:
                valid = False
                break
            data[key] = _unescape(value, parameter=True)
        if valid:
            segments.append({"type": parts[0], "data": data,
                             **({"cq_source": match[0]} if keep_source else {})})
        else:
            append_text(match[0])
        start = match.end()
        if len(segments) > MAX_SEGMENTS:
            raise _invalid("Message contains too many segments")
    append_text(text[start:])
    return segments


def _string(value: Any, default: str = "", *, limit: int = 1024) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or len(value) > limit:
        raise _invalid("Invalid bounded text field")
    return value


def _segments(message: Any) -> list[dict[str, Any]]:
    if isinstance(message, str):
        message = _cq_segments(message)
    if not isinstance(message, list) or len(message) > MAX_SEGMENTS:
        raise _invalid("Message must contain a bounded segment array or CQ string")
    normalized: list[dict[str, Any]] = []
    for segment in message:
        if not isinstance(segment, dict):
            raise _invalid("Invalid message segment")
        kind, data = segment.get("type"), segment.get("data")
        if not isinstance(kind, str) or not _IDENTIFIER.fullmatch(kind):
            raise _invalid("Invalid message segment type")
        if not isinstance(data, dict):
            raise _invalid("Invalid message segment data")
        if kind == "text":
            if not isinstance(data.get("text"), str):
                raise _invalid("Text segment requires text")
            normalized.append({"type": "text", "text": data["text"]})
        elif kind == "at":
            # An invalid at is data, never a trigger or an authorization input.
            try:
                user_id = "all" if data.get("qq") == "all" else _id(data.get("qq"))
            except ForestError:
                normalized.append({"type": "unsupported", "platform_type": kind, "data": data})
            else:
                normalized.append({"type": "mention", "external_user_id": user_id})
        elif kind == "reply":
            try:
                message_id = _id(data.get("id"), message=True)
            except ForestError:
                normalized.append({"type": "unsupported", "platform_type": kind, "data": data})
            else:
                normalized.append({"type": "reply", "platform_message_id": message_id})
        elif kind == "face" and isinstance(data.get("id"), (int, str)) and re.fullmatch(r"[0-9]{1,5}", str(data["id"])):
            normalized.append({"type": "face", "face_id": str(int(data["id"]))})
        elif kind == "image":
            # This is a typed acquisition hint, not a fetch permit or Context URL.
            source = {
                "kind": "onebot-source",
                "uri": _string(data.get("url"), limit=8192) or None,
                "platform_file": _string(data.get("file"), limit=8192) or None,
                "state": "PENDING_CAPTURE",
            }
            normalized.append({"type": "image", "media_ref": source})
        else:
            normalized.append({"type": "unsupported", "platform_type": kind, "data": data})
    return normalized


@dataclass(frozen=True)
class NormalizedMessage:
    """Immutable canonical encoding; accessors return independent JSON values."""

    _body: bytes

    def as_dict(self) -> dict[str, Any]:
        return _json(self._body, MAX_EVENT_BYTES)


def parse_event(body: bytes, *, self_id: str) -> NormalizedMessage | None:
    """Pure normalization after gateway authentication; never trust raw_message.

    Platform sender.role is an informational snapshot, not a Forest grant.
    Anonymous senders must not inherit grants of the platform's shared QQ ID.
    """
    event = _json(body, MAX_EVENT_BYTES)
    if not isinstance(event, dict):
        raise _invalid("Event must be an object")
    configured_self_id = _id(self_id)
    if _id(event.get("self_id")) != configured_self_id:
        raise ForestError("ONEBOT_IDENTITY_MISMATCH", "Event does not belong to this binding", 403)
    post_type = event.get("post_type")
    if not isinstance(post_type, str):
        raise _invalid("Invalid event type")
    if post_type in {"notice", "request", "meta_event"}:
        return None
    if event.get("post_type") != "message":
        raise _invalid("Unsupported event envelope")
    kind = event.get("message_type")
    if not isinstance(kind, str) or kind not in _ACTIONS:
        raise _invalid("Unsupported conversation kind")
    user_id = _id(event.get("user_id"))
    sender = event.get("sender", {})
    if not isinstance(sender, dict):
        raise _invalid("Invalid sender snapshot")
    if sender.get("user_id") is not None and _id(sender["user_id"]) != user_id:
        raise _invalid("Sender snapshot contradicts event identity")
    timestamp = event.get("time")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
        raise _invalid("Invalid event time")
    try:
        sent_at = datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")
    except (ValueError, OverflowError, OSError):
        raise _invalid("Invalid event time") from None
    nickname = _string(sender.get("nickname"))
    card = _string(sender.get("card"))
    author: dict[str, Any] = {
        "external_user_id": user_id,
        "display_name": card or nickname or user_id,
        "group_card": card,
        "is_self": user_id == configured_self_id,
        "role": sender.get("role")
        if isinstance(sender.get("role"), str) and sender["role"] in {"owner", "admin", "member"}
        else None,
    }
    anonymous = event.get("anonymous")
    if anonymous is not None:
        if kind != "group" or not isinstance(anonymous, dict):
            raise _invalid("Invalid anonymous sender")
        author["anonymous"] = {
            "id": _id(anonymous.get("id")),
            "name": _string(anonymous.get("name")),
        }
    elif event.get("sub_type") == "anonymous":
        # Identity cannot silently become a normal principal if metadata is absent.
        author["anonymous"] = {"id": None, "name": ""}
    result = {
        "platform": PLATFORM,
        "self_id": configured_self_id,
        "channel_kind": kind,
        "channel_id": _id(event.get("group_id")) if kind == "group" else user_id,
        "thread_id": "",
        "platform_message_id": _id(event.get("message_id"), message=True),
        "sent_at": sent_at,
        "author": author,
        "segments": _segments(event.get("message")),
    }
    return NormalizedMessage(_encode(result, MAX_EVENT_BYTES))


def normalize_event(
    body: bytes, *, self_id: str, secret: str, signature: str | None
) -> NormalizedMessage | None:
    verify_signature(body, secret, signature)
    return parse_event(body, self_id=self_id)


def _http_url(value: str, *, origin_only: bool = False) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 8192:
        raise _invalid("Invalid HTTP endpoint")
    if any(ord(char) <= 32 or ord(char) == 127 for char in value) or "\\" in value:
        raise _invalid("Invalid HTTP endpoint")
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme in {"http", "https"}
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and (parsed.port is None or 1 <= parsed.port <= 65535)
        )
        if origin_only:
            valid = valid and not parsed.query and parsed.path in {"", "/"}
        if not valid:
            raise ValueError("Invalid origin")
        # Pure protocol validation; HTTP transport canonicalizes its own origin.
        parsed.hostname.encode("idna")
        result = value
    except (ValueError, UnicodeError):
        raise _invalid("Invalid HTTP endpoint") from None
    return result.rstrip("/") if origin_only else result

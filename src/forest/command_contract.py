"""Pure helpers shared by command declarations, package admission, and workers."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any

IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
ARGUMENT_MODES = frozenset({"none", "tail_text"})
_BIDI_CONTROLS = frozenset(
    chr(value)
    for value in (
        0x061C,
        0x200E,
        0x200F,
        *range(0x202A, 0x202F),
        *range(0x2066, 0x206A),
    )
)


def normalize_command_name(value: Any, *, require_normalized: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError("Command name must be a string")
    normalized = unicodedata.normalize("NFC", value)
    if require_normalized and normalized != value:
        raise ValueError("Command name must already use NFC normalization")
    if (
        not normalized.startswith("/")
        or len(normalized) < 2
        or len(normalized) > 64
        or any(character.isspace() for character in normalized)
        or any(unicodedata.category(character) in {"Cc", "Cs"} for character in normalized)
        or any(character in _BIDI_CONTROLS for character in normalized)
    ):
        raise ValueError(
            "Command name must be one slash-prefixed token of at most 64 Unicode scalars"
        )
    return normalized


def command_identifier(name: Any, explicit: Any = None) -> str:
    normalized = normalize_command_name(name)
    if explicit is not None:
        if not isinstance(explicit, str) or not IDENTIFIER.fullmatch(explicit):
            raise ValueError("Command ID must be a bounded identifier")
        return explicit
    candidate = normalized[1:]
    if IDENTIFIER.fullmatch(candidate):
        return candidate
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    return f"command-{digest}"


def validate_command_permission(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not IDENTIFIER.fullmatch(value)
        or not value.startswith("command.")
        or len(value) <= len("command.")
    ):
        raise ValueError("Command permission must be a command.* bounded identifier")
    return value


def validate_argument_mode(value: Any) -> str:
    if value not in ARGUMENT_MODES:
        raise ValueError("Command argument mode must be none or tail_text")
    return value


def validate_command_priority(value: Any) -> int:
    if type(value) is not int or not -100 <= value <= 100:
        raise ValueError("Command priority must be an integer from -100 to 100")
    return value


def schema_digest(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()

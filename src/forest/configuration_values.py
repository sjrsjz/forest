"""Scope-neutral immutable configuration values and JSON Pointer lookup."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class EffectiveModuleConfiguration:
    revision: int
    payload_id: str
    payload_digest: str
    validation_digest: str
    value: object | None = None


_MISSING = object()


def _pointer_value(value: object, path: str) -> object:
    for token in path[1:].split("/"):
        key = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, Mapping):
            value = value.get(key, _MISSING)
        elif isinstance(value, list) and key.isascii() and key.isdecimal():
            index = int(key)
            value = value[index] if index < len(value) else _MISSING
        else:
            return _MISSING
        if value is _MISSING:
            return _MISSING
    return value

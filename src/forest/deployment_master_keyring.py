"""Bounded, versioned deployment master-key material loaded at process start."""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from forest.errors import ForestError

_KEY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,255}\Z")
_PROTOCOL = "forest.deployment-master-keyring/1"
_MAX_BYTES = 65_536
_MAX_KEYS = 32


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate")
        value[key] = item
    return value


def _reject_constant(_value):
    raise ValueError("constant")


@dataclass(frozen=True)
class DeploymentMasterKeyring:
    active_key_id: str
    _keys: Mapping[str, bytes] = field(repr=False)

    @classmethod
    def from_bytes(cls, content: bytes) -> "DeploymentMasterKeyring":
        try:
            if (
                not 1 <= len(content) <= _MAX_BYTES
                or any(not 33 <= byte <= 126 for byte in content)
            ):
                raise ValueError("size")
            document = json.loads(
                content,
                object_pairs_hook=_object,
                parse_constant=_reject_constant,
            )
            if not isinstance(document, dict) or set(document) != {
                "protocol",
                "active_key_id",
                "keys",
            }:
                raise ValueError("document")
            if document["protocol"] != _PROTOCOL:
                raise ValueError("protocol")
            active_key_id = document["active_key_id"]
            encoded_keys = document["keys"]
            if (
                not isinstance(active_key_id, str)
                or _KEY_ID.fullmatch(active_key_id) is None
                or not isinstance(encoded_keys, dict)
                or not 1 <= len(encoded_keys) <= _MAX_KEYS
            ):
                raise ValueError("identity")
            keys = {}
            for key_id, encoded in encoded_keys.items():
                if (
                    not isinstance(key_id, str)
                    or _KEY_ID.fullmatch(key_id) is None
                    or not isinstance(encoded, str)
                    or len(encoded) != 44
                ):
                    raise ValueError("key")
                key = base64.b64decode(encoded, validate=True)
                if len(key) != 32 or base64.b64encode(key).decode("ascii") != encoded:
                    raise ValueError("key")
                keys[key_id] = key
            if active_key_id not in keys or len(set(keys.values())) != len(keys):
                raise ValueError("active")
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError, binascii.Error):
            raise ForestError(
                "SUPERVISOR_MASTER_KEYRING_INVALID",
                "Deployment master keyring is invalid",
                422,
            ) from None
        return cls(active_key_id, MappingProxyType(keys))

    @classmethod
    def read(cls, path: Path) -> "DeploymentMasterKeyring":
        try:
            content = path.read_bytes()
        except OSError:
            raise ForestError(
                "SUPERVISOR_MASTER_KEYRING_INVALID",
                "Deployment master keyring is unavailable",
                422,
            ) from None
        return cls.from_bytes(content)

    def resolve(self, key_id: str) -> bytes | None:
        return self._keys.get(key_id)

    @property
    def key_ids(self) -> tuple[str, ...]:
        return tuple(self._keys)

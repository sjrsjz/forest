"""Strict one-use bootstrap credential for the deployment Web authority."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from forest.errors import ForestError

_PROTOCOL = "forest.deployment-web-bootstrap/1"
_USERNAME = re.compile(r"[a-z][a-z0-9._-]{2,63}\Z")


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
class DeploymentWebBootstrap:
    username: str
    display_name: str
    password: str = field(repr=False)

    @classmethod
    def from_bytes(cls, content: bytes) -> "DeploymentWebBootstrap":
        try:
            if not 1 <= len(content) <= 4096 or b"\x00" in content:
                raise ValueError("size")
            document = json.loads(
                content,
                object_pairs_hook=_object,
                parse_constant=_reject_constant,
            )
            if not isinstance(document, dict) or set(document) != {
                "protocol",
                "username",
                "display_name",
                "password",
            }:
                raise ValueError("document")
            username = document["username"]
            display_name = document["display_name"]
            password = document["password"]
            if (
                document["protocol"] != _PROTOCOL
                or not isinstance(username, str)
                or _USERNAME.fullmatch(username) is None
                or not isinstance(display_name, str)
                or not 1 <= len(display_name.encode("utf-8")) <= 128
                or any(ord(char) < 32 or ord(char) == 127 for char in display_name)
                or not isinstance(password, str)
                or len(password) < 12
                or len(password.encode("utf-8")) > 1024
            ):
                raise ValueError("value")
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
            raise ForestError(
                "SUPERVISOR_WEB_BOOTSTRAP_INVALID",
                "Deployment Web bootstrap credential is invalid",
                422,
            ) from None
        return cls(username, display_name, password)

    @classmethod
    def read(cls, path: Path) -> "DeploymentWebBootstrap":
        try:
            content = path.read_bytes()
        except OSError:
            raise ForestError(
                "SUPERVISOR_WEB_BOOTSTRAP_INVALID",
                "Deployment Web bootstrap credential is unavailable",
                422,
            ) from None
        return cls.from_bytes(content)

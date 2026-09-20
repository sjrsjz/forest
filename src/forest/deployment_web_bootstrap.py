"""Strict installer-delivered pairing credential for first Web setup."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from forest.errors import ForestError

_PROTOCOL = "forest.deployment-web-bootstrap/2"
_SETUP_ID = re.compile(r"setup/[A-Za-z0-9_-]{16,128}\Z")
_PAIRING_CODE = re.compile(r"[A-Za-z0-9_-]{43,128}\Z")


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
    setup_id: str
    pairing_code: str = field(repr=False)
    issued_at: datetime
    expires_at: datetime

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
                "setup_id",
                "pairing_code",
                "issued_at",
                "expires_at",
            }:
                raise ValueError("document")
            setup_id = document["setup_id"]
            pairing_code = document["pairing_code"]
            issued_at = datetime.fromisoformat(document["issued_at"])
            expires_at = datetime.fromisoformat(document["expires_at"])
            if issued_at.tzinfo is None or expires_at.tzinfo is None:
                raise ValueError("timezone")
            issued_at = issued_at.astimezone(timezone.utc)
            expires_at = expires_at.astimezone(timezone.utc)
            if (
                document["protocol"] != _PROTOCOL
                or not isinstance(setup_id, str)
                or _SETUP_ID.fullmatch(setup_id) is None
                or not isinstance(pairing_code, str)
                or _PAIRING_CODE.fullmatch(pairing_code) is None
                or not issued_at < expires_at <= issued_at + timedelta(hours=24)
            ):
                raise ValueError("value")
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError, KeyError):
            raise ForestError(
                "SUPERVISOR_WEB_BOOTSTRAP_INVALID",
                "Deployment Web pairing credential is invalid",
                422,
            ) from None
        return cls(setup_id, pairing_code, issued_at, expires_at)

    @classmethod
    def read(cls, path: Path) -> "DeploymentWebBootstrap":
        try:
            content = path.read_bytes()
        except OSError:
            raise ForestError(
                "SUPERVISOR_WEB_BOOTSTRAP_INVALID",
                "Deployment Web pairing credential is unavailable",
                422,
            ) from None
        return cls.from_bytes(content)

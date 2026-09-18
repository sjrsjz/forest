"""Shared immutable identities for runtime authorities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class HostLease:
    bot_id: str
    host_id: str
    incarnation: str
    owner_epoch: int
    expires_at: datetime

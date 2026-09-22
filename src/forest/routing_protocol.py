"""Typed delivery boundary owned by ``routing.graph``.

The authority passes immutable facts through the Contract Router.  Package
Workers receive only the JSON projection returned by ``worker_payload``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from forest.packages import digest_json

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_CONTRACT = re.compile(r"[1-9][0-9]*\.[0-9]+\Z")


def _bounded(value: object, limit: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value.encode("utf-8")) <= limit
        and not any(ord(char) < 32 or ord(char) == 127 for char in value)
    )


@dataclass(frozen=True)
class DurableEventDeliveryRequest:
    event_id: str
    delivery_id: str
    subscriber_id: str
    topic: str
    contract: str
    publisher_component_id: str
    publisher_generation_id: str
    attempt: int
    payload: Any

    def __post_init__(self) -> None:
        if (
            not _bounded(self.event_id, 256)
            or not _bounded(self.delivery_id, 256)
            or _IDENTIFIER.fullmatch(self.subscriber_id) is None
            or _IDENTIFIER.fullmatch(self.topic) is None
            or _CONTRACT.fullmatch(self.contract) is None
            or _IDENTIFIER.fullmatch(self.publisher_component_id) is None
            or not _bounded(self.publisher_generation_id, 512)
            or type(self.attempt) is not int
            or self.attempt < 1
        ):
            raise ValueError("Durable event delivery request is invalid")
        digest_json(self.payload)

    def worker_payload(self) -> dict:
        return {
            "protocol": "forest.component-event-delivery/1",
            "event_id": self.event_id,
            "delivery_id": self.delivery_id,
            "subscriber_id": self.subscriber_id,
            "topic": self.topic,
            "contract": self.contract,
            "publisher_component_id": self.publisher_component_id,
            "publisher_generation_id": self.publisher_generation_id,
            "attempt": self.attempt,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class DurableComponentJobRequest:
    job_id: str
    handler_id: str
    scheduled_by_component_id: str
    scheduled_by_generation_id: str
    attempt: int
    payload: dict

    def __post_init__(self) -> None:
        if (
            not _bounded(self.job_id, 256)
            or _IDENTIFIER.fullmatch(self.handler_id) is None
            or _IDENTIFIER.fullmatch(self.scheduled_by_component_id) is None
            or not _bounded(self.scheduled_by_generation_id, 512)
            or type(self.attempt) is not int
            or self.attempt < 1
            or not isinstance(self.payload, dict)
        ):
            raise ValueError("Durable ComponentJob request is invalid")
        digest_json(self.payload)

    def worker_payload(self) -> dict:
        return {
            "protocol": "forest.component-job-delivery/1",
            "job_id": self.job_id,
            "handler_id": self.handler_id,
            "scheduled_by_component_id": self.scheduled_by_component_id,
            "scheduled_by_generation_id": self.scheduled_by_generation_id,
            "attempt": self.attempt,
            "payload": self.payload,
        }

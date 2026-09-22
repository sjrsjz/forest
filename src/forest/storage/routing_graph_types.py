"""Immutable route and claim values shared by routing authorities."""

from __future__ import annotations

import re
from dataclasses import dataclass

from forest.errors import ForestError

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_CONTRACT = re.compile(r"[1-9][0-9]*\.[0-9]+\Z")
_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")

def _bounded(value: object, field: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("ROUTING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ForestError("ROUTING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ForestError("ROUTING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _error_code(value: object) -> str:
    if not isinstance(value, str) or _ERROR_CODE.fullmatch(value) is None:
        raise ForestError("ROUTING_RESULT_INVALID", "Invalid routing error code", 422)
    return value


@dataclass(frozen=True)
class EventPublicationRoute:
    publisher_component_id: str
    publisher_generation_id: str
    topic: str
    contract: str
    event_schema_digest: str

    def validate(self) -> None:
        _identifier(self.publisher_component_id, "publisher_component_id")
        _bounded(self.publisher_generation_id, "publisher_generation_id", 512)
        _identifier(self.topic, "topic")
        if _CONTRACT.fullmatch(self.contract) is None:
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid event contract", 422)
        _digest(self.event_schema_digest, "event_schema_digest")


@dataclass(frozen=True)
class EventSubscriptionRoute:
    subscriber_id: str
    contract_major: int
    topic: str
    contract: str
    event_schema_digest: str
    owner_component_id: str
    owner_generation_id: str
    handler_service_id: str
    handler_service_version: int
    max_attempts: int

    def validate(self) -> None:
        _identifier(self.subscriber_id, "subscriber_id")
        _identifier(self.topic, "topic")
        _identifier(self.owner_component_id, "owner_component_id")
        _identifier(self.handler_service_id, "handler_service_id")
        _bounded(self.owner_generation_id, "owner_generation_id", 512)
        _digest(self.event_schema_digest, "event_schema_digest")
        if (
            type(self.contract_major) is not int
            or self.contract_major <= 0
            or _CONTRACT.fullmatch(self.contract) is None
            or int(self.contract.split(".", 1)[0]) != self.contract_major
            or type(self.handler_service_version) is not int
            or self.handler_service_version <= 0
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
        ):
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid subscription route", 422)


@dataclass(frozen=True)
class ComponentJobRoute:
    owner_component_id: str
    owner_generation_id: str
    handler_id: str
    input_schema_digest: str
    handler_service_id: str
    handler_service_version: int
    max_attempts: int
    timeout_seconds: float
    max_concurrency: int

    def validate(self) -> None:
        for value, field in (
            (self.owner_component_id, "owner_component_id"),
            (self.handler_id, "handler_id"),
            (self.handler_service_id, "handler_service_id"),
        ):
            _identifier(value, field)
        _bounded(self.owner_generation_id, "owner_generation_id", 512)
        _digest(self.input_schema_digest, "input_schema_digest")
        if (
            type(self.handler_service_version) is not int
            or self.handler_service_version <= 0
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0 < self.timeout_seconds <= 120
            or type(self.max_concurrency) is not int
            or not 1 <= self.max_concurrency <= 16
        ):
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid ComponentJob route", 422)


@dataclass(frozen=True)
class EventDeliveryClaim:
    delivery_id: str
    event_id: str
    subscriber_id: str
    topic: str
    contract: str
    publisher_component_id: str
    publisher_generation_id: str
    payload_id: str
    payload_digest: str
    causal_root_kind: str
    causal_root_id: str
    attempt_no: int
    attempt_id: str
    claim_token: str
    routing_generation_id: str
    target_generation_id: str
    handler_service_id: str
    handler_service_version: int
    max_attempts: int


@dataclass(frozen=True)
class ComponentJobClaim:
    job_id: str
    owner_component_id: str
    scheduled_by_generation_id: str
    handler_id: str
    input_payload_id: str
    input_payload_digest: str
    causal_root_kind: str
    causal_root_id: str
    attempt_no: int
    attempt_id: str
    claim_token: str
    routing_generation_id: str
    target_generation_id: str
    handler_service_id: str
    handler_service_version: int
    max_attempts: int
    timeout_seconds: float


def _validated_unique(items, key, message: str):
    values = tuple(items)
    identities = [key(item) for item in values]
    if len(identities) != len(set(identities)):
        raise ForestError("ROUTING_DIRECTORY_CONFLICT", message, 409)
    for item in values:
        item.validate()
    return tuple(sorted(values, key=key))


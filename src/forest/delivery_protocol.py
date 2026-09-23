"""Typed values crossing the kernel-to-Delivery package boundary."""

from __future__ import annotations

import re
from dataclasses import dataclass

from forest.errors import ForestError

_SHA256 = re.compile(r"sha256:[a-f0-9]{64}\Z")


def _text(value: object, field: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("DELIVERY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True)
class DeliveryPayloadRef:
    payload_id: str
    digest: str
    owner_component_id: str
    owner_kind: str
    owner_id: str

    def validate(self, field: str) -> None:
        _text(self.payload_id, f"{field}.payload_id", 256)
        if not isinstance(self.digest, str) or _SHA256.fullmatch(self.digest) is None:
            raise ForestError(
                "DELIVERY_INPUT_INVALID", f"Invalid {field}.digest", 422
            )
        _text(self.owner_component_id, f"{field}.owner_component_id", 128)
        _text(self.owner_kind, f"{field}.owner_kind", 128)
        _text(self.owner_id, f"{field}.owner_id", 512)

    def canonical(self) -> dict:
        self.validate("payload")
        return {
            "payload_id": self.payload_id,
            "digest": self.digest,
            "owner_component_id": self.owner_component_id,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
        }


@dataclass(frozen=True)
class DeliveryPart:
    part_operation_id: str
    effect_operation_ref: str
    payload: DeliveryPayloadRef

    def validate(self) -> None:
        _text(self.part_operation_id, "part_operation_id", 256)
        _text(self.effect_operation_ref, "effect_operation_ref", 256)
        if type(self.payload) is not DeliveryPayloadRef:
            raise ForestError(
                "DELIVERY_INPUT_INVALID", "Invalid part.payload", 422
            )
        self.payload.validate("part.payload")

    def canonical(self) -> dict:
        self.validate()
        return {
            "part_operation_id": self.part_operation_id,
            "effect_operation_ref": self.effect_operation_ref,
            "payload": self.payload.canonical(),
        }

"""Versioned in-process values for the model Provider contract."""

from __future__ import annotations

from dataclasses import dataclass

from forest.models import ModelCapabilities, PreparedModelRequest


@dataclass(frozen=True)
class ProviderPayloadRef:
    payload_id: str
    digest: str
    owner_component_id: str
    owner_kind: str
    owner_id: str

    def __post_init__(self) -> None:
        if (
            not all(
                isinstance(value, str) and value
                for value in (
                    self.payload_id,
                    self.owner_component_id,
                    self.owner_kind,
                    self.owner_id,
                )
            )
            or not isinstance(self.digest, str)
            or len(self.digest) != 71
            or not self.digest.startswith("sha256:")
        ):
            raise ValueError("Provider payload reference is invalid")

    def as_dict(self) -> dict:
        return {
            "payload_id": self.payload_id,
            "digest": self.digest,
            "owner_component_id": self.owner_component_id,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
        }


@dataclass(frozen=True)
class BoundProviderRequest:
    component_id: str
    generation_id: str
    provider_profile_ref: str
    capability_digest: str
    capabilities: ModelCapabilities
    prepared: PreparedModelRequest
    projection: ProviderPayloadRef
    wire: ProviderPayloadRef

    def __post_init__(self) -> None:
        if (
            not isinstance(self.component_id, str)
            or not self.component_id
            or not isinstance(self.generation_id, str)
            or not self.generation_id
            or not isinstance(self.provider_profile_ref, str)
            or not self.provider_profile_ref
            or not isinstance(self.capability_digest, str)
            or len(self.capability_digest) != 71
            or not self.capability_digest.startswith("sha256:")
            or not isinstance(self.capabilities, ModelCapabilities)
            or not isinstance(self.prepared, PreparedModelRequest)
            or not isinstance(self.projection, ProviderPayloadRef)
            or not isinstance(self.wire, ProviderPayloadRef)
            or self.projection.owner_component_id != self.component_id
            or self.projection.owner_kind != "model.projection"
            or self.projection.owner_id != self.prepared.request_id
            or self.wire.owner_component_id != self.component_id
            or self.wire.owner_kind != "model.wire"
            or self.wire.owner_id != self.prepared.transport_attempt_id
            or self.wire.digest != self.prepared.payload_digest
        ):
            raise ValueError("Bound Provider request identity is invalid")
        self.prepared.verify()

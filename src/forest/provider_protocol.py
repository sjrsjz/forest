"""Versioned in-process values for the model Provider contract."""

from __future__ import annotations

from dataclasses import dataclass

from forest.models import ModelCapabilities, PreparedModelRequest


@dataclass(frozen=True)
class BoundProviderRequest:
    component_id: str
    generation_id: str
    provider_profile_ref: str
    capability_digest: str
    capabilities: ModelCapabilities
    prepared: PreparedModelRequest

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
        ):
            raise ValueError("Bound Provider request identity is invalid")
        self.prepared.verify()

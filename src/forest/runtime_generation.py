"""Immutable contracts used to prepare one Bot runtime generation graph."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeStorageBinding:
    schema_id: str
    schema_version: str
    namespace: str
    baseline_sha256: str
    package_digest: str
    repository_contract: str
    execution_profile: str

    def as_fact(self) -> dict[str, str]:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "namespace": self.namespace,
            "baseline_sha256": self.baseline_sha256,
            "package_digest": self.package_digest,
            "repository_contract": self.repository_contract,
            "execution_profile": self.execution_profile,
        }


@dataclass(frozen=True)
class PreparedGenerationContext:
    """Registry-owned identities a module must echo in its prepared binding."""

    activation_id: str
    component_id: str
    requires: tuple[str, ...]
    package_id: str
    package_version: str
    package_digest: str
    driver: str
    trust_level: str
    descriptor_digest: str
    route_contract_digest: str
    management_contract_digest: str
    management_ui_digest: str
    state_schema: int
    state_contract_digest: str
    storage: RuntimeStorageBinding | None
    retention_requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreparedGenerationBinding:
    """Facts a prepared module freezes before it can enter a runtime graph."""

    component_id: str
    requires: tuple[str, ...]
    package_id: str
    package_version: str
    package_digest: str
    driver: str
    trust_level: str
    descriptor_digest: str
    route_contract_digest: str
    management_contract_digest: str
    management_ui_digest: str
    config_revision: int
    config_payload_id: str
    config_payload_digest: str
    instance_id: str
    worker_incarnation: str
    state_schema: int
    state_contract_digest: str
    capability_grants_digest: str
    data_admission: bool
    control_admission: bool
    storage: RuntimeStorageBinding | None = None
    retention_requires: tuple[str, ...] = ()

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.prepared-generation/1",
            "component_id": self.component_id,
            "requires": list(self.requires),
            "retention_requires": list(self.retention_requires),
            "package": {
                "package_id": self.package_id,
                "package_version": self.package_version,
                "package_digest": self.package_digest,
                "driver": self.driver,
                "trust_level": self.trust_level,
            },
            "descriptor_digest": self.descriptor_digest,
            "route_contract_digest": self.route_contract_digest,
            "management_contract_digest": self.management_contract_digest,
            "management_ui_digest": self.management_ui_digest,
            "configuration": {
                "revision": self.config_revision,
                "payload_id": self.config_payload_id,
                "payload_digest": self.config_payload_digest,
            },
            "instance_id": self.instance_id,
            "worker_incarnation": self.worker_incarnation,
            "state_schema": self.state_schema,
            "state_contract_digest": self.state_contract_digest,
            "capability_grants_digest": self.capability_grants_digest,
            "data_admission": self.data_admission,
            "control_admission": self.control_admission,
            "storage": self.storage.as_fact() if self.storage is not None else None,
        }


@dataclass(frozen=True)
class RuntimeGenerationGraph:
    graph_id: str
    graph_revision: int
    graph_digest: str
    activation_id: str
    generations: dict[str, str]


@dataclass(frozen=True)
class BoundGenerationIdentity:
    """Exact durable identity assigned to one prepared module generation."""

    component_id: str
    generation_id: str
    graph_id: str
    graph_revision: int
    graph_digest: str
    activation_id: str


@dataclass(frozen=True)
class ActiveGenerationRoute:
    """Route fact returned only while one exact Generation may admit work."""

    component_id: str
    generation_id: str
    route_epoch: int
    admission_lane: str
    graph_id: str
    graph_revision: int

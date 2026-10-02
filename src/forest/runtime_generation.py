"""Immutable contracts shared by runtime graphs in every module scope."""

from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import UUID

from forest.errors import ForestError

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RAW_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


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


def validate_prepared_bindings(
    bindings: tuple[PreparedGenerationBinding, ...] | list[PreparedGenerationBinding],
    activation_id: str,
    *,
    available_components: set[str] | None = None,
) -> list[PreparedGenerationBinding]:
    """Validate the scope-neutral facts before any Generation may be persisted."""

    try:
        UUID(activation_id)
    except (TypeError, ValueError, AttributeError):
        raise ForestError(
            "RUNTIME_ACTIVATION_ID_INVALID", "Runtime activation identity must be a UUID", 422
        ) from None
    ordered = sorted(bindings, key=lambda item: item.component_id)
    components = {item.component_id for item in ordered}
    required_components = components | (available_components or set())
    if (not ordered and not available_components) or len(components) != len(ordered):
        raise ForestError(
            "RUNTIME_GENERATION_BINDING_INVALID",
            "Runtime generation bindings must be unique and the Graph non-empty", 422,
        )
    for item in ordered:
        storage = item.storage
        strings = (
            item.component_id, item.package_id, item.package_version, item.driver,
            item.config_payload_id, item.instance_id, item.worker_incarnation,
        )
        digests = (
            item.package_digest, item.descriptor_digest, item.route_contract_digest,
            item.management_contract_digest, item.management_ui_digest,
            item.config_payload_digest, item.state_contract_digest,
            item.capability_grants_digest,
        )
        invalid_storage = storage is not None and (
            not all(
                isinstance(value, str) and value
                for value in (
                    storage.schema_id, storage.schema_version, storage.namespace,
                    storage.repository_contract,
                )
            )
            or not _RAW_DIGEST.fullmatch(storage.baseline_sha256)
            or not _DIGEST.fullmatch(storage.package_digest)
            or storage.execution_profile not in {"declarative", "official"}
        )
        if (
            not isinstance(item.component_id, str)
            or _IDENTIFIER.fullmatch(item.component_id) is None
            or any(not isinstance(value, str) or not value for value in strings)
            or any(not isinstance(value, str) or _DIGEST.fullmatch(value) is None for value in digests)
            or item.trust_level not in {"official", "installed"}
            or type(item.config_revision) is not int or item.config_revision <= 0
            or type(item.state_schema) is not int or item.state_schema <= 0
            or type(item.data_admission) is not bool
            or type(item.control_admission) is not bool
            or len(item.requires) != len(set(item.requires))
            or item.component_id in item.requires
            or set(item.requires) - required_components
            or len(item.retention_requires) != len(set(item.retention_requires))
            or item.component_id in item.retention_requires
            or set(item.retention_requires) - required_components
            or invalid_storage
        ):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_INVALID",
                f"Prepared Generation binding is invalid for {item.component_id}", 422,
            )
    return ordered

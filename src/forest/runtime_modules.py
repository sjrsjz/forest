"""Dependency-ordered runtime modules for Bot, deployment, and host scopes.

The supervisor consumes descriptors and lifecycle operations; concrete module
construction lives here instead of in the runtime adapter composition root.
Official and packaged modules converge on this registry contract without
making Agent or Memory kernel types. Bot hosts additionally persist their
generation graph; other scope hosts use the same lifecycle without pretending
to own a Bot kernel lease.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from typing import Any
from uuid import UUID, uuid4

from forest.errors import ForestError
from forest.packages import contract_matches, contract_range, digest_json
from forest.runtime_contracts import RuntimeContractRouter, RuntimeKernelServiceSpec
from forest.runtime_generation import (
    BoundGenerationIdentity,
    PreparedGenerationBinding,
    PreparedGenerationContext,
    RuntimeStorageBinding,
)
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)


@dataclass(frozen=True)
class RuntimeArtifactIdentity:
    """Immutable code/resource envelope that produced a runtime Generation."""

    package_id: str
    package_version: str
    package_digest: str
    scope: str
    driver: str
    trust_level: str

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.package_id)
            or not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z.+-]{0,127}", self.package_version)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", self.package_digest)
            or self.scope not in {"deployment", "host", "bot"}
            or not re.fullmatch(r"[a-z][a-z0-9._-]*/[1-9][0-9]*", self.driver)
            or self.trust_level not in {"official", "installed"}
        ):
            raise ValueError("Runtime artifact identity is invalid")

    def as_fact(self) -> dict:
        return {
            "package_id": self.package_id,
            "package_version": self.package_version,
            "package_digest": self.package_digest,
            "scope": self.scope,
            "driver": self.driver,
            "trust_level": self.trust_level,
        }


@lru_cache(maxsize=3)
def official_artifact(scope: str = "bot") -> RuntimeArtifactIdentity:
    """Hash the complete current Forest distribution, including trusted UI assets."""

    root = files("forest")
    captured: dict[str, str] = {}

    def walk(node, prefix: str = "") -> None:
        for child in sorted(node.iterdir(), key=lambda item: item.name):
            relative = f"{prefix}/{child.name}" if prefix else child.name
            if child.is_dir():
                if child.name != "__pycache__":
                    walk(child, relative)
            elif child.name != "RECORD" and child.suffix.lower() in {
                ".py",
                ".sql",
                ".html",
                ".css",
                ".js",
                ".json",
                ".toml",
            }:
                captured[relative] = hashlib.sha256(child.read_bytes()).hexdigest()

    walk(root)
    try:
        package_version = version("forest")
    except PackageNotFoundError:  # pragma: no cover - source tree without installation
        package_version = "0.1.0"
    package_digest = digest_json(
        {
            "protocol": "forest.official-distribution/1",
            "package_id": "forest",
            "package_version": package_version,
            "files": captured,
        }
    )
    return RuntimeArtifactIdentity(
        package_id="forest",
        package_version=package_version,
        package_digest=package_digest,
        scope=scope,
        driver="forest.in-process-trusted/1",
        trust_level="official",
    )


@dataclass(frozen=True)
class RuntimeManagementContribution:
    """One generation-bound workspace contribution from a runtime module."""

    contribution_id: str
    label: str
    scope_kinds: tuple[str, ...]
    template: str
    module: str
    view_protocol: str = "forest.trusted-management-view/1"
    style: str | None = None
    group: str = "module"
    order: int = 100

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.contribution_id):
            raise ValueError("Management contribution identity is invalid")
        if not self.label.strip() or any(ord(char) < 32 for char in self.label):
            raise ValueError("Management contribution labels must be printable")
        if (
            not self.scope_kinds
            or len(set(self.scope_kinds)) != len(self.scope_kinds)
            or any(
                scope not in {"deployment", "host", "bot", "binding", "component"}
                for scope in self.scope_kinds
            )
        ):
            raise ValueError("Management contribution scopes are invalid")
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", self.group):
            raise ValueError("Management contribution group is invalid")
        if type(self.order) is not int or not -10_000 <= self.order <= 10_000:
            raise ValueError("Management contribution order is invalid")
        for value, suffix in ((self.template, ".html"), (self.module, ".js")):
            if (
                not value.startswith("/static/")
                or not value.endswith(suffix)
                or ".." in value.split("/")
                or "\\" in value
            ):
                raise ValueError("Management contribution assets must use canonical /static paths")
        if self.view_protocol != "forest.trusted-management-view/1":
            raise ValueError("Unsupported runtime management view protocol")
        if self.style is not None and (
            not self.style.startswith("/static/")
            or not self.style.endswith(".css")
            or ".." in self.style.split("/")
            or "\\" in self.style
        ):
            raise ValueError("Runtime management styles must use canonical /static paths")

    def as_fact(self, *, public: bool = False) -> dict:
        surface = {
            "kind": "trusted-module",
            "protocol": self.view_protocol,
            "template": self.template,
            "module": self.module,
            "style": self.style,
        }
        return {
            "protocol": "forest.management-contribution/1",
            "contribution_id": self.contribution_id,
            "label": self.label,
            "scope_kinds": list(self.scope_kinds),
            "group": self.group,
            "order": self.order,
            "surface": surface
            if public
            else {key: value for key, value in surface.items() if key != "kind"},
        }


@dataclass(frozen=True)
class RuntimePackageManagementContribution:
    """One immutable package-owned UI loaded in an opaque-origin sandbox."""

    contribution_id: str
    label: str
    scope_kinds: tuple[str, ...]
    component_id: str
    artifact_sha256: str
    group: str = "configuration"
    order: int = 10

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.contribution_id):
            raise ValueError("Management contribution identity is invalid")
        if not self.label.strip() or any(ord(char) < 32 for char in self.label):
            raise ValueError("Management contribution labels must be printable")
        if (
            not self.scope_kinds
            or len(set(self.scope_kinds)) != len(self.scope_kinds)
            or any(
                scope not in {"deployment", "host", "bot", "binding", "component"}
                for scope in self.scope_kinds
            )
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.component_id)
            or not re.fullmatch(r"[0-9a-f]{64}", self.artifact_sha256)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", self.group)
            or type(self.order) is not int
            or not -10_000 <= self.order <= 10_000
        ):
            raise ValueError("Package management contribution is invalid")

    def as_fact(self, *, public: bool = False) -> dict:
        surface = {
            "protocol": "forest.management-ui/1",
            "artifact": (
                f"/management/v1/modules/{self.component_id}/management-ui/"
                f"{self.artifact_sha256}"
            ),
            "sha256": self.artifact_sha256,
        }
        if public:
            surface["kind"] = "sandboxed-package"
        return {
            "protocol": "forest.management-contribution/1",
            "contribution_id": self.contribution_id,
            "label": self.label,
            "scope_kinds": list(self.scope_kinds),
            "group": self.group,
            "order": self.order,
            "surface": surface,
        }


@dataclass(frozen=True)
class RuntimeAgentToolContribution:
    """One generation-bound Agent tool published by its executor module."""

    name: str
    service_id: str
    version: int
    declaration: dict
    approval_required: bool = True
    supports_query: bool = False

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,127}", self.name)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.service_id)
            or type(self.version) is not int
            or self.version < 1
            or type(self.approval_required) is not bool
            or type(self.supports_query) is not bool
            or not isinstance(self.declaration, dict)
            or self.declaration.get("name") != self.name
            or not isinstance(self.declaration.get("description"), str)
            or not self.declaration["description"].strip()
            or not isinstance(self.declaration.get("parameters"), dict)
        ):
            raise ValueError("Runtime Agent tool contribution is invalid")
        # Require a JSON-serializable declaration at descriptor construction.
        digest_json(self.declaration)

    @property
    def contract_digest(self) -> str:
        return digest_json(
            {
                "protocol": "forest.agent-tool/1",
                "service_id": self.service_id,
                "version": self.version,
                "declaration": self.declaration,
            }
        )

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.agent-tool-contribution/1",
            "name": self.name,
            "service_id": self.service_id,
            "version": self.version,
            "declaration": self.declaration,
            "contract_digest": self.contract_digest,
            "approval_required": self.approval_required,
            "supports_query": self.supports_query,
        }


@dataclass(frozen=True)
class RuntimeShellCommandContribution:
    """One command route, syntax contract and executor service owned by a module."""

    command_id: str
    command_version: int
    service_id: str
    service_version: int
    argument_schema: dict
    management_operation_id: str
    target_scope: str
    service_class: str
    required_capabilities: tuple[str, ...]
    option_aliases: tuple[tuple[str, str], ...] = ()
    output_policy: str = "SHORT"
    timeout_seconds: int = 10

    def __post_init__(self) -> None:
        properties = (
            self.argument_schema.get("properties", {})
            if isinstance(self.argument_schema, dict)
            else {}
        )
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.command_id)
            or type(self.command_version) is not int
            or self.command_version < 1
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.service_id)
            or type(self.service_version) is not int
            or self.service_version < 1
            or properties.get("protocol", {}).get("const")
            != "forest.shell.arguments/1"
            or properties.get("command_id", {}).get("const") != self.command_id
            or not isinstance(properties.get("arguments"), dict)
            or not re.fullmatch(
                r"[a-z0-9][a-z0-9._-]{0,255}", self.management_operation_id
            )
            or self.target_scope not in {"CONVERSATION", "BINDING", "BOT", "DEPLOYMENT"}
            or self.service_class not in {"P0", "P1"}
            or not self.required_capabilities
            or len(self.required_capabilities) != len(set(self.required_capabilities))
            or any(
                not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,199}", item)
                for item in self.required_capabilities
            )
            or len(self.option_aliases) != len(set(self.option_aliases))
            or any(
                not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", alias)
                or target not in properties["arguments"].get("properties", {})
                for alias, target in self.option_aliases
            )
            or self.output_policy not in {"SHORT", "ARTIFACT", "SILENT"}
            or type(self.timeout_seconds) is not int
            or not 1 <= self.timeout_seconds <= 900
        ):
            raise ValueError("Runtime Shell command contribution is invalid")
        digest_json(self.argument_schema)

    @property
    def argument_schema_digest(self) -> str:
        return digest_json(self.argument_schema)

    @property
    def required_capabilities_digest(self) -> str:
        return digest_json(
            {
                "protocol": "forest.management-capabilities/1",
                "capabilities": list(self.required_capabilities),
            }
        )

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.shell-command-contribution/1",
            "command_id": self.command_id,
            "command_version": self.command_version,
            "service_id": self.service_id,
            "service_version": self.service_version,
            "argument_schema": self.argument_schema,
            "argument_schema_digest": self.argument_schema_digest,
            "management_operation_id": self.management_operation_id,
            "target_scope": self.target_scope,
            "service_class": self.service_class,
            "required_capabilities": list(self.required_capabilities),
            "required_capabilities_digest": self.required_capabilities_digest,
            "option_aliases": [list(item) for item in self.option_aliases],
            "output_policy": self.output_policy,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass(frozen=True)
class RuntimeJobTypeContribution:
    """One durable job type executed by a generation-bound module service."""

    job_type: str
    job_version: int
    service_id: str
    service_version: int
    input_schema: dict
    max_attempts: int = 3
    timeout_seconds: int = 300
    max_concurrency: int = 1

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.job_type)
            or type(self.job_version) is not int
            or self.job_version < 1
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.service_id)
            or type(self.service_version) is not int
            or self.service_version < 1
            or not isinstance(self.input_schema, dict)
            or self.input_schema.get("$schema")
            != "https://json-schema.org/draft/2020-12/schema"
            or self.input_schema.get("type") != "object"
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
            or type(self.timeout_seconds) is not int
            or not 1 <= self.timeout_seconds <= 86_400
            or type(self.max_concurrency) is not int
            or not 1 <= self.max_concurrency <= 128
        ):
            raise ValueError("Runtime job type contribution is invalid")
        digest_json(self.input_schema)

    @property
    def input_schema_digest(self) -> str:
        return digest_json(self.input_schema)

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.job-type-contribution/1",
            "job_type": self.job_type,
            "job_version": self.job_version,
            "service_id": self.service_id,
            "service_version": self.service_version,
            "input_schema": self.input_schema,
            "input_schema_digest": self.input_schema_digest,
            "max_attempts": self.max_attempts,
            "timeout_seconds": self.timeout_seconds,
            "max_concurrency": self.max_concurrency,
        }


@dataclass(frozen=True)
class RuntimeEventSubscriptionContribution:
    """One durable logical subscriber implemented by a generation-bound service."""

    subscriber_id: str
    topic: str
    contract: str
    event_schema: dict
    service_id: str
    service_version: int = 1
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.subscriber_id)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.topic)
            or not re.fullmatch(r"[1-9][0-9]*\.[0-9]+", self.contract)
            or not isinstance(self.event_schema, dict)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.service_id)
            or type(self.service_version) is not int
            or self.service_version < 1
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
        ):
            raise ValueError("Runtime event subscription contribution is invalid")
        digest_json(self.event_schema)

    @property
    def contract_major(self) -> int:
        return int(self.contract.split(".", 1)[0])

    @property
    def event_schema_digest(self) -> str:
        return digest_json(self.event_schema)

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.event-subscription-contribution/1",
            "subscriber_id": self.subscriber_id,
            "topic": self.topic,
            "contract": self.contract,
            "contract_major": self.contract_major,
            "event_schema": self.event_schema,
            "event_schema_digest": self.event_schema_digest,
            "service_id": self.service_id,
            "service_version": self.service_version,
            "max_attempts": self.max_attempts,
        }


@dataclass(frozen=True)
class RuntimeEventPublicationContribution:
    """One event contract a generation-bound component may publish."""

    topic: str
    contract: str
    event_schema: dict

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.topic)
            or not re.fullmatch(r"[1-9][0-9]*\.[0-9]+", self.contract)
            or not isinstance(self.event_schema, dict)
        ):
            raise ValueError("Runtime event publication contribution is invalid")
        digest_json(self.event_schema)

    @property
    def event_schema_digest(self) -> str:
        return digest_json(self.event_schema)

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.event-publication-contribution/1",
            "topic": self.topic,
            "contract": self.contract,
            "event_schema": self.event_schema,
            "event_schema_digest": self.event_schema_digest,
        }


@dataclass(frozen=True)
class RuntimeComponentJobContribution:
    """One component-owned durable job handler bound to its package Generation."""

    handler_id: str
    input_schema: dict
    service_id: str
    service_version: int = 1
    max_attempts: int = 3
    timeout_seconds: float = 30
    max_concurrency: int = 1

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.handler_id)
            or not isinstance(self.input_schema, dict)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.service_id)
            or type(self.service_version) is not int
            or self.service_version < 1
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0 < self.timeout_seconds <= 120
            or type(self.max_concurrency) is not int
            or not 1 <= self.max_concurrency <= 16
        ):
            raise ValueError("Runtime ComponentJob contribution is invalid")
        digest_json(self.input_schema)

    @property
    def input_schema_digest(self) -> str:
        return digest_json(self.input_schema)

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.component-job-contribution/1",
            "handler_id": self.handler_id,
            "input_schema": self.input_schema,
            "input_schema_digest": self.input_schema_digest,
            "service_id": self.service_id,
            "service_version": self.service_version,
            "max_attempts": self.max_attempts,
            "timeout_seconds": self.timeout_seconds,
            "max_concurrency": self.max_concurrency,
        }


def configuration_management_contribution(
    label: str = "配置",
) -> RuntimeManagementContribution:
    """Declare the shared trusted renderer for a module-owned configuration contract."""

    return RuntimeManagementContribution(
        "configuration",
        label,
        ("bot",),
        "/static/modules/clean-configuration.html",
        "/static/clean-configuration.js",
        style="/static/modules/clean-configuration.css",
        group="configuration",
        order=10,
    )


@dataclass(frozen=True)
class RuntimeStorageBundle:
    """Verified immutable SQL owned by one runtime module generation."""

    spec: StorageBundleSpec
    baseline: bytes
    package_digest: str
    execution_profile: str = "declarative"

    def __post_init__(self) -> None:
        if self.execution_profile == "declarative":
            validate_storage_baseline(self.baseline, self.spec.baseline_sha256)
        elif self.execution_profile == "official":
            validate_official_storage_baseline(self.baseline, self.spec.baseline_sha256)
        else:
            raise ForestError("INVALID_STORAGE_BUNDLE", "Storage execution profile is invalid", 422)
        if not isinstance(self.package_digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", self.package_digest
        ):
            raise ForestError("INVALID_STORAGE_BUNDLE", "Storage package digest is invalid", 422)

    def as_fact(self) -> dict:
        return {
            **self.spec.as_fact(),
            "package_digest": self.package_digest,
            "execution_profile": self.execution_profile,
        }


def official_storage_bundle(
    component_id: str,
    schema_id: str,
    repository_contract: str,
    resource: str,
) -> RuntimeStorageBundle:
    """Bind one official baseline to a storage-only content identity.

    Runtime code and trusted management assets belong to the Generation artifact
    digest.  The schema ledger must only change when this module's baseline or
    storage contract changes; otherwise an unrelated UI edit would make an
    unchanged database look incompatible.
    """

    common = files("forest").joinpath("storage/official/module_configuration.sql").read_bytes()
    owned = files("forest").joinpath(resource).read_bytes()
    baseline = common + b"\n" + owned
    spec = StorageBundleSpec(
        component_id=component_id,
        schema_id=schema_id,
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract=repository_contract,
    )
    storage_digest = digest_json(
        {
            "protocol": "forest.official-storage-bundle/1",
            "package_id": "forest",
            "component_id": component_id,
            "schema": spec.as_fact(),
            "execution_profile": "official",
        }
    )
    return RuntimeStorageBundle(
        spec,
        baseline,
        storage_digest,
        execution_profile="official",
    )


class OwnedWorkLifecycle(StrEnum):
    """Whether a module owns durable work that must drain before quiescence."""

    NONE = "NONE"
    FREEZE_DRAIN = "FREEZE_DRAIN"


@dataclass(frozen=True)
class RuntimeModuleDescriptor:
    component_id: str
    requires: tuple[str, ...]
    factory: Callable[[Any, dict[str, Any]], Any]
    artifact: RuntimeArtifactIdentity
    management: tuple[
        RuntimeManagementContribution | RuntimePackageManagementContribution, ...
    ] = ()
    storage: RuntimeStorageBundle | None = None
    wakes_on_work: bool = False
    work_notify_order: int = 100
    kernel_services: tuple[RuntimeKernelServiceSpec, ...] = ()
    agent_tools: tuple[RuntimeAgentToolContribution, ...] = ()
    shell_commands: tuple[RuntimeShellCommandContribution, ...] = ()
    job_types: tuple[RuntimeJobTypeContribution, ...] = ()
    event_publications: tuple[RuntimeEventPublicationContribution, ...] = ()
    event_subscriptions: tuple[RuntimeEventSubscriptionContribution, ...] = ()
    component_jobs: tuple[RuntimeComponentJobContribution, ...] = ()
    owned_work_lifecycle: OwnedWorkLifecycle = OwnedWorkLifecycle.NONE
    provided_services: tuple[tuple[str, str], ...] = ()
    startup_service_requires: tuple[tuple[str, str], ...] = ()
    retention_requires: tuple[str, ...] = ()
    retention_service_requires: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        identities = [item.contribution_id for item in self.management]
        if len(identities) != len(set(identities)):
            raise ValueError("Runtime management contribution identities must be unique")
        if type(self.wakes_on_work) is not bool:
            raise ValueError("Runtime work notification declaration must be boolean")
        if type(self.work_notify_order) is not int or not 0 <= self.work_notify_order <= 10_000:
            raise ValueError("Runtime work notification order is invalid")
        service_keys = [(item.service_id, item.version) for item in self.kernel_services]
        if len(service_keys) != len(set(service_keys)):
            raise ValueError("Kernel service contribution identities must be unique")
        tool_names = [item.name for item in self.agent_tools]
        tool_services = [(item.service_id, item.version) for item in self.agent_tools]
        if len(tool_names) != len(set(tool_names)) or len(tool_services) != len(
            set(tool_services)
        ):
            raise ValueError("Runtime Agent tool contribution identities must be unique")
        command_keys = [
            (item.command_id, item.command_version) for item in self.shell_commands
        ]
        if len(command_keys) != len(set(command_keys)):
            raise ValueError("Runtime Shell command contribution identities must be unique")
        job_keys = [(item.job_type, item.job_version) for item in self.job_types]
        if len(job_keys) != len(set(job_keys)):
            raise ValueError("Runtime job type contribution identities must be unique")
        publication_keys = [
            (item.topic, item.contract) for item in self.event_publications
        ]
        if len(publication_keys) != len(set(publication_keys)):
            raise ValueError("Runtime event publication identities must be unique")
        subscription_keys = [
            (item.subscriber_id, item.contract_major) for item in self.event_subscriptions
        ]
        if len(subscription_keys) != len(set(subscription_keys)):
            raise ValueError("Runtime event subscription identities must be unique")
        component_job_ids = [item.handler_id for item in self.component_jobs]
        if len(component_job_ids) != len(set(component_job_ids)):
            raise ValueError("Runtime ComponentJob identities must be unique")
        if not isinstance(self.owned_work_lifecycle, OwnedWorkLifecycle):
            raise ValueError("Runtime owned-work lifecycle declaration is invalid")
        if len(self.provided_services) != len(set(self.provided_services)):
            raise ValueError("Runtime provided service identities must be unique")
        if len(self.startup_service_requires) != len(set(self.startup_service_requires)):
            raise ValueError("Runtime startup service requirements must be unique")
        if (
            len(self.retention_requires) != len(set(self.retention_requires))
            or self.component_id in self.retention_requires
            or len(self.retention_service_requires)
            != len(set(self.retention_service_requires))
        ):
            raise ValueError("Runtime retention requirements must be unique and external")


class ModuleLifecycleState(StrEnum):
    DISCOVERED = "DISCOVERED"
    VALIDATED = "VALIDATED"
    PREPARED = "PREPARED"
    ACTIVE = "ACTIVE"
    QUIESCING = "QUIESCING"
    QUIESCENT = "QUIESCENT"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


_TRANSITIONS = {
    ModuleLifecycleState.DISCOVERED: {ModuleLifecycleState.VALIDATED},
    ModuleLifecycleState.VALIDATED: {
        ModuleLifecycleState.PREPARED,
        ModuleLifecycleState.STOPPED,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.PREPARED: {
        ModuleLifecycleState.ACTIVE,
        ModuleLifecycleState.STOPPED,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.ACTIVE: {
        ModuleLifecycleState.QUIESCING,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.QUIESCING: {
        ModuleLifecycleState.QUIESCENT,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.QUIESCENT: {
        ModuleLifecycleState.ACTIVE,
        ModuleLifecycleState.STOPPED,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.FAILED: {ModuleLifecycleState.STOPPED},
    ModuleLifecycleState.STOPPED: set(),
}


class RuntimeModuleRegistry:
    """Build and supervise modules from an acyclic descriptor graph."""

    def __init__(
        self,
        runtime,
        descriptors: tuple[RuntimeModuleDescriptor, ...],
        *,
        activation_id: str | None = None,
    ):
        self.runtime = runtime
        self.descriptors = tuple(descriptors)
        self.instances: dict[str, Any] = {}
        self.generation_bindings: dict[str, PreparedGenerationBinding] = {}
        self.order: list[str] = []
        self.states: dict[str, ModuleLifecycleState] = {
            descriptor.component_id: ModuleLifecycleState.DISCOVERED
            for descriptor in self.descriptors
        }
        self._lifecycle_lock = None
        self._closed = False
        self._prewarmed = False
        self._contract_routes_sealed = False
        self._replacement_quiesced: tuple[str, ...] = ()
        self._replacement_owned_frozen: tuple[str, ...] = ()
        self._replacement_operation_id: str | None = None
        self._replacement_components: tuple[str, ...] | None = None
        self._generation_identities_bound = False
        self.graph_identity = None
        self.activation_id = str(uuid4()) if activation_id is None else activation_id
        try:
            UUID(self.activation_id)
        except (TypeError, ValueError):
            raise ForestError(
                "RUNTIME_ACTIVATION_ID_INVALID", "Runtime activation identity is invalid", 422
            ) from None
        component_ids = {descriptor.component_id for descriptor in self.descriptors}
        self._component_ids = component_ids
        dynamic_requires = {component_id: set() for component_id in component_ids}
        if "agent.runtime" in component_ids:
            dynamic_requires["agent.runtime"].update(
                descriptor.component_id
                for descriptor in self.descriptors
                if descriptor.agent_tools and descriptor.component_id != "agent.runtime"
            )
        if "shell.command" in component_ids:
            dynamic_requires["shell.command"].update(
                descriptor.component_id
                for descriptor in self.descriptors
                if descriptor.shell_commands and descriptor.component_id != "shell.command"
            )
        if "job.scheduler" in component_ids:
            dynamic_requires["job.scheduler"].update(
                descriptor.component_id
                for descriptor in self.descriptors
                if descriptor.job_types and descriptor.component_id != "job.scheduler"
            )
        self._dynamic_requires = {
            component_id: tuple(sorted(required))
            for component_id, required in dynamic_requires.items()
        }
        providers: dict[tuple[str, int], tuple[str, str]] = {}
        for descriptor in self.descriptors:
            for service_id, service_version in descriptor.provided_services:
                major = contract_range(service_version)[0][0]
                key = (service_id, major)
                if key in providers:
                    raise ForestError(
                        "RUNTIME_SERVICE_PROVIDER_CONFLICT",
                        "Runtime service has no unique provider endpoint",
                        409,
                        details={"service_id": service_id, "version": major},
                    )
                providers[key] = (descriptor.component_id, service_version)
        self._service_startup_requires = {}
        self._service_retention_requires = {}
        for descriptor in self.descriptors:
            for requirements, destination, missing_message in (
                (
                    descriptor.startup_service_requires,
                    self._service_startup_requires,
                    "Startup service has no provider satisfying its contract",
                ),
                (
                    descriptor.retention_service_requires,
                    self._service_retention_requires,
                    "Retained service has no provider satisfying its contract",
                ),
            ):
                required_components = set()
                for service_id, contract in requirements:
                    lower_major = contract_range(contract)[0][0]
                    provider = providers.get((service_id, lower_major))
                    if provider is None or not contract_matches(contract, provider[1]):
                        raise ForestError(
                            "RUNTIME_SERVICE_DEPENDENCY_MISSING",
                            missing_message,
                            422,
                            details={
                                "component_id": descriptor.component_id,
                                "service_id": service_id,
                            },
                        )
                    if provider[0] != descriptor.component_id:
                        required_components.add(provider[0])
                destination[descriptor.component_id] = tuple(sorted(required_components))
        self.contract_router = RuntimeContractRouter(self)
        self.scope_kind = getattr(runtime, "scope_kind", "bot")
        self.generation_authority = getattr(runtime, "generation_authority", None)
        self._build()

    def _effective_requires(self, descriptor: RuntimeModuleDescriptor) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    *descriptor.requires,
                    *self._service_startup_requires[descriptor.component_id],
                    *self._dynamic_requires[descriptor.component_id],
                )
            )
        )

    def _retention_requires(self, descriptor: RuntimeModuleDescriptor) -> tuple[str, ...]:
        return tuple(
            sorted(
                (
                    set(descriptor.retention_requires)
                    | set(self._service_retention_requires[descriptor.component_id])
                )
                & self._component_ids
                - set(self._effective_requires(descriptor))
                - {descriptor.component_id}
            )
        )

    def replacement_closure(self, component_id: str) -> tuple[str, ...]:
        """Include every Generation whose frozen references touch the target."""

        if component_id not in self.states:
            raise ForestError(
                "RUNTIME_MODULE_NOT_FOUND", "Reload target is not in this RuntimeGraph", 404
            )
        affected = {component_id}
        while True:
            expanded = affected | {
                descriptor.component_id
                for descriptor in self.descriptors
                if (
                    set(self._effective_requires(descriptor))
                    | set(self._retention_requires(descriptor))
                ) & affected
            }
            if expanded == affected:
                return tuple(item for item in self.order if item in affected)
            affected = expanded

    def _descriptor_fact(self, descriptor: RuntimeModuleDescriptor) -> dict:
        instance = self.instances.get(descriptor.component_id)
        contract = getattr(instance, "management_contract", None)
        fact = {
            "protocol": "forest.runtime-module/1",
            "component_id": descriptor.component_id,
            "requires": list(self._effective_requires(descriptor)),
            "retention_requires": list(self._retention_requires(descriptor)),
            "factory_ref": (
                "forest.package-generation-adapter/1"
                if descriptor.artifact.driver == "forest.python-worker/1"
                else descriptor.factory.__module__ + ":" + descriptor.factory.__qualname__
            ),
            "artifact": descriptor.artifact.as_fact(),
            "management_contributions": [
                contribution.as_fact() for contribution in descriptor.management
            ],
            "management_contract": contract.describe() if contract is not None else None,
            "runtime_contract": getattr(instance, "runtime_contract", None),
            "state_schema": getattr(instance, "state_schema", None),
            "state_contract": getattr(instance, "state_contract", None),
            "storage": descriptor.storage.as_fact() if descriptor.storage is not None else None,
            "wakes_on_work": descriptor.wakes_on_work,
            "work_notify_order": descriptor.work_notify_order,
            "kernel_services": [item.as_fact() for item in descriptor.kernel_services],
            "provided_services": [list(item) for item in descriptor.provided_services],
            "startup_service_requires": [
                list(item) for item in descriptor.startup_service_requires
            ],
            "agent_tools": [item.as_fact() for item in descriptor.agent_tools],
            "shell_commands": [item.as_fact() for item in descriptor.shell_commands],
            "job_types": [item.as_fact() for item in descriptor.job_types],
            "event_publications": [
                item.as_fact() for item in descriptor.event_publications
            ],
            "event_subscriptions": [
                item.as_fact() for item in descriptor.event_subscriptions
            ],
            "component_jobs": [item.as_fact() for item in descriptor.component_jobs],
            "owned_work_lifecycle": descriptor.owned_work_lifecycle,
        }
        return {**fact, "descriptor_digest": digest_json(fact)}

    async def _prepare_generation_binding(
        self, descriptor: RuntimeModuleDescriptor, instance: Any
    ) -> PreparedGenerationBinding:
        prepare = getattr(instance, "prepare_generation", None)
        if (
            prepare is None
            or getattr(instance, "bind_generation", None) is None
            or getattr(instance, "activate_generation", None) is None
            or not hasattr(instance, "runtime_contract")
            or not hasattr(instance, "state_contract")
            or not hasattr(instance, "state_schema")
            or not hasattr(instance, "module_configuration")
        ):
            raise ForestError(
                "RUNTIME_GENERATION_PREPARE_UNAVAILABLE",
                f"Module {descriptor.component_id} does not expose the clean preparation contract",
                503,
            )
        fact = self._descriptor_fact(descriptor)
        management_contract = fact["management_contract"]
        contributions = fact["management_contributions"]
        storage = (
            RuntimeStorageBinding(
                descriptor.storage.spec.schema_id,
                descriptor.storage.spec.schema_version,
                descriptor.storage.spec.namespace,
                descriptor.storage.spec.baseline_sha256,
                descriptor.storage.package_digest,
                descriptor.storage.spec.repository_contract,
                descriptor.storage.execution_profile,
            )
            if descriptor.storage is not None
            else None
        )
        context = PreparedGenerationContext(
            activation_id=self.activation_id,
            component_id=descriptor.component_id,
            requires=self._effective_requires(descriptor),
            retention_requires=self._retention_requires(descriptor),
            package_id=descriptor.artifact.package_id,
            package_version=descriptor.artifact.package_version,
            package_digest=descriptor.artifact.package_digest,
            driver=descriptor.artifact.driver,
            trust_level=descriptor.artifact.trust_level,
            descriptor_digest=fact["descriptor_digest"],
            route_contract_digest=digest_json(instance.runtime_contract),
            management_contract_digest=digest_json(management_contract),
            management_ui_digest=digest_json(contributions),
            state_schema=instance.state_schema,
            state_contract_digest=digest_json(instance.state_contract),
            storage=storage,
        )
        binding = prepare(context)
        if inspect.isawaitable(binding):
            binding = await binding
        if not isinstance(binding, PreparedGenerationBinding):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_INVALID",
                f"Module {descriptor.component_id} returned no prepared Generation binding",
                503,
            )
        expected = {
            "component_id": context.component_id,
            "requires": context.requires,
            "retention_requires": context.retention_requires,
            "package_id": context.package_id,
            "package_version": context.package_version,
            "package_digest": context.package_digest,
            "driver": context.driver,
            "trust_level": context.trust_level,
            "descriptor_digest": context.descriptor_digest,
            "route_contract_digest": context.route_contract_digest,
            "management_contract_digest": context.management_contract_digest,
            "management_ui_digest": context.management_ui_digest,
            "state_schema": context.state_schema,
            "state_contract_digest": context.state_contract_digest,
            "storage": context.storage,
        }
        if any(getattr(binding, key) != value for key, value in expected.items()):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                f"Module {descriptor.component_id} changed Registry-owned Generation facts",
                409,
            )
        return binding

    async def _bind_generation_identities(self) -> None:
        if self.generation_authority is None or self._generation_identities_bound:
            return
        if self.graph_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "A prepared runtime graph is required before Generation identities can bind",
                503,
            )
        for component_id in self.order:
            identity = BoundGenerationIdentity(
                component_id=component_id,
                generation_id=self.graph_identity.generations[component_id],
                graph_id=self.graph_identity.graph_id,
                graph_revision=self.graph_identity.graph_revision,
                graph_digest=self.graph_identity.graph_digest,
                activation_id=self.graph_identity.activation_id,
            )
            bound = self.instances[component_id].bind_generation(identity)
            if inspect.isawaitable(bound):
                await bound
        self._generation_identities_bound = True

    def _lease(self, *, allow_closing=False):
        if allow_closing and hasattr(self.runtime, "_cleanup_lease"):
            return self.runtime._cleanup_lease()
        return self.runtime.kernel._authority(allow_closing=allow_closing)

    @staticmethod
    def _lost_persistence_authority(exc):
        return isinstance(exc, ForestError) and exc.code in {
            "STALE_HOST",
            "KERNEL_UNAVAILABLE",
            "STORAGE_SEALED",
        }

    async def _record_current_states(self, lease, *, failure_code=None):
        if self.generation_authority is None or self.graph_identity is None:
            return
        states = {}
        for component_id, state in self.states.items():
            if state == ModuleLifecycleState.FAILED:
                states[component_id] = ("FAILED", failure_code or "MODULE_LIFECYCLE_FAILED")
            elif state == ModuleLifecycleState.STOPPED:
                states[component_id] = ("STOPPED", None)
            elif state in {ModuleLifecycleState.QUIESCING, ModuleLifecycleState.QUIESCENT}:
                states[component_id] = ("QUIESCING", None)
            elif state == ModuleLifecycleState.ACTIVE:
                states[component_id] = ("ACTIVE", None)
            else:
                states[component_id] = ("PREPARED", None)
        await self.generation_authority.record_states(lease, self.graph_identity, states)

    def _transition(self, component_id, state):
        current = self.states[component_id]
        if state not in _TRANSITIONS[current]:
            raise ForestError(
                "RUNTIME_MODULE_LIFECYCLE_INVALID",
                f"Module {component_id} cannot move from {current} to {state}",
            )
        self.states[component_id] = state

    def _build(self) -> None:
        remaining = {item.component_id: item for item in self.descriptors}
        if len(remaining) != len(self.descriptors):
            raise ForestError(
                "DUPLICATE_RUNTIME_MODULE", "Runtime module identities must be unique"
            )
        tool_names = [
            tool.name for descriptor in self.descriptors for tool in descriptor.agent_tools
        ]
        tool_services = [
            (tool.service_id, tool.version)
            for descriptor in self.descriptors
            for tool in descriptor.agent_tools
        ]
        if len(tool_names) != len(set(tool_names)) or len(tool_services) != len(
            set(tool_services)
        ):
            raise ForestError(
                "RUNTIME_AGENT_TOOL_CONFLICT",
                "Runtime Agent tool names and services must be graph-unique",
                409,
            )
        command_keys = [
            (command.command_id, command.command_version)
            for descriptor in self.descriptors
            for command in descriptor.shell_commands
        ]
        if len(command_keys) != len(set(command_keys)):
            raise ForestError(
                "RUNTIME_SHELL_COMMAND_CONFLICT",
                "Runtime Shell command identities must be graph-unique",
                409,
            )
        job_keys = [
            (job.job_type, job.job_version)
            for descriptor in self.descriptors
            for job in descriptor.job_types
        ]
        job_services = [
            ((job.service_id, job.service_version), descriptor.component_id)
            for descriptor in self.descriptors
            for job in descriptor.job_types
        ]
        service_owners: dict[tuple[str, int], set[str]] = {}
        for service_key, owner in job_services:
            service_owners.setdefault(service_key, set()).add(owner)
        if len(job_keys) != len(set(job_keys)) or any(
            len(owners) > 1 for owners in service_owners.values()
        ):
            raise ForestError(
                "RUNTIME_JOB_TYPE_CONFLICT",
                "Runtime job types and executor services must be graph-unique",
                409,
            )
        subscription_keys = [
            (subscription.subscriber_id, subscription.contract_major)
            for descriptor in self.descriptors
            for subscription in descriptor.event_subscriptions
        ]
        if len(subscription_keys) != len(set(subscription_keys)):
            raise ForestError(
                "RUNTIME_EVENT_SUBSCRIPTION_CONFLICT",
                "Durable subscriber identities must be graph-unique",
                409,
            )
        event_schemas: dict[tuple[str, str], str] = {}
        for descriptor in self.descriptors:
            contracts = (
                *descriptor.event_publications,
                *descriptor.event_subscriptions,
            )
            for contract in contracts:
                key = (contract.topic, contract.contract)
                digest = contract.event_schema_digest
                existing = event_schemas.setdefault(key, digest)
                if existing != digest:
                    raise ForestError(
                        "RUNTIME_EVENT_CONTRACT_CONFLICT",
                        "One exact event contract has multiple schemas",
                        409,
                    )
        for descriptor in self.descriptors:
            identity = descriptor.component_id
            if (
                not identity
                or len(identity) > 128
                or identity[0] not in "abcdefghijklmnopqrstuvwxyz0123456789"
                or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for char in identity)
                or len(set(descriptor.requires)) != len(descriptor.requires)
                or identity in descriptor.requires
                or descriptor.artifact.scope != self.scope_kind
                or (
                    descriptor.storage is not None
                    and (
                        descriptor.storage.spec.component_id != identity
                        or (
                            descriptor.storage.execution_profile != "official"
                            and descriptor.storage.package_digest
                            != descriptor.artifact.package_digest
                        )
                        or (
                            descriptor.storage.execution_profile == "official"
                            and descriptor.artifact.trust_level != "official"
                        )
                    )
                )
            ):
                raise ForestError(
                    "RUNTIME_MODULE_DESCRIPTOR_INVALID", "Runtime module descriptor is invalid"
                )
            self._transition(identity, ModuleLifecycleState.VALIDATED)
        resolved = set()
        while remaining:
            ready = sorted(
                (
                    item
                    for item in remaining.values()
                    if all(
                        required in resolved
                        for required in self._effective_requires(item)
                    )
                ),
                key=lambda item: item.component_id,
            )
            if not ready:
                raise ForestError(
                    "RUNTIME_MODULE_DEPENDENCY_CYCLE",
                    "Runtime module dependencies are missing or cyclic",
                )
            for descriptor in ready:
                self.order.append(descriptor.component_id)
                resolved.add(descriptor.component_id)
                remaining.pop(descriptor.component_id)

    async def _install_storage(self, descriptor: RuntimeModuleDescriptor) -> None:
        if descriptor.storage is None:
            return
        installer = getattr(self.runtime, "schema_bundle_installer", None)
        install = getattr(installer, "install", None)
        if install is None:
            raise ForestError(
                "SCHEMA_BUNDLE_INSTALLER_UNAVAILABLE",
                "A storage-owning runtime module requires the host schema installer",
                503,
            )
        await install(
            descriptor.storage.spec,
            descriptor.storage.baseline,
            descriptor.storage.package_digest,
            execution_profile=descriptor.storage.execution_profile,
        )

    async def _prepare_modules(self) -> None:
        for component_id in self.order:
            if self.states[component_id] == ModuleLifecycleState.PREPARED:
                continue
            if self.states[component_id] != ModuleLifecycleState.VALIDATED:
                raise ForestError(
                    "RUNTIME_MODULE_LIFECYCLE_INVALID",
                    "Runtime module graph cannot be prepared from its current state",
                )
            descriptor = next(
                item for item in self.descriptors if item.component_id == component_id
            )
            try:
                await self._install_storage(descriptor)
                dependencies = self.contract_router.context(component_id)
                instance = descriptor.factory(self.runtime, dependencies)
                self._validate_owned_work_lifecycle(descriptor, instance)
                self.instances[component_id] = instance
                self.contract_router.register(component_id, instance)
                if self.generation_authority is not None:
                    self.generation_bindings[component_id] = await self._prepare_generation_binding(
                        descriptor, instance
                    )
                self._transition(component_id, ModuleLifecycleState.PREPARED)
            except BaseException:
                self._transition(component_id, ModuleLifecycleState.FAILED)
                raise

    async def _rollback_preparation(self) -> list[Exception]:
        errors = []
        for component_id in reversed(self.order):
            state = self.states[component_id]
            if state == ModuleLifecycleState.PREPARED:
                try:
                    await self._cleanup(component_id)
                except Exception as exc:
                    errors.append(exc)
            elif state == ModuleLifecycleState.FAILED:
                if component_id in self.instances:
                    try:
                        await self._cleanup(component_id)
                    except Exception as exc:
                        errors.append(exc)
                else:
                    self._transition(component_id, ModuleLifecycleState.STOPPED)
            elif state == ModuleLifecycleState.VALIDATED:
                self._transition(component_id, ModuleLifecycleState.STOPPED)
        return errors

    def get(self, component_id: str):
        return self.instances[component_id]

    def notify_work(self) -> None:
        """Wake active modules that declared the generic durable-work signal."""

        declared = sorted(
            (item for item in self.descriptors if item.wakes_on_work),
            key=lambda item: (item.work_notify_order, item.component_id),
        )
        for descriptor in declared:
            if self.states[descriptor.component_id] == ModuleLifecycleState.ACTIVE:
                self.instances[descriptor.component_id].notify()

    async def _prepare_graph(self):
        lease = None
        try:
            if self.generation_authority is None:
                raise ForestError(
                    "RUNTIME_GENERATION_AUTHORITY_REQUIRED",
                    "Runtime module lifecycle requires a Generation authority",
                    503,
                )
            await self._prepare_modules()
            lease = self._lease()
            if self.graph_identity is None:
                self.graph_identity = await self.generation_authority.prepare_graph(
                    lease,
                    [self.generation_bindings[item] for item in self.order],
                    self.activation_id,
                )
            await self._bind_generation_identities()
            if not self._contract_routes_sealed:
                for descriptor in self.descriptors:
                    for spec in descriptor.kernel_services:
                        if descriptor.artifact.trust_level != "official":
                            raise ForestError(
                                "RUNTIME_KERNEL_SERVICE_FORBIDDEN",
                                "Installed modules cannot contribute in-process kernel services",
                                403,
                            )
                        self.contract_router.register_kernel_service(spec)
                self.contract_router.seal()
                self._contract_routes_sealed = True
            return lease
        except BaseException as exc:
            rollback_errors = await self._rollback_preparation()
            if lease is not None and self.graph_identity is not None:
                try:
                    await self._record_current_states(
                        lease,
                        failure_code=(
                            exc.code
                            if isinstance(exc, ForestError)
                            else "RUNTIME_MODULE_PREPARE_FAILED"
                        ),
                    )
                except Exception as rollback_error:
                    rollback_errors.append(rollback_error)
            if rollback_errors:
                raise ForestError(
                    "RUNTIME_MODULE_PREPARE_ROLLBACK_FAILED",
                    "Runtime module preparation failed and resources remain owned",
                    503,
                    details={"cleanup_errors": len(rollback_errors)},
                ) from exc
            raise

    async def prepare(self) -> None:
        """Install storage and construct the graph without admitting runtime work."""

        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                raise ForestError(
                    "RUNTIME_MODULE_REGISTRY_CLOSED", "Runtime module registry is closed", 503
                )
            if all(
                self.states[item] in {ModuleLifecycleState.PREPARED, ModuleLifecycleState.ACTIVE}
                for item in self.order
            ):
                return
            await self._prepare_graph()

    async def start(self) -> None:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                raise ForestError(
                    "RUNTIME_MODULE_REGISTRY_CLOSED", "Runtime module registry is closed", 503
                )
            if all(self.states[item] == ModuleLifecycleState.ACTIVE for item in self.order):
                return
            lease = await self._prepare_graph()
            await self._start_clean(lease)

    async def prewarm(self) -> None:
        """Construct clean Generation resources without publishing their routes."""

        if self.generation_authority is None:
            raise ForestError(
                "RUNTIME_GENERATION_PREWARM_UNAVAILABLE",
                "Only clean Generation graphs support separate prewarming",
                409,
            )
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                raise ForestError(
                    "RUNTIME_MODULE_REGISTRY_CLOSED",
                    "Runtime module registry is closed",
                    503,
                )
            if all(self.states[item] == ModuleLifecycleState.ACTIVE for item in self.order):
                return
            lease = await self._prepare_graph()
            await self._start_clean(lease, publish=False)

    async def _start_clean(self, lease, *, publish: bool = True) -> None:
        """Prewarm, publish, then admit a clean immutable Generation graph."""

        prewarmed: list[str] = []
        activated: list[str] = []
        current = None
        published = False
        try:
            if not self._prewarmed:
                for component_id in self.order:
                    current = component_id
                    if self.states[component_id] != ModuleLifecycleState.PREPARED:
                        raise ForestError(
                            "RUNTIME_MODULE_LIFECYCLE_INVALID",
                            "Runtime module graph is not prepared for prewarming",
                        )
                    start = getattr(self.instances[component_id], "start", None)
                    if start is not None:
                        await start()
                    prewarmed.append(component_id)
                self._prewarmed = True

            if not publish:
                return

            current = None
            await self.generation_authority.publish_graph(
                lease,
                self.graph_identity,
                configuration_activators={
                    component_id: self.instances[component_id].module_configuration
                    for component_id in self.order
                },
                publication_participants={
                    component_id: self.instances[component_id]
                    for component_id in self.order
                    if getattr(self.instances[component_id], "publish_generation_in", None)
                    is not None
                },
            )
            published = True

            for component_id in self.order:
                current = component_id
                activate = self.instances[component_id].activate_generation
                result = activate()
                if inspect.isawaitable(result):
                    close = getattr(result, "close", None)
                    if close is not None:
                        close()
                    raise ForestError(
                        "RUNTIME_GENERATION_ACTIVATION_INVALID",
                        "Post-publication activation must be local and synchronous",
                        503,
                    )
                self._transition(component_id, ModuleLifecycleState.ACTIVE)
                activated.append(component_id)
            self.contract_router.activate()
            self._open_owned_admission(self._owned_work_components())
        except BaseException as exc:
            failure_code = (
                exc.code if isinstance(exc, ForestError) else "RUNTIME_MODULE_START_FAILED"
            )
            rollback_errors: list[Exception] = []
            if published:
                try:
                    await self.generation_authority.retire_graph(lease, self.graph_identity)
                except Exception as rollback_error:
                    rollback_errors.append(rollback_error)
            if current is not None and self.states[current] == ModuleLifecycleState.PREPARED:
                self._transition(current, ModuleLifecycleState.FAILED)
                try:
                    await self.generation_authority.record_states(
                        lease,
                        self.graph_identity,
                        {current: ("FAILED", failure_code)},
                    )
                except Exception as rollback_error:
                    rollback_errors.append(rollback_error)

            for component_id in reversed(self.order):
                if component_id not in self.instances:
                    continue
                try:
                    state = self.states[component_id]
                    if state == ModuleLifecycleState.ACTIVE:
                        await self._quiesce_and_cleanup(component_id)
                    elif state in {
                        ModuleLifecycleState.PREPARED,
                        ModuleLifecycleState.FAILED,
                    }:
                        await self._cleanup(component_id)
                except Exception as rollback_error:
                    rollback_errors.append(rollback_error)
            try:
                await self._record_current_states(lease, failure_code=failure_code)
            except Exception as rollback_error:
                rollback_errors.append(rollback_error)
            if rollback_errors:
                raise ForestError(
                    "RUNTIME_MODULE_START_ROLLBACK_FAILED",
                    "Runtime module startup failed and resources remain owned",
                    503,
                    details={
                        "start_error": failure_code,
                        "cleanup_errors": len(rollback_errors),
                    },
                ) from exc
            raise

    async def drain_owned_work_for_replacement(
        self,
        operation_id: str,
        *,
        components: tuple[str, ...] | None = None,
    ) -> None:
        """Freeze and drain only the Generations selected for replacement."""

        if components is not None and (
            not isinstance(components, tuple)
            or not components
            or any(not isinstance(item, str) or item not in self.states for item in components)
            or len(components) != len(set(components))
        ):
            raise ForestError(
                "RUNTIME_GRAPH_MEMBER_SELECTION_INVALID",
                "Replacement must select distinct current Graph members",
                422,
            )
        selected = (
            tuple(item for item in self.order if item in components)
            if components is not None
            else None
        )
        if not operation_id or len(operation_id.encode("utf-8")) > 256:
            raise ForestError(
                "RUNTIME_REPLACEMENT_OPERATION_INVALID",
                "Replacement operation identity is invalid",
                422,
            )
        if self.generation_authority is None or self.graph_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_DRAIN_UNAVAILABLE",
                "RuntimeGraph replacement drain requires a clean published graph",
                409,
            )
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._replacement_operation_id is not None:
                if (
                    self._replacement_operation_id != operation_id
                    or self._replacement_components != selected
                ):
                    raise ForestError(
                        "RUNTIME_REPLACEMENT_OPERATION_CONFLICT",
                        "RuntimeGraph is already draining for another replacement",
                        409,
                    )
                return
            if any(state != ModuleLifecycleState.ACTIVE for state in self.states.values()):
                raise ForestError(
                    "RUNTIME_MODULE_LIFECYCLE_INVALID",
                    "Only a fully active RuntimeGraph can drain for replacement",
                    409,
                )
            lease = self._lease()
            await self.generation_authority.freeze_graph_admission(
                lease,
                self.graph_identity,
                components=selected,
            )
            self._replacement_operation_id = operation_id
            self._replacement_components = selected
            drained: list[str] = []
            try:
                if selected is None:
                    self.contract_router.deactivate()
                self._replacement_owned_frozen = self._freeze_owned_admission(
                    components=selected
                )
                await self._drain_frozen_owned_work(
                    self._replacement_owned_frozen
                )
                for component_id in reversed(self.order):
                    if (
                        component_id == "adapter.onebot"
                        or (selected is not None and component_id not in selected)
                    ):
                        continue
                    owned = getattr(self.instances[component_id], "owned_tasks", None)
                    tasks = tuple(owned()) if owned is not None else ()
                    if not tasks and not getattr(
                        self.instances[component_id],
                        "requires_replacement_quiesce",
                        False,
                    ):
                        continue
                    begin_replacement = getattr(
                        self.instances[component_id],
                        "begin_replacement",
                        None,
                    )
                    if begin_replacement is not None:
                        result = begin_replacement(operation_id)
                        if inspect.isawaitable(result):
                            await result
                    await self._quiesce_only(component_id)
                    drained.append(component_id)
                self._replacement_quiesced = tuple(drained)
            except BaseException as drain_error:
                self._replacement_quiesced = tuple(drained)
                try:
                    await self._resume_owned_work_locked(lease)
                except BaseException as resume_error:
                    raise ForestError(
                        "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED",
                        "RuntimeGraph drain failed and the previous graph cannot safely resume",
                        503,
                        details={
                            "drain_error": (
                                drain_error.code
                                if isinstance(drain_error, ForestError)
                                else type(drain_error).__name__
                            ),
                            "resume_error": (
                                resume_error.code
                                if isinstance(resume_error, ForestError)
                                else type(resume_error).__name__
                            ),
                        },
                    ) from resume_error
                raise

    async def resume_owned_work_after_failed_replacement(self) -> None:
        """Resume a frozen graph when its prepared successor did not publish."""

        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            await self._resume_owned_work_locked(self._lease())

    async def _resume_owned_work_locked(self, lease) -> None:
        operation_id = self._replacement_operation_id
        if operation_id is None:
            return
        quiesced = set(self._replacement_quiesced)
        if any(
            state
            != (
                ModuleLifecycleState.QUIESCENT
                if component_id in quiesced
                else ModuleLifecycleState.ACTIVE
            )
            for component_id, state in self.states.items()
        ):
            raise ForestError(
                "RUNTIME_GRAPH_RESUME_UNSAFE",
                "A partially failed RuntimeGraph cannot reopen admission",
                503,
            )
        selected = self._replacement_components
        agent = (
            self.instances.get("agent.runtime")
            if selected is None or "agent.runtime" in selected
            else None
        )
        pending = set(quiesced)
        for component_id in self.order:
            if component_id not in pending:
                continue
            resume = getattr(self.instances[component_id], "resume", None)
            if resume is not None:
                result = resume()
                if inspect.isawaitable(result):
                    await result
            activate = self.instances[component_id].activate_generation
            result = activate()
            if inspect.isawaitable(result):
                close = getattr(result, "close", None)
                if close is not None:
                    close()
                raise ForestError(
                    "RUNTIME_GENERATION_ACTIVATION_INVALID",
                    "Replacement rollback activation must be local and synchronous",
                    503,
                )
            self._transition(component_id, ModuleLifecycleState.ACTIVE)
            self._replacement_quiesced = tuple(
                item for item in self._replacement_quiesced if item != component_id
            )
        if selected is None:
            self.contract_router.activate()
        resumed_runs = ()
        if agent is not None:
            resumed_runs = await agent.resume_reload_wait(operation_id)
        conversation = self.instances.get("conversation.scheduler")
        if conversation is not None:
            for run in resumed_runs:
                await conversation.resume_deferred(
                    run["conversation_ref"],
                    reason_code="AGENT_RELOAD_WAIT",
                    reason_ref=operation_id,
                )
        self._open_owned_admission(self._replacement_owned_frozen)
        await self.generation_authority.resume_graph_admission(
            lease,
            self.graph_identity,
            components=selected,
        )
        self._replacement_operation_id = None
        self._replacement_components = None
        if resumed_runs:
            self.notify_work()

    def _owned_work_components(self) -> tuple[str, ...]:
        return tuple(
            component_id
            for component_id in self.order
            if next(
                item
                for item in self.descriptors
                if item.component_id == component_id
            ).owned_work_lifecycle
            == OwnedWorkLifecycle.FREEZE_DRAIN
        )

    @staticmethod
    def _validate_owned_work_lifecycle(
        descriptor: RuntimeModuleDescriptor, instance: Any
    ) -> None:
        hooks = (
            "freeze_owned_admission",
            "drain_owned_work",
            "open_owned_admission",
        )
        present = tuple(callable(getattr(instance, name, None)) for name in hooks)
        declared = descriptor.owned_work_lifecycle == OwnedWorkLifecycle.FREEZE_DRAIN
        if (declared and not all(present)) or (not declared and any(present)):
            raise ForestError(
                "RUNTIME_OWNED_WORK_CONTRACT_INVALID",
                f"Module {descriptor.component_id} owned-work lifecycle does not match its descriptor",
                503,
            )

    def _freeze_owned_admission(
        self, *, components: tuple[str, ...] | None = None
    ) -> tuple[str, ...]:
        """Close selected owned-work gates before any asynchronous drain can yield."""

        frozen: list[str] = []
        try:
            for component_id in self._owned_work_components():
                if components is not None and component_id not in components:
                    continue
                result = self.instances[component_id].freeze_owned_admission()
                if inspect.isawaitable(result):
                    close = getattr(result, "close", None)
                    if close is not None:
                        close()
                    raise ForestError(
                        "RUNTIME_OWNED_ADMISSION_INVALID",
                        "Owned-work admission freeze must be local and synchronous",
                        503,
                    )
                frozen.append(component_id)
        except BaseException:
            for component_id in reversed(frozen):
                self.instances[component_id].open_owned_admission()
            raise
        return tuple(frozen)

    async def _drain_frozen_owned_work(
        self, frozen: tuple[str, ...]
    ) -> None:
        for component_id in reversed(frozen):
            result = self.instances[component_id].drain_owned_work()
            if not inspect.isawaitable(result):
                raise ForestError(
                    "RUNTIME_OWNED_DRAIN_INVALID",
                    "Owned-work drain must be asynchronous",
                    503,
                )
            await result

    def _open_owned_admission(self, components: tuple[str, ...]) -> None:
        for component_id in components:
            result = self.instances[component_id].open_owned_admission()
            if inspect.isawaitable(result):
                close = getattr(result, "close", None)
                if close is not None:
                    close()
                raise ForestError(
                    "RUNTIME_OWNED_ADMISSION_INVALID",
                    "Owned-work admission resume must be local and synchronous",
                    503,
                )
            self._replacement_owned_frozen = tuple(
                item
                for item in self._replacement_owned_frozen
                if item != component_id
            )

    async def close(self) -> list[Exception]:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                return []
            lease = None
            superseded = False
            if self.generation_authority is not None and self.graph_identity is not None:
                try:
                    lease = self._lease(allow_closing=True)
                    await self.generation_authority.freeze_graph_admission(
                        lease, self.graph_identity
                    )
                except ForestError as exc:
                    if (
                        exc.code == "RUNTIME_GRAPH_NOT_CURRENT"
                    ):
                        # A newer graph published under the same HostLease has
                        # already fenced this graph from new work. Keep the
                        # lease so this owner can drain its process-local
                        # resources and persist their terminal lifecycle facts.
                        superseded = True
                    elif not self._lost_persistence_authority(exc):
                        raise
                    else:
                        # A fenced owner must still release process-local resources.
                        # Its persistent graph remains historical until the current
                        # owner atomically replaces or retires it.
                        lease = None
            self.contract_router.deactivate()
            errors = []
            if not self._replacement_owned_frozen and not self._replacement_quiesced:
                try:
                    frozen = self._freeze_owned_admission()
                    await self._drain_frozen_owned_work(frozen)
                except Exception as exc:
                    errors.append(exc)
            for component_id in reversed(self.order):
                try:
                    state = self.states[component_id]
                    if state == ModuleLifecycleState.ACTIVE:
                        await self._quiesce_and_cleanup(component_id)
                    elif state in {
                        ModuleLifecycleState.PREPARED,
                        ModuleLifecycleState.FAILED,
                        ModuleLifecycleState.QUIESCENT,
                    }:
                        if component_id in self.instances:
                            await self._cleanup(component_id)
                        else:
                            self._transition(component_id, ModuleLifecycleState.STOPPED)
                    elif state == ModuleLifecycleState.VALIDATED:
                        self._transition(component_id, ModuleLifecycleState.STOPPED)
                except Exception as exc:
                    errors.append(exc)
            if lease is not None:
                if not superseded:
                    try:
                        await self.generation_authority.retire_graph(lease, self.graph_identity)
                    except ForestError as exc:
                        if not self._lost_persistence_authority(exc):
                            errors.append(exc)
                        lease = None
            if lease is not None:
                failure_code = (
                    errors[0].code
                    if errors and isinstance(errors[0], ForestError)
                    else ("RUNTIME_MODULE_CLEANUP_FAILED" if errors else None)
                )
                await self._record_current_states(lease, failure_code=failure_code)
            self._closed = not errors and all(
                state == ModuleLifecycleState.STOPPED for state in self.states.values()
            )
            return errors

    async def _quiesce_and_cleanup(self, component_id):
        await self._quiesce_only(component_id)
        await self._cleanup(component_id)

    async def _quiesce_only(self, component_id):
        self._transition(component_id, ModuleLifecycleState.QUIESCING)
        quiesce = getattr(self.instances[component_id], "quiesce", None)
        if quiesce is not None:
            try:
                await quiesce()
            except BaseException:
                self._transition(component_id, ModuleLifecycleState.FAILED)
                await self._cleanup(component_id)
                raise
        self._transition(component_id, ModuleLifecycleState.QUIESCENT)

    async def _cleanup(self, component_id):
        close = getattr(self.instances[component_id], "close", None)
        try:
            if close is not None:
                await close()
        except BaseException:
            if self.states[component_id] != ModuleLifecycleState.FAILED:
                self._transition(component_id, ModuleLifecycleState.FAILED)
            raise
        self._transition(component_id, ModuleLifecycleState.STOPPED)

    def tasks(self):
        result = []
        for component_id in self.order:
            if self.states[component_id] != ModuleLifecycleState.ACTIVE:
                continue
            instance = self.instances[component_id]
            owned = getattr(instance, "owned_tasks", None)
            tasks = (
                tuple(owned())
                if owned is not None
                else ((task,) if (task := getattr(instance, "task", None)) is not None else ())
            )
            if any(not isinstance(task, asyncio.Task) for task in tasks):
                raise ForestError(
                    "RUNTIME_MODULE_TASK_INVALID", "Runtime module returned an invalid owned task"
                )
            result.extend(tasks)
        if len({id(task) for task in result}) != len(result):
            raise ForestError(
                "RUNTIME_MODULE_TASK_DUPLICATE", "Runtime modules cannot share task ownership"
            )
        return result

    def failures(self):
        result = {}
        for component_id in self.order:
            failure = getattr(self.instances[component_id], "failure", None)
            if self.states[component_id] == ModuleLifecycleState.FAILED:
                result[component_id] = failure or "MODULE_LIFECYCLE_FAILED"
            elif failure is not None:
                result[component_id] = failure
        return result

    def describe(self) -> list[dict]:
        return [
            {
                "component_id": descriptor.component_id,
                "requires": list(self._effective_requires(descriptor)),
                "state": self.states[descriptor.component_id],
                "artifact": descriptor.artifact.as_fact(),
                "graph_id": self.graph_identity.graph_id
                if self.graph_identity is not None
                else None,
                "generation_id": (
                    self.graph_identity.generations[descriptor.component_id]
                    if self.graph_identity is not None
                    else None
                ),
                "management_contributions": [
                    contribution.as_fact(public=True) for contribution in descriptor.management
                ],
                "management_contract": (
                    contract.describe()
                    if (
                        contract := getattr(
                            self.instances.get(descriptor.component_id), "management_contract", None
                        )
                    )
                    is not None
                    else None
                ),
                "storage": (
                    descriptor.storage.as_fact() if descriptor.storage is not None else None
                ),
            }
            for descriptor in self.descriptors
        ]

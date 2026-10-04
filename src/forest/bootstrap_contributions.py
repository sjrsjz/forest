"""Module-owned configuration contributions used before a Bot runtime exists."""

from __future__ import annotations

import re
from copy import deepcopy
from dataclasses import dataclass
from typing import Callable

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_modules import RuntimeManagementContribution


@dataclass(frozen=True)
class BootstrapPackageSurface:
    """Opaque-origin setup UI captured from one exact package artifact."""

    component_id: str
    package_digest: str
    artifact_sha256: str
    scope_kinds: tuple[str, ...] = ("deployment",)

    @property
    def contribution_id(self) -> str:
        return "bootstrap-" + self.component_id

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.component_id) is None:
            raise ValueError("Bootstrap UI component identity is invalid")
        if re.fullmatch(r"sha256:[a-f0-9]{64}", self.package_digest) is None:
            raise ValueError("Bootstrap UI package digest is invalid")
        if re.fullmatch(r"[a-f0-9]{64}", self.artifact_sha256) is None:
            raise ValueError("Bootstrap UI artifact digest is invalid")

    def as_fact(self, *, public: bool = False) -> dict:
        surface = {
            "protocol": "forest.bootstrap-ui/1",
            "artifact": (
                f"/management/v1/bootstrap-ui/{self.component_id}/"
                f"{self.package_digest.removeprefix('sha256:')}/{self.artifact_sha256}"
            ),
            "sha256": self.artifact_sha256,
        }
        if public:
            surface["kind"] = "sandboxed-bootstrap"
        return {"surface": surface}


@dataclass(frozen=True)
class BootstrapConfigurationContribution:
    """One installable component's schema, initial value, and setup surface."""

    component_id: str
    label: str
    description: str
    schema: dict
    initial_value: dict
    surface: RuntimeManagementContribution | BootstrapPackageSurface
    package_digest: str
    order: int = 100
    secret_refs: tuple[tuple[str, str, bool], ...] = ()
    origins: tuple[tuple[str, tuple[str, ...]], ...] = ()
    validator: Callable[[dict], object] | None = None

    def __post_init__(self) -> None:
        if self.surface.scope_kinds != ("deployment",):
            raise ValueError("Bootstrap contribution surface must belong to deployment scope")
        if re.fullmatch(r"sha256:[a-f0-9]{64}", self.package_digest) is None:
            raise ValueError("Bootstrap contribution package digest is invalid")
        if isinstance(self.surface, BootstrapPackageSurface) and (
            self.surface.component_id != self.component_id
            or self.surface.package_digest != self.package_digest
        ):
            raise ValueError("Bootstrap UI differs from its contribution package")
        if type(self.order) is not int or not -10_000 <= self.order <= 10_000:
            raise ValueError("Bootstrap contribution order is invalid")
        if not self.description.strip() or any(ord(char) < 32 for char in self.description):
            raise ValueError("Bootstrap contribution description is invalid")
        Draft202012Validator.check_schema(self.schema)
        if tuple(Draft202012Validator(self.schema).iter_errors(self.initial_value)):
            raise ValueError("Bootstrap contribution initial value does not match its schema")
        required = set(self.schema.get("required", ()))
        properties = self.schema.get("properties", {})
        for field, purpose, allow_empty in self.secret_refs:
            property_schema = properties.get(field)
            if (
                field not in required or not isinstance(property_schema, dict)
                or property_schema.get("type") != "string"
                or not purpose or type(allow_empty) is not bool
            ):
                raise ValueError("Bootstrap Secret declaration is invalid")
        for field, schemes in self.origins:
            property_schema = properties.get(field)
            if (
                field not in required or not isinstance(property_schema, dict)
                or property_schema.get("type") != "string"
                or not schemes or set(schemes) - {"http", "https", "ws", "wss"}
            ):
                raise ValueError("Bootstrap origin declaration is invalid")
        if self.validator is not None:
            self.validator(deepcopy(self.initial_value))

    @property
    def schema_digest(self) -> str:
        return digest_json(self.schema)

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.bootstrap-configuration-contribution/1",
            "component_id": self.component_id,
            "label": self.label,
            "description": self.description,
            "order": self.order,
            "schema_digest": self.schema_digest,
            "package_digest": self.package_digest,
            "configuration_schema": deepcopy(self.schema),
            "initial_value": deepcopy(self.initial_value),
            "surface": self.surface.as_fact(public=True)["surface"],
        }


class BootstrapConfigurationRegistry:
    """Validated install-time component directory consumed by generic provisioning."""

    def __init__(self, contributions: tuple[BootstrapConfigurationContribution, ...]) -> None:
        ordered = tuple(sorted(contributions, key=lambda item: (item.order, item.component_id)))
        component_ids = [item.component_id for item in ordered]
        surface_ids = [item.surface.contribution_id for item in ordered]
        if not ordered:
            raise ValueError("Bootstrap contribution registry cannot be empty")
        if len(set(component_ids)) != len(component_ids):
            raise ValueError("Bootstrap component identity is duplicated")
        if len(set(surface_ids)) != len(surface_ids):
            raise ValueError("Bootstrap contribution surface identity is duplicated")
        self.contributions = ordered
        self._by_component = {item.component_id: item for item in ordered}

    def facts(self) -> list[dict]:
        return [item.as_fact() for item in self.contributions]

    def schema_digests(self) -> dict[str, str]:
        return {
            item.component_id: item.schema_digest for item in self.contributions
        }

    def package_digests(self) -> dict[str, str]:
        return {
            item.component_id: item.package_digest for item in self.contributions
        }

    def validate_components(self, components: object) -> dict[str, dict]:
        if not isinstance(components, dict) or set(components) != set(self._by_component):
            raise ForestError(
                "BOOTSTRAP_COMPONENT_SET_INVALID",
                "Bootstrap component set differs from the registered install catalog",
                422,
            )
        for component_id, contribution in self._by_component.items():
            errors = tuple(
                Draft202012Validator(contribution.schema).iter_errors(
                    components[component_id]
                )
            )
            if errors:
                raise ForestError(
                    "BOOTSTRAP_COMPONENT_CONFIGURATION_INVALID",
                    "Bootstrap component configuration is invalid",
                    422,
                    details={"component_id": component_id},
                )
            if contribution.validator is not None:
                try:
                    contribution.validator(deepcopy(components[component_id]))
                except ForestError as exc:
                    raise ForestError(
                        "BOOTSTRAP_COMPONENT_CONFIGURATION_INVALID",
                        "Bootstrap component validator rejected the configuration",
                        422,
                        details={
                            "component_id": component_id,
                            "validator_code": exc.code,
                        },
                    ) from exc
        return components

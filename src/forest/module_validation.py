"""Pure validation of Host-pinned component configuration and dependency snapshots."""

from __future__ import annotations

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.kernel import DependencyPlan, resolve_dependencies
from forest.packages import PackageSnapshot


def validate_component_values(
    packages: dict[str, PackageSnapshot], component_id: str, values: dict
) -> PackageSnapshot:
    if (
        not isinstance(values, dict)
        or not {"package_digest", "config", "grants"} <= set(values)
        or set(values)
        - {"package_digest", "config", "grants", "message_handler_takeover"}
        or (
            "message_handler_takeover" in values
            and type(values["message_handler_takeover"]) is not bool
        )
    ):
        raise ForestError("CONFIG_INVALID", "Configuration fields do not match", 422)
    digest = values["package_digest"]
    if not isinstance(digest, str):
        raise ForestError("CONFIG_INVALID", "Package digest must be text", 422)
    snapshot = packages.get(digest)
    if snapshot is None or snapshot.component["id"] != component_id:
        raise ForestError("PACKAGE_UNAVAILABLE", "Select an available component package", 422)
    if (
        values.get("message_handler_takeover", False)
        and snapshot.component.get("message_handler_takeover", "none") != "compatible"
    ):
        raise ForestError(
            "CONFIG_INVALID",
            "The selected package does not declare compatible Handler takeover",
            422,
        )
    if not isinstance(values["config"], dict) or not Draft202012Validator(
        snapshot.schema(snapshot.component["config_schema"])
    ).is_valid(values["config"]):
        raise ForestError(
            "CONFIG_INVALID", "Component configuration does not match its schema", 422
        )
    requested = snapshot.component.get("capabilities", {}).get("requests", [])
    if (
        not isinstance(values["grants"], list)
        or not all(isinstance(grant, str) for grant in values["grants"])
        or sorted(values["grants"]) != sorted(requested)
    ):
        raise ForestError(
            "CAPABILITY_DENIED", "Explicitly grant the package's required capabilities", 403
        )
    return snapshot


def validate_desired_snapshot(
    packages: dict[str, PackageSnapshot],
    revisions: list[dict],
    *,
    include_disabled: frozenset[str] = frozenset(),
) -> tuple[DependencyPlan, dict[str, PackageSnapshot]]:
    snapshots = {}
    for revision in revisions:
        component_id = revision["component_id"]
        snapshot = validate_component_values(packages, component_id, revision["values"])
        if revision.get("desired_enabled", True) or component_id in include_disabled:
            snapshots[component_id] = snapshot
    return resolve_dependencies(snapshots), snapshots

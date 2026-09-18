"""Pure validation of Host-pinned component configuration and dependency snapshots."""

from __future__ import annotations

from dataclasses import dataclass

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.packages import PackageSnapshot, contract_matches


@dataclass
class DependencyPlan:
    order: list[str]
    bindings: dict[str, dict[str, tuple[str, str]]]
    closures: dict[str, tuple[str, ...]]
    degraded: dict[str, list[dict[str, str]]]


def resolve_dependencies(snapshots: dict[str, PackageSnapshot]) -> DependencyPlan:
    """Resolve an immutable package graph without importing plugin code."""
    providers: dict[tuple[str, int], tuple[str, str]] = {}
    for component_id, snapshot in snapshots.items():
        for provided in snapshot.component["provides"]:
            key = provided["name"], int(provided["contract"].split(".")[0])
            if key in providers:
                raise ForestError(
                    "DUPLICATE_SERVICE",
                    "Two components provide the same service and contract major",
                    422,
                )
            providers[key] = component_id, provided["contract"]

    bindings: dict[str, dict[str, tuple[str, str]]] = {}
    graph: dict[str, set[str]] = {}
    degraded: dict[str, list[dict[str, str]]] = {}
    for component_id, snapshot in snapshots.items():
        bindings[component_id], graph[component_id], degraded[component_id] = {}, set(), []
        for required in snapshot.component.get("requires", []):
            matches = [
                provider
                for (name, _), provider in providers.items()
                if name == required["name"] and contract_matches(required["contract"], provider[1])
            ]
            if len(matches) > 1:
                raise ForestError(
                    "DEPENDENCY_AMBIGUOUS",
                    "Dependency contract resolves to more than one provider",
                    422,
                )
            if not matches:
                code = (
                    "DEPENDENCY_INCOMPATIBLE"
                    if any(name == required["name"] for name, _ in providers)
                    else "DEPENDENCY_MISSING"
                )
                if required.get("required", True):
                    raise ForestError(
                        code,
                        "Required component service is unavailable or incompatible",
                        422,
                    )
                degraded[component_id].append(
                    {"service": required["name"], "contract": required["contract"], "reason": code}
                )
                continue
            bindings[component_id][required["name"]] = matches[0]
            graph[component_id].add(matches[0][0])

    order: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(component_id: str) -> None:
        if component_id in visiting:
            raise ForestError(
                "DEPENDENCY_CYCLE", "Synchronous component dependencies must form a DAG", 422
            )
        if component_id in visited:
            return
        visiting.add(component_id)
        for target in sorted(graph[component_id]):
            visit(target)
        visiting.remove(component_id)
        visited.add(component_id)
        order.append(component_id)

    for component_id in sorted(graph):
        visit(component_id)

    closures: dict[str, tuple[str, ...]] = {}
    for component_id in order:
        closure = set(graph[component_id])
        for target in graph[component_id]:
            closure.update(closures[target])
        closures[component_id] = tuple(sorted(closure))
    return DependencyPlan(order, bindings, closures, degraded)


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

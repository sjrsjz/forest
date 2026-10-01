"""Host-side read-only Graph preview for a frozen Bot component selection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping

from forest.builtin_packages import (
    PackageCandidateInspection,
    compile_installed_candidate_descriptor,
    inspect_official_candidate_descriptor,
)
from forest.component_selection import (
    ComponentSelectionReader,
    ComponentSelectionSnapshot,
    select_package_members,
)
from forest.errors import ForestError
from forest.packages import PackageCatalog, digest_json
from forest.runtime_modules import (
    RuntimeGraphTransitionPlan,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
)


@dataclass(frozen=True)
class SelectionGraphPreview:
    selection: ComponentSelectionSnapshot
    source_graph_id: str
    source_graph_revision: int
    target_component_id: str
    impact: RuntimeGraphTransitionPlan
    packages: tuple[PackageCandidateInspection, ...]
    plan_digest: str


async def project_selection_graph_in(
    current: RuntimeModuleRegistry,
    reader: ComponentSelectionReader,
    official_descriptors: tuple[RuntimeModuleDescriptor, ...],
    catalog: PackageCatalog,
    store,
    *,
    kernel,
    conn,
    component_id: str,
    source_kind: str,
    package_digest: str | None,
    manifest_digest: str | None,
    approved_capabilities: tuple[str, ...],
    desired_enabled: bool,
    expected_selection_revision: int,
    expected_graph_revision: int,
    expected_configuration_revision: int | None,
    ordered_configuration_locks: bool = False,
) -> SelectionGraphPreview:
    """Use the package projection and formal Graph planner on one caller transaction."""

    selection = await reader.project_in(
        conn,
        component_id=component_id,
        source_kind=source_kind,
        package_digest=package_digest,
        manifest_digest=manifest_digest,
        approved_capabilities=approved_capabilities,
        desired_enabled=desired_enabled,
        expected_selection_revision=expected_selection_revision,
    )
    return await preview_selected_graph(
        current, selection, official_descriptors, catalog, store,
        kernel=kernel,
        target_component_id=component_id,
        expected_graph_revision=expected_graph_revision,
        expected_configuration_revisions=(
            {component_id: expected_configuration_revision}
            if expected_configuration_revision is not None else None
        ),
        conn=conn,
        ordered_configuration_locks=ordered_configuration_locks,
    )


async def preview_selected_graph(
    current: RuntimeModuleRegistry,
    selection: ComponentSelectionSnapshot,
    official_descriptors: tuple[RuntimeModuleDescriptor, ...],
    catalog: PackageCatalog,
    store,
    *,
    kernel,
    target_component_id: str,
    expected_graph_revision: int,
    expected_configuration_revisions: Mapping[str, int] | None = None,
    conn=None,
    ordered_configuration_locks: bool = False,
) -> SelectionGraphPreview:
    """Compile package facts under the caller's snapshot or ordered lock policy."""

    if conn is None:
        async with store.pool.connection() as owned_conn, owned_conn.transaction():
            await owned_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            return await _preview_selected_graph_in(
                current, selection, official_descriptors, catalog, store,
                kernel=kernel, target_component_id=target_component_id,
                expected_graph_revision=expected_graph_revision,
                expected_configuration_revisions=expected_configuration_revisions,
                conn=owned_conn,
                ordered_configuration_locks=ordered_configuration_locks,
            )
    return await _preview_selected_graph_in(
        current, selection, official_descriptors, catalog, store,
        kernel=kernel, target_component_id=target_component_id,
        expected_graph_revision=expected_graph_revision,
        expected_configuration_revisions=expected_configuration_revisions,
        conn=conn,
        ordered_configuration_locks=ordered_configuration_locks,
    )


async def _preview_selected_graph_in(
    current: RuntimeModuleRegistry,
    selection: ComponentSelectionSnapshot,
    official_descriptors: tuple[RuntimeModuleDescriptor, ...],
    catalog: PackageCatalog,
    store,
    *,
    kernel,
    target_component_id: str,
    expected_graph_revision: int,
    expected_configuration_revisions: Mapping[str, int] | None,
    conn,
    ordered_configuration_locks: bool,
) -> SelectionGraphPreview:
    """Read every package fact on one transaction; order locks for commit checks."""

    graph = current.graph_identity
    if graph is None or graph.graph_revision != expected_graph_revision:
        raise ForestError(
            "RUNTIME_GRAPH_REVISION_CONFLICT",
            "RuntimeGraph revision changed before component preview",
            409,
        )
    pinned_revisions = dict(expected_configuration_revisions or {})
    enabled_installed = {
        entry.component_id for entry in selection.entries
        if entry.source_kind == "installed" and entry.desired_enabled
    }
    if (
        set(pinned_revisions) - enabled_installed
        or any(type(revision) is not int or revision < 1 for revision in pinned_revisions.values())
        or (target_component_id in enabled_installed and target_component_id not in pinned_revisions)
    ):
        raise ForestError(
            "MODULE_CONFIGURATION_REVISION_REQUIRED",
            "Selected installed package requires an exact target configuration revision",
            422,
        )
    selected = select_package_members(selection, official_descriptors, catalog)
    inspection_slots = asyncio.Semaphore(4)

    async def inspect_official(descriptor):
        async with inspection_slots:
            return await inspect_official_candidate_descriptor(
                descriptor, catalog, store, kernel=kernel, conn=conn
            )

    async def inspect_installed(entry):
        fact, descriptor = await compile_installed_candidate_descriptor(
            entry.package_digest,
            catalog,
            store,
            kernel=kernel,
            approved_capabilities=entry.approved_capabilities,
            expected_manifest_digest=entry.manifest_digest,
            expected_configuration_revision=pinned_revisions.get(entry.component_id),
            conn=conn,
        )
        if descriptor.component_id != entry.component_id:
            raise ForestError(
                "COMPONENT_SELECTION_ARTIFACT_CHANGED",
                "Resolved package component differs from its selection",
                409,
            )
        return fact, descriptor

    inspected = []
    descriptors = []
    if ordered_configuration_locks:
        members = sorted(
            [(item.component_id, "official", item) for item in selected.official]
            + [(item.component_id, "installed", item) for item in selected.installed],
            key=lambda item: item[0],
        )
        for _, kind, item in members:
            if kind == "official":
                inspected.append(await inspect_official_candidate_descriptor(
                    item, catalog, store, kernel=kernel, conn=conn
                ))
            else:
                fact, descriptor = await inspect_installed(item)
                inspected.append(fact)
                descriptors.append(descriptor)
    else:
        inspected.extend(await asyncio.gather(*(
            inspect_official(descriptor) for descriptor in selected.official
        )))
        for entry in selected.installed:
            fact, descriptor = await inspect_installed(entry)
            inspected.append(fact)
            descriptors.append(descriptor)
    candidate = RuntimeModuleRegistry(
        current.runtime, kernel.descriptors + selected.official + tuple(descriptors)
    )
    impact = candidate.preview_graph_transition(
        current, target_component_id=target_component_id
    )
    packages = tuple(sorted(inspected, key=lambda item: item.component_id))
    plan_digest = digest_json({
        "protocol": "forest.component-selection-plan/1",
        "bot_id": store.bot_id,
        "source_graph_id": graph.graph_id,
        "source_graph_revision": expected_graph_revision,
        "selection_revision": selection.revision,
        "selection_digest": selection.digest,
        "target_component_id": target_component_id,
        "members": [
            {"component_id": component_id, "descriptor_digest": descriptor_digest}
            for component_id, descriptor_digest in candidate.preview_descriptor_digests(current)
        ],
        "packages": [item.as_fact() for item in packages],
        "impact": {
            field: list(getattr(impact, field))
            for field in ("replace", "retain", "remove", "add", "drain", "prepare")
        },
        "worker_capacity": impact.worker_capacity.as_fact(),
    })
    return SelectionGraphPreview(
        selection, graph.graph_id, expected_graph_revision, target_component_id,
        impact, packages, plan_digest,
    )

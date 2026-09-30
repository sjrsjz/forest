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
) -> SelectionGraphPreview:
    """Compile read-only package facts; a supplied connection must be REPEATABLE READ."""

    if conn is None:
        async with store.pool.connection() as owned_conn, owned_conn.transaction():
            await owned_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            return await _preview_selected_graph_in(
                current, selection, official_descriptors, catalog, store,
                kernel=kernel, target_component_id=target_component_id,
                expected_graph_revision=expected_graph_revision,
                expected_configuration_revisions=expected_configuration_revisions,
                conn=owned_conn,
            )
    return await _preview_selected_graph_in(
        current, selection, official_descriptors, catalog, store,
        kernel=kernel, target_component_id=target_component_id,
        expected_graph_revision=expected_graph_revision,
        expected_configuration_revisions=expected_configuration_revisions,
        conn=conn,
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
) -> SelectionGraphPreview:
    """Use one transaction snapshot for every package configuration fact."""

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

    inspected = list(await asyncio.gather(*(
        inspect_official(descriptor) for descriptor in selected.official
    )))
    descriptors = []
    for entry in selected.installed:
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
    })
    return SelectionGraphPreview(
        selection, graph.graph_id, expected_graph_revision, target_component_id,
        impact, packages, plan_digest,
    )

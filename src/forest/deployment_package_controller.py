"""Own deployment package selection and the active RuntimeGraph lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

from forest.deployment_packages import bootstrap_builtin_deployment_package
from forest.errors import ForestError
from forest.packages import PackageCatalog, PackageSnapshot
from forest.runtime_modules import RuntimeModuleRegistry


class DeploymentPackageController:
    def __init__(
        self, runtime, catalog: PackageCatalog, *, official_sources: tuple[Path, ...]
    ) -> None:
        self.runtime = runtime
        self.catalog = catalog
        self.official_sources = official_sources
        self._registry: RuntimeModuleRegistry | None = None
        self._snapshots: dict[str, PackageSnapshot] = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._fatal: ForestError | None = None
        self.fatal_event = asyncio.Event()

    @property
    def fatal_error(self) -> ForestError | None:
        return self._fatal

    def _fail(self, error: ForestError) -> ForestError:
        self._fatal = error
        self.fatal_event.set()
        return error

    @property
    def registry(self) -> RuntimeModuleRegistry:
        if self._fatal is not None:
            raise self._fatal
        if self._registry is None or self._closed:
            raise ForestError(
                "DEPLOYMENT_RUNTIME_UNAVAILABLE", "Deployment package graph is unavailable", 503
            )
        return self._registry

    async def _assemble(self, snapshots: dict[str, PackageSnapshot]) -> RuntimeModuleRegistry:
        descriptors = [
            await bootstrap_builtin_deployment_package(snapshot, self.runtime)
            for _component_id, snapshot in sorted(snapshots.items())
        ]
        return RuntimeModuleRegistry(self.runtime, tuple(descriptors))

    async def start(self) -> RuntimeModuleRegistry:
        async with self._lock:
            if self._closed or self._registry is not None:
                raise ForestError(
                    "DEPLOYMENT_RUNTIME_LIFECYCLE_INVALID",
                    "Deployment package controller has already started or closed",
                    409,
                )
            defaults = {}
            for source in self.official_sources:
                snapshot = self.catalog.build(source)
                component_id = snapshot.component["id"]
                if component_id in defaults:
                    raise ForestError(
                        "DEPLOYMENT_PACKAGE_DUPLICATE", "Official component is duplicated", 409
                    )
                defaults[component_id] = snapshot
            graph, selected = await self.runtime.generation_authority.active_package_selection()
            snapshots = (
                {
                    component_id: self.catalog.get(digest)
                    for component_id, digest in selected.items()
                }
                if graph is not None
                else defaults
            )
            if not snapshots or any(
                snapshot.component["id"] != component_id
                for component_id, snapshot in snapshots.items()
            ):
                raise ForestError(
                    "DEPLOYMENT_PACKAGE_SELECTION_INVALID",
                    "Active package selection does not match its components",
                    409,
                )
            registry = await self._assemble(snapshots)
            try:
                await registry.start()
            except BaseException:
                await registry.close()
                raise
            self._registry = registry
            self._snapshots = snapshots
            return registry

    async def replace(
        self, component_id: str, package_digest: str, *, expected_graph_revision: int
    ) -> RuntimeModuleRegistry:
        """Replace one package under a graph-revision CAS; never reopen an unknown head."""

        async with self._lock:
            previous = self.registry
            if previous.graph_identity.graph_revision != expected_graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT",
                    "Deployment RuntimeGraph changed before replacement",
                    409,
                )
            if component_id not in self._snapshots:
                raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Replacement target is absent", 404)
            snapshot = self.catalog.get(package_digest)
            if snapshot.component["id"] != component_id:
                raise ForestError(
                    "DEPLOYMENT_PACKAGE_SELECTION_INVALID",
                    "Replacement artifact has another component identity",
                    409,
                )
            snapshots = {**self._snapshots, component_id: snapshot}
            candidate = await self._assemble(snapshots)
            affected = candidate.plan_graph_transition(previous, target_component_id=component_id)
            drained = False
            try:
                await candidate.prewarm()
                await previous.drain_owned_work_for_replacement(
                    candidate.activation_id,
                    components=(affected if len(affected) < len(previous.order) else None),
                    candidate_components=candidate.prepared_replacement_facts(),
                )
                drained = True
                await candidate.start()
                candidate.accept_retained_ownership()
            except BaseException as error:
                committed = candidate.publication_committed
                if drained and not committed and candidate.graph_identity is not None:
                    try:
                        head = await self.runtime.generation_authority.current_graph()
                        committed = (
                            head is None or head.graph_id != previous.graph_identity.graph_id
                        )
                    except BaseException:
                        committed = True
                try:
                    cleanup_errors = await candidate.close()
                except BaseException as cleanup_error:
                    failure = self._fail(
                        ForestError(
                            "RUNTIME_GRAPH_REPLACEMENT_CLEANUP_FAILED",
                            "Failed deployment candidate could not close",
                            503,
                        )
                    )
                    raise failure from cleanup_error
                if committed:
                    failure = self._fail(
                        ForestError(
                            "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE",
                            "Deployment Graph publication has an uncertain local outcome",
                            503,
                        )
                    )
                    raise failure from error
                if cleanup_errors:
                    failure = self._fail(
                        ForestError(
                            "RUNTIME_GRAPH_REPLACEMENT_CLEANUP_FAILED",
                            "Failed deployment candidate did not close cleanly",
                            503,
                        )
                    )
                    raise failure from cleanup_errors[0]
                if (
                    isinstance(error, ForestError)
                    and error.code == "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED"
                ):
                    self._fail(error)
                    raise
                if drained:
                    try:
                        await previous.resume_owned_work_after_failed_replacement()
                    except BaseException as resume_error:
                        failure = self._fail(
                            ForestError(
                                "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED",
                                "Previous deployment Graph could not resume",
                                503,
                            )
                        )
                        raise failure from resume_error
                raise
            self._registry = candidate
            self._snapshots = snapshots
            try:
                errors = await previous.close()
            except BaseException as close_error:
                failure = self._fail(
                    ForestError(
                        "RUNTIME_GRAPH_DRAIN_FAILED",
                        "Superseded deployment Graph could not close",
                        503,
                    )
                )
                raise failure from close_error
            if errors:
                failure = self._fail(
                    ForestError(
                        "RUNTIME_GRAPH_DRAIN_FAILED",
                        "Superseded deployment Graph did not drain cleanly",
                        503,
                    )
                )
                raise failure from errors[0]
            return candidate

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            registry = self._registry
            self._registry = None
            if registry is not None:
                errors = await registry.close()
                if errors:
                    raise ForestError(
                        "RUNTIME_GRAPH_DRAIN_FAILED",
                        "Deployment package Graph did not close cleanly",
                        503,
                    ) from errors[0]

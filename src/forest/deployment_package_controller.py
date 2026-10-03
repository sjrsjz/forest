"""Own deployment package selection and the active RuntimeGraph lifecycle."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

from forest.deployment_packages import (
    bootstrap_builtin_deployment_package,
    bootstrap_installed_deployment_package,
)
from forest.errors import ForestError
from forest.packages import PackageCatalog, PackageSnapshot, digest_json
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

    async def _assemble(
        self, snapshots: dict[str, PackageSnapshot], *, activation_id: str | None = None,
        plan_only: bool = False, frozen_configurations: dict[str, dict] | None = None,
    ) -> RuntimeModuleRegistry:
        descriptors = []
        for _component_id, snapshot in sorted(snapshots.items()):
            declared = snapshot.storage_bundle()
            official = self.catalog.is_official_deployment(snapshot.digest)
            if declared is not None and declared[0]["execution_profile"] == "official" and not official:
                raise ForestError(
                    "OFFICIAL_PACKAGE_NOT_REGISTERED",
                    "Deployment artifact has no host-issued official provenance", 403,
                )
            compile_package = (
                bootstrap_builtin_deployment_package
                if official else bootstrap_installed_deployment_package
            )
            descriptors.append(await compile_package(
                snapshot, self.runtime, plan_only=plan_only,
                frozen_configuration=(frozen_configurations or {}).get(snapshot.component["id"]),
            ))
        return RuntimeModuleRegistry(self.runtime, tuple(descriptors), activation_id=activation_id)

    def _selected_snapshot(self, digest: str) -> PackageSnapshot:
        snapshot = self.catalog.get(digest)
        declared = snapshot.storage_bundle()
        if declared is not None and declared[0]["execution_profile"] == "official":
            return self.catalog.get_official_deployment(digest)
        return snapshot

    async def _active_configurations(self, graph) -> dict[str, dict]:
        current, selection = await self.runtime.generation_authority.active_package_selection()
        if current is None or current.graph_id != graph.graph_id or (
            current.graph_revision != graph.graph_revision
        ):
            raise ForestError(
                "RUNTIME_GRAPH_NOT_CURRENT", "Deployment Graph changed", 409,
            )
        return {
            component_id: binding["configuration"]
            for component_id, binding in selection.items()
        }

    async def read_package_registry(self, query_id: str, payload: dict) -> dict:
        """Resolve one read through the currently active deployment module Generation."""

        if query_id not in {"packages", "artifact"} or not isinstance(payload, dict):
            raise ForestError(
                "DEPLOYMENT_PACKAGE_QUERY_INVALID", "Package registry query is invalid", 422
            )
        registry = self.registry
        if registry.states.get("deployment.package-registry") != "ACTIVE":
            raise ForestError(
                "DEPLOYMENT_PACKAGE_REGISTRY_UNAVAILABLE",
                "Deployment package registry Generation is unavailable", 503,
            )
        return await registry.get("deployment.package-registry").management_contract.query(
            query_id, payload
        )

    async def preview_change(
        self, *, operation_kind: str, component_id: str,
        package_digest: str, expected_graph_revision: int,
    ) -> dict:
        """Validate a candidate and calculate Graph impact without provisioning."""

        if not isinstance(operation_kind, str) or operation_kind not in {"ADD", "REPLACE"}:
            raise ForestError(
                "DEPLOYMENT_GRAPH_CHANGE_INPUT_INVALID", "Graph change kind is invalid", 422
            )
        if (
            not isinstance(component_id, str)
            or re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", component_id) is None
            or not isinstance(package_digest, str)
            or re.fullmatch(r"sha256:[a-f0-9]{64}", package_digest) is None
            or type(expected_graph_revision) is not int
            or expected_graph_revision < 1
        ):
            raise ForestError(
                "DEPLOYMENT_GRAPH_CHANGE_INPUT_INVALID", "Graph change input is invalid", 422
            )
        async with self._lock:
            previous = self.registry
            graph = previous.graph_identity
            if graph.graph_revision != expected_graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT", "Deployment Graph changed", 409
                )
            if (component_id in self._snapshots) != (operation_kind == "REPLACE"):
                raise ForestError(
                    "RUNTIME_MODULE_SELECTION_CONFLICT",
                    "Deployment module membership differs from the requested operation", 409,
                )
            snapshot = self._selected_snapshot(package_digest)
            if snapshot.component["id"] != component_id:
                raise ForestError(
                    "DEPLOYMENT_PACKAGE_SELECTION_INVALID",
                    "Candidate artifact has another component identity", 409,
                )
            candidate = await self._assemble(
                {**self._snapshots, component_id: snapshot}, plan_only=True,
                frozen_configurations=await self._active_configurations(graph),
            )
            impact = candidate.preview_graph_transition(
                previous, target_component_id=component_id
            )
            facts = {
                "protocol": "forest.deployment-graph-change-preview/1",
                "operation_kind": operation_kind,
                "source_graph_id": graph.graph_id,
                "source_graph_revision": graph.graph_revision,
                "component_id": component_id,
                "package_digest": snapshot.digest,
                "manifest_digest": digest_json(snapshot.manifest),
                "members": [
                    {"component_id": member, "descriptor_digest": descriptor_digest}
                    for member, descriptor_digest
                    in candidate.preview_descriptor_digests(previous)
                ],
                "impact": {
                    field: list(getattr(impact, field))
                    for field in ("replace", "retain", "remove", "add", "drain", "prepare")
                },
                "worker_capacity": impact.worker_capacity.as_fact(),
            }
            return {**facts, "plan_digest": digest_json(facts)}

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
                snapshot = self.catalog.build_official(source)
                component_id = snapshot.component["id"]
                if component_id in defaults:
                    raise ForestError(
                        "DEPLOYMENT_PACKAGE_DUPLICATE", "Official component is duplicated", 409
                    )
                defaults[component_id] = snapshot
            graph, selected = await self.runtime.generation_authority.active_package_selection()
            snapshots = (
                {
                    component_id: self._selected_snapshot(binding["package_digest"])
                    for component_id, binding in selected.items()
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
            registry = await self._assemble(
                snapshots,
                frozen_configurations={
                    component_id: binding["configuration"]
                    for component_id, binding in selected.items()
                },
            )
            try:
                await registry.start()
            except BaseException:
                await registry.close()
                raise
            self._registry = registry
            self._snapshots = snapshots
            return registry

    async def replace(
        self,
        component_id: str,
        package_digest: str,
        *,
        expected_graph_revision: int,
        activation_id: str | None = None,
    ) -> RuntimeModuleRegistry:
        """Replace one package under a graph-revision CAS; never reopen an unknown head."""

        return await self._change(
            component_id, package_digest,
            expected_graph_revision=expected_graph_revision,
            activation_id=activation_id, adding=False,
        )

    async def configure(
        self, component_id: str, package_digest: str, *,
        expected_graph_revision: int, configuration_revision: int,
        configuration_payload_digest: str, activation_id: str,
    ) -> RuntimeModuleRegistry:
        """Publish one committed desired revision as a new Generation."""

        if (
            type(configuration_revision) is not int
            or configuration_revision < 1
            or not isinstance(configuration_payload_digest, str)
            or re.fullmatch(r"sha256:[a-f0-9]{64}", configuration_payload_digest) is None
        ):
            raise ForestError(
                "DEPLOYMENT_CONFIGURATION_CHANGE_INVALID", "Configuration identity is invalid", 422,
            )
        return await self._change(
            component_id, package_digest,
            expected_graph_revision=expected_graph_revision,
            activation_id=activation_id, adding=False,
            configuration_binding={
                "revision": configuration_revision,
                "payload_id": "config:" + configuration_payload_digest[7:],
                "payload_digest": configuration_payload_digest,
                "source": "desired",
            },
        )

    async def add(
        self,
        component_id: str,
        package_digest: str,
        *,
        expected_graph_revision: int,
        activation_id: str | None = None,
    ) -> RuntimeModuleRegistry:
        """Publish a new deployment package Generation while retaining unrelated members."""

        return await self._change(
            component_id, package_digest,
            expected_graph_revision=expected_graph_revision,
            activation_id=activation_id, adding=True,
        )

    async def _change(
        self,
        component_id: str,
        package_digest: str,
        *,
        expected_graph_revision: int,
        activation_id: str | None,
        adding: bool,
        configuration_binding: dict | None = None,
    ) -> RuntimeModuleRegistry:

        async with self._lock:
            previous = self.registry
            if previous.graph_identity.graph_revision != expected_graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT",
                    "Deployment RuntimeGraph changed before the requested change",
                    409,
                )
            if adding == (component_id in self._snapshots):
                raise ForestError(
                    "RUNTIME_MODULE_SELECTION_CONFLICT",
                    "Deployment module membership differs from the requested operation", 409,
                )
            snapshot = self._selected_snapshot(package_digest)
            if snapshot.component["id"] != component_id:
                raise ForestError(
                    "DEPLOYMENT_PACKAGE_SELECTION_INVALID",
                    "Graph change artifact has another component identity",
                    409,
                )
            if configuration_binding is not None and (
                self._snapshots[component_id].digest != package_digest
            ):
                raise ForestError(
                    "DEPLOYMENT_CONFIGURATION_CHANGE_INVALID",
                    "Configuration change cannot replace its package", 409,
                )
            snapshots = {**self._snapshots, component_id: snapshot}
            frozen = await self._active_configurations(previous.graph_identity)
            if configuration_binding is not None:
                frozen[component_id] = configuration_binding
            candidate = await self._assemble(
                snapshots, activation_id=activation_id,
                frozen_configurations=frozen,
            )
            affected = candidate.plan_graph_transition(previous, target_component_id=component_id)
            drained = False
            try:
                await candidate.prewarm()
                if affected:
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

    async def serve_graph_changes(self, authority) -> None:
        """Reconcile durable requests before executing another Graph transition."""

        while not self._closed:
            request = await authority.claim(self.runtime.authority_lease())
            if request is None:
                await asyncio.sleep(0.5)
                continue
            try:
                evidence = await authority.published_graph(request)
                if evidence is None:
                    if request["attempt_count"] > 1:
                        raise ForestError(
                            "DEPLOYMENT_GRAPH_CHANGE_INTERRUPTED",
                            "Prior deployment Graph change ended before publication",
                            409,
                        )
                    if (
                        self.registry.graph_identity.graph_revision
                        != request["expected_graph_revision"]
                    ):
                        raise ForestError(
                            "RUNTIME_GRAPH_REVISION_CONFLICT",
                            "Deployment Graph changed before queued change",
                            409,
                        )
                    if request["operation_kind"] == "CONFIGURE":
                        await self.configure(
                            request["target_component_id"], request["package_digest"],
                            expected_graph_revision=request["expected_graph_revision"],
                            configuration_revision=request["configuration_revision"],
                            configuration_payload_digest=request["configuration_payload_digest"],
                            activation_id=request["activation_id"],
                        )
                    else:
                        operation = self.add if request["operation_kind"] == "ADD" else self.replace
                        await operation(
                            request["target_component_id"], request["package_digest"],
                            expected_graph_revision=request["expected_graph_revision"],
                            activation_id=request["activation_id"],
                        )
                    evidence = await authority.published_graph(request)
                    if evidence is None:
                        raise ForestError(
                            "DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID",
                            "Published deployment Graph has no durable evidence",
                            503,
                        )
                await authority.settle(
                    self.runtime.authority_lease(),
                    operation_id=request["operation_id"],
                    graph=evidence,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                evidence = await authority.published_graph(request)
                await authority.settle(
                    self.runtime.authority_lease(),
                    operation_id=request["operation_id"],
                    graph=evidence,
                    error_code=(
                        None
                        if evidence is not None
                        else (
                            error.code
                            if isinstance(error, ForestError)
                            else "RUNTIME_REPLACEMENT_FAILED"
                        )
                    ),
                )
                if self._fatal is not None:
                    raise self._fatal from error

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

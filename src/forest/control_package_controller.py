"""Own one control scope's package selection and RuntimeGraph lifecycle."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

from forest.control_packages import (
    bootstrap_installed_control_package,
    bootstrap_trusted_control_package,
    control_storage_bundle,
)
from forest.errors import ForestError
from forest.packages import PackageCatalog, PackageSnapshot, digest_json
from forest.runtime_modules import RuntimeModuleRegistry
from forest.schema_bundle_preflight import require_installable_storage_in


class ControlPackageController:
    def __init__(
        self, runtime, catalog: PackageCatalog, *, official_sources: tuple[Path, ...]
    ) -> None:
        if getattr(runtime, "scope_kind", None) not in {"deployment", "host"}:
            raise ValueError("Control package controller requires a control scope")
        self.runtime = runtime
        self.scope_kind = runtime.scope_kind
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
                f"{self.scope_kind.upper()}_RUNTIME_UNAVAILABLE",
                "Control package graph is unavailable", 503,
            )
        return self._registry

    def driver_exports(self) -> tuple[dict, ...]:
        """Describe active, trusted Worker services offered as driver kinds.

        The source reference excludes the graph revision: an unrelated control
        package replacement must retain the same provider Generation identity.
        This read port does not admit Bot calls or lease the provider.
        """

        registry = self.registry
        graph = registry.graph_identity
        if graph is None:
            raise ForestError(
                "CONTROL_DRIVER_EXPORT_UNAVAILABLE",
                "Control Graph has no published Generation identity", 503,
            )
        exports = []
        for component_id, snapshot in sorted(self._snapshots.items()):
            declarations = snapshot.component.get("effect_driver_exports", ())
            if not declarations:
                continue
            generation = registry.generation_bindings[component_id]
            if generation.trust_level not in {"official", "reviewed"}:
                raise ForestError(
                    "CONTROL_DRIVER_EXPORT_UNTRUSTED",
                    "Untrusted control Generation cannot export a driver", 403,
                )
            services = {
                item["name"]: item for item in snapshot.component["provides"]
            }
            for declaration in declarations:
                service = services[declaration["service"]]
                identity = {
                    "protocol": "forest.control-effect-driver-source/1",
                    "scope": self.scope_kind,
                    "scope_id": self.runtime.scope_id,
                    "component_id": component_id,
                    "generation_id": graph.generations[component_id],
                    "package_digest": snapshot.digest,
                    "trust_level": generation.trust_level,
                    "kind": declaration["kind"],
                    "service_id": service["name"],
                    "contract": service["contract"],
                    "semantics": service["semantics"],
                    "input_schema_digest": digest_json(
                        snapshot.schema(service["input_schema"])
                    ),
                    "output_schema_digest": digest_json(
                        snapshot.schema(service["output_schema"])
                    ),
                }
                exports.append({
                    **identity,
                    "source_ref": digest_json(identity),
                    "graph_id": graph.graph_id,
                    "graph_revision": graph.graph_revision,
                })
        return tuple(sorted(exports, key=lambda item: (item["kind"], item["component_id"])))

    async def _assemble(
        self, snapshots: dict[str, PackageSnapshot], *, activation_id: str | None = None,
        plan_only: bool = False, frozen_configurations: dict[str, dict] | None = None,
    ) -> RuntimeModuleRegistry:
        exported_kinds = [
            declaration["kind"]
            for snapshot in snapshots.values()
            for declaration in snapshot.component.get("effect_driver_exports", ())
        ]
        if len(exported_kinds) != len(set(exported_kinds)):
            raise ForestError(
                "CONTROL_DRIVER_KIND_CONFLICT",
                "Control Graph cannot publish duplicate driver kinds", 409,
            )
        descriptors = []
        for _component_id, snapshot in sorted(snapshots.items()):
            declared = snapshot.storage_bundle()
            official = getattr(
                self.catalog, f"is_official_{self.scope_kind}"
            )(snapshot.digest)
            reviewed = self.catalog.is_reviewed_control(
                snapshot.digest, scope=self.scope_kind,
            )
            if (
                declared is not None
                and declared[0]["execution_profile"] == "official"
                and not (official or reviewed)
            ):
                raise ForestError(
                    "REVIEWED_PACKAGE_NOT_REGISTERED",
                    "Control artifact has no owner-issued trusted provenance", 403,
                )
            if official or reviewed:
                descriptor = await bootstrap_trusted_control_package(
                    snapshot, self.runtime, plan_only=plan_only,
                    frozen_configuration=(frozen_configurations or {}).get(snapshot.component["id"]),
                    trust_level="official" if official else "reviewed",
                )
            else:
                descriptor = await bootstrap_installed_control_package(
                    snapshot, self.runtime, plan_only=plan_only,
                    frozen_configuration=(frozen_configurations or {}).get(snapshot.component["id"]),
                )
            descriptors.append(descriptor)
        return RuntimeModuleRegistry(self.runtime, tuple(descriptors), activation_id=activation_id)

    def _selected_snapshot(self, digest: str) -> PackageSnapshot:
        snapshot = self.catalog.get(digest)
        declared = snapshot.storage_bundle()
        if self.catalog.is_reviewed_control(digest, scope=self.scope_kind):
            return self.catalog.get_reviewed_control(digest, scope=self.scope_kind)
        if declared is not None and declared[0]["execution_profile"] == "official":
            return getattr(self.catalog, f"get_official_{self.scope_kind}")(digest)
        return snapshot

    async def _active_configurations(self, graph) -> dict[str, dict]:
        current, selection = await self.runtime.generation_authority.active_package_selection()
        if current is None or current.graph_id != graph.graph_id or (
            current.graph_revision != graph.graph_revision
        ):
            raise ForestError(
                "RUNTIME_GRAPH_NOT_CURRENT", "Control Graph changed", 409,
            )
        return {
            component_id: binding["configuration"]
            for component_id, binding in selection.items()
        }

    async def preview_change(
        self, *, operation_kind: str, component_id: str,
        package_digest: str, expected_graph_revision: int,
    ) -> dict:
        """Validate a candidate and calculate Graph impact without provisioning."""

        if not isinstance(operation_kind, str) or operation_kind not in {"ADD", "REPLACE"}:
            raise ForestError(
                f"{self.scope_kind.upper()}_GRAPH_CHANGE_INPUT_INVALID",
                "Graph change kind is invalid", 422,
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
                f"{self.scope_kind.upper()}_GRAPH_CHANGE_INPUT_INVALID",
                "Graph change input is invalid", 422,
            )
        async with self._lock:
            previous = self.registry
            graph = previous.graph_identity
            if graph.graph_revision != expected_graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT", "Control Graph changed", 409
                )
            if (component_id in self._snapshots) != (operation_kind == "REPLACE"):
                raise ForestError(
                    "RUNTIME_MODULE_SELECTION_CONFLICT",
                    "Control module membership differs from the requested operation", 409,
                )
            snapshot = self._selected_snapshot(package_digest)
            if snapshot.component["id"] != component_id:
                raise ForestError(
                    f"{self.scope_kind.upper()}_PACKAGE_SELECTION_INVALID",
                    "Candidate artifact has another component identity", 409,
                )
            candidate = await self._assemble(
                {**self._snapshots, component_id: snapshot}, plan_only=True,
                frozen_configurations=await self._active_configurations(graph),
            )
            async with self.runtime.store.pool.connection() as conn:
                await require_installable_storage_in(
                    conn, scope=self.scope_kind, scope_id=self.runtime.scope_id,
                    storages=tuple(
                        control_storage_bundle(item, self.scope_kind)
                        for item in {**self._snapshots, component_id: snapshot}.values()
                    ),
                )
            impact = candidate.preview_graph_transition(
                previous, target_component_id=component_id
            )
            facts = {
                "protocol": f"forest.{self.scope_kind}-graph-change-preview/1",
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
                    f"{self.scope_kind.upper()}_RUNTIME_LIFECYCLE_INVALID",
                    "Control package controller has already started or closed",
                    409,
                )
            defaults = {}
            for source in self.official_sources:
                snapshot = self.catalog.build_official(source, scope=self.scope_kind)
                component_id = snapshot.component["id"]
                if component_id in defaults:
                    raise ForestError(
                        f"{self.scope_kind.upper()}_PACKAGE_DUPLICATE",
                        "Official component is duplicated", 409,
                    )
                defaults[component_id] = snapshot
            graph, selected = await self.runtime.generation_authority.active_package_selection()
            snapshots = (
                {
                    component_id: self._selected_snapshot(binding["package_digest"])
                    for component_id, binding in selected.items()
                }
                if graph is not None or selected
                else defaults
            )
            if not snapshots or any(
                snapshot.component["id"] != component_id
                for component_id, snapshot in snapshots.items()
            ):
                raise ForestError(
                    f"{self.scope_kind.upper()}_PACKAGE_SELECTION_INVALID",
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
                f"{self.scope_kind.upper()}_CONFIGURATION_CHANGE_INVALID",
                "Configuration identity is invalid", 422,
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
        """Publish a new control package Generation while retaining unrelated members."""

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
                    "Control RuntimeGraph changed before the requested change",
                    409,
                )
            if adding == (component_id in self._snapshots):
                raise ForestError(
                    "RUNTIME_MODULE_SELECTION_CONFLICT",
                    "Control module membership differs from the requested operation", 409,
                )
            snapshot = self._selected_snapshot(package_digest)
            if snapshot.component["id"] != component_id:
                raise ForestError(
                    f"{self.scope_kind.upper()}_PACKAGE_SELECTION_INVALID",
                    "Graph change artifact has another component identity",
                    409,
                )
            if configuration_binding is not None and (
                self._snapshots[component_id].digest != package_digest
            ):
                raise ForestError(
                    f"{self.scope_kind.upper()}_CONFIGURATION_CHANGE_INVALID",
                    "Configuration change cannot replace its package", 409,
                )
            snapshots = {**self._snapshots, component_id: snapshot}
            frozen = await self._active_configurations(previous.graph_identity)
            if configuration_binding is not None:
                frozen[component_id] = configuration_binding
            await self._assemble(
                snapshots, plan_only=True, frozen_configurations=frozen,
            )
            async with self.runtime.store.pool.connection() as conn:
                await require_installable_storage_in(
                    conn, scope=self.scope_kind, scope_id=self.runtime.scope_id,
                    storages=tuple(
                        control_storage_bundle(item, self.scope_kind)
                        for item in snapshots.values()
                    ),
                )
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
                            "Failed control candidate could not close",
                            503,
                        )
                    )
                    raise failure from cleanup_error
                if committed:
                    failure = self._fail(
                        ForestError(
                            "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE",
                            "Control Graph publication has an uncertain local outcome",
                            503,
                        )
                    )
                    raise failure from error
                if cleanup_errors:
                    failure = self._fail(
                        ForestError(
                            "RUNTIME_GRAPH_REPLACEMENT_CLEANUP_FAILED",
                            "Failed control candidate did not close cleanly",
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
                                "Previous control Graph could not resume",
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
                        "Superseded control Graph could not close",
                        503,
                    )
                )
                raise failure from close_error
            if errors:
                failure = self._fail(
                    ForestError(
                        "RUNTIME_GRAPH_DRAIN_FAILED",
                        "Superseded control Graph did not drain cleanly",
                        503,
                    )
                )
                raise failure from errors[0]
            return candidate

    async def serve_graph_changes(self, authority) -> None:
        """Reconcile durable requests before executing another Graph transition."""

        if getattr(authority, "scope", None) != self.scope_kind:
            raise ValueError("Graph change authority belongs to another control scope")
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
                            f"{self.scope_kind.upper()}_GRAPH_CHANGE_INTERRUPTED",
                            "Prior control Graph change ended before publication",
                            409,
                        )
                    if (
                        self.registry.graph_identity.graph_revision
                        != request["expected_graph_revision"]
                    ):
                        raise ForestError(
                            "RUNTIME_GRAPH_REVISION_CONFLICT",
                            "Control Graph changed before queued change",
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
                            f"{self.scope_kind.upper()}_GRAPH_CHANGE_RESULT_INVALID",
                            "Published control Graph has no durable evidence",
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
                        "Control package Graph did not close cleanly",
                        503,
                    ) from errors[0]

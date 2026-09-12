"""Per-Bot coordinator: database admission, supervised workers and scoped RPC.

No transaction spans plugin execution. Database fences remain authoritative even
if this process loses a lease or keeps an obsolete in-memory route.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

from forest.drivers.python_worker import PythonWorker, WorkerIdentity, WorkerLimits
from forest.errors import ForestError
from forest.packages import PackageSnapshot, contract_matches, digest_json
from forest.storage.bot import BotStore, CallGrant, HostLease


@dataclass
class LoadedComponent:
    snapshot: PackageSnapshot
    worker: PythonWorker
    grants: frozenset[str]
    validators: dict[str, tuple[Draft202012Validator, Draft202012Validator, int]]
    policy_revision: int
    route_epoch: int = 0
    dependencies: tuple[str, ...] = ()
    service_bindings: dict[str, tuple[str, str]] = field(default_factory=dict)
    degraded_dependencies: list[dict[str, str]] = field(default_factory=list)


@dataclass
class DependencyPlan:
    order: list[str]
    bindings: dict[str, dict[str, tuple[str, str]]]
    closures: dict[str, tuple[str, ...]]
    degraded: dict[str, list[dict[str, str]]]


def resolve_dependencies(snapshots: dict[str, PackageSnapshot]) -> DependencyPlan:
    """Resolve only immutable manifests; never import plugin code or touch a DB."""
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
    bindings, graph, degraded = {}, {}, {}
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
                        code, "Required component service is unavailable or incompatible", 422
                    )
                degraded[component_id].append(
                    {"service": required["name"], "contract": required["contract"], "reason": code}
                )
                continue
            bindings[component_id][required["name"]] = matches[0]
            graph[component_id].add(matches[0][0])
    order, visiting, visited = [], set(), set()

    def visit(component_id):
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
    closures = {}
    for component_id in order:
        closure = set(graph[component_id])
        for target in graph[component_id]:
            closure.update(closures[target])
        closures[component_id] = tuple(sorted(closure))
    return DependencyPlan(order, bindings, closures, degraded)


class Kernel:
    def __init__(
        self,
        store: BotStore,
        *,
        host_id: str = "local",
        lease_seconds: float = 30,
        max_workers: int = 16,
        max_calls: int = 256,
        max_control_calls: int = 8,
    ) -> None:
        if (
            not 3 <= lease_seconds <= 120
            or not 1 <= max_workers <= 64
            or not 1 <= max_calls <= 4096
            or not 1 <= max_control_calls <= 16
        ):
            raise ValueError("Invalid kernel resource limits")
        self.store = store
        self.host_id = host_id
        self.incarnation = str(uuid4())
        self.lease_seconds = lease_seconds
        self.max_workers, self.max_calls = max_workers, max_calls
        self.max_control_calls = max_control_calls
        self.lease: HostLease | None = None
        self.components: dict[str, LoadedComponent] = {}
        self._workers: set[PythonWorker] = set()
        self._calls: dict[str, tuple[WorkerIdentity, CallGrant, frozenset[str]]] = {}
        self._call_bindings: dict[str, dict[str, tuple[str, str]]] = {}
        self._call_count = 0
        self._control_call_count = 0
        self._calls_changed = asyncio.Condition()
        self._operations: dict[str, int] = {}
        self._deployment_lock = asyncio.Lock()
        self._plan_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._heartbeat: asyncio.Task | None = None
        self._closing = False
        self._close_task: asyncio.Task | None = None
        self.failure: str | None = None
        self.cleanup_errors: list[str] = []

    async def start(self) -> None:
        async with self._start_lock:
            if self.lease is not None or self._closing:
                raise ForestError("LIFECYCLE_INVALID", "Kernel cannot be started twice")
            self.lease = await self.store.acquire_host(
                self.host_id,
                self.incarnation,
                ttl_seconds=self.lease_seconds,
            )
            if self._closing:
                raise ForestError(
                    "KERNEL_CLOSING", "Kernel closed while acquiring execution authority"
                )
            self._heartbeat = asyncio.create_task(self._renew(), name="kernel-lease-renewal")

    def _authority(self, *, allow_closing: bool = False) -> HostLease:
        if self.lease is None or (self._closing and not allow_closing) or self.failure is not None:
            raise ForestError("KERNEL_UNAVAILABLE", "Kernel has no valid local admission", 503)
        return self.lease

    async def _renew(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.lease_seconds / 3)
                self.lease = await self.store.renew_host(
                    self._authority(allow_closing=True),
                    ttl_seconds=self.lease_seconds,
                )
                for operation_id, epoch in list(self._operations.items()):
                    try:
                        await self.store.renew_operation(self.lease, operation_id, epoch, 120)
                    except ForestError as exc:
                        if exc.code not in {
                            "OPERATION_COMPLETED",
                            "OPERATION_NOT_ACTIVE",
                            "INVALID_OPERATION_PHASE",
                        }:
                            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure = exc.code if isinstance(exc, ForestError) else "AUTHORITY_UNAVAILABLE"
            # A lost lease never authorizes a final write or a cleanup against a new owner.
            await self._terminate_remaining()

    async def _terminate_remaining(self) -> None:
        workers = list(self._workers)
        results = await asyncio.gather(
            *(worker.terminate() for worker in workers), return_exceptions=True
        )
        for worker, result in zip(workers, results):
            if isinstance(result, BaseException) or worker.alive:
                self.cleanup_errors.append("WORKER_CLEANUP_FAILED")
            if not worker.alive:
                self._workers.discard(worker)

    @staticmethod
    def _route_receipt(route: dict) -> dict:
        return {
            key: route[key]
            for key in ("bot_id", "component_id", "generation_id", "route_epoch", "admission")
        }

    @staticmethod
    def _validate(validator: Draft202012Validator, value: Any, code: str) -> None:
        try:
            encoded = json.dumps(value, allow_nan=False).encode("utf-8")
        except (ValueError, TypeError, UnicodeError):
            raise ForestError(code, "Value is not a JSON DTO", 422) from None
        if len(encoded) > 200_000 or not validator.is_valid(value):
            raise ForestError(code, "Value does not match the pinned package schema", 422)

    @staticmethod
    def _normalize_grants(grants) -> frozenset[str]:
        if (
            not isinstance(grants, (list, tuple, set, frozenset))
            or len(grants) > 64
            or any(not isinstance(item, str) or not 1 <= len(item) <= 256 for item in grants)
        ):
            raise ForestError(
                "INVALID_GRANTS", "Capability grants must be a bounded string set", 422
            )
        return frozenset(grants)

    def _dependency_plan(
        self, replacements: dict[str, PackageSnapshot] | None = None
    ) -> DependencyPlan:
        snapshots = {key: component.snapshot for key, component in self.components.items()}
        snapshots.update(replacements or {})
        return resolve_dependencies(snapshots)

    def _apply_dependencies(self, plan: DependencyPlan) -> None:
        for key, component in self.components.items():
            component.dependencies = plan.closures[key]
            component.service_bindings = dict(plan.bindings[key])
            component.degraded_dependencies = list(plan.degraded[key])

    def dependency_status(self) -> dict:
        return {
            key: {
                "dependency_components": list(component.dependencies),
                "degraded_dependencies": [dict(item) for item in component.degraded_dependencies],
            }
            for key, component in self.components.items()
        }

    async def install(
        self,
        snapshot: PackageSnapshot,
        *,
        config: dict | None = None,
        grants: frozenset[str] = frozenset(),
        operation_id: str | None = None,
    ) -> dict:
        async with self._plan_lock:
            return await self._install_one(
                snapshot, config=config, grants=grants, operation_id=operation_id
            )

    async def install_many(self, specifications: list[dict]) -> list[dict]:
        """Preflight the complete DAG, then deploy dependencies before callers.

        Each route remains its own durable operation. A later failure preserves
        successful routes and reports their receipts; no atomic batch is implied.
        """
        if not isinstance(specifications, list) or not 1 <= len(specifications) <= self.max_workers:
            raise ForestError(
                "INVALID_DEPLOYMENT_PLAN", "A bounded nonempty component list is required", 422
            )
        async with self._plan_lock:
            self._authority()
            by_component = {}
            for item in specifications:
                if (
                    not isinstance(item, dict)
                    or set(item) - {"snapshot", "config", "grants", "operation_id"}
                    or "snapshot" not in item
                ):
                    raise ForestError(
                        "INVALID_DEPLOYMENT_PLAN", "Invalid component deployment entry", 422
                    )
                snapshot = item["snapshot"]
                snapshot = PackageSnapshot(
                    snapshot.root, snapshot.digest, json.loads(json.dumps(snapshot.manifest))
                )
                key = snapshot.component["id"]
                if key in by_component:
                    raise ForestError(
                        "DUPLICATE_COMPONENT",
                        "A deployment plan cannot replace a component twice",
                        422,
                    )
                config = item.get("config", {})
                self._validate(Draft202012Validator({}), config, "CONFIG_INVALID")
                config = json.loads(json.dumps(config, allow_nan=False))
                self._validate(
                    Draft202012Validator(snapshot.schema(snapshot.component["config_schema"])),
                    config,
                    "CONFIG_INVALID",
                )
                grants = self._normalize_grants(item.get("grants", ()))
                if not set(snapshot.component.get("capabilities", {}).get("requests", [])).issubset(
                    grants
                ):
                    raise ForestError(
                        "CAPABILITY_DENIED", "Required package capabilities were not granted", 403
                    )
                by_component[key] = {
                    "snapshot": snapshot,
                    "config": config,
                    "grants": grants,
                    "operation_id": item.get("operation_id") or str(uuid4()),
                }
            for item in by_component.values():
                await asyncio.to_thread(item["snapshot"].verify)
            plan = self._dependency_plan(
                {key: item["snapshot"] for key, item in by_component.items()}
            )
            completed = []
            try:
                for key in plan.order:
                    if key in by_component:
                        completed.append(await self._install_one(**by_component[key]))
            except ForestError as error:
                raise ForestError(
                    error.code,
                    error.message,
                    error.status,
                    {"completed": completed, "failed_component": key},
                ) from None
            return completed

    async def _install_one(
        self,
        snapshot: PackageSnapshot,
        *,
        config: dict | None = None,
        grants: frozenset[str] = frozenset(),
        operation_id: str | None = None,
    ) -> dict:
        """Prepare then drain/publish one component; incompatible migration is rejected."""
        # Capture caller-owned objects before awaiting I/O; later UI edits must not
        # change a registered generation or the payload associated with a digest.
        config = {} if config is None else config
        self._validate(Draft202012Validator({}), config, "CONFIG_INVALID")
        config = json.loads(json.dumps(config, allow_nan=False))
        snapshot = PackageSnapshot(
            snapshot.root,
            snapshot.digest,
            json.loads(json.dumps(snapshot.manifest, allow_nan=False)),
        )
        grants = self._normalize_grants(grants)
        async with self._deployment_lock:
            authority = self._authority()
            await asyncio.to_thread(snapshot.verify)
            spec = snapshot.component
            dependency_plan = self._dependency_plan({spec["id"]: snapshot})
            self._validate(
                Draft202012Validator(snapshot.schema(spec["config_schema"])),
                config,
                "CONFIG_INVALID",
            )
            requested = frozenset(spec.get("capabilities", {}).get("requests", []))
            if not requested.issubset(grants):
                raise ForestError(
                    "CAPABILITY_DENIED", "Required package capabilities were not granted", 403
                )
            if len(self._workers) >= self.max_workers:
                raise ForestError(
                    "INSUFFICIENT_HEADROOM", "No capacity for a candidate Worker", 429
                )
            validators = {
                svc["name"]: (
                    Draft202012Validator(snapshot.schema(svc["input_schema"])),
                    Draft202012Validator(snapshot.schema(svc["output_schema"])),
                    int(svc["contract"].split(".")[0]),
                )
                for svc in spec["provides"]
            }
            component_id = spec["id"]
            old = self.components.get(component_id)
            if old is not None and not old.worker.alive:
                await self._retire(old, reason="worker_failed", revoke=True)
                if old.worker in self._workers:
                    raise ForestError(
                        "WORKER_CLEANUP_FAILED", "Failed Worker resources remain owned", 503
                    )
                self.components.pop(component_id, None)
                old = None
            old_route = await self.store.get_route(component_id)
            epoch = old_route["route_epoch"] if old_route else 0
            operation_id = operation_id or str(uuid4())
            previous = await self.store.read_operation(operation_id)
            if previous is not None:
                expected_digest = digest_json(
                    {
                        "package": snapshot.digest,
                        "config": config,
                        "route_epoch": previous["expected_route_epoch"],
                        "grants": sorted(grants),
                    }
                )
                if (
                    previous["input_digest"] != expected_digest
                    or previous["component_id"] != component_id
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Lifecycle operation has different input"
                    )
                # A replay is a read of a current committed route, never activation of
                # an old disposed Worker or reconstruction from an obsolete snapshot.
                await self.store.snapshot_state(authority, component_id)
                if previous["phase"] == "COMMITTED":
                    current = await self.store.get_route(component_id)
                    if (
                        not current
                        or current["generation_id"] != previous["candidate_generation_id"]
                    ):
                        raise ForestError(
                            "ROUTE_CONFLICT", "The committed route was subsequently replaced"
                        )
                    return self._route_receipt(current)
                raise ForestError(
                    "OPERATION_PENDING"
                    if previous["phase"] in {"PLAN", "FROZEN", "QUIESCENT"}
                    else "OPERATION_FAILED",
                    "Lifecycle operation already exists",
                )
            identity = WorkerIdentity(
                instance_id=str(uuid4()),
                generation_id=str(uuid4()),
                incarnation=str(uuid4()),
                bot_id=self.store.bot_id,
            )
            worker = PythonWorker(
                snapshot,
                identity,
                self._state_rpc,
                limits=WorkerLimits(**spec.get("resources", {})),
                service_handler=self._service_rpc,
            )
            policy_revision = (await self.store.runtime())["policy_revision"]
            new = LoadedComponent(snapshot, worker, requested & grants, validators, policy_revision)
            published = False
            frozen = False
            operation = None
            timeout = spec.get("lifecycle", {})
            self._workers.add(worker)
            try:
                await self.store.register_generation(
                    authority,
                    component_id,
                    identity.generation_id,
                    snapshot.digest,
                    digest_json(config),
                    identity.incarnation,
                    config=config,
                    instance_id=identity.instance_id,
                    capability_grants=sorted(grants),
                )
                operation = await self.store.begin_operation(
                    authority,
                    component_id,
                    operation_id,
                    digest_json(
                        {
                            "package": snapshot.digest,
                            "config": config,
                            "route_epoch": epoch,
                            "grants": sorted(grants),
                        }
                    ),
                    identity.generation_id,
                    epoch,
                    ttl_seconds=120,
                )
                self._operations[operation_id] = operation["op_epoch"]
                await worker.launch(timeout=timeout.get("prepare_timeout_s", 10))
                state = await self.store.snapshot_state(self._authority(), component_id)
                await worker.prepare(
                    config,
                    restored_state={"durable_state": state},
                    timeout=timeout.get("prepare_timeout_s", 10),
                )
                await worker.start(timeout=timeout.get("start_timeout_s", 10))
                if (await worker.health()).get("healthy") is not True:
                    raise ForestError("WORKER_UNHEALTHY", "Candidate failed readiness")
                await self.store.mark_ready(self._authority(), identity.generation_id)
                await self.store.freeze(self._authority(), operation_id, operation["op_epoch"])
                frozen = True
                if old is not None:
                    # Existing parent trees may not have called this target yet.
                    # Keep transport ACTIVE until every reserved caller drains;
                    # PostgreSQL freeze already blocks new top-level reservations.
                    async with asyncio.timeout(timeout.get("drain_timeout_s", 30)):
                        async with self._calls_changed:
                            await self._calls_changed.wait_for(
                                lambda: (
                                    not any(
                                        entry[1].component_id == component_id
                                        or component_id in entry[1].dependency_components
                                        for entry in self._calls.values()
                                    )
                                )
                            )
                    await old.worker.quiesce(timeout=timeout.get("drain_timeout_s", 30))
                    await self.store.quiesce_operation(
                        self._authority(), operation_id, operation["op_epoch"]
                    )
                    await worker.restore(await old.worker.snapshot())
                    if (await worker.health()).get("healthy") is not True:
                        raise ForestError("WORKER_UNHEALTHY", "Restored candidate failed readiness")
                # Driver completion follows owned children and their IPC; database also
                # verifies no persistent admitted calls remain at the publication point.
                route = await self.store.publish(
                    self._authority(), operation_id, operation["op_epoch"]
                )
                published = True
                new.route_epoch = route["route_epoch"]
                self.components[component_id] = new
                self._apply_dependencies(dependency_plan)
                await worker.activate()  # transport admission only; no plugin activation hook
                if old is not None:
                    await self._retire(old, reason="replaced", revoke=False)
                return self._route_receipt(route)
            except BaseException:
                if not published and operation is not None:
                    # Losing the publication acknowledgement is not evidence of
                    # rollback. Never resume old code based only on a local flag.
                    try:
                        committed = await self.store.read_operation(operation_id)
                        published = committed is not None and committed["phase"] == "COMMITTED"
                    except Exception:
                        # Authority cannot be read: stop both execution containers;
                        # a future coordinator recovers the committed DB route.
                        published = True
                if published:
                    self.components.pop(component_id, None)
                    await self._retire(new, reason="publish_failed", revoke=True)
                    if old is not None:
                        await self._retire(old, reason="replaced", revoke=False)
                else:
                    if operation is not None:
                        with suppress(Exception):
                            await self.store.abort_operation(
                                authority, operation_id, operation["op_epoch"]
                            )
                    await self._retire(new, reason="candidate_failed", revoke=True)
                    if frozen and old is not None:
                        if old.worker.alive and not self._closing:
                            try:
                                if old.worker.state == "QUIESCENT":
                                    await old.worker.resume()
                                elif old.worker.state != "ACTIVE":
                                    raise ForestError(
                                        "RESUME_UNSAFE",
                                        "Old generation did not remain active or become quiescent",
                                    )
                            except Exception:
                                await self._retire(old, reason="resume_failed", revoke=True)
                                self.components.pop(component_id, None)
                        else:
                            await self._retire(old, reason="drain_failed", revoke=True)
                            self.components.pop(component_id, None)
                raise
            finally:
                self._operations.pop(operation_id, None)

    async def _retire(self, component: LoadedComponent, *, reason: str, revoke: bool) -> None:
        worker = component.worker
        if revoke and self.lease is not None and self.failure is None:
            try:
                await self.store.fail_generation(self.lease, worker.identity.generation_id, reason)
            except Exception:
                self.cleanup_errors.append("REVOKE_FAILED")
        try:
            await worker.stop(
                reason=reason,
                timeout=component.snapshot.component.get("lifecycle", {}).get("stop_timeout_s", 5),
            )
        except BaseException:
            await worker.terminate()
        if worker.alive:
            self.cleanup_errors.append("WORKER_CLEANUP_FAILED")
            return
        self._workers.discard(worker)
        if self.lease is not None and self.failure is None:
            try:
                await self.store.dispose_generation(
                    self.lease, worker.identity.generation_id, reason
                )
            except Exception:
                self.cleanup_errors.append("DISPOSE_RECORD_FAILED")

    async def _state_rpc(
        self, identity: WorkerIdentity, call_id: str, action: str, payload: dict
    ) -> Any:
        self._authority(allow_closing=True)
        admitted = self._calls.get(call_id)
        if admitted is None or admitted[0] != identity:
            raise ForestError(
                "INVALID_CALL_CONTEXT", "This connection has no such admitted call", 403
            )
        _, grant, capabilities = admitted
        capability = {"get": "state.read", "commit": "state.write"}.get(action)
        if capability is None or capability not in capabilities:
            raise ForestError("CAPABILITY_DENIED", "State capability is not granted", 403)
        fields = (
            {"key"} if action == "get" else {"key", "value", "expected_revision", "operation_id"}
        )
        if (
            set(payload) != fields
            or not isinstance(payload.get("key"), str)
            or not (1 <= len(payload["key"]) <= 128)
        ):
            raise ForestError("INVALID_STATE_REQUEST", "State RPC has invalid fields", 422)
        if action == "get":
            return await self.store.read_state(grant, payload["key"])
        if type(payload["expected_revision"]) is not int or payload["expected_revision"] < 0:
            raise ForestError(
                "INVALID_STATE_REQUEST", "State revision must be a nonnegative integer", 422
            )
        if (
            not isinstance(payload["operation_id"], str)
            or not 1 <= len(payload["operation_id"]) <= 128
        ):
            raise ForestError(
                "INVALID_STATE_REQUEST", "State step requires a bounded operation ID", 422
            )
        return await self.store.commit_state(
            grant,
            payload["key"],
            payload["expected_revision"],
            payload["value"],
            payload["operation_id"],
            digest_json({k: v for k, v in payload.items() if k != "operation_id"}),
        )

    async def invoke(
        self,
        component_id: str,
        service: str,
        payload: Any,
        *,
        operation_id: str,
        contract_major: int = 1,
        timeout: float = 30,
        expected_policy_revision: int | None = None,
    ) -> Any:
        self._authority()
        if expected_policy_revision is not None and (
            type(expected_policy_revision) is not int or expected_policy_revision < 1
        ):
            raise ForestError("INVALID_CALL", "Policy revision must be a positive integer", 422)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= 120
        ):
            raise ForestError(
                "INVALID_CALL", "Call timeout must be between zero and 120 seconds", 422
            )
        self._validate(Draft202012Validator({}), payload, "INPUT_INVALID")
        payload = json.loads(json.dumps(payload, allow_nan=False))
        if self._call_count >= self.max_calls:
            raise ForestError("BACKPRESSURE", "Kernel call capacity is exhausted", 429)
        self._call_count += 1
        try:
            async with asyncio.timeout(timeout):
                return await self._invoke(
                    component_id,
                    service,
                    payload,
                    operation_id=operation_id,
                    contract_major=contract_major,
                    timeout=timeout,
                    expected_policy_revision=expected_policy_revision,
                )
        finally:
            self._call_count -= 1

    async def invoke_control(
        self,
        component_id: str,
        service: str,
        payload: Any,
        *,
        operation_id: str,
        contract_major: int = 1,
        timeout: float = 10,
        expected_policy_revision: int | None = None,
    ) -> Any:
        """Host-only bounded codec lane, available during normal business pause.

        No Worker RPC or plugin priority field selects this entry point. It has
        separate host capacity, no state grants, and no component dependencies.
        """
        self._authority()
        if component_id != "adapter.onebot" or service not in {
            "onebot.normalize",
            "onebot.prepare",
        }:
            raise ForestError(
                "CONTROL_SCOPE_DENIED", "Control invocation is restricted to the OneBot codec", 403
            )
        if type(timeout) not in {int, float} or not 0 < timeout <= 30:
            raise ForestError(
                "INVALID_CALL", "Control codec deadline must be zero to 30 seconds", 422
            )
        if expected_policy_revision is not None and (
            type(expected_policy_revision) is not int or expected_policy_revision < 1
        ):
            raise ForestError("INVALID_CALL", "Policy revision must be a positive integer", 422)
        self._validate(Draft202012Validator({}), payload, "INPUT_INVALID")
        payload = json.loads(json.dumps(payload, allow_nan=False))
        if self._control_call_count >= self.max_control_calls:
            raise ForestError("BACKPRESSURE", "Control codec capacity is exhausted", 429)
        self._control_call_count += 1
        try:
            async with asyncio.timeout(timeout):
                return await self._invoke(
                    component_id,
                    service,
                    payload,
                    operation_id=operation_id,
                    contract_major=contract_major,
                    timeout=timeout,
                    expected_policy_revision=expected_policy_revision,
                    admission_lane="CONTROL",
                )
        finally:
            self._control_call_count -= 1

    async def _service_rpc(
        self, identity: WorkerIdentity, parent_call_id: str, request: dict
    ) -> Any:
        self._authority(allow_closing=True)
        parent = self._calls.get(parent_call_id)
        if parent is None or parent[0] != identity:
            raise ForestError(
                "INVALID_CALL_CONTEXT", "Service RPC has no authenticated parent invocation", 403
            )
        if parent[1].admission_lane != "DATA":
            raise ForestError(
                "CONTROL_SCOPE_DENIED", "Control codec cannot call another component", 403
            )
        if not isinstance(request, dict) or set(request) != {
            "service",
            "payload",
            "contract_major",
            "operation_id",
            "timeout",
        }:
            raise ForestError(
                "INVALID_SERVICE_REQUEST", "Service RPC contains unsupported fields", 422
            )
        name, major = request["service"], request["contract_major"]
        if (
            not isinstance(name, str)
            or not 1 <= len(name) <= 128
            or type(major) is not int
            or major < 1
        ):
            raise ForestError(
                "INVALID_SERVICE_REQUEST", "Service name or contract major is invalid", 422
            )
        if "service.call:" + name not in parent[2]:
            raise ForestError(
                "CAPABILITY_DENIED", "Caller is not granted this component service", 403
            )
        binding = self._call_bindings.get(parent_call_id, {}).get(name)
        if binding is None:
            raise ForestError(
                "DEPENDENCY_UNAVAILABLE",
                "Service is not in this call's resolved dependency plan",
                409,
            )
        target_id, version = binding
        if int(version.split(".")[0]) != major:
            raise ForestError(
                "CONTRACT_INCOMPATIBLE", "Requested major differs from the declared dependency", 422
            )
        ancestor = parent_call_id
        depth = 0
        while ancestor is not None:
            entry = self._calls.get(ancestor)
            if entry is None:
                raise ForestError(
                    "INVALID_CALL_CONTEXT", "An ancestor invocation is no longer live", 403
                )
            if entry[1].component_id == target_id:
                raise ForestError(
                    "CALL_CYCLE", "Synchronous calls cannot revisit an ancestor component", 409
                )
            depth += 1
            ancestor = entry[1].parent_call_id
        if depth > 8:
            raise ForestError(
                "CALL_DEPTH_EXCEEDED", "Component call tree exceeds eight nested calls", 429
            )
        remaining = (parent[1].deadline_at - datetime.now(timezone.utc)).total_seconds()
        requested_timeout = request["timeout"]
        if requested_timeout is not None and (
            type(requested_timeout) not in {int, float} or not 0 < requested_timeout <= 120
        ):
            raise ForestError("INVALID_SERVICE_REQUEST", "Invalid child call timeout", 422)
        timeout = min(
            120, remaining, requested_timeout if requested_timeout is not None else remaining
        )
        if timeout <= 0:
            raise ForestError("CALL_EXPIRED", "Parent deadline has elapsed", 504)
        if self._call_count >= self.max_calls:
            # Never wait for a slot held by our own parent. Bounded rejection
            # allows its call tree to unwind instead of deadlocking the pool.
            raise ForestError("BACKPRESSURE", "No capacity for another child call", 429)
        self._validate(Draft202012Validator({}), request["payload"], "INPUT_INVALID")
        payload = json.loads(json.dumps(request["payload"], allow_nan=False))
        self._call_count += 1
        try:
            async with asyncio.timeout(timeout):
                return await self._invoke(
                    target_id,
                    name,
                    payload,
                    operation_id=request["operation_id"],
                    contract_major=major,
                    timeout=timeout,
                    parent_call_id=parent_call_id,
                    expected_policy_revision=parent[1].policy_revision,
                )
        except TimeoutError:
            raise ForestError(
                "DEADLINE_EXCEEDED", "Component child invocation exceeded its deadline", 504
            ) from None
        finally:
            self._call_count -= 1

    async def _invoke(
        self,
        component_id: str,
        service: str,
        payload: Any,
        *,
        operation_id: str,
        contract_major: int,
        timeout: float,
        parent_call_id: str | None = None,
        expected_policy_revision: int | None = None,
        admission_lane: str = "DATA",
    ) -> Any:
        authority = self._authority(allow_closing=parent_call_id is not None)
        parent = self._calls.get(parent_call_id) if parent_call_id else None
        if parent_call_id is not None and parent is None:
            raise ForestError("INVALID_CALL_CONTEXT", "Parent invocation is no longer live", 403)
        component = self.components.get(component_id)
        if component is None:
            raise ForestError("SERVICE_NOT_FOUND", "Service is not loaded", 404)
        if admission_lane == "CONTROL" and (
            component_id != "adapter.onebot"
            or service not in {"onebot.normalize", "onebot.prepare"}
            or parent is not None
            or component.grants
            or component.dependencies
            or component.snapshot.component.get("requires")
            or component.snapshot.component.get("capabilities", {}).get("requests")
        ):
            raise ForestError(
                "CONTROL_SCOPE_DENIED",
                "Control codec must be pure and have no dependencies or capabilities",
                403,
            )
        policy_revision = (
            component.policy_revision
            if expected_policy_revision is None
            else expected_policy_revision
        )
        if policy_revision != component.policy_revision:
            raise ForestError(
                "POLICY_REVOKED", "Invocation belongs to a different authorization revision", 403
            )
        if (
            not isinstance(operation_id, str)
            or not 1 <= len(operation_id) <= 128
            or not 0 < timeout <= 120
            or type(contract_major) is not int
            or contract_major < 1
        ):
            raise ForestError("INVALID_CALL", "Invalid operation identity or deadline", 422)
        self._validate(Draft202012Validator({}), payload, "INPUT_INVALID")
        input_digest = digest_json(
            {
                "component": component_id,
                "service": service,
                "contract_major": contract_major,
                "payload": payload,
                **({"admission_lane": "CONTROL"} if admission_lane == "CONTROL" else {}),
            }
        )

        async def existing_call():
            if parent is None:
                return await self.store.authorized_call(
                    authority, operation_id, policy_revision, admission_lane=admission_lane
                )
            return await self.store.authorized_child_call(parent[1], operation_id, policy_revision)

        previous = await existing_call()
        if previous is not None:
            if previous["input_digest"] != input_digest or previous["component_id"] != component_id:
                raise ForestError("IDEMPOTENCY_CONFLICT", "Call operation has different input", 409)
            if previous["status"] == "COMPLETED":
                return previous["result"]
            raise ForestError(
                "OPERATION_PENDING" if previous["status"] == "ADMITTED" else "CALL_FAILED",
                "Existing call must be reconciled, not executed again",
                409,
            )
        if service not in component.validators:
            raise ForestError("SERVICE_NOT_FOUND", "Service is not loaded", 404)
        input_validator, output_validator, major = component.validators[service]
        if contract_major != major:
            raise ForestError(
                "CONTRACT_INCOMPATIBLE", "Service contract major is not available", 422
            )
        self._validate(input_validator, payload, "INPUT_INVALID")
        if component.worker.state != "ACTIVE":
            raise ForestError(
                "SERVICE_RELOADING", "Worker transport is not accepting new calls", 409
            )
        call_id = str(uuid4())
        dependencies = tuple(component.dependencies)
        bindings = dict(component.service_bindings)
        try:
            admit = (
                self.store.admit_control
                if admission_lane == "CONTROL"
                else self.store.admit
                if parent is None
                else self.store.admit_child
            )
            grant = await admit(
                authority if parent is None else parent[1],
                component_id,
                call_id,
                operation_id,
                input_digest,
                timeout_seconds=timeout,
                expected_policy_revision=policy_revision,
                expected_generation_id=component.worker.identity.generation_id,
                expected_worker_incarnation=component.worker.identity.incarnation,
                expected_route_epoch=component.route_epoch,
                dependency_components=dependencies,
                **({"service_name": service} if admission_lane == "CONTROL" else {}),
            )
        except ForestError as exc:
            if exc.code in {"CALL_COMPLETED", "OPERATION_PENDING"}:
                saved = await existing_call()
                if (
                    saved
                    and saved["status"] == "COMPLETED"
                    and saved["input_digest"] == input_digest
                ):
                    return saved["result"]
            raise
        except BaseException:
            # Admission acknowledgement itself can be lost. The preallocated
            # call ID lets cleanup close only this call if its transaction committed.
            with suppress(Exception):
                await self.store.fail_call(authority, call_id, "ADMISSION_INTERRUPTED")
            raise
        # A stale cache must never deliver a new-generation grant to an old Worker.
        if grant.generation_id != component.worker.identity.generation_id or (
            grant.worker_incarnation != component.worker.identity.incarnation
        ):
            await self.store.fail_call(authority, call_id, "STALE_ROUTE")
            raise ForestError(
                "STALE_ROUTE", "Local route does not match the committed generation", 409
            )
        self._calls[call_id] = (component.worker.identity, grant, component.grants)
        self._call_bindings[call_id] = bindings
        try:
            timeout = min(timeout, (grant.deadline_at - datetime.now(timezone.utc)).total_seconds())
            if timeout <= 0:
                raise ForestError("CALL_EXPIRED", "Call deadline elapsed during admission", 504)
            result = await component.worker.invoke(
                service, payload, call_id=call_id, operation_id=operation_id, timeout=timeout
            )
            self._validate(output_validator, result, "OUTPUT_INVALID")
            await self.store.finish_call(grant, result)
            return result
        except BaseException as exc:
            code = exc.code if isinstance(exc, ForestError) else type(exc).__name__
            with suppress(Exception):
                await self.store.fail_call(self._authority(allow_closing=True), call_id, code)
            if not component.worker.alive:
                await self._retire(component, reason="worker_failed", revoke=True)
                if self.components.get(component_id) is component:
                    self.components.pop(component_id, None)
            raise
        finally:
            self._calls.pop(call_id, None)
            self._call_bindings.pop(call_id, None)
            async with self._calls_changed:
                self._calls_changed.notify_all()

    async def cancel(self, call_id: str) -> None:
        await self.store.cancel_call(self._authority(), call_id)
        current = self._calls.get(call_id)
        if current:
            for worker in list(self._workers):
                if worker.identity == current[0]:
                    await worker.cancel(call_id)
                    break

    async def close(self) -> None:
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._close(), name="kernel-resource-cleanup")
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        self._closing = True
        async with self._start_lock:
            pass
        async with self._deployment_lock:
            # Close caller components before their dependencies. The heartbeat
            # remains live while existing authorized trees finish their work.
            order, visited = [], set()

            def visit(key):
                if key in visited:
                    return
                visited.add(key)
                for dependency in self.components[key].dependencies:
                    if dependency in self.components:
                        visit(dependency)
                order.append(key)

            for key in sorted(self.components):
                visit(key)
            for key in reversed(order):
                component = self.components.get(key)
                if component is None:
                    continue
                try:
                    async with asyncio.timeout(
                        component.snapshot.component.get("lifecycle", {}).get("drain_timeout_s", 30)
                    ):
                        async with self._calls_changed:
                            await self._calls_changed.wait_for(
                                lambda: (
                                    not any(
                                        entry[1].component_id == key
                                        or key in entry[1].dependency_components
                                        for entry in self._calls.values()
                                    )
                                )
                            )
                    if component.worker.state == "ACTIVE":
                        await component.worker.quiesce()
                except Exception:
                    # Deadline/fence failure still ends with bounded retirement.
                    pass
                await self._retire(component, reason="shutdown", revoke=True)
            self.components.clear()
            await self._terminate_remaining()
            if self._heartbeat is not None:
                self._heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await self._heartbeat
            if self.lease is not None and self.failure is None:
                with suppress(Exception):
                    await self.store.release_host(self.lease)

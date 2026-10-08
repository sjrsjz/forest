"""Standard component configuration and manifest-frozen package UnitOfWork execution."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope, PackageOwnedWorkCallScope
from forest.packages import digest_json


def _step_operation(operation_id: str, plan_name: str, step_id: str) -> str:
    digest = hashlib.sha256(
        (operation_id + "\0" + plan_name + "\0" + step_id).encode("utf-8")
    ).hexdigest()
    return "uow-step:" + digest


@dataclass(frozen=True)
class ComponentConfigurationApplyScope:
    """Exact Generation and causal identity for one management configuration apply."""

    component_id: str
    generation_id: str
    call_id: str
    operation_id: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.component_id, str)
            or not 1 <= len(self.component_id.encode()) <= 128
            or not isinstance(self.generation_id, str)
            or not 1 <= len(self.generation_id.encode()) <= 256
            or not isinstance(self.call_id, str)
            or not 1 <= len(self.call_id.encode()) <= 256
            or not isinstance(self.operation_id, str)
            or not 1 <= len(self.operation_id.encode()) <= 256
        ):
            raise ValueError("Component configuration apply scope is invalid")


class PostgresComponentConfigurationApplyCoordinator:
    """Commit one Generation's desired configuration with Worker Host control intent."""

    def __init__(
        self, store, runtime, *, component_id: str, configuration, control_handle,
        pre_commit_validator=None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.component_id = component_id
        self.configuration = configuration
        self.control_handle = control_handle
        self.pre_commit_validator = pre_commit_validator

    async def execute(
        self,
        scope: ComponentConfigurationApplyScope,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
        authorization,
        authorization_guard,
        authorization_replay_guard=None,
    ) -> dict:
        if (
            type(scope) is not ComponentConfigurationApplyScope
            or scope.component_id != self.component_id
            or scope.operation_id != operation_id
            or authorization is None
            or authorization_guard is None
        ):
            raise ForestError(
                "COMPONENT_CONFIGURATION_APPLY_AUTHORITY_INVALID",
                "Configuration apply requires the target Generation and Web authority",
                403,
            )
        try:
            async with asyncio.timeout(30):
                return await self._execute_in_transaction(
                    scope,
                    operation_id=operation_id,
                    plan_id=plan_id,
                    expected_desired_revision=expected_desired_revision,
                    authorization=authorization,
                    authorization_guard=authorization_guard,
                    authorization_replay_guard=authorization_replay_guard,
                )
        except ForestError:
            raise
        except TimeoutError as exc:
            raise ForestError(
                "COMPONENT_CONFIGURATION_APPLY_TIMEOUT",
                "Configuration apply exceeded its deadline",
                504,
            ) from exc
        except psycopg.Error as exc:
            raise ForestError(
                "COMPONENT_CONFIGURATION_APPLY_FAILED",
                "Configuration apply transaction failed",
                503,
            ) from exc

    async def _execute_in_transaction(
        self,
        scope: ComponentConfigurationApplyScope,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
        authorization,
        authorization_guard,
        authorization_replay_guard,
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.runtime.verify_active_generation_in(
                conn,
                component_id=self.component_id,
                generation_id=scope.generation_id,
                admission_lane="CONTROL",
            )
            control = self.control_handle.resolve()
            for component_id in sorted({self.component_id, control.component_id}):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ("forest.uow.participant:" + component_id,),
                )
            await self.runtime.verify_generation_member_in(
                conn,
                component_id=control.component_id,
                generation_id=control.generation_id,
            )
            saved = await self.configuration.applied_by_operation_in(
                conn,
                operation_id=operation_id,
                plan_id=plan_id,
                expected_desired_revision=expected_desired_revision,
            )
            if saved is None:
                saved = await self.configuration.apply_in(
                    conn,
                    operation_id=operation_id,
                    plan_id=plan_id,
                    expected_desired_revision=expected_desired_revision,
                    authorization=authorization,
                    authorization_guard=authorization_guard,
                    pre_commit_validator=self.pre_commit_validator,
                )
            else:
                if authorization_replay_guard is None:
                    raise ForestError(
                        "COMPONENT_CONFIGURATION_REPLAY_AUTHORITY_UNAVAILABLE",
                        "Configuration replay requires current Web authority",
                        403,
                    )
                await authorization_replay_guard(
                    conn,
                    ticket=authorization,
                    component_id=self.component_id,
                    operation_id=operation_id,
                    action_key="configuration.apply",
                    input_digest=digest_json(
                        {
                            "plan_id": plan_id,
                            "expected_desired_revision": expected_desired_revision,
                        }
                    ),
                    plan_digest=digest_json({"plan_id": plan_id}),
                )
            configuration = {
                "component_id": self.component_id,
                "generation_id": scope.generation_id,
                "plan_id": plan_id,
                "revision": saved.revision,
                "payload_digest": saved.payload_digest,
            }
            rebuild = await control.invoke_in(
                conn,
                "component.configuration-rebuild.record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": self.component_id,
                        "generation_id": scope.generation_id,
                    },
                    "input": {
                        "target_component_id": self.component_id,
                        "target_generation_id": scope.generation_id,
                        "plan_id": plan_id,
                        "desired_revision": saved.revision,
                        "payload_digest": saved.payload_digest,
                        "expected_graph_revision": authorization.target_graph_revision,
                        "requested_by": authorization.web_subject_ref,
                    },
                    "prior": {"configuration": configuration},
                },
                operation_id=_step_operation(
                    operation_id, "component.configuration.apply", "control"
                ),
                causal_root_kind="management.configuration",
                causal_root_id=scope.operation_id,
                parent_call_id=scope.call_id,
                timeout_seconds=20,
            )
            return {"configuration": saved, "rebuild": rebuild}


class PostgresPackageUnitOfWorkCoordinator:
    """Execute an immutable participant sequence in one caller-owned transaction."""

    def __init__(
        self,
        store,
        runtime,
        *,
        component_id: str,
        participant_handles: Mapping[str, Any],
        work_notifier: Callable[[], None] | None = None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.component_id = component_id
        self.participant_handles = dict(participant_handles)
        self.work_notifier = work_notifier

    async def execute(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        if scope.component_id != self.component_id:
            raise ForestError(
                "PACKAGE_UOW_CALLER_INVALID",
                "UnitOfWork caller differs from the frozen package binding",
                409,
            )
        started = time.monotonic()
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self.store.pool.connection() as conn, conn.transaction():
                    if isinstance(scope, PackageOwnedWorkCallScope):
                        await self.runtime.verify_generation_member_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    else:
                        await self.runtime.verify_active_generation_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                            admission_lane="DATA",
                        )
                    participants = {
                        component_id: self._resolve(component_id)
                        for component_id in sorted(
                            {step["component_id"] for step in declaration["steps"]}
                        )
                    }
                    # Lock participant namespaces in one global order before the
                    # manifest's semantic step order starts taking business row locks.
                    # This keeps composition expressive without reintroducing
                    # cross-plan deadlocks.
                    for component_id in sorted(participants):
                        await conn.execute(
                            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            ("forest.uow.participant:" + component_id,),
                        )
                    for component_id, participant in participants.items():
                        await self.runtime.verify_generation_member_in(
                            conn,
                            component_id=component_id,
                            generation_id=participant.generation_id,
                        )
                    prior = {}
                    for step in declaration["steps"]:
                        remaining = timeout_seconds - (time.monotonic() - started)
                        if remaining <= 0:
                            raise TimeoutError
                        participant = participants[step["component_id"]]
                        projected = self._pointer(payload, step.get("input_path"))
                        generation_field = step.get("participant_generation_field")
                        if generation_field is not None:
                            if generation_field in projected:
                                raise ForestError(
                                    "PACKAGE_UOW_INPUT_INVALID",
                                    "UnitOfWork input cannot provide a participant Generation",
                                    422,
                                )
                            projected = {
                                **projected,
                                generation_field: participant.generation_id,
                            }
                        result = await participant.invoke_in(
                            conn,
                            step["participant"],
                            {
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": scope.component_id,
                                    "generation_id": scope.generation_id,
                                },
                                "input": projected,
                                "prior": prior,
                            },
                            operation_id=_step_operation(
                                scope.call_operation_id,
                                declaration["name"],
                                step["id"],
                            ),
                            causal_root_kind=scope.causal_root_kind,
                            causal_root_id=scope.causal_root_id,
                            parent_call_id=parent_call_id,
                            timeout_seconds=remaining,
                        )
                        prior[step["id"]] = result
                    result = prior[declaration["steps"][-1]["id"]]
                if self.work_notifier is not None:
                    self.work_notifier()
                return result
        except ForestError:
            raise
        except TimeoutError as exc:
            raise ForestError(
                "PACKAGE_UOW_TIMEOUT", "UnitOfWork plan exceeded its deadline", 504
            ) from exc
        except psycopg.Error as exc:
            raise ForestError("PACKAGE_UOW_FAILED", "UnitOfWork transaction failed", 503) from exc

    def _resolve(self, component_id: str):
        handle = self.participant_handles.get(component_id)
        if handle is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNDECLARED",
                "UnitOfWork plan requested an undeclared participant",
                503,
            )
        return handle.resolve()

    @staticmethod
    def _pointer(value: dict, pointer: str | None) -> Any:
        if pointer is None:
            return value
        current: Any = value
        for encoded in pointer[1:].split("/"):
            token = encoded.replace("~1", "/").replace("~0", "~")
            if not isinstance(current, dict) or token not in current:
                raise ForestError(
                    "PACKAGE_UOW_INPUT_INVALID",
                    "UnitOfWork step input projection is absent",
                    422,
                )
            current = current[token]
        if not isinstance(current, dict):
            raise ForestError(
                "PACKAGE_UOW_INPUT_INVALID",
                "UnitOfWork step input projection must be an object",
                422,
            )
        return current


def package_uow_coordinator_factory(
    *, store, runtime, work_notifier: Callable[[], None] | None = None
):
    def create(component_id: str, participant_handles: Mapping[str, Any]):
        return PostgresPackageUnitOfWorkCoordinator(
            store,
            runtime,
            component_id=component_id,
            participant_handles=participant_handles,
            work_notifier=work_notifier,
        )

    return create

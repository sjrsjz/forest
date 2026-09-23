"""Manifest-frozen cross-module UnitOfWork execution for package Workers."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Mapping
from typing import Any

import psycopg

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope, PackageOwnedWorkCallScope


def _step_operation(operation_id: str, plan_name: str, step_id: str) -> str:
    digest = hashlib.sha256(
        (operation_id + "\0" + plan_name + "\0" + step_id).encode("utf-8")
    ).hexdigest()
    return "uow-step:" + digest


class PostgresPackageUnitOfWorkCoordinator:
    """Execute an immutable participant sequence in one caller-owned transaction."""

    def __init__(
        self,
        store,
        runtime,
        *,
        component_id: str,
        participant_handles: Mapping[str, Any],
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.component_id = component_id
        self.participant_handles = dict(participant_handles)

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
                    return prior[declaration["steps"][-1]["id"]]
        except ForestError:
            raise
        except TimeoutError as exc:
            raise ForestError(
                "PACKAGE_UOW_TIMEOUT", "UnitOfWork plan exceeded its deadline", 504
            ) from exc
        except psycopg.Error as exc:
            raise ForestError(
                "PACKAGE_UOW_FAILED", "UnitOfWork transaction failed", 503
            ) from exc

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


def package_uow_coordinator_factory(*, store, runtime):
    def create(component_id: str, participant_handles: Mapping[str, Any]):
        return PostgresPackageUnitOfWorkCoordinator(
            store,
            runtime,
            component_id=component_id,
            participant_handles=participant_handles,
        )

    return create

"""Generation-fenced native Query and Command ports for control-scope packages."""

from __future__ import annotations

import asyncio

import psycopg

from forest.errors import ForestError


class _ControlPort:
    scope_kind: str

    def __init__(
        self, store, generations, *, component_id: str, ports: tuple[dict, ...],
        handlers: dict,
    ) -> None:
        if self.scope_kind not in {"deployment", "host"}:
            raise ValueError("Native control port scope is invalid")
        self.store = store
        self.scope_id = getattr(store, self.scope_kind + "_id")
        self.generations = generations
        self.component_id = component_id
        self.ports = {item["name"]: item["kind"] for item in ports}
        self.handlers = dict(handlers)

    def _bound(self, scope, declaration: dict) -> bool:
        return (
            scope.scope_kind == self.scope_kind
            and scope.scope_id == self.scope_id
            and scope.component_id == self.component_id
            and scope.bot_id is None
            and self.ports.get(declaration["name"]) == declaration["kind"]
        )

    def _handler(self, declaration: dict):
        handler = self.handlers.get((self.component_id, declaration["kind"]))
        if handler is None:
            raise ForestError(
                "KERNEL_MANAGEMENT_PORT_UNAVAILABLE",
                "Kernel management port has no bound handler", 503,
            )
        return handler


class ControlKernelQueryAuthority(_ControlPort):
    """Only read through the currently admitted control Generation."""

    async def invoke_query(
        self, scope, declaration: dict, payload: dict, *, parent_call_id: str,
        timeout_seconds: float,
    ) -> dict:
        if not self._bound(scope, declaration):
            raise ForestError(
                "KERNEL_MANAGEMENT_BINDING_INVALID",
                "Kernel Query differs from its control-scope binding", 409,
            )
        handler = self._handler(declaration)
        async with asyncio.timeout(timeout_seconds):
            await self.generations.verify_active_generation(
                component_id=self.component_id, generation_id=scope.generation_id,
                admission_lane="DATA",
            )
            result = await handler.query(payload)
            await self.generations.verify_active_generation(
                component_id=self.component_id, generation_id=scope.generation_id,
                admission_lane="DATA",
            )
        return result


class ControlKernelManagementAuthority(_ControlPort):
    """Run the caller's authorization preflight and native write in one transaction."""

    async def invoke_management_action(
        self, scope, declaration: dict, payload: dict, *, parent_call_id: str,
        timeout_seconds: float, before_write, management_authorization,
        result_validator,
    ) -> dict:
        if (
            not self._bound(scope, declaration)
            or scope.call_operation_id is None
            or management_authorization is None
            or not callable(before_write)
        ):
            raise ForestError(
                "KERNEL_MANAGEMENT_BINDING_INVALID",
                "Kernel Command differs from its control-scope binding", 409,
            )
        handler = self._handler(declaration)
        prepared = await handler.prepare(payload, operation_id=scope.call_operation_id)
        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                await self.generations.verify_active_generation_in(
                    conn, component_id=self.component_id,
                    generation_id=scope.generation_id, admission_lane="CONTROL",
                )
                await before_write(conn)
                result = await handler.execute_in(
                    conn, prepared, operation_id=scope.call_operation_id,
                )
                if not result_validator(result):
                    raise ForestError(
                        "MODULE_MANAGEMENT_OUTPUT_INVALID",
                        "Kernel Command result violates its frozen package schema", 502,
                    )
            after_commit = getattr(handler, "after_commit", None)
            if after_commit is not None:
                after_commit(result)
            return result
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                "KERNEL_MANAGEMENT_TRANSACTION_FAILED",
                "Kernel management Command transaction failed", 503,
            ) from exc

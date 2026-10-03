"""Generation-fenced, transaction-bound kernel Commands for official UI packages.

These ports never enter the package Worker. A handler may prepare an opaque value
outside the transaction, but its only durable effect must occur in execute_in.
"""

from __future__ import annotations

from dataclasses import asdict

import psycopg

from forest.errors import ForestError


class DeploymentKernelManagementAuthority:
    def __init__(
        self, store, generations, *, component_id: str, ports: tuple[dict, ...],
        handlers: dict,
    ) -> None:
        self.store = store
        self.generations = generations
        self.component_id = component_id
        self.ports = {item["name"]: item["kind"] for item in ports}
        self.handlers = dict(handlers)

    async def invoke_management_action(
        self, scope, declaration: dict, payload: dict, *, parent_call_id: str,
        timeout_seconds: float, before_write, management_authorization,
        result_validator,
    ) -> dict:
        if (
            scope.scope_kind != "deployment"
            or scope.scope_id != self.store.deployment_id
            or scope.component_id != self.component_id
            or scope.bot_id is not None
            or scope.call_operation_id is None
            or self.ports.get(declaration["name"]) != declaration["kind"]
            or management_authorization is None
            or not callable(before_write)
        ):
            raise ForestError(
                "KERNEL_MANAGEMENT_BINDING_INVALID",
                "Kernel Command differs from its official deployment binding", 409,
            )
        handler = self.handlers.get((self.component_id, declaration["kind"]))
        if handler is None:
            raise ForestError(
                "KERNEL_MANAGEMENT_PORT_UNAVAILABLE",
                "Kernel management Command has no bound handler", 503,
            )
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
                return result
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                "KERNEL_MANAGEMENT_TRANSACTION_FAILED",
                "Kernel management Command transaction failed", 503,
            ) from exc


class _PreparedSecretPut:
    __slots__ = ("secret_ref", "purpose", "plaintext", "expected_revision")

    def __init__(self, payload: dict) -> None:
        self.secret_ref = payload["secret_ref"]
        self.purpose = payload["purpose"]
        try:
            self.plaintext = payload["plaintext"].encode("utf-8")
        except UnicodeError as exc:
            raise ForestError(
                "SECRET_INPUT_INVALID", "Secret text is not UTF-8", 422,
            ) from exc
        self.expected_revision = payload["expected_revision"]


class SecretPutKernelCommand:
    """Keep plaintext and master-key use inside the trusted deployment process."""

    def __init__(self, secrets) -> None:
        self.secrets = secrets

    async def prepare(self, payload: dict, *, operation_id: str) -> _PreparedSecretPut:
        return _PreparedSecretPut(payload)

    async def execute_in(
        self, conn, prepared: _PreparedSecretPut, *, operation_id: str,
    ) -> dict:
        saved = await self.secrets.put_in(
            conn, operation_id=operation_id, secret_ref=prepared.secret_ref,
            purpose=prepared.purpose, plaintext=prepared.plaintext,
            expected_revision=prepared.expected_revision,
        )
        return asdict(saved)

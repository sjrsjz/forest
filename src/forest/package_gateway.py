"""Kernel implementation of the isolated package capability boundary."""

from __future__ import annotations

from typing import Any

from forest.errors import ForestError
from forest.package_capabilities import (
    PackageCallScope,
    PackageServiceError,
    PackageServiceRoute,
)
from forest.storage.component_state import ComponentStateCommands, ComponentStateQueries
from forest.storage.service_operations import KernelServiceOperationLedger


class KernelPackageServiceOperationLedger:
    """Expose the kernel service ledger through its storage-independent port."""

    def __init__(self, runtime, payloads) -> None:
        self._ledger = KernelServiceOperationLedger(runtime, payloads)

    async def begin(self, scope, route, input_value):
        return await self._ledger.begin(scope, route, input_value)

    async def succeed(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        result: Any,
    ) -> None:
        await self._ledger.succeed(scope, route, result)

    async def fail(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        error: PackageServiceError,
    ) -> None:
        await self._ledger.fail(scope, route, error)


class KernelPackageCapabilityGateway:
    """Kernel-owned capability ports; package adapters never receive repositories."""

    supported_capabilities = frozenset({"state.read", "state.write"})

    def __init__(self, runtime, payloads) -> None:
        self._bot_id = runtime.store.bot_id
        self._state_queries = ComponentStateQueries(runtime, payloads)
        self._state_commands = ComponentStateCommands(runtime, payloads)

    def _verify_scope(self, scope: PackageCallScope) -> None:
        if scope.bot_id != self._bot_id:
            raise ForestError(
                "PACKAGE_CAPABILITY_SCOPE_MISMATCH",
                "Package capability request belongs to another Bot",
                403,
            )

    async def read_state(self, scope: PackageCallScope, *, state_key: str) -> dict:
        self._verify_scope(scope)
        return await self._state_queries.get(
            component_id=scope.component_id,
            generation_id=scope.generation_id,
            state_key=state_key,
        )

    async def commit_state(
        self,
        scope: PackageCallScope,
        *,
        state_key: str,
        value: Any,
        expected_revision: int,
        operation_id: str,
    ) -> dict:
        self._verify_scope(scope)
        return await self._state_commands.commit(
            component_id=scope.component_id,
            generation_id=scope.generation_id,
            call_id=scope.call_id,
            state_key=state_key,
            value=value,
            expected_revision=expected_revision,
            operation_id=operation_id,
        )

"""Kernel implementation of the isolated package capability boundary."""

from __future__ import annotations

from typing import Any

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.storage.component_state import ComponentStateCommands, ComponentStateQueries


class KernelPackageCapabilityGateway:
    """Kernel-owned capability ports; package adapters never receive repositories."""

    supported_capabilities = frozenset({"state.read", "state.write"})

    def __init__(self, runtime, payloads) -> None:
        self._state_queries = ComponentStateQueries(runtime, payloads)
        self._state_commands = ComponentStateCommands(runtime, payloads)

    async def read_state(self, scope: PackageCallScope, *, state_key: str) -> dict:
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
        return await self._state_commands.commit(
            component_id=scope.component_id,
            generation_id=scope.generation_id,
            call_id=scope.call_id,
            state_key=state_key,
            value=value,
            expected_revision=expected_revision,
            operation_id=operation_id,
        )

    async def publish_event(self, scope: PackageCallScope, request: dict) -> dict:
        raise ForestError(
            "CAPABILITY_UNAVAILABLE",
            "The durable event authority is not connected to package Workers",
            503,
        )

    async def schedule_job(self, scope: PackageCallScope, request: dict) -> dict:
        raise ForestError(
            "CAPABILITY_UNAVAILABLE",
            "The durable job authority is not connected to package Workers",
            503,
        )

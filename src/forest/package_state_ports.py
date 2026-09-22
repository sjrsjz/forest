"""Kernel-owned state ports for isolated package Workers."""

from __future__ import annotations

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.storage.component_state import ComponentStateCommands, ComponentStateQueries


class KernelPackageStateQueries:
    """Read-only component state port exposed to isolated package Workers."""

    def __init__(self, runtime, payloads) -> None:
        self._bot_id = runtime.store.bot_id
        self._state_queries = ComponentStateQueries(runtime, payloads)

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


class KernelPackageStateCommands:
    """State mutation authority exposed to isolated package Workers."""

    def __init__(self, runtime, payloads) -> None:
        self._bot_id = runtime.store.bot_id
        self._state_commands = ComponentStateCommands(runtime, payloads)

    def _verify_scope(self, scope: PackageCallScope) -> None:
        if scope.bot_id != self._bot_id:
            raise ForestError(
                "PACKAGE_CAPABILITY_SCOPE_MISMATCH",
                "Package capability request belongs to another Bot",
                403,
            )

    async def commit_state(
        self,
        scope: PackageCallScope,
        *,
        state_key: str,
        value,
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

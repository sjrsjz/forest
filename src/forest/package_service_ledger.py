"""Kernel adapter for durable package service outcomes."""

from __future__ import annotations

from typing import Any

from forest.package_capabilities import (
    PackageCallScope,
    PackageServiceError,
    PackageServiceRoute,
)
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

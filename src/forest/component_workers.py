"""Lifecycle owner for installable component workers used by runtime modules."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from forest.errors import ForestError


class ComponentWorkerHost:
    def __init__(self, kernel, packages: dict) -> None:
        self.kernel = kernel
        self.packages = dict(packages)
        self.errors: dict[str, str] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._closed = False

    @property
    def failure(self):
        return None

    async def ensure(self, component_ids=None) -> None:
        ids = tuple(self.packages) if component_ids is None else tuple(component_ids)
        unknown = sorted(set(ids) - set(self.packages))
        if unknown:
            raise ForestError(
                "COMPONENT_PACKAGE_NOT_FOUND",
                "No admitted package exists for a required component",
                503,
                details={"component_ids": unknown},
            )
        results = await asyncio.gather(
            *(self.ensure_one(component_id) for component_id in ids),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def ensure_one(self, component_id: str) -> None:
        async with self._locks.setdefault(component_id, asyncio.Lock()):
            if self._closed:
                raise ForestError(
                    "COMPONENT_WORKER_HOST_CLOSED", "Component worker host is closed", 503
                )
            self.kernel._authority()
            component = self.kernel.components.get(component_id)
            if (
                component is not None
                and component.worker.alive
                and component.worker.state == "ACTIVE"
            ):
                return
            try:
                await self.kernel.install(
                    self.packages[component_id],
                    config={},
                    grants=frozenset(),
                    operation_id="infrastructure:" + str(uuid4()),
                )
                self.errors.pop(component_id, None)
            except Exception as exc:
                self.errors[component_id] = (
                    exc.code if isinstance(exc, ForestError) else "COMPONENT_START_FAILED"
                )
                raise

    async def close(self) -> None:
        self._closed = True


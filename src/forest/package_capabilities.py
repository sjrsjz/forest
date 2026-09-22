"""Storage-independent capability contracts for isolated package Workers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class PackageCallScope:
    bot_id: str
    component_id: str
    generation_id: str
    instance_id: str
    worker_incarnation: str
    call_id: str
    call_operation_id: str


class PackageCapabilityGateway(Protocol):
    supported_capabilities: frozenset[str]

    async def read_state(self, scope: PackageCallScope, *, state_key: str) -> dict: ...

    async def commit_state(
        self,
        scope: PackageCallScope,
        *,
        state_key: str,
        value: Any,
        expected_revision: int,
        operation_id: str,
    ) -> dict: ...

    async def publish_event(self, scope: PackageCallScope, request: dict) -> dict: ...

    async def schedule_job(self, scope: PackageCallScope, request: dict) -> dict: ...

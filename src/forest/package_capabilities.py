"""Storage-independent capability contracts for isolated package Workers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
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
    causal_root_kind: str
    causal_root_id: str


@dataclass(frozen=True)
class PackageJobCallScope(PackageCallScope):
    """Host-only extension available to effects nested under a durable Job call."""

    cancellation_requested: Callable[[], Awaitable[bool]]


@dataclass(frozen=True)
class PackageServiceRoute:
    service_id: str
    graph_id: str
    graph_revision: int
    admission_lane: str
    owned_work: bool
    parent_call_id: str | None
    timeout_seconds: float


@dataclass(frozen=True)
class PackageServiceError:
    code: str
    message: str
    status: int
    details: dict | None = None


@dataclass(frozen=True)
class PackageServiceAdmission:
    execute: bool
    result: Any = None
    error: PackageServiceError | None = None


class PackageStateQueries(Protocol):
    async def read_state(self, scope: PackageCallScope, *, state_key: str) -> dict: ...


class PackageStateCommands(Protocol):
    async def commit_state(
        self,
        scope: PackageCallScope,
        *,
        state_key: str,
        value: Any,
        expected_revision: int,
        operation_id: str,
    ) -> dict: ...


class PackageDomainQueries(Protocol):
    async def invoke_query(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any: ...


class PackageDomainCommands(Protocol):
    async def invoke_command(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any: ...


class PackageUnitOfWorkAuthority(Protocol):
    async def invoke_participant_in(
        self,
        conn,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any: ...


class PackageServiceOperationLedger(Protocol):
    async def begin(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        input_value: dict,
    ) -> PackageServiceAdmission: ...

    async def succeed(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        result: Any,
    ) -> None: ...

    async def fail(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        error: PackageServiceError,
    ) -> None: ...

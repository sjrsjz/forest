"""Stable Host lease ownership for the Host package RuntimeGraph."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from forest.errors import ForestError
from forest.storage.host_identity import HostIdentityAuthority, HostLease


class HostSupervisor:
    """Own one Host incarnation; the package controller owns its Graph."""

    def __init__(
        self, identity: HostIdentityAuthority, *, incarnation: str | None = None,
        lease_seconds: int = 30, renew_seconds: float = 10,
    ) -> None:
        if (
            type(lease_seconds) is not int or not 3 <= lease_seconds <= 300
            or not isinstance(renew_seconds, (int, float))
            or not 0.05 <= renew_seconds < lease_seconds / 2
        ):
            raise ValueError("Host lease renewal interval is invalid")
        self.identity = identity
        self.incarnation = incarnation or "incarnation/" + str(uuid4())
        self.lease_seconds = lease_seconds
        self.renew_seconds = renew_seconds
        self.lease: HostLease | None = None
        self.failure: str | None = None
        self._closing = False
        self._renewal_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.lease is not None or self._renewal_task is not None or self._closing:
            raise ForestError(
                "HOST_SUPERVISOR_ALREADY_STARTED", "Host Supervisor already started", 409,
            )
        await self.identity.initialize()
        self.lease = await self.identity.claim(
            self.incarnation, lease_seconds=self.lease_seconds,
        )
        self._renewal_task = asyncio.create_task(
            self._renew(), name="host-supervisor-lease/" + self.incarnation,
        )

    def authority_lease(self, *, allow_closing: bool = False) -> HostLease:
        if type(self.lease) is not HostLease or (self._closing and not allow_closing):
            raise ForestError(
                "HOST_RUNTIME_UNAVAILABLE", "Host Supervisor lease is unavailable", 503,
            )
        return self.lease

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return (self._renewal_task,) if self._renewal_task is not None else ()

    async def close(self) -> None:
        self._closing = True
        task, self._renewal_task = self._renewal_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        lease, self.lease = self.lease, None
        if lease is not None:
            try:
                await self.identity.release(lease)
            except ForestError as exc:
                if exc.code != "STALE_HOST_LEASE":
                    raise

    async def _renew(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(self.renew_seconds)
                if self._closing:
                    return
                self.lease = await self.identity.renew(
                    self.lease, lease_seconds=self.lease_seconds,
                )
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            self._closing = True
            raise
        except Exception:
            self.failure = "HOST_SUPERVISOR_RENEWAL_FAILED"
            self._closing = True
            raise

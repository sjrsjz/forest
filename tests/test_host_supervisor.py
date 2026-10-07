"""Host lease loss closes admission before a stale process can keep serving."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from forest.errors import ForestError
from forest.host_supervisor import HostSupervisor
from forest.storage.host_identity import HostLease


class _LosingIdentity:
    def __init__(self):
        self.renewals = 0
        self.releases = 0

    async def initialize(self):
        return None

    async def claim(self, incarnation, *, lease_seconds):
        return HostLease(
            "host/" + "a" * 32, incarnation, 1,
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds),
        )

    async def renew(self, lease, *, lease_seconds):
        self.renewals += 1
        if self.renewals == 2:
            raise ForestError("STALE_HOST_LEASE", "Host lease was taken over", 409)
        return HostLease(
            lease.host_id, lease.incarnation, lease.owner_epoch,
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds),
        )

    async def release(self, lease):
        self.releases += 1
        raise ForestError("STALE_HOST_LEASE", "Host lease was taken over", 409)


async def test_host_supervisor_stops_admission_when_renewal_loses_lease():
    identity = _LosingIdentity()
    supervisor = HostSupervisor(identity, lease_seconds=3, renew_seconds=0.05)
    await supervisor.start()
    assert supervisor.authority_lease().owner_epoch == 1
    with pytest.raises(ForestError) as lost:
        await asyncio.wait_for(supervisor.owned_tasks()[0], timeout=1)
    assert lost.value.code == "STALE_HOST_LEASE"
    assert supervisor.failure == "STALE_HOST_LEASE"
    with pytest.raises(ForestError) as closed:
        supervisor.authority_lease()
    assert closed.value.code == "HOST_RUNTIME_UNAVAILABLE"
    await supervisor.close()
    assert identity.renewals == 2
    assert identity.releases == 1

import asyncio
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.package_owned_work import PackageOwnedWorkSupervisor


def _supervisor(drain_policy, authority, failures):
    return PackageOwnedWorkSupervisor(
        bot_id="bot/test",
        component_id="component.test",
        declarations=({
            "name": "test.claim",
            "drain_policy": drain_policy,
            "max_concurrency": 1,
            "poll_interval_ms": 10,
            "lease_seconds": 30,
            "claim_timeout_s": 1,
        },),
        schemas={},
        authority=authority,
        operation_ledger=object(),
        binding=lambda: (
            SimpleNamespace(generation_id="generation/test"),
            SimpleNamespace(instance_id="instance/test", worker_incarnation="worker/test"),
            None,
        ),
        generation_member_verifier=lambda **_kwargs: None,
        active=lambda: True,
        failed=failures.append,
        call_started=lambda _call_id: None,
        call_finished=lambda _call_id: None,
    )


@pytest.mark.parametrize("drain_policy", ["inflight", "backlog"])
async def test_failed_claim_ends_replacement_drain(drain_policy):
    claim_started = asyncio.Event()
    release_claim = asyncio.Event()
    failures = []

    class FailingAuthority:
        async def invoke_owned_work(self, *_args, **_kwargs):
            claim_started.set()
            await release_claim.wait()
            raise ForestError("OWNED_WORK_CLAIM_FAILED", "Claim failed", 503)

    supervisor = _supervisor(drain_policy, FailingAuthority(), failures)
    supervisor.start()
    supervisor.open_admission()
    try:
        await asyncio.wait_for(claim_started.wait(), 1)
        supervisor.freeze_admission()
        release_claim.set()
        with pytest.raises(ForestError) as rejected:
            await asyncio.wait_for(supervisor.drain(), 1)
        assert rejected.value.code == "RUNTIME_OWNED_DRAIN_FAILED"
        assert rejected.value.details == {
            "component_id": "component.test",
            "failure": "OWNED_WORK_CLAIM_FAILED",
        }
        assert failures == ["OWNED_WORK_CLAIM_FAILED"]
    finally:
        await supervisor.close()


async def test_timed_out_backlog_probe_retries_without_declaring_queue_empty():
    first_timeout = asyncio.Event()
    second_started = asyncio.Event()
    release_second = asyncio.Event()
    failures = []

    class TimedOutAuthority:
        def __init__(self):
            self.calls = 0

        async def invoke_owned_work(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                first_timeout.set()
                raise ForestError(
                    "DOMAIN_AUTHORITY_TIMEOUT", "Claim transaction timed out", 504
                )
            second_started.set()
            await release_second.wait()
            return None

    authority = TimedOutAuthority()
    supervisor = _supervisor("backlog", authority, failures)
    supervisor.start()
    supervisor.open_admission()
    try:
        await asyncio.wait_for(first_timeout.wait(), 1)
        supervisor.freeze_admission()
        drain = asyncio.create_task(supervisor.drain())
        await asyncio.wait_for(second_started.wait(), 1)
        assert not drain.done()
        assert supervisor._drain_empty == set()
        release_second.set()
        await asyncio.wait_for(drain, 1)
        assert authority.calls >= 2
        assert failures == []
    finally:
        await supervisor.close()

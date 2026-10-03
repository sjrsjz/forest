import asyncio
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.package_capabilities import PackageServiceAdmission
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
        recovered=lambda: False,
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


async def test_inflight_freeze_during_poll_delay_does_not_start_new_claim():
    class CountingAuthority:
        calls = 0

        async def invoke_owned_work(self, *_args, **_kwargs):
            self.calls += 1
            return None

    authority = CountingAuthority()
    supervisor = _supervisor("inflight", authority, [])
    supervisor.open_admission()
    name = supervisor.declarations[0]["name"]
    supervisor._next_claim_at[name] = asyncio.get_running_loop().time() + 0.05
    pending = asyncio.create_task(supervisor._claim_when_due(supervisor.declarations[0]))
    await asyncio.sleep(0)
    assert supervisor._claim_locks[name].locked()
    supervisor.freeze_admission()
    assert await asyncio.wait_for(pending, 1) is None
    assert authority.calls == 0


async def test_renewal_covers_worker_and_result_commit_before_settlement():
    release_delivery = asyncio.Event()
    commit_started = asyncio.Event()
    release_commit = asyncio.Event()
    first_renew = asyncio.Event()
    second_renew = asyncio.Event()
    phases = []

    class Authority:
        async def invoke_owned_work(self, phase, _scope, _declaration, payload, **_kwargs):
            phases.append(phase)
            if phase == "renew":
                assert payload["claim_token"] == "claim/test"
                (first_renew if phases.count("renew") == 1 else second_renew).set()
                return {"protocol": "forest.owned-work-renewed/1", "work_id": "work/test"}
            assert phase == "settle"
            assert payload["outcome"] == {"status": "SUCCEEDED", "result": {"ok": True}}
            return {"protocol": "forest.owned-work-settled/1", "work_id": "work/test"}

    class Worker:
        async def deliver_job(self, *_args, **_kwargs):
            await release_delivery.wait()
            return {"ok": True}

    class Ledger:
        async def begin(self, *_args):
            return PackageServiceAdmission(execute=True)

        async def succeed(self, *_args):
            commit_started.set()
            await release_commit.wait()

    supervisor = _supervisor("inflight", Authority(), [])
    declaration = {
        **supervisor.declarations[0], "handler": "test.claim",
        "handler_timeout_s": 5, "settle_timeout_s": 1,
        "renew_function": "test_renew", "renew_interval_s": 0.01,
        "renew_timeout_s": 1,
    }
    supervisor.schemas = {"test.claim": (None, SimpleNamespace(is_valid=lambda _x: True))}
    supervisor.operation_ledger = Ledger()
    supervisor.binding = lambda: (
        SimpleNamespace(graph_id="graph/test", graph_revision=1,
                        generation_id="generation/test"),
        SimpleNamespace(instance_id="instance/test", worker_incarnation="worker/test"),
        Worker(),
    )
    claim = {
        "work_id": "work/test", "claim_token": "claim/test", "attempt": 1,
        "operation_id": "operation/test", "causal_root_kind": "test",
        "causal_root_id": "root/test", "payload": {},
    }
    running = asyncio.create_task(supervisor._execute(declaration, claim))
    await asyncio.wait_for(first_renew.wait(), 1)
    release_delivery.set()
    await asyncio.wait_for(commit_started.wait(), 1)
    await asyncio.wait_for(second_renew.wait(), 1)
    release_commit.set()
    await asyncio.wait_for(running, 1)
    assert phases[-1] == "settle"


async def test_lost_renewal_preserves_unsettled_effect():
    delivery_cancelled = asyncio.Event()
    phases = []

    class Authority:
        async def invoke_owned_work(self, phase, *_args, **_kwargs):
            phases.append(phase)
            raise ForestError("STALE_CLAIM", "Claim was lost", 409)

    class Worker:
        async def deliver_job(self, *_args, **_kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                delivery_cancelled.set()

    class Ledger:
        async def begin(self, *_args):
            return PackageServiceAdmission(execute=True)

        async def fail(self, *_args):
            phases.append("ledger-fail")

    supervisor = _supervisor("inflight", Authority(), [])
    declaration = {
        **supervisor.declarations[0], "handler": "test.claim",
        "handler_timeout_s": 5, "settle_timeout_s": 1,
        "renew_function": "test_renew", "renew_interval_s": 0.01,
        "renew_timeout_s": 1,
    }
    supervisor.schemas = {"test.claim": (None, SimpleNamespace(is_valid=lambda _x: True))}
    supervisor.operation_ledger = Ledger()
    supervisor.binding = lambda: (
        SimpleNamespace(graph_id="graph/test", graph_revision=1,
                        generation_id="generation/test"),
        SimpleNamespace(instance_id="instance/test", worker_incarnation="worker/test"),
        Worker(),
    )
    claim = {
        "work_id": "work/test", "claim_token": "claim/test", "attempt": 1,
        "operation_id": "operation/test", "causal_root_kind": "test",
        "causal_root_id": "root/test", "payload": {},
    }
    with pytest.raises(ForestError) as lost:
        await supervisor._execute(declaration, claim)
    assert lost.value.code == "OWNED_WORK_RENEWAL_LOST"
    assert delivery_cancelled.is_set()
    assert phases == ["renew"]

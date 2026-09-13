import asyncio

import pytest

from forest.errors import ForestError
from forest.sdk import (
    CallScope,
    Plugin,
    PluginContext,
    _current_call,
    declared_jobs,
    job,
)


async def unused_state(*_args):
    raise AssertionError("state RPC is not used")


async def test_job_handle_is_call_scoped_and_forwards_fixed_request():
    requests = []

    async def schedule(call_id, payload):
        requests.append((call_id, payload))
        return {"job_id": "job-a"}

    root = PluginContext({}, unused_state, job_rpc=schedule)
    with pytest.raises(ForestError) as missing:
        await root.jobs.schedule("work.a", {}, operation_id="job-op")
    assert missing.value.code == "CALL_CONTEXT_REQUIRED"
    scope = CallScope("call-a", asyncio.get_running_loop().time() + 5, "root-op")
    token = _current_call.set(scope)
    call = root._for_call(scope)
    try:
        result = await call.jobs.schedule(
            "work.a", {"value": 1}, operation_id="job-op", delay_seconds=2.5
        )
        assert result == {"job_id": "job-a"}
        assert requests == [("call-a", {
            "handler_id": "work.a", "payload": {"value": 1},
            "operation_id": "job-op", "delay_seconds": 2.5,
        })]
    finally:
        scope.active = False
        _current_call.reset(token)
    with pytest.raises(ForestError) as stale:
        await call.jobs.schedule("work.a", {}, operation_id="late")
    assert stale.value.code == "CALL_CONTEXT_REQUIRED"


def test_job_declaration_has_stable_identity_and_exact_signature():
    class Valid(Plugin):
        @job("work.a")
        async def execute(self, task, ctx):
            return task, ctx

    assert set(declared_jobs(Valid())) == {"work.a"}

    class Invalid(Plugin):
        @job("work.a")
        async def execute(self, payload):
            return payload

    with pytest.raises(ForestError) as error:
        declared_jobs(Invalid())
    assert error.value.code == "JOB_DECLARATION_INVALID"


@pytest.mark.parametrize("delay", [-1, True, 30 * 24 * 60 * 60 + 1])
async def test_job_delay_is_bounded(delay):
    root = PluginContext({}, unused_state, job_rpc=lambda *_: None)
    scope = CallScope("call-a", asyncio.get_running_loop().time() + 5, "root-op")
    token = _current_call.set(scope)
    try:
        with pytest.raises(ForestError) as error:
            await root._for_call(scope).jobs.schedule(
                "work.a", {}, operation_id="job-op", delay_seconds=delay
            )
        assert error.value.code == "INVALID_JOB_REQUEST"
    finally:
        scope.active = False
        _current_call.reset(token)

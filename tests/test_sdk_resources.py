from __future__ import annotations

import asyncio

import pytest

from forest.errors import ForestError
from forest.sdk import CallScope, OwnedResources, PluginContext, _current_call


class SyncResource:
    def __init__(self, log, name, *, fail=False):
        self.log, self.name, self.fail = log, name, fail

    def close(self):
        self.log.append(self.name)
        if self.fail:
            raise RuntimeError("private close details")


class AsyncResource(SyncResource):
    async def aclose(self):
        await asyncio.sleep(0)
        self.close()


class CancelledResource(SyncResource):
    async def aclose(self):
        self.log.append(self.name)
        raise asyncio.CancelledError


async def unused_rpc(*_args):
    raise AssertionError("resource tests must not use host RPC")


async def test_call_resources_release_and_finish_in_lifo_order():
    log = []
    root = PluginContext({}, unused_rpc)
    scope = CallScope("call-a", asyncio.get_running_loop().time() + 5, "operation-a")
    token = _current_call.set(scope)
    call = root._for_call(scope)
    try:
        first = call.resources.register(SyncResource(log, "first"))
        second = call.resources.register(AsyncResource(log, "second"))
        assert await call.resources.release(second) is None
        await root._resource_owner.finish_call(scope)
        assert log == ["second", "first"]
        with pytest.raises(ForestError) as error:
            await call.resources.release(first)
        assert error.value.code == "RESOURCE_NOT_OWNED"
    finally:
        await root._resource_owner.close_call(scope)
        scope.active = False
        _current_call.reset(token)


async def test_generation_and_call_handles_cannot_cross_scopes():
    log = []
    root = PluginContext({}, unused_rpc)
    generation = root.resources.register(SyncResource(log, "generation"))
    scope = CallScope("call-b", asyncio.get_running_loop().time() + 5, "operation-b")
    token = _current_call.set(scope)
    call = root._for_call(scope)
    try:
        call_resource = call.resources.register(SyncResource(log, "call"))
        with pytest.raises(ForestError) as error:
            root.resources.register(SyncResource(log, "forbidden"))
        assert error.value.code == "CALL_CONTEXT_REQUIRED"
        with pytest.raises(ForestError) as error:
            await root._resource_owner.release(generation, scope=scope)
        assert error.value.code == "RESOURCE_SCOPE_MISMATCH"
        await root._resource_owner.finish_call(scope)
        assert log == ["call"]
    finally:
        scope.active = False
        _current_call.reset(token)
    with pytest.raises(ForestError) as error:
        call.resources.register(SyncResource(log, "late"))
    assert error.value.code == "CALL_CONTEXT_REQUIRED"
    await root.resources.release(generation)
    assert log == ["call", "generation"]
    assert call_resource is not generation


async def test_limits_identity_and_cleanup_failures_are_bounded_and_do_not_skip_resources():
    log = []
    owner = OwnedResources(limit=2)
    first = owner.register(SyncResource(log, "first"), scope=None)
    owner.register(SyncResource(log, "failing", fail=True), scope=None, name="failing")
    with pytest.raises(ForestError) as error:
        owner.register(first, scope=None)
    assert error.value.code == "RESOURCE_ALREADY_OWNED"
    with pytest.raises(ForestError) as error:
        owner.register(SyncResource(log, "overflow"), scope=None)
    assert error.value.code == "OWNED_RESOURCE_LIMIT"
    with pytest.raises(ForestError) as error:
        await owner.close()
    assert error.value.code == "OWNED_RESOURCE_FAILED"
    assert error.value.details == {"failures": ["RuntimeError"]}
    assert log == ["failing", "first"]
    assert not owner.entries


async def test_cancellation_during_bulk_close_does_not_skip_remaining_resources():
    log = []
    owner = OwnedResources()
    owner.register(SyncResource(log, "first"), scope=None)
    owner.register(CancelledResource(log, "cancelled"), scope=None)
    with pytest.raises(asyncio.CancelledError):
        await owner.close()
    assert log == ["cancelled", "first"]
    assert list(owner.failures) == ["CancelledError"]
    assert not owner.entries


def test_resource_validation_does_not_invoke_equality_or_accept_missing_closer():
    class HostileEquality:
        def __eq__(self, other):
            raise AssertionError("resource identity must not call equality")

        def close(self):
            pass

    owner = OwnedResources()
    owner.register(HostileEquality(), scope=None)
    owner.register(HostileEquality(), scope=None)
    with pytest.raises(ForestError) as error:
        owner.register(object(), scope=None)
    assert error.value.code == "RESOURCE_CLOSER_REQUIRED"


@pytest.mark.parametrize("limit", [0, 257, True, 1.5])
def test_resource_limit_is_strict(limit):
    with pytest.raises(ValueError):
        OwnedResources(limit)

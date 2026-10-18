"""A shutdown error cannot outlive the resource ownership it describes."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from forest.bot_host import BotHost
from forest.control_driver_binding_gate import ControlDriverBindingGate
from forest.errors import ForestError
from forest.runtime_modules import (
    ModuleLifecycleState,
    OwnedWorkLifecycle,
    RuntimeArtifactIdentity,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
)


class Module:
    def __init__(self, *, fail_drain=False, fail_close=False):
        self.fail_drain = fail_drain
        self.fail_close = fail_close
        self.closed = False

    def freeze_owned_admission(self):
        pass

    async def drain_owned_work(self):
        if self.fail_drain:
            raise ForestError("TEST_DRAIN_FAILED", "Drain failed before cleanup", 503)

    def open_owned_admission(self):
        pass

    async def close(self):
        if self.fail_close:
            raise ForestError("TEST_RESOURCE_NOT_RELEASED", "Resource is still owned", 503)
        self.closed = True


async def host_with_module(module):
    descriptor = RuntimeModuleDescriptor(
        component_id="test.module", requires=(), factory=lambda *_: module,
        artifact=RuntimeArtifactIdentity(
            package_id="test.module", package_version="1.0.0",
            package_digest="sha256:" + "a" * 64, scope="bot",
            driver="forest.native/1", trust_level="official",
        ),
        owned_work_lifecycle=OwnedWorkLifecycle.FREEZE_DRAIN,
    )
    registry = RuntimeModuleRegistry(SimpleNamespace(), (descriptor,))
    registry.instances[descriptor.component_id] = module
    registry.states[descriptor.component_id] = ModuleLifecycleState.ACTIVE
    host = object.__new__(BotHost)
    host.settings = SimpleNamespace(bot_id="bot/shutdown")
    host.registry = registry
    host._driver_binding_gate = ControlDriverBindingGate()
    host._driver_binding_gate.attach(host)
    host._replacement_lock = asyncio.Lock()
    host._supervision_task = host._replacement_task = host._renewal_task = None
    host._lease = object()
    host._kernel = SimpleNamespace(runtime=SimpleNamespace(release_host=AsyncMock()))
    host.store = SimpleNamespace(close=AsyncMock())
    return host, registry


async def test_failed_drain_releases_binding_once_workers_are_closed():
    module = Module(fail_drain=True)
    host, registry = await host_with_module(module)
    with pytest.raises(ForestError, match="did not close cleanly") as caught:
        await host.close()
    assert caught.value.details["errors"] == ["TEST_DRAIN_FAILED"]
    assert module.closed and registry.resources_released
    assert host.registry is None and host._lease is None
    host._kernel.runtime.release_host.assert_awaited_once()
    host.store.close.assert_awaited_once()
    host._driver_binding_gate.require_empty()
    # A new incarnation may claim the same Bot identity after the reported error.
    replacement = SimpleNamespace(settings=host.settings, registry=registry)
    host._driver_binding_gate.attach(replacement)
    host._driver_binding_gate.detach(replacement)


async def test_resource_cleanup_failure_retains_owner_until_retry_succeeds():
    module = Module(fail_close=True)
    host, registry = await host_with_module(module)
    lease = host._lease
    with pytest.raises(ForestError):
        await host.close()
    assert not registry.resources_released
    assert host.registry is registry and host._lease is lease
    host._kernel.runtime.release_host.assert_not_awaited()
    host.store.close.assert_not_awaited()
    with pytest.raises(ForestError) as still_owned:
        host._driver_binding_gate.attach(SimpleNamespace(settings=host.settings, registry=registry))
    assert still_owned.value.code == "CONTROL_DRIVER_CONSUMER_CONFLICT"
    module.fail_close = False
    await host.close()
    assert registry.resources_released and module.closed
    host._driver_binding_gate.require_empty()


async def test_lease_release_failure_retains_binding_and_can_be_retried():
    host, registry = await host_with_module(Module())
    host._kernel.runtime.release_host.side_effect = ForestError("TEST_LEASE_RELEASE_FAILED", "Retry", 503)
    with pytest.raises(ForestError):
        await host.close()
    assert registry.resources_released and host.registry is None
    assert host._lease is not None
    host.store.close.assert_not_awaited()
    with pytest.raises(ForestError):
        host._driver_binding_gate.require_empty()
    host._kernel.runtime.release_host.side_effect = None
    await host.close()
    host._driver_binding_gate.require_empty()


async def test_graph_retirement_failure_retains_owner_even_after_worker_exit():
    module = Module()
    host, registry = await host_with_module(module)
    authority = SimpleNamespace(
        freeze_graph_admission=AsyncMock(), record_states=AsyncMock(),
        retire_graph=AsyncMock(side_effect=ForestError("TEST_RETIRE_FAILED", "Retry", 503)),
    )
    registry.runtime.authority_lease = lambda **_: host._lease
    registry.generation_authority = authority
    registry.graph_identity = SimpleNamespace(graph_id="graph/test")
    with pytest.raises(ForestError):
        await host.close()
    assert module.closed and not registry.resources_released
    assert host.registry is registry and host._lease is not None
    host._kernel.runtime.release_host.assert_not_awaited()
    with pytest.raises(ForestError):
        host._driver_binding_gate.require_empty()
    authority.retire_graph.side_effect = None
    await host.close()
    host._driver_binding_gate.require_empty()

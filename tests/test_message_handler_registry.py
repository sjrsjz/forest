"""Effective ordinary-message handler registry in real PostgreSQL Bot stores."""

import asyncio

import pytest
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.message_handler_registry import MessageHandlerRegistryStore

pytestmark = pytest.mark.postgres


def definition(handler_id="ordinary", *, handler=None, priority=0):
    return {
        "handler_id": handler_id,
        "handler": handler or handler_id,
        "priority": priority,
    }


async def register(store, lease, component, generation, definitions):
    await store.register_generation(
        lease,
        component,
        generation,
        "sha256:" + (generation[-1] * 64),
        "sha256:" + "0" * 64,
        "worker-" + generation,
        message_handler_definitions=definitions,
    )
    await store.mark_ready(lease, generation)


async def activate(store, lease, component, generation, epoch):
    operation = await store.begin_operation(
        lease,
        component,
        "activate-" + generation,
        "plan-" + generation,
        generation,
        epoch,
    )
    await store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    return await store.publish(lease, operation["operation_id"], operation["op_epoch"])


def source(tmp_path):
    root = tmp_path / "handler-registry-source"
    module = root / "src" / "handler_registry"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname="handler-registry"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import Pass, message_handler, plugin

@plugin(id="handler.registry", version="0.1.0")
class HandlerRegistry:
    @message_handler("ordinary", priority=25)
    async def ordinary(self, event, ctx):
        return Pass()
''',
        encoding="utf-8",
    )
    return root


async def test_lifecycle_publishes_and_upgrades_handler_snapshot(bot_database):
    store, _ = bot_database
    lease = await store.acquire_host("host", "handler-registry-host")
    registry = MessageHandlerRegistryStore(store)
    await register(store, lease, "greeter", "gen-1", [definition(priority=5)])
    first = await activate(store, lease, "greeter", "gen-1", 0)
    snapshot = await registry.snapshot()
    assert snapshot["revision"] == 1
    assert snapshot["handlers"][0]["generation_id"] == "gen-1"
    assert snapshot["handlers"][0]["route_epoch"] == first["route_epoch"] == 1

    await register(
        store,
        lease,
        "greeter",
        "gen-2",
        [definition(handler="ordinary_v2", priority=20)],
    )
    second = await activate(store, lease, "greeter", "gen-2", 1)
    snapshot = await registry.snapshot()
    assert snapshot["revision"] == 2
    assert snapshot["handlers"][0]["handler"] == "ordinary_v2"
    assert snapshot["handlers"][0]["priority"] == 20
    assert snapshot["handlers"][0]["route_epoch"] == second["route_epoch"] == 2


async def test_registry_orders_all_components_deterministically(bot_database):
    store, _ = bot_database
    lease = await store.acquire_host("host", "handler-order-host")
    await register(store, lease, "zeta", "gen-z", [definition("same", priority=10)])
    await register(store, lease, "alpha", "gen-a", [definition("later", priority=-5)])
    await register(store, lease, "alpha2", "gen-b", [definition("same", priority=10)])

    async def publish(component, generation):
        return await activate(store, lease, component, generation, 0)

    results = await asyncio.gather(
        publish("zeta", "gen-z"),
        publish("alpha", "gen-a"),
        publish("alpha2", "gen-b"),
    )
    assert all(row["route_epoch"] == 1 for row in results)
    snapshot = await MessageHandlerRegistryStore(store).snapshot()
    assert snapshot["revision"] == 3
    assert [
        (row["priority"], row["component_id"], row["handler_id"])
        for row in snapshot["handlers"]
    ] == [
        (10, "alpha2", "same"),
        (10, "zeta", "same"),
        (-5, "alpha", "later"),
    ]


async def test_removal_requires_retirement_and_retired_identity_cannot_revive(bot_database):
    store, _ = bot_database
    lease = await store.acquire_host("host", "handler-retirement-host")
    registry = MessageHandlerRegistryStore(store)
    await register(store, lease, "greeter", "gen-1", [definition()])
    await activate(store, lease, "greeter", "gen-1", 0)
    await register(store, lease, "greeter", "gen-2", [])
    operation = await store.begin_operation(
        lease, "greeter", "remove-handler", "remove-plan", "gen-2", 1
    )
    await store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    with pytest.raises(ForestError) as removal:
        await store.publish(lease, operation["operation_id"], operation["op_epoch"])
    assert removal.value.code == "MESSAGE_HANDLER_REMOVAL_REQUIRES_RETIREMENT"

    retired = await registry.retire(
        lease,
        component_id="greeter",
        handler_id="ordinary",
        operation_id="retire-ordinary",
        expected_management_revision=0,
    )
    assert retired["resulting_management_revision"] == 1
    await store.publish(lease, operation["operation_id"], operation["op_epoch"])
    assert (await registry.snapshot())["handlers"] == []

    await register(store, lease, "greeter", "gen-3", [definition()])
    third = await store.begin_operation(
        lease, "greeter", "revive-handler", "revive-plan", "gen-3", 2
    )
    await store.freeze(lease, third["operation_id"], third["op_epoch"])
    with pytest.raises(ForestError) as revival:
        await store.publish(lease, third["operation_id"], third["op_epoch"])
    assert revival.value.code == "MESSAGE_HANDLER_REACTIVATION_REQUIRES_MANAGEMENT"


async def test_disable_hides_and_reenable_reuses_handler_identity(bot_database):
    store, _ = bot_database
    lease = await store.acquire_host("host", "handler-disable-host")
    registry = MessageHandlerRegistryStore(store)
    await register(store, lease, "greeter", "gen-1", [definition()])
    await activate(store, lease, "greeter", "gen-1", 0)
    operation = await store.begin_operation(
        lease,
        "greeter",
        "disable-greeter",
        "disable-plan",
        None,
        1,
        operation_kind="DISABLE",
    )
    await store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    route = await store.publish_disable(
        lease, operation["operation_id"], operation["op_epoch"]
    )
    assert route["generation_id"] is None
    assert (await registry.snapshot())["handlers"] == []

    await register(
        store,
        lease,
        "greeter",
        "gen-2",
        [definition(handler="ordinary_v2")],
    )
    await activate(store, lease, "greeter", "gen-2", 2)
    assert (await registry.snapshot())["handlers"][0]["generation_id"] == "gen-2"


async def test_real_kernel_package_publishes_handler_registry(bot_database, tmp_path):
    store, _ = bot_database
    kernel = Kernel(store)
    try:
        await kernel.start()
        route = await kernel.install(PackageCatalog(tmp_path / "catalog").build(source(tmp_path)))
        snapshot = await MessageHandlerRegistryStore(store).snapshot()
        assert snapshot["revision"] == 1
        assert snapshot["handlers"][0]["component_id"] == "handler.registry"
        assert snapshot["handlers"][0]["handler_id"] == "ordinary"
        assert snapshot["handlers"][0]["priority"] == 25
        assert snapshot["handlers"][0]["generation_id"] == route["generation_id"]
        assert snapshot["handlers"][0]["route_epoch"] == route["route_epoch"]
    finally:
        await kernel.close()

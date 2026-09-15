"""Effective command registry integration against disposable PostgreSQL Bot stores."""

import asyncio

import pytest
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog, digest_json
from forest.storage.command_registry import CommandRegistryStore

pytestmark = pytest.mark.postgres


def command_source(tmp_path):
    source = tmp_path / "command-plugin"
    module = source / "src" / "registry_probe"
    module.mkdir(parents=True)
    (source / "pyproject.toml").write_text(
        '[project]\nname="registry-probe"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import command, plugin
@plugin(id="registry.probe", version="0.1.0")
class Probe:
    @command("/探针", "command.probe", id="probe", args="tail_text")
    async def probe(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    return source


def definition(
    command_id: str = "hello",
    name: str = "/hello",
    *,
    handler: str = "hello",
    permission: str = "command.hello",
) -> dict:
    return {
        "command_id": command_id,
        "name": name,
        "handler": handler,
        "permission": permission,
        "argument_mode": "none",
        "input_schema": "schemas/command.json",
        "schema_digest": digest_json({"type": "object"}),
        "priority": 0,
    }


async def register(instance, lease, component: str, generation: str, commands) -> None:
    await instance.register_generation(
        lease,
        component,
        generation,
        "sha256:" + (generation[-1] * 64),
        "sha256:" + "0" * 64,
        "worker-" + generation,
        command_definitions=commands,
    )
    await instance.mark_ready(lease, generation)


async def activate(instance, lease, component: str, generation: str, epoch: int) -> dict:
    operation = await instance.begin_operation(
        lease,
        component,
        "activate-" + generation,
        "plan-" + generation,
        generation,
        epoch,
    )
    await instance.freeze(lease, operation["operation_id"], operation["op_epoch"])
    return await instance.publish(lease, operation["operation_id"], operation["op_epoch"])


async def test_lifecycle_publication_binds_effective_command_snapshot(bot_database):
    bot_store, _ = bot_database
    lease = await bot_store.acquire_host("host", "command-registry-host")
    await register(bot_store, lease, "greeter", "gen-1", [definition()])
    route = await activate(bot_store, lease, "greeter", "gen-1", 0)
    snapshot = await CommandRegistryStore(bot_store).snapshot()
    assert snapshot["revision"] == 1
    assert len(snapshot["commands"]) == 1
    head = snapshot["commands"][0]
    assert head["command_id"] == "hello"
    assert head["generation_id"] == "gen-1"
    assert head["route_epoch"] == route["route_epoch"] == 1

    await register(
        bot_store,
        lease,
        "greeter",
        "gen-2",
        [definition(handler="hello_v2", permission="command.hello-v2")],
    )
    route = await activate(bot_store, lease, "greeter", "gen-2", 1)
    snapshot = await CommandRegistryStore(bot_store).snapshot()
    assert snapshot["revision"] == 2
    assert snapshot["commands"][0]["handler"] == "hello_v2"
    assert snapshot["commands"][0]["route_epoch"] == route["route_epoch"] == 2


async def test_real_package_worker_and_kernel_publish_command_registry(
    bot_database, tmp_path
):
    bot_store, _ = bot_database
    snapshot = PackageCatalog(tmp_path / "catalog").build(command_source(tmp_path))
    kernel = Kernel(bot_store)
    try:
        await kernel.start()
        route = await kernel.install(snapshot)
        registry = await CommandRegistryStore(bot_store).snapshot()
        assert route["generation_id"] == registry["commands"][0]["generation_id"]
        assert registry["commands"][0]["command_id"] == "probe"
        assert registry["commands"][0]["name"] == "/探针"
        assert registry["commands"][0]["argument_mode"] == "tail_text"
    finally:
        await kernel.close()


async def test_removal_requires_durable_retirement_and_never_reactivates(bot_database):
    bot_store, _ = bot_database
    lease = await bot_store.acquire_host("host", "command-retirement-host")
    registry = CommandRegistryStore(bot_store)
    await register(bot_store, lease, "greeter", "gen-1", [definition()])
    await activate(bot_store, lease, "greeter", "gen-1", 0)
    await register(bot_store, lease, "greeter", "gen-2", [])
    operation = await bot_store.begin_operation(
        lease, "greeter", "remove-command", "remove-plan", "gen-2", 1
    )
    await bot_store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    with pytest.raises(ForestError) as caught:
        await bot_store.publish(lease, operation["operation_id"], operation["op_epoch"])
    assert caught.value.code == "COMMAND_REMOVAL_REQUIRES_RETIREMENT"
    assert (await bot_store.get_route("greeter"))["generation_id"] == "gen-1"

    retired = await registry.retire(
        lease,
        component_id="greeter",
        command_id="hello",
        operation_id="retire-hello",
        expected_management_revision=0,
    )
    assert retired["resulting_management_revision"] == 1
    route = await bot_store.publish(lease, operation["operation_id"], operation["op_epoch"])
    assert route["generation_id"] == "gen-2"
    assert (await registry.snapshot())["commands"] == []

    await register(bot_store, lease, "greeter", "gen-3", [definition()])
    operation = await bot_store.begin_operation(
        lease, "greeter", "reactivate-command", "reactivate-plan", "gen-3", 2
    )
    await bot_store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    with pytest.raises(ForestError) as caught:
        await bot_store.publish(lease, operation["operation_id"], operation["op_epoch"])
    assert caught.value.code == "COMMAND_REACTIVATION_REQUIRES_MANAGEMENT"


async def test_disable_hides_command_and_same_identity_can_be_reenabled(bot_database):
    bot_store, _ = bot_database
    lease = await bot_store.acquire_host("host", "command-disable-host")
    registry = CommandRegistryStore(bot_store)
    await register(bot_store, lease, "greeter", "gen-1", [definition()])
    await activate(bot_store, lease, "greeter", "gen-1", 0)
    operation = await bot_store.begin_operation(
        lease,
        "greeter",
        "disable-greeter",
        "disable-plan",
        None,
        1,
        operation_kind="DISABLE",
    )
    await bot_store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    route = await bot_store.publish_disable(
        lease, operation["operation_id"], operation["op_epoch"]
    )
    assert route["generation_id"] is None
    assert (await registry.snapshot())["commands"] == []

    await register(bot_store, lease, "greeter", "gen-2", [definition(handler="hello_v2")])
    await activate(bot_store, lease, "greeter", "gen-2", 2)
    assert (await registry.snapshot())["commands"][0]["generation_id"] == "gen-2"


async def test_concurrent_same_name_publication_has_one_deterministic_winner(bot_database):
    bot_store, _ = bot_database
    lease = await bot_store.acquire_host("host", "command-race-host")
    await register(bot_store, lease, "alpha", "gen-a", [definition("alpha", "/same")])
    await register(bot_store, lease, "beta", "gen-b", [definition("beta", "/same")])
    registry = CommandRegistryStore(bot_store)

    async def publish(generation, component):
        async with bot_store.pool.connection() as conn, conn.transaction():
            return await registry.publish_generation_in(conn, generation, component, 1)

    results = await asyncio.gather(
        publish("gen-a", "alpha"), publish("gen-b", "beta"), return_exceptions=True
    )
    assert len([item for item in results if isinstance(item, int)]) == 1
    failures = [item for item in results if isinstance(item, ForestError)]
    assert len(failures) == 1 and failures[0].code == "COMMAND_NAME_CONFLICT"
    snapshot = await registry.snapshot()
    assert snapshot["revision"] == 1
    assert len(snapshot["commands"]) == 1

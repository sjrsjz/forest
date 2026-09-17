import os
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.packages import digest_json
from forest.routing_graph import RouteGraphStore
from forest.runtime_modules import (
    BotModuleRegistry,
    RuntimeArtifactIdentity,
    RuntimeModuleDescriptor,
)
from forest.storage.bot import BotStore

pytestmark = pytest.mark.postgres


class Module:
    def __init__(self, name, events, *, fail=False, close_failures=0):
        self.name = name
        self.events = events
        self.fail = fail
        self.close_failures = close_failures

    async def start(self):
        self.events.append(("start", self.name))
        if self.fail:
            raise RuntimeError(self.name)

    async def quiesce(self):
        self.events.append(("quiesce", self.name))

    async def close(self):
        self.events.append(("close", self.name))
        if self.close_failures:
            self.close_failures -= 1
            raise RuntimeError("close:" + self.name)


def descriptor(name, requires, events, *, fail=False, close_failures=0):
    return RuntimeModuleDescriptor(
        name,
        tuple(requires),
        lambda _platform, _modules: Module(
            name, events, fail=fail, close_failures=close_failures
        ),
        RuntimeArtifactIdentity(
            "tests.runtime",
            "1.0.0",
            "sha256:" + "0" * 64,
            "bot",
            "tests.in-process/1",
            "installed",
        ),
    )


class KernelAuthority:
    def __init__(self, lease):
        self.lease = lease

    def _authority(self, *, allow_closing=False):
        return self.lease


@pytest_asyncio.fixture
async def runtime_store():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private test PostgreSQL instance")
    name = "forest_runtime_graph_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-runtime")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("runtime-host", "runtime-incarnation-1", 300)
        yield store, lease
    finally:
        await store.close()
        assert name.startswith("forest_runtime_graph_") and len(name) == 53
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def test_registry_atomically_publishes_native_generations_and_dependency_edges(runtime_store):
    store, lease = runtime_store
    events = []
    platform = SimpleNamespace(
        bot=SimpleNamespace(store=store),
        kernel=KernelAuthority(lease),
    )
    registry = BotModuleRegistry(
        platform,
        (
            descriptor("consumer", ("provider",), events),
            descriptor("provider", (), events),
        ),
    )
    await registry.start()

    graph = await RouteGraphStore(store).snapshot()
    nodes = {node["node_id"]: node for node in graph["nodes"]}
    assert nodes["runtime:provider"]["generation_id"] == registry.graph_identity.generations[
        "provider"
    ]
    assert nodes["runtime:consumer"]["lifecycle_state"] == "ACTIVE"
    assert {
        (edge["source"], edge["target"], edge["status"])
        for edge in graph["edges"]
        if edge["kind"] == "requires_component"
    } == {("runtime:consumer", "runtime:provider", "RESOLVED")}
    assert graph["revisions"]["runtime"] == 1

    async with store.pool.connection() as conn:
        head = await (
            await conn.execute(
                "SELECT * FROM public.bot_runtime_graph_heads WHERE bot_id=%s", (store.bot_id,)
            )
        ).fetchone()
        publications = await (
            await conn.execute(
                "SELECT operation_kind,graph_id FROM public.bot_runtime_graph_publications "
                "WHERE bot_id=%s ORDER BY revision",
                (store.bot_id,),
            )
        ).fetchall()
    assert head["graph_id"] == registry.graph_identity.graph_id
    assert [tuple(row.values()) for row in publications] == [
        ("ACTIVATE", registry.graph_identity.graph_id)
    ]
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE public.bot_runtime_graph_heads SET revision=revision+1 "
                    "WHERE bot_id=%s",
                    (store.bot_id,),
                )

    assert await registry.close() == []
    retired = await RouteGraphStore(store).snapshot()
    assert all(node["kind"] != "runtime_module" for node in retired["nodes"])
    assert retired["revisions"]["runtime"] == 2
    provider_generation = registry.graph_identity.generations["provider"]
    async with store.pool.connection() as conn:
        sequence = await (
            await conn.execute(
                "SELECT max(sequence) AS sequence FROM public.bot_runtime_module_lifecycle_facts "
                "WHERE bot_id=%s AND generation_id=%s",
                (store.bot_id, provider_generation),
            )
        ).fetchone()
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO public.bot_runtime_module_lifecycle_facts("
                    "bot_id,generation_id,sequence,state,host_incarnation,host_owner_epoch) "
                    "VALUES (%s,%s,%s,'ACTIVE',%s,%s)",
                    (
                        store.bot_id,
                        provider_generation,
                        sequence["sequence"] + 1,
                        lease.incarnation,
                        lease.owner_epoch,
                    ),
                )


async def test_reactivation_under_same_host_allocates_fresh_generations(runtime_store):
    store, lease = runtime_store
    host = SimpleNamespace(
        bot=SimpleNamespace(store=store),
        kernel=KernelAuthority(lease),
    )
    first = BotModuleRegistry(host, (descriptor("owned", (), []),))
    await first.start()
    first_generation = first.graph_identity.generations["owned"]
    await first.close()

    second = BotModuleRegistry(host, (descriptor("owned", (), []),))
    await second.start()
    second_generation = second.graph_identity.generations["owned"]
    assert second.graph_identity.graph_id != first.graph_identity.graph_id
    assert second_generation != first_generation

    async with store.pool.connection() as conn:
        states = await (
            await conn.execute(
                "SELECT generation_id,state FROM public.bot_runtime_module_lifecycle_facts "
                "WHERE bot_id=%s ORDER BY created_at,sequence",
                (store.bot_id,),
            )
        ).fetchall()
    assert {row["generation_id"] for row in states} == {
        first_generation,
        second_generation,
    }
    assert [row["state"] for row in states if row["generation_id"] == first_generation] == [
        "PREPARED", "ACTIVE", "QUIESCING", "STOPPED"
    ]
    assert [row["state"] for row in states if row["generation_id"] == second_generation] == [
        "PREPARED", "ACTIVE"
    ]
    await second.close()


async def test_failed_start_never_publishes_partial_runtime_graph(runtime_store):
    store, lease = runtime_store
    events = []
    platform = SimpleNamespace(
        bot=SimpleNamespace(store=store),
        kernel=KernelAuthority(lease),
    )
    registry = BotModuleRegistry(
        platform,
        (
            descriptor("first", (), events),
            descriptor("second", ("first",), events, fail=True),
        ),
    )
    with pytest.raises(RuntimeError, match="second"):
        await registry.start()

    async with store.pool.connection() as conn:
        head = await (
            await conn.execute(
                "SELECT * FROM public.bot_runtime_graph_heads WHERE bot_id=%s", (store.bot_id,)
            )
        ).fetchone()
        publications = await (
            await conn.execute(
                "SELECT count(*) AS count FROM public.bot_runtime_graph_publications "
                "WHERE bot_id=%s",
                (store.bot_id,),
            )
        ).fetchone()
        facts = await (
            await conn.execute(
                "SELECT g.component_id,f.state,f.error_code FROM "
                "public.bot_runtime_module_lifecycle_facts f JOIN "
                "public.bot_runtime_module_generations g USING(bot_id,generation_id) "
                "WHERE f.bot_id=%s ORDER BY g.component_id,f.sequence",
                (store.bot_id,),
            )
        ).fetchall()
    assert head["graph_id"] is None and head["revision"] == 0
    assert publications["count"] == 0
    assert [(row["component_id"], row["state"]) for row in facts] == [
        ("first", "PREPARED"),
        ("first", "STOPPED"),
        ("second", "PREPARED"),
        ("second", "FAILED"),
        ("second", "STOPPED"),
    ]
    assert next(row for row in facts if row["state"] == "FAILED")["error_code"] == (
        "RUNTIME_MODULE_START_FAILED"
    )


async def test_failed_close_is_persisted_and_retry_reaches_stopped(runtime_store):
    store, lease = runtime_store
    platform = SimpleNamespace(
        bot=SimpleNamespace(store=store),
        kernel=KernelAuthority(lease),
    )
    registry = BotModuleRegistry(
        platform,
        (descriptor("owned", (), [], close_failures=1),),
    )
    await registry.start()
    errors = await registry.close()
    assert len(errors) == 1

    async def latest_state():
        async with store.pool.connection() as conn:
            return await (
                await conn.execute(
                    "SELECT f.state,f.error_code FROM public.bot_runtime_module_lifecycle_facts f "
                    "WHERE f.bot_id=%s AND f.generation_id=%s ORDER BY f.sequence DESC LIMIT 1",
                    (store.bot_id, registry.graph_identity.generations["owned"]),
                )
            ).fetchone()

    failed = await latest_state()
    assert failed["state"] == "FAILED"
    assert failed["error_code"] == "RUNTIME_MODULE_CLEANUP_FAILED"
    assert await registry.close() == []
    assert (await latest_state())["state"] == "STOPPED"


def test_runtime_descriptor_digest_covers_dependency_and_factory_identity():
    events = []
    left = BotModuleRegistry(SimpleNamespace(), (descriptor("module", (), events),))
    right = BotModuleRegistry(
        SimpleNamespace(),
        (descriptor("provider", (), events), descriptor("module", ("provider",), events)),
    )
    left_fact = left._descriptor_facts()[0]
    right_fact = next(item for item in right._descriptor_facts() if item["component_id"] == "module")
    assert left_fact["descriptor_digest"] == digest_json(
        {key: value for key, value in left_fact.items() if key != "descriptor_digest"}
    )
    assert left_fact["descriptor_digest"] != right_fact["descriptor_digest"]

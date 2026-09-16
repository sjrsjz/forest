"""Fleet recovery: network isolation, bounded supervision, and real owner fencing."""

import asyncio
import base64
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from psycopg import sql

import forest.management.fleet as fleet_module
import forest.storage.module_administration as module_administration_module
from forest.errors import ForestError
from forest.management.fleet import BotFleet, ManagedBot
from forest.security import SecretVault


class FakeStore:
    instances = []

    def __init__(self, dsn, bot_id):
        self.bot_id, self.dsn, self.closed = bot_id, dsn, False
        self.instances.append(self)

    async def open(self):
        pass

    async def migrate(self):
        pass

    async def close(self):
        self.closed = True


class FakeKernel:
    instances = []

    def __init__(self, store, **kwargs):
        self.store, self.failure, self.closed = store, None, False
        self._workers, self.components = set(), {}
        self.lease = SimpleNamespace(expires_at=datetime.now(timezone.utc) + timedelta(minutes=1))
        self.instances.append(self)

    def _authority(self):
        if self.failure or self.closed:
            raise ForestError("KERNEL_UNAVAILABLE", "Synthetic authority lost", 503)
        return self.lease

    async def start(self):
        pass

    async def close(self):
        self.closed = True


class FakeControl:
    def __init__(self, store):
        self.store = store

    async def trust(self, *args, **kwargs):
        pass

    async def desired(self, *args):
        return []


class FakeBindingRuntime:
    def __init__(self, bot, vault, packages, **kwargs):
        self.bot, self.vault, self.packages = bot, vault, packages
        self._tasks = []
        self.module_registry = SimpleNamespace(describe=lambda: [])
        self._modules = {
            "adapter.onebot": SimpleNamespace(
                shell_ingress=SimpleNamespace(snapshot=lambda: {})
            )
        }
        self.failure = None
        self.started = self.closed = False

    def module(self, component_id):
        return self._modules[component_id]

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True


def fake_catalog(**kwargs):
    return SimpleNamespace(vault=object(), **kwargs)


@pytest.fixture
def fake_runtime(monkeypatch):
    FakeStore.instances = []
    FakeKernel.instances = []
    monkeypatch.setattr(fleet_module, "BotStore", FakeStore)
    monkeypatch.setattr(fleet_module, "Kernel", FakeKernel)
    monkeypatch.setattr(fleet_module, "BotControl", FakeControl)
    monkeypatch.setattr(fleet_module, "BindingRuntime", FakeBindingRuntime)
    return SimpleNamespace(issuer_id="test", key_revision=1, public_key="synthetic")


async def wait_for(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def test_bad_bot_does_not_hold_global_attach_lock(tmp_path, fake_runtime):
    entered, release = asyncio.Event(), asyncio.Event()

    async def resolve(bot_id):
        if bot_id == "slow":
            entered.set()
            await release.wait()
        return "synthetic"

    fleet = BotFleet(fake_catalog(resolve_dsn=resolve), fake_runtime, tmp_path)
    slow = asyncio.create_task(fleet.attach("slow"))
    try:
        await entered.wait()
        fast = await asyncio.wait_for(fleet.attach("fast"), 1)
        assert fast.ready and not slow.done()
        assert await fleet.attach("fast") is fast
        assert len([store for store in FakeStore.instances if store.bot_id == "fast"]) == 1
    finally:
        release.set()
        await slow
        await fleet.close()


async def test_attach_concurrency_is_bounded_and_shutdown_joins_pending_owners(
    tmp_path, fake_runtime
):
    entered, maximum = 0, 0
    release = asyncio.Event()

    async def resolve(bot_id):
        nonlocal entered, maximum
        entered += 1
        maximum = max(maximum, entered)
        try:
            await release.wait()
            return "synthetic"
        finally:
            entered -= 1

    fleet = BotFleet(
        fake_catalog(resolve_dsn=resolve), fake_runtime, tmp_path, max_concurrent_attaches=2
    )
    tasks = [asyncio.create_task(fleet.attach(str(number))) for number in range(6)]
    await wait_for(lambda: entered == 2)
    assert maximum == 2
    await fleet.close()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert entered == 0 and not fleet.bots and not fleet._attaching


async def test_cleanup_failure_retains_old_owner_and_prevents_replacement(tmp_path, fake_runtime):
    async def resolve(bot_id):
        return "synthetic"

    fleet = BotFleet(fake_catalog(resolve_dsn=resolve), fake_runtime, tmp_path)
    old = await fleet.attach("owned")
    original_close = old.kernel.close
    old.kernel.failure = "HOST_LEASE_EXPIRED"
    marker = object()
    old.kernel._workers.add(marker)
    with pytest.raises(ForestError) as denied:
        await fleet.attach("owned")
    assert denied.value.code == "WORKER_CLEANUP_FAILED"
    assert fleet.bots["owned"] is old and len(FakeKernel.instances) == 1
    assert not fleet.status("owned")["attached"] and not old.store.closed
    old.kernel._workers.clear()
    old.kernel.close = original_close
    recovered = await fleet.attach("owned")
    assert recovered is not old and old.store.closed and recovered.ready
    await fleet.close()


async def test_attach_timeout_does_not_abandon_or_wait_indefinitely_for_cleanup(
    tmp_path, fake_runtime, monkeypatch
):
    cleanup_entered, release_cleanup = asyncio.Event(), asyncio.Event()

    async def resolve(bot_id):
        return "synthetic"

    async def blocked_open(self):
        await asyncio.Event().wait()

    async def blocked_close(self):
        cleanup_entered.set()
        await release_cleanup.wait()
        self.closed = True

    original_open, original_close = FakeStore.open, FakeKernel.close
    monkeypatch.setattr(FakeStore, "open", blocked_open)
    monkeypatch.setattr(FakeKernel, "close", blocked_close)
    fleet = BotFleet(fake_catalog(resolve_dsn=resolve), fake_runtime, tmp_path, attach_timeout=0.05)
    try:
        with pytest.raises(ForestError) as expired:
            await asyncio.wait_for(fleet.attach("timed-out"), 0.5)
        assert expired.value.code == "BOT_ATTACH_TIMEOUT"
        await cleanup_entered.wait()
        old = fleet.bots["timed-out"]
        assert old.cleanup_task and not old.cleanup_task.done() and not old.store.closed
        with pytest.raises(ForestError) as blocked:
            await asyncio.wait_for(fleet.attach("timed-out"), 0.5)
        assert blocked.value.code == "BOT_ATTACH_TIMEOUT"
        assert len(FakeKernel.instances) == 1
        monkeypatch.setattr(FakeStore, "open", original_open)
        monkeypatch.setattr(FakeKernel, "close", original_close)
        release_cleanup.set()
        new = await fleet.attach("timed-out")
        assert new is not old and old.store.closed
    finally:
        release_cleanup.set()
        await fleet.close()


async def test_background_attach_recovers_without_directory_head_of_line_blocking(
    tmp_path, fake_runtime
):
    failed = asyncio.Event()

    async def resolve(bot_id):
        if bot_id == "bad":
            failed.set()
            raise ForestError("DATABASE_UNAVAILABLE", "Synthetic bad database", 503)
        return "synthetic"

    async def ready():
        return [{"bot_id": name} for name in ["bad", "good", "later"]]

    fleet = BotFleet(
        fake_catalog(resolve_dsn=resolve, list_ready=ready),
        fake_runtime,
        tmp_path,
        max_concurrent_attaches=2,
        reconcile_interval=0.01,
        retry_base=1,
    )
    monitor = asyncio.create_task(fleet.run_forever())
    try:
        await failed.wait()
        await wait_for(lambda: all(fleet.status(key)["attached"] for key in ("good", "later")))
        assert fleet.errors["bad"] == "DATABASE_UNAVAILABLE"
        old = fleet.bots["good"]
        old.kernel.failure = "OWNER_REPLACED"
        await wait_for(
            lambda: fleet.bots.get("good") is not old and fleet.status("good")["attached"]
        )
        assert old.kernel.closed and old.store.closed
    finally:
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
        await fleet.close()


async def test_directory_generation_change_disposes_old_owner_before_reconnect(
    tmp_path, fake_runtime
):
    generation = 1

    async def resolve_storage(bot_id):
        return {"dsn": f"synthetic-{generation}", "storage_generation": generation}

    async def ready():
        return [{"bot_id": "moving", "storage_generation": generation}]

    fleet = BotFleet(
        fake_catalog(resolve_storage=resolve_storage, list_ready=ready),
        fake_runtime,
        tmp_path,
        reconcile_interval=0.01,
    )
    monitor = asyncio.create_task(fleet.run_forever())
    try:
        await wait_for(lambda: fleet.status("moving")["attached"])
        old = fleet.bots["moving"]
        assert old.directory_generation == 1
        generation = 2
        await wait_for(
            lambda: fleet.bots.get("moving") is not old
            and fleet.status("moving")["storage_generation"] == 2
        )
        assert old.kernel.closed and old.store.closed
        assert fleet.bots["moving"].store.dsn == "synthetic-2"
    finally:
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
        await fleet.close()


async def test_request_attach_resolves_directory_head_before_reusing_owner(
    tmp_path, fake_runtime
):
    generation = 1

    async def resolve_storage(bot_id):
        return {"dsn": f"synthetic-{generation}", "storage_generation": generation}

    fleet = BotFleet(
        fake_catalog(resolve_storage=resolve_storage), fake_runtime, tmp_path
    )
    try:
        old = await fleet.attach("moving")
        generation = 2
        current = await fleet.attach("moving")
        assert current is not old and current.directory_generation == 2
        assert old.kernel.closed and old.store.closed
        assert current.store.dsn == "synthetic-2"
    finally:
        await fleet.close()


async def test_failed_configuration_backs_off_but_new_revision_retries_immediately(
    tmp_path, fake_runtime
):
    fleet = BotFleet(fake_catalog(), fake_runtime, tmp_path, retry_base=10)
    store = FakeStore("synthetic", "test")
    kernel = FakeKernel(store)
    calls = []
    current = {
        "component_id": "counter",
        "revision": 1,
        "values": {"package_digest": "synthetic", "config": {}, "grants": []},
    }

    async def desired(*args):
        return [dict(current)]

    async def bind(*args):
        calls.append("bind")

    async def install(*args, **kwargs):
        calls.append("install")
        raise ForestError("PLUGIN_FAILED", "Synthetic deterministic failure")

    fleet.validate = lambda *args: None
    fleet.packages["synthetic"] = object()
    kernel.install = install
    bot = ManagedBot(
        store, kernel, SimpleNamespace(desired=desired, bind_activation=bind), ready=True
    )
    for _ in range(5):
        await fleet._reconcile_once(bot)
    assert calls == ["bind", "install"]
    assert bot.retries["counter"][:2] == ((1, 0, True), 1)
    current["revision"] = 2
    await fleet._reconcile_once(bot)
    assert calls == ["bind", "install", "bind", "install"]
    assert bot.errors["counter"] == "PLUGIN_FAILED"


async def test_component_activation_forwards_handler_takeover_opt_in(
    tmp_path, fake_runtime, monkeypatch
):
    fleet = BotFleet(fake_catalog(), fake_runtime, tmp_path)
    store = FakeStore("synthetic", "test")
    kernel = FakeKernel(store)
    calls = []

    async def bind(*args, **kwargs):
        calls.append(("config", kwargs))

    async def install(*args, **kwargs):
        calls.append(("install", kwargs))

    class FakeModuleAdministration:
        def __init__(self, *args, **kwargs):
            pass

        async def bind_activation(self, *args, **kwargs):
            calls.append(("state", kwargs))

    monkeypatch.setattr(
        module_administration_module, "ModuleAdministration", FakeModuleAdministration
    )
    fleet.validate = lambda *args: None
    fleet.packages["synthetic"] = object()
    kernel.install = install
    bot = ManagedBot(
        store, kernel, SimpleNamespace(bind_activation=bind), ready=True
    )
    values = {
        "package_digest": "synthetic",
        "config": {},
        "grants": [],
        "message_handler_takeover": True,
    }
    await fleet._activate(bot, "counter", 1, values, 0)
    assert calls[-1][0] == "install"
    assert calls[-1][1]["message_handler_takeover"] is True
    assert bot.applied == {"counter": 1}


async def test_disable_does_not_require_the_retired_package_to_remain_available(
    tmp_path, fake_runtime, monkeypatch
):
    fleet = BotFleet(fake_catalog(), fake_runtime, tmp_path)
    store = FakeStore("synthetic", "test")
    kernel = FakeKernel(store)
    calls = []
    revision = {
        "component_id": "retired",
        "revision": 4,
        "values": {"package_digest": "missing", "config": {}, "grants": []},
        "effective_revision": 3,
        "effective_values": {
            "package_digest": "also-missing", "config": {}, "grants": []
        },
        "desired_state_revision": 1,
        "effective_state_revision": 0,
        "desired_enabled": False,
        "effective_enabled": True,
    }

    async def desired(*args):
        return [dict(revision)]

    async def disable(component, *, operation_id):
        calls.append(("disable", component, operation_id))

    class FakeModuleAdministration:
        def __init__(self, *args, **kwargs):
            pass

        async def bind_activation(
            self, lease, component, state_revision, operation_id, *, enabled
        ):
            calls.append(("bind", component, state_revision, enabled, operation_id))

    monkeypatch.setattr(
        module_administration_module, "ModuleAdministration", FakeModuleAdministration
    )
    kernel.disable = disable
    bot = ManagedBot(store, kernel, SimpleNamespace(desired=desired), ready=True)
    await fleet._reconcile_once(bot)

    assert [call[0] for call in calls] == ["bind", "disable"]
    assert calls[0][1:4] == ("retired", 1, False)
    assert calls[0][4] == calls[1][2]
    assert bot.applied_states == {"retired": 1}
    assert not bot.errors


async def test_incomplete_enabled_dependency_graph_blocks_pending_disable(
    tmp_path, fake_runtime, monkeypatch
):
    fleet = BotFleet(fake_catalog(), fake_runtime, tmp_path)
    store = FakeStore("synthetic", "test")
    kernel = FakeKernel(store)
    calls = []
    provider_values = {"package_digest": "provider", "config": {}, "grants": []}
    broken_values = {"package_digest": "broken", "config": {}, "grants": []}
    revisions = [
        {
            "component_id": "provider",
            "revision": 2,
            "values": provider_values,
            "effective_revision": 1,
            "effective_values": provider_values,
            "desired_state_revision": 1,
            "effective_state_revision": 0,
            "desired_enabled": False,
            "effective_enabled": True,
        },
        {
            "component_id": "broken",
            "revision": 1,
            "values": broken_values,
            "desired_state_revision": 0,
            "effective_state_revision": 0,
            "desired_enabled": True,
            "effective_enabled": False,
        },
    ]

    async def desired(*args):
        return [dict(row) for row in revisions]

    def validate(component, values):
        if component == "broken":
            raise ForestError("PACKAGE_UNAVAILABLE", "Synthetic package is absent", 409)

    async def disable(*args, **kwargs):
        calls.append("disable")

    class FakeModuleAdministration:
        def __init__(self, *args, **kwargs):
            pass

        async def bind_activation(self, *args, **kwargs):
            calls.append("bind")

    monkeypatch.setattr(
        module_administration_module, "ModuleAdministration", FakeModuleAdministration
    )
    fleet.validate = validate
    fleet.packages["provider"] = object()
    kernel.disable = disable
    bot = ManagedBot(store, kernel, SimpleNamespace(desired=desired), ready=True)
    await fleet._reconcile_once(bot)

    assert calls == []
    assert bot.errors["provider"] == "DEPENDENCY_GRAPH_INCOMPLETE"
    assert bot.errors["broken"] == "PACKAGE_UNAVAILABLE"


@pytest_asyncio.fixture
async def actual_fleet(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a dedicated test PostgreSQL")
    name = "forest_fleet_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))

    async def resolve(bot_id):
        return uri

    key = Ed25519PrivateKey.generate()
    authority = SimpleNamespace(
        issuer_id=str(uuid4()),
        key_revision=1,
        public_key=base64.b64encode(key.public_key().public_bytes_raw()).decode("ascii"),
    )
    fleet = BotFleet(
        SimpleNamespace(resolve_dsn=resolve, vault=SecretVault(tmp_path / "vault")),
        authority,
        tmp_path,
    )
    try:
        async def no_registered_packages():
            return []
        # This fixture supplies only a private Bot database, without a deployment registry.
        fleet.registry.registered = no_registered_packages
        await fleet.initialize()
        yield fleet
    finally:
        await fleet.close()
        assert name.startswith("forest_fleet_") and len(name) == 45
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_real_expired_host_is_reaped_before_new_owner_and_state_survives(actual_fleet):
    fleet = actual_fleet
    old = await fleet.attach("real-bot")
    snapshot = next(iter(fleet.packages.values()))
    config = {"label": "recovery", "multiplier": 1}
    grants = frozenset({"state.read", "state.write"})
    await old.kernel.install(snapshot, config=config, grants=grants)
    old_worker = old.kernel.components["forest.counter"].worker
    before = await old.kernel.invoke(
        "forest.counter", "counter.increment", {"increment": 4}, operation_id="before"
    )
    old_lease = old.kernel.lease
    old.kernel._heartbeat.cancel()
    await asyncio.gather(old.kernel._heartbeat, return_exceptions=True)
    async with old.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE bot_host_lease SET expires_at=clock_timestamp()-interval '1 second'"
        )
    old.kernel.lease_seconds = 0.03
    old.kernel._heartbeat = asyncio.create_task(old.kernel._renew())
    await wait_for(lambda: old.kernel.failure is not None)
    assert not fleet.status("real-bot")["attached"]
    new = await fleet.attach("real-bot")
    assert new is not old and not old_worker.alive and not old.kernel._workers
    assert new.kernel.lease.owner_epoch > old_lease.owner_epoch
    assert new.kernel.lease.incarnation != old_lease.incarnation
    async with new.store.pool.connection() as conn:
        runtime_head = await (
            await conn.execute(
                "SELECT g.host_incarnation FROM public.bot_runtime_graph_heads h JOIN "
                "public.bot_runtime_graphs g USING(bot_id,graph_id) WHERE h.bot_id=%s",
                (new.store.bot_id,),
            )
        ).fetchone()
        old_states = await (
            await conn.execute(
                "SELECT l.state FROM public.bot_runtime_graphs g JOIN "
                "public.bot_runtime_module_generations m USING(bot_id,graph_id) JOIN LATERAL "
                "(SELECT f.state FROM public.bot_runtime_module_lifecycle_facts f "
                "WHERE f.bot_id=m.bot_id AND f.generation_id=m.generation_id "
                "ORDER BY f.sequence DESC LIMIT 1) l ON true WHERE g.bot_id=%s "
                "AND g.host_incarnation=%s",
                (new.store.bot_id, old_lease.incarnation),
            )
        ).fetchall()
    assert runtime_head["host_incarnation"] == new.kernel.lease.incarnation
    assert old_states and {row["state"] for row in old_states} == {"STOPPED"}
    with pytest.raises(ForestError):
        await new.store.renew_host(old_lease)
    await new.kernel.install(snapshot, config=config, grants=grants)
    after = await new.kernel.invoke(
        "forest.counter", "counter.increment", {"increment": 2}, operation_id="after"
    )
    assert before["value"] == 4 and after["value"] == 6


@pytest.mark.postgres
async def test_real_failed_worker_restarts_same_desired_revision(actual_fleet):
    fleet = actual_fleet
    bot = await fleet.attach("real-bot")
    bot.task.cancel()
    await asyncio.gather(bot.task, return_exceptions=True)
    snapshot = next(iter(fleet.packages.values()))
    values = {
        "package_digest": snapshot.digest,
        "config": {"label": "recover-worker", "multiplier": 1},
        "grants": ["state.read", "state.write"],
    }

    async def desired(*args):
        return [{"component_id": "forest.counter", "revision": 1, "values": values}]

    async def bind(*args):
        pass

    bot.control = SimpleNamespace(desired=desired, bind_activation=bind)
    await fleet._reconcile_once(bot)
    old_worker = bot.kernel.components["forest.counter"].worker
    await bot.kernel.invoke(
        "forest.counter", "counter.increment", {"increment": 1}, operation_id="before"
    )
    old_worker.process.kill()
    await asyncio.to_thread(old_worker.process.wait)
    await fleet._reconcile_once(bot)
    new_worker = bot.kernel.components["forest.counter"].worker
    assert old_worker is not new_worker and not old_worker.alive and new_worker.alive
    assert bot.applied["forest.counter"] == 1 and not bot.errors
    assert (
        await bot.kernel.invoke(
            "forest.counter", "counter.increment", {"increment": 2}, operation_id="after"
        )
    )["value"] == 3


@pytest.mark.postgres
async def test_web_policy_version_cannot_be_rebound_to_new_component_authority(actual_fleet):
    fleet = actual_fleet
    bot = await fleet.attach("real-bot")
    snapshot = next(iter(fleet.packages.values()))
    config = {"label": "policy", "multiplier": 1}
    grants = frozenset({"state.read", "state.write"})
    await bot.kernel.install(snapshot, config=config, grants=grants)
    old_policy = bot.kernel.components["forest.counter"].policy_revision
    async with bot.store.pool.connection() as conn:
        await conn.execute("UPDATE bot_runtime_state SET policy_revision=policy_revision+1")
    await bot.kernel.install(snapshot, config=config, grants=grants)
    with pytest.raises(ForestError) as stale:
        await bot.kernel.invoke(
            "forest.counter",
            "counter.increment",
            {"increment": 1},
            operation_id="web-ticket",
            expected_policy_revision=old_policy,
        )
    assert stale.value.code == "POLICY_REVOKED"
    assert await bot.store.read_call("web-ticket") is None
    result = await bot.kernel.invoke(
        "forest.counter",
        "counter.increment",
        {"increment": 1},
        operation_id="web-current",
        expected_policy_revision=old_policy + 1,
    )
    assert result["value"] == 1

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.clean_multi_bot_supervisor import CleanMultiBotSupervisor
from forest.errors import ForestError
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_catalog import DeploymentCatalogRepository


@pytest_asyncio.fixture
async def deployment_catalog():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_deployment_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = CleanDeploymentStore(uri, "deployment/test")
    await store.open()
    repository = DeploymentCatalogRepository(store)
    try:
        yield repository
    finally:
        await store.close()
        assert name.startswith("forest_deployment_") and len(name) == 50
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_deployment_catalog_keeps_bot_identity_intent_and_observation_separate(
    deployment_catalog,
):
    repository = deployment_catalog
    first_identity = await repository.initialize()
    second_identity = await repository.initialize()
    assert first_identity == second_identity

    alpha = await repository.register_ready(
        operation_id="provision/alpha",
        bot_id="bot/alpha",
        display_name="Alpha",
        storage_profile_ref="storage-profile/alpha/1",
    )
    replay = await repository.register_ready(
        operation_id="provision/alpha",
        bot_id="bot/alpha",
        display_name="Alpha",
        storage_profile_ref="storage-profile/alpha/1",
    )
    beta = await repository.register_ready(
        operation_id="provision/beta",
        bot_id="bot/beta",
        display_name="Beta",
        storage_profile_ref="storage-profile/beta/1",
    )
    assert replay == alpha
    assert alpha["desired_state"] == beta["desired_state"] == "OFFLINE"
    assert alpha["observed_state"] is None

    alpha = await repository.set_desired(
        operation_id="runtime/alpha/online",
        bot_id="bot/alpha",
        desired_state="ONLINE",
        expected_revision=alpha["intent_revision"],
    )
    assert alpha["desired_state"] == "ONLINE"
    with pytest.raises(ForestError) as caught:
        await repository.set_desired(
            operation_id="runtime/alpha/stale",
            bot_id="bot/alpha",
            desired_state="OFFLINE",
            expected_revision=1,
        )
    assert caught.value.code == "BOT_INTENT_REVISION_CONFLICT"

    lease = await repository.claim_supervisor("host/one", "incarnation/one")
    starting = await repository.observe(
        lease,
        bot_id="bot/alpha",
        observed_state="STARTING",
    )
    online = await repository.observe(
        lease,
        bot_id="bot/alpha",
        observed_state="ONLINE",
        graph_id="graph/alpha/1",
    )
    assert online["revision"] == starting["revision"] + 1
    assert (await repository.inspect("bot/alpha"))["graph_id"] == "graph/alpha/1"
    assert (await repository.inspect("bot/beta"))["observed_state"] is None

    with pytest.raises(ForestError) as caught:
        await repository.claim_supervisor("host/two", "incarnation/two")
    assert caught.value.code == "DEPLOYMENT_SUPERVISOR_BUSY"
    await repository.release_supervisor(lease)
    successor = await repository.claim_supervisor("host/two", "incarnation/two")
    assert successor.owner_epoch == lease.owner_epoch + 1
    with pytest.raises(ForestError) as caught:
        await repository.observe(
            lease,
            bot_id="bot/alpha",
            observed_state="OFFLINE",
        )
    assert caught.value.code == "STALE_DEPLOYMENT_SUPERVISOR"


@pytest.mark.postgres
async def test_deployment_failure_backoff_is_fenced_and_visible(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    bot = await repository.register_ready(
        operation_id="provision/failing",
        bot_id="bot/failing",
        display_name="Failing",
        storage_profile_ref="storage-profile/failing/1",
    )
    await repository.set_desired(
        operation_id="runtime/failing/online",
        bot_id=bot["bot_id"],
        desired_state="ONLINE",
        expected_revision=bot["intent_revision"],
    )
    lease = await repository.claim_supervisor("host/one", "incarnation/one")
    first = await repository.observe(
        lease,
        bot_id=bot["bot_id"],
        observed_state="FAILED",
        error_code="BOT_START_FAILED",
        retry_seconds=30,
    )
    second = await repository.observe(
        lease,
        bot_id=bot["bot_id"],
        observed_state="FAILED",
        error_code="BOT_START_FAILED",
        retry_seconds=60,
    )
    assert second["consecutive_failures"] == first["consecutive_failures"] + 1 == 2
    projected = await repository.inspect(bot["bot_id"])
    assert projected["desired_state"] == "ONLINE"
    assert projected["observed_state"] == "FAILED"
    assert projected["retry_not_before"] is not None


@pytest.mark.postgres
async def test_deployment_catalog_rejects_unknown_schema_objects(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    async with repository.store.pool.connection() as conn:
        await conn.execute("CREATE TABLE forest_deployment.unknown_object(id integer)")
    with pytest.raises(ForestError) as caught:
        await repository.initialize()
    assert caught.value.code == "DEPLOYMENT_SCHEMA_TAMPERED"


async def _eventually(assertion, *, timeout: float = 3) -> None:
    async def wait() -> None:
        while True:
            if await assertion():
                return
            await asyncio.sleep(0.01)

    async with asyncio.timeout(timeout):
        await wait()


@pytest.mark.postgres
async def test_multi_bot_supervisor_reconciles_independent_hosts(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    bots = []
    for name in ("alpha", "beta"):
        bot = await repository.register_ready(
            operation_id=f"provision/{name}",
            bot_id=f"bot/{name}",
            display_name=name.title(),
            storage_profile_ref=f"storage-profile/{name}/1",
        )
        bots.append(
            await repository.set_desired(
                operation_id=f"runtime/{name}/online",
                bot_id=bot["bot_id"],
                desired_state="ONLINE",
                expected_revision=bot["intent_revision"],
            )
        )

    instances = {}

    class FakeHost:
        def __init__(self, settings):
            self.settings = settings
            self.started = False
            self.closed = False
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/" + settings.bot_id)
            )
            instances[settings.bot_id] = self

        async def start(self):
            self.started = True

        async def close(self):
            self.closed = True

    supervisor = CleanMultiBotSupervisor(
        repository,
        settings_resolver=lambda bot: SimpleNamespace(bot_id=bot["bot_id"]),
        host_factory=FakeHost,
        host_id="host/test",
        incarnation="incarnation/test",
        poll_seconds=0.05,
    )
    await supervisor.start()
    try:
        async def both_online():
            rows = await repository.list_bots()
            return len(rows) == 2 and all(row["observed_state"] == "ONLINE" for row in rows)

        await _eventually(both_online)
        assert set(instances) == {"bot/alpha", "bot/beta"}

        alpha = await repository.inspect("bot/alpha")
        await repository.set_desired(
            operation_id="runtime/alpha/offline",
            bot_id="bot/alpha",
            desired_state="OFFLINE",
            expected_revision=alpha["intent_revision"],
        )
        supervisor.notify()

        async def alpha_offline():
            return (await repository.inspect("bot/alpha"))["observed_state"] == "OFFLINE"

        await _eventually(alpha_offline)
        assert instances["bot/alpha"].closed is True
        assert instances["bot/beta"].closed is False
        assert (await repository.inspect("bot/beta"))["observed_state"] == "ONLINE"
    finally:
        await supervisor.close()
    assert instances["bot/beta"].closed is True


@pytest.mark.postgres
async def test_multi_bot_supervisor_contains_one_bot_start_failure(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    for name in ("healthy", "broken"):
        bot = await repository.register_ready(
            operation_id=f"provision/{name}",
            bot_id=f"bot/{name}",
            display_name=name.title(),
            storage_profile_ref=f"storage-profile/{name}/1",
        )
        await repository.set_desired(
            operation_id=f"runtime/{name}/online",
            bot_id=bot["bot_id"],
            desired_state="ONLINE",
            expected_revision=bot["intent_revision"],
        )

    class FakeHost:
        def __init__(self, settings):
            self.settings = settings
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/" + settings.bot_id)
            )

        async def start(self):
            if self.settings.bot_id == "bot/broken":
                raise ForestError("BOT_TEST_START_FAILED", "Expected failure", 503)

        async def close(self):
            return None

    supervisor = CleanMultiBotSupervisor(
        repository,
        settings_resolver=lambda bot: SimpleNamespace(bot_id=bot["bot_id"]),
        host_factory=FakeHost,
        host_id="host/fault-test",
        incarnation="incarnation/fault-test",
        poll_seconds=0.05,
    )
    await supervisor.start()
    try:
        async def converged():
            rows = {row["bot_id"]: row for row in await repository.list_bots()}
            return (
                rows["bot/healthy"]["observed_state"] == "ONLINE"
                and rows["bot/broken"]["observed_state"] == "FAILED"
            )

        await _eventually(converged)
        assert supervisor.failure is None
        broken = await repository.inspect("bot/broken")
        assert broken["error_code"] == "BOT_TEST_START_FAILED"
        assert broken["retry_not_before"] is not None
    finally:
        await supervisor.close()

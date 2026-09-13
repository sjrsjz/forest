"""Real Python workers using a dedicated, real PostgreSQL Bot authority."""

import asyncio
import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.bot import BotStore
from forest.storage.postgres import PostgresStore

pytestmark = pytest.mark.postgres
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"
GRANTS = frozenset({"state.read", "state.write"})


@pytest_asyncio.fixture
async def bot_database():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a dedicated test PostgreSQL")
    name = "forest_kernel_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "test-bot")
    try:
        await store.open()
        await store.migrate()
        yield store, uri
    finally:
        await store.close()
        assert name.startswith("forest_kernel_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def package(tmp_path, version="0.1.0", *, code=None, min_increment=1):
    source = tmp_path / ("source-" + version)
    shutil.copytree(EXAMPLE, source)
    manifest = source / "module.toml"
    manifest.write_text(manifest.read_text().replace('version = "0.1.0"', f'version = "{version}"'))
    if code is not None:
        (source / "python" / "counter.py").write_text(code, encoding="utf-8")
    if min_increment != 1:
        path = source / "schemas" / "input.json"
        schema = json.loads(path.read_text())
        schema["properties"]["increment"]["minimum"] = min_increment
        path.write_text(json.dumps(schema))
    return PackageCatalog(tmp_path / "catalog").build(source)


async def wait_for(predicate, timeout=5):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def increment(kernel, operation, value=1):
    return await kernel.invoke("example.counter", "counter.increment", {"increment": value},
                               operation_id=operation)


async def test_worker_state_and_idempotent_result_survive_new_host(tmp_path, bot_database):
    store, _ = bot_database
    snapshot = package(tmp_path)
    first = Kernel(store)
    await first.start()
    try:
        route = await first.install(snapshot, grants=GRANTS, operation_id="install-counter")
        assert route["route_epoch"] == 1
        identity_before_retry = first.components["example.counter"].worker.identity
        assert await first.install(snapshot, grants=GRANTS, operation_id="install-counter") == route
        assert first.components["example.counter"].worker.identity == identity_before_retry
        result = await increment(first, "increment-1", 3)
        assert result == {"value": 3, "revision": 1}
        assert await increment(first, "increment-1", 3) == result
        with pytest.raises(ForestError) as conflict:
            await increment(first, "increment-1", 4)
        assert conflict.value.code == "IDEMPOTENCY_CONFLICT"
        identity = first.components["example.counter"].worker.identity
        old_epoch = first.lease.owner_epoch
    finally:
        await first.close()
    assert not first._workers and not first.cleanup_errors
    second = Kernel(store)
    await second.start()
    try:
        assert second.lease.owner_epoch > old_epoch
        route = await second.install(snapshot, grants=GRANTS)
        assert route["route_epoch"] > 1
        assert second.components["example.counter"].worker.identity != identity
        assert await increment(second, "increment-1", 3) == result
        assert await increment(second, "increment-2", 4) == {"value": 7, "revision": 2}
    finally:
        await second.close()


async def test_failed_candidate_keeps_old_route_and_process(tmp_path, bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        original = await kernel.install(package(tmp_path), grants=GRANTS)
        worker = kernel.components["example.counter"].worker
        invalid = package(tmp_path, "0.2.0", code="raise RuntimeError('candidate import failed')\n")
        with pytest.raises(ForestError):
            await kernel.install(invalid, grants=GRANTS)
        assert worker.alive
        assert kernel.components["example.counter"].worker is worker
        assert (await store.get_route("example.counter"))["generation_id"] == original["generation_id"]
        assert await increment(kernel, "old-still-works") == {"value": 1, "revision": 1}
        assert len(kernel._workers) == 1
    finally:
        await kernel.close()
    assert not kernel.cleanup_errors


async def test_owned_child_and_database_call_finish_are_drained_before_publish(tmp_path, bot_database):
    store, _ = bot_database
    code = '''import asyncio
from forest.sdk import Plugin, service
class Counter(Plugin):
    @service("counter.increment")
    async def increment(self, payload, ctx):
        async def child():
            await asyncio.sleep(0.4)
            await ctx.state.commit("counter", payload["increment"], expected_revision=0,
                operation_id=ctx.operations.stable("child-write:v1"))
        ctx.tasks.spawn_owned(child(), name="owned-state-write")
        return {"value": payload["increment"], "revision": 1}
'''
    kernel = Kernel(store)
    await kernel.start()
    task = None
    try:
        await kernel.install(package(tmp_path, code=code), grants=GRANTS)
        old = kernel.components["example.counter"].worker
        task = asyncio.create_task(increment(kernel, "with-child", 5))
        await wait_for(lambda: bool(kernel._calls))
        replacement = await kernel.install(package(tmp_path, "0.2.0"), grants=GRANTS)
        assert await task == {"value": 5, "revision": 1}
        assert replacement["route_epoch"] == 2
        assert not old.alive
        assert await increment(kernel, "after-drain", 1) == {"value": 6, "revision": 2}
    finally:
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await kernel.close()


async def test_current_maintenance_rejects_old_call_write(tmp_path, bot_database):
    store, _ = bot_database
    original = (EXAMPLE / "python" / "counter.py").read_text()
    slow = "import asyncio\n" + original.replace(
        'current = await ctx.state.get("counter")',
        'await asyncio.sleep(0.5)\n        current = await ctx.state.get("counter")',
    )
    kernel = Kernel(store)
    await kernel.start()
    task = None
    try:
        await kernel.install(package(tmp_path, code=slow), grants=GRANTS)
        task = asyncio.create_task(increment(kernel, "pause-in-flight"))
        await wait_for(lambda: bool(kernel._calls))
        state = await store.runtime()
        await store.set_maintenance(kernel.lease, state["maintenance_epoch"], False)
        with pytest.raises(ForestError):
            await task
        assert await store.snapshot_state(kernel.lease, "example.counter") == {}
        assert (await store.read_call("pause-in-flight"))["status"] == "FAILED"
        with pytest.raises(ForestError):
            await increment(kernel, "paused-new-call")
    finally:
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await kernel.close()


async def test_deadline_kills_blocked_worker_and_closes_authority(tmp_path, bot_database):
    store, _ = bot_database
    code = '''import time
from forest.sdk import Plugin, service
class Counter(Plugin):
    @service("counter.increment")
    async def increment(self, payload, ctx):
        while True:
            time.sleep(1)
'''
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(package(tmp_path, code=code), grants=GRANTS)
        worker = kernel.components["example.counter"].worker
        with pytest.raises((ForestError, TimeoutError)):
            await kernel.invoke("example.counter", "counter.increment", {"increment": 1},
                                operation_id="blocked-call", timeout=0.3)
        assert not worker.alive
        assert not kernel._calls and not kernel._workers
        assert not (await store.get_route("example.counter"))["admission"]
        assert (await store.read_call("blocked-call"))["status"] == "FAILED"
    finally:
        await kernel.close()


async def test_bot_authority_cannot_become_deployment_database(bot_database):
    store, dsn = bot_database
    deployment = PostgresStore(dsn)
    try:
        await deployment.open()
        with pytest.raises(ForestError) as rejected:
            await deployment.migrate()
        assert rejected.value.code == "DATABASE_SCOPE_MISMATCH"
        async with store.pool.connection() as conn:
            row = await (await conn.execute("SELECT to_regclass('public.web_accounts') AS table")).fetchone()
            assert row["table"] is None
    finally:
        await deployment.close()


async def test_policy_revocation_also_fences_receipt_reads_and_new_admission(tmp_path, bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(package(tmp_path), grants=GRANTS)
        await increment(kernel, "completed-before-revocation")
        policy = (await store.runtime())["policy_revision"]
        await store.revoke_policy(kernel.lease, policy)
        for operation in ("completed-before-revocation", "new-after-revocation"):
            with pytest.raises(ForestError):
                await increment(kernel, operation)
        assert await store.read_call("new-after-revocation") is None
    finally:
        await kernel.close()


async def test_taken_over_host_cannot_replay_result_before_heartbeat(tmp_path, bot_database, monkeypatch):
    from forest.storage.exposures import ExposureStore

    scans_allowed = asyncio.Event()
    scan = ExposureStore.scan

    async def delayed_scan(self, *args, **kwargs):
        await scans_allowed.wait()
        return await scan(self, *args, **kwargs)

    # Deliberately hold the separate content supervisor too: replay itself must
    # check ownership even when neither background observer has seen takeover.
    monkeypatch.setattr(ExposureStore, "scan", delayed_scan)
    store, _ = bot_database
    kernel = Kernel(store, lease_seconds=120)
    await kernel.start()
    try:
        await kernel.install(package(tmp_path), grants=GRANTS)
        await increment(kernel, "completed-before-takeover")
        worker = kernel.components["example.counter"].worker
        async with store.pool.connection() as conn:
            await conn.execute("UPDATE public.bot_host_lease SET expires_at=clock_timestamp()-interval '1 second'")
        successor = await store.acquire_host("successor", str(uuid4()))
        assert kernel.failure is None  # The local heartbeat has not observed takeover.
        with pytest.raises(ForestError):
            await increment(kernel, "completed-before-takeover")
        await kernel.close()
        assert not worker.alive
        assert (await store.renew_host(successor)).owner_epoch == successor.owner_epoch
    finally:
        await kernel.close()


async def test_cancelling_close_caller_does_not_abandon_resource_cleanup(tmp_path, bot_database, monkeypatch):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    await kernel.install(package(tmp_path), grants=GRANTS)
    worker = kernel.components["example.counter"].worker
    entered, release = asyncio.Event(), asyncio.Event()
    stop = worker.stop

    async def held_stop(*args, **kwargs):
        entered.set()
        await release.wait()
        await stop(*args, **kwargs)

    monkeypatch.setattr(worker, "stop", held_stop)
    first = asyncio.create_task(kernel.close())
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        second = asyncio.create_task(kernel.close())
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.sleep(0)
        assert not second.done()
        release.set()
        await asyncio.wait_for(second, 5)
        assert not worker.alive and not kernel._workers
        assert not kernel.cleanup_errors
    finally:
        release.set()
        await kernel.close()
        await asyncio.gather(first, *([second] if second else []), return_exceptions=True)


async def test_publish_transport_gap_does_not_poison_logical_call(tmp_path, bot_database, monkeypatch):
    from forest.drivers.python_worker import PythonWorker

    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    entered, release = asyncio.Event(), asyncio.Event()
    activate = PythonWorker.activate

    async def held_activate(worker, *args, **kwargs):
        entered.set()
        await release.wait()
        await activate(worker, *args, **kwargs)

    monkeypatch.setattr(PythonWorker, "activate", held_activate)
    installing = asyncio.create_task(kernel.install(package(tmp_path), grants=GRANTS))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        with pytest.raises(ForestError) as waiting:
            await increment(kernel, "arrived-during-publication")
        assert waiting.value.code == "SERVICE_RELOADING"
        assert await store.read_call("arrived-during-publication") is None
        release.set()
        await installing
        assert await increment(kernel, "arrived-during-publication") == {"value": 1, "revision": 1}
    finally:
        release.set()
        await asyncio.gather(installing, return_exceptions=True)
        await kernel.close()


async def test_route_changes_before_admit_without_creating_failed_call(tmp_path, bot_database, monkeypatch):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    entered, release = asyncio.Event(), asyncio.Event()
    admit = store.admit

    async def held_admit(*args, **kwargs):
        entered.set()
        await release.wait()
        return await admit(*args, **kwargs)

    task = None
    try:
        await kernel.install(package(tmp_path), grants=GRANTS)
        monkeypatch.setattr(store, "admit", held_admit)
        task = asyncio.create_task(increment(kernel, "old-cache"))
        await asyncio.wait_for(entered.wait(), 5)
        await kernel.install(package(tmp_path, "0.2.0"), grants=GRANTS)
        release.set()
        with pytest.raises(ForestError) as stale:
            await task
        assert stale.value.code == "STALE_ROUTE"
        assert await store.read_call("old-cache") is None
        monkeypatch.setattr(store, "admit", admit)
        assert await increment(kernel, "old-cache") == {"value": 1, "revision": 1}
    finally:
        release.set()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await kernel.close()


async def test_completed_request_replay_uses_original_schema(tmp_path, bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(package(tmp_path), grants=GRANTS)
        original = await increment(kernel, "old-schema")
        await kernel.install(package(tmp_path, "0.2.0", min_increment=5), grants=GRANTS)
        assert await increment(kernel, "old-schema") == original
        with pytest.raises(ForestError) as invalid:
            await increment(kernel, "new-schema")
        assert invalid.value.code == "INPUT_INVALID"
    finally:
        await kernel.close()


async def test_close_waits_for_pending_start_and_releases_acquired_lease(bot_database, monkeypatch):
    store, _ = bot_database
    kernel = Kernel(store)
    entered, release = asyncio.Event(), asyncio.Event()
    acquire = store.acquire_host

    async def held_acquire(*args, **kwargs):
        entered.set()
        await release.wait()
        return await acquire(*args, **kwargs)

    monkeypatch.setattr(store, "acquire_host", held_acquire)
    starting = asyncio.create_task(kernel.start())
    closing = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        closing = asyncio.create_task(kernel.close())
        await asyncio.sleep(0)
        assert not closing.done()
        release.set()
        with pytest.raises(ForestError) as stopped:
            await starting
        assert stopped.value.code == "KERNEL_CLOSING"
        await closing
        replacement = await acquire("new-host", str(uuid4()))
        assert replacement.owner_epoch > kernel.lease.owner_epoch
        assert kernel._heartbeat is None
    finally:
        release.set()
        await asyncio.gather(starting, *([closing] if closing else []), return_exceptions=True)
        await kernel.close()


async def test_concurrent_start_creates_exactly_one_lease_supervisor(bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    try:
        results = await asyncio.gather(kernel.start(), kernel.start(), return_exceptions=True)
        assert sum(isinstance(result, ForestError) for result in results) == 1
        assert kernel._heartbeat is not None and not kernel._heartbeat.done()
        assert (await store.renew_host(kernel.lease)).owner_epoch == 1
    finally:
        await kernel.close()
    assert kernel._heartbeat.done()


async def test_lost_publication_acknowledgement_never_resumes_old_generation(tmp_path, bot_database, monkeypatch):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        original = package(tmp_path)
        await kernel.install(original, grants=GRANTS)
        await increment(kernel, "before-lost-ack")
        old = kernel.components["example.counter"].worker
        publish = store.publish

        async def lose_ack(*args, **kwargs):
            await publish(*args, **kwargs)
            raise psycopg.OperationalError("simulated loss of committed publication acknowledgement")

        monkeypatch.setattr(store, "publish", lose_ack)
        with pytest.raises(psycopg.OperationalError):
            await kernel.install(package(tmp_path, "0.2.0"), grants=GRANTS,
                                 operation_id="lost-publish-ack")
        assert (await store.read_operation("lost-publish-ack"))["phase"] == "COMMITTED"
        assert not old.alive
        assert "example.counter" not in kernel.components and not kernel._workers
        route = await store.get_route("example.counter")
        assert route["generation_id"] != old.identity.generation_id and not route["admission"]
        monkeypatch.setattr(store, "publish", publish)
        restored = await kernel.install(original, grants=GRANTS)
        assert restored["route_epoch"] > route["route_epoch"]
        assert await increment(kernel, "after-lost-ack") == {"value": 2, "revision": 2}
    finally:
        await kernel.close()

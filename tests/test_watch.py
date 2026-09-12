from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog, source_fingerprint
from forest.watch import DevWatcher

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"
CODE = """from forest.sdk import Plugin, service
from helper import VALUE

class Counter(Plugin):
    @service("counter.increment")
    async def increment(self, payload, ctx):
        return {"value": VALUE, "revision": 1}
"""


def source_package(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(EXAMPLE, source)
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text().replace('["state.read", "state.write"]', "[]"), encoding="utf-8"
    )
    (source / "python" / "counter.py").write_text(CODE, encoding="utf-8")
    (source / "python" / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    return source


async def eventually(predicate, *, timeout=15):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.02)


async def value(kernel):
    return (
        await kernel.invoke(
            "example.counter", "counter.increment", {"increment": 1}, operation_id=uuid4().hex
        )
    )["value"]


@pytest_asyncio.fixture
async def watching(tmp_path, bot_database):  # noqa: F811
    store, _ = bot_database
    source = source_package(tmp_path)
    kernel = Kernel(store, lease_seconds=120)
    catalog = PackageCatalog(tmp_path / "catalog")
    events = []
    watcher = DevWatcher(
        kernel, catalog, source, debounce=0.05, poll_interval=0.025, on_event=events.append
    )
    try:
        await kernel.start()
        await watcher.start()
        await eventually(lambda: watcher.published_count == 1 or watcher.failure)
        assert watcher.failure is None, events
        yield watcher, kernel, source, events
    finally:
        await watcher.close()
        await kernel.close()


@pytest.mark.postgres
async def test_helper_edit_reloads_full_snapshot_and_keeps_only_one_worker(watching):
    watcher, kernel, source, events = watching
    old = kernel.components["example.counter"].worker
    (source / "python" / "helper.py").write_text("VALUE = 9\n", encoding="utf-8")
    await eventually(lambda: watcher.published_count == 2 or watcher.failure)
    assert watcher.failure is None, events
    assert await value(kernel) == 9
    assert not old.alive and len(kernel._workers) == 1
    head = await watcher.store.current(kernel._authority(), watcher.watch_id)
    assert head["good_request_id"] == head["desired_request_id"]
    assert head["last_good"]["source_digest"] == source_fingerprint(source)
    assert "+dev." in kernel.components["example.counter"].snapshot.manifest["version"]


@pytest.mark.postgres
@pytest.mark.parametrize(
    "broken",
    [
        "syntax error here!\n",
        "raise RuntimeError('import must fail')\n" + CODE,
        CODE
        + "\n    async def prepare(self, ctx, config, restored_state):\n        raise RuntimeError('prepare fails')\n",
        CODE + "\n    async def start(self):\n        raise RuntimeError('start fails')\n",
        CODE + "\n    async def health(self):\n        return {'healthy': False}\n",
    ],
)
async def test_bad_source_import_and_hooks_preserve_last_good(watching, broken):
    watcher, kernel, source, events = watching
    old = kernel.components["example.counter"].worker
    original = await kernel.store.get_route("example.counter")
    (source / "python" / "counter.py").write_text(broken, encoding="utf-8")
    await eventually(
        lambda: any(event["status"] == "failed" for event in events) or watcher.failure
    )
    assert watcher.failure is None, events
    assert await value(kernel) == 1
    assert kernel.components["example.counter"].worker is old and old.alive
    assert (await kernel.store.get_route("example.counter"))["generation_id"] == original[
        "generation_id"
    ]
    assert len(kernel._workers) == 1
    head = await watcher.store.current(kernel._authority(), watcher.watch_id)
    assert head["desired_request_id"] != head["good_request_id"]


@pytest.mark.postgres
async def test_invalid_new_edit_fences_already_ready_old_candidate_in_database(
    watching, monkeypatch
):
    watcher, kernel, source, events = watching
    old = kernel.components["example.counter"].worker
    original = await kernel.store.get_route("example.counter")
    entered, release = asyncio.Event(), asyncio.Event()
    publish = kernel.store.publish

    async def held_publish(*args, **kwargs):
        entered.set()
        await release.wait()
        return await publish(*args, **kwargs)

    monkeypatch.setattr(kernel.store, "publish", held_publish)
    (source / "python" / "helper.py").write_text("VALUE = 2\n", encoding="utf-8")
    try:
        await asyncio.wait_for(entered.wait(), 15)
        pending = await watcher.store.current(kernel._authority(), watcher.watch_id)
        (source / "python" / "helper.py").write_text("invalid new source!\n", encoding="utf-8")
        await eventually(
            lambda: (
                watcher._pending is not None
                and watcher._pending["request_id"] != pending["desired_request_id"]
            )
        )
        release.set()
        await eventually(
            lambda: (
                len([event for event in events if event["status"] == "failed"]) >= 2
                or watcher.failure
            )
        )
        assert watcher.failure is None, events
        assert any(event.get("error", {}).get("code") == "WATCH_SUPERSEDED" for event in events)
        assert kernel.components["example.counter"].worker is old and old.alive
        assert (await kernel.store.get_route("example.counter"))["generation_id"] == original[
            "generation_id"
        ]
        assert await value(kernel) == 1
        assert watcher.published_count == 1
    finally:
        release.set()


@pytest.mark.postgres
async def test_restart_restores_committed_artifact_before_invalid_workspace(watching):
    watcher, first, source, _ = watching
    good = await watcher.store.current(first._authority(), watcher.watch_id)
    await watcher.close()
    await first.close()
    (source / "python" / "helper.py").write_text("broken workspace!\n", encoding="utf-8")
    second = Kernel(first.store, lease_seconds=120)
    events = []
    restarted = DevWatcher(
        second, watcher.catalog, source, debounce=0.01, poll_interval=0.025, on_event=events.append
    )
    try:
        await second.start()
        await restarted.start()
        assert events[0]["status"] == "restored"
        assert (
            second.components["example.counter"].snapshot.digest
            == good["last_good"]["package_digest"]
        )
        assert await value(second) == 1
        await eventually(lambda: any(e["status"] == "failed" for e in events))
        assert await value(second) == 1
        head = await restarted.store.current(second._authority(), restarted.watch_id)
        assert head["good_request_id"] == good["good_request_id"]
        assert head["desired_request_id"] != good["desired_request_id"]
    finally:
        await restarted.close()
        await second.close()


@pytest.mark.postgres
async def test_restart_reconciles_pending_request_without_reusing_failed_lifecycle(watching):
    watcher, first, source, events = watching
    (source / "python" / "counter.py").write_text(
        "raise RuntimeError('bad import')\n" + CODE, encoding="utf-8"
    )
    await eventually(lambda: any(e["status"] == "failed" for e in events))
    failed_head = await watcher.store.current(first._authority(), watcher.watch_id)
    await watcher.close()
    await first.close()
    (source / "python" / "counter.py").write_text(CODE, encoding="utf-8")
    second = Kernel(first.store, lease_seconds=120)
    restarted = DevWatcher(second, watcher.catalog, source, debounce=0.01, poll_interval=0.025)
    try:
        await second.start()
        await restarted.start()
        await eventually(lambda: restarted.published_count == 1 or restarted.failure)
        assert restarted.failure is None
        head = await restarted.store.current(second._authority(), restarted.watch_id)
        assert head["good_request_id"] == head["desired_request_id"]
        assert head["desired_request_id"] != failed_head["desired_request_id"]
        assert await value(second) == 1
    finally:
        await restarted.close()
        await second.close()


@pytest.mark.postgres
async def test_watch_authorization_is_frozen_and_cannot_bypass_activation(watching):
    watcher, kernel, source, _ = watching
    with pytest.raises(ForestError, match="frozen") as expanded:
        await watcher.store.initialize(
            kernel._authority(), watcher.source_identity, {}, frozenset({"state.read"})
        )
    assert expanded.value.code == "WATCH_AUTHORIZATION_CONFLICT"
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text().replace("requests = []", 'requests = ["state.read"]'), encoding="utf-8"
    )
    await eventually(lambda: watcher.last_event.get("error", {}).get("code") == "CAPABILITY_DENIED")
    assert await value(kernel) == 1
    original = kernel.components["example.counter"].snapshot
    with pytest.raises(psycopg.errors.RaiseException) as bypass:
        await kernel.install(original, operation_id="unbound-publication")
    assert bypass.value.diag.message_primary == "WATCH_ACTIVATION_REQUIRED"
    assert await value(kernel) == 1


@pytest.mark.postgres
async def test_hundred_reload_cycles_return_owned_resources_to_baseline(watching):
    watcher, kernel, source, events = watching
    baseline_tasks = len(asyncio.all_tasks())
    retired = []
    for index in range(2, 102):
        retired.append(kernel.components["example.counter"].worker)
        (source / "python" / "helper.py").write_text(f"VALUE = {index}\n", encoding="utf-8")
        await eventually(lambda: watcher.published_count == index or watcher.failure)
        assert watcher.failure is None, events[-5:]
        assert len(kernel._workers) == 1 and not kernel._operations and not kernel._calls
    assert await value(kernel) == 101
    await asyncio.sleep(0.1)
    assert len(asyncio.all_tasks()) <= baseline_tasks + 1
    assert all(
        not worker.alive and not worker._pending and not worker._state_tasks for worker in retired
    )
    assert all(worker._reader_task is None or worker._reader_task.done() for worker in retired)
    assert len(watcher._io) <= 1  # The live observer may currently be capturing.
    await watcher.close()
    assert not watcher._io
    await kernel.close()
    assert not kernel._workers and not kernel.cleanup_errors


async def test_debounce_coalesces_and_continuous_edits_have_bounded_scheduling(tmp_path):
    # Isolate the scheduler clock from process startup and PostgreSQL latency.
    from types import SimpleNamespace

    watcher = DevWatcher(
        SimpleNamespace(store=None, failure=None),
        PackageCatalog(tmp_path / "catalog"),
        tmp_path / "source",
        debounce=0.3,
        max_delay=0.5,
        poll_interval=0.01,
    )
    started = asyncio.get_running_loop().time()
    watcher._first_dirty = watcher._last_dirty = started
    watcher._pending = {"sequence": 1}
    selected = asyncio.create_task(watcher._next())
    for index in range(2, 10):
        await asyncio.sleep(0.055)
        watcher._last_dirty = asyncio.get_running_loop().time()
        watcher._pending = {"sequence": index}
        watcher._changed.set()
    request = await asyncio.wait_for(selected, 0.2)
    elapsed = asyncio.get_running_loop().time() - started
    assert 0.45 <= elapsed < 0.75
    assert request["sequence"] == 9 and watcher._pending is None


@pytest.mark.postgres
@pytest.mark.parametrize("expires", ["host", "operation"])
async def test_final_guard_rechecks_time_after_waiting_for_watch_head(
    watching, monkeypatch, expires
):
    watcher, kernel, source, events = watching
    original = await kernel.store.get_route("example.counter")
    entered, release = asyncio.Event(), asyncio.Event()
    publish = kernel.store.publish

    async def held_publish(*args, **kwargs):
        entered.set()
        await release.wait()
        return await publish(*args, **kwargs)

    monkeypatch.setattr(kernel.store, "publish", held_publish)
    (source / "python" / "helper.py").write_text("VALUE = 2\n", encoding="utf-8")
    try:
        await asyncio.wait_for(entered.wait(), 15)
        # A separate transaction holds only the last business lock. Publication
        # must reach it while its authority is still valid, then expire waiting.
        async with kernel.store.pool.connection() as blocker, blocker.transaction():
            await blocker.execute(
                "SELECT 1 FROM public.bot_dev_watch_heads WHERE bot_id=%s AND watch_id=%s FOR UPDATE",
                (kernel.store.bot_id, watcher.watch_id),
            )
            async with kernel.store.pool.connection() as editor:
                if expires == "host":
                    await editor.execute(
                        "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '500 milliseconds' WHERE bot_id=%s",
                        (kernel.store.bot_id,),
                    )
                else:
                    await editor.execute(
                        "UPDATE public.bot_lifecycle_operations SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE bot_id=%s AND phase='QUIESCENT'",
                        (kernel.store.bot_id,),
                    )
            release.set()
            async with kernel.store.pool.connection() as observer:
                async with asyncio.timeout(0.4):
                    while True:
                        row = await (
                            await observer.execute(
                                "SELECT count(*) AS n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE %s",
                                ("UPDATE public.bot_lifecycle_operations SET phase='COMMITTED'%",),
                            )
                        ).fetchone()
                        if row["n"]:
                            break
                        await asyncio.sleep(0.01)
            await asyncio.sleep(0.55)
        await eventually(
            lambda: (
                watcher.failure
                or any(e.get("error", {}).get("code") == "WATCH_AUTHORITY_EXPIRED" for e in events)
            )
        )
        route = await kernel.store.get_route("example.counter")
        assert route["generation_id"] == original["generation_id"]
        async with kernel.store.pool.connection() as reader:
            head = await (
                await reader.execute(
                    "SELECT * FROM public.bot_dev_watch_heads WHERE bot_id=%s AND watch_id=%s",
                    (kernel.store.bot_id, watcher.watch_id),
                )
            ).fetchone()
            assert head["good_request_id"] != head["desired_request_id"]
        if expires == "operation":
            assert watcher.failure is None and await value(kernel) == 1
    finally:
        release.set()


@pytest.mark.postgres
async def test_close_owns_filesystem_work_after_caller_cancellation(watching, monkeypatch):
    import threading

    import forest.watch as watch_module

    watcher, kernel, _, _ = watching
    entered, release = threading.Event(), threading.Event()
    fingerprint = watch_module.source_fingerprint

    def held_fingerprint(*args):
        entered.set()
        release.wait(10)
        return fingerprint(*args)

    monkeypatch.setattr(watch_module, "source_fingerprint", held_fingerprint)
    first = second = None
    try:
        await eventually(entered.is_set)
        first = asyncio.create_task(watcher.close())
        await asyncio.sleep(0.02)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(watcher.close())
        await asyncio.sleep(0.02)
        assert not second.done() and watcher._io
        release.set()
        await asyncio.wait_for(second, 5)
        assert not watcher._io and watcher.task.done()
        assert await value(kernel) == 1
    finally:
        release.set()
        await asyncio.gather(*[task for task in (first, second) if task], return_exceptions=True)


@pytest.mark.postgres
async def test_cli_oneshot_and_watch_argument_contract(tmp_path, bot_database, monkeypatch, capsys):  # noqa: F811
    from argparse import Namespace

    from forest.cli import main, run_plugin_command

    store, dsn = bot_database
    source = source_package(tmp_path)
    monkeypatch.setenv("FOREST_BOT_DSN", dsn)
    args = Namespace(
        command="dev",
        bot=store.bot_id,
        package=source,
        watch=None,
        service="counter.increment",
        input_json='{"increment":1}',
        config_json="{}",
        operation_id="cli-once",
        grant=[],
        cache_dir=tmp_path / "catalog",
    )
    await run_plugin_command(args)
    assert json.loads(capsys.readouterr().out)["result"]["value"] == 1
    monkeypatch.setattr(
        sys,
        "argv",
        ["forest", "dev", "--bot", "dev", "--watch", str(source), "--service", "counter.increment"],
    )
    with pytest.raises(SystemExit) as invalid:
        main()
    assert invalid.value.code == 2


async def test_cli_lock_and_archive_roundtrip(tmp_path, capsys):
    from argparse import Namespace

    from forest.cli import run_plugin_command

    source = source_package(tmp_path)
    await run_plugin_command(
        Namespace(command="plugin", plugin_command="lock", source=source, requirements=[])
    )
    locked = json.loads(capsys.readouterr().out)
    assert locked["status"] == "locked"
    lock = json.loads((source / "dependencies.lock.json").read_text())
    assert lock["requirements"] == [] and lock["wheels"] == []
    previous = (source / "dependencies.lock.json").read_bytes()
    with pytest.raises(ForestError):
        await run_plugin_command(
            Namespace(
                command="plugin",
                plugin_command="lock",
                source=source,
                requirements=["missing==1.0"],
            )
        )
    assert (source / "dependencies.lock.json").read_bytes() == previous
    catalog = PackageCatalog(tmp_path / "first")
    snapshot = catalog.build(source)
    archive = tmp_path / "package.zip"
    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="export",
            digest=snapshot.digest,
            output=archive,
            cache_dir=catalog.root,
        )
    )
    assert json.loads(capsys.readouterr().out)["status"] == "exported"
    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="import",
            archive=archive,
            cache_dir=tmp_path / "second",
        )
    )
    imported = json.loads(capsys.readouterr().out)
    assert imported["status"] == "imported" and imported["package_digest"] == snapshot.digest
    PackageCatalog(tmp_path / "second").get(snapshot.digest).verify()


@pytest.mark.postgres
async def test_cli_continuous_lifecycle_cancellation_releases_workers_and_host(
    tmp_path, bot_database, monkeypatch, capsys
):  # noqa: F811
    from argparse import Namespace

    import forest.watch as watch_module
    from forest.cli import run_watch_command

    store, dsn = bot_database
    source = source_package(tmp_path)
    captured = []

    class CapturedWatcher(DevWatcher):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured.append(self)

    monkeypatch.setattr(watch_module, "DevWatcher", CapturedWatcher)
    monkeypatch.setenv("FOREST_BOT_DSN", dsn)
    command = asyncio.create_task(
        run_watch_command(
            Namespace(bot=store.bot_id, watch=source, cache_dir=tmp_path / "catalog", grant=[]), {}
        )
    )
    try:
        await eventually(lambda: captured and captured[0].published_count == 1)
        worker = next(iter(captured[0].kernel._workers))
        assert worker.alive and not command.done()
        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command
        assert not worker.alive and not captured[0].kernel._workers
        assert captured[0].task.done() and not captured[0]._io
        successor = await store.acquire_host("after-cli", uuid4().hex)
        assert successor.owner_epoch > captured[0].kernel.lease.owner_epoch
        lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert any(event["status"] == "published" for event in lines)
        assert lines[-1]["status"] == "stopped"
    finally:
        command.cancel()
        await asyncio.gather(command, return_exceptions=True)


@pytest.mark.postgres
async def test_close_cancels_owned_start_without_stranding_a_late_supervisor(watching, monkeypatch):
    watcher, kernel, source, _ = watching
    other = DevWatcher(kernel, watcher.catalog, source)
    entered, release = asyncio.Event(), asyncio.Event()
    initialize = other.store.initialize

    async def held_initialize(*args, **kwargs):
        entered.set()
        await release.wait()
        return await initialize(*args, **kwargs)

    monkeypatch.setattr(other.store, "initialize", held_initialize)
    starting = asyncio.create_task(other.start())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await other.close()
        with pytest.raises(asyncio.CancelledError):
            await starting
        release.set()
        assert other.task is None and other._start_task.done() and not other._io
        assert await value(kernel) == 1
    finally:
        release.set()
        await other.close()
        await asyncio.gather(starting, return_exceptions=True)

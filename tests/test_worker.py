from __future__ import annotations

import asyncio
import json
import os
import shutil
import struct
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from forest.drivers import PythonWorker, WorkerIdentity, WorkerLimits
from forest.drivers.protocol import encode_frame, read_frame
from forest.errors import ForestError

SERVICES = [
    f"probe.{name}"
    for name in [
        "echo",
        "counter",
        "wait",
        "child",
        "failchild",
        "block",
        "crash",
        "badreturn",
        "large",
        "forge",
        "latecommit",
        "config",
        "cache",
    ]
]


def package(tmp_path: Path, class_name="Probe", services=None):
    root = tmp_path / uuid4().hex
    module_root = root / "python"
    module_root.mkdir(parents=True)
    shutil.copyfile(Path(__file__).parent / "fixtures/plugins/probe.py", module_root / "probe.py")
    return SimpleNamespace(
        root=root,
        module_root=module_root,
        entrypoint=f"probe:{class_name}",
        digest="sha256:" + "0" * 64,
        manifest={
            "components": [
                {
                    "provides": [
                        {"name": name} for name in SERVICES if services is None or name in services
                    ]
                }
            ]
        },
    )


def identity():
    return WorkerIdentity(str(uuid4()), str(uuid4()), str(uuid4()), "bot-test")


async def active_worker(tmp_path, *, handler=None, config=None, limits=None, class_name="Probe"):
    worker = PythonWorker(package(tmp_path, class_name), identity(), handler, limits=limits)
    try:
        await worker.launch()
        await worker.prepare(config or {})
        await worker.start()
        assert (await worker.health())["healthy"] is True
        await worker.activate()
        return worker
    except BaseException:
        await worker.terminate()
        raise


async def test_real_process_identity_environment_and_stable_operations(tmp_path, monkeypatch):
    for name in (
        "FOREST_TEST_DSN",
        "FOREST_DATABASE_PASSWORD_FILE",
        "OPENAI_API_KEY",
        "PGPASSWORD",
        "AWS_SECRET_ACCESS_KEY",
    ):
        monkeypatch.setenv(name, "synthetic-secret")
    worker = await active_worker(tmp_path)
    try:
        first = await worker.invoke(
            "probe.echo", {"text": "图文 DTO"}, call_id="attempt-a", operation_id="source-message-1"
        )
        second = await worker.invoke(
            "probe.echo", {}, call_id="attempt-b", operation_id="source-message-1"
        )
        assert first["pid"] == worker.pid and worker.pid != os.getpid()
        assert first["call_id"] == "attempt-a"
        assert first["operation"] == second["operation"]
        assert first["payload"] == {"text": "图文 DTO"}
        assert not any(first["environment"].values())
        assert worker.pid == worker.process.pid
        assert worker.process.args[1:5] == ["-I", "-S", "-B", "-c"]
        assert worker.process.args[-2:] == worker.environment.search_paths
        worker.environment.verify()
    finally:
        await worker.stop()
    assert not worker.alive and worker._temporary is None


async def test_candidate_admission_and_lifecycle_snapshot_restore(tmp_path):
    worker = PythonWorker(package(tmp_path), identity())
    try:
        await worker.launch()
        prepared = await worker.prepare({"nested": {"value": 1}}, {"version": 1})
        assert set(prepared["services"]) == set(SERVICES)
        await worker.start()
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.echo", {})
        assert error.value.code == "WORKER_NOT_ACTIVE"
        await worker.restore({"version": 2})
        await worker.activate()
        assert (await worker.invoke("probe.config", {}))["immutable"] is True
        assert (await worker.invoke("probe.echo", {}))["restored"] == {"version": 2}
        await worker.quiesce()
        assert await worker.snapshot() == {"version": 2}
        with pytest.raises(ForestError):
            await worker.invoke("probe.echo", {})
        await worker.resume()
        assert (await worker.invoke("probe.echo", {}))["pid"] == worker.pid
    finally:
        await worker.stop()


async def test_candidate_cannot_commit_state(tmp_path):
    calls = []

    async def gateway(*args):
        calls.append(args)

    worker = PythonWorker(package(tmp_path), identity(), gateway)
    await worker.launch()
    with pytest.raises(ForestError) as error:
        await worker.prepare({"candidate_write": True})
    assert error.value.code == "CALL_CONTEXT_REQUIRED"
    assert not calls and not worker.alive


async def test_gateway_uses_host_identity_and_rejects_forged_scope(tmp_path):
    observed = []

    async def gateway(owner, call_id, action, payload):
        observed.append((owner, call_id, action, payload))
        return {"value": 2, "revision": 3}

    worker = await active_worker(tmp_path, handler=gateway)
    try:
        await worker.invoke("probe.counter", {}, call_id="host-issued", operation_id="event-2")
        assert all(call[0] is worker.identity and call[1] == "host-issued" for call in observed)
        assert [item[2] for item in observed] == ["get", "commit"]
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.forge", {"call_id": "unowned-call"})
        assert error.value.code == "CALL_CONTEXT_REQUIRED"
        with pytest.raises(ForestError) as error:
            await worker.invoke(
                "probe.forge", {"request": {"key": "counter", "bot_id": "other-bot"}}
            )
        assert error.value.code == "STATE_REQUEST_INVALID"
        assert len(observed) == 2
    finally:
        await worker.stop()


async def test_state_rpc_after_result_never_reaches_host_gateway(tmp_path):
    writes = []

    async def gateway(*args):
        writes.append(args)
        return None

    worker = await active_worker(tmp_path, handler=gateway)
    try:
        assert await worker.invoke("probe.latecommit", {}) == "early-result"
        await worker.health()
        assert not writes
    finally:
        await worker.stop()


async def test_oversized_state_result_is_bounded_and_does_not_hang_worker(tmp_path):
    async def gateway(*args):
        return {"value": "x" * 10000}

    worker = await active_worker(
        tmp_path, handler=gateway, limits=WorkerLimits(max_frame_bytes=4096)
    )
    try:
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.counter", {})
        assert error.value.code == "STATE_RESULT_INVALID"
        assert (await worker.invoke("probe.echo", {}))["pid"] == worker.pid
    finally:
        await worker.stop()


async def test_owned_children_finish_before_reply_and_quiesce(tmp_path):
    started, release = asyncio.Event(), asyncio.Event()
    writes = []

    async def gateway(owner, call_id, action, payload):
        if action == "get":
            started.set()
            await release.wait()
        else:
            writes.append(payload)
        return {"value": None, "revision": 0}

    worker = await active_worker(tmp_path, handler=gateway)
    invocation = asyncio.create_task(worker.invoke("probe.child", {}, call_id="parent-call"))
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        assert not invocation.done()
        draining = asyncio.create_task(worker.quiesce())
        await asyncio.sleep(0)
        assert not draining.done()
        release.set()
        assert await invocation == {"handler_returned": True}
        await draining
        assert len(writes) == 1
        assert (await worker.health())["owned_tasks"] == 0
    finally:
        release.set()
        await worker.stop()
        await asyncio.gather(invocation, return_exceptions=True)


@pytest.mark.parametrize(
    "first_still_active", [False, True], ids=["expired-call", "other-live-call"]
)
async def test_cached_context_and_handles_never_acquire_next_calls_authority(
    tmp_path, first_still_active
):
    started, release = asyncio.Event(), asyncio.Event()
    observed = []

    async def gateway(owner, call_id, action, payload):
        observed.append((call_id, payload["key"]))
        if payload["key"] == "cache-gate":
            started.set()
            await release.wait()
        return {"value": 7, "revision": 1}

    worker = await active_worker(tmp_path, handler=gateway)
    first = asyncio.create_task(
        worker.invoke(
            "probe.cache", {"action": "save", "wait": first_still_active}, call_id="call-a"
        )
    )
    try:
        if first_still_active:
            await asyncio.wait_for(started.wait(), timeout=3)
        else:
            await first
        result = await worker.invoke("probe.cache", {"action": "reuse"}, call_id="call-b")
        for name in [
            "context",
            "state",
            "operations",
            "tasks",
            "component_state",
            "component_operations",
            "component_tasks",
        ]:
            assert result[name] == "CALL_CONTEXT_REQUIRED"
        assert result["fresh_call"] == "call-b"
        assert result["fresh_state"] == {"value": 7, "revision": 1}
        assert [item for item in observed if item[1] == "counter"] == [("call-b", "counter")]
        release.set()
        await first
    finally:
        release.set()
        await worker.stop()
        await asyncio.gather(first, return_exceptions=True)


async def test_owned_child_failure_is_observed(tmp_path):
    worker = await active_worker(tmp_path)
    try:
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.failchild", {})
        assert error.value.code == "OWNED_TASK_FAILED"
        assert (await worker.health())["owned_tasks"] == 0
    finally:
        await worker.stop()


async def test_bounded_invocation_queue_rejects_overload(tmp_path):
    started, release = asyncio.Event(), asyncio.Event()

    async def gateway(*args):
        started.set()
        await release.wait()
        return None

    worker = await active_worker(
        tmp_path, handler=gateway, limits=WorkerLimits(max_inflight=1, max_queue_items=1)
    )
    first = asyncio.create_task(worker.invoke("probe.wait", {}))
    second = None
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        second = asyncio.create_task(worker.invoke("probe.wait", {}))
        await asyncio.sleep(0)
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.wait", {})
        assert error.value.code == "OVERLOADED"
        release.set()
        assert await first == await second == {"done": True}
    finally:
        release.set()
        await worker.stop()
        await asyncio.gather(*[task for task in (first, second) if task], return_exceptions=True)


async def test_cooperative_cancellation_preserves_worker(tmp_path):
    started = asyncio.Event()
    finished = asyncio.Event()

    async def gateway(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    worker = await active_worker(tmp_path, handler=gateway)
    invocation = asyncio.create_task(worker.invoke("probe.wait", {}, call_id="cancel-me"))
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        await worker.cancel("cancel-me")
        with pytest.raises(ForestError) as error:
            await invocation
        assert error.value.code == "CALL_CANCELLED"
        assert finished.is_set()
        assert not worker._state_tasks
        assert worker.alive
        assert (await worker.invoke("probe.echo", {}))["pid"] == worker.pid
    finally:
        await worker.stop()
        await asyncio.gather(invocation, return_exceptions=True)


async def test_queued_cancel_never_delivers_handler(tmp_path):
    started, release = asyncio.Event(), asyncio.Event()
    observed = []

    async def gateway(owner, call_id, action, payload):
        observed.append(call_id)
        started.set()
        await release.wait()
        return None

    worker = await active_worker(
        tmp_path, handler=gateway, limits=WorkerLimits(max_inflight=1, max_queue_items=1)
    )
    first = asyncio.create_task(worker.invoke("probe.wait", {}, call_id="running"))
    second = None
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        second = asyncio.create_task(worker.invoke("probe.wait", {}, call_id="queued"))
        await asyncio.sleep(0)
        assert worker._admitted == 2
        await worker.cancel("queued")
        with pytest.raises(ForestError) as error:
            await second
        assert error.value.code == "CALL_CANCELLED"
        assert worker._admitted == 1
        release.set()
        await first
        assert observed == ["running"]
    finally:
        release.set()
        await worker.stop()
        await asyncio.gather(*[task for task in (first, second) if task], return_exceptions=True)


async def test_blocking_handler_deadline_kills_and_reaps_worker(tmp_path):
    started = asyncio.Event()

    async def gateway(*args):
        started.set()
        return None

    worker = await active_worker(tmp_path, handler=gateway)
    invocation = asyncio.create_task(worker.invoke("probe.block", {}, timeout=0.3))
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        before = time.monotonic()
        with pytest.raises(ForestError) as error:
            await invocation
        assert error.value.code == "DEADLINE_EXCEEDED"
        assert time.monotonic() - before < 3
        assert not worker.alive
        assert worker.process.returncode is not None
        assert worker._temporary is None
    finally:
        await worker.terminate()


async def test_prepare_failure_reaps_candidate_and_manifest_mismatch(tmp_path):
    worker = PythonWorker(package(tmp_path, "FailPrepare"), identity())
    await worker.launch()
    with pytest.raises(ForestError) as error:
        await worker.prepare({})
    assert error.value.code == "PLUGIN_FAILED"
    assert not worker.alive
    mismatch = PythonWorker(package(tmp_path, services=["probe.echo"]), identity())
    await mismatch.launch()
    with pytest.raises(ForestError) as error:
        await mismatch.prepare({})
    assert error.value.code == "SERVICE_MANIFEST_MISMATCH"
    assert not mismatch.alive


async def test_stop_cancels_background_and_forces_blocking_hook(tmp_path):
    marker = tmp_path / "cleanup.txt"
    worker = await active_worker(
        tmp_path, config={"background": True, "cleanup_marker": str(marker)}
    )
    assert (await worker.health())["owned_tasks"] == 1
    await worker.stop()
    assert marker.read_text() == "cancelled"
    assert not worker.alive
    blocking = await active_worker(tmp_path, class_name="BlockStop")
    before = time.monotonic()
    await blocking.stop(timeout=0.2)
    assert time.monotonic() - before < 3
    assert not blocking.alive


async def test_invalid_and_large_plugin_results_are_bounded(tmp_path):
    worker = await active_worker(tmp_path, limits=WorkerLimits(max_frame_bytes=4096))
    try:
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.badreturn", {})
        assert error.value.code == "IPC_FRAME_INVALID"
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.large", {"size": 10000})
        assert error.value.code == "IPC_FRAME_TOO_LARGE"
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.echo", {"text": "x" * 10000})
        assert error.value.code == "IPC_FRAME_TOO_LARGE"
        assert worker.alive
    finally:
        await worker.stop()


@pytest.mark.parametrize("changed", ["token", "identity"])
async def test_real_worker_wrong_token_or_identity_cannot_handshake(tmp_path, monkeypatch, changed):
    original_popen = subprocess.Popen

    class ChangedBootstrap:
        def __init__(self, pipe):
            self.pipe = pipe

        def write(self, data):
            bootstrap = json.loads(data)
            if changed == "token":
                bootstrap["token"] = "wrong-one-time-token"
            else:
                bootstrap["identity"]["bot_id"] = "other-bot"
            return self.pipe.write(json.dumps(bootstrap).encode() + b"\n")

        def flush(self):
            self.pipe.flush()

        def close(self):
            self.pipe.close()

        @property
        def closed(self):
            return self.pipe.closed

    def intercept(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        if kwargs.get("stdin") == subprocess.PIPE:
            process.stdin = ChangedBootstrap(process.stdin)
        return process

    monkeypatch.setattr("forest.drivers.python_worker.subprocess.Popen", intercept)
    worker = PythonWorker(package(tmp_path), identity())
    with pytest.raises(asyncio.TimeoutError):
        await worker.launch(timeout=0.5)
    assert worker.state == "STOPPED" and not worker.alive
    assert worker._server is None and worker._temporary is None


async def test_abrupt_worker_exit_fails_pending_call_and_releases_handles(tmp_path):
    worker = await active_worker(tmp_path)
    try:
        with pytest.raises(ForestError) as error:
            await worker.invoke("probe.crash", {})
        assert error.value.code == "WORKER_DISCONNECTED"
    finally:
        await worker.terminate()
    assert not worker.alive and not worker._pending and not worker._state_tasks
    assert worker._temporary is None


async def test_owned_task_limit_rejects_without_leaking_coroutines():
    import inspect

    from forest.sdk import OwnedTasks

    tasks = OwnedTasks(limit=1)
    tasks.spawn_owned(asyncio.Event().wait())
    extra = asyncio.Event().wait()
    with pytest.raises(ForestError) as error:
        tasks.spawn_owned(extra)
    assert error.value.code == "OWNED_TASK_LIMIT"
    assert inspect.getcoroutinestate(extra) == inspect.CORO_CLOSED
    await tasks.close()
    assert not tasks.tasks


@pytest.mark.parametrize(
    "data",
    [
        b'{"wire_version":1,"x":1,"x":2}',
        b'{"wire_version":1,"x":NaN}',
        b'{"wire_version":99}',
        b"[1,2,3]",
    ],
)
async def test_frame_rejects_duplicate_keys_nan_and_invalid_envelope(data):
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack("!I", len(data)) + data)
    with pytest.raises(ForestError):
        await read_frame(reader)


async def test_frame_rejects_oversized_length_without_reading_payload():
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack("!I", 100000000))
    with pytest.raises(ForestError) as error:
        await read_frame(reader)
    assert error.value.code == "IPC_FRAME_TOO_LARGE"


def test_frame_rejects_excessive_depth_and_executable_objects():
    nested = {}
    for _ in range(40):
        nested = {"child": nested}
    for value in (nested, {"callable": lambda: None}, {"integer": 2**100}, {"bytes": b"secret"}):
        with pytest.raises(ForestError):
            encode_frame(value)

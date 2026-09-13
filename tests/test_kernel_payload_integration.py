"""Actual Worker delivery, immutable receipts and exposure-driven retirement."""

import asyncio
import json

import pytest
from test_kernel import GRANTS, package, wait_for
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.memory_types import SourceRef
from forest.packages import PackageCatalog
from forest.storage.exposures import ExposureStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres

ECHO = '''from forest.sdk import Plugin, service
class Counter(Plugin):
    @service("counter.increment")
    async def increment(self, payload, ctx):
        return {"value": payload["increment"], "revision": 1}
'''


async def row(store, sql, args=()):
    async with store.pool.connection() as conn:
        return await (await conn.execute(sql, args)).fetchone()


async def source(kernel, name):
    return await PayloadStore(kernel.store).put_json(
        kernel.lease, owner_kind="context.parts", owner_id=name,
        value={"text": name}, sources=(SourceRef(name),), expected_deletion_epoch=0,
    )


async def test_real_receipt_replay_preserves_reference_and_parent_authority(bot_database, tmp_path):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(package(tmp_path, code=ECHO), grants=GRANTS)
        original, other = await source(kernel, "origin"), await source(kernel, "other")
        kwargs = dict(operation_id="with-origin", input_parent_ids=(original.payload_id,))
        receipt = await kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 7}, **kwargs
        )
        assert receipt.value == {"value": 7, "revision": 1}
        assert receipt.result_digest.startswith("sha256:")
        assert await kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 7}, **kwargs
        ) == receipt
        stored = await row(store, "SELECT * FROM public.bot_calls WHERE operation_id='with-origin'")
        assert stored["result"] is None and stored["result_payload_id"] == receipt.result_payload_id
        assert (await store.read_call("with-origin")).get("result") is None
        manifest = await row(store, "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
                             (receipt.result_payload_id,))
        assert stored["input_payload_id"] in manifest["manifest"]["parents"]
        with pytest.raises(ForestError) as caught:
            await kernel.invoke_with_receipt(
                "example.counter", "counter.increment", {"increment": 7},
                operation_id="with-origin", input_parent_ids=(other.payload_id,),
            )
        assert caught.value.code == "IDEMPOTENCY_CONFLICT"
        await PayloadStore(store).forget_sources(
            kernel.lease, "forget-origin", (SourceRef("origin"),), expected_deletion_epoch=0
        )
        with pytest.raises(ForestError):
            await kernel.invoke_with_receipt(
                "example.counter", "counter.increment", {"increment": 7}, **kwargs
            )
        assert (await row(store, "SELECT status FROM public.bot_calls WHERE operation_id='with-origin'"))["status"] == "COMPLETED"
    finally:
        await kernel.close()


async def test_json_null_is_completed_receipt_not_missing_body(bot_database, tmp_path):
    store, _ = bot_database
    package(tmp_path, code=ECHO.replace('return {"value": payload["increment"], "revision": 1}', 'return None'))
    source_path = tmp_path / "source-0.1.0"
    (source_path / "schemas" / "output.json").write_text(json.dumps({"type": "null"}))
    snapshot = PackageCatalog(tmp_path / "null-catalog").build(source_path)
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(snapshot, grants=GRANTS)
        receipt = await kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 1}, operation_id="null"
        )
        assert receipt.value is None and receipt.result_payload_id
        assert await kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 1}, operation_id="null"
        ) == receipt
        assert (await row(store, "SELECT status FROM public.bot_calls WHERE operation_id='null'"))["status"] == "COMPLETED"
    finally:
        await kernel.close()


async def test_revoked_input_while_waiting_for_worker_slot_never_sends_frame(bot_database, tmp_path, monkeypatch):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    task = None
    acquired = 0
    try:
        await kernel.install(package(tmp_path, code=ECHO), grants=GRANTS)
        worker = kernel.components["example.counter"].worker
        parent = await source(kernel, "queued-origin")
        frames = []
        request = worker._request

        async def observed(kind, *args, **kwargs):
            if kind == "invoke":
                frames.append(kind)
            return await request(kind, *args, **kwargs)

        monkeypatch.setattr(worker, "_request", observed)
        for _ in range(worker.limits.max_inflight):
            await worker._slots.acquire()
            acquired += 1
        task = asyncio.create_task(kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 1},
            operation_id="queued", input_parent_ids=(parent.payload_id,),
        ))
        await wait_for(lambda: bool(kernel._calls))
        await PayloadStore(store).forget_sources(
            kernel.lease, "forget-queued", (SourceRef("queued-origin"),), expected_deletion_epoch=0
        )
        while acquired:
            worker._slots.release()
            acquired -= 1
        with pytest.raises(ForestError):
            await task
        assert not frames
        saved = await row(store, "SELECT status,result_payload_id FROM public.bot_calls WHERE operation_id='queued'")
        assert saved == {"status": "FAILED", "result_payload_id": None}
        await wait_for(lambda: not worker.alive and not kernel.components, timeout=5)
        assert kernel.failure is None and not kernel._heartbeat.done()
    finally:
        while acquired:
            worker._slots.release()
            acquired -= 1
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await kernel.close()


async def test_exhausted_worker_closes_admission_then_drains_without_epoch_change(bot_database, tmp_path):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    task = None
    release_file = tmp_path / "release-draining-worker"
    try:
        wait = (
            f"while not Path({json.dumps(str(release_file))}).exists():\n"
            "            await asyncio.sleep(0.01)\n"
            "        return {"
        )
        code = "import asyncio\nfrom pathlib import Path\n" + ECHO.replace("return {", wait)
        await kernel.install(package(tmp_path, code=code), grants=GRANTS)
        worker = kernel.components["example.counter"].worker
        generation = worker.identity.generation_id
        before = await store.get_route("example.counter")
        exposures = ExposureStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            await store._routes(conn, ["example.counter"])
            await store._host(conn, kernel.lease)
            head = await exposures.guard_generation_in(conn, generation)
            refs = []
            for index in range(255 - head["payload_count"]):
                ref = await exposures.payloads.put_json_in(
                    conn, owner_kind="state.value", owner_id=f"host-delivery-{index}",
                    value=index, expected_deletion_epoch=0,
                )
                refs.append(ref.payload_id)
            await exposures.expose_in(conn, generation, "host-preloaded", "STATE_READ", refs, "state-fixture")
        task = asyncio.create_task(kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 5}, operation_id="draining"
        ))
        await wait_for(lambda: bool(worker._calls))
        with pytest.raises(ForestError) as error:
            await kernel.invoke(
                "example.counter", "counter.increment", {"increment": 2}, operation_id="overflow"
            )
        assert error.value.code == "GENERATION_EXPOSURE_LIMIT"
        async with asyncio.timeout(5):
            while (await store.get_route("example.counter"))["admission"]:
                await asyncio.sleep(0.02)
        closed = await store.get_route("example.counter")
        assert closed["route_epoch"] == before["route_epoch"]
        assert worker.alive and not task.done()
        release_file.write_text("release")
        assert (await task).value == {"value": 5, "revision": 1}
        await wait_for(lambda: not worker.alive and not kernel.components)
        assert kernel.failure is None and not kernel._heartbeat.done()
        assert await store.read_call("overflow") is None
    finally:
        release_file.touch()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await kernel.close()


async def test_prior_worker_cache_taints_later_result_and_state_then_reaps(bot_database, tmp_path):
    store, _ = bot_database
    code = '''from forest.sdk import Plugin, service
class Counter(Plugin):
    @service("counter.increment")
    async def increment(self, payload, ctx):
        if not hasattr(self, "cached"):
            self.cached = payload["increment"]
        if payload["increment"] == 2:
            await ctx.state.commit("cached", self.cached, expected_revision=0,
                operation_id=ctx.operations.stable("cache-write"))
        return {"value": self.cached, "revision": 1}
'''
    snapshot = package(tmp_path, code=code)
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(snapshot, grants=GRANTS)
        worker = kernel.components["example.counter"].worker
        origin = await source(kernel, "cached-origin")
        await kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 7}, operation_id="cache-a",
            input_parent_ids=(origin.payload_id,),
        )
        later = await kernel.invoke_with_receipt(
            "example.counter", "counter.increment", {"increment": 2}, operation_id="cache-b",
        )
        assert later.value["value"] == 7
        state = await row(store, "SELECT * FROM public.bot_component_state WHERE key='cached'")
        assert state["value"] is None and state["revision"] == 1
        payloads = PayloadStore(store)
        assert await payloads.read_json(kernel.lease, state["value_payload_id"], expected_deletion_epoch=0) == 7
        await payloads.forget_sources(kernel.lease, "forget-cache", (SourceRef("cached-origin"),), expected_deletion_epoch=0)
        for identity in (later.result_payload_id, state["value_payload_id"]):
            with pytest.raises(ForestError):
                await payloads.read_json(kernel.lease, identity, expected_deletion_epoch=1)
        with pytest.raises(ForestError):
            await kernel.invoke("example.counter", "counter.increment", {"increment": 3}, operation_id="cache-c")
        assert await store.read_call("cache-c") is None
        await wait_for(lambda: not worker.alive and not kernel.components)
        # Same desired artifact can return with a clean Worker; unavailable state
        # keeps its revision fence but is never restored as a cached value.
        await kernel.install(snapshot, grants=GRANTS)
        assert await kernel.invoke("example.counter", "counter.increment", {"increment": 9}, operation_id="fresh-cache") == {"value": 9, "revision": 1}
        assert kernel.failure is None
    finally:
        await kernel.close()


async def test_candidate_config_revoked_after_snapshot_never_reaches_prepare(bot_database, tmp_path, monkeypatch):
    from forest.drivers.python_worker import PythonWorker

    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        original = await kernel.install(package(tmp_path, code=ECHO), grants=GRANTS)
        old = kernel.components["example.counter"].worker
        snapshots, frames = store.snapshot_state, []
        request = PythonWorker._request

        async def revoke_candidate(lease, component_id, *, generation_id=None):
            value = await snapshots(lease, component_id, generation_id=generation_id)
            if generation_id and generation_id != original["generation_id"]:
                domain = await row(store,
                    "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='GENERATION_CONFIG' AND owner_id=%s",
                    (generation_id,))
                await PayloadStore(store).forget_sources(
                    lease, "forget-candidate", (SourceRef(domain["source_unit_id"]),), expected_deletion_epoch=0
                )
            return value

        async def observe(self, command, *args, **kwargs):
            if self is not old:
                frames.append(command)
            return await request(self, command, *args, **kwargs)

        monkeypatch.setattr(store, "snapshot_state", revoke_candidate)
        monkeypatch.setattr(PythonWorker, "_request", observe)
        with pytest.raises(ForestError) as error:
            await kernel.install(package(tmp_path, "0.2.0", code=ECHO), grants=GRANTS)
        assert error.value.code == "GENERATION_EXPOSURE_REVOKED"
        assert "prepare" not in frames and "start" not in frames and "restore" not in frames
        assert old.alive and kernel.components["example.counter"].worker is old
        assert (await store.get_route("example.counter"))["generation_id"] == original["generation_id"]
        assert len(kernel._workers) == 1
        assert await kernel.invoke("example.counter", "counter.increment", {"increment": 3}, operation_id="old-valid") == {"value": 3, "revision": 1}
    finally:
        await kernel.close()


async def test_exposure_supervisor_failure_terminates_owned_workers(bot_database, tmp_path, monkeypatch):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(package(tmp_path, code=ECHO), grants=GRANTS)
        worker = kernel.components["example.counter"].worker

        async def fail_scan(*args, **kwargs):
            raise OSError("storage transport failed during supervision")

        monkeypatch.setattr(ExposureStore, "scan", fail_scan)
        await wait_for(lambda: kernel.failure is not None and not worker.alive and not kernel._workers)
        assert kernel.failure == "EXPOSURE_SUPERVISION_FAILED"
        assert not kernel._workers
        with pytest.raises(ForestError) as error:
            await kernel.invoke("example.counter", "counter.increment", {"increment": 1}, operation_id="no-admission")
        assert error.value.code == "KERNEL_UNAVAILABLE"
    finally:
        await kernel.close()
    assert kernel._heartbeat.done() and kernel._exposure_monitor.done()
    assert not kernel._exposure_retirements


async def test_child_receipt_becomes_parent_worker_exposure_and_result_parent(bot_database, tmp_path):
    from test_services import package as service_package
    from test_services import spec

    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install_many([
            spec(service_package(tmp_path, "caller", requires=["target"])),
            spec(service_package(tmp_path, "target")),
        ])
        receipt = await kernel.invoke_with_receipt("caller", "caller.run", {"value": 42}, operation_id="parent")
        assert receipt.value["value"] == 42
        child = await row(store, "SELECT * FROM public.bot_calls WHERE parent_call_id IS NOT NULL")
        assert child["result"] is None
        assert await row(store,
            "SELECT 1 AS found FROM public.bot_payload_derivations WHERE payload_id=%s AND parent_payload_id=%s",
            (receipt.result_payload_id, child["result_payload_id"])) == {"found": 1}
        delivered = await row(store,
            "SELECT payload_refs FROM public.bot_generation_exposures WHERE kind='CHILD_RESULT'")
        assert delivered["payload_refs"] == [child["result_payload_id"]]
        domain = await row(store,
            "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='SERVICE_INPUT' AND owner_id=%s",
            (child["call_id"],))
        await PayloadStore(store).forget_sources(kernel.lease, "forget-child", (SourceRef(domain["source_unit_id"]),), expected_deletion_epoch=0)
        with pytest.raises(ForestError):
            await PayloadStore(store).read_json(kernel.lease, receipt.result_payload_id, expected_deletion_epoch=1)
        await wait_for(lambda: not kernel.components and not kernel._workers)
        assert kernel.failure is None
    finally:
        await kernel.close()

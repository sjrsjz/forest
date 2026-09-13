"""Actual committed admissions cannot disappear inside a delayed Host ACK."""

import asyncio

import pytest
from test_kernel import GRANTS, package, wait_for
from test_kernel import bot_database as bot_database
from test_kernel_payload_integration import ECHO, row, source

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.memory_types import SourceRef
from forest.packages import PackageCatalog
from forest.storage.exposures import ExposureStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("ending", ["complete", "revoked", "deadline"])
async def test_exhausted_retirement_waits_for_committed_admission_ack(
    bot_database,
    tmp_path,
    monkeypatch,
    ending,
):
    store, _ = bot_database
    snapshot = package(tmp_path, code=ECHO)
    if ending == "deadline":
        manifest = tmp_path / "source-0.1.0" / "module.toml"
        manifest.write_text(
            manifest.read_text().replace(
                "[components.capabilities]",
                "[components.lifecycle]\ndrain_timeout_s = 1.5\n\n[components.capabilities]",
            )
        )
        snapshot = PackageCatalog(tmp_path / "bounded-catalog").build(manifest.parent)
    kernel = Kernel(store)
    await kernel.start()
    committed, release_ack = asyncio.Event(), asyncio.Event()
    task = None
    try:
        await kernel.install(snapshot, grants=GRANTS)
        worker = kernel.components["example.counter"].worker
        generation = worker.identity.generation_id
        original_source = await source(kernel, "ack-input-source")
        exposures = ExposureStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            await store._routes(conn, ["example.counter"])
            await store._host(conn, kernel.lease)
            head = await exposures.guard_generation_in(conn, generation)
            refs = []
            for index in range(255 - head["payload_count"]):
                ref = await exposures.payloads.put_json_in(
                    conn,
                    owner_kind="state.value",
                    owner_id=f"prepared-{index}",
                    value=index,
                    expected_deletion_epoch=0,
                )
                refs.append(ref.payload_id)
            await exposures.expose_in(conn, generation, "preload", "STATE_READ", refs, "state")

        original_admit = store.admit

        async def delayed_ack(*args, **kwargs):
            grant = await original_admit(*args, **kwargs)
            if grant.operation_id == "delayed-ack":
                # The real transaction and its context managers already exited.
                # Only delivery of that successful ACK to Kernel is suspended.
                committed.set()
                await release_ack.wait()
            return grant

        monkeypatch.setattr(store, "admit", delayed_ack)
        task = asyncio.create_task(
            kernel.invoke_with_receipt(
                "example.counter",
                "counter.increment",
                {"increment": 7},
                operation_id="delayed-ack",
                input_parent_ids=(original_source.payload_id,),
            )
        )
        async with asyncio.timeout(5):
            await committed.wait()
        call = await store.read_call("delayed-ack")
        assert call["status"] == "ADMITTED" and call["call_id"] not in kernel._calls
        with pytest.raises(ForestError) as error:
            await kernel.invoke(
                "example.counter",
                "counter.increment",
                {"increment": 2},
                operation_id="overflow",
            )
        assert error.value.code == "GENERATION_EXPOSURE_LIMIT"
        async with asyncio.timeout(5):
            while (await store.get_route("example.counter"))["admission"]:
                await asyncio.sleep(0.01)
        # Let the retirement's first emptiness check execute while ACK is still
        # blocked. The durable row, not a sleep in the Worker, keeps it alive.
        await asyncio.sleep(0.25)
        assert (await store.read_call("delayed-ack"))["status"] == "ADMITTED"
        assert worker.alive and call["call_id"] not in kernel._calls

        if ending == "complete":
            release_ack.set()
            assert (await task).value == {"value": 7, "revision": 1}
            assert (await store.read_call("delayed-ack"))["status"] == "COMPLETED"
        else:
            if ending == "revoked":
                await PayloadStore(store).forget_sources(
                    kernel.lease,
                    "erase-before-ack",
                    (SourceRef("ack-input-source"),),
                    expected_deletion_epoch=0,
                )
            # REVOKED interrupts the wait; the configured deadline bounds even
            # a still-live persistent call whose ACK never arrived.
            await wait_for(lambda: not worker.alive and not kernel.components, timeout=5)
            assert not release_ack.is_set()
            assert (await store.read_call("delayed-ack"))["status"] == "FAILED"
            release_ack.set()
            with pytest.raises(ForestError):
                await task
        await wait_for(lambda: not worker.alive and not kernel.components, timeout=5)
        decision = await row(
            store,
            "SELECT status FROM public.bot_generation_exposure_heads WHERE generation_id=%s",
            (generation,),
        )
        assert decision["status"] == ("REVOKED" if ending == "revoked" else "EXHAUSTED")
        assert kernel.failure is None and not kernel._heartbeat.done()
    finally:
        release_ack.set()
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await kernel.close()

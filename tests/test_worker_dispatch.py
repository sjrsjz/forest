"""The final Host fence runs after real Worker capacity, before invoke bytes."""

import asyncio

import pytest
from test_worker import active_worker, identity, package

from forest.drivers.python_worker import PythonWorker, WorkerLimits
from forest.errors import ForestError


async def test_before_send_waits_for_real_slot_and_rejection_never_dispatches(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    checked = []
    calls = []

    async def gateway(identity, call_id, action, payload):
        calls.append(call_id)
        entered.set()
        await release.wait()
        return {"value": 0, "revision": 0}

    async def revoked():
        checked.append(True)
        raise ForestError("GENERATION_EXPOSURE_REVOKED", "Source removed", 410)

    worker = await active_worker(tmp_path, handler=gateway, limits=WorkerLimits(max_inflight=1))
    first = asyncio.create_task(worker.invoke("probe.wait", {}, call_id="running"))
    pending = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        pending = asyncio.create_task(
            worker.invoke("probe.counter", {}, call_id="queued", before_send=revoked)
        )
        async with asyncio.timeout(5):
            while worker._admitted != 2:
                await asyncio.sleep(0)
        assert checked == [] and calls == ["running"]
        release.set()
        assert await first == {"done": True}
        with pytest.raises(ForestError) as caught:
            await pending
        assert caught.value.code == "GENERATION_EXPOSURE_REVOKED"
        assert checked == [True] and calls == ["running"]
        assert worker._admitted == 0 and not worker._reserved_calls and not worker._calls
        assert (await worker.invoke("probe.echo", {"after": True}))["payload"] == {"after": True}
    finally:
        release.set()
        for task in (first, pending):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (first, pending) if task is not None), return_exceptions=True
        )
        await worker.stop()


async def test_cancellation_during_final_fence_releases_slot_without_worker_invocation(tmp_path):
    entered = asyncio.Event()
    calls = []

    async def gateway(*args):
        calls.append(args)
        return {"value": 0, "revision": 0}

    async def waiting():
        entered.set()
        await asyncio.Event().wait()

    worker = await active_worker(tmp_path, handler=gateway, limits=WorkerLimits(max_inflight=1))
    pending = asyncio.create_task(
        worker.invoke("probe.counter", {}, call_id="guarded", before_send=waiting)
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await worker.cancel("guarded")
        with pytest.raises(ForestError) as caught:
            await pending
        assert caught.value.code == "CALL_CANCELLED"
        assert not calls and worker._admitted == 0 and not worker._pending
        assert (await worker.invoke("probe.echo", {}))["pid"] == worker.pid
    finally:
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await worker.stop()


async def test_typed_state_repair_metadata_survives_both_ipc_directions(tmp_path):
    accepted = {
        "current_revision": 7,
        "value_digest": "sha256:" + "a" * 64,
        "payload_state": "UNAVAILABLE",
    }
    cases = [
        ("KERNEL_CONTENT_REVOKED", accepted, accepted),
        ("KERNEL_CONTENT_REVOKED", {**accepted, "body": "CANARY"}, None),
        ("KERNEL_CONTENT_REVOKED", {**accepted, "current_revision": {"body": "CANARY"}}, None),
        ("KERNEL_CONTENT_REVOKED", {**accepted, "current_revision": True}, None),
        ("KERNEL_CONTENT_REVOKED", {**accepted, "current_revision": 0}, None),
        ("KERNEL_CONTENT_REVOKED", {**accepted, "value_digest": "CANARY"}, None),
        ("KERNEL_CONTENT_REVOKED", {**accepted, "payload_state": "ACTIVE"}, None),
        ("PLUGIN_REJECTED", accepted, None),
    ]

    async def gateway(worker_identity, call_id, action, payload):
        code, details, _ = cases[int(payload["key"])]
        raise ForestError(code, "Synthetic failure", 409, details=details)

    snapshot = package(tmp_path, class_name="MetadataProbe", services=["probe.echo"])
    (snapshot.module_root / "probe.py").write_text('''from forest.errors import ForestError
from forest.sdk import Plugin, service
class MetadataProbe(Plugin):
    @service("probe.echo", semantics="read")
    async def metadata(self, payload, ctx):
        if payload.get("local"):
            raise ForestError("KERNEL_CONTENT_REVOKED", "Plugin failure", 409,
                details={"current_revision": {"body": "CANARY"}})
        try:
            await ctx.state.get(str(payload["case"]))
        except ForestError as exc:
            if payload["propagate"]:
                raise
            return exc.details
''', encoding="utf-8")
    worker = PythonWorker(snapshot, identity(), gateway)
    try:
        await worker.launch()
        await worker.prepare({})
        await worker.start()
        await worker.activate()
        for index, (code, _, expected) in enumerate(cases):
            # A successful response shows precisely what the plugin saw.
            assert await worker.invoke("probe.echo", {"case": index, "propagate": False}) == expected
            # Let the same Host error traverse the second IPC direction.
            with pytest.raises(ForestError) as caught:
                await worker.invoke("probe.echo", {"case": index, "propagate": True})
            assert caught.value.code == code and caught.value.details == expected
        with pytest.raises(ForestError) as caught:
            await worker.invoke("probe.echo", {"local": True})
        assert caught.value.details is None
    finally:
        await worker.stop()

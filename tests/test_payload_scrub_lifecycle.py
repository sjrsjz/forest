"""Payload body cleanup belongs to the active kernel.payload Generation."""

import asyncio
from types import SimpleNamespace

import pytest

from forest.core_runtime import PreparedPayloadAuthorityModule


@pytest.mark.asyncio
async def test_payload_scrubber_drains_backlog_and_stops_at_generation_quiesce():
    class Payloads:
        def __init__(self):
            self.calls = 0
            self.drained = asyncio.Event()

        async def scrub(self, *, limit):
            assert limit == 100
            self.calls += 1
            if self.calls == 2:
                self.drained.set()
            return {"scrubbed": 100 if self.calls == 1 else 0}

    payloads = Payloads()
    module = PreparedPayloadAuthorityModule("kernel.payload", payloads, None, None, (), None)
    await module.bind_generation(SimpleNamespace(
        component_id="kernel.payload", generation_id="generation/test",
    ))
    await module.start()
    assert module.owned_tasks() == ()

    module.activate_generation()
    task, = module.owned_tasks()
    await asyncio.wait_for(payloads.drained.wait(), 1)
    assert not task.done()
    await module.quiesce()
    assert task.done() and module.owned_tasks() == ()
    await module.close()


@pytest.mark.asyncio
async def test_payload_scrubber_failure_surfaces_during_quiesce():
    class BrokenPayloads:
        async def scrub(self, *, limit):
            raise RuntimeError("database unavailable")

    module = PreparedPayloadAuthorityModule(
        "kernel.payload", BrokenPayloads(), None, None, (), None,
    )
    await module.bind_generation(SimpleNamespace(
        component_id="kernel.payload", generation_id="generation/broken",
    ))
    await module.start()
    module.activate_generation()
    task, = module.owned_tasks()
    with pytest.raises(RuntimeError, match="database unavailable"):
        await task
    with pytest.raises(RuntimeError, match="database unavailable"):
        await module.quiesce()
    await module.close()

from types import SimpleNamespace

from forest.builtin.component_worker_host.python.component_worker_host import (
    ComponentWorkerHost,
)
from forest.errors import ForestError


async def test_selection_reclaim_replays_already_accepted_kernel_request():
    calls = []
    receipt = {
        "protocol": "forest.runtime-graph-replacement-receipt/3",
        "operation_id": "component-selection:selection/first",
        "activation_id": "activation/first",
        "state": "QUEUED",
        "expected_graph_revision": 7,
        "target_component_id": "counter",
        "selection_revision": 1,
        "selection_digest": "sha256:" + "a" * 64,
    }

    class Queries:
        async def call(self, name, payload):
            assert name == "component.selection.snapshot" and payload == {}
            return {"revision": 2}

    class Services:
        async def call(self, name, payload, **_kwargs):
            calls.append((name, payload))
            assert payload["operation_id"] == receipt["operation_id"]
            if name == "runtime.graph-replacement.status":
                return {"state": "QUEUED"}
            assert name == "runtime.graph-replacement.request"
            assert payload["selection_revision"] == 1
            return receipt

    ctx = SimpleNamespace(
        queries=Queries(), services=Services(),
        operations=SimpleNamespace(stable=lambda step: "stable:" + step),
    )
    task = {"payload": {
        "request_id": "selection/first", "snapshot": {"revision": 1},
        "component_id": "counter", "expected_graph_revision": 7,
        "requested_by": "admin/test", "selection_revision": 1,
    }}
    result = await ComponentWorkerHost().submit_selection(task, ctx)
    assert result == {
        "disposition": "SUBMITTED", "receipt": receipt, "error_code": None,
    }
    assert [name for name, _payload in calls] == [
        "runtime.graph-replacement.status", "runtime.graph-replacement.request",
    ]


async def test_selection_reclaim_supersedes_only_when_kernel_has_no_request():
    calls = []

    class Queries:
        async def call(self, _name, _payload):
            return {"revision": 2}

    class Services:
        async def call(self, name, _payload, **_kwargs):
            calls.append(name)
            raise ForestError(
                "RUNTIME_REPLACEMENT_NOT_FOUND", "Request does not exist", 404,
            )

    ctx = SimpleNamespace(
        queries=Queries(), services=Services(),
        operations=SimpleNamespace(stable=lambda step: "stable:" + step),
    )
    task = {"payload": {
        "request_id": "selection/first", "snapshot": {"revision": 1},
    }}
    result = await ComponentWorkerHost().submit_selection(task, ctx)
    assert result == {
        "disposition": "SUPERSEDED", "receipt": None,
        "error_code": "COMPONENT_SELECTION_CHANGED",
    }
    assert calls == ["runtime.graph-replacement.status"]

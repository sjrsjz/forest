from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from forest.control_driver_recovery import HostDriverHandoffRecovery
from forest.errors import ForestError


class _Ledger:
    def __init__(self):
        self.results = []

    async def finish_consumer(self, lease, **result):
        self.results.append(("finish", lease, result))

    async def recover_consumer(self, lease, **result):
        self.results.append(("recover", lease, result))


class _Supervisor:
    def __init__(self, host=None, *, desired="ONLINE"):
        self.host = host
        self.catalog = SimpleNamespace(inspect=self.inspect)
        self.desired = desired

    @staticmethod
    def management_scope_id(bot_id):
        return bot_id

    async def inspect(self, bot_id):
        return {"bot_id": bot_id, "desired_state": self.desired,
                "observed_state": "FAILED"}

    @asynccontextmanager
    async def management_host(self, scope_id):
        if self.host is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot is starting", 409)
        yield self.host


def _recovery(*, cold=True, bot_available=True, binding="new", desired="ONLINE"):
    old_host = "host-planned"
    current_host = "host-cold" if cold else old_host
    old_bot = "bot-planned"
    current_bot = "bot-cold" if cold else old_bot
    host_identity = SimpleNamespace(graph_id=current_host, graph_revision=7,
                                    activation_id=current_host)
    bot_identity = SimpleNamespace(graph_id=current_bot, graph_revision=9,
                                   activation_id=current_bot)
    host_registry = SimpleNamespace(
        activation_id=current_host, graph_identity=host_identity,
        descriptors=(SimpleNamespace(component_id="driver",
                                     artifact=SimpleNamespace(package_digest="pkg")),),
    )
    bot_registry = SimpleNamespace(
        activation_id=current_bot, graph_identity=bot_identity,
        descriptors=(SimpleNamespace(effect_driver_bindings=(
            ("effect", "kind", binding),
        )),),
    )

    async def current_graph():
        return host_identity

    async def graph_for_activation(activation_id):
        assert activation_id == current_bot
        return {"graph_id": current_bot, "graph_revision": 9,
                "activation_id": current_bot, "state": "ACTIVE"}

    controller = SimpleNamespace(
        registry=host_registry,
        runtime=SimpleNamespace(
            generation_authority=SimpleNamespace(current_graph=current_graph),
            authority_lease=lambda: "lease",
        ),
        driver_exports=lambda: ({"kind": "kind", "source_ref": "new"},),
    )
    controller._fail = lambda error: error
    bot = SimpleNamespace(registry=bot_registry,
                          _kernel=SimpleNamespace(runtime=SimpleNamespace(
                              graph_for_activation=graph_for_activation)))
    supervisor = _Supervisor(bot if bot_available else None, desired=desired)
    ledger = _Ledger()
    recovery = HostDriverHandoffRecovery(
        supervisor=supervisor, controller=controller,
        gate=asyncio.Lock(), ledger=ledger,
    )
    request = {"activation_id": old_host, "target_component_id": "driver",
               "package_digest": "pkg"}
    pending = ({"bot_id": "one", "bot_activation_id": old_bot,
                "source_bindings": [["effect", "kind", "old"]],
                "target_bindings": [["effect", "kind", "new"]]},)
    return recovery, ledger, request, pending


@pytest.mark.asyncio
async def test_pending_handoff_waits_for_online_intent_retry_then_records_cold_graphs():
    recovery, ledger, request, pending = _recovery(bot_available=False)
    await recovery(request, pending)
    assert ledger.results == []
    recovery.supervisor.host = _recovery()[0].supervisor.host
    await recovery(request, pending)
    assert len(ledger.results) == 1
    kind, lease, proof = ledger.results[0]
    assert (kind, lease) == ("recover", "lease")
    assert proof["host_graph"]["graph_id"] == "host-cold"
    assert proof["bot_graph"]["graph_id"] == "bot-cold"
    assert proof["bot_bindings"] == (("effect", "kind", "new"),)


@pytest.mark.asyncio
async def test_withdrawn_bot_settles_failure_without_graph_proof():
    recovery, ledger, request, pending = _recovery(bot_available=False, desired="OFFLINE")
    await recovery(request, pending)
    assert ledger.results[0][0] == "finish"
    assert ledger.results[0][2]["state"] == "FAILED"


@pytest.mark.asyncio
async def test_live_bot_on_old_source_cannot_settle_handoff():
    recovery, ledger, request, pending = _recovery(binding="old")
    with pytest.raises(ForestError, match="current Host driver") as error:
        await recovery(request, pending)
    assert error.value.code == "HOST_DRIVER_RECOVERY_BINDING_MISMATCH"
    assert ledger.results == []

@pytest.mark.asyncio
async def test_host_request_waits_for_pending_consumer_before_success():
    from forest.control_package_controller import ControlPackageController

    request = {"activation_id": "handoff", "operation_id": "operation"}
    evidence = {"graph_id": "host-graph", "graph_revision": 2, "state": "RETIRED"}
    controller = SimpleNamespace(
        scope_kind="host", _closed=False,
        runtime=SimpleNamespace(authority_lease=lambda: "lease"),
    )

    class Authority:
        scope = "host"
        state = "PENDING"
        claimed = False
        result = None

        async def claim(self, lease):
            assert lease == "lease"
            if self.claimed:
                return None
            self.claimed = True
            return request

        async def published_graph(self, selected):
            assert selected is request
            return evidence

        async def consumer_results(self, activation_id):
            assert activation_id == "handoff"
            return ({"state": self.state, "bot_id": "one"},)

        async def settle(self, lease, **result):
            self.result = result
            controller._closed = True

    authority = Authority()
    calls = 0

    async def recover(selected, pending):
        nonlocal calls
        assert selected is request and pending[0]["state"] == "PENDING"
        calls += 1
        if calls == 2:
            authority.state = "SUCCEEDED"

    await ControlPackageController.serve_graph_changes(
        controller, authority, recover_consumers=recover,
    )
    assert calls == 2
    assert authority.result == {"operation_id": "operation", "graph": evidence}

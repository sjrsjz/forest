"""Cross-scope driver sources remain fenced across Bot graph generations."""

from types import SimpleNamespace

import pytest

from forest.control_driver_binding_gate import ControlDriverBindingGate
from forest.errors import ForestError


@pytest.mark.asyncio
async def test_binding_snapshot_changes_only_with_bot_graph_publication():
    gate = ControlDriverBindingGate()
    old_source = "sha256:" + "a" * 64
    new_source = "sha256:" + "b" * 64

    def graph(source):
        return SimpleNamespace(descriptors=(
            SimpleNamespace(effect_driver_bindings=(
                ("consumer.effect", "example.driver", source),
            )),
        ))

    host = SimpleNamespace(
        settings=SimpleNamespace(bot_id="bot/one"),
        registry=graph(old_source),
    )
    async with gate:
        gate.attach(host)

    # Merely assembling a new graph does not release the old Host Generation.
    candidate = graph(new_source)
    with pytest.raises(ForestError) as still_old:
        gate.require_unbound(({"source_ref": old_source},))
    assert still_old.value.code == "CONTROL_DRIVER_CONSUMERS_ACTIVE"

    async with gate:
        host.registry = candidate
        gate.refresh(host)
    gate.require_unbound(({"source_ref": old_source},))
    with pytest.raises(ForestError) as now_new:
        gate.require_unbound(({"source_ref": new_source},))
    assert now_new.value.code == "CONTROL_DRIVER_CONSUMERS_ACTIVE"

    async with gate:
        gate.detach(host)
        gate.require_empty()
        gate.require_unbound(({"source_ref": new_source},))

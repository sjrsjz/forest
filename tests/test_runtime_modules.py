from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.runtime_modules import BotModuleRegistry, RuntimeModuleDescriptor


class Module:
    def __init__(self, name, events, *, fail=False):
        self.name, self.events, self.fail = name, events, fail
        self.task = None
        self.failure = None

    async def start(self):
        self.events.append(("start", self.name))
        if self.fail:
            raise RuntimeError(self.name)

    async def close(self):
        self.events.append(("close", self.name))


def descriptor(name, requires, events, *, fail=False):
    return RuntimeModuleDescriptor(
        name, tuple(requires), lambda _platform, _modules: Module(name, events, fail=fail)
    )


@pytest.mark.asyncio
async def test_registry_owns_dependency_order_and_reverse_lifecycle():
    events = []
    platform = SimpleNamespace()
    registry = BotModuleRegistry(platform, (
        descriptor("consumer", ("provider",), events),
        descriptor("independent", (), events),
        descriptor("provider", (), events),
    ))
    assert registry.order.index("provider") < registry.order.index("consumer")
    assert not hasattr(platform, "consumer")
    await registry.start()
    await registry.close()
    starts = [name for action, name in events if action == "start"]
    closes = [name for action, name in events if action == "close"]
    assert closes == list(reversed(starts))


def test_registry_rejects_missing_or_cyclic_dependencies():
    events = []
    with pytest.raises(ForestError) as caught:
        BotModuleRegistry(SimpleNamespace(), (
            descriptor("left", ("right",), events),
            descriptor("right", ("left",), events),
        ))
    assert caught.value.code == "RUNTIME_MODULE_DEPENDENCY_CYCLE"


@pytest.mark.asyncio
async def test_registry_rolls_back_started_modules_when_start_fails():
    events = []
    registry = BotModuleRegistry(SimpleNamespace(), (
        descriptor("first", (), events),
        descriptor("second", ("first",), events, fail=True),
    ))
    with pytest.raises(RuntimeError, match="second"):
        await registry.start()
    assert events == [("start", "first"), ("start", "second"), ("close", "first")]

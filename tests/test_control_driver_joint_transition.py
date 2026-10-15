"""Joint Host/Bot publication preserves the cross-scope source boundary."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from forest.control_driver_joint_transition import (
    execute_joint_host_driver_transition,
)
from forest.errors import ForestError


class _Graph:
    def __init__(self, trace, name, *, source="", fail_at=None):
        self.trace = trace
        self.name = name
        self.fail_at = fail_at
        self.activation_id = name + "-activation"
        self.graph_identity = SimpleNamespace(graph_id=name)
        self.publication_committed = False
        self.order = ("consumer",)
        self.descriptors = (SimpleNamespace(
            effect_driver_bindings=(("consumer.effect", "test.driver", source),),
        ),)

    async def prewarm(self):
        self.trace.append(self.name + "-prewarm")
        if self.fail_at == "prewarm":
            raise ForestError("PREWARM_FAILED", "Injected prewarm failure", 503)

    async def prepare(self):
        self.trace.append(self.name + "-prepare")
        if self.fail_at == "prepare":
            raise ForestError("PREPARE_FAILED", "Injected prepare failure", 503)

    async def start(self):
        self.trace.append(self.name + "-publish")
        self.publication_committed = True

    async def drain_owned_work_for_replacement(self, *_args, **_kwargs):
        self.trace.append(self.name + "-drain")

    async def resume_owned_work_after_failed_replacement(self):
        self.trace.append(self.name + "-resume")

    async def close(self):
        self.trace.append(self.name + "-close")
        return []

    def preview_graph_transition(self, _previous):
        return SimpleNamespace(worker_capacity=object())

    def plan_graph_transition(self, _previous):
        return ("consumer",)

    def prepared_replacement_facts(self):
        return {}

    def accept_retained_ownership(self):
        self.trace.append(self.name + "-retain")


class _SelectionReader:
    @asynccontextmanager
    async def publication_fence(self, **_kwargs):
        yield


class _Bot:
    def __init__(
        self, trace, future_source, *, fail_at=None,
        bot_id="bot/one", label="",
    ):
        self.settings = SimpleNamespace(bot_id=bot_id)
        self._replacement_lock = asyncio.Lock()
        self._closing = False
        self._fatal_replacement_error = None
        self.registry = _Graph(trace, "old-bot" + label, source="old-source")
        self.candidate = _Graph(
            trace, "new-bot" + label, source=future_source, fail_at=fail_at
        )
        self.selection_reader = _SelectionReader()
        self.trace = trace

    async def _assemble_candidate(self, *, activation_id, prepared_host_graph):
        self.trace.append("bot-assemble")
        assert len(activation_id) == 36
        assert prepared_host_graph[0].name == "new-host"
        return SimpleNamespace(
            registry=self.candidate,
            selection=SimpleNamespace(revision=1, digest="selection"),
            official_descriptors=(),
            official_defaults=(),
            official_effect_drivers=None,
            selection_reader=self.selection_reader,
        )

    async def _admit_worker_capacity(self, _capacity):
        self.trace.append("bot-capacity")


class _Gate:
    def __init__(self, trace):
        self.trace = trace

    def refresh(self, _bot):
        self.trace.append("gate-refresh")


class _Controller:
    def __init__(self, trace):
        self.trace = trace
        self._driver_binding_gate = _Gate(trace)
        self._registry = _Graph(trace, "old-host")
        self._snapshots = {"driver": object()}
        self.failed = None
        self.runtime = SimpleNamespace(
            generation_authority=SimpleNamespace(current_graph=None)
        )

    def driver_exports(self):
        return ({"source_ref": "old-source", "component_id": "driver"},)

    def _fail(self, error):
        self.failed = error
        return error


@pytest.mark.asyncio
async def test_joint_transition_starts_bot_proxy_only_after_host_publication():
    trace = []
    controller = _Controller(trace)
    old_host = controller._registry
    new_host = _Graph(trace, "new-host")
    bot = _Bot(trace, "new-source")
    snapshots = {"driver": object()}

    result = await execute_joint_host_driver_transition(
        controller, old_host, new_host, snapshots, ("driver",), (bot,),
    )

    assert result is new_host
    assert controller._registry is new_host
    assert controller._snapshots is snapshots
    assert bot.registry is bot.candidate
    assert trace.index("new-bot-prepare") < trace.index("old-bot-drain")
    assert trace.index("old-bot-drain") < trace.index("new-host-publish")
    assert trace.index("new-host-publish") < trace.index("new-bot-prewarm")
    assert trace.index("new-bot-publish") < trace.index("gate-refresh")
    assert trace[-2:] == ["old-bot-close", "old-host-close"]


@pytest.mark.asyncio
async def test_bot_preparation_failure_restores_unpublished_host_candidate():
    trace = []
    controller = _Controller(trace)
    old_host = controller._registry
    new_host = _Graph(trace, "new-host")
    bot = _Bot(trace, "new-source", fail_at="prepare")
    old_bot = bot.registry

    with pytest.raises(ForestError) as error:
        await execute_joint_host_driver_transition(
            controller, old_host, new_host, {"driver": object()},
            ("driver",), (bot,),
        )

    assert error.value.code == "PREPARE_FAILED"
    assert controller._registry is old_host
    assert bot.registry is old_bot
    assert "new-host-publish" not in trace
    assert "new-bot-close" in trace
    assert "new-host-close" in trace
    assert "old-bot-drain" not in trace


@pytest.mark.asyncio
async def test_bot_activation_failure_after_host_commit_never_resumes_old_source():
    trace = []
    controller = _Controller(trace)
    old_host = controller._registry
    new_host = _Graph(trace, "new-host")
    bot = _Bot(trace, "new-source", fail_at="prewarm")

    with pytest.raises(ForestError) as error:
        await execute_joint_host_driver_transition(
            controller, old_host, new_host, {"driver": object()},
            ("driver",), (bot,),
        )

    assert error.value.code == "CONTROL_DRIVER_JOINT_POST_PUBLICATION_FAILURE"
    assert controller.failed is error.value
    assert bot._fatal_replacement_error is error.value
    assert "new-host-publish" in trace
    assert "old-bot-resume" not in trace
    assert "old-host-resume" not in trace


@pytest.mark.asyncio
async def test_two_consumers_drain_before_shared_host_publication():
    trace = []
    controller = _Controller(trace)
    old_host = controller._registry
    new_host = _Graph(trace, "new-host")
    first = _Bot(
        trace, "new-source", bot_id="bot/a", label="-a",
    )
    second = _Bot(
        trace, "new-source", bot_id="bot/b", label="-b",
    )
    unrelated = _Bot(
        trace, "other-source", bot_id="bot/c", label="-c",
    )
    old_unrelated = unrelated.registry

    await execute_joint_host_driver_transition(
        controller, old_host, new_host, {"driver": object()},
        ("driver",), (first, second),
    )

    assert first.registry is first.candidate
    assert second.registry is second.candidate
    assert unrelated.registry is old_unrelated
    assert trace.index("old-bot-a-drain") < trace.index("new-host-publish")
    assert trace.index("old-bot-b-drain") < trace.index("new-host-publish")
    assert trace.index("new-host-publish") < trace.index("new-bot-a-prewarm")
    assert trace.index("new-host-publish") < trace.index("new-bot-b-prewarm")
    assert not any(entry.endswith("-c-prepare") for entry in trace)

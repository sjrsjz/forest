from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.runtime_modules import (
    OFFICIAL_MODULES,
    BotModuleRegistry,
    ModuleLifecycleState,
    RuntimeManagementView,
    RuntimeModuleDescriptor,
    RuntimeStorageBundle,
)
from forest.schema_bundles import StorageBundleSpec


class Module:
    def __init__(self, name, events, *, fail=False, close_failures=0):
        self.name, self.events, self.fail = name, events, fail
        self.close_failures = close_failures
        self.task = None
        self.failure = None

    async def start(self):
        self.events.append(("start", self.name))
        if self.fail:
            raise RuntimeError(self.name)

    async def close(self):
        self.events.append(("close", self.name))
        if self.close_failures:
            self.close_failures -= 1
            raise RuntimeError("close:" + self.name)

    async def quiesce(self):
        self.events.append(("quiesce", self.name))


def descriptor(name, requires, events, *, fail=False, close_failures=0):
    return RuntimeModuleDescriptor(
        name,
        tuple(requires),
        lambda _platform, _modules: Module(
            name, events, fail=fail, close_failures=close_failures
        ),
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
    assert set(registry.states.values()) == {ModuleLifecycleState.VALIDATED}
    assert registry.instances == {}
    assert registry.order.index("provider") < registry.order.index("consumer")
    assert not hasattr(platform, "consumer")
    await registry.start()
    assert set(registry.states.values()) == {ModuleLifecycleState.ACTIVE}
    await registry.close()
    assert set(registry.states.values()) == {ModuleLifecycleState.STOPPED}
    starts = [name for action, name in events if action == "start"]
    closes = [name for action, name in events if action == "close"]
    assert closes == list(reversed(starts))


@pytest.mark.asyncio
async def test_storage_is_installed_before_factory_and_prepared_state():
    import hashlib

    events = []
    baseline = b"CREATE TABLE records (record_id text PRIMARY KEY);"
    spec = StorageBundleSpec(
        component_id="owned",
        schema_id="owned.records",
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract="owned.records/1",
    )

    package_digest = "sha256:" + "1" * 64

    class Installer:
        async def install(self, installed_spec, installed_baseline, installed_package):
            events.append(("install", installed_spec, installed_baseline, installed_package))

    platform = SimpleNamespace(schema_bundle_installer=Installer())

    def factory(_platform, _modules):
        events.append(("factory",))
        return Module("owned", events)

    registry = BotModuleRegistry(
        platform,
        (
            RuntimeModuleDescriptor(
                "owned", (), factory,
                storage=RuntimeStorageBundle(spec, baseline, package_digest),
            ),
        ),
    )
    assert registry.states["owned"] == ModuleLifecycleState.VALIDATED
    await registry.start()
    assert [event[0] for event in events] == ["install", "factory", "start"]
    assert registry.states["owned"] == ModuleLifecycleState.ACTIVE
    await registry.close()


@pytest.mark.asyncio
async def test_storage_install_failure_never_constructs_module():
    import hashlib

    baseline = b"CREATE TABLE records (record_id text PRIMARY KEY);"
    spec = StorageBundleSpec(
        component_id="owned",
        schema_id="owned.records",
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract="owned.records/1",
    )
    constructed = []

    class Installer:
        async def install(self, _spec, _baseline, _package_digest):
            raise ForestError("SCHEMA_BUNDLE_MISMATCH", "wrong bundle", 409)

    registry = BotModuleRegistry(
        SimpleNamespace(schema_bundle_installer=Installer()),
        (
            RuntimeModuleDescriptor(
                "owned",
                (),
                lambda _platform, _modules: constructed.append(True),
                storage=RuntimeStorageBundle(spec, baseline, "sha256:" + "2" * 64),
            ),
        ),
    )
    with pytest.raises(ForestError) as caught:
        await registry.start()
    assert caught.value.code == "SCHEMA_BUNDLE_MISMATCH"
    assert constructed == []
    assert registry.instances == {}
    assert registry.states["owned"] == ModuleLifecycleState.STOPPED


def test_storage_bundle_must_be_owned_by_descriptor():
    import hashlib

    baseline = b"CREATE TABLE records (record_id text PRIMARY KEY);"
    storage = RuntimeStorageBundle(
        StorageBundleSpec(
            component_id="other",
            schema_id="other.records",
            schema_version="1.0.0",
            baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract="other.records/1",
        ),
        baseline,
        "sha256:" + "3" * 64,
    )
    with pytest.raises(ForestError) as caught:
        BotModuleRegistry(
            SimpleNamespace(),
            (RuntimeModuleDescriptor("owned", (), lambda *_: object(), storage=storage),),
        )
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"


def test_registry_rejects_missing_or_cyclic_dependencies():
    events = []
    with pytest.raises(ForestError) as caught:
        BotModuleRegistry(SimpleNamespace(), (
            descriptor("left", ("right",), events),
            descriptor("right", ("left",), events),
        ))
    assert caught.value.code == "RUNTIME_MODULE_DEPENDENCY_CYCLE"


def test_official_management_views_are_declared_by_module_descriptors():
    descriptors = {item.component_id: item for item in OFFICIAL_MODULES}
    agent = descriptors["agent.runtime"].management_view
    memory = descriptors["memory.graph"].management_view
    routing = descriptors["routing.graph"].management_view
    assert (agent.label, agent.template, agent.module) == (
        "Agent 与模型", "/static/modules/agent.runtime.html", "/static/agent-panel.js"
    )
    assert agent.style == "/static/modules/agent.runtime.css"
    assert (memory.label, memory.template, memory.module) == (
        "图文记忆", "/static/modules/memory.graph.html", "/static/memory.js"
    )
    assert memory.style == "/static/modules/memory.graph.css"
    assert (routing.label, routing.template, routing.module) == (
        "路由与执行图", "/static/modules/routing.graph.html", "/static/routing-graph.js"
    )
    assert routing.style == "/static/modules/routing.graph.css"
    assert all(
        item.management_view is None
        for component_id, item in descriptors.items()
        if component_id not in {"agent.runtime", "memory.graph", "routing.graph"}
    )


def test_policy_stores_are_owned_by_routing_modules_not_platform_attributes():
    class Platform:
        def __init__(self):
            self.bot = SimpleNamespace(store=object())

    platform = Platform()
    command = next(item for item in OFFICIAL_MODULES if item.component_id == "command.router")
    message = next(item for item in OFFICIAL_MODULES if item.component_id == "message.router")
    command_runtime = command.factory(platform, {})
    message_runtime = message.factory(platform, {})
    assert command_runtime.policies.store is platform.bot.store
    assert command_runtime.limits.store is platform.bot.store
    assert message_runtime.policies.store is platform.bot.store
    assert message_runtime.limits.store is platform.bot.store
    assert not hasattr(platform, "command_policies")
    assert not hasattr(platform, "message_handler_policies")


@pytest.mark.parametrize(
    ("template", "module", "protocol"),
    [
        ("https://example.test/view.html", "/static/view.js", "forest.trusted-management-view/1"),
        ("/static/../secret.html", "/static/view.js", "forest.trusted-management-view/1"),
        ("/static/view.html", "/static/view.txt", "forest.trusted-management-view/1"),
        ("/static/view.html", "/static/view.js", "forest.trusted-management-view/2"),
    ],
)
def test_runtime_management_view_rejects_untrusted_assets(template, module, protocol):
    with pytest.raises(ValueError):
        RuntimeManagementView("view", template, module, protocol)


def test_runtime_management_view_rejects_untrusted_style():
    with pytest.raises(ValueError):
        RuntimeManagementView(
            "view", "/static/view.html", "/static/view.js", style="/static/../view.css"
        )


@pytest.mark.asyncio
async def test_registry_rolls_back_started_modules_when_start_fails():
    events = []
    registry = BotModuleRegistry(SimpleNamespace(), (
        descriptor("first", (), events),
        descriptor("second", ("first",), events, fail=True),
    ))
    with pytest.raises(RuntimeError, match="second"):
        await registry.start()
    assert events == [
        ("start", "first"),
        ("start", "second"),
        ("close", "second"),
        ("quiesce", "first"),
        ("close", "first"),
    ]
    assert set(registry.states.values()) == {ModuleLifecycleState.STOPPED}


@pytest.mark.asyncio
async def test_registry_continues_start_rollback_after_failed_module_cleanup():
    events = []
    registry = BotModuleRegistry(SimpleNamespace(), (
        descriptor("first", (), events),
        descriptor("second", ("first",), events, fail=True, close_failures=1),
    ))
    with pytest.raises(ForestError) as caught:
        await registry.start()
    assert caught.value.code == "RUNTIME_MODULE_START_ROLLBACK_FAILED"
    assert events == [
        ("start", "first"),
        ("start", "second"),
        ("close", "second"),
        ("quiesce", "first"),
        ("close", "first"),
    ]
    assert registry.states["first"] == ModuleLifecycleState.STOPPED
    assert registry.states["second"] == ModuleLifecycleState.FAILED


@pytest.mark.asyncio
async def test_registry_retries_failed_cleanup_without_restarting_modules():
    events = []
    registry = BotModuleRegistry(SimpleNamespace(), (
        descriptor("owned", (), events, close_failures=1),
    ))
    await registry.start()
    errors = await registry.close()
    assert len(errors) == 1
    assert registry.states["owned"] == ModuleLifecycleState.FAILED
    assert not registry._closed
    assert await registry.close() == []
    assert registry.states["owned"] == ModuleLifecycleState.STOPPED
    assert events == [
        ("start", "owned"), ("quiesce", "owned"),
        ("close", "owned"), ("close", "owned"),
    ]


@pytest.mark.parametrize("identity,requires", [
    ("Agent Runtime", ()),
    ("agent.runtime", ("agent.runtime",)),
    ("agent.runtime", ("memory.graph", "memory.graph")),
])
def test_registry_rejects_invalid_descriptor_identity_or_edges(identity, requires):
    with pytest.raises(ForestError) as caught:
        BotModuleRegistry(SimpleNamespace(), (descriptor(identity, requires, []),))
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"

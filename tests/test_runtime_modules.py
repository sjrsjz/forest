from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.management.host_modules import OFFICIAL_HOST_MODULES, HostModuleHost
from forest.runtime_modules import (
    OFFICIAL_MODULES,
    ModuleLifecycleState,
    RuntimeArtifactIdentity,
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
    RuntimeStorageBundle,
    official_artifact,
    official_storage_bundle,
)
from forest.schema_bundles import StorageBundleSpec


def artifact(marker="0", scope="bot"):
    return RuntimeArtifactIdentity(
        "tests.runtime",
        "1.0.0",
        "sha256:" + marker * 64,
        scope,
        "tests.in-process/1",
        "installed",
    )


@pytest.mark.asyncio
async def test_host_runtime_registry_has_its_own_scope_and_contract():
    host = HostModuleHost(
        "host", "host-id", "incarnation", "deployment-id",
        object(), object(), object(),
    )
    registry = RuntimeModuleRegistry(host, OFFICIAL_HOST_MODULES)
    host.module_registry = registry
    await registry.start()
    try:
        assert registry.scope_kind == "host"
        assert registry.states["host.runtime"] == ModuleLifecycleState.ACTIVE
        assert registry.states["host.storage"] == ModuleLifecycleState.ACTIVE
        runtime = registry.get("host.runtime")
        assert runtime.management_contract.describe()["operations"] == [
            {
                "operation_id": "runtime",
                "kind": "query",
                "authority": "host.runtime.read",
            }
        ]
        contribution, = OFFICIAL_HOST_MODULES[0].management
        assert contribution.scope_kinds == ("host",)
        assert contribution.template == "/static/modules/host.runtime.html"
    finally:
        assert await registry.close() == []


class Module:
    def __init__(self, name, events, *, fail=False, close_failures=0):
        self.name, self.events, self.fail = name, events, fail
        self.close_failures = close_failures
        self.task = None
        self.failure = None
        self.notifications = 0

    def notify(self):
        self.notifications += 1
        self.events.append(("notify", self.name))

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
        artifact(),
    )


@pytest.mark.asyncio
async def test_registry_owns_dependency_order_and_reverse_lifecycle():
    events = []
    platform = SimpleNamespace()
    registry = RuntimeModuleRegistry(platform, (
        descriptor("consumer", ("provider",), events),
        descriptor("independent", (), events),
        descriptor("provider", (), events),
    ))
    assert set(registry.states.values()) == {ModuleLifecycleState.VALIDATED}
    assert registry.instances == {}
    assert registry.order.index("provider") < registry.order.index("consumer")
    assert not hasattr(platform, "consumer")
    await registry.prepare()
    assert set(registry.states.values()) == {ModuleLifecycleState.PREPARED}
    assert not events
    await registry.start()
    assert set(registry.states.values()) == {ModuleLifecycleState.ACTIVE}
    await registry.close()
    assert set(registry.states.values()) == {ModuleLifecycleState.STOPPED}
    starts = [name for action, name in events if action == "start"]
    closes = [name for action, name in events if action == "close"]
    assert closes == list(reversed(starts))


@pytest.mark.asyncio
async def test_registry_routes_generic_work_notifications_from_descriptors():
    events = []
    registry = RuntimeModuleRegistry(
        SimpleNamespace(),
        (
            RuntimeModuleDescriptor(
                "idle", (), lambda *_: Module("idle", events), artifact()
            ),
            RuntimeModuleDescriptor(
                "worker", (), lambda *_: Module("worker", events), artifact(),
                wakes_on_work=True, work_notify_order=20,
            ),
            RuntimeModuleDescriptor(
                "first", (), lambda *_: Module("first", events), artifact(),
                wakes_on_work=True, work_notify_order=10,
            ),
        ),
    )
    await registry.start()
    events.clear()
    registry.notify_work()
    assert registry.get("idle").notifications == 0
    assert registry.get("worker").notifications == 1
    assert registry.get("first").notifications == 1
    assert events == [("notify", "first"), ("notify", "worker")]
    await registry.close()


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
        async def install(
            self,
            installed_spec,
            installed_baseline,
            installed_package,
            *,
            execution_profile,
        ):
            events.append(
                (
                    "install",
                    installed_spec,
                    installed_baseline,
                    installed_package,
                    execution_profile,
                )
            )

    platform = SimpleNamespace(schema_bundle_installer=Installer())

    def factory(_platform, _modules):
        events.append(("factory",))
        return Module("owned", events)

    registry = RuntimeModuleRegistry(
        platform,
        (
            RuntimeModuleDescriptor(
                "owned", (), factory, artifact("1"),
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
        async def install(
            self, _spec, _baseline, _package_digest, *, execution_profile
        ):
            assert execution_profile == "declarative"
            raise ForestError("SCHEMA_BUNDLE_MISMATCH", "wrong bundle", 409)

    registry = RuntimeModuleRegistry(
        SimpleNamespace(schema_bundle_installer=Installer()),
        (
            RuntimeModuleDescriptor(
                "owned", (),
                lambda _platform, _modules: constructed.append(True),
                artifact("2"),
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


@pytest.mark.asyncio
async def test_clean_generation_authority_rejects_modules_without_prepare_contract():
    events = []
    registry = RuntimeModuleRegistry(
        SimpleNamespace(generation_authority=object()),
        (descriptor("legacy", (), events),),
    )
    with pytest.raises(ForestError) as caught:
        await registry.prepare()
    assert caught.value.code == "RUNTIME_GENERATION_PREPARE_UNAVAILABLE"
    assert registry.states["legacy"] == ModuleLifecycleState.STOPPED
    assert events == [("close", "legacy")]


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
        RuntimeModuleRegistry(
            SimpleNamespace(),
            (
                RuntimeModuleDescriptor(
                    "owned", (), lambda *_: object(), artifact("3"), storage=storage
                ),
            ),
        )
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"


def test_storage_bundle_must_match_descriptor_package_digest():
    import hashlib

    baseline = b"CREATE TABLE records (record_id text PRIMARY KEY);"
    storage = RuntimeStorageBundle(
        StorageBundleSpec(
            component_id="owned",
            schema_id="owned.records",
            schema_version="1.0.0",
            baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract="owned.records/1",
        ),
        baseline,
        "sha256:" + "4" * 64,
    )
    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(
            SimpleNamespace(),
            (
                RuntimeModuleDescriptor(
                    "owned", (), lambda *_: object(), artifact("5"), storage=storage
                ),
            ),
        )
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"


def test_installed_module_cannot_select_official_storage_execution():
    import hashlib

    baseline = b"CREATE FUNCTION owned() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;"
    storage = RuntimeStorageBundle(
        StorageBundleSpec(
            component_id="owned",
            schema_id="owned.records",
            schema_version="1.0.0",
            baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract="owned.records/1",
        ),
        baseline,
        "sha256:" + "6" * 64,
        execution_profile="official",
    )
    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(
            SimpleNamespace(),
            (
                RuntimeModuleDescriptor(
                    "owned", (), lambda *_: object(), artifact("6"), storage=storage
                ),
            ),
        )
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"


def test_official_modules_bind_distribution_artifact_into_projection():
    registry = RuntimeModuleRegistry(SimpleNamespace())
    artifacts = {item.artifact for item in OFFICIAL_MODULES}
    assert len(artifacts) == 1
    official = artifacts.pop()
    assert official.package_id == "forest"
    assert official.scope == "bot"
    assert official.driver == "forest.in-process-trusted/1"
    assert official.trust_level == "official"
    assert all(
        item["artifact"] == official.as_fact() for item in registry.describe()
    )
    assert all(
        item["artifact"] == official.as_fact() for item in registry._descriptor_facts()
    )


def test_official_kernel_runtime_bundle_is_distribution_bound_and_business_neutral():
    storage = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    assert storage.package_digest == official_artifact("bot").package_digest
    assert storage.execution_profile == "official"
    assert storage.spec.namespace.startswith("forest_m_forest_kernel_runtime_")
    lowered = storage.baseline.lower()
    for forbidden in (b"agent", b"onebot", b"memory", b"bot_bindings", b"bot_jobs"):
        assert forbidden not in lowered


def test_registry_rejects_missing_or_cyclic_dependencies():
    events = []
    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(SimpleNamespace(), (
            descriptor("left", ("right",), events),
            descriptor("right", ("left",), events),
        ))
    assert caught.value.code == "RUNTIME_MODULE_DEPENDENCY_CYCLE"


@pytest.mark.asyncio
async def test_registry_binds_descriptor_scope_to_its_host_lifecycle():
    events = []
    descriptor = RuntimeModuleDescriptor(
        "deployment.test",
        (),
        lambda *_: Module("deployment.test", events),
        artifact("6", "deployment"),
    )
    registry = RuntimeModuleRegistry(
        SimpleNamespace(scope_kind="deployment"), (descriptor,)
    )
    await registry.start()
    assert registry.states == {"deployment.test": ModuleLifecycleState.ACTIVE}
    assert await registry.close() == []
    assert events == [
        ("start", "deployment.test"),
        ("quiesce", "deployment.test"),
        ("close", "deployment.test"),
    ]


def test_registry_rejects_artifact_from_another_scope():
    descriptor = RuntimeModuleDescriptor(
        "bot.test", (), lambda *_: object(), artifact("7", "bot")
    )
    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(SimpleNamespace(scope_kind="deployment"), (descriptor,))
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"


def test_official_management_contributions_are_declared_by_module_descriptors():
    descriptors = {item.component_id: item for item in OFFICIAL_MODULES}
    runtime, = descriptors["kernel.runtime"].management
    components, = descriptors["component.worker-host"].management
    onebot, = descriptors["adapter.onebot"].management
    agent, = descriptors["agent.runtime"].management
    memory, = descriptors["memory.graph"].management
    routing, = descriptors["routing.graph"].management
    audit, = descriptors["kernel.audit"].management
    command, = descriptors["command.router"].management
    message, = descriptors["message.router"].management
    schedules, = descriptors["scheduler.jobs"].management
    assert (runtime.label, runtime.template, runtime.module) == (
        "运行与业务接纳", "/static/modules/kernel.runtime.html", "/static/kernel-runtime.js"
    )
    assert runtime.scope_kinds == ("bot",) and runtime.contribution_id == "runtime"
    assert (components.label, components.template, components.module) == (
        "可安装组件",
        "/static/modules/component.worker-host.html",
        "/static/component-worker-host.js",
    )
    assert components.scope_kinds == ("bot",) and components.contribution_id == "components"
    assert (agent.label, agent.template, agent.module) == (
        "Agent 与模型", "/static/modules/agent.runtime.html", "/static/agent-panel.js"
    )
    assert agent.style == "/static/modules/agent.runtime.css"
    assert agent.scope_kinds == ("bot",) and agent.contribution_id == "agent"
    assert (memory.label, memory.template, memory.module) == (
        "图文记忆", "/static/modules/memory.graph.html", "/static/memory.js"
    )
    assert memory.style == "/static/modules/memory.graph.css"
    assert (routing.label, routing.template, routing.module) == (
        "路由与执行图", "/static/modules/routing.graph.html", "/static/routing-graph.js"
    )
    assert routing.style == "/static/modules/routing.graph.css"
    assert (audit.label, audit.template, audit.module) == (
        "审计元数据", "/static/modules/kernel.audit.html", "/static/kernel-audit.js"
    )
    assert audit.scope_kinds == ("bot",) and audit.contribution_id == "audit"
    assert (schedules.contribution_id, schedules.scope_kinds, schedules.module) == (
        "schedules", ("bot",), "/static/jobs-panel.js"
    )
    assert (onebot.contribution_id, onebot.scope_kinds, onebot.module) == (
        "connections", ("bot",), "/static/onebot-panel.js"
    )
    assert onebot.template == "/static/modules/adapter.onebot.html"
    assert onebot.style == "/static/modules/adapter.onebot.css"
    assert (command.contribution_id, command.scope_kinds, command.module) == (
        "commands", ("bot",), "/static/command-router.js"
    )
    assert command.template == "/static/modules/command.router.html"
    assert (message.contribution_id, message.scope_kinds, message.module) == (
        "messages", ("bot",), "/static/message-router.js"
    )
    assert message.template == "/static/modules/message.router.html"
    assert all(
        item.management == ()
        for component_id, item in descriptors.items()
        if component_id not in {
            "adapter.onebot", "agent.runtime", "command.router", "component.worker-host",
            "kernel.audit", "kernel.runtime", "memory.graph",
            "message.router", "routing.graph", "scheduler.jobs"
        }
    )


def test_policy_stores_are_owned_by_routing_modules_not_platform_attributes():
    class Platform:
        def __init__(self):
            self.bot = SimpleNamespace(store=object())
            self.kernel = SimpleNamespace(_authority=lambda: object())

    platform = Platform()
    adapter = SimpleNamespace(bindings=object(), messages=object())
    command = next(item for item in OFFICIAL_MODULES if item.component_id == "command.router")
    message = next(item for item in OFFICIAL_MODULES if item.component_id == "message.router")
    command_runtime = command.factory(platform, {"adapter.onebot": adapter})
    message_runtime = message.factory(platform, {"adapter.onebot": adapter})
    assert command_runtime.policies.store is platform.bot.store
    assert command_runtime.limits.store is platform.bot.store
    assert command_runtime.bindings is adapter.bindings
    assert command_runtime.diagnostics.messages is adapter.messages
    assert message_runtime.policies.store is platform.bot.store
    assert message_runtime.limits.store is platform.bot.store
    assert message_runtime.bindings is adapter.bindings
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
def test_runtime_management_contribution_rejects_untrusted_assets(template, module, protocol):
    with pytest.raises(ValueError):
        RuntimeManagementContribution("view", "view", ("bot",), template, module, protocol)


def test_runtime_management_contribution_rejects_untrusted_style():
    with pytest.raises(ValueError):
        RuntimeManagementContribution(
            "view", "view", ("bot",), "/static/view.html", "/static/view.js",
            style="/static/../view.css"
        )


def test_runtime_descriptor_rejects_duplicate_management_contributions():
    contribution = RuntimeManagementContribution(
        "view", "View", ("bot",), "/static/view.html", "/static/view.js"
    )
    with pytest.raises(ValueError, match="identities must be unique"):
        RuntimeModuleDescriptor(
            "owned", (), lambda *_: object(), artifact(),
            management=(contribution, contribution),
        )


@pytest.mark.asyncio
async def test_registry_rolls_back_started_modules_when_start_fails():
    events = []
    registry = RuntimeModuleRegistry(SimpleNamespace(), (
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
    registry = RuntimeModuleRegistry(SimpleNamespace(), (
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
    registry = RuntimeModuleRegistry(SimpleNamespace(), (
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
        RuntimeModuleRegistry(SimpleNamespace(), (descriptor(identity, requires, []),))
    assert caught.value.code == "RUNTIME_MODULE_DESCRIPTOR_INVALID"

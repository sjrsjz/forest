"""Real independent Workers and PostgreSQL-authorized component call trees."""

import asyncio
import json
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.kernel import Kernel, resolve_dependencies
from forest.packages import PackageCatalog
from forest.sdk import CallScope, PluginContext, _current_call
from forest.storage.bot import BotStore


@pytest_asyncio.fixture
async def service_store():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a dedicated test PostgreSQL")
    name = "forest_services_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "service-bot")
    try:
        await store.open()
        await store.migrate()
        yield store
    finally:
        await store.close()
        assert name.startswith("forest_services_") and len(name) == 48
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest_asyncio.fixture
async def running_kernel(service_store):
    kernel = Kernel(service_store)
    await kernel.start()
    try:
        yield kernel
    finally:
        await kernel.close()
        assert not kernel._workers and not kernel._calls and not kernel.cleanup_errors


def package(
    tmp_path,
    name,
    *,
    requires=(),
    capabilities=None,
    contract="1.0",
    code=None,
    version="0.1.0",
    input_schema=None,
    output_schema=None,
    provided_name=None,
):
    source = tmp_path / (name + "-" + uuid4().hex)
    (source / "python").mkdir(parents=True)
    (source / "schemas").mkdir()
    dependencies = [
        (item, ">=1.0,<2.0", True) if isinstance(item, str) else item for item in requires
    ]
    provided_name = provided_name or name + ".run"
    if capabilities is None:
        capabilities = ["service.call:" + item[0] + ".run" for item in dependencies]
    manifest = f'''manifest_version = 1
package_id = "test.{name}"
version = "{version}"
kernel_api = ">=1.0,<2.0"
runtime = "python-worker"
entrypoint = "implementation:Component"
[[components]]
id = "{name}"
scope = "bot"
state_schema = 1
reload_mode = "drain"
config_schema = "schemas/config.json"
[components.capabilities]
requests = {json.dumps(capabilities)}
[components.resources]
max_inflight = 2
max_queue_items = 4
[[components.provides]]
name = "{provided_name}"
contract = "{contract}"
input_schema = "schemas/input.json"
output_schema = "schemas/output.json"
'''
    for target, wanted, required in dependencies:
        manifest += f'''[[components.requires]]
name = "{target}.run"
contract = "{wanted}"
required = {str(required).lower()}
startup_dependency = true
'''
    (source / "module.toml").write_text(manifest, encoding="utf-8")
    for kind, schema in (
        ("config", {"type": "object"}),
        ("input", input_schema or {}),
        ("output", output_schema or {}),
    ):
        (source / "schemas" / (kind + ".json")).write_text(json.dumps(schema), encoding="utf-8")
    if code is None:
        body = 'return {"value": payload["value"], "pid": os.getpid()}'
        if dependencies:
            body = f'''return await ctx.services.call("{dependencies[0][0]}.run", payload,
                operation_id=ctx.operations.stable("forward:v1"))'''
        code = f'''import os
from forest.sdk import Plugin, service
class Component(Plugin):
    @service("{provided_name}")
    async def run(self, payload, ctx):
        {body}
'''
    (source / "python" / "implementation.py").write_text(code, encoding="utf-8")
    return PackageCatalog(tmp_path / "catalog").build(source)


def spec(snapshot, **kwargs):
    return {
        "snapshot": snapshot,
        "grants": frozenset(snapshot.component.get("capabilities", {}).get("requests", [])),
        **kwargs,
    }


async def invoke(kernel, name="caller", *, operation="request", payload=None, timeout=5):
    return await kernel.invoke(
        name,
        name + ".run",
        {"value": 3} if payload is None else payload,
        operation_id=operation,
        timeout=timeout,
    )


@pytest.mark.parametrize("failure", ["missing", "major", "cycle", "duplicate"])
def test_dependency_errors_are_static_before_any_import(tmp_path, failure):
    caller = package(
        tmp_path, "caller", requires=["target"], code="raise RuntimeError('not imported')"
    )
    snapshots = {"caller": caller}
    codes = {
        "missing": "DEPENDENCY_MISSING",
        "major": "DEPENDENCY_INCOMPATIBLE",
        "cycle": "DEPENDENCY_CYCLE",
        "duplicate": "DUPLICATE_SERVICE",
    }
    if failure != "missing":
        snapshots["target"] = package(
            tmp_path,
            "target",
            contract="2.0" if failure == "major" else "1.0",
            requires=["caller"] if failure == "cycle" else [],
        )
    if failure == "duplicate":
        snapshots["other"] = package(tmp_path, "other", provided_name="target.run")
    with pytest.raises(ForestError) as error:
        resolve_dependencies(snapshots)
    assert error.value.code == codes[failure]


async def test_service_handle_cannot_reauthorize_cached_context_in_another_call():
    requests = []

    async def state_rpc(*args):
        raise AssertionError("No state request expected")

    async def rpc(call_id, payload):
        requests.append((call_id, payload))
        return payload["payload"]

    base = PluginContext({}, state_rpc, service_rpc=rpc)
    first = CallScope("call-a", 10**12, "stable-a")
    second = CallScope("call-b", 10**12, "stable-b")
    cached, current = base._for_call(first), base._for_call(second)
    token = _current_call.set(first)
    try:
        assert await cached.services.call("target.run", 1, operation_id="one") == 1
        _current_call.set(second)
        for handle in (base.services, cached.services):
            with pytest.raises(ForestError) as error:
                await handle.call("target.run", 2, operation_id="two")
            assert error.value.code == "CALL_CONTEXT_REQUIRED"
        result = current.tasks.spawn_owned(
            current.services.call("target.run", 3, operation_id="three")
        )
        assert await result == 3
        await base._task_owner.finish_call(second)
        assert [request[0] for request in requests] == ["call-a", "call-b"]
    finally:
        _current_call.reset(token)
        await base._task_owner.close()


@pytest.mark.postgres
async def test_topological_install_real_processes_and_persistent_multihop_tree(
    tmp_path, running_kernel
):
    kernel = running_kernel
    caller = package(tmp_path, "caller", requires=["middle"])
    middle = package(tmp_path, "middle", requires=["target"])
    target = package(tmp_path, "target")
    routes = await kernel.install_many([spec(caller), spec(target), spec(middle)])
    assert [route["component_id"] for route in routes] == ["target", "middle", "caller"]
    pids = [component.worker.process.pid for component in kernel.components.values()]
    assert len(set(pids)) == 3 and os.getpid() not in pids
    assert kernel.dependency_status()["caller"]["dependency_components"] == ["middle", "target"]
    result = await invoke(kernel)
    assert result == {"value": 3, "pid": kernel.components["target"].worker.process.pid}
    assert await invoke(kernel) == result
    async with kernel.store.pool.connection() as conn:
        rows = await (await conn.execute("SELECT * FROM bot_calls ORDER BY call_depth")).fetchall()
    assert len(rows) == 3 and all(row["status"] == "COMPLETED" for row in rows)
    assert [row["call_depth"] for row in rows] == [0, 1, 2]
    assert rows[1]["parent_call_id"] == rows[0]["call_id"]
    assert rows[2]["parent_call_id"] == rows[1]["call_id"]
    assert all(row["root_call_id"] == rows[0]["call_id"] for row in rows)
    assert rows[2]["deadline_at"] <= rows[1]["deadline_at"] <= rows[0]["deadline_at"]
    with pytest.raises(ForestError) as denied:
        await kernel.store.authorized_call(kernel.lease, rows[2]["operation_id"], 1)
    assert denied.value.code == "NESTED_AUTHORITY_REQUIRED"


@pytest.mark.postgres
async def test_batch_preflight_and_optional_degradation_leave_running_worker(
    tmp_path, running_kernel
):
    kernel = running_kernel
    await kernel.install_many([spec(package(tmp_path, "target"))])
    old = kernel.components["target"].worker
    bad = package(tmp_path, "bad", requires=["missing"])
    with pytest.raises(ForestError) as missing:
        await kernel.install_many([spec(package(tmp_path, "new")), spec(bad)])
    assert missing.value.code == "DEPENDENCY_MISSING"
    assert set(kernel.components) == {"target"} and old.alive
    optional = package(tmp_path, "optional", requires=[("missing", ">=1.0,<2.0", False)])
    await kernel.install_many([spec(optional)])
    assert kernel.dependency_status()["optional"]["degraded_dependencies"] == [
        {"service": "missing.run", "contract": ">=1.0,<2.0", "reason": "DEPENDENCY_MISSING"}
    ]
    with pytest.raises(ForestError) as unavailable:
        await invoke(kernel, "optional")
    assert unavailable.value.code == "DEPENDENCY_UNAVAILABLE"
    assert await invoke(kernel, "target", operation="still-live") == {
        "value": 3,
        "pid": old.process.pid,
    }


@pytest.mark.postgres
async def test_disable_publishes_empty_route_after_worker_quiescence(tmp_path, running_kernel):
    kernel = running_kernel
    target = package(tmp_path, "disable-target")
    await kernel.install(target)
    old = kernel.components["disable-target"].worker
    before = await kernel.store.get_route("disable-target")
    result = await kernel.disable("disable-target", operation_id="disable-target-op")
    operation = await kernel.store.read_operation("disable-target-op")
    route = await kernel.store.get_route("disable-target")
    assert before["generation_id"] is not None
    assert result["generation_id"] is None and route["generation_id"] is None
    assert route["route_epoch"] == before["route_epoch"] + 1
    assert not route["admission"] and not old.alive
    assert operation["operation_kind"] == "DISABLE" and operation["phase"] == "COMMITTED"


@pytest.mark.postgres
async def test_disable_waits_for_inflight_and_closes_new_admission(tmp_path, running_kernel):
    kernel = running_kernel
    target = package(tmp_path, "disable-busy")
    await kernel.install(target)
    worker = kernel.components["disable-busy"].worker
    entered, proceed, frozen = asyncio.Event(), asyncio.Event(), asyncio.Event()
    actual_invoke, actual_freeze = worker.invoke, kernel.store.freeze

    async def delayed(*args, **kwargs):
        entered.set()
        await proceed.wait()
        return await actual_invoke(*args, **kwargs)

    async def freeze(*args, **kwargs):
        result = await actual_freeze(*args, **kwargs)
        frozen.set()
        return result

    worker.invoke = delayed
    kernel.store.freeze = freeze
    running = asyncio.create_task(invoke(kernel, "disable-busy", operation="before-disable"))
    disabling = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        disabling = asyncio.create_task(
            kernel.disable("disable-busy", operation_id="disable-busy-op")
        )
        await asyncio.wait_for(frozen.wait(), 5)
        assert not disabling.done()
        with pytest.raises(ForestError) as closed:
            await invoke(kernel, "disable-busy", operation="after-disable-freeze")
        assert closed.value.code == "ADMISSION_CLOSED"
        proceed.set()
        await running
        assert (await disabling)["generation_id"] is None
    finally:
        proceed.set()
        await asyncio.gather(
            running, *([disabling] if disabling is not None else []),
            return_exceptions=True,
        )
        kernel.store.freeze = actual_freeze


@pytest.mark.postgres
async def test_enabled_dependent_blocks_provider_disable(tmp_path, running_kernel):
    kernel = running_kernel
    target = package(tmp_path, "disable-provider")
    caller = package(tmp_path, "disable-caller", requires=[(
        "disable-provider", ">=1.0,<2.0", True
    )])
    await kernel.install_many([spec(caller), spec(target)])
    route = await kernel.store.get_route("disable-provider")
    with pytest.raises(ForestError) as blocked:
        await kernel.disable("disable-provider", operation_id="blocked-disable")
    assert blocked.value.code == "DEPENDENCY_IN_USE"
    assert blocked.value.details["dependent_components"] == ["disable-caller"]
    current = await kernel.store.get_route("disable-provider")
    assert current["generation_id"] == route["generation_id"] and current["admission"]


@pytest.mark.postgres
@pytest.mark.parametrize("failure", ["capability", "major", "input", "output"])
async def test_service_capabilities_and_schema_at_host_boundary(tmp_path, running_kernel, failure):
    kernel = running_kernel
    caps = [] if failure == "capability" else ["service.call:target.run"]
    code = """from forest.sdk import Plugin, service
class Component(Plugin):
    @service("caller.run")
    async def run(self, payload, ctx):
        return await ctx.services.call("target.run", payload, contract_major=MAJOR,
            operation_id=ctx.operations.stable("forward:v1"))
""".replace("MAJOR", "2" if failure == "major" else "1")
    target = package(
        tmp_path,
        "target",
        input_schema={"type": "integer"} if failure == "input" else {},
        output_schema={"type": "integer"} if failure == "output" else {},
    )
    caller = package(tmp_path, "caller", requires=["target"], capabilities=caps, code=code)
    await kernel.install_many([spec(caller), spec(target)])
    with pytest.raises(ForestError) as denied:
        await invoke(kernel)
    assert (
        denied.value.code
        == {
            "capability": "CAPABILITY_DENIED",
            "major": "CONTRACT_INCOMPATIBLE",
            "input": "INPUT_INVALID",
            "output": "OUTPUT_INVALID",
        }[failure]
    )
    async with kernel.store.pool.connection() as conn:
        count = (
            await (
                await conn.execute("SELECT count(*) AS n FROM bot_calls WHERE call_depth>0")
            ).fetchone()
        )["n"]
    assert count == (1 if failure == "output" else 0)


@pytest.mark.postgres
async def test_nested_capacity_fails_fast_instead_of_waiting_for_parent_slot(
    tmp_path, service_store
):
    kernel = Kernel(service_store, max_calls=1)
    await kernel.start()
    try:
        await kernel.install_many(
            [
                spec(package(tmp_path, "caller", requires=["target"])),
                spec(package(tmp_path, "target")),
            ]
        )
        with pytest.raises(ForestError) as overloaded:
            await invoke(kernel, timeout=2)
        assert overloaded.value.code == "BACKPRESSURE"
        assert kernel._call_count == 0 and not kernel._calls
    finally:
        await kernel.close()


@pytest.mark.postgres
async def test_reload_waits_for_reserved_parent_before_late_child(tmp_path, running_kernel):
    kernel = running_kernel
    target = package(tmp_path, "target")
    caller = package(tmp_path, "caller", requires=["target"])
    await kernel.install_many([spec(caller), spec(target)])
    old = kernel.components["target"].worker
    entered, proceed, frozen = asyncio.Event(), asyncio.Event(), asyncio.Event()
    actual_service, actual_freeze = kernel._service_rpc, kernel.store.freeze

    async def delayed(identity, call_id, request):
        entered.set()
        await proceed.wait()
        return await actual_service(identity, call_id, request)

    async def freeze(*args, **kwargs):
        result = await actual_freeze(*args, **kwargs)
        frozen.set()
        return result

    kernel.components["caller"].worker.service_handler = delayed
    kernel.store.freeze = freeze
    task = asyncio.create_task(invoke(kernel, timeout=15))
    replacement = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        replacement = asyncio.create_task(
            kernel.install(package(tmp_path, "target", version="0.2.0"))
        )
        await asyncio.wait_for(frozen.wait(), 5)
        assert old.state == "ACTIVE" and not replacement.done()
        with pytest.raises(ForestError) as blocked:
            await invoke(kernel, operation="after-freeze")
        assert blocked.value.code == "ADMISSION_CLOSED"
        proceed.set()
        assert await task == {"value": 3, "pid": old.process.pid}
        assert (await replacement)["route_epoch"] == 2
        assert not old.alive
        assert (await invoke(kernel, operation="new-route"))["pid"] != old.process.pid
    finally:
        proceed.set()
        await asyncio.gather(task, *([replacement] if replacement else []), return_exceptions=True)
        kernel.store.freeze = actual_freeze


@pytest.mark.postgres
async def test_parent_cancellation_fences_child_write_and_cleans_entire_tree(
    tmp_path, running_kernel
):
    kernel = running_kernel
    target = package(
        tmp_path,
        "target",
        capabilities=["state.write"],
        code="""from forest.sdk import Plugin, service
class Component(Plugin):
    @service("target.run")
    async def run(self, payload, ctx):
        return await ctx.state.commit("value", payload["value"], expected_revision=0,
            operation_id=ctx.operations.stable("write:v1"))
""",
    )
    await kernel.install_many(
        [spec(package(tmp_path, "caller", requires=["target"])), spec(target)]
    )
    entered, proceed = asyncio.Event(), asyncio.Event()
    actual_commit = kernel.store.commit_state
    grants = []

    async def delayed(grant, *args, **kwargs):
        grants.append(grant)
        entered.set()
        await proceed.wait()
        return await actual_commit(grant, *args, **kwargs)

    kernel.store.commit_state = delayed
    task = asyncio.create_task(invoke(kernel))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        root = next(entry[1] for entry in kernel._calls.values() if entry[1].call_depth == 0)
        await kernel.cancel(root.call_id)
        proceed.set()
        with pytest.raises((ForestError, asyncio.CancelledError)):
            await task
        with pytest.raises(ForestError) as stale:
            await actual_commit(grants[0], "value", 0, 3, "stale-write", "sha256:synthetic")
        assert stale.value.code in {"CALL_CANCELLED", "CALL_NOT_ACTIVE", "CALL_CLOSED"}
        assert not await kernel.store.snapshot_state(kernel.lease, "target")
        assert not kernel._calls
    finally:
        proceed.set()
        await asyncio.gather(task, return_exceptions=True)
        kernel.store.commit_state = actual_commit


@pytest.mark.postgres
async def test_reverse_shutdown_and_lifecycle_grant_replay(tmp_path, running_kernel):
    kernel = running_kernel
    target = package(tmp_path, "target")
    await kernel.install_many(
        [
            spec(package(tmp_path, "caller", requires=["target"])),
            spec(target, operation_id="install-target"),
        ]
    )
    with pytest.raises(ForestError) as conflict:
        await kernel.install(
            target, grants=frozenset({"state.read"}), operation_id="install-target"
        )
    assert conflict.value.code == "IDEMPOTENCY_CONFLICT"
    retired = []
    actual_retire = kernel._retire

    async def retire(component, **kwargs):
        retired.append(component.snapshot.component["id"])
        await actual_retire(component, **kwargs)

    kernel._retire = retire
    await kernel.close()
    assert retired == ["caller", "target"]


@pytest.mark.postgres
async def test_nested_state_receipt_replay_and_json_isolation(tmp_path, running_kernel):
    kernel = running_kernel
    caller = package(
        tmp_path,
        "caller",
        requires=["target"],
        code="""from forest.sdk import Plugin, service
class Component(Plugin):
    @service("caller.run")
    async def run(self, payload, ctx):
        operation = ctx.operations.stable("same-child:v1")
        first = await ctx.services.call("target.run", payload, operation_id=operation)
        first["local_only"] = True
        second = await ctx.services.call("target.run", payload, operation_id=operation)
        return {"first": first, "second": second, "input": payload}
""",
    )
    target = package(
        tmp_path,
        "target",
        capabilities=["state.read", "state.write"],
        code="""from forest.sdk import Plugin, service
class Component(Plugin):
    @service("target.run")
    async def run(self, payload, ctx):
        current = await ctx.state.get("value")
        await ctx.state.commit("value", payload["value"], expected_revision=current["revision"],
            operation_id=ctx.operations.stable("write:v1"))
        payload["mutated_in_target"] = True
        return {"value": payload["value"]}
""",
    )
    await kernel.install_many([spec(caller), spec(target)])
    original = {"value": 7}
    result = await invoke(kernel, payload=original)
    assert result == {
        "first": {"value": 7, "local_only": True},
        "second": {"value": 7},
        "input": {"value": 7},
    }
    assert original == {"value": 7}
    saved = await kernel.store.snapshot_state(kernel.lease, "target")
    assert saved["value"]["revision"] == 1 and saved["value"]["value"] == 7
    async with kernel.store.pool.connection() as conn:
        rows = await (await conn.execute("SELECT status FROM bot_calls")).fetchall()
        generation = await (
            await conn.execute(
                "SELECT capability_grants FROM bot_generations WHERE component_id='target'"
            )
        ).fetchone()
    assert len(rows) == 2 and all(row["status"] == "COMPLETED" for row in rows)
    assert generation["capability_grants"] == ["state.read", "state.write"]


@pytest.mark.postgres
async def test_nested_shorter_deadline_returns_typed_error_and_allows_parent_recovery(
    tmp_path, running_kernel
):
    kernel = running_kernel
    caller = package(
        tmp_path,
        "caller",
        requires=["target"],
        code="""from forest.errors import ForestError
from forest.sdk import Plugin, service
class Component(Plugin):
    @service("caller.run")
    async def run(self, payload, ctx):
        try:
            await ctx.services.call("target.run", payload, timeout=0.1,
                operation_id=ctx.operations.stable("slow-child:v1"))
        except ForestError as error:
            return {"code": error.code}
""",
    )
    target = package(
        tmp_path,
        "target",
        code="""import asyncio
from forest.sdk import Plugin, service
class Component(Plugin):
    @service("target.run")
    async def run(self, payload, ctx):
        await asyncio.sleep(2)
        return payload
""",
    )
    await kernel.install_many([spec(caller), spec(target)])
    assert await invoke(kernel) == {"code": "DEADLINE_EXCEEDED"}
    assert not kernel._calls and not kernel._call_bindings and kernel._call_count == 0
    assert all(component.worker.alive for component in kernel.components.values())
    assert all(
        component.worker._admitted == 0
        and not component.worker._reserved_calls
        and not component.worker._invocation_tasks
        for component in kernel.components.values()
    )
    async with kernel.store.pool.connection() as conn:
        rows = await (
            await conn.execute("SELECT call_id,parent_call_id,call_depth,status FROM bot_calls ORDER BY call_depth")
        ).fetchall()
        assert (await (await conn.execute("SELECT count(*) AS n FROM bot_live_calls")).fetchone())["n"] == 0
    assert len(rows) in {1, 2}
    assert rows[0]["status"] == "COMPLETED" and rows[0]["call_depth"] == 0
    assert rows[0]["parent_call_id"] is None
    # The 100 ms child budget includes admission. Expiry before that transaction
    # commits leaves no child row; a committed child must have been failed.
    for child in rows[1:]:
        assert child["status"] == "FAILED" and child["call_depth"] == 1
        assert child["parent_call_id"] == rows[0]["call_id"]


@pytest.mark.postgres
async def test_batch_candidate_failure_reports_partial_receipts_without_replacing_old(
    tmp_path, running_kernel
):
    kernel = running_kernel
    await kernel.install(package(tmp_path, "old"))
    old = kernel.components["old"].worker
    caller = package(
        tmp_path, "caller", requires=["target"], code="raise RuntimeError('candidate failed')"
    )
    with pytest.raises(ForestError) as failed:
        await kernel.install_many([spec(caller), spec(package(tmp_path, "target"))])
    assert failed.value.details["failed_component"] == "caller"
    assert [row["component_id"] for row in failed.value.details["completed"]] == ["target"]
    assert set(kernel.components) == {"old", "target"} and old.alive
    assert (await invoke(kernel, "old"))["pid"] == old.process.pid

"""Real PostgreSQL and Worker coverage for durable component jobs."""

import asyncio
import json
import os
import shutil
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.types.json import Jsonb

from forest.dispatch_trace import DispatchTraceStore
from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.bot import BotStore
from forest.storage.component_jobs import ComponentJobStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def component_job_store():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a dedicated test PostgreSQL")
    name = "forest_component_jobs_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "component-job-bot")
    try:
        await store.open()
        await store.migrate()
        yield store
    finally:
        await store.close()
        assert name.startswith("forest_component_jobs_") and len(name) == 54
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def component_job_package(
    tmp_path: Path,
    *,
    version: str = "0.1.0",
    fail: bool = False,
    include_handler: bool = True,
    decorate_handler: bool = True,
    max_attempts: int = 3,
    max_concurrency: int = 1,
):
    source = tmp_path / f"worker-{version}-{uuid4().hex}"
    (source / "python").mkdir(parents=True)
    (source / "schemas").mkdir()
    capabilities = ["state.read", "state.write"]
    if include_handler:
        capabilities.append("job.schedule:work.persist")
    manifest = f'''manifest_version = 1
package_id = "test.component-jobs"
version = "{version}"
kernel_api = ">=1.0,<2.0"
runtime = "python-worker"
entrypoint = "implementation:Component"
[[components]]
id = "component.jobs"
scope = "bot"
state_schema = 1
reload_mode = "drain"
config_schema = "schemas/config.json"
[components.capabilities]
requests = {json.dumps(capabilities)}
[[components.provides]]
name = "component.jobs.schedule"
contract = "1.0"
input_schema = "schemas/input.json"
output_schema = "schemas/output.json"
'''
    if include_handler:
        manifest += f'''[[components.jobs]]
handler_id = "work.persist"
input_schema = "schemas/job.json"
max_attempts = {max_attempts}
timeout_s = 10
max_concurrency = {max_concurrency}
'''
    (source / "module.toml").write_text(manifest, encoding="utf-8")
    schemas = {
        "config": {"type": "object"},
        "input": {
            "type": "object", "required": ["value", "job_operation"],
            "properties": {
                "value": {"type": "integer"},
                "job_operation": {"type": "string"},
                "delay_seconds": {"type": "number"},
            },
            "additionalProperties": False,
        },
        "output": {},
        "job": {
            "type": "object", "required": ["value"],
            "properties": {"value": {"type": "integer"}},
            "additionalProperties": False,
        },
    }
    for key, value in schemas.items():
        (source / "schemas" / f"{key}.json").write_text(json.dumps(value), encoding="utf-8")
    handler = ""
    if decorate_handler:
        body = 'raise RuntimeError("private job failure")' if fail else '''current = await ctx.state.get("runs")
        value = current["value"] or []
        if task["job_id"] not in [item["job_id"] for item in value]:
            value.append({"job_id": task["job_id"], "value": task["payload"]["value"],
                          "version": %r})
            await ctx.state.commit(
                "runs", value, current["revision"],
                ctx.operations.stable("run:" + task["job_id"]),
            )
        return {"completed": task["job_id"]}''' % version
        handler = f'''    @job("work.persist")
    async def persist(self, task, ctx):
        {body}
'''
    code = f'''from forest.sdk import Plugin, job, service
class Component(Plugin):
    @service("component.jobs.schedule")
    async def schedule(self, payload, ctx):
        return await ctx.jobs.schedule(
            "work.persist", {{"value": payload["value"]}},
            operation_id=payload["job_operation"],
            delay_seconds=payload.get("delay_seconds", 0),
        )
{handler}'''
    (source / "python" / "implementation.py").write_text(code, encoding="utf-8")
    return PackageCatalog(tmp_path / "catalog").build(source)


async def wait_jobs(store, expected, *, timeout=15):
    async with asyncio.timeout(timeout):
        while True:
            rows = await ComponentJobStore(store).jobs()
            if expected(rows):
                return rows
            await asyncio.sleep(0.05)


async def test_job_schedule_is_convergent_and_lost_ack_does_not_repeat_handler(
    tmp_path, component_job_store, monkeypatch
):
    kernel = Kernel(component_job_store)
    await kernel.start()
    acknowledged = ComponentJobStore.acknowledge
    lost = False

    async def lose_first_ack(self, *args, **kwargs):
        nonlocal lost
        await acknowledged(self, *args, **kwargs)
        if not lost:
            lost = True
            raise RuntimeError("synthetic lost acknowledgement")

    monkeypatch.setattr(ComponentJobStore, "acknowledge", lose_first_ack)
    try:
        snapshot = component_job_package(tmp_path)
        grants = frozenset(snapshot.component["capabilities"]["requests"])
        await kernel.install(snapshot, grants=grants)
        first, duplicate = await asyncio.gather(
            kernel.invoke(
                "component.jobs", "component.jobs.schedule",
                {"value": 7, "job_operation": "same-job"}, operation_id="schedule-a",
            ),
            kernel.invoke(
                "component.jobs", "component.jobs.schedule",
                {"value": 7, "job_operation": "same-job"}, operation_id="schedule-b",
            ),
        )
        assert first["job_id"] == duplicate["job_id"]
        rows = await wait_jobs(
            component_job_store,
            lambda items: len(items) == 1 and items[0]["status"] == "ACKED",
        )
        assert rows[0]["attempt_count"] == 1
        state = await component_job_store.snapshot_state(kernel.lease, "component.jobs")
        assert state["runs"]["value"] == [
            {"job_id": first["job_id"], "value": 7, "version": "0.1.0"}
        ]
        async with component_job_store.pool.connection() as conn:
            source = await (
                await conn.execute(
                    "SELECT c.operation_id FROM public.bot_component_jobs j "
                    "JOIN public.bot_calls c ON (c.bot_id,c.call_id)=(j.bot_id,j.source_call_id)"
                )
            ).fetchone()
            entries, truncated = await DispatchTraceStore(
                component_job_store
            )._causal_entries(
                conn,
                {source["operation_id"]: {
                    "kind": "test_operation", "id": source["operation_id"]
                }},
                [],
            )
        assert truncated is False
        job = next(entry for entry in entries if entry["kind"] == "component_job")
        assert job["status"] == "ACKED"
        assert job["references"]["caused_by"]["kind"] == "kernel_call"
        worker = next(
            entry for entry in entries
            if entry["kind"] == "kernel_call"
            and entry["references"].get("caused_by", {}).get("kind") == "component_job"
        )
        assert worker["status"] == "COMPLETED"
        assert worker["generation_id"] == job["generation_id"]
    finally:
        await kernel.close()


async def test_pending_job_moves_to_compatible_reloaded_handler(tmp_path, component_job_store):
    kernel = Kernel(component_job_store)
    await kernel.start()
    try:
        failing = component_job_package(tmp_path, fail=True, max_attempts=8)
        grants = frozenset(failing.component["capabilities"]["requests"])
        await kernel.install(failing, grants=grants)
        receipt = await kernel.invoke(
            "component.jobs", "component.jobs.schedule",
            {"value": 9, "job_operation": "reload-job"}, operation_id="schedule-reload",
        )
        await wait_jobs(
            component_job_store,
            lambda items: len(items) == 1 and items[0]["status"] == "RETRY_WAIT",
        )
        replacement = component_job_package(tmp_path, version="0.2.0", max_attempts=8)
        await kernel.install(replacement, grants=grants)
        await wait_jobs(
            component_job_store,
            lambda items: len(items) == 1 and items[0]["status"] == "ACKED",
        )
        state = await component_job_store.snapshot_state(kernel.lease, "component.jobs")
        assert state["runs"]["value"] == [
            {"job_id": receipt["job_id"], "value": 9, "version": "0.2.0"}
        ]
    finally:
        await kernel.close()


async def test_job_manifest_mismatch_removal_and_capability_are_rejected(
    tmp_path, component_job_store
):
    kernel = Kernel(component_job_store)
    await kernel.start()
    try:
        mismatched = component_job_package(tmp_path, decorate_handler=False)
        grants = frozenset(mismatched.component["capabilities"]["requests"])
        with pytest.raises(ForestError) as mismatch:
            await kernel.install(mismatched, grants=grants)
        assert mismatch.value.code == "JOB_MANIFEST_MISMATCH"

        original = component_job_package(tmp_path, version="0.1.1")
        await kernel.install(original, grants=grants)
        worker = kernel.components["component.jobs"].worker
        removed = component_job_package(
            tmp_path, version="0.2.0", include_handler=False, decorate_handler=False
        )
        with pytest.raises(ForestError) as removal:
            await kernel.install(removed, grants=frozenset({"state.read", "state.write"}))
        assert removal.value.code == "JOB_HANDLER_REMOVAL_REQUIRES_MANAGEMENT"
        assert kernel.components["component.jobs"].worker is worker and worker.alive

        with pytest.raises(ForestError) as denied:
            await kernel.install(component_job_package(tmp_path, version="0.3.0"), grants=frozenset())
        assert denied.value.code == "CAPABILITY_DENIED"
    finally:
        await kernel.close()


async def test_serial_handler_claim_and_fenced_reload_do_not_spend_attempt(
    tmp_path, component_job_store
):
    kernel = Kernel(component_job_store)
    await kernel.start()
    try:
        original = component_job_package(tmp_path, max_attempts=1)
        grants = frozenset(original.component["capabilities"]["requests"])
        await kernel.install(original, grants=grants)
        kernel._component_job_dispatcher.cancel()
        await asyncio.gather(kernel._component_job_dispatcher, return_exceptions=True)
        first = await kernel.invoke(
            "component.jobs", "component.jobs.schedule",
            {"value": 1, "job_operation": "first"}, operation_id="schedule-first",
        )
        await kernel.invoke(
            "component.jobs", "component.jobs.schedule",
            {"value": 2, "job_operation": "second"}, operation_id="schedule-second",
        )
        jobs = ComponentJobStore(component_job_store)
        claimed = await jobs.claim(kernel.lease)
        assert claimed is not None and claimed["job_id"] == first["job_id"]
        assert await jobs.claim(kernel.lease) is None

        replacement = component_job_package(
            tmp_path, version="0.2.0", max_attempts=1
        )
        await kernel.install(replacement, grants=grants)
        await kernel._deliver_component_job(claimed)
        rows = await jobs.jobs()
        assert rows[0]["status"] == "RETRY_WAIT"
        assert rows[0]["attempt_count"] == 0
        assert rows[0]["dispatch_count"] == 0

        rerouted = None
        async with asyncio.timeout(2):
            while rerouted is None:
                rerouted = await jobs.claim(kernel.lease)
                if rerouted is None:
                    await asyncio.sleep(0.05)
        assert rerouted is not None
        assert rerouted["job_id"] == first["job_id"]
        assert rerouted["target_generation_id"] != claimed["target_generation_id"]
        await kernel._deliver_component_job(rerouted)
        rows = await jobs.jobs()
        assert rows[0]["status"] == "ACKED"
        second = await jobs.claim(kernel.lease)
        assert second is not None and second["handler_id"] == "work.persist"
    finally:
        await kernel.close()


async def test_job_delay_schema_and_operation_conflicts_are_enforced(
    tmp_path, component_job_store
):
    kernel = Kernel(component_job_store)
    await kernel.start()
    try:
        snapshot = component_job_package(tmp_path)
        grants = frozenset(snapshot.component["capabilities"]["requests"])
        await kernel.install(snapshot, grants=grants)
        receipt = await kernel.invoke(
            "component.jobs", "component.jobs.schedule",
            {"value": 3, "job_operation": "delayed", "delay_seconds": 30},
            operation_id="schedule-delayed",
        )
        await asyncio.sleep(0.6)
        rows = await ComponentJobStore(component_job_store).jobs()
        assert rows[0]["job_id"] == receipt["job_id"] and rows[0]["status"] == "PENDING"

        with pytest.raises(ForestError) as conflict:
            await kernel.invoke(
                "component.jobs", "component.jobs.schedule",
                {"value": 4, "job_operation": "delayed", "delay_seconds": 30},
                operation_id="schedule-conflict",
            )
        assert conflict.value.code == "IDEMPOTENCY_CONFLICT"

        changed = component_job_package(tmp_path, version="0.2.0")
        schema_path = changed.root / "schemas" / "job.json"
        # Package snapshots are immutable, so build another package with a new
        # version and a changed schema through its source tree.
        source = tmp_path / "changed-schema"
        shutil.copytree(changed.root, source)
        manifest = (source / "module.toml").read_text(encoding="utf-8")
        manifest = manifest.replace('version = "0.2.0"', 'version = "0.3.0"')
        (source / "module.toml").write_text(manifest, encoding="utf-8")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        schema["properties"]["label"] = {"type": "string"}
        (source / "schemas" / "job.json").write_text(json.dumps(schema), encoding="utf-8")
        incompatible = PackageCatalog(tmp_path / "catalog").build(source)
        with pytest.raises(ForestError) as identity:
            await kernel.install(incompatible, grants=grants)
        assert identity.value.code == "JOB_HANDLER_IDENTITY_CONFLICT"
    finally:
        await kernel.close()


async def test_component_job_management_permission_upgrade_preserves_least_privilege(
    component_job_store,
):
    scopes = {
        "readonly": ["message.read"],
        "config-reader": ["config.read"],
        "replay": ["message.read", "service.invoke"],
        "planner": ["config.read", "config.plan"],
        "apply-without-cancel": ["message.read", "config.apply"],
        "full": [
            "message.read",
            "service.invoke",
            "config.plan",
            "config.apply",
            "message.cancel",
        ],
        "empty": [],
    }
    async with component_job_store.pool.connection() as conn, conn.transaction():
        for issuer, actions in scopes.items():
            await conn.execute(
                "INSERT INTO public.bot_web_issuers("
                "bot_id,issuer_id,key_revision,public_key,actions) VALUES (%s,%s,1,%s,%s)",
                (
                    component_job_store.bot_id,
                    issuer,
                    "component-job-permission-test-key",
                    Jsonb(actions),
                ),
            )
        source = (
            files("forest.storage")
            .joinpath("bot_migrations", "057_component_job_management.sql")
            .read_text(encoding="utf-8")
        )
        permission_upgrade = "UPDATE public.bot_web_issuers" + source.split(
            "UPDATE public.bot_web_issuers", 1
        )[1]
        await conn.execute(permission_upgrade)
        rows = await (
            await conn.execute(
                "SELECT issuer_id,actions FROM public.bot_web_issuers "
                "WHERE issuer_id=ANY(%s)",
                (list(scopes),),
            )
        ).fetchall()
    actual = {row["issuer_id"]: set(row["actions"]) for row in rows}
    assert actual["readonly"] == {"message.read", "component_job.read"}
    assert actual["config-reader"] == {"config.read", "component_job.read"}
    assert actual["replay"] == {
        "message.read",
        "service.invoke",
        "component_job.read",
        "component_job.replay",
    }
    assert actual["planner"] == {
        "config.read",
        "config.plan",
        "component_job.read",
        "component_job.retire.plan",
    }
    assert actual["apply-without-cancel"] == {
        "message.read",
        "config.apply",
        "component_job.read",
    }
    assert actual["full"] == set(scopes["full"]) | {
        "component_job.read",
        "component_job.replay",
        "component_job.retire.plan",
        "component_job.retire.apply",
    }
    assert actual["empty"] == set()

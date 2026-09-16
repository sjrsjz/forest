"""Archive admission -> durable registry -> real Bot Worker -> version recovery."""

import asyncio
import io
import re
import secrets
import shutil
import threading
import zipfile
from pathlib import Path

import pytest
from test_web_bots import (
    ORIGIN,
    apply,
    effective,
    invoke,
    login,
    make_plan,
    post,
    ready,
    running,
    setup,
)
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings
from forest.packages import PackageCatalog

pytestmark = pytest.mark.postgres
BUILTIN = Path(__file__).resolve().parents[1] / "src/forest/builtin/counter"


def archive(tmp_path, version, *, broken=False, large=False, reserved=False):
    source = tmp_path / version
    shutil.copytree(BUILTIN, source)
    manifest = (source / "module.toml").read_text()
    if not reserved:
        manifest = manifest.replace("forest.counter", "demo.counter")
    manifest = re.sub(r'(?m)^version = "[^"]+"$', f'version = "{version}"', manifest, count=1)
    (source / "module.toml").write_text(manifest)
    if broken:
        code = source / "python/counter.py"
        code.write_text(code.read_text() + "\nraise RuntimeError('unhealthy package')\n")
    if large:
        (source / "resource.txt").write_text(secrets.token_hex(150_000))
    catalog = PackageCatalog(tmp_path / "export")
    built = catalog.build(source)
    return catalog.export_archive(built.digest)


async def upload(client, data, key, *, status=201, headers=None):
    response = await client.post("/management/v1/packages", content=data,
        headers={"content-type": "application/zip", "idempotency-key": key, **(headers or {})})
    assert response.status_code == status, response.text
    return response.json()


async def test_web_package_upgrade_failure_history_restore_and_restart(tmp_path, web_bots_database):
    data_dir = tmp_path / "state"
    token = BootstrapState(data_dir).initialize()
    settings = WebSettings(data_dir, ORIGIN, managed_dsn=web_bots_database)
    good = archive(tmp_path, "1.0.0", large=True)
    bad = archive(tmp_path, "2.0.0", broken=True)
    new = archive(tmp_path, "3.0.0")
    assert len(good) > 65536
    async with running(settings) as (app, client):
        await setup(client, token)
        first = await upload(client, good, "import-first")
        assert await upload(client, good, "import-first") == first
        broken = await upload(client, bad, "import-bad")
        third = await upload(client, new, "import-new")
        assert not app.state.runtime.fleet.bots
        exported = await client.get(f"/management/v1/packages/{first['package_digest']}/archive")
        assert exported.status_code == 200
        imported = PackageCatalog(tmp_path / "roundtrip").import_archive(exported.content)
        assert imported.digest == first["package_digest"]
        bot = await post(client, "/management/v1/bots", {"display_name": "Packages"}, "bot", status=202)
        await ready(client, bot)
        base = f"/management/v1/bots/{bot['bot_id']}/components/demo.counter"
        await apply(client, base, await make_plan(client, base, first, "Original", 2, 0, "p1"), "a1")
        await effective(client, base, 1)
        runtime = (await client.get(f"/management/v1/bots/{bot['bot_id']}")).json()["runtime"]
        modules = {row["component_id"]: row for row in runtime["runtime_modules"]}
        assert set(modules) == {
            "adapter.onebot",
            "agent.runtime",
            "command.router",
            "conversation.scheduler",
            "memory.graph",
            "message.router",
            "routing.graph",
            "scheduler.jobs",
            "shell.command",
        }
        assert modules["memory.graph"]["requires"] == ["adapter.onebot"]
        assert modules["agent.runtime"]["requires"] == ["adapter.onebot", "memory.graph"]
        assert modules["conversation.scheduler"]["requires"] == [
            "adapter.onebot", "agent.runtime", "command.router", "message.router"
        ]
        assert modules["command.router"]["requires"] == ["routing.graph"]
        assert modules["message.router"]["requires"] == ["routing.graph"]
        assert modules["routing.graph"]["management_contract"]["operations"] == [
            {"operation_id": "snapshot", "kind": "query", "authority": "module.read"},
            {
                "operation_id": "snapshot-version",
                "kind": "query",
                "authority": "module.read",
            },
            {
                "operation_id": "dispatch-traces",
                "kind": "query",
                "authority": "module.read",
            },
        ]
        assert modules["agent.runtime"]["management_view"]["kind"] == "trusted-module"
        assert modules["agent.runtime"]["management_view"]["protocol"] == (
            "forest.trusted-management-view/1"
        )
        assert modules["memory.graph"]["management_view"]["template"].endswith(
            "/memory.graph.html"
        )
        assert modules["memory.graph"]["management_view"]["style"].endswith(
            "/memory.graph.css"
        )
        assert modules["conversation.scheduler"]["management_view"] is None
        assert modules["agent.runtime"]["management_contract"]["protocol"] == (
            "forest.module-management/1"
        )
        agent_operations = {
            row["operation_id"]: row
            for row in modules["agent.runtime"]["management_contract"]["operations"]
        }
        assert agent_operations["configuration.plan"] == {
            "operation_id": "configuration.plan",
            "kind": "action",
            "authority": "agent.config.plan",
        }
        assert modules["conversation.scheduler"]["management_contract"] is None
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/agent/configuration")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/memory/configuration")
        ).status_code == 404
        assert (await invoke(client, base, 2, "call-1"))["result"]["value"] == 4
        graph_response = await client.get(
            f"/management/v1/bots/{bot['bot_id']}/runtime-modules/routing.graph/queries/snapshot"
        )
        assert graph_response.status_code == 200, graph_response.text
        graph = graph_response.json()
        assert graph["protocol"] == "forest.route-graph/1"
        assert any(
            node["kind"] == "service"
            and node["component_id"] == "demo.counter"
            and node["service_name"] == "counter.increment"
            for node in graph["nodes"]
        )
        trace_response = await client.get(
            f"/management/v1/bots/{bot['bot_id']}/runtime-modules/"
            "routing.graph/queries/dispatch-traces?limit=5"
        )
        assert trace_response.status_code == 200, trace_response.text
        assert trace_response.json() == {"filter": {"limit": 5}, "traces": []}
        await apply(client, base, await make_plan(client, base, broken, "Broken", 9, 1, "p2"), "a2")
        # A failed candidate must leave the old durable effective revision/Worker live.
        async with asyncio.timeout(30):
            while True:
                state = (await client.get(base + "/configuration")).json()
                if any(op["phase"] in {"FAILED", "ABANDONED"} for op in state["operations"]):
                    break
                if any(row["error_code"] for row in state["runtime"]["components"]):
                    break
                await asyncio.sleep(0.1)
        assert state["desired_revision"] == 2 and state["effective_revision"] == 1
        assert (await invoke(client, base, 1, "call-old"))["result"]["value"] == 6
        await apply(client, base, await make_plan(client, base, third, "New", 5, 2, "p3"), "a3")
        await effective(client, base, 3)
        assert (await invoke(client, base, 1, "call-new"))["result"]["value"] == 11
        history = (await client.get(base + "/configuration/history")).json()
        assert [r["revision"] for r in history["revisions"]] == [3, 2, 1]
        assert history["next_before"] is None
        older = (await client.get(base + "/configuration/history?before=2")).json()
        assert [r["revision"] for r in older["revisions"]] == [1]
        restored = await post(client, base + "/change-plans", {
            "expected_revision": 3, "values": history["revisions"][-1]["values"],
        }, "restore-plan")
        await apply(client, base, restored, "restore")
        await effective(client, base, 4)
        assert (await invoke(client, base, 1, "call-restored"))["result"]["value"] == 13
    async with running(settings) as (app, client):
        assert app.state.runtime.store is not None, app.state.runtime.recovery_error
        await login(client)
        packages = (await client.get("/management/v1/packages")).json()["packages"]
        assert {p["version"] for p in packages if p["package_id"] == "demo.counter"} == {"1.0.0", "2.0.0", "3.0.0"}
        await effective(client, base, 4)
        assert (await invoke(client, base, 1, "after-restart"))["result"]["value"] == 15


async def test_package_endpoint_auth_and_static_admission(tmp_path, web_bots_database):
    data_dir = tmp_path / "state"
    token = BootstrapState(data_dir).initialize()
    settings = WebSettings(data_dir, ORIGIN, managed_dsn=web_bots_database)
    good = archive(tmp_path, "1.0.0")
    async with running(settings) as (_, client):
        await upload(client, good, "no-session", status=401)
        await setup(client, token)
        builtin = next(
            package for package in (await client.get("/management/v1/packages")).json()["packages"]
            if package["package_id"] == "forest.counter"
        )
        assert builtin["management_ui"]["protocol"] == "forest.management-ui/1"
        ui = await client.get(
            f"/management/v1/packages/{builtin['package_digest']}/management-ui"
        )
        assert ui.status_code == 200
        assert ui.headers["x-forest-management-ui-digest"] == builtin["management_ui"]["sha256"]
        assert "connect-src 'none'" in ui.headers["content-security-policy"]
        assert "forest.management-ui/1" in ui.text
        await upload(client, good, "wrong-origin", status=403, headers={"origin": "https://untrusted.invalid"})
        await upload(client, good, "wrong-csrf", status=403, headers={"x-csrf-token": "wrong"})
        await upload(client, good, "wrong-type", status=415, headers={"content-type": "application/json"})
        result = await upload(client, archive(tmp_path, "4.0.0", reserved=True), "reserved", status=422)
        assert result["code"] == "PACKAGE_RESERVED"
        unsafe = io.BytesIO()
        with zipfile.ZipFile(unsafe, "w") as output:
            output.writestr("../escape.py", "pass")
        await upload(client, unsafe.getvalue(), "traversal", status=422)
        await upload(client, good, "one-key")
        result = await upload(client, archive(tmp_path, "2.0.0"), "one-key", status=409)
        assert result["code"] == "IDEMPOTENCY_CONFLICT"
        listing = (await client.get("/management/v1/packages")).json()["packages"]
        assert [p["version"] for p in listing if p["package_id"] == "demo.counter"] == ["1.0.0"]
        await upload(client, b"x" * (32 * 1024 * 1024 + 1), "too-large", status=413)


async def test_restart_restores_last_effective_while_new_desired_is_broken(tmp_path, web_bots_database):
    data_dir = tmp_path / "state"
    token = BootstrapState(data_dir).initialize()
    settings = WebSettings(data_dir, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (_, client):
        await setup(client, token)
        first = await upload(client, archive(tmp_path, "1.0.0"), "first")
        broken = await upload(client, archive(tmp_path, "2.0.0", broken=True), "broken")
        bot = await post(client, "/management/v1/bots", {"display_name": "Recovery"}, "bot", status=202)
        await ready(client, bot)
        base = f"/management/v1/bots/{bot['bot_id']}/components/demo.counter"
        await apply(client, base, await make_plan(client, base, first, "Old", 2, 0, "p1"), "a1")
        await effective(client, base, 1)
        assert (await invoke(client, base, 3, "before"))["result"]["value"] == 6
        await apply(client, base, await make_plan(client, base, broken, "Bad", 9, 1, "p2"), "a2")
    async with running(settings) as (_, client):
        await login(client)
        async with asyncio.timeout(30):
            while True:
                state = (await client.get(base + "/configuration")).json()
                if any(row["loaded_revision"] == 1 and row["worker_state"] == "ACTIVE"
                       for row in state["runtime"]["components"]):
                    break
                await asyncio.sleep(0.1)
        assert state["desired_revision"] == 2 and state["effective_revision"] == 1
        assert (await invoke(client, base, 1, "after"))["result"]["value"] == 8
        assert any(op["phase"] == "COMMITTED" for op in state["operations"])


async def test_revocation_during_import_does_not_pin_rejected_version(tmp_path, web_bots_database, monkeypatch):
    data_dir = tmp_path / "state"
    token = BootstrapState(data_dir).initialize()
    settings = WebSettings(data_dir, ORIGIN, managed_dsn=web_bots_database)
    rejected = archive(tmp_path, "1.0.0", broken=True)
    # Same formal identity, different complete content after the rejected request.
    accepted = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(rejected)) as source, zipfile.ZipFile(accepted, "w") as target:
        for name in source.namelist():
            content = source.read(name)
            if name == "python/counter.py":
                content = content.replace(b"raise RuntimeError('unhealthy package')", b"# valid")
            target.writestr(name, content)
    entered, release = threading.Event(), threading.Event()
    async with running(settings) as (app, client):
        await setup(client, token)
        artifacts = app.state.runtime.fleet.artifacts
        original = artifacts.import_archive

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            entered.set()
            assert release.wait(15)
            return result

        monkeypatch.setattr(artifacts, "import_archive", blocked)
        pending = asyncio.create_task(upload(client, rejected, "rejected", status=401))
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            assert (await client.post("/management/v1/logout", json={})).status_code == 200
        finally:
            release.set()
        await pending
        monkeypatch.setattr(artifacts, "import_archive", original)
        await login(client)
        result = await upload(client, accepted.getvalue(), "accepted")
        assert result["version"] == "1.0.0"

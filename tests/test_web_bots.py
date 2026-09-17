"""HTTP -> catalog -> private Bot DB -> real Worker, including app restart."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings, create_app

ORIGIN = "http://testserver"
PASSWORD = "web-bot-test-only-password"


@pytest_asyncio.fixture
async def web_bots_database():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must select a private PostgreSQL test instance")
    database = "forest_web_bots_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row)
    await admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(database)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + database, "", ""))
    try:
        yield uri
    finally:
        # Every target is allocated by this one disposable deployment, and is
        # checked against its complete Bot UUID before destructive test cleanup.
        async with await psycopg.AsyncConnection.connect(uri, autocommit=True, row_factory=dict_row) as conn:
            exists = await (await conn.execute("SELECT to_regclass('bot_storage_plans') AS present")).fetchone()
            rows = [] if not exists["present"] else await (await conn.execute(
                "SELECT p.* FROM bot_storage_plans p JOIN bot_catalog c USING(bot_id) WHERE c.mode='managed'"
            )).fetchall()
        for row in rows:
            suffix = row["bot_id"].replace("-", "")
            assert len(suffix) == 32 and int(suffix, 16) >= 0
            assert row["database_name"] == "forest_b_" + suffix
            assert row["role_name"] == "forest_r_" + suffix
            assert row["database_name"] != conninfo_to_dict(dsn)["dbname"]
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(row["database_name"])))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(row["role_name"])))
        assert database.startswith("forest_web_bots_") and len(database) == 48
        assert database != conninfo_to_dict(dsn)["dbname"]
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        await admin.close()


@asynccontextmanager
async def running(settings):
    app = create_app(settings)
    async with app.router.lifespan_context(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=ORIGIN, headers={"origin": ORIGIN},
    ) as client:
        yield app, client


async def login(client):
    result = await client.post("/management/v1/login", json={"username": "admin", "password": PASSWORD})
    assert result.status_code == 200, result.text
    client.headers["x-csrf-token"] = client.cookies["forest_csrf"]


async def setup(client, token):
    result = await client.post("/management/v1/setup", json={
        "token": token, "username": "admin", "password": PASSWORD,
    })
    assert result.status_code == 200, result.text
    await login(client)


async def bot_catalog_root(client):
    session = await client.get("/management/v1/session")
    assert session.status_code == 200, session.text
    deployment_id = session.json()["deployment_id"]
    return (
        f"/management/v1/scopes/deployment/{deployment_id}/modules/"
        "deployment.bot-catalog"
    )


async def catalog_query(client, operation, params=None):
    root = await bot_catalog_root(client)
    response = await client.get(root + "/queries/" + operation, params=params)
    assert response.status_code == 200, response.text
    return response.json()


async def catalog_action(client, operation, body, key, *, status=200):
    root = await bot_catalog_root(client)
    response = await client.post(
        root + "/actions/" + operation,
        json=body,
        headers={"idempotency-key": key},
    )
    assert response.status_code == status, response.text
    return response.json()


async def package_registry_root(client):
    session = await client.get("/management/v1/session")
    assert session.status_code == 200, session.text
    return (
        f"/management/v1/scopes/deployment/{session.json()['deployment_id']}/modules/"
        "deployment.package-registry"
    )


async def package_query(client):
    response = await client.get(
        await package_registry_root(client) + "/queries/packages"
    )
    assert response.status_code == 200, response.text
    return response.json()


async def post(client, url, body, key, *, status=200):
    if url == "/management/v1/bots":
        url = await bot_catalog_root(client) + "/actions/bot.create"
    response = await client.post(url, json=body, headers={"idempotency-key": key})
    assert response.status_code == status, response.text
    return response.json()


async def ready(client, bot):
    async with asyncio.timeout(35):
        while True:
            job = await catalog_query(
                client, "provision-job", {"job_id": bot["job_id"]}
            )
            assert job["stage"] != "FAILED", job
            if job["stage"] == "READY":
                return job
            await asyncio.sleep(0.1)


async def effective(client, base, revision):
    result = None
    try:
        async with asyncio.timeout(40):
            while True:
                response = await client.get(base + "/configuration")
                assert response.status_code == 200, response.text
                result = response.json()
                loaded = any(row["loaded_revision"] == revision and not row["error_code"]
                             for row in result["runtime"]["components"])
                if result["effective_revision"] == revision and loaded:
                    return result
                await asyncio.sleep(0.1)
    except TimeoutError:
        pytest.fail(f"Configuration did not become effective: {result}")


async def module_effective(client, base, state_revision, enabled, config_revision):
    result = None
    try:
        async with asyncio.timeout(40):
            while True:
                response = await client.get(base + "/configuration")
                assert response.status_code == 200, response.text
                result = response.json()
                loaded = any(
                    row["component_id"] == "forest.counter" and not row["error_code"]
                    for row in result["runtime"]["components"]
                )
                if (
                    result["desired_state_revision"] == state_revision
                    and result["effective_state_revision"] == state_revision
                    and result["desired_enabled"] is enabled
                    and result["effective_enabled"] is enabled
                    and result["effective_revision"] == config_revision
                    and loaded is enabled
                ):
                    return result
                await asyncio.sleep(0.1)
    except TimeoutError:
        pytest.fail(f"Module state did not become effective: {result}")


async def make_plan(client, base, package, label, multiplier, revision, key):
    return await post(client, base + "/change-plans", {
        "expected_revision": revision,
        "values": {"package_digest": package["package_digest"], "grants": package["grants"],
                   "config": {"label": label, "multiplier": multiplier}},
    }, key)


async def apply(client, base, plan, key, *, status=202):
    return await post(client, base + "/management-requests",
                      {k: plan[k] for k in ("plan_id", "plan_digest", "expected_revision")}, key,
                      status=status)


async def invoke(client, base, increment, key, *, status=200):
    return await post(client, base + "/invoke",
                      {"service": "counter.increment", "payload": {"increment": increment}}, key,
                      status=status)


async def persisted_configuration(app, bot_id, expected):
    """Real publication succeeds using payloads while legacy columns stay NULL."""
    bot = app.state.runtime.fleet.bots[bot_id]
    async with bot.store.pool.connection() as conn:
        generation = await (await conn.execute(
            "SELECT g.config,g.state_value_schema,g.config_payload_id,"
            "public.forest_generation_configuration(g.bot_id,g.generation_id) AS actual_config "
            "FROM public.bot_generations g JOIN public.bot_component_routes r "
            "ON r.bot_id=g.bot_id AND r.generation_id=g.generation_id "
            "WHERE r.bot_id=%s AND r.component_id='forest.counter'", (bot_id,),
        )).fetchone()
    assert generation["config"] is None and generation["state_value_schema"] is None
    assert generation["config_payload_id"] and generation["actual_config"] == expected


@pytest.mark.postgres
async def test_two_bots_configure_run_reload_and_restart_over_http(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        created = [await post(client, "/management/v1/bots", {"display_name": name}, key, status=202)
                   for name, key in (("Bot Alpha", "create-alpha"), ("Bot Beta", "create-beta"))]
        assert created[0]["bot_id"] != created[1]["bot_id"]
        repeated_create = await post(client, "/management/v1/bots", {"display_name": "Bot Alpha"},
                                     "create-alpha", status=202)
        assert repeated_create == created[0]
        await asyncio.gather(*(ready(client, bot) for bot in created))
        listing = await catalog_query(client, "bots")
        assert len(listing["bots"]) == 2 and listing["managed_available"]
        beta = await catalog_query(client, "bot", {"bot_id": created[1]["bot_id"]})
        renamed = await catalog_action(
            client,
            "bot.rename",
            {
                "bot_id": beta["bot_id"],
                "display_name": "Bot Beta renamed",
                "expected_revision": beta["revision"],
            },
            "rename-beta",
        )
        assert renamed["display_name"] == "Bot Beta renamed"
        assert await catalog_action(
            client,
            "bot.rename",
            {
                "bot_id": beta["bot_id"],
                "display_name": "Bot Beta renamed",
                "expected_revision": beta["revision"],
            },
            "rename-beta",
        ) == renamed
        assert (await client.get("/management/v1/bots")).status_code == 404
        assert (
            await client.get(f"/management/v1/provision-jobs/{created[0]['job_id']}")
        ).status_code == 404
        packages = (await package_query(client))["packages"]
        package = next(item for item in packages if item["component_id"] == "forest.counter")
        bases = [f"/management/v1/bots/{bot['bot_id']}/components/forest.counter" for bot in created]
        plans = [await make_plan(client, base, package, label, multiplier, 0, "initial-plan")
                 for base, label, multiplier in zip(bases, ("Alpha", "Beta"), (2, 3), strict=True)]
        accepted = [await apply(client, base, plan, "initial-apply")
                    for base, plan in zip(bases, plans, strict=True)]
        assert all(item["desired_revision"] == 1 for item in accepted)
        await asyncio.gather(*(effective(client, base, 1) for base in bases))
        initial = [await invoke(client, base, 3, "first-increment") for base in bases]
        assert initial[0]["result"] == {"value": 6, "revision": 1, "label": "Alpha"}
        assert initial[1]["result"] == {"value": 9, "revision": 1, "label": "Beta"}

        runtime_root = (
            f"/management/v1/scopes/bot/{created[0]['bot_id']}/modules/kernel.runtime"
        )
        runtime_path = runtime_root + "/queries/runtime"
        runtime = (await client.get(runtime_path)).json()
        assert runtime["data_admission"] and runtime["maintenance_epoch"] == 1
        ingress = runtime["supervisor"]["ingress"]
        assert ingress["lanes"] == {
            lane: {"active": 0, "accepted": 0, "overloaded": 0}
            for lane in ("ORDINARY", "PRIVILEGED_CONTROL", "REJECTION")
        }
        assert ingress["qualification_cache"] == {"hits": 0, "misses": 0, "entries": 0}
        maintenance_path = runtime_root + "/actions/maintenance"
        paused = await post(client, maintenance_path, {
            "expected_maintenance_epoch": 1, "data_admission": False,
            "reason": "provider maintenance",
        }, "pause-alpha", status=202)
        assert paused["maintenance_epoch"] == 2 and not paused["data_admission"]
        assert await post(client, maintenance_path, {
            "expected_maintenance_epoch": 1, "data_admission": False,
            "reason": "provider maintenance",
        }, "pause-alpha", status=202) == paused
        conflict = await post(client, maintenance_path, {
            "expected_maintenance_epoch": 1, "data_admission": True,
            "reason": "stale browser tab",
        }, "stale-resume", status=409)
        assert conflict["code"] == "MAINTENANCE_CONFLICT"
        runtime = (await client.get(runtime_path)).json()
        assert not runtime["data_admission"] and runtime["maintenance_epoch"] == 2
        restored = await post(client, maintenance_path, {
            "expected_maintenance_epoch": 2, "data_admission": True,
            "reason": "provider recovered",
        }, "resume-alpha", status=202)
        assert restored["maintenance_epoch"] == 3 and restored["data_admission"]
        assert (
            await client.get(f"/management/v1/bots/{created[0]['bot_id']}/runtime")
        ).status_code == 404
        assert (
            await client.post(f"/management/v1/bots/{created[0]['bot_id']}/maintenance")
        ).status_code == 404

        # Two editor tabs both preview the same target revision. Only the first
        # accepted plan advances desired; the second gets a concrete CAS conflict.
        next_plan, rival = await asyncio.gather(
            make_plan(client, bases[0], package, "Alpha v2", 10, 1, "tab-one-plan"),
            make_plan(client, bases[0], package, "Rival", 100, 1, "tab-two-plan"),
        )
        applied = await apply(client, bases[0], next_plan, "tab-one-apply")
        assert await apply(client, bases[0], next_plan, "tab-one-apply") == applied
        conflict = await apply(client, bases[0], rival, "tab-two-apply", status=409)
        assert conflict["code"] == "REVISION_CONFLICT"
        now = await effective(client, bases[0], 2)
        assert now["effective_values"]["config"] == {"label": "Alpha v2", "multiplier": 10}
        await persisted_configuration(app, created[0]["bot_id"], now["effective_values"]["config"])
        changed = await invoke(client, bases[0], 1, "after-reload")
        assert changed["result"] == {"value": 16, "revision": 2, "label": "Alpha v2"}
        other = await invoke(client, bases[1], 1, "after-reload")
        assert other["result"] == {"value": 12, "revision": 2, "label": "Beta"}
        # Historical result replay is independent of the currently loaded config.
        assert await invoke(client, bases[0], 3, "first-increment") == initial[0]
        mismatch = await invoke(client, bases[0], 99, "after-reload", status=409)
        assert mismatch["code"] == "IDEMPOTENCY_CONFLICT"
        assert await apply(client, bases[0], plans[0], "initial-apply") == accepted[0]

    # New ASGI application, supervisor, host incarnations and Worker processes;
    # only the same encrypted deployment state and dedicated databases survive.
    async with running(settings) as (app, client):
        await login(client)
        listed = (await catalog_query(client, "bots"))["bots"]
        assert {row["bot_id"] for row in listed} == {bot["bot_id"] for bot in created}
        await asyncio.gather(*(effective(client, base, revision)
                               for base, revision in zip(bases, (2, 1), strict=True)))
        await persisted_configuration(app, created[0]["bot_id"], {"label": "Alpha v2", "multiplier": 10})
        assert await invoke(client, bases[0], 1, "after-reload") == changed
        assert await apply(client, bases[0], next_plan, "tab-one-apply") == applied
        values = [await invoke(client, base, 1, "after-restart") for base in bases]
        assert values[0]["result"] == {"value": 26, "revision": 3, "label": "Alpha v2"}
        assert values[1]["result"] == {"value": 15, "revision": 3, "label": "Beta"}


@pytest.mark.postgres
async def test_web_module_validate_disable_edit_and_enable_is_atomic(
    tmp_path, web_bots_database
):
    data = tmp_path / "module-state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (_, client):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Module lifecycle"},
            "module-create", status=202,
        )
        await ready(client, bot)
        package = next(
            row for row in (await package_query(client))["packages"]
            if row["component_id"] == "forest.counter"
        )
        base = f"/management/v1/bots/{bot['bot_id']}/components/forest.counter"
        plan = await make_plan(client, base, package, "Before disable", 1, 0, "module-plan")
        await apply(client, base, plan, "module-apply")
        await effective(client, base, 1)
        assert (await invoke(client, base, 2, "module-before"))["result"]["value"] == 2

        module = (await client.get(base + "/module")).json()["modules"][0]
        assert module["state"] == "ACTIVE" and module["admission"]
        runtime_root = f"/management/v1/scopes/bot/{bot['bot_id']}/modules/kernel.runtime"
        runtime_path = runtime_root + "/queries/runtime"
        maintenance_path = runtime_root + "/actions/maintenance"
        runtime = (await client.get(runtime_path)).json()
        await post(client, maintenance_path, {
            "expected_maintenance_epoch": runtime["maintenance_epoch"],
            "data_admission": False,
            "reason": "module projection check",
        }, "module-pause", status=202)
        paused_module = (await client.get(base + "/module")).json()["modules"][0]
        assert paused_module["state"] == "ACTIVE" and paused_module["admission"], paused_module
        paused_runtime = (await client.get(runtime_path)).json()
        await post(client, maintenance_path, {
            "expected_maintenance_epoch": paused_runtime["maintenance_epoch"],
            "data_admission": True,
            "reason": "module projection check complete",
        }, "module-resume", status=202)

        validated = await post(client, base + "/module/validate", {
            "expected_config_revision": 1, "expected_state_revision": 0,
        }, "ignored-read-key")
        assert validated["status"] == "VALID" and validated["required_components"] == []

        disabled = await post(client, base + "/module/state", {
            "expected_state_revision": 0, "enabled": False, "reason": "isolate failure",
        }, "module-disable", status=202)
        assert await post(client, base + "/module/state", {
            "expected_state_revision": 0, "enabled": False, "reason": "isolate failure",
        }, "module-disable", status=202) == disabled
        assert disabled["desired_state_revision"] == 1
        await module_effective(client, base, 1, False, 1)

        next_plan = await make_plan(
            client, base, package, "While disabled", 2, 1, "disabled-config-plan"
        )
        await apply(client, base, next_plan, "disabled-config-apply")
        await asyncio.sleep(0.25)
        pending = (await client.get(base + "/configuration")).json()
        assert pending["desired_revision"] == 2 and pending["effective_revision"] == 1
        assert not pending["effective_enabled"]

        enabled = await post(client, base + "/module/state", {
            "expected_state_revision": 1, "enabled": True, "reason": "validated recovery",
        }, "module-enable", status=202)
        assert enabled["desired_state_revision"] == 2
        final = await module_effective(client, base, 2, True, 2)
        assert final["effective_values"]["config"]["label"] == "While disabled"
        after = await invoke(client, base, 1, "module-after")
        assert after["result"] == {"value": 4, "revision": 2, "label": "While disabled"}


@pytest.mark.postgres
async def test_bot_endpoints_reject_csrf_cross_target_plan_and_revoked_replay(tmp_path, web_bots_database):
    data = tmp_path / "state"
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    token = BootstrapState(data).initialize()
    async with running(settings) as (_, client):
        assert (await client.get("/management/v1/bots")).status_code == 404
        await setup(client, token)
        catalog = await bot_catalog_root(client)
        rejected = await client.post(
            catalog + "/actions/bot.create", json={"display_name": "Blocked"},
            headers={"x-csrf-token": "wrong", "idempotency-key": "blocked"},
        )
        assert rejected.status_code == 403
        bots = [await post(client, "/management/v1/bots", {"display_name": key}, key, status=202)
                for key in ("first", "second")]
        await asyncio.gather(*(ready(client, bot) for bot in bots))
        package = (await package_query(client))["packages"][0]
        bases = [f"/management/v1/bots/{bot['bot_id']}/components/{package['component_id']}" for bot in bots]
        plan = await make_plan(client, bases[0], package, "First", 1, 0, "plan")
        moved = await apply(client, bases[1], plan, "cross-target", status=404)
        assert moved["code"] == "PLAN_NOT_FOUND"
        await apply(client, bases[0], plan, "apply")
        await effective(client, bases[0], 1)
        await invoke(client, bases[0], 1, "invoke")
        cookie = client.cookies["forest_session"]
        assert (await client.post("/management/v1/logout", json={})).status_code == 200
        client.cookies.set("forest_session", cookie)
        response = await client.post(bases[0] + "/invoke", json={
            "service": "counter.increment", "payload": {"increment": 1},
        }, headers={"idempotency-key": "invoke"})
        assert response.status_code == 401
        public = response.text + (await client.get("/management/v1/setup")).text
        assert PASSWORD not in public and web_bots_database not in public

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


async def post(client, url, body, key, *, status=200):
    response = await client.post(url, json=body, headers={"idempotency-key": key})
    assert response.status_code == status, response.text
    return response.json()


async def ready(client, bot):
    async with asyncio.timeout(35):
        while True:
            result = await client.get(f"/management/v1/provision-jobs/{bot['job_id']}")
            assert result.status_code == 200, result.text
            job = result.json()
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
        listing = (await client.get("/management/v1/bots")).json()
        assert len(listing["bots"]) == 2 and listing["managed_available"]
        packages = (await client.get("/management/v1/packages")).json()["packages"]
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
        listed = (await client.get("/management/v1/bots")).json()["bots"]
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
async def test_bot_endpoints_reject_csrf_cross_target_plan_and_revoked_replay(tmp_path, web_bots_database):
    data = tmp_path / "state"
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    token = BootstrapState(data).initialize()
    async with running(settings) as (_, client):
        assert (await client.get("/management/v1/bots")).status_code == 401
        await setup(client, token)
        rejected = await client.post("/management/v1/bots", json={"display_name": "Blocked"},
                                     headers={"x-csrf-token": "wrong", "idempotency-key": "blocked"})
        assert rejected.status_code == 403
        bots = [await post(client, "/management/v1/bots", {"display_name": key}, key, status=202)
                for key in ("first", "second")]
        await asyncio.gather(*(ready(client, bot) for bot in bots))
        package = (await client.get("/management/v1/packages")).json()["packages"][0]
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

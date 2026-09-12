"""Real HTTP handlers and PostgreSQL, including the local bootstrap boundary."""

import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings, create_app
from forest.storage.postgres import PostgresStore

ORIGIN = "http://testserver"
PASSWORD = "test-only-strong-password"


@pytest_asyncio.fixture
async def web_database():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must point to a private PostgreSQL test instance")
    name = "forest_web_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    try:
        yield uri
    finally:
        assert name.startswith("forest_web_") and len(name) == 43
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def initialize(client, token, dsn):
    response = await client.post(
        "/management/v1/setup",
        json={
            "token": token,
            "dsn": dsn,
            "username": "admin",
            "password": PASSWORD,
        },
    )
    assert response.status_code == 200, response.text


async def login(client):
    response = await client.post(
        "/management/v1/login", json={"username": "admin", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    client.headers["x-csrf-token"] = client.cookies["forest_csrf"]


@pytest.mark.postgres
async def test_browser_management_transactions_survive_restart(tmp_path, web_database):
    token = BootstrapState(tmp_path / "state").initialize()
    settings = WebSettings(tmp_path / "state", ORIGIN)
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=transport,
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as client,
    ):
        assert (await client.get("/management/v1/configuration")).status_code == 401
        await initialize(client, token, web_database)
        await login(client)
        first = await client.get("/management/v1/configuration")
        assert first.json()["head_revision"] == 0
        values = {"display_name": "我的 Forest", "session_ttl_minutes": 90}
        plan = (
            await client.post(
                "/management/v1/change-plans",
                json={
                    "values": values,
                    "expected_revision": 0,
                },
            )
        ).json()
        rival = (
            await client.post(
                "/management/v1/change-plans",
                json={
                    "values": {**values, "display_name": "另一个窗口"},
                    "expected_revision": 0,
                },
            )
        ).json()
        payload = {k: plan[k] for k in ("plan_id", "plan_digest", "expected_revision")}
        accepted = await client.post(
            "/management/v1/management-requests",
            json=payload,
            headers={"Idempotency-Key": "first-apply"},
        )
        assert accepted.status_code == 200, accepted.text
        repeated = await client.post(
            "/management/v1/management-requests",
            json=payload,
            headers={"Idempotency-Key": "first-apply"},
        )
        assert repeated.json() == accepted.json()
        conflict = await client.post(
            "/management/v1/management-requests",
            json={k: rival[k] for k in ("plan_id", "plan_digest", "expected_revision")},
            headers={"Idempotency-Key": "rival-apply"},
        )
        assert conflict.status_code == 409, conflict.text
        assert conflict.json()["code"] == "REVISION_CONFLICT"
        cookie = client.cookies["forest_session"]
        assert (await client.post("/management/v1/logout", json={})).status_code == 200
        client.cookies.set("forest_session", cookie)
        assert (await client.get("/management/v1/configuration")).status_code == 401
    restored = create_app(settings)
    async with (
        restored.router.lifespan_context(restored),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restored),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as client,
    ):
        assert not (await client.get("/management/v1/setup")).json()["setup_required"]
        await login(client)
        value = (await client.get("/management/v1/configuration")).json()
        assert value["head_revision"] == 1
        assert value["effective"]["values"] == values


@pytest.mark.postgres
async def test_csrf_validation_and_secrets_are_not_echoed(tmp_path, web_database):
    token = BootstrapState(tmp_path / "state").initialize()
    app = create_app(WebSettings(tmp_path / "state", ORIGIN))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as client,
    ):
        bad = await client.post(
            "/management/v1/setup", json={"token": token, "password": "leak-me"}
        )
        assert bad.status_code == 422
        assert token not in bad.text and "leak-me" not in bad.text
        await initialize(client, token, web_database)
        await login(client)
        response = await client.post(
            "/management/v1/change-plans",
            json={
                "values": {"display_name": "bad", "session_ttl_minutes": 5},
                "expected_revision": 0,
            },
            headers={"x-csrf-token": "wrong"},
        )
        assert response.status_code == 403
        response = await client.post(
            "/management/v1/logout", json={}, headers={"origin": "https://attacker.invalid"}
        )
        assert response.status_code == 403
        response = await client.post(
            "/management/v1/change-plans",
            json={
                "values": {"display_name": "bad", "session_ttl_minutes": True},
                "expected_revision": 0,
            },
        )
        assert response.status_code == 422
        response = await client.post("/management/v1/login", content=b"x" * 65537)
        assert response.status_code == 413
        html = await client.get("/")
        assert "frame-ancestors 'none'" in html.headers["content-security-policy"]
        assert token not in html.text and PASSWORD not in html.text


@pytest.mark.postgres
async def test_complete_bootstrap_does_not_recreate_missing_authority(tmp_path, web_database):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    settings = WebSettings(state, ORIGIN)
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as client,
    ):
        await initialize(client, token, web_database)
    # This is a disposable UUID database created by this test fixture only.
    async with await psycopg.AsyncConnection.connect(web_database, autocommit=True) as conn:
        await conn.execute("DROP SCHEMA public CASCADE")
        await conn.execute("CREATE SCHEMA public")
    restored = create_app(settings)
    async with (
        restored.router.lifespan_context(restored),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restored),
            base_url=ORIGIN,
        ) as client,
    ):
        status = (await client.get("/management/v1/setup")).json()
        assert not status["setup_required"]
        assert not status["database_ready"]
        assert status["recovery_error"] == "DATABASE_IDENTITY_MISMATCH"
    async with await psycopg.AsyncConnection.connect(web_database) as conn:
        row = await (await conn.execute("SELECT to_regclass('web_accounts')")).fetchone()
        assert row == (None,)


@pytest.mark.postgres
async def test_database_failure_is_safe_and_session_can_retry(tmp_path, web_database, monkeypatch):
    token = BootstrapState(tmp_path / "state").initialize()
    app = create_app(WebSettings(tmp_path / "state", ORIGIN))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as client,
    ):
        await initialize(client, token, web_database)
        await login(client)

        async def unavailable(*args):
            raise psycopg.OperationalError("private connection details password=do-not-echo")

        with monkeypatch.context() as patch:
            patch.setattr(PostgresStore, "get_session", unavailable)
            failed = await client.get("/management/v1/configuration")
            assert failed.status_code == 503
            assert failed.json()["code"] == "DATABASE_UNAVAILABLE"
            assert "do-not-echo" not in failed.text
            assert "private connection" not in failed.text
        # An availability error did not delete the durable session or mutate config.
        recovered = await client.get("/management/v1/configuration")
        assert recovered.status_code == 200
        assert recovered.json()["head_revision"] == 0

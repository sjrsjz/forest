"""Real HTTP handlers and PostgreSQL, including the local bootstrap boundary."""

import asyncio
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


async def login_as(client, username, password):
    response = await client.post(
        "/management/v1/login", json={"username": username, "password": password}
    )
    if response.status_code == 200:
        client.headers["x-csrf-token"] = client.cookies["forest_csrf"]
    return response


async def create_account(client, username, password, role, current_password, key):
    return await client.post(
        "/management/v1/accounts",
        json={
            "username": username,
            "password": password,
            "role": role,
            "current_password": current_password,
        },
        headers={"idempotency-key": key},
    )


@pytest.mark.postgres
async def test_account_creation_is_idempotent_and_viewer_is_read_only(tmp_path, web_database):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN))
    viewer_password = "viewer-test-only-password"
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as admin,
    ):
        await initialize(admin, token, web_database)
        await login(admin)
        body = ("observer", viewer_password, "viewer", PASSWORD, "create-observer")
        created = await create_account(admin, *body)
        assert created.status_code == 201, created.text
        repeated = await create_account(admin, *body)
        assert repeated.status_code == 201 and repeated.json() == created.json()
        conflict = await create_account(
            admin, "observer", viewer_password, "admin", PASSWORD, "create-observer"
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "IDEMPOTENCY_CONFLICT"
        duplicate = await create_account(
            admin, "observer", viewer_password, "viewer", PASSWORD, "duplicate-name"
        )
        assert duplicate.status_code == 409
        assert duplicate.json()["code"] == "ACCOUNT_NAME_CONFLICT"
        current_id = (await admin.get("/management/v1/session")).json()["account_id"]
        self_disable = await admin.post(
            f"/management/v1/accounts/{current_id}/change-plans",
            json={"expected_revision": 1, "role": "admin", "disabled": True},
        )
        assert self_disable.status_code == 409
        assert self_disable.json()["code"] == "SELF_DISABLE_REQUIRES_ANOTHER_ADMIN"
        demotion = await admin.post(
            f"/management/v1/accounts/{current_id}/change-plans",
            json={"expected_revision": 1, "role": "viewer", "disabled": False},
        )
        frozen = demotion.json()
        last_admin = await admin.post(
            f"/management/v1/accounts/{current_id}/management-requests",
            json={
                "expected_revision": 1,
                "plan_id": frozen["plan_id"],
                "plan_digest": frozen["plan_digest"],
                "current_password": PASSWORD,
            },
            headers={"idempotency-key": "demote-only-admin"},
        )
        assert last_admin.status_code == 409
        assert last_admin.json()["code"] == "LAST_ADMIN_REQUIRED"

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as viewer:
            logged_in = await login_as(viewer, "observer", viewer_password)
            assert logged_in.status_code == 200
            session = (await viewer.get("/management/v1/session")).json()
            assert session["role"] == "viewer" and not session["can_write"]
            assert (await viewer.get("/management/v1/configuration")).status_code == 200
            assert (await viewer.get("/management/v1/bots")).status_code == 200
            denied = await viewer.post(
                "/management/v1/change-plans",
                json={"expected_revision": 0, "values": {}},
            )
            assert denied.status_code == 403 and denied.json()["code"] == "PERMISSION_DENIED"
            assert (await viewer.get("/management/v1/accounts")).status_code == 403
            assert (await viewer.post("/management/v1/logout", json={})).status_code == 200


@pytest.mark.postgres
async def test_account_apply_revokes_sessions_and_records_no_passwords(tmp_path, web_database):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN))
    viewer_password = "viewer-session-test-password"
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as admin,
    ):
        await initialize(admin, token, web_database)
        await login(admin)
        created = await create_account(
            admin, "session-viewer", viewer_password, "viewer", PASSWORD, "create-session-viewer"
        )
        assert created.status_code == 201, created.text
        account = created.json()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as viewer:
            assert (await login_as(viewer, "session-viewer", viewer_password)).status_code == 200
            plan = await admin.post(
                f"/management/v1/accounts/{account['id']}/change-plans",
                json={"expected_revision": 1, "role": "viewer", "disabled": True},
            )
            assert plan.status_code == 200, plan.text
            frozen = plan.json()
            applied = await admin.post(
                f"/management/v1/accounts/{account['id']}/management-requests",
                json={
                    "expected_revision": frozen["expected_revision"],
                    "plan_id": frozen["plan_id"],
                    "plan_digest": frozen["plan_digest"],
                    "current_password": PASSWORD,
                },
                headers={"idempotency-key": "disable-session-viewer"},
            )
            assert applied.status_code == 200, applied.text
            assert applied.json()["disabled"] and applied.json()["revoked_sessions"] == 1
            assert (await viewer.get("/management/v1/session")).status_code == 401

    async with await psycopg.AsyncConnection.connect(web_database) as conn:
        records = await (await conn.execute(
            "SELECT action,details::text FROM audit_records WHERE action LIKE 'account.%' "
            "UNION ALL SELECT topic,payload::text FROM outbox_events "
            "WHERE topic LIKE 'account.%' ORDER BY 1"
        )).fetchall()
    public = repr(records)
    assert viewer_password not in public and PASSWORD not in public
    assert {row[0] for row in records} == {
        "account.create", "account.create.accepted", "account.apply", "account.apply.accepted"
    }


@pytest.mark.postgres
async def test_self_password_change_revokes_cookie_and_requires_new_password(tmp_path, web_database):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN))
    replacement = "replacement-admin-test-password"
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ORIGIN, headers={"origin": ORIGIN}
        ) as client,
    ):
        await initialize(client, token, web_database)
        await login(client)
        account_id = (await client.get("/management/v1/session")).json()["account_id"]
        preview = await client.post(
            f"/management/v1/accounts/{account_id}/change-plans",
            json={
                "expected_revision": 1,
                "role": "admin",
                "disabled": False,
                "new_password": replacement,
            },
        )
        assert preview.status_code == 200, preview.text
        assert replacement not in preview.text and preview.json()["after"]["password_change"]
        frozen = preview.json()
        async with app.state.runtime.store.pool.connection() as conn:
            secret = await (await conn.execute(
                "SELECT password_hash_ciphertext,password_secret_digest "
                "FROM web_account_plans WHERE plan_id=%s",
                (frozen["plan_id"],),
            )).fetchone()
        assert not secret["password_hash_ciphertext"].startswith("$scrypt$")
        assert secret["password_secret_digest"].startswith("hmac-sha256:")
        applied = await client.post(
            f"/management/v1/accounts/{account_id}/management-requests",
            json={
                "expected_revision": 1,
                "plan_id": frozen["plan_id"],
                "plan_digest": frozen["plan_digest"],
                "current_password": PASSWORD,
            },
            headers={"idempotency-key": "change-own-password"},
        )
        assert applied.status_code == 200, applied.text
        assert applied.json()["reauthentication_required"]
        assert (await client.get("/management/v1/session")).status_code == 401
        assert (await login_as(client, "admin", PASSWORD)).status_code == 401
        assert (await login_as(client, "admin", replacement)).status_code == 200


@pytest.mark.postgres
async def test_concurrent_admin_demotions_keep_one_enabled_admin(tmp_path, web_database):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN))
    second_password = "second-admin-test-password"
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ORIGIN, headers={"origin": ORIGIN}
        ) as first,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ORIGIN, headers={"origin": ORIGIN}
        ) as second,
    ):
        await initialize(first, token, web_database)
        await login(first)
        created = await create_account(
            first, "second-admin", second_password, "admin", PASSWORD, "create-second-admin"
        )
        assert created.status_code == 201, created.text
        assert (await login_as(second, "second-admin", second_password)).status_code == 200
        accounts = (await first.get("/management/v1/accounts")).json()["accounts"]
        first_id = next(row["id"] for row in accounts if row["username"] == "admin")
        second_id = created.json()["id"]

        first_plan = await first.post(
            f"/management/v1/accounts/{first_id}/change-plans",
            json={"expected_revision": 1, "role": "viewer", "disabled": False},
        )
        second_plan = await second.post(
            f"/management/v1/accounts/{second_id}/change-plans",
            json={"expected_revision": 1, "role": "viewer", "disabled": False},
        )
        assert first_plan.status_code == second_plan.status_code == 200

        async def apply(client, account_id, plan, password, key):
            frozen = plan.json()
            return await client.post(
                f"/management/v1/accounts/{account_id}/management-requests",
                json={
                    "expected_revision": frozen["expected_revision"],
                    "plan_id": frozen["plan_id"],
                    "plan_digest": frozen["plan_digest"],
                    "current_password": password,
                },
                headers={"idempotency-key": key},
            )

        results = await asyncio.gather(
            apply(first, first_id, first_plan, PASSWORD, "demote-first"),
            apply(second, second_id, second_plan, second_password, "demote-second"),
        )
        assert sorted(response.status_code for response in results) == [200, 409]
        failure = next(response for response in results if response.status_code == 409)
        assert failure.json()["code"] == "LAST_ADMIN_REQUIRED"
        if results[0].status_code == 200:
            winner_client, winner_name, winner_password = first, "admin", PASSWORD
            winner_id, winner_plan, winner_key = first_id, first_plan, "demote-first"
            original = results[0]
        else:
            winner_client, winner_name, winner_password = second, "second-admin", second_password
            winner_id, winner_plan, winner_key = second_id, second_plan, "demote-second"
            original = results[1]
        login_result = await login_as(winner_client, winner_name, winner_password)
        assert login_result.status_code == 200 and login_result.json()["role"] == "viewer"
        recovered = await apply(
            winner_client, winner_id, winner_plan, winner_password, winner_key
        )
        assert recovered.status_code == 200 and recovered.json() == original.json()
        assert (await winner_client.get("/management/v1/session")).status_code == 200

    async with await psycopg.AsyncConnection.connect(web_database) as conn:
        remaining = await (await conn.execute(
            "SELECT count(*) FROM web_accounts WHERE role='admin' AND NOT disabled"
        )).fetchone()
    assert remaining[0] == 1

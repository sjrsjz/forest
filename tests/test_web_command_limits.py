"""Web-managed durable business-command admission limits."""

import pytest
from test_management import create_account, login_as
from test_web_bots import ORIGIN, PASSWORD, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings
from forest.storage.command_limits import DEFAULT_COMMAND_LIMITS

pytestmark = pytest.mark.postgres


async def test_web_limit_preview_apply_reauth_and_payload_proof(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        page = (await client.get("/static/modules/command.router.html")).text
        assert "业务命令容量与调用者限流" in page
        script = (await client.get("/static/business-command-diagnostics.js")).text
        assert "action('limit.plan')" in script
        bot = await post(
            client,
            "/management/v1/bots",
            {"display_name": "Command limits"},
            "create-command-limit-bot",
            status=202,
        )
        await ready(client, bot)
        module = "/management/v1/scopes/bot/" + bot["bot_id"] + "/modules/command.router"
        initial = (await client.get(module + "/queries/limits")).json()
        assert initial == {
            "revision": 0,
            "values": DEFAULT_COMMAND_LIMITS,
            "defaults": DEFAULT_COMMAND_LIMITS,
            "values_payload_state": "DEFAULT",
        }
        values = {
            **DEFAULT_COMMAND_LIMITS,
            "max_pending_bot": 40,
            "max_pending_binding": 20,
            "max_pending_principal": 4,
            "principal_burst": 3,
            "principal_refill_seconds": 15,
        }
        plan_url = module + "/actions/limit.plan"
        denied = await client.post(
            plan_url,
            json={
                "expected_revision": 0,
                "values": values,
                "current_password": "wrong-password-value",
            },
            headers={"idempotency-key": "wrong-command-limit-password"},
        )
        assert denied.status_code == 403
        assert denied.json()["code"] == "REAUTHENTICATION_FAILED"
        plan = await post(
            client,
            plan_url,
            {"expected_revision": 0, "values": values, "current_password": PASSWORD},
            "plan-command-limits",
        )
        assert plan["before_values"] == DEFAULT_COMMAND_LIMITS
        assert plan["values"] == values and PASSWORD not in str(plan)
        apply_body = {
            "expected_revision": 0,
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
            "current_password": PASSWORD,
        }
        apply_url = module + "/actions/limit.apply"
        applied = await post(client, apply_url, apply_body, "apply-command-limits")
        assert applied == {"revision": 1, "status": "APPLIED"}
        assert await post(client, apply_url, apply_body, "apply-command-limits") == applied
        saved = (await client.get(module + "/queries/limits")).json()
        assert saved["revision"] == 1
        assert saved["values"] == values
        assert saved["values_payload_state"] == "ACTIVE"

        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        async with managed.store.pool.connection() as conn:
            plan_row = await (
                await conn.execute(
                    "SELECT values_payload_id FROM public.bot_command_limit_plans "
                    "WHERE bot_id=%s AND plan_id=%s",
                    (bot["bot_id"], plan["plan_id"]),
                )
            ).fetchone()
            operation = await (
                await conn.execute(
                    "SELECT input_payload_id FROM public.bot_web_operations "
                    "WHERE bot_id=%s AND action='command.limit.plan'",
                    (bot["bot_id"],),
                )
            ).fetchone()
            parents = await (
                await conn.execute(
                    "SELECT parent_payload_id FROM public.bot_payload_derivations "
                    "WHERE bot_id=%s AND payload_id=%s",
                    (bot["bot_id"], plan_row["values_payload_id"]),
                )
            ).fetchall()
        assert parents == [{"parent_payload_id": operation["input_payload_id"]}]

        viewer_password = "command-limit-viewer-password"
        created = await create_account(
            client,
            "limit-viewer",
            viewer_password,
            "viewer",
            PASSWORD,
            "create-limit-viewer",
        )
        assert created.status_code == 201, created.text
        await client.post("/management/v1/logout", json={})
        assert (await login_as(client, "limit-viewer", viewer_password)).status_code == 200
        assert (await client.get(module + "/queries/limits")).status_code == 200
        rejected = await client.post(
            plan_url,
            json={
                "expected_revision": 1,
                "values": values,
                "current_password": viewer_password,
            },
            headers={"idempotency-key": "viewer-command-limit-plan"},
        )
        assert rejected.status_code == 403

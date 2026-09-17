"""Web-managed durable message-handler admission limits."""

import pytest
from test_management import create_account, login_as
from test_web_bots import ORIGIN, PASSWORD, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings
from forest.storage.message_handler_limits import DEFAULT_MESSAGE_HANDLER_LIMITS

pytestmark = pytest.mark.postgres


async def test_web_limit_preview_apply_reauth_and_payload_proof(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        page = (await client.get("/static/modules/message.router.html")).text
        assert "普通消息 Handler 容量与调用者限流" in page
        script = (await client.get("/static/message-handler-diagnostics.js")).text
        assert "action('limit.plan')" in script
        bot = await post(
            client,
            "/management/v1/bots",
            {"display_name": "Handler limits"},
            "create-handler-limit-bot",
            status=202,
        )
        await ready(client, bot)
        module = "/management/v1/scopes/bot/" + bot["bot_id"] + "/modules/message.router"
        initial = (await client.get(module + "/queries/limits")).json()
        assert initial == {
            "revision": 0,
            "values": DEFAULT_MESSAGE_HANDLER_LIMITS,
            "defaults": DEFAULT_MESSAGE_HANDLER_LIMITS,
            "values_payload_state": "DEFAULT",
        }
        values = {
            **DEFAULT_MESSAGE_HANDLER_LIMITS,
            "max_pending_bot": 40,
            "max_pending_binding": 20,
            "max_pending_principal": 4,
            "max_handlers_per_chain": 16,
            "principal_burst": 3,
            "principal_refill_seconds": 15,
            "deadline_seconds": 45,
        }
        plan_url = module + "/actions/limit.plan"
        denied = await client.post(
            plan_url,
            json={
                "expected_revision": 0,
                "values": values,
                "current_password": "wrong-password-value",
            },
            headers={"idempotency-key": "wrong-handler-limit-password"},
        )
        assert denied.status_code == 403
        assert denied.json()["code"] == "REAUTHENTICATION_FAILED"
        plan = await post(
            client,
            plan_url,
            {"expected_revision": 0, "values": values, "current_password": PASSWORD},
            "plan-handler-limits",
        )
        assert plan["before_values"] == DEFAULT_MESSAGE_HANDLER_LIMITS
        assert plan["values"] == values and PASSWORD not in str(plan)
        apply_body = {
            "expected_revision": 0,
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
            "current_password": PASSWORD,
        }
        apply_url = module + "/actions/limit.apply"
        applied = await post(client, apply_url, apply_body, "apply-handler-limits")
        assert applied == {"revision": 1, "status": "APPLIED"}
        assert await post(client, apply_url, apply_body, "apply-handler-limits") == applied
        saved = (await client.get(module + "/queries/limits")).json()
        assert saved["revision"] == 1
        assert saved["values"] == values
        assert saved["values_payload_state"] == "ACTIVE"

        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        async with managed.store.pool.connection() as conn:
            plan_row = await (
                await conn.execute(
                    "SELECT values_payload_id FROM public.bot_message_handler_limit_plans "
                    "WHERE bot_id=%s AND plan_id=%s",
                    (bot["bot_id"], plan["plan_id"]),
                )
            ).fetchone()
            operation = await (
                await conn.execute(
                    "SELECT input_payload_id FROM public.bot_web_operations "
                    "WHERE bot_id=%s AND action='message.handler.limit.plan'",
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

        viewer_password = "handler-limit-viewer-password"
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
            headers={"idempotency-key": "viewer-handler-limit-plan"},
        )
        assert rejected.status_code == 403

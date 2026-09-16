"""Web Handler diagnostics expose redacted metadata and idempotent wakeups."""

import pytest
from test_management import create_account, login_as
from test_message_handler_execution import handler_source
from test_messages import event
from test_web_bindings import configure, onebot_server
from test_web_bots import ORIGIN, PASSWORD, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings
from forest.packages import PackageCatalog

pytestmark = pytest.mark.postgres


async def test_web_handler_diagnostics_are_redacted_and_recovery_replays(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with onebot_server(self_id="1000") as server:
        async with running(settings) as (app, client):
            await setup(client, token)
            page = (await client.get("/")).text
            assert "普通消息 Handler 运行诊断" in page
            module = (await client.get("/static/message-handler-diagnostics.js")).text
            assert "export function messageHandlerDiagnosticsPanel" in module

            bot = await post(
                client,
                "/management/v1/bots",
                {"display_name": "Handler diagnostics"},
                "create-handler-diagnostic-bot",
                status=202,
            )
            await ready(client, bot)
            root = "/management/v1/bots/" + bot["bot_id"]
            managed = await app.state.runtime.fleet.attach(bot["bot_id"])
            await managed.kernel.install(
                PackageCatalog(tmp_path / "packages").build(handler_source(tmp_path))
            )
            binding = await post(
                client,
                root + "/bindings",
                {"self_id": "1000", "display_name": "Diagnostic QQ"},
                "create-handler-diagnostic-binding",
                status=201,
            )
            binding_id = binding["binding_id"]
            await configure(client, root + "/bindings/" + binding_id, server["origin"])
            for handler_id in ("first", "second"):
                policy_root = (
                    root
                    + "/bindings/"
                    + binding_id
                    + "/components/actor.handlers/message-handlers/"
                    + handler_id
                )
                plan = await post(
                    client,
                    policy_root + "/policy-change-plans",
                    {
                        "expected_revision": 0,
                        "values": {"mode": "EVERYONE", "user_ids": []},
                        "current_password": PASSWORD,
                    },
                    "plan-handler-diagnostic-policy-" + handler_id,
                )
                await post(
                    client,
                    policy_root + "/policy-management-requests",
                    {
                        "expected_revision": 0,
                        "plan_id": plan["plan_id"],
                        "plan_digest": plan["plan_digest"],
                        "current_password": PASSWORD,
                    },
                    "apply-handler-diagnostic-policy-" + handler_id,
                )

            scheduler = managed.platform.module("conversation.scheduler")
            await scheduler._tick_lock.acquire()
            actor = None
            try:
                secret = "private-handler-message"
                accepted = await managed.platform.messages.ingest(
                    managed.kernel.lease,
                    binding_id,
                    event(9201, group="diagnostic-group", user="sensitive-user", text=secret),
                    decision="NO_TRIGGER",
                )
                actor = await managed.platform.messages.acquire_actor(
                    managed.kernel.lease,
                    accepted["conversation_id"],
                    "handler-diagnostic-owner",
                    ttl_seconds=30,
                )
                selected = await managed.platform.messages.select_input(
                    actor, "claim-handler-diagnostic"
                )
                assert selected["status"] == "MESSAGE_HANDLER"

                response = await client.get(root + "/message-handlers/diagnostics?limit=25")
                assert response.status_code == 200, response.text
                body = response.json()
                assert body["summary"]["pending"] == 1
                assert body["summary"]["claimed"] == 1
                assert body["summary"]["claimed_call_absent"] == 1
                assert body["chains"][0]["handler_id"] == "first"
                assert body["chains"][0]["call_status"] is None
                assert body["attempts"][0]["handler_id"] == "first"
                assert secret not in response.text
                assert "sensitive-user" not in response.text

                filtered = await client.get(
                    root
                    + "/message-handlers/diagnostics?binding_id="
                    + binding_id
                    + "&limit=10"
                )
                assert filtered.status_code == 200
                assert filtered.json()["filter"]["binding_id"] == binding_id
                missing = await client.get(
                    root + "/message-handlers/diagnostics?binding_id=missing&limit=10"
                )
                assert missing.status_code == 404
                unknown = await client.get(root + "/message-handlers/diagnostics?unknown=1")
                assert unknown.status_code == 422

                recovery_url = root + "/message-handlers/recovery"
                assert (await client.post(recovery_url, json={"limit": 8})).status_code == 422
                first = await post(client, recovery_url, {"limit": 8}, "recover-handlers")
                replay = await post(client, recovery_url, {"limit": 8}, "recover-handlers")
                assert replay == first
                assert first["inspected"] == 1
                assert first["abandoned_to_reconcile"] == 1
                assert first["scheduler_wakeup"] is True
                async with managed.store.pool.connection() as conn:
                    operations = await (
                        await conn.execute(
                            "SELECT count(*) AS n FROM public.bot_web_operations "
                            "WHERE bot_id=%s AND action='message.handler.execution.recover'",
                            (bot["bot_id"],),
                        )
                    ).fetchone()
                assert operations["n"] == 1
            finally:
                if actor is not None:
                    await managed.platform.messages.release_actor(actor)
                scheduler._tick_lock.release()

            viewer_password = "handler-diagnostic-viewer-password"
            created = await create_account(
                client,
                "handler-viewer",
                viewer_password,
                "viewer",
                PASSWORD,
                "create-handler-viewer",
            )
            assert created.status_code == 201, created.text
            await client.post("/management/v1/logout", json={})
            assert (await login_as(client, "handler-viewer", viewer_password)).status_code == 200
            assert (await client.get(root + "/message-handlers/diagnostics")).status_code == 200
            denied = await client.post(
                root + "/message-handlers/recovery",
                json={"limit": 8},
                headers={"idempotency-key": "viewer-handler-recovery-denied"},
            )
            assert denied.status_code == 403
            assert denied.json()["code"] == "PERMISSION_DENIED"

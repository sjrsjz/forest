"""Web business-command diagnostics expose metadata and replay bounded recovery."""

import pytest
from test_business_command_actor import actor, command_source
from test_management import create_account, login_as
from test_messages import event
from test_web_bindings import configure, onebot_server
from test_web_bots import ORIGIN, PASSWORD, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings
from forest.packages import PackageCatalog

pytestmark = pytest.mark.postgres


async def test_web_command_diagnostics_are_redacted_and_recovery_replays(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with onebot_server(self_id="1000") as server:
        async with running(settings) as (app, client):
            await setup(client, token)
            page = (await client.get("/")).text
            assert "业务命令运行诊断" in page
            module = (await client.get("/static/business-command-diagnostics.js")).text
            assert "export function businessCommandDiagnosticsPanel" in module

            bot = await post(
                client,
                "/management/v1/bots",
                {"display_name": "Command diagnostics"},
                "create-command-diagnostic-bot",
                status=202,
            )
            await ready(client, bot)
            root = "/management/v1/bots/" + bot["bot_id"]
            managed = await app.state.runtime.fleet.attach(bot["bot_id"])
            await managed.kernel.install(
                PackageCatalog(tmp_path / "packages").build(command_source(tmp_path))
            )
            binding = await post(
                client,
                root + "/bindings",
                {"self_id": "1000", "display_name": "Diagnostic QQ"},
                "create-command-diagnostic-binding",
                status=201,
            )
            binding_id = binding["binding_id"]
            binding_root = root + "/bindings/" + binding_id
            await configure(client, binding_root, server["origin"])
            policy_root = binding_root + "/commands/actor"
            plan = await post(
                client,
                policy_root + "/policy-change-plans",
                {
                    "expected_revision": 0,
                    "values": {"mode": "EVERYONE", "user_ids": []},
                    "current_password": PASSWORD,
                },
                "plan-command-diagnostic-policy",
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
                "apply-command-diagnostic-policy",
            )
            scheduler = managed.platform.module("conversation.scheduler")
            await scheduler._tick_lock.acquire()
            messages = managed.platform.module("adapter.onebot").messages

            secret_argument = "private-command-argument"
            accepted = await messages.ingest(
                managed.kernel.lease,
                binding_id,
                event(1, group="group-1", user="20001", text="/actor " + secret_argument),
                decision="NO_TRIGGER",
            )
            owner = await actor(messages, managed.kernel, accepted["conversation_id"])
            await messages.select_input(owner, "claim-diagnostic-command")
            await messages.record_business_command_failure(
                owner,
                "fail-diagnostic-command",
                request_id=accepted["business_request_id"],
                error_code="CALL_FAILED",
                retryable=False,
            )
            classify = messages._business_plan_in

            async def unavailable(conn, binding, value):
                return {"failed": True, "reason": "BUSINESS_COMMAND_POLICY_UNAVAILABLE"}

            messages._business_plan_in = unavailable
            try:
                pending = await messages.ingest(
                    managed.kernel.lease,
                    binding_id,
                    event(2, group="group-2", user="20002", text="/actor queued"),
                    decision="NO_TRIGGER",
                )
            finally:
                messages._business_plan_in = classify
            assert pending["trigger_decision"] == "PENDING"

            response = await client.get(root + "/business-commands/diagnostics?limit=25")
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["summary"]["failed_requests"] == 1
            assert body["summary"]["pending_notices"] == 1
            assert body["summary"]["pending_classifications"] == 1
            assert body["requests"][0]["command_name"] == "/actor"
            assert body["requests"][0]["last_error_code"] == "CALL_FAILED"
            assert body["execution_attempts"][0]["error_code"] == "CALL_FAILED"
            assert body["notices"][0]["kind"] == "BUSINESS_COMMAND_FAILED"
            assert secret_argument not in response.text
            assert "20001" not in response.text

            filtered = await client.get(
                root + "/business-commands/diagnostics?binding_id=" + binding_id + "&limit=10"
            )
            assert filtered.status_code == 200
            assert filtered.json()["filter"]["binding_id"] == binding_id
            missing = await client.get(
                root + "/business-commands/diagnostics?binding_id=missing&limit=10"
            )
            assert missing.status_code == 404
            assert missing.json()["code"] == "BINDING_NOT_FOUND"
            unknown = await client.get(root + "/business-commands/diagnostics?unknown=1")
            assert unknown.status_code == 422

            recovery_url = root + "/business-commands/recovery"
            absent_key = await client.post(recovery_url, json={"limit": 8})
            assert absent_key.status_code == 422
            first = await post(client, recovery_url, {"limit": 8}, "recover-commands")
            replay = await post(client, recovery_url, {"limit": 8}, "recover-commands")
            assert replay == first
            assert first["classification_completed"] == 1
            assert first["classification_expired"] == 0
            async with managed.store.pool.connection() as conn:
                operations = await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM public.bot_web_operations "
                        "WHERE bot_id=%s AND action='command.execution.recover'",
                        (bot["bot_id"],),
                    )
                ).fetchone()
            assert operations["n"] == 1
            scheduler._tick_lock.release()

            viewer_password = "command-diagnostic-viewer-password"
            created = await create_account(
                client,
                "command-viewer",
                viewer_password,
                "viewer",
                PASSWORD,
                "create-command-viewer",
            )
            assert created.status_code == 201, created.text
            await client.post("/management/v1/logout", json={})
            assert (await login_as(client, "command-viewer", viewer_password)).status_code == 200
            assert (await client.get(root + "/business-commands/diagnostics")).status_code == 200
            denied = await client.post(
                recovery_url,
                json={"limit": 8},
                headers={"idempotency-key": "viewer-recovery-denied"},
            )
            assert denied.status_code == 403
            assert denied.json()["code"] == "PERMISSION_DENIED"

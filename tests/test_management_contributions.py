from pathlib import Path

import pytest
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings

pytestmark = pytest.mark.postgres


async def test_bot_scope_discovers_generation_bound_management_contributions(
    tmp_path: Path, web_bots_database: str
):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    settings = WebSettings(state, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (_, client):
        await setup(client, token)
        bot = await post(client, "/management/v1/bots", {"display_name": "Scope"}, "bot", status=202)
        await ready(client, bot)

        response = await client.get(
            f"/management/v1/scopes/bot/{bot['bot_id']}/modules"
        )
        assert response.status_code == 200, response.text
        projection = response.json()
        assert projection["scope"] == {"kind": "bot", "id": bot["bot_id"]}
        modules = {row["component_id"]: row for row in projection["modules"]}
        contribution, = modules["agent.runtime"]["management_contributions"]
        assert contribution["protocol"] == "forest.management-contribution/1"
        assert contribution["scope_kinds"] == ["bot"]
        onebot, = modules["adapter.onebot"]["management_contributions"]
        assert onebot["contribution_id"] == "connections"
        assert onebot["surface"]["template"] == "/static/modules/adapter.onebot.html"
        assert onebot["surface"]["module"] == "/static/onebot-panel.js"
        assert {
            item["operation_id"]
            for item in modules["adapter.onebot"]["management_contract"]["operations"]
        } == {
            "binding.create",
            "binding.registration.resume",
            "bindings",
            "binding.configuration",
            "binding.plan",
            "binding.apply",
            "conversations",
            "delivery",
            "delivery.reconcile",
        }
        assert modules["conversation.scheduler"]["management_contributions"] == []
        audit, = modules["kernel.audit"]["management_contributions"]
        assert audit["contribution_id"] == "audit"
        assert audit["surface"]["template"] == "/static/modules/kernel.audit.html"
        assert modules["kernel.audit"]["management_contract"]["operations"] == [
            {"operation_id": "records", "kind": "query", "authority": "audit.read"}
        ]
        commands, = modules["command.router"]["management_contributions"]
        assert commands["contribution_id"] == "commands"
        assert commands["surface"]["template"] == "/static/modules/command.router.html"
        assert {
            item["operation_id"]
            for item in modules["command.router"]["management_contract"]["operations"]
        } == {
            "bindings", "policies", "policy.plan", "policy.apply", "limits",
            "limit.plan", "limit.apply", "diagnostics", "recovery",
        }
        messages, = modules["message.router"]["management_contributions"]
        assert messages["contribution_id"] == "messages"
        assert messages["surface"]["template"] == "/static/modules/message.router.html"
        assert {
            item["operation_id"]
            for item in modules["message.router"]["management_contract"]["operations"]
        } == {
            "bindings", "policies", "policy.plan", "policy.apply", "limits",
            "limit.plan", "limit.apply", "diagnostics", "recovery",
        }
        assert {
            item["operation_id"]
            for item in modules["routing.graph"]["management_contract"]["operations"]
        } >= {
            "events",
            "event.replay",
            "event.retirement.plan",
            "event.retirement.apply",
            "component-jobs",
            "component-job.replay",
            "component-job.retirement.plan",
            "component-job.retirement.apply",
        }
        schedules, = modules["scheduler.jobs"]["management_contributions"]
        assert schedules["contribution_id"] == "schedules"
        assert {
            item["operation_id"]
            for item in modules["scheduler.jobs"]["management_contract"]["operations"]
        } == {
            "definitions",
            "definition.plan",
            "definition.apply",
            "occurrence.run",
            "definition.cancel",
            "occurrence.replay",
        }

        adapter = (
            f"/management/v1/scopes/bot/{bot['bot_id']}/modules/adapter.onebot/queries"
        )
        assert (await client.get(adapter + "/bindings")).json() == {
            "bindings": [],
            "errors": {},
        }
        assert (await client.get(adapter + "/conversations")).json() == {
            "conversations": []
        }
        command = (
            f"/management/v1/scopes/bot/{bot['bot_id']}/modules/command.router/queries"
        )
        assert (await client.get(command + "/bindings")).json() == {"bindings": []}
        message = (
            f"/management/v1/scopes/bot/{bot['bot_id']}/modules/message.router/queries"
        )
        assert (await client.get(message + "/bindings")).json() == {"bindings": []}
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/jobs")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/bindings")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/events")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/component-jobs")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/business-command-limits")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/business-commands/diagnostics")
        ).status_code == 404
        assert (
            await client.get(
                f"/management/v1/bots/{bot['bot_id']}/bindings/missing/command-policies"
            )
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/message-handler-limits")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/message-handlers/diagnostics")
        ).status_code == 404
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/audit")
        ).status_code == 404
        assert (
            await client.get(
                f"/management/v1/bots/{bot['bot_id']}/bindings/missing/"
                "message-handler-policies"
            )
        ).status_code == 404

        graph = await client.get(
            f"/management/v1/scopes/bot/{bot['bot_id']}/modules/"
            "routing.graph/queries/snapshot"
        )
        assert graph.status_code == 200, graph.text
        legacy = await client.get(
            f"/management/v1/bots/{bot['bot_id']}/runtime-modules/"
            "routing.graph/queries/snapshot"
        )
        assert legacy.status_code == 404
        deployment_id = (await client.get("/management/v1/session")).json()["deployment_id"]
        deployment = await client.get(
            f"/management/v1/scopes/deployment/{deployment_id}/modules"
        )
        assert deployment.status_code == 200, deployment.text
        deployment_modules = {
            row["component_id"]: row for row in deployment.json()["modules"]
        }
        accounts = deployment_modules["deployment.accounts"]
        assert accounts["artifact"]["scope"] == "deployment"
        assert accounts["state"] == "ACTIVE"
        assert accounts["management_contributions"][0]["surface"] == {
            "kind": "trusted-module",
            "protocol": "forest.trusted-management-view/1",
            "template": "/static/modules/deployment.accounts.html",
            "module": "/static/accounts-panel.js",
            "style": None,
        }
        assert accounts["management_contract"]["operations"] == [
            {"operation_id": "accounts", "kind": "query",
             "authority": "deployment.accounts.read", "principal": "admin"},
            {"operation_id": "account.create", "kind": "action",
             "authority": "deployment.accounts.create", "principal": "admin"},
            {"operation_id": "account.change.plan", "kind": "action",
             "authority": "deployment.accounts.change.plan", "principal": "admin"},
            {"operation_id": "account.change.apply", "kind": "action",
             "authority": "deployment.accounts.change.apply"},
        ]
        audit = deployment_modules["deployment.audit"]
        assert audit["artifact"]["scope"] == "deployment"
        assert audit["state"] == "ACTIVE"
        assert audit["management_contract"]["operations"] == [{
            "operation_id": "records",
            "kind": "query",
            "authority": "deployment.audit.read",
            "principal": "admin",
        }]
        assert audit["management_contributions"][0]["surface"] == {
            "kind": "trusted-module",
            "protocol": "forest.trusted-management-view/1",
            "template": "/static/modules/deployment.audit.html",
            "module": "/static/deployment-audit.js",
            "style": None,
        }
        assert (
            await client.get(f"/management/v1/scopes/host/{deployment_id}/modules")
        ).status_code == 404

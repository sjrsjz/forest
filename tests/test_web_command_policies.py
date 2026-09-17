"""Web-managed per-Binding command policy over real Bot PostgreSQL storage."""

import asyncio
from pathlib import Path

import pytest
from test_web_bindings import create_binding
from test_web_bots import ORIGIN, PASSWORD, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings
from forest.packages import PackageCatalog

pytestmark = pytest.mark.postgres


def command_source(tmp_path: Path, *, version="0.1.0", permission="command.hello") -> Path:
    source = tmp_path / ("policy-command-" + version)
    module = source / "src" / "policy_command"
    module.mkdir(parents=True)
    (source / "pyproject.toml").write_text(
        f'[project]\nname="policy-command"\nversion="{version}"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        f'''from forest.sdk import command, plugin
@plugin(id="policy.command", version="{version}")
class PolicyCommand:
    @command("/hello", "{permission}", id="hello", args="tail_text")
    async def hello(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    return source


async def provision(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    return token, settings


async def test_web_command_policy_requires_reauth_and_persists_source_body(
    tmp_path, web_bots_database
):
    token, settings = await provision(tmp_path, web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        assert "组件命令授权" in (await client.get("/")).text
        assert "export function commandPoliciesPanel" in (
            await client.get("/static/command-policies.js")
        ).text
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Command policy"},
            "create-command-policy-bot", status=202,
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        package = PackageCatalog(tmp_path / "packages").build(command_source(tmp_path))
        await managed.kernel.install(package)
        binding = await create_binding(client, root, {"self_id": "10001", "display_name": "QQ"}, "create-command-policy-binding")
        path = root + "/bindings/" + binding["binding_id"]
        response = await client.get(path + "/command-policies")
        assert response.status_code == 200, response.text
        initial = response.json()
        assert initial["registry_revision"] == 1
        assert initial["commands"] == [{
            **{key: initial["commands"][0][key] for key in (
                "command_id", "name", "component_id", "package_digest", "permission",
                "argument_mode", "priority", "spec_digest",
            )},
            "revision": 0,
            "values": {"mode": "DISABLED", "user_ids": []},
            "values_payload_state": "DEFAULT",
        }]
        command = initial["commands"][0]
        assert command["command_id"] == "hello" and command["name"] == "/hello"

        plan_url = path + "/commands/hello/policy-change-plans"
        plan_body = {
            "expected_revision": 0,
            "values": {"mode": "USER_SET", "user_ids": ["20002", "10003", "20002"]},
            "current_password": "wrong-password-value",
        }
        denied = await client.post(
            plan_url, json=plan_body, headers={"idempotency-key": "wrong-policy-password"}
        )
        assert denied.status_code == 403 and denied.json()["code"] == "REAUTHENTICATION_FAILED"

        plan_body["current_password"] = PASSWORD
        plan = await post(client, plan_url, plan_body, "plan-command-policy")
        assert plan["values"] == {"mode": "USER_SET", "user_ids": ["10003", "20002"]}
        assert PASSWORD not in str(plan)
        apply_url = path + "/commands/hello/policy-management-requests"
        apply_body = {
            "expected_revision": plan["expected_revision"], "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"], "current_password": PASSWORD,
        }
        applied = await post(client, apply_url, apply_body, "apply-command-policy")
        assert applied["revision"] == 1 and applied["status"] == "APPLIED"
        assert await post(client, apply_url, apply_body, "apply-command-policy") == applied

        saved = (await client.get(path + "/command-policies")).json()["commands"][0]
        assert saved["revision"] == 1
        assert saved["values"] == plan["values"]
        assert saved["values_payload_state"] == "ACTIVE"
        async with managed.store.pool.connection() as conn:
            operation = await (await conn.execute(
                "SELECT input_payload_id FROM public.bot_web_operations "
                "WHERE bot_id=%s AND action='command.policy.plan'",
                (bot["bot_id"],),
            )).fetchone()
            plan_row = await (await conn.execute(
                "SELECT resource_id,values_payload_id FROM public.bot_command_policy_plans "
                "WHERE bot_id=%s AND plan_id=%s", (bot["bot_id"], plan["plan_id"]),
            )).fetchone()
            parents = await (await conn.execute(
                "SELECT parent_payload_id FROM public.bot_payload_derivations "
                "WHERE bot_id=%s AND payload_id=%s", (bot["bot_id"], plan_row["values_payload_id"]),
            )).fetchall()
            request_bodies = await (await conn.execute(
                "SELECT convert_from(b.content,'UTF8') AS body FROM public.bot_payload_bodies b "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE h.bot_id=%s AND h.owner_kind='control.request'", (bot["bot_id"],),
            )).fetchall()
        assert parents == [{"parent_payload_id": operation["input_payload_id"]}]
        assert all(PASSWORD not in row["body"] for row in request_bodies)


async def test_policy_preview_is_rejected_after_command_spec_hot_reload(
    tmp_path, web_bots_database
):
    token, settings = await provision(tmp_path, web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Stale command policy"},
            "create-stale-policy-bot", status=202,
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        catalog = PackageCatalog(tmp_path / "packages")
        await managed.kernel.install(catalog.build(command_source(tmp_path, version="0.1.0")))
        binding = await create_binding(client, root, {"self_id": "10002", "display_name": "QQ"}, "create-stale-policy-binding")
        path = root + "/bindings/" + binding["binding_id"] + "/commands/hello"
        plan = await post(
            client, path + "/policy-change-plans",
            {"expected_revision": 0, "values": {"mode": "ADMINS", "user_ids": []},
             "current_password": PASSWORD},
            "stale-policy-plan",
        )
        await managed.kernel.install(catalog.build(command_source(
            tmp_path, version="0.2.0", permission="command.hello-v2"
        )))
        response = await client.post(
            path + "/policy-management-requests",
            json={"expected_revision": 0, "plan_id": plan["plan_id"],
                  "plan_digest": plan["plan_digest"], "current_password": PASSWORD},
            headers={"idempotency-key": "stale-policy-apply"},
        )
        assert response.status_code == 409
        assert response.json()["code"] == "COMMAND_POLICY_PLAN_STALE"
        policies = (await client.get(
            root + "/bindings/" + binding["binding_id"] + "/command-policies"
        )).json()
        assert policies["commands"][0]["revision"] == 0
        assert policies["commands"][0]["permission"] == "command.hello-v2"


async def test_command_policy_expected_revision_has_single_winner(tmp_path, web_bots_database):
    token, settings = await provision(tmp_path, web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Policy race"},
            "create-policy-race-bot", status=202,
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        await managed.kernel.install(PackageCatalog(tmp_path / "packages").build(command_source(tmp_path)))
        binding = await create_binding(client, root, {"self_id": "10003", "display_name": "QQ"}, "create-policy-race-binding")
        path = root + "/bindings/" + binding["binding_id"] + "/commands/hello"
        plans = []
        for index, mode in enumerate(("EVERYONE", "ADMINS")):
            plans.append(await post(
                client, path + "/policy-change-plans",
                {"expected_revision": 0, "values": {"mode": mode, "user_ids": []},
                 "current_password": PASSWORD},
                f"race-plan-{index}",
            ))
        responses = await asyncio.gather(*(
            client.post(
                path + "/policy-management-requests",
                json={"expected_revision": 0, "plan_id": plan["plan_id"],
                      "plan_digest": plan["plan_digest"], "current_password": PASSWORD},
                headers={"idempotency-key": f"race-apply-{index}"},
            )
            for index, plan in enumerate(plans)
        ))
        assert sorted(response.status_code for response in responses) == [200, 409]
        assert next(response for response in responses if response.status_code == 409).json()[
            "code"
        ] == "REVISION_CONFLICT"

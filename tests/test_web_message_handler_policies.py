"""Web-managed per-Binding message handler policy over real Bot PostgreSQL storage."""

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


def handler_source(tmp_path: Path, *, version="0.1.0", priority=10) -> Path:
    source = tmp_path / ("policy-handler-" + version)
    module = source / "src" / "policy_handler"
    module.mkdir(parents=True)
    (source / "pyproject.toml").write_text(
        f'[project]\nname="policy-handler"\nversion="{version}"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        f'''from forest.sdk import Pass, message_handler, plugin
@plugin(id="policy.handler", version="{version}")
class PolicyHandler:
    @message_handler("ordinary", priority={priority})
    async def ordinary(self, event, ctx):
        return Pass()
''',
        encoding="utf-8",
    )
    return source


async def provision(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    return token, settings


async def test_web_message_handler_policy_requires_reauth_and_persists_source_body(
    tmp_path, web_bots_database
):
    token, settings = await provision(tmp_path, web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        assert "普通群消息处理策略" in (await client.get("/")).text
        assert "export function messageHandlerPoliciesPanel" in (
            await client.get("/static/message-handler-policies.js")
        ).text
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Message handler policy"},
            "create-handler-policy-bot", status=202,
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        package = PackageCatalog(tmp_path / "packages").build(handler_source(tmp_path))
        await managed.kernel.install(package)
        binding = await create_binding(client, root, {"self_id": "10001", "display_name": "QQ"}, "create-handler-policy-binding")
        path = root + "/bindings/" + binding["binding_id"]
        response = await client.get(path + "/message-handler-policies")
        assert response.status_code == 200, response.text
        initial = response.json()
        assert initial["registry_revision"] == 1
        assert initial["handlers"] == [{
            **{key: initial["handlers"][0][key] for key in (
                "handler_id", "handler", "component_id", "package_digest", "priority",
                "spec_digest",
            )},
            "revision": 0,
            "values": {"mode": "DISABLED", "user_ids": []},
            "values_payload_state": "DEFAULT",
        }]
        handler = initial["handlers"][0]
        assert handler["handler_id"] == "ordinary" and handler["handler"] == "ordinary"

        policy_path = path + "/components/policy.handler/message-handlers/ordinary"
        plan_url = policy_path + "/policy-change-plans"
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
        plan = await post(client, plan_url, plan_body, "plan-handler-policy")
        assert plan["values"] == {"mode": "USER_SET", "user_ids": ["10003", "20002"]}
        assert PASSWORD not in str(plan)
        apply_url = policy_path + "/policy-management-requests"
        apply_body = {
            "expected_revision": plan["expected_revision"], "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"], "current_password": PASSWORD,
        }
        applied = await post(client, apply_url, apply_body, "apply-handler-policy")
        assert applied["revision"] == 1 and applied["status"] == "APPLIED"
        assert await post(client, apply_url, apply_body, "apply-handler-policy") == applied

        saved = (await client.get(path + "/message-handler-policies")).json()["handlers"][0]
        assert saved["revision"] == 1
        assert saved["values"] == plan["values"]
        assert saved["values_payload_state"] == "ACTIVE"
        async with managed.store.pool.connection() as conn:
            operation = await (await conn.execute(
                "SELECT input_payload_id FROM public.bot_web_operations "
                "WHERE bot_id=%s AND action='message.handler.policy.plan'",
                (bot["bot_id"],),
            )).fetchone()
            plan_row = await (await conn.execute(
                "SELECT resource_id,values_payload_id FROM public.bot_message_handler_policy_plans "
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


async def test_policy_preview_is_rejected_after_handler_spec_hot_reload(
    tmp_path, web_bots_database
):
    token, settings = await provision(tmp_path, web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Stale message handler policy"},
            "create-stale-policy-bot", status=202,
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        catalog = PackageCatalog(tmp_path / "packages")
        await managed.kernel.install(catalog.build(handler_source(tmp_path, version="0.1.0")))
        binding = await create_binding(client, root, {"self_id": "10002", "display_name": "QQ"}, "create-stale-policy-binding")
        path = (root + "/bindings/" + binding["binding_id"]
                + "/components/policy.handler/message-handlers/ordinary")
        plan = await post(
            client, path + "/policy-change-plans",
            {"expected_revision": 0, "values": {"mode": "ADMINS", "user_ids": []},
             "current_password": PASSWORD},
            "stale-policy-plan",
        )
        await managed.kernel.install(catalog.build(handler_source(
            tmp_path, version="0.2.0", priority=20
        )))
        response = await client.post(
            path + "/policy-management-requests",
            json={"expected_revision": 0, "plan_id": plan["plan_id"],
                  "plan_digest": plan["plan_digest"], "current_password": PASSWORD},
            headers={"idempotency-key": "stale-policy-apply"},
        )
        assert response.status_code == 409
        assert response.json()["code"] == "MESSAGE_HANDLER_POLICY_PLAN_STALE"
        policies = (await client.get(
            root + "/bindings/" + binding["binding_id"] + "/message-handler-policies"
        )).json()
        assert policies["handlers"][0]["revision"] == 0
        assert policies["handlers"][0]["priority"] == 20


async def test_message_handler_policy_expected_revision_has_single_winner(tmp_path, web_bots_database):
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
        await managed.kernel.install(PackageCatalog(tmp_path / "packages").build(handler_source(tmp_path)))
        binding = await create_binding(client, root, {"self_id": "10003", "display_name": "QQ"}, "create-policy-race-binding")
        path = (root + "/bindings/" + binding["binding_id"]
                + "/components/policy.handler/message-handlers/ordinary")
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

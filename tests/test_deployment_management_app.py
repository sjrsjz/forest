from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

from httpx import ASGITransport, AsyncClient

from forest.clean_management_contract import (
    CleanManagementActionResult,
    CleanModuleConfigurationContract,
)
from forest.deployment_management_app import create_deployment_management_app
from forest.errors import ForestError


class _Configuration:
    component_id = "test.component"

    async def desired(self, *, include_value=False):
        return SimpleNamespace(
            revision=1,
            payload_id="payload/desired",
            payload_digest="sha256:" + "1" * 64,
            validation_digest="sha256:" + "2" * 64,
            value={"enabled": True} if include_value else None,
        )

    async def effective(self, *, include_value=False):
        return await self.desired(include_value=include_value)


class _GenerationAuthority:
    async def status(self):
        return {
            "bot_id": "bot/managed",
            "graph_state": "ACTIVE",
            "graph_id": "graph/managed",
        }


class _Contract(CleanModuleConfigurationContract):
    async def query(self, query_id, payload):
        return {"query_id": query_id, "payload": payload}

    async def action(self, action_id, *, operation_id, payload):
        return CleanManagementActionResult(
            {"action_id": action_id, "operation_id": operation_id, "payload": payload}
        )


class _Registry:
    def __init__(self, module):
        self.module = module
        self.graph_identity = SimpleNamespace(graph_id="graph/managed", graph_revision=1)

    def get(self, component_id):
        if component_id != "test.component":
            raise KeyError(component_id)
        return self.module

    def describe(self):
        return [
            {
                "component_id": "test.component",
                "state": "ACTIVE",
                "generation_id": "generation/test/1",
                "management_contributions": [],
                "management_contract": self.module.management_contract.describe(),
            }
        ]


async def test_deployment_gateway_routes_one_online_bot_scope_without_merging_registries():
    contract = _Contract(
        _Configuration(),
        schema={"type": "object", "properties": {"enabled": {"type": "boolean"}}},
    )
    host = SimpleNamespace(
        settings=SimpleNamespace(bot_id="bot/managed"),
        generation_authority=_GenerationAuthority(),
    )
    host.registry = _Registry(SimpleNamespace(management_contract=contract))

    class Supervisor:
        async def management_scopes(self):
            return (
                {
                    "scope_id": "bot-scope-test",
                    "scope_kind": "bot",
                    "bot_id": "bot/managed",
                    "label": "Managed",
                    "available": True,
                    "desired_state": "ONLINE",
                    "observed_state": "ONLINE",
                },
            )

        @asynccontextmanager
        async def management_host(self, scope_id):
            if scope_id != "bot-scope-test":
                raise ForestError(
                    "BOT_MANAGEMENT_SCOPE_NOT_FOUND", "Bot scope does not exist", 404
                )
            yield host

    app = create_deployment_management_app(
        deployment_id="deployment/test",
        components=(),
        access_token="deployment-management-token",
        bot_supervisor=Supervisor(),
    )
    headers = {"x-forest-management-token": "deployment-management-token"}
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://forest.test"
    ) as client:
        deployment = await client.get("/management/v1/runtime", headers=headers)
        assert [scope["scope_id"] for scope in deployment.json()["scopes"]] == [
            "deployment",
            "bot-scope-test",
        ]
        assert (
            await client.get("/management/v1/scopes/bot/bot-scope-test/runtime")
        ).status_code == 401
        runtime = await client.get(
            "/management/v1/scopes/bot/bot-scope-test/runtime", headers=headers
        )
        assert runtime.json()["runtime"]["bot_id"] == "bot/managed"
        assert [item["component_id"] for item in runtime.json()["modules"]] == [
            "test.component"
        ]
        configuration = await client.get(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/configuration",
            headers=headers,
        )
        assert configuration.status_code == 200
        assert configuration.json()["desired"]["value"] == {"enabled": True}
        query = await client.post(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/queries/test.query",
            headers=headers,
            json={"cursor": "next"},
        )
        assert query.json() == {
            "query_id": "test.query",
            "payload": {"cursor": "next"},
        }
        action = await client.post(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/actions/test.action",
            headers={**headers, "idempotency-key": "bot-action-1"},
            json={"enabled": False},
        )
        assert action.status_code == 200
        assert action.json()["action_id"] == "test.action"
        assert action.json()["operation_id"].startswith("web:sha256:")
        assert action.json()["graph_id"] == "graph/managed"
        missing = await client.get(
            "/management/v1/scopes/bot/missing/runtime", headers=headers
        )
        assert missing.status_code == 404

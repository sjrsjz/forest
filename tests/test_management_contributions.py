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
        assert modules["conversation.scheduler"]["management_contributions"] == []
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
        assert (await client.get(adapter + "/bindings")).json() == {"bindings": []}
        assert (await client.get(adapter + "/conversations")).json() == {
            "conversations": []
        }
        assert (
            await client.get(f"/management/v1/bots/{bot['bot_id']}/jobs")
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
        invalid = await client.get(
            f"/management/v1/scopes/deployment/{bot['bot_id']}/modules"
        )
        assert invalid.status_code == 404

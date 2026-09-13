"""HTTP exposure of redacted Bot audit metadata."""

import pytest
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings

pytestmark = pytest.mark.postgres


async def test_bot_audit_endpoint_exposes_only_allowlisted_metadata(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with running(
        WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    ) as (app, client):
        await setup(client, token)
        bot = await post(
            client,
            "/management/v1/bots",
            {"display_name": "Audit Bot"},
            "create-audit-bot",
            status=202,
        )
        await ready(client, bot)
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        async with managed.store.pool.connection() as conn, conn.transaction():
            await managed.store._event(
                conn,
                "fixture.http",
                "runtime:http-audit",
                {"secret": "http-secret", "message": "must-not-reach-browser"},
            )

        root = f"/management/v1/bots/{bot['bot_id']}/audit"
        response = await client.get(root + "?action=fixture.http&limit=1")
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["next_before"] is None and len(result["records"]) == 1
        row = result["records"][0]
        assert row["action"] == "fixture.http" and row["origin_kind"] == "runtime"
        assert set(row) == {
            "audit_id", "action", "operation_id", "origin_kind", "has_receipt", "created_at"
        }
        assert "http-secret" not in response.text
        assert "must-not-reach-browser" not in response.text

        unknown = await client.get(root + "?details=true")
        assert unknown.status_code == 422
        assert unknown.json()["code"] == "AUDIT_QUERY_INVALID"

"""Actual HTTP history copies retain their selected revision through publication."""

import json
from types import SimpleNamespace

import pytest
from test_memory_config_payloads import forget_input
from test_web_bots import (
    ORIGIN,
    apply,
    effective,
    make_plan,
    package_query,
    post,
    ready,
    running,
    setup,
)
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings


@pytest.mark.postgres
async def test_http_history_copy_tracks_real_revision_and_erasure(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        created = await post(
            client, "/management/v1/bots", {"display_name": "History"}, "create", status=202
        )
        await ready(client, created)
        packages = (await package_query(client))["packages"]
        package = next(row for row in packages if row["component_id"] == "forest.counter")
        base = f"/management/v1/bots/{created['bot_id']}/components/forest.counter"
        first = await make_plan(client, base, package, "original", 2, 0, "initial-plan")
        await apply(client, base, first, "initial-apply")
        await effective(client, base, 1)
        bot = app.state.runtime.fleet.bots[created["bot_id"]]
        async with bot.store.pool.connection() as conn:
            original = await (
                await conn.execute(
                    "SELECT w.operation_id,b.content FROM public.bot_control_value_sources s "
                    "JOIN public.bot_web_operations w ON w.bot_id=s.bot_id AND w.operation_id=s.operation_id "
                    "JOIN public.bot_payload_bodies b ON b.bot_id=w.bot_id AND b.payload_id=w.input_payload_id "
                    "WHERE s.bot_id=%s AND s.family='config' AND s.source_kind='PLAN' AND s.source_id=%s",
                    (bot.store.bot_id, first["plan_id"]),
                )
            ).fetchone()
        assert "source_revision" not in json.loads(bytes(original["content"]))["value"]
        history = await client.get(base + "/configuration/history")
        assert history.status_code == 200, history.text
        historical = history.json()["revisions"][0]
        values = historical["values"]
        values["config"]["label"] = "edited-history"
        body = {"expected_revision": 1, "values": values, "source_revision": historical["revision"]}
        for invalid in (
            {**body, "source_revision": True},
            {**body, "source_family": "agent.config"},
        ):
            rejected = await client.post(
                base + "/change-plans", json=invalid, headers={"idempotency-key": "invalid-source"}
            )
            assert rejected.status_code == 422, rejected.text
        copied = await post(client, base + "/change-plans", body, "copy-plan")
        await apply(client, base, copied, "copy-apply")
        current = await effective(client, base, 2)
        assert current["effective_values"]["config"]["label"] == "edited-history"
        async with bot.store.pool.connection() as conn:
            inherited = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_control_value_sources s JOIN public.bot_web_operations w "
                    "ON w.bot_id=s.bot_id AND w.operation_id=s.operation_id "
                    "JOIN public.bot_payload_derivations d ON d.bot_id=w.bot_id AND d.payload_id=w.input_payload_id "
                    "JOIN public.bot_config_revisions r ON r.bot_id=d.bot_id AND r.values_payload_id=d.parent_payload_id "
                    "WHERE s.source_id=%s AND s.family='config' AND s.source_kind='PLAN' "
                    "AND r.component_id='forest.counter' AND r.revision=1",
                    (copied["plan_id"],),
                )
            ).fetchone()
        assert inherited
        await forget_input(
            SimpleNamespace(store=bot.store, lease=bot.kernel._authority()),
            original["operation_id"],
        )
        read = await client.get(base + "/configuration")
        assert read.status_code == 200, read.text
        assert read.json()["desired_revision"] == read.json()["effective_revision"] == 2
        assert read.json()["desired_values"] is read.json()["effective_values"] is None
        assert (
            read.json()["desired_values_payload_state"]
            == read.json()["effective_values_payload_state"]
            == "UNAVAILABLE"
        )
        replay = await post(client, base + "/change-plans", body, "copy-plan")
        assert (
            replay["operation_status"] == "RECORDED"
            and replay["result_payload_state"] == "UNAVAILABLE"
        )
        rejected = await post(
            client,
            base + "/change-plans",
            {**body, "expected_revision": 2},
            "late-copy",
            status=409,
        )
        assert rejected["code"] == "CONTROL_CONTENT_REVOKED"

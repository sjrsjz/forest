"""Actual management HTTP -> independent Bot configuration -> restart recovery."""

import asyncio
import json

import httpx
import pytest
from test_web_bindings import configure, get, onebot_server, receive, wait_for
from test_web_bots import ORIGIN, login, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings

pytestmark = pytest.mark.postgres


async def read(client, root):
    response = await client.get(root + "/memory/configuration")
    assert response.status_code == 200, response.text
    return response.json()


async def preview(client, root, *, revision=0, key="plan", **changes):
    return await post(
        client,
        root + "/memory/change-plans",
        {
            "expected_revision": revision,
            "values": {"enabled": False, **changes},
            "api_key": "test-private-memory-key" if revision == 0 else None,
        },
        key,
    )


async def apply(client, root, plan, key="apply", status=200):
    return await post(
        client,
        root + "/memory/management-requests",
        {name: plan[name] for name in ("expected_revision", "plan_id", "plan_digest")},
        key,
        status=status,
    )


async def test_two_bots_memory_secrets_cas_and_restart(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        bots = [
            await post(client, "/management/v1/bots", {"display_name": name}, name, status=202)
            for name in ("Memory A", "Memory B")
        ]
        await asyncio.gather(*(ready(client, bot) for bot in bots))
        roots = ["/management/v1/bots/" + bot["bot_id"] for bot in bots]
        first = await preview(client, roots[0], daily_request_budget=101)
        assert await preview(client, roots[0], daily_request_budget=101) == first
        accepted = await apply(client, roots[0], first)
        assert await apply(client, roots[0], first) == accepted
        assert accepted["revision"] == 1
        saved = await read(client, roots[0])
        assert saved["has_api_key"] and saved["values"]["daily_request_budget"] == 101
        assert (await read(client, roots[1]))["revision"] == 0
        assert "test-private-memory-key" not in json.dumps([first, accepted, saved])
        # Different target Bot cannot adopt this Bot's signed/configured plan.
        await apply(client, roots[1], first, "foreign-plan", status=404)
        a = await preview(client, roots[0], revision=1, key="draft-a", batch_size=12)
        b = await preview(client, roots[0], revision=1, key="draft-b", batch_size=14)
        await apply(client, roots[0], a, "apply-a")
        conflict = await apply(client, roots[0], b, "apply-b", status=409)
        assert conflict["code"] == "REVISION_CONFLICT"
        saved = await read(client, roots[0])
        assert saved["revision"] == 2 and saved["values"]["batch_size"] == 12
        assert saved["has_api_key"] and saved["secret_revision"] == 1
        status = await client.get(roots[0] + "/memory/status")
        assert status.status_code == 200, status.text
        assert (
            "test-private-memory-key" not in status.text and "secret_ciphertext" not in status.text
        )
        bot = await app.state.runtime.fleet.attach(bots[0]["bot_id"])
        runtime = await bot.platform.module("memory.graph").config.runtime(
            bot.kernel._authority()
        )
        assert (
            app.state.runtime.catalog.vault.decrypt(runtime["secret_ciphertext"])
            == "test-private-memory-key"
        )
    async with running(settings) as (_, client):
        await login(client)
        assert await read(client, roots[0]) == saved
        assert (await read(client, roots[1]))["revision"] == 0
        clear = await post(
            client,
            roots[0] + "/memory/change-plans",
            {
                "expected_revision": 2,
                "values": saved["values"],
                "clear_api_key": True,
            },
            "clear-plan",
        )
        await apply(client, roots[0], clear, "clear-apply")
        current = await read(client, roots[0])
        assert not current["has_api_key"] and current["secret_revision"] == 2


async def test_memory_http_requires_session_csrf_exact_schema_and_idempotency(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with running(WebSettings(data, ORIGIN, managed_dsn=web_bots_database)) as (app, client):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Memory auth"}, "bot", status=202
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        body = {"expected_revision": 0, "values": {"enabled": False}}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ORIGIN
        ) as outsider:
            result = await outsider.get(root + "/memory/configuration")
            assert result.status_code == 401
        denied = await client.post(
            root + "/memory/change-plans",
            json=body,
            headers={"x-csrf-token": "invalid", "idempotency-key": "csrf"},
        )
        assert denied.status_code == 403
        assert (await client.post(root + "/memory/change-plans", json=body)).status_code == 422
        for extra in (
            {"api_key": "plaintext-invalid\nkey"},
            {"api_key": "replacement", "clear_api_key": True},
            {"values": {"media_max_bytes": 4194305}},
            {"values": {"dimensions": True}},
            {"values": {"secret": "forbidden"}},
        ):
            rejected = await client.post(
                root + "/memory/change-plans",
                json={**body, **extra},
                headers={"idempotency-key": "invalid"},
            )
            assert rejected.status_code == 422, rejected.text
            assert "plaintext-invalid" not in rejected.text
        assert (await read(client, root))["revision"] == 0


async def test_web_lists_and_forgets_a_reviewed_multigroup_memory_source(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with onebot_server() as server:
        async with running(settings) as (_, client):
            await setup(client, token)
            bot = await post(
                client, "/management/v1/bots", {"display_name": "Memory forget"},
                "forget-bot", status=202,
            )
            await ready(client, bot)
            root = "/management/v1/bots/" + bot["bot_id"]
            binding = await post(
                client, root + "/bindings",
                {"self_id": "10001", "display_name": "Memory QQ"},
                "forget-binding", status=201,
            )
            await configure(
                client, root + "/bindings/" + binding["binding_id"], server["origin"],
                response_mode="disabled",
            )
            assert (await receive(
                client, binding["binding_id"], 41, "remember image description", group_id=31001
            )).status_code == 200
            assert (await receive(
                client, binding["binding_id"], 42, "other group stays", group_id=31002
            )).status_code == 200
            await wait_for(
                lambda: get(client, root + "/conversations"),
                lambda value: value["conversations"]
                and len(value["conversations"]) == 2
                and all(row["context_applied_seq"] == 1 for row in value["conversations"]),
            )
            listed = await get(client, root + "/memory/sources?limit=50")
            assert listed["deletion_epoch"] == 0 and len(listed["sources"]) == 2
            source = next(
                row for row in listed["sources"] if row["source"]["channel_id"] == "31001"
            )
            other = next(
                row for row in listed["sources"] if row["source"]["channel_id"] == "31002"
            )
            assert source["source"]["channel_id"] == "31001"
            assert source["author"]["external_user_id"] == "20001"
            association = await post(
                client,
                root + "/memory/association-plans",
                {
                    "expected_deletion_epoch": 0,
                    "expected_revision": 0,
                    "active": True,
                    "source_unit_ids": [source["source_unit_id"], other["source_unit_id"]],
                },
                "association-plan",
            )
            association_request = {
                key: association[key]
                for key in ("expected_deletion_epoch", "plan_id", "plan_digest")
            }
            associated = await post(
                client,
                root + "/memory/association-requests",
                association_request,
                "association-apply",
            )
            assert await post(
                client,
                root + "/memory/association-requests",
                association_request,
                "association-apply",
            ) == associated
            current_associations = await wait_for(
                lambda: get(client, root + "/memory/associations?limit=50"),
                lambda value: len(value["associations"]) == 1
                and value["associations"][0]["projection_state"] == "CURRENT",
            )
            assert {
                current_associations["associations"][0][side]["source"]["channel_id"]
                for side in ("left", "right")
            } == {"31001", "31002"}
            proposal = await post(
                client,
                root + "/memory/forget-plans",
                {"expected_deletion_epoch": 0, "source_unit_ids": [source["source_unit_id"]]},
                "forget-plan",
            )
            request = {key: proposal[key] for key in (
                "expected_deletion_epoch", "plan_id", "plan_digest"
            )}
            applied = await post(
                client, root + "/memory/forget-requests", request, "forget-apply"
            )
            assert await post(
                client, root + "/memory/forget-requests", request, "forget-apply"
            ) == applied
            assert applied["deletion_epoch"] == 1
            current = await get(client, root + "/memory/sources?limit=50")
            assert current["deletion_epoch"] == 1
            assert {row["source"]["channel_id"] for row in current["sources"]} == {"31002"}
            assert (await get(client, root + "/memory/associations?limit=50"))[
                "associations"
            ] == []
            drained = await wait_for(
                lambda: get(client, root + "/memory/sources?limit=50"),
                lambda value: value["scrub"] == {"revoked_payloads": 0, "pending_links": 0},
            )
            assert drained["deletion_epoch"] == 1
            bad = await client.get(root + "/memory/sources?unexpected=true")
            assert bad.status_code == 422
            bad = await client.get(root + "/memory/associations?unexpected=true")
            assert bad.status_code == 422

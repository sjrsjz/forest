"""Real Web authority and PostgreSQL coverage for component-event management."""

import asyncio

import httpx
import pytest
from test_events import event_package, install_spec, wait_deliveries
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.errors import ForestError
from forest.management.app import WebSettings
from forest.storage.events import EventStore

pytestmark = pytest.mark.postgres


async def managed_bot(tmp_path, app, client, name):
    bot = await post(
        client, "/management/v1/bots", {"display_name": name}, name, status=202
    )
    await ready(client, bot)
    managed = await app.state.runtime.fleet.attach(bot["bot_id"])
    return bot, managed, "/management/v1/bots/" + bot["bot_id"] + "/events"


async def test_web_replays_dead_delivery_with_new_epoch_and_stable_operation(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with running(WebSettings(data, ORIGIN, managed_dsn=web_bots_database)) as (
        app,
        client,
    ):
        await setup(client, token)
        _, managed, root = await managed_bot(tmp_path, app, client, "Event replay")
        consumer = event_package(
            tmp_path, "failing", subscriber_id="profile.web-replay", fail=True,
            max_attempts=1,
        )
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await managed.kernel.install_many(
            [install_spec(consumer), install_spec(publisher)]
        )
        await managed.kernel.invoke(
            "publisher", "publisher.run", {"value": 31}, operation_id="web-replay-event"
        )
        dead = (
            await wait_deliveries(
                managed.store,
                lambda rows: len(rows) == 1 and rows[0]["status"] == "DEAD",
            )
        )[0]
        old_operation = dead["call_operation_id"]
        managed.kernel._event_dispatcher.cancel()
        await asyncio.gather(managed.kernel._event_dispatcher, return_exceptions=True)

        view = await client.get(root + "?status=DEAD&limit=10")
        assert view.status_code == 200, view.text
        public = view.json()["deliveries"][0]
        assert public["replay_allowed"] is True
        assert "payload" not in public and "result" not in public and "claim_token" not in public

        request = {"delivery_id": dead["delivery_id"], "expected_revision": 0}
        attempts = await asyncio.gather(*(
            client.post(
                root + "/replays", json=request, headers={"idempotency-key": key}
            )
            for key in ("replay-race-a", "replay-race-b")
        ))
        assert sorted(response.status_code for response in attempts) == [200, 409]
        accepted_response = next(response for response in attempts if response.status_code == 200)
        rejected_response = next(response for response in attempts if response.status_code == 409)
        accepted_key = (
            "replay-race-a" if attempts[0] is accepted_response else "replay-race-b"
        )
        accepted = accepted_response.json()
        assert rejected_response.json()["code"] == "EVENT_DELIVERY_REVISION_CONFLICT"
        assert await post(client, root + "/replays", request, accepted_key) == accepted
        assert accepted["replay_epoch"] == 1 and accepted["management_revision"] == 1
        conflict = await post(
            client, root + "/replays", request, "replay-after-race", status=409
        )
        assert conflict["code"] == "EVENT_DELIVERY_REVISION_CONFLICT"

        replayed = await EventStore(managed.store).claim(
            managed.kernel.lease, lease_seconds=3
        )
        assert replayed is not None
        assert replayed["replay_epoch"] == 1
        assert replayed["call_operation_id"] != old_operation
        await managed.kernel._deliver_event(replayed)
        rows = await EventStore(managed.store).deliveries()
        assert rows[0]["status"] == "DEAD" and rows[0]["attempt_count"] == 1

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ORIGIN
        ) as outsider:
            assert (await outsider.get(root)).status_code == 401
        assert (await client.post(root + "/replays", json=request)).status_code == 422
        invalid = await client.post(
            root + "/replays",
            json={**request, "unexpected": True},
            headers={"idempotency-key": "invalid-event-replay"},
        )
        assert invalid.status_code == 422


async def test_retirement_fences_claims_and_allows_only_removal_deployment(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with running(WebSettings(data, ORIGIN, managed_dsn=web_bots_database)) as (
        app,
        client,
    ):
        await setup(client, token)
        _, managed, root = await managed_bot(tmp_path, app, client, "Event retire")
        subscriber_id = "profile.web-retire"
        consumer = event_package(
            tmp_path, "consumer", subscriber_id=subscriber_id, max_attempts=3,
            version="0.1.0",
        )
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await managed.kernel.install_many(
            [install_spec(consumer), install_spec(publisher)]
        )
        managed.kernel._event_dispatcher.cancel()
        await asyncio.gather(managed.kernel._event_dispatcher, return_exceptions=True)
        await managed.kernel.invoke(
            "publisher", "publisher.run", {"value": 41}, operation_id="retire-pending"
        )
        events = EventStore(managed.store)
        claimed = await events.claim(managed.kernel.lease, lease_seconds=3)
        assert claimed is not None

        proposal = await post(
            client,
            root + "/retirement-plans",
            {"subscriber_id": subscriber_id, "contract_major": 1, "expected_revision": 0},
            "retire-busy-plan",
        )
        request = {
            "expected_revision": proposal["expected_revision"],
            "plan_id": proposal["plan_id"],
            "plan_digest": proposal["plan_digest"],
        }
        busy_result = await post(
            client, root + "/retirement-requests", request, "retire-busy", status=409
        )
        assert busy_result["code"] == "EVENT_SUBSCRIPTION_BUSY"
        await events.defer(managed.kernel.lease, claimed, delay_seconds=60)

        proposal = await post(
            client,
            root + "/retirement-plans",
            {"subscriber_id": subscriber_id, "contract_major": 1, "expected_revision": 0},
            "retire-ready-plan",
        )
        request = {
            "expected_revision": proposal["expected_revision"],
            "plan_id": proposal["plan_id"],
            "plan_digest": proposal["plan_digest"],
        }
        retired = await post(
            client, root + "/retirement-requests", request, "retire-ready"
        )
        assert await post(
            client, root + "/retirement-requests", request, "retire-ready"
        ) == retired
        assert retired["management_revision"] == 1
        assert retired["retired_delivery_count"] == 1
        rows = await events.deliveries()
        assert rows[0]["status"] == "RETIRED"
        assert rows[0]["last_error_code"] == "SUBSCRIPTION_RETIRED"
        assert await events.claim(managed.kernel.lease, lease_seconds=3) is None

        removed = event_package(tmp_path, "consumer", version="0.2.0")
        await managed.kernel.install(removed)
        restored = event_package(
            tmp_path, "consumer", subscriber_id=subscriber_id, version="0.3.0"
        )
        with pytest.raises(ForestError) as reactivation:
            await managed.kernel.install(
                restored, grants=frozenset({"state.read", "state.write"})
            )
        assert reactivation.value.code == "SUBSCRIPTION_REACTIVATION_REQUIRES_MANAGEMENT"

        await managed.kernel.invoke(
            "publisher", "publisher.run", {"value": 42}, operation_id="after-retirement"
        )
        assert len(await events.deliveries()) == 1
        view = (await client.get(root + "?limit=20")).json()
        assert view["subscriptions"][0]["active"] is False
        assert view["deliveries"][0]["status"] == "RETIRED"

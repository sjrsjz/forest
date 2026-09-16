"""Job administration through HTTP, private Bot storage and real provider sockets."""

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from test_web_agent import MODEL_KEY, model_server
from test_web_bindings import configure, get, onebot_server, receive, wait_for
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings

pytestmark = pytest.mark.postgres


def definition(conversation_id, **changes):
    start = datetime.now(UTC) + timedelta(days=1)
    return {
        "name": "Daily group report", "enabled": True, "kind": "conversation.trigger",
        "conversation_id": conversation_id, "prompt": "scheduled daily report",
        "schedule": {"kind": "interval", "timezone": "Asia/Shanghai",
                     "start_at": start.isoformat(), "end_at": None,
                     "interval_seconds": 3600, "cron": None, "dst_fold": "first"},
        "misfire": "coalesce", "misfire_grace_seconds": 60, "catch_up_limit": 3,
        "max_overlap": 1, "delegation_expires_at": (start + timedelta(days=7)).isoformat(),
        "max_runs": 100, **changes,
    }


@asynccontextmanager
async def configured(tmp_path, dsn):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=dsn)
    async with (onebot_server() as onebot, model_server() as model,
                running(settings) as (app, client)):
        model["release_first"].set()
        await setup(client, token)
        bot = await post(client, "/management/v1/bots", {"display_name": "Scheduled"},
                         "bot", status=202)
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        plan = await post(client, root + "/runtime-modules/agent.runtime/actions/configuration.plan", {
            "expected_revision": 0, "api_key": MODEL_KEY,
            "values": {"enabled": True, "endpoint": model["endpoint"],
                       "model": "test-model", "stream": False},
        }, "agent-plan")
        await post(client, root + "/runtime-modules/agent.runtime/actions/configuration.apply", {
            key: plan[key] for key in ("expected_revision", "plan_id", "plan_digest")
        }, "agent-apply")
        binding = await post(client, root + "/bindings", {
            "self_id": "10001", "display_name": "QQ",
        }, "binding", status=201)
        await configure(client, root + "/bindings/" + binding["binding_id"],
                        onebot["origin"], response_mode="agent")
        assert (await receive(client, binding["binding_id"], 1, "ordinary group context")).status_code == 200
        groups = await wait_for(lambda: get(client, root + "/conversations"),
                               lambda row: bool(row["conversations"]))
        yield app, client, bot, root, groups["conversations"][0], model, onebot


async def planned(client, root, values, *, revision=0, key="plan", job_id="daily"):
    return await post(client, root + "/jobs/change-plans", {
        "job_id": job_id, "expected_revision": revision, "values": values,
    }, key)


async def applied(client, root, plan, *, key="apply", status=200):
    return await post(client, root + "/jobs/management-requests", {
        field: plan[field] for field in ("job_id", "expected_revision", "plan_id", "plan_digest")
    }, key, status=status)


async def test_web_job_plan_manual_run_is_durable_and_delivered_once(tmp_path, web_bots_database):
    async with configured(tmp_path, web_bots_database) as (_, client, _, root, group, model, onebot):
        values = definition(group["conversation_id"])
        plan = await planned(client, root, values)
        assert plan["before_values"] is None and plan["next_occurrences"]
        assert await planned(client, root, values) == plan
        result = await applied(client, root, plan)
        assert result["revision"] == 1
        assert await applied(client, root, plan) == result
        listing = await get(client, root + "/jobs")
        job = listing["jobs"][0]
        assert job["conversation"]["channel_id"] == "30001"
        assert job["values"]["prompt"] == values["prompt"]
        assert job["delegation"]["subject_id"]
        assert not model["requests"] and not onebot["sends"]
        body = {"job_id": "daily", "expected_revision": 1}
        queued = await post(client, root + "/jobs/run-requests", body, "manual-one")
        assert await post(client, root + "/jobs/run-requests", body, "manual-one") == queued
        committed = await wait_for(lambda: get(client, root + "/jobs"),
                                  lambda row: any(item["reply_id"] for item in row["occurrences"]))
        occurrence = committed["occurrences"][0]
        assert occurrence["run_id"] and occurrence["event_id"] and occurrence["trigger_id"]
        await wait_for(lambda: get(client, root + "/jobs"),
                       lambda row: row["occurrences"][0]["delivery_state"] == "SENT")
        no_repeat = await post(client, root + "/jobs/replays", {
            "occurrence_id": occurrence["occurrence_id"], "expected_revision": occurrence["revision"],
        }, "no-replay-after-ingress", status=409)
        assert no_repeat["code"] == "JOB_REPLAY_CONFLICT"
        assert len(model["requests"]) == len(onebot["sends"]) == 1
        assert values["prompt"] in json.dumps(model["requests"][0])
        assert "30001" in json.dumps(model["requests"][0])
        assert not model["errors"]
        cancelled = await post(client, root + "/jobs/cancellations", {
            **body, "scope": "future", "reason": "Stop future report generation",
        }, "stop")
        assert cancelled["revision"] == 2
        final = await get(client, root + "/jobs")
        assert not final["jobs"][0]["enabled"]
        assert final["occurrences"][0]["occurrence_id"] == occurrence["occurrence_id"]


async def test_web_jobs_cas_authority_and_bot_source_isolation(tmp_path, web_bots_database):
    async with configured(tmp_path, web_bots_database) as (app, client, _, root, group, _, _):
        values = definition(group["conversation_id"])
        await applied(client, root, await planned(client, root, values))
        first = await planned(client, root, {**values, "name": "Editor A"}, revision=1, key="edit-a")
        second = await planned(client, root, {**values, "name": "Editor B"}, revision=1, key="edit-b")
        assert (await applied(client, root, first, key="apply-a"))["revision"] == 2
        assert (await applied(client, root, second, key="apply-b", status=409))["code"] == "REVISION_CONFLICT"
        other = await post(client, "/management/v1/bots", {"display_name": "Other"}, "other", status=202)
        await ready(client, other)
        foreign = await post(client, "/management/v1/bots/" + other["bot_id"] + "/jobs/change-plans", {
            "job_id": "foreign", "expected_revision": 0, "values": values,
        }, "foreign", status=404)
        assert foreign["code"] == "CONVERSATION_NOT_FOUND"
        forged = await post(client, root + "/jobs/change-plans", {
            "job_id": "forged", "expected_revision": 0,
            "values": {**values, "delegation": {"subject_id": "some-other-admin"}},
        }, "forged", status=422)
        assert "some-other-admin" not in json.dumps(forged)
        body = {"job_id": "daily", "expected_revision": 2}
        missing = await client.post(root + "/jobs/run-requests", json=body)
        assert missing.status_code == 422 and missing.json()["code"] == "IDEMPOTENCY_KEY_REQUIRED"
        csrf = await client.post(root + "/jobs/run-requests", json=body,
                                 headers={"idempotency-key": "bad-csrf", "x-csrf-token": ""})
        assert csrf.status_code == 403
        origin = await client.post(root + "/jobs/run-requests", json=body,
                                   headers={"idempotency-key": "bad-origin", "origin": "https://elsewhere.invalid"})
        assert origin.status_code == 403
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=ORIGIN) as anonymous:
            assert (await anonymous.get(root + "/jobs")).status_code == 401
        absent = await post(client, root + "/jobs/replays", {
            "occurrence_id": "missing", "expected_revision": 1,
        }, "missing-occurrence", status=404)
        assert absent["code"] == "JOB_NOT_FOUND"


async def test_web_cron_preview_and_cancellation_receipt_replay(tmp_path, web_bots_database):
    async with configured(tmp_path, web_bots_database) as (_, client, _, root, group, _, _):
        values = definition(group["conversation_id"])
        values["schedule"].update(kind="cron", interval_seconds=None, cron="0 9 * * 1-5", dst_fold="both")
        plan = await planned(client, root, values)
        assert all(datetime.fromisoformat(value).astimezone(UTC).hour == 1 for value in plan["next_occurrences"])
        await applied(client, root, plan)
        body = {"job_id": "daily", "expected_revision": 1,
                "scope": "pending", "reason": "Cancel unstarted occurrences"}
        first = await post(client, root + "/jobs/cancellations", body, "cancel")
        assert await post(client, root + "/jobs/cancellations", body, "cancel") == first
        stale = await post(client, root + "/jobs/run-requests", {
            "job_id": "daily", "expected_revision": 1,
        }, "stale", status=409)
        assert stale["code"] == "REVISION_CONFLICT"
        assert (await get(client, root + "/jobs"))["jobs"][0]["values"]["enabled"]


async def test_web_retry_never_enqueued_failure_keeps_occurrence_identity(tmp_path, web_bots_database):
    async with configured(tmp_path, web_bots_database) as (app, client, bot, root, group, model, onebot):
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        # Hold automatic claims at its documented no-authority boundary. Use
        # the same host storage methods as a dispatcher to checkpoint a known
        # failure before durable ingress, then exercise the real Web retry.
        jobs = managed.platform.module("scheduler.jobs")
        jobs.authorize = None
        values = definition(group["conversation_id"])
        await applied(client, root, await planned(client, root, values))
        queued = await post(client, root + "/jobs/run-requests", {
            "job_id": "daily", "expected_revision": 1,
        }, "manual-fails-before-ingress")
        store = jobs.store
        lease = jobs._lease()
        claim = await store.claim_pending(lease, "web-retry-fixture", ttl_seconds=30)
        assert claim is not None
        await store.fail(lease, claim, "DEFINITELY_NOT_ENQUEUED")
        failed = (await get(client, root + "/jobs"))["occurrences"][0]
        assert failed["replay_allowed"] and not failed["event_id"]
        payload = {"occurrence_id": failed["occurrence_id"], "expected_revision": failed["revision"]}
        replayed = await post(client, root + "/jobs/replays", payload, "retry-one")
        assert await post(client, root + "/jobs/replays", payload, "retry-one") == replayed
        assert replayed["occurrence_id"] == queued["occurrence_id"]
        assert replayed["revision"] == failed["revision"] + 1
        remaining = (await get(client, root + "/jobs"))["occurrences"]
        assert len(remaining) == 1 and remaining[0]["status"] == "QUEUED"
        assert not model["requests"] and not onebot["sends"]

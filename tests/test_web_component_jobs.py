"""Real Web authority and PostgreSQL coverage for component-job management."""

import asyncio

import httpx
import pytest
from test_component_jobs import component_job_package, wait_jobs
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.bootstrap import BootstrapState
from forest.errors import ForestError
from forest.management.app import WebSettings
from forest.storage.component_jobs import ComponentJobStore

pytestmark = pytest.mark.postgres


async def managed_bot(app, client, name):
    bot = await post(
        client, "/management/v1/bots", {"display_name": name}, name, status=202
    )
    await ready(client, bot)
    managed = await app.state.runtime.fleet.attach(bot["bot_id"])
    return bot, managed, "/management/v1/scopes/bot/" + bot["bot_id"] + "/modules/routing.graph"


async def test_web_replays_dead_job_with_new_epoch_and_stable_operation(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with running(WebSettings(data, ORIGIN, managed_dsn=web_bots_database)) as (
        app,
        client,
    ):
        await setup(client, token)
        _, managed, root = await managed_bot(app, client, "Component job replay")
        package = component_job_package(tmp_path, fail=True, max_attempts=1)
        grants = frozenset(package.component["capabilities"]["requests"])
        await managed.kernel.install(package, grants=grants)
        receipt = await managed.kernel.invoke(
            "component.jobs",
            "component.jobs.schedule",
            {"value": 31, "job_operation": "web-replay-job"},
            operation_id="web-replay-call",
        )
        dead = (
            await wait_jobs(
                managed.store,
                lambda rows: len(rows) == 1 and rows[0]["status"] == "DEAD",
            )
        )[0]
        async with managed.store.pool.connection() as conn:
            old_operation = (
                await (
                    await conn.execute(
                        "SELECT call_operation_id FROM public.bot_component_jobs "
                        "WHERE bot_id=%s AND job_id=%s",
                        (managed.store.bot_id, dead["job_id"]),
                    )
                ).fetchone()
            )["call_operation_id"]
        managed.kernel._component_job_dispatcher.cancel()
        await asyncio.gather(
            managed.kernel._component_job_dispatcher, return_exceptions=True
        )

        view = await client.get(root + "/queries/component-jobs?status=DEAD&limit=10")
        assert view.status_code == 200, view.text
        public = view.json()["jobs"][0]
        assert public["job_id"] == receipt["job_id"]
        assert public["replay_allowed"] is True
        assert "payload" not in public and "result" not in public and "claim_token" not in public

        request = {"job_id": dead["job_id"], "expected_revision": 0}
        attempts = await asyncio.gather(
            *(
                client.post(
                    root + "/actions/component-job.replay",
                    json=request,
                    headers={"idempotency-key": key},
                )
                for key in ("job-replay-race-a", "job-replay-race-b")
            )
        )
        assert sorted(response.status_code for response in attempts) == [200, 409]
        accepted_response = next(
            response for response in attempts if response.status_code == 200
        )
        rejected_response = next(
            response for response in attempts if response.status_code == 409
        )
        accepted_key = (
            "job-replay-race-a"
            if attempts[0] is accepted_response
            else "job-replay-race-b"
        )
        accepted = accepted_response.json()
        assert rejected_response.json()["code"] == "COMPONENT_JOB_REVISION_CONFLICT"
        assert await post(client, root + "/actions/component-job.replay", request, accepted_key) == accepted
        assert accepted["replay_epoch"] == 1
        assert accepted["management_revision"] == 1
        conflict = await post(
            client, root + "/actions/component-job.replay", request, "job-replay-after-race", status=409
        )
        assert conflict["code"] == "COMPONENT_JOB_REVISION_CONFLICT"

        replayed = await ComponentJobStore(managed.store).claim(
            managed.kernel.lease, lease_seconds=3
        )
        assert replayed is not None
        assert replayed["replay_epoch"] == 1
        assert replayed["call_operation_id"] != old_operation
        await managed.kernel._deliver_component_job(replayed)
        rows = await ComponentJobStore(managed.store).jobs()
        assert rows[0]["status"] == "DEAD"
        assert rows[0]["attempt_count"] == 1

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ORIGIN
        ) as outsider:
            assert (await outsider.get(root + "/queries/component-jobs")).status_code == 401
        assert (await client.post(root + "/actions/component-job.replay", json=request)).status_code == 422
        invalid = await client.post(
            root + "/actions/component-job.replay",
            json={**request, "unexpected": True},
            headers={"idempotency-key": "invalid-job-replay"},
        )
        assert invalid.status_code == 422


async def test_retirement_fences_old_generation_and_allows_only_removal_deployment(
    tmp_path, web_bots_database
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with running(WebSettings(data, ORIGIN, managed_dsn=web_bots_database)) as (
        app,
        client,
    ):
        await setup(client, token)
        _, managed, root = await managed_bot(app, client, "Component job retire")
        package = component_job_package(tmp_path, version="0.1.0")
        grants = frozenset(package.component["capabilities"]["requests"])
        await managed.kernel.install(package, grants=grants)
        managed.kernel._component_job_dispatcher.cancel()
        await asyncio.gather(
            managed.kernel._component_job_dispatcher, return_exceptions=True
        )
        first = await managed.kernel.invoke(
            "component.jobs",
            "component.jobs.schedule",
            {"value": 41, "job_operation": "retire-first"},
            operation_id="retire-first-call",
        )
        await managed.kernel.invoke(
            "component.jobs",
            "component.jobs.schedule",
            {"value": 42, "job_operation": "retire-second"},
            operation_id="retire-second-call",
        )
        jobs = ComponentJobStore(managed.store)
        claimed = await jobs.claim(managed.kernel.lease, lease_seconds=3)
        assert claimed is not None and claimed["job_id"] == first["job_id"]

        proposal = await post(
            client,
            root + "/actions/component-job.retirement.plan",
            {
                "component_id": "component.jobs",
                "handler_id": "work.persist",
                "expected_revision": 0,
            },
            "retire-busy-plan",
        )
        assert proposal["impact"]["inflight"] == 1
        request = {
            "expected_revision": proposal["expected_revision"],
            "plan_id": proposal["plan_id"],
            "plan_digest": proposal["plan_digest"],
        }
        busy_result = await post(
            client, root + "/actions/component-job.retirement.apply", request, "retire-busy", status=409
        )
        assert busy_result["code"] == "COMPONENT_JOB_HANDLER_BUSY"
        await jobs.defer(managed.kernel.lease, claimed, delay_seconds=60)

        proposal = await post(
            client,
            root + "/actions/component-job.retirement.plan",
            {
                "component_id": "component.jobs",
                "handler_id": "work.persist",
                "expected_revision": 0,
            },
            "retire-ready-plan",
        )
        assert proposal["impact"]["retire_pending"] == 2
        request = {
            "expected_revision": proposal["expected_revision"],
            "plan_id": proposal["plan_id"],
            "plan_digest": proposal["plan_digest"],
        }
        retired = await post(
            client, root + "/actions/component-job.retirement.apply", request, "retire-ready"
        )
        assert await post(
            client, root + "/actions/component-job.retirement.apply", request, "retire-ready"
        ) == retired
        assert retired["management_revision"] == 1
        assert retired["retired_job_count"] == 2
        rows = await jobs.jobs()
        assert [row["status"] for row in rows] == ["RETIRED", "RETIRED"]
        assert all(row["last_error_code"] == "JOB_HANDLER_RETIRED" for row in rows)
        assert await jobs.claim(managed.kernel.lease, lease_seconds=3) is None

        # An exact retry still converges after retirement, while new work from
        # the already-loaded generation is fenced immediately.
        retry = await managed.kernel.invoke(
            "component.jobs",
            "component.jobs.schedule",
            {"value": 41, "job_operation": "retire-first"},
            operation_id="retire-existing-call-retry",
        )
        assert retry["job_id"] == first["job_id"]
        with pytest.raises(ForestError) as fenced:
            await managed.kernel.invoke(
                "component.jobs",
                "component.jobs.schedule",
                {"value": 43, "job_operation": "retire-new"},
                operation_id="retire-new-call",
            )
        assert fenced.value.code == "JOB_DECLARATION_MISMATCH"

        removed = component_job_package(
            tmp_path,
            version="0.2.0",
            include_handler=False,
            decorate_handler=False,
        )
        await managed.kernel.install(
            removed, grants=frozenset({"state.read", "state.write"})
        )
        restored = component_job_package(tmp_path, version="0.3.0")
        with pytest.raises(ForestError) as reactivation:
            await managed.kernel.install(restored, grants=grants)
        assert reactivation.value.code == "JOB_HANDLER_REACTIVATION_REQUIRES_MANAGEMENT"

        view = (await client.get(root + "/queries/component-jobs?limit=20")).json()
        assert view["handlers"][0]["active"] is False
        assert [row["status"] for row in view["jobs"]] == ["RETIRED", "RETIRED"]

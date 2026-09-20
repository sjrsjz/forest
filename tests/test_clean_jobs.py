from __future__ import annotations

import asyncio

import pytest
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_job_shell import JOB_SHELL_COMMANDS
from forest.clean_job_module import bootstrap_clean_job
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.runtime_modules import official_storage_bundle
from forest.shell_command_parser import ShellCommandParser
from forest.storage.job_scheduler_authority import JobRoute, JobSchedulerRepository


def _route() -> JobRoute:
    contribution = next(
        item
        for item in bootstrap_job_type_contributions()
        if item.job_type == "runtime.health-snapshot"
    )
    return JobRoute(
        contribution.job_type,
        contribution.job_version,
        contribution.input_schema_digest,
        contribution.max_attempts,
        contribution.timeout_seconds,
        contribution.max_concurrency,
        "kernel.runtime",
        "generation/runtime/1",
        contribution.service_id,
        contribution.service_version,
    )


def bootstrap_job_type_contributions():
    from forest.builtin_runtime_job import RUNTIME_HEALTH_SNAPSHOT_JOB

    return (RUNTIME_HEALTH_SNAPSHOT_JOB,)


def test_job_shell_parser_uses_module_owned_schemas():
    parser = ShellCommandParser(None, JOB_SHELL_COMMANDS)

    run = parser.parse('jobs run --type runtime.health-snapshot --input "{}"')
    cancel = parser.parse(
        "jobs cancel --job-id job/42 --expect-revision 3 --reason ADMIN_REQUESTED"
    )
    listed = parser.parse("jobs list --state UNKNOWN --type runtime.health-snapshot --limit 5")

    assert run.document["arguments"] == {
        "job_type": "runtime.health-snapshot",
        "input_json": "{}",
    }
    assert cancel.document["arguments"] == {
        "job_id": "job/42",
        "expected_revision": 3,
        "reason_code": "ADMIN_REQUESTED",
    }
    assert listed.document["arguments"] == {
        "state": "UNKNOWN",
        "job_type": "runtime.health-snapshot",
        "limit": 5,
    }


async def _repository(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    storage = official_storage_bundle(
        "job.scheduler",
        "forest.job.scheduler",
        "forest.job-scheduler.repository/1",
        "storage/official/job_scheduler.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    return JobSchedulerRepository(
        store, namespace=storage.spec.namespace, payloads=kernel.payload
    )


@pytest.mark.postgres
async def test_job_repository_concurrent_claim_cancel_and_fenced_completion(clean_authority):
    repository = await _repository(clean_authority)
    route = _route()
    queued = await repository.enqueue(
        route=route,
        operation_id="job/create/one",
        requested_by="qq:42",
        source_conversation_ref="onebot:group:933179609",
        input_value={},
    )
    replay = await repository.enqueue(
        route=route,
        operation_id="job/create/one",
        requested_by="qq:42",
        source_conversation_ref="onebot:group:933179609",
        input_value={},
    )
    assert replay == queued

    claims = await asyncio.gather(
        repository.claim(
            worker_generation_id="generation/scheduler/1",
            routes={(route.job_type, route.job_version): route},
        ),
        repository.claim(
            worker_generation_id="generation/scheduler/1",
            routes={(route.job_type, route.job_version): route},
        ),
    )
    owned = [claim for claim in claims if claim is not None]
    assert len(owned) == 1
    claim = owned[0]
    renewed = await repository.renew(claim)
    assert renewed.revision == claim.revision
    assert renewed.lease_until > claim.lease_until
    cancellation = await repository.cancel(
        operation_id="job/cancel/one",
        job_id=claim.job_id,
        expected_revision=claim.revision,
        requested_by="qq:42",
    )
    assert cancellation["resulting_state"] == "RUNNING"
    assert await repository.cancellation_requested(claim) is True

    cancelled = await repository.succeed(claim, result={"late": True})
    assert cancelled["state"] == "CANCELLED"
    assert cancelled["result_payload_id"] is None
    with pytest.raises(ForestError) as caught:
        await repository.succeed(claim, result={"late": True})
    assert caught.value.code == "STALE_JOB_CLAIM"


@pytest.mark.postgres
async def test_expired_job_claim_cannot_renew_or_settle(clean_authority):
    repository = await _repository(clean_authority)
    route = _route()
    queued = await repository.enqueue(
        route=route,
        operation_id="job/create/expired",
        requested_by="test:lease",
        input_value={},
    )
    first = await repository.claim(
        worker_generation_id="generation/scheduler/1",
        routes={(route.job_type, route.job_version): route},
    )
    assert first is not None and first.job_id == queued["job_id"]
    async with repository.store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET revision=revision+1,"
                "lease_until=clock_timestamp()-interval '1 second',"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s"
            ).format(repository._table("jobs")),
            (repository.store.bot_id, first.job_id),
        )
    with pytest.raises(ForestError) as caught:
        await repository.renew(first)
    assert caught.value.code == "STALE_JOB_CLAIM"
    with pytest.raises(ForestError) as caught:
        await repository.succeed(first, result={"stale": True})
    assert caught.value.code == "STALE_JOB_CLAIM"

    second = await repository.claim(
        worker_generation_id="generation/scheduler/2",
        routes={(route.job_type, route.job_version): route},
    )
    assert second is not None
    assert second.claim_token != first.claim_token
    assert second.attempt_count == 2


@pytest.mark.postgres
async def test_job_repository_retry_success_and_bounded_query(clean_authority):
    repository = await _repository(clean_authority)
    route = _route()
    await repository.enqueue(
        route=route,
        operation_id="job/create/retry",
        requested_by="web:operator",
        input_value={},
    )
    first = await repository.claim(
        worker_generation_id="generation/scheduler/1",
        routes={(route.job_type, route.job_version): route},
    )
    assert first is not None
    retry = await repository.fail(
        first,
        error_code="DEPENDENCY_UNAVAILABLE",
        retryable=True,
        retry_delay_seconds=0,
    )
    assert retry["state"] == "QUEUED"
    second = await repository.claim(
        worker_generation_id="generation/scheduler/2",
        routes={(route.job_type, route.job_version): route},
    )
    assert second is not None and second.attempt_count == 2
    succeeded = await repository.succeed(second, result={"healthy": True})
    assert succeeded["state"] == "SUCCEEDED"
    assert (await repository.inspect(second.job_id))["result_payload_id"]
    rows = await repository.list_jobs(
        state="SUCCEEDED", job_type="runtime.health-snapshot", limit=10
    )
    assert [row["job_id"] for row in rows] == [second.job_id]


@pytest.mark.postgres
async def test_job_module_bootstrap_owns_independent_schema(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    jobs = await bootstrap_clean_job(store, installer, kernel=kernel)

    assert jobs.descriptor.component_id == "job.scheduler"
    assert [item.command_id for item in jobs.descriptor.shell_commands] == [
        "jobs.list",
        "jobs.inspect",
        "jobs.run",
        "jobs.cancel",
    ]
    assert jobs.descriptor.storage.spec.namespace != "public"

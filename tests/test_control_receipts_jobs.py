"""A revoked Job definition cannot reappear through its management list."""

import pytest
from test_control_receipts import forget, rows
from test_jobs import configure, request
from test_jobs import jobs as jobs
from test_web_authority import environment as environment

pytestmark = pytest.mark.postgres


async def test_job_management_read_uses_definition_revision_source_and_keeps_cancel_available(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    assert (await request(env, store, "read", "read", {}))["jobs"][0]["values"]["prompt"]
    plan_op = (
        await rows(
            env, "SELECT operation_id FROM public.bot_web_operations WHERE action='job.plan'"
        )
    )[0]["operation_id"]
    await forget(env, plan_op)
    unavailable = (await request(env, store, "read", "read", {}))["jobs"][0]
    assert unavailable["values"] is None and unavailable["delegation"] is None
    assert unavailable["values_payload_state"] == "UNAVAILABLE"
    assert unavailable["delegation_payload_state"] == "UNAVAILABLE"
    # Metadata-only future cancellation remains available for accepted work.
    cancelled = await request(
        env,
        store,
        "cancel",
        "cancel",
        {
            "job_id": "reminder",
            "expected_revision": 1,
            "scope": "future",
            "reason": "stop future tasks",
        },
    )
    assert cancelled["revision"] == 2

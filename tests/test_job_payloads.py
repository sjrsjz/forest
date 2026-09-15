"""Job prompts and delegated authority use removable, source-bound payloads."""

import asyncio
import json

import psycopg
import pytest
import pytest_asyncio
from binding_seed import create_online_binding
from psycopg.types.json import Jsonb
from test_agent_config_payloads import erase_payload
from test_jobs import configure, manual, request
from test_messages import event
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.storage.job_payloads import delegation_metadata
from forest.storage.jobs import JobStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def job_case(environment):
    env = environment
    await create_online_binding(env.store, env.lease, "binding-a", "1000", ticket=env.ticket)
    messages = MessageStore(env.store)
    receipt = await messages.ingest(env.lease, "binding-a", event(1), decision="NO_TRIGGER")
    yield env, JobStore(messages), receipt["conversation_id"]


async def test_job_plan_and_revision_store_only_closed_operational_metadata(job_case):
    env, store, conversation = job_case
    applied = await configure(
        env,
        store,
        conversation,
        name="PRIVATE JOB NAME",
        prompt="PRIVATE JOB PROMPT",
        max_overlap=3,
    )
    async with env.store.pool.connection() as conn:
        plan = await (await conn.execute(
            "SELECT * FROM public.bot_job_plans WHERE bot_id=%s ORDER BY expires_at DESC LIMIT 1",
            (env.store.bot_id,),
        )).fetchone()
        revision = await (await conn.execute(
            "SELECT * FROM public.bot_job_revisions WHERE bot_id=%s AND job_id='reminder' AND revision=1",
            (env.store.bot_id,),
        )).fetchone()
        headers = await (await conn.execute(
            "SELECT payload_id,owner_id,manifest FROM public.bot_payload_headers WHERE bot_id=%s "
            "AND payload_id=ANY(%s) ORDER BY payload_id",
            (
                env.store.bot_id,
                [plan["values_payload_id"], revision["values_payload_id"],
                 revision["delegation_payload_id"]],
            ),
        )).fetchall()
        gate = await (await conn.execute(
            "SELECT minimum_generation FROM public.bot_schema_writer_gate"
        )).fetchone()
    assert gate["minimum_generation"] == 17
    assert plan["values"] is revision["values"] is revision["delegation"] is None
    assert plan["values_metadata"] == revision["values_metadata"]
    assert plan["values_metadata"]["max_overlap"] == 3
    assert "name" not in plan["values_metadata"] and "prompt" not in plan["values_metadata"]
    assert "PRIVATE" not in json.dumps(plan["values_metadata"])
    assert revision["delegation_metadata"] == delegation_metadata(applied["delegation"])
    assert "subject_id" not in revision["delegation_metadata"]
    assert revision["plan_id"] == plan["plan_id"]
    assert len(headers) == 3
    assert all(row["owner_id"].startswith("config-values-v1:sha256:") for row in headers)
    assert len(headers[0]["manifest"]["parents"]) >= 1

    visible = await request(env, store, "read", "read", {"job_id": "reminder"})
    assert visible["jobs"][0]["values"]["prompt"] == "PRIVATE JOB PROMPT"
    assert visible["jobs"][0]["delegation"] == applied["delegation"]
    assert visible["jobs"][0]["values_payload_state"] == "ACTIVE"
    assert visible["jobs"][0]["delegation_payload_state"] == "ACTIVE"


async def test_forgetting_job_source_removes_both_bodies_and_revokes_occurrences(job_case):
    env, store, conversation = job_case
    await configure(env, store, conversation, prompt="FORGET THIS JOB PROMPT")
    queued = await manual(env, store)
    async with env.store.pool.connection() as conn:
        revision = await (await conn.execute(
            "SELECT * FROM public.bot_job_revisions WHERE bot_id=%s AND job_id='reminder' AND revision=1",
            (env.store.bot_id,),
        )).fetchone()
    await erase_payload(env.store, env.lease, revision["values_payload_id"])

    visible = await request(env, store, "read", "read", {"job_id": "reminder"})
    job = visible["jobs"][0]
    assert job["values"] is None and job["delegation"] is None
    assert job["values_payload_state"] == "UNAVAILABLE"
    assert job["delegation_payload_state"] == "UNAVAILABLE"
    assert await store.claim_pending(env.lease, "revoked-worker") is None
    with pytest.raises(ForestError) as caught:
        await manual(env, store, revision=1, operation="run-forgotten-job")
    assert caught.value.code == "CONTROL_CONTENT_REVOKED"
    assert await store.reconcile(env.lease) == 1
    async with env.store.pool.connection() as conn:
        occurrence = await (await conn.execute(
            "SELECT status FROM public.bot_job_occurrences WHERE bot_id=%s AND occurrence_id=%s",
            (env.store.bot_id, queued["occurrence_id"]),
        )).fetchone()
        bodies = await (await conn.execute(
            "SELECT payload_id FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=ANY(%s)",
            (
                env.store.bot_id,
                [revision["values_payload_id"], revision["delegation_payload_id"]],
            ),
        )).fetchall()
    assert occurrence["status"] == "CANCELLED"
    assert bodies == []


async def test_job_payload_metadata_is_immutable(job_case):
    env, store, conversation = job_case
    await configure(env, store, conversation)
    async with env.store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute(
                "UPDATE public.bot_job_revisions SET values_metadata=%s "
                "WHERE bot_id=%s AND job_id='reminder' AND revision=1",
                (Jsonb({"max_overlap": 8}), env.store.bot_id),
            )


async def test_concurrent_claimers_cannot_lease_one_occurrence_twice(job_case):
    env, store, conversation = job_case
    await configure(env, store, conversation)
    queued = await manual(env, store)
    first, second = await asyncio.gather(
        store.claim_pending(env.lease, "worker-a"),
        store.claim_pending(env.lease, "worker-b"),
    )
    claimed = [item for item in (first, second) if item is not None]
    assert len(claimed) == 1 and claimed[0].occurrence_id == queued["occurrence_id"]

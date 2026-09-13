"""045 real configuration/body provenance and metadata-only settlement."""

import json
from uuid import uuid4

import psycopg
import pytest
from test_agent_config import apply, plan, read
from test_agent_store import authorized_messages, prepare, specification, start
from test_messages import messages as messages
from test_web_authority import environment as environment

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.models import ModelResult
from forest.packages import digest_json
from forest.storage.agent_config import AgentConfig
from forest.storage.agent_specs import values_metadata
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def rows(store, query, args=()):
    async with store.pool.connection() as conn:
        return await (await conn.execute(query, args)).fetchall()


async def erase_payload(store, lease, payload_id):
    payloads = PayloadStore(store)
    async with store.pool.connection() as conn:
        found = await (await conn.execute(
            "SELECT source_unit_id,source_revision,part_id FROM public.bot_payload_sources "
            "WHERE bot_id=%s AND payload_id=%s ORDER BY source_unit_id,part_id",
            (store.bot_id, payload_id),
        )).fetchall()
        epoch = (await (await conn.execute(
            "SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s", (store.bot_id,)
        )).fetchone())["deletion_epoch"]
    assert found
    await payloads.forget_sources(lease, "erase-" + uuid4().hex,
                                 tuple(SourceRef(**row) for row in found), expected_deletion_epoch=epoch)
    while (await payloads.scrub(lease))["remaining"]:
        pass


async def test_independent_replacement_plan_survives_deleted_preview_before_values(environment):
    env = environment
    first = await plan(env, operation="first-plan", values={**DEFAULT_AGENT_SETTINGS, "preset_context": ["PRIVATE-A"]})
    await apply(env, first)
    second_values = {**DEFAULT_AGENT_SETTINGS, "preset_context": ["INDEPENDENT-B"]}
    second = await plan(env, revision=1, values=second_values, operation="second-plan")
    old = (await rows(env.store, "SELECT * FROM public.bot_agent_config_revisions WHERE revision=1"))[0]
    plans = await rows(env.store, "SELECT * FROM public.bot_agent_config_plans WHERE plan_id=%s", (second["plan_id"],))
    assert old["values"] is plans[0]["values"] is None
    assert old["values_metadata"] == values_metadata(DEFAULT_AGENT_SETTINGS)
    await erase_payload(env.store, env.lease, old["values_payload_id"])
    unavailable = await read(env)
    assert unavailable["revision"] == 1 and unavailable["values"] is None
    assert unavailable["values_payload_state"] == "UNAVAILABLE"
    replay = await plan(env, revision=1, values=second_values, operation="second-plan")
    assert replay["result_payload_state"] == "UNAVAILABLE"
    assert (await apply(env, second))["revision"] == 2
    assert (await read(env))["values"] == second_values
    headers = await rows(env.store, "SELECT h.*,b.content FROM public.bot_payload_headers h "
                         "LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id) WHERE h.payload_id=%s",
                         (plans[0]["values_payload_id"],))
    assert headers[0]["state"] == "ACTIVE" and b"INDEPENDENT-B" in bytes(headers[0]["content"])
    assert old["values_payload_id"] not in headers[0]["manifest"]["parents"]


async def test_config_body_is_exact_and_metadata_cannot_hide_nested_text(environment):
    env = environment
    values = {**DEFAULT_AGENT_SETTINGS, "preset_context": ["KEEP-IN-PAYLOAD"], "model": "model-private"}
    proposed = await plan(env, values=values, secret="NEVER-IN-PAYLOAD")
    await apply(env, proposed, operation="actual-apply")
    saved = (await rows(env.store, "SELECT * FROM public.bot_agent_config_revisions"))[0]
    body = (await rows(env.store, "SELECT content FROM public.bot_payload_bodies WHERE payload_id=%s",
                       (saved["values_payload_id"],)))[0]
    assert json.loads(bytes(body["content"]))["values"] == values
    assert b"NEVER-IN-PAYLOAD" not in bytes(body["content"])
    assert saved["values"] is None and "KEEP-IN-PAYLOAD" not in repr(saved["values_metadata"])
    result = (await rows(env.store, "SELECT result_payload_id FROM public.bot_web_operations WHERE operation_id='actual-apply'"))[0]
    parents = await rows(env.store, "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
                        (result["result_payload_id"],))
    assert saved["values_payload_id"] in {row["parent_payload_id"] for row in parents}
    # The immutable row rejects attempts to smuggle body values into metadata.
    async with env.store.pool.connection() as conn, conn.transaction():
        with pytest.raises(psycopg.Error):
            async with conn.transaction():
                await conn.execute("UPDATE public.bot_agent_config_revisions SET values_metadata="
                                   "values_metadata||'{\"extra\":{\"body\":\"SECRET\"}}'::jsonb")
    assert (await AgentConfig(env.store, env.vault).runtime(env.lease))["values"] == values


@pytest.mark.parametrize("status", ["RESPONDED", "UNKNOWN", "NOT_SENT"])
async def test_rev0_spec_deletion_settles_known_cost_and_retains_unknown_session(messages, status):
    state, actor, head = await start(messages, spec={**specification(), "preset_context": ["RUN-PRIVATE"]})
    _, request, permit = await prepare(state, actor, head)
    if status != "NOT_SENT":
        await state.check_model_permit(actor, permit, request)
    stored = (await rows(state.store, "SELECT * FROM public.bot_agent_runs WHERE run_id=%s", (head["run_id"],)))[0]
    assert stored["spec"] is None and stored["spec_digest"] == digest_json(head["spec"])
    domains = await rows(state.store, "SELECT d.kind FROM public.bot_payload_domains d JOIN public.bot_payload_sources s "
                         "USING(bot_id,source_unit_id) WHERE s.payload_id=%s", (stored["spec_payload_id"],))
    assert domains == [{"kind": "AGENT_SPEC"}]
    await erase_payload(state.store, actor.host, stored["spec_payload_id"])
    result = ModelResult(status, request.request_id, request.transport_attempt_id, request.payload_digest,
                         text="LATE-PRIVATE-OUTPUT", usage_json=json.dumps({"prompt_tokens": 7, "completion_tokens": 3}))
    finished = await state.complete_model(actor, permit, result)
    assert finished["phase"] == ("BLOCKED_UNKNOWN" if status == "UNKNOWN" else "FAILED")
    assert finished["spec"] is None and finished["spec_payload_state"] == "UNAVAILABLE"
    attempt = (await rows(state.store, "SELECT * FROM public.bot_model_transport_attempts WHERE transport_attempt_id=%s",
                          (permit.transport_attempt_id,)))[0]
    assert attempt["result_payload_id"] is None and attempt["result_digest"] == digest_json(result.as_dict())
    assert "LATE-PRIVATE" not in repr(attempt["result_metadata"])
    budget = (await rows(state.store, "SELECT * FROM public.bot_model_budget_reservations WHERE transport_attempt_id=%s",
                         (permit.transport_attempt_id,)))[0]
    assert budget["status"] == ("UNKNOWN" if status == "UNKNOWN" else "SETTLED")
    assert budget["charged_tokens"] == (None if status == "UNKNOWN" else 10 if status == "RESPONDED" else 0)
    session = (await rows(state.store, "SELECT active_run_id FROM public.bot_conversation_sessions WHERE session_id=%s",
                          (head["session_id"],)))[0]
    assert session["active_run_id"] == (head["run_id"] if status == "UNKNOWN" else None)
    with pytest.raises(ForestError):
        await state.check_model_permit(actor, permit, request)
    if status == "UNKNOWN":
        assert (await state.complete_model(actor, permit, result))["phase"] == finished["phase"]


async def test_revocation_checkpoint_before_late_known_usage_does_not_lose_the_bill(messages):
    state, actor, head = await start(messages)
    _, request, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, request)
    await erase_payload(state.store, actor.host, head["spec_payload_id"])
    assert (await state.fail_revoked(actor, head["run_id"]))["phase"] == "BLOCKED_UNKNOWN"
    result = ModelResult("RESPONDED", request.request_id, request.transport_attempt_id,
                         request.payload_digest, text="LATE-NO-BODY", usage_json='{"input_tokens":8,"output_tokens":4}')
    finished = await state.complete_model(actor, permit, result)
    assert finished["phase"] == "FAILED"
    attempt = (await rows(state.store, "SELECT * FROM public.bot_model_transport_attempts WHERE transport_attempt_id=%s",
                          (request.transport_attempt_id,)))[0]
    assert attempt["result_payload_id"] is None and attempt["result_digest"] == digest_json(result.as_dict())
    assert attempt["result_metadata"]["usage"] == {"prompt_tokens": 8, "completion_tokens": 4}
    budget = (await rows(state.store, "SELECT * FROM public.bot_model_budget_reservations WHERE transport_attempt_id=%s",
                         (request.transport_attempt_id,)))[0]
    assert budget["status"] == "SETTLED" and budget["charged_tokens"] == 12


async def test_actual_config_revision_is_a_parent_of_run_and_model_snapshot(environment):
    env = environment
    values = {**DEFAULT_AGENT_SETTINGS, "enabled": True, "model": "real-configured-model"}
    await apply(env, await plan(env, values=values))
    messages = await authorized_messages(env)
    state, actor, head = await start(messages, spec={"config_revision": 1, "secret_revision": 1, "values": values})
    _, request, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, request)
    cfg = (await rows(env.store, "SELECT * FROM public.bot_agent_config_revisions WHERE revision=1"))[0]
    spec_parents = await rows(env.store, "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
                             (head["spec_payload_id"],))
    assert spec_parents == [{"parent_payload_id": cfg["values_payload_id"]}]
    step = (await rows(env.store, "SELECT * FROM public.bot_agent_steps WHERE logical_step_id=%s", (permit.logical_step_id,)))[0]
    snapshot_parents = await rows(env.store, "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
                                 (step["snapshot_payload_id"],))
    assert head["spec_payload_id"] in {row["parent_payload_id"] for row in snapshot_parents}
    await erase_payload(env.store, env.lease, cfg["values_payload_id"])
    result = ModelResult("RESPONDED", request.request_id, request.transport_attempt_id, request.payload_digest,
                         text="DO-NOT-RESTORE", usage_json='{"total_tokens":11}')
    assert (await state.complete_model(actor, permit, result))["phase"] == "FAILED"
    attempt = (await rows(env.store, "SELECT * FROM public.bot_model_transport_attempts WHERE transport_attempt_id=%s",
                          (permit.transport_attempt_id,)))[0]
    assert attempt["result_payload_id"] is None
    assert attempt["result_metadata"]["discarded_reason"] == "AGENT_MEMORY_REVOKED"

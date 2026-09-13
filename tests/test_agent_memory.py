"""Real PostgreSQL model, tool and final guards for frozen recalled evidence."""

import json
from uuid import uuid4

import pytest
from test_agent_store import respond, start, tool_calls
from test_memory_config import environment as environment
from test_memory_index_store import outcome, rows, text_source
from test_memory_media import configure
from test_memory_media import media as media
from test_messages import receive

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.models import ModelCapabilities, ModelResult, prepare_model_request
from forest.storage.agent_memory import AgentMemoryStore
from forest.storage.memory_index import MemoryIndexStore
from forest.storage.memory_queries import MemoryQueryStore

pytestmark = pytest.mark.postgres


async def memory_step(media, *, enable=True):
    env, images, _ = media
    if enable:
        await configure(env, [])
    state, actor, head = await start((images.messages, env.lease, None))
    snapshot = await state.snapshot(actor, head["run_id"])
    index = MemoryIndexStore(images.messages, images)
    queries = MemoryQueryStore(index)
    memory = AgentMemoryStore(state, queries)
    decision = await memory.begin(actor, snapshot)
    return state, actor, snapshot, memory, decision


async def bound(media):
    env, images, _ = media
    await configure(env, [])
    await text_source(media, 90, group="old-group", text="另一群老用户保存的事实：森林需要阳光。")
    index = MemoryIndexStore(images.messages, images)
    await index.enroll(env.lease, 1)
    ids = [row["facet_id"] for row in await index.pending(env.lease, 1)]
    doc = (await index.prepare(env.lease, 1, ids))["prepared"]
    await index.check_permit(env.lease, doc, dispatch_token="doc")
    await index.complete(env.lease, doc, outcome(doc), dispatch_token="doc")
    state, actor, head = await start((images.messages, env.lease, None))
    snapshot = await state.snapshot(actor, head["run_id"])
    queries = MemoryQueryStore(index)
    memory = AgentMemoryStore(state, queries)
    decision = await memory.begin(actor, snapshot)
    query = await queries.prepare(
        env.lease,
        1,
        query_id=decision["query_id"],
        conversation_id=actor.conversation_id,
        input_cut=decision["input_cut"],
        source_unit_ids=decision["source_unit_ids"],
    )
    request = (
        await index.prepare(
            env.lease,
            1,
            query["facet_ids"],
            request_id=query["query_id"],
            purpose="query",
            conversation_id=actor.conversation_id,
            input_cut=decision["input_cut"],
        )
    )["prepared"]
    await index.check_permit(env.lease, request, dispatch_token="query")
    await index.complete(env.lease, request, outcome(request), dispatch_token="query")
    bundle = await queries.retrieve(env.lease, query["query_id"])
    assert bundle["status"] == "READY" and len(bundle["evidence"]) == 1
    assert bundle["evidence"][0]["source"]["channel_id"] == "old-group"
    assert (await memory.finish(actor, snapshot))[0] == bundle
    return state, actor, snapshot, memory, bundle


def projection(snapshot, evidence=None):
    return prepare_model_request(
        snapshot,
        ModelCapabilities("test", "test", "v1"),
        request_id=uuid4().hex,
        transport_attempt_id=uuid4().hex,
        output_tokens=64,
        stream=False,
        evidence=evidence,
    )


async def forget(media, bundle):
    return await media[2].payloads.forget_sources(
        media[0].lease,
        "erase-" + uuid4().hex,
        [SourceRef(bundle["evidence"][0]["source_unit_id"])],
        expected_deletion_epoch=0,
    )


async def test_disabled_decision_is_frozen_when_memory_is_later_enabled(media):
    state, actor, snapshot, memory, decision = await memory_step(media, enable=False)
    assert decision["status"] == "DISABLED"
    await configure(media[0], [])
    assert (await memory.begin(actor, snapshot))["status"] == "DISABLED"
    assert await memory.finish(actor, snapshot) == (None, {})
    request = projection(snapshot)
    await state.prepare_model(actor, snapshot["run_id"], "permit", request)
    assert not await rows(media, "SELECT * FROM public.bot_memory_queries")


async def test_pending_decision_cannot_be_omitted_and_later_messages_do_not_change_query(media):
    state, actor, snapshot, memory, decision = await memory_step(media)
    await receive(media[1].messages, media[0].lease, "later-trigger", trigger=True)
    await receive(media[1].messages, media[0].lease, "tail", trigger=False)
    assert await memory.begin(actor, snapshot) == decision
    assert decision["input_cut"] == snapshot["selected_input_end_seq"]
    with pytest.raises(ForestError) as error:
        await state.prepare_model(actor, snapshot["run_id"], "permit", projection(snapshot))
    assert error.value.code == "AGENT_MEMORY_NOT_READY"
    query = await memory.queries.prepare(
        media[0].lease,
        1,
        query_id=decision["query_id"],
        conversation_id=actor.conversation_id,
        input_cut=decision["input_cut"],
        source_unit_ids=decision["source_unit_ids"],
    )
    assert len(query["facet_ids"]) == 1
    assert await memory.finish(actor, snapshot, error_code="MEMORY_QUERY_TIMEOUT") == (None, {})
    request = projection(snapshot)
    await state.prepare_model(actor, snapshot["run_id"], "permit", request)


async def test_bound_evidence_is_required_and_actual_model_can_publish_with_sources(media):
    state, actor, snapshot, _, bundle = await bound(media)
    with pytest.raises(ForestError) as error:
        await state.prepare_model(actor, snapshot["run_id"], "omitted", projection(snapshot))
    assert error.value.code == "AGENT_MEMORY_NOT_READY"
    request = projection(snapshot, bundle)
    grant = await state.prepare_model(actor, snapshot["run_id"], "permit", request)
    assert (
        request.manifest["input_digest"]
        == (await rows(media, "SELECT snapshot_digest FROM public.bot_agent_steps"))[0][
            "snapshot_digest"
        ]
    )
    assert request.manifest["memory_evidence"]["digest"] == bundle["digest"]
    result = await respond(state, actor, grant, request)
    assert result["phase"] == "FINAL_READY"


async def test_forget_between_prepare_and_http_denies_dispatch_and_allows_termination(media):
    state, actor, snapshot, _, bundle = await bound(media)
    request = projection(snapshot, bundle)
    grant = await state.prepare_model(actor, snapshot["run_id"], "permit", request)
    await forget(media, bundle)
    with pytest.raises(ForestError) as error:
        await state.check_model_permit(actor, grant, request)
    assert error.value.code == "AGENT_MEMORY_REVOKED"
    failed = await state.fail(actor, snapshot["run_id"], error.value.code)
    assert failed["phase"] == "FAILED"
    assert not await rows(media, "SELECT * FROM public.bot_conversation_replies")


async def test_forget_after_http_settles_known_cost_without_storing_returned_content(media):
    state, actor, snapshot, _, bundle = await bound(media)
    request = projection(snapshot, bundle)
    grant = await state.prepare_model(actor, snapshot["run_id"], "permit", request)
    await state.check_model_permit(actor, grant, request)
    await forget(media, bundle)
    result = ModelResult(
        "RESPONDED",
        request.request_id,
        request.transport_attempt_id,
        request.payload_digest,
        text="已删除证据的派生秘密",
        tool_calls_json=json.dumps(tool_calls(snapshot)),
        usage_json=json.dumps({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}),
    )
    ended = await state.complete_model(actor, grant, result)
    assert ended["phase"] == "FAILED" and ended["error_code"] == "AGENT_MEMORY_REVOKED"
    saved = (await rows(media, "SELECT result FROM public.bot_model_transport_attempts"))[0][
        "result"
    ]
    assert "text" not in saved and "tool_calls" not in saved
    assert saved["usage"]["total_tokens"] == 15
    assert (
        await rows(media, "SELECT charged_tokens,reserved_tokens FROM public.bot_agent_budget_days")
    ) == [{"charged_tokens": 15, "reserved_tokens": 0}]
    assert not await rows(media, "SELECT * FROM public.bot_agent_tools")
    assert not await rows(media, "SELECT * FROM public.bot_conversation_replies")


async def test_forget_blocks_tool_effect_admission(media):
    state, actor, snapshot, _, bundle = await bound(media)
    request = projection(snapshot, bundle)
    grant = await state.prepare_model(actor, snapshot["run_id"], "permit", request)
    ended = await respond(state, actor, grant, request, calls=tool_calls(snapshot))
    assert ended["phase"] == "WAIT_TOOL"
    await forget(media, bundle)
    with pytest.raises(ForestError) as error:
        await state.permit_tool(
            actor,
            snapshot["run_id"],
            "call-0",
            authority_trigger_id=snapshot["triggers"][0]["trigger_id"],
            requires_approval=False,
        )
    assert error.value.code == "AGENT_MEMORY_REVOKED"
    assert (await state.fail(actor, snapshot["run_id"], error.value.code))["phase"] == "FAILED"


async def test_config_revocation_blocks_frozen_final_and_binding_cannot_be_replaced(media):
    from forest.storage.delivery import DeliveryStore

    state, actor, snapshot, memory, bundle = await bound(media)
    request = projection(snapshot, bundle)
    grant = await state.prepare_model(actor, snapshot["run_id"], "permit", request)
    assert (await respond(state, actor, grant, request))["phase"] == "FINAL_READY"
    await configure(media[0], [], revision=1, enabled=False)
    with pytest.raises(ForestError) as error:
        await state.commit_final(actor, snapshot["run_id"], "final", DeliveryStore(state.store))
    assert error.value.code == "AGENT_MEMORY_REVOKED"
    assert (await state.fail(actor, snapshot["run_id"], error.value.code))["phase"] == "FAILED"
    assert (
        await rows(media, "SELECT status,bundle_digest FROM public.bot_agent_memory_bindings")
    ) == [{"status": "BOUND", "bundle_digest": bundle["digest"]}]

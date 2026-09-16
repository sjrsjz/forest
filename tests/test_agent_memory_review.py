"""Real PG review races at the Agent evidence, Kernel and query boundaries."""

import asyncio
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_agent_memory
from test_agent_memory import bound, forget, projection
from test_agent_store import respond, specification, tool_calls
from test_embeddings import send, server
from test_memory_config import apply, plan
from test_memory_config import environment as environment
from test_memory_index_store import rows
from test_memory_media import media as media
from test_messages import receive

from forest.agent import AgentRunner
from forest.errors import ForestError
from forest.kernel import Kernel
from forest.memory_runtime import MemoryRuntime
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS
from forest.packages import PackageCatalog, digest_json
from forest.storage.delivery import DeliveryStore

pytestmark = pytest.mark.postgres
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


async def tool_step(media, tmp_path, monkeypatch):
    env = media[0]
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    package = PackageCatalog(tmp_path / "packages").build(EXAMPLE)
    await kernel.install(package, grants=frozenset({"state.read", "state.write"}))
    definition = {
        "name": "increment",
        "component_id": "example.counter",
        "service": "counter.increment",
        "contract_major": 1,
        "authority": "trigger",
        "approval": "never",
        "effect": "idempotent",
        "input_schema": package.schema("schemas/input.json"),
        "package_digest": package.digest,
        "contract_digest": digest_json(package.component["provides"][0]),
    }
    spec = specification()
    spec["tools"] = [definition]
    spec["values"]["allowed_tools"] = [definition]
    original_start = test_agent_memory.start

    async def start_with_spec(messages):
        return await original_start(messages, spec=spec)

    # Parameterize the shared fixture's Run creation, not any production guard.
    monkeypatch.setattr(test_agent_memory, "start", start_with_spec)
    try:
        state, actor, snapshot, memory, bundle = await bound(media)
        request = projection(snapshot, bundle)
        permit = await state.prepare_model(actor, snapshot["run_id"], "model", request)
        calls = [
            {
                "id": "ignored-model-call-id",
                "name": "increment",
                "arguments": {
                    "authority_trigger_id": snapshot["trigger_ids"][0],
                    "arguments": {"increment": 1},
                },
            }
        ]
        head = await respond(state, actor, permit, request, calls=calls)
        return kernel, state, actor, snapshot, memory, bundle, head
    except BaseException:
        await kernel.close()
        raise


async def test_real_kernel_state_gateway_rechecks_evidence_after_tool_admission(
    media, tmp_path, monkeypatch
):
    kernel, state, actor, snapshot, _, bundle, _ = await tool_step(media, tmp_path, monkeypatch)
    original = media[0].store.commit_state
    entered = False

    async def revoke_then_commit(*args, **kwargs):
        nonlocal entered
        entered = True
        await forget(media, bundle)
        return await original(*args, **kwargs)

    monkeypatch.setattr(media[0].store, "commit_state", revoke_then_commit)
    try:
        permit = await state.permit_tool(
            actor,
            snapshot["run_id"],
            "call-0",
            authority_trigger_id=snapshot["trigger_ids"][0],
        )
        with pytest.raises(ForestError) as rejected:
            await kernel.invoke(
                "example.counter",
                "counter.increment",
                {"increment": 1},
                operation_id=permit.operation_id,
                agent_tool_id=permit.operation_id,
                expected_policy_revision=actor.policy_revision,
            )
        assert entered, "The actual native Worker must reach the host state-write Gateway"
        # Both the generation-wide exposure guard and the narrower Agent
        # evidence guard reject this same revocation before any state write.
        assert rejected.value.code in {"AGENT_MEMORY_REVOKED", "GENERATION_EXPOSURE_REVOKED"}
        assert not await rows(
            media,
            "SELECT * FROM public.bot_state_steps WHERE call_id IN (SELECT call_id FROM public.bot_calls WHERE agent_tool_id IS NOT NULL)",
        )
        assert not await rows(
            media, "SELECT * FROM public.bot_component_state WHERE component_id='example.counter'"
        )
        assert (await state.fail(actor, snapshot["run_id"], rejected.value.code))[
            "phase"
        ] == "BLOCKED_UNKNOWN"
        # This test calls Kernel directly and never delivers its failure to the
        # Agent observation boundary. A dispatched tool without an observation
        # must remain UNKNOWN; it cannot be replayed or silently discarded.
        assert await rows(media, "SELECT status FROM public.bot_agent_tools WHERE operation_id=%s",
            (permit.operation_id,)) == [{"status": "UNKNOWN"}]
        assert not await rows(media, "SELECT * FROM public.bot_conversation_replies")
    finally:
        await kernel.close()


async def test_revoke_before_kernel_call_exists_finishes_run_instead_of_unknown(
    media, tmp_path, monkeypatch
):
    kernel, state, actor, snapshot, _, bundle, _ = await tool_step(media, tmp_path, monkeypatch)
    original = kernel.invoke_with_receipt
    entered = False

    async def revoke_then_invoke(*args, **kwargs):
        nonlocal entered
        entered = True
        await forget(media, bundle)
        return await original(*args, **kwargs)

    async def saved_spec(_actor, saved):
        return saved

    async def unused(*args):
        return None

    monkeypatch.setattr(kernel, "invoke_with_receipt", revoke_then_invoke)
    runner = AgentRunner(
        state.messages,
        kernel,
        state=state,
        resolve_spec=saved_spec,
        resolve_client=unused,
        materialize_context=unused,
        authorize_tool=unused,
    )
    try:
        context = await state.messages.context(actor, run_id=snapshot["run_id"])
        result = await runner.advance(actor, {"run_id": snapshot["run_id"]}, context)
        assert entered, "The Runner must reach the actual Kernel receipt admission boundary"
        assert not await rows(
            media, "SELECT * FROM public.bot_calls WHERE agent_tool_id IS NOT NULL"
        )
        assert result.status == "FAILED", (
            "A revoked call never admitted by Kernel is definitely unexecuted"
        )
        assert result.error_code == "AGENT_MEMORY_REVOKED"
        assert not await rows(media, "SELECT * FROM public.bot_agent_tools WHERE status='UNKNOWN'")
        assert not await rows(media, "SELECT * FROM public.bot_conversation_replies")
    finally:
        await kernel.close()


async def test_same_cut_tool_round_reuses_frozen_bundle_and_latest_trigger_advances_only_next_step(
    media,
):
    state, actor, snapshot, memory, bundle = await bound(media)
    original = deepcopy(snapshot)

    async def close_round(current, evidence, label):
        request = projection(current, evidence)
        permit = await state.prepare_model(actor, current["run_id"], label, request)
        calls = tool_calls(current)
        calls[0]["id"] = "call-" + label
        proposed = await respond(state, actor, permit, request, calls=calls)
        call_id = proposed["tools"][0]["tool_call_id"]
        grant = await state.permit_tool(
            actor,
            current["run_id"],
            call_id,
            authority_trigger_id=current["trigger_ids"][0],
        )
        await state.observe_tool(actor, grant, status="SUCCEEDED", result={"value": label})
        await state.close_tools(actor, current["run_id"])
        await state.boundary(actor, current["run_id"], "boundary-" + label)
        return await state.snapshot(actor, current["run_id"])

    second = await close_round(snapshot, bundle, "first")
    assert second["logical_step_id"] != snapshot["logical_step_id"]
    assert second["selected_input_end_seq"] == snapshot["selected_input_end_seq"]
    decision = await memory.begin(actor, second)
    assert decision["query_id"] == bundle["query_id"]
    assert (await memory.finish(actor, second))[0] == bundle
    await receive(state.messages, actor.host, "latest-question", trigger=True, user="new-user")
    await receive(state.messages, actor.host, "untriggered-tail", trigger=False)
    assert await memory.begin(actor, second) == {
        **decision,
        "status": "BOUND",
        "bundle_id": bundle["bundle_id"],
        "bundle_digest": bundle["digest"],
    }
    third = await close_round(second, bundle, "second")
    next_decision = await memory.begin(actor, third)
    assert third["selected_input_end_seq"] == snapshot["selected_input_end_seq"] + 1
    assert next_decision["query_id"] != bundle["query_id"]
    assert next_decision["source_unit_ids"] != decision["source_unit_ids"]
    assert all("untriggered-tail" not in str(unit) for unit in third["units"])
    assert any(
        unit.get("author", {}).get("external_user_id") == "new-user" for unit in third["units"]
    )
    assert snapshot == original
    assert len(await rows(media, "SELECT * FROM public.bot_memory_queries")) == 1


async def test_revoked_closed_tool_context_cannot_flush_but_cancel_can_drain_run(media):
    state, actor, snapshot, _, bundle = await bound(media)
    request = projection(snapshot, bundle)
    permit = await state.prepare_model(actor, snapshot["run_id"], "model", request)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    grant = await state.permit_tool(
        actor, snapshot["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(
        actor, grant, status="SUCCEEDED", result={"value": "derived-private-result"}
    )
    await state.close_tools(actor, snapshot["run_id"])
    await forget(media, bundle)
    await receive(state.messages, actor.host, "next-trigger", trigger=True)
    with pytest.raises(ForestError) as denied:
        await state.boundary(actor, snapshot["run_id"], "revoked-boundary")
    assert denied.value.code == "AGENT_MEMORY_REVOKED"
    cancelled = await state.fail(
        actor,
        snapshot["run_id"],
        "USER_CANCEL",
        status="CANCELLED",
        delivery=DeliveryStore(state.store),
    )
    assert cancelled["phase"] == "CANCELLED"
    assert not await rows(media, "SELECT * FROM public.bot_context_units WHERE kind='RUN_STEP'")
    assert not await rows(
        media, "SELECT * FROM public.bot_conversation_sessions WHERE active_run_id IS NOT NULL"
    )


@pytest.mark.parametrize("cancel", [False, True], ids=["query-timeout", "external-cancel"])
async def test_foreground_query_timeout_cancels_real_http_and_releases_owned_tasks(media, cancel):
    env, images, _ = media
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hanging(writer, request):
        entered.set()
        await release.wait()
        await send(writer, {"error": "late"}, status=500)

    async with server(hanging) as (endpoint, requests, _):
        await apply(
            env,
            await plan(
                env,
                values={
                    **DEFAULT_MEMORY_SETTINGS,
                    "enabled": True,
                    "endpoint": endpoint,
                    "deployment_id": "timeout-local",
                    "query_timeout": 1,
                },
                secret="local-test-key",
            ),
        )
        state, actor, head = await test_agent_memory.start((images.messages, env.lease, None))
        snapshot = await state.snapshot(actor, head["run_id"])
        runtime = MemoryRuntime(
            SimpleNamespace(
                bot=SimpleNamespace(store=env.store), messages=images.messages, vault=env.vault
            ),
            messages=images.messages,
        )
        try:
            task = asyncio.create_task(runtime.retrieve_for_step(actor, snapshot, state))
            await asyncio.wait_for(entered.wait(), 3)
            if cancel:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 4)
            else:
                bundle, _ = await asyncio.wait_for(task, 4)
                assert bundle is None or bundle["status"] == "DEGRADED"
            assert len(requests) == 1
            assert not runtime.clients and not runtime.executions
            attempts = await rows(media, "SELECT state FROM public.bot_memory_embedding_attempts")
            assert attempts == [{"state": "UNKNOWN"}]
            assert not await rows(media, "SELECT * FROM public.bot_memory_vectors")
            assert not await rows(media, "SELECT * FROM public.bot_model_transport_attempts")
        finally:
            release.set()
            await runtime.close()


async def test_query_deadline_includes_actual_postgres_search_after_http_completed(
    media, monkeypatch
):
    env, images, _ = media
    searching = asyncio.Event()

    async def embeddings(writer, request):
        await send(
            writer,
            {
                "model": "jina-clip-v2",
                "object": "list",
                "usage": {"total_tokens": 7},
                "data": [{"object": "embedding", "index": 0, "embedding": [1.0] + [0.0] * 511}],
            },
        )

    async with server(embeddings) as (endpoint, requests, _):
        await apply(
            env,
            await plan(
                env,
                values={
                    **DEFAULT_MEMORY_SETTINGS,
                    "enabled": True,
                    "endpoint": endpoint,
                    "deployment_id": "search-timeout-local",
                    "query_timeout": 1,
                },
                secret="local-test-key",
            ),
        )
        state, actor, head = await test_agent_memory.start((images.messages, env.lease, None))
        snapshot = await state.snapshot(actor, head["run_id"])
        runtime = MemoryRuntime(
            SimpleNamespace(
                bot=SimpleNamespace(store=env.store), messages=images.messages, vault=env.vault
            ),
            messages=images.messages,
        )
        original = runtime.queries._search

        async def slow_pg_search(*args, **kwargs):
            # Real DB cancellation must close a query already outside embedding
            # HTTP. The existing exact-vector search still runs if not cancelled.
            async with env.store.pool.connection() as conn:
                searching.set()
                await conn.execute("SELECT pg_sleep(10)")
            return await original(*args, **kwargs)

        monkeypatch.setattr(runtime.queries, "_search", slow_pg_search)
        try:
            task = asyncio.create_task(runtime.retrieve_for_step(actor, snapshot, state))
            await asyncio.wait_for(searching.wait(), 3)
            assert await asyncio.wait_for(task, 3) == (None, {})
            assert len(requests) == 1
            assert not runtime.executions and not runtime.clients
            assert await rows(
                media, "SELECT status,error_code FROM public.bot_agent_memory_bindings"
            ) == [{"status": "BYPASSED", "error_code": "MEMORY_QUERY_TIMEOUT"}]
            assert await rows(
                media,
                "SELECT charged_tokens,reserved_tokens FROM public.bot_memory_embedding_budget_days",
            ) == [{"charged_tokens": 7, "reserved_tokens": 0}]
            assert not await rows(media, "SELECT * FROM public.bot_memory_evidence_bundles")
            # The frozen bypass can be used by this exact step without retrying
            # or silently selecting new candidates after the time budget.
            assert await runtime.retrieve_for_step(actor, snapshot, state) == (None, {})
            assert len(requests) == 1
            await state.prepare_model(
                actor, snapshot["run_id"], "without-timed-out-evidence", projection(snapshot)
            )
        finally:
            await runtime.close()

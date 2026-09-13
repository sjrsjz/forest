"""Real Agent checkpoints with deterministic Provider results and native tools."""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from jsonschema import Draft202012Validator
from test_messages import messages as messages
from test_messages import owner, receive

from forest.agent import AgentRunner
from forest.errors import ForestError
from forest.group_scheduler import GroupScheduler
from forest.kernel import Kernel
from forest.models import ModelCapabilities, ModelResult
from forest.packages import PackageCatalog, digest_json
from forest.storage.agent_payloads import AgentPayloadStore
from forest.storage.delivery import DeliveryStore

pytestmark = pytest.mark.postgres
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


@pytest.mark.parametrize(
    "schema,valid,invalid",
    [
        (
            {
                "type": "object",
                "$defs": {"number": {"type": "integer"}},
                "properties": {
                    "amount": {"$ref": "#/$defs/number"},
                    "literal": {"const": {"$ref": "#/$defs/number"}},
                },
                "required": ["amount", "literal"],
                "default": {"$ref": "#/$defs/number"},
                "examples": [{"$ref": "#/$defs/number"}],
            },
            {"amount": 1, "literal": {"$ref": "#/$defs/number"}},
            {"amount": "1", "literal": {"$ref": "#/$defs/number"}},
        ),
        (
            {
                "type": "object",
                "properties": {"child": {"$ref": "#"}},
                "additionalProperties": False,
            },
            {"child": {"child": {}}},
            {"child": {"unexpected": 1}},
        ),
        (
            {
                "$dynamicAnchor": "node",
                "type": "object",
                "$defs": {"number": {"$anchor": "number", "type": "integer"}},
                "properties": {"amount": {"$ref": "#number"}, "child": {"$dynamicRef": "#node"}},
                "additionalProperties": False,
            },
            {"amount": 1, "child": {"amount": 2}},
            {"child": {"amount": "2"}},
        ),
        (
            {
                "type": "object",
                "$defs": {"n": {"type": "integer"}},
                "properties": {"amount": {"$dynamicRef": "#/$defs/n"}},
            },
            {"amount": 1},
            {"amount": "1"},
        ),
    ],
)
def test_wrapped_tool_contract_preserves_local_references_anchors_and_literal_instances(
    schema, valid, invalid
):
    original = deepcopy(schema)
    parameters = AgentRunner._tool_schemas(
        {"tools": [{"name": "example", "input_schema": schema}]},
        {"triggers": [{"trigger_id": "trigger-one"}]},
    )[0]["parameters"]
    Draft202012Validator.check_schema(parameters)
    wrapped = Draft202012Validator(parameters)
    service = Draft202012Validator(schema)
    assert schema == original
    assert service.is_valid(valid) and not service.is_valid(invalid)
    assert wrapped.is_valid({"authority_trigger_id": "trigger-one", "arguments": valid})
    assert not wrapped.is_valid({"authority_trigger_id": "trigger-one", "arguments": invalid})
    assert not wrapped.is_valid({"authority_trigger_id": "another-trigger", "arguments": valid})
    for keyword in ("default", "examples"):
        if keyword in schema:
            assert parameters["properties"]["arguments"][keyword] == original[keyword]


class ScriptedClient:
    def __init__(self):
        self.prepared = []
        self.responses = []

    async def dispatch(self, prepared, *, permitcheck, timeout=None):
        await permitcheck(prepared)
        self.prepared.append(prepared)
        response = self.responses.pop(0) if self.responses else {"text": "final response"}
        if callable(response):
            response = await response(prepared)
        return ModelResult(
            response.get("status", "RESPONDED"),
            prepared.request_id,
            prepared.transport_attempt_id,
            prepared.payload_digest,
            text=response.get("text", ""),
            tool_calls_json=json.dumps(response.get("tool_calls", [])),
            usage_json=json.dumps(
                {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            ),
        )


async def eventually(check, timeout=10):
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.02)


async def query(repository, sql, parameters=()):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute(sql, parameters)).fetchone()


@pytest_asyncio.fixture
async def agent_env(messages):
    repository, lease, _ = messages
    # The mailbox fixture already owns a real host lease. This Kernel executes
    # under that exact authority rather than creating a second owner.
    kernel = Kernel(repository.store)
    kernel.lease = lease
    client = ScriptedClient()
    spec = {
        "config_revision": 0,
        "values": {
            "max_steps": 8,
            "run_timeout": 240,
            "model_timeout": 30,
            "daily_token_budget": 1000000,
            "max_output_tokens": 1024,
            "stream": False,
            "allowed_tools": [],
        },
        "tools": [],
    }
    authorized = []

    async def resolve_spec(actor, saved):
        return saved or spec

    async def resolve_client(actor, saved):
        return client, ModelCapabilities("test-provider", "test-model", "capabilities-v1")

    async def materialize(actor, saved, snapshot):
        return snapshot, {}

    async def authorize(actor, saved, definition, trigger):
        authorized.append((definition["name"], trigger["trigger_id"], trigger["principal_ref"]))

    runner = AgentRunner(
        repository,
        kernel,
        resolve_spec=resolve_spec,
        resolve_client=resolve_client,
        materialize_context=materialize,
        authorize_tool=authorize,
    )
    try:
        yield repository, lease, kernel, runner, client, spec, authorized
    finally:
        await kernel.close()


async def begin(environment, *, group="group-1", number=1):
    repository, lease, _, _, _, _, _ = environment
    message = await receive(repository, lease, number, trigger=True, group=group)
    actor = await owner(repository, lease, message["conversation_id"], uuid4().hex)
    selected = await repository.select_input(actor, "initial:" + uuid4().hex)
    context = await repository.context(actor, run_id=selected["run_id"])
    return actor, selected, context


async def resume(environment, actor):
    repository, lease, _, _, _, _, _ = environment
    await repository.release_actor(actor)
    actor = await owner(repository, lease, actor.conversation_id, uuid4().hex)
    selected = await repository.select_input(actor, "resume:" + uuid4().hex)
    return actor, selected, await repository.context(actor, run_id=selected["run_id"])


async def install_tool(environment, tmp_path, *, approval="never"):
    _, _, kernel, _, _, spec, _ = environment
    snapshot = PackageCatalog(tmp_path / "packages").build(EXAMPLE)
    await kernel.install(snapshot, grants=frozenset({"state.read", "state.write"}))
    definition = {
        "name": "increment",
        "component_id": "example.counter",
        "service": "counter.increment",
        "contract_major": 1,
        "authority": "trigger",
        "approval": approval,
        "effect": "idempotent",
        "input_schema": snapshot.schema("schemas/input.json"),
        "package_digest": snapshot.digest,
        "contract_digest": digest_json(snapshot.component["provides"][0]),
    }
    spec["tools"] = [definition]
    spec["values"]["allowed_tools"] = [definition]
    return definition


def proposed(trigger_id, *, call_id="call-one", increment=1):
    return {
        "tool_calls": [
            {
                "id": call_id,
                "name": "increment",
                "arguments": {
                    "authority_trigger_id": trigger_id,
                    "arguments": {"increment": increment},
                },
            }
        ]
    }


async def test_final_ready_recovery_uses_saved_revision_and_does_not_claim_new_trigger(agent_env):
    repository, lease, _, runner, client, _, _ = agent_env
    actor, selected, context = await begin(agent_env)
    disposition = await runner.advance(actor, selected, context)
    assert disposition.status == "FINAL_READY"
    await receive(repository, lease, 2, trigger=True)
    await receive(repository, lease, 3)
    actor, resumed, context = await resume(agent_env, actor)
    recovered = await runner.advance(actor, resumed, context)
    assert recovered.status == "FINAL_READY" and recovered.run_id == disposition.run_id
    reply = await runner.finalize(actor, recovered, DeliveryStore(repository.store))
    assert reply["input_revision"] == 1
    assert len(client.prepared) == 1
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_context_input_batches"))[
        "n"
    ] == 1
    assert (
        await query(
            repository,
            "SELECT count(*) AS n FROM public.bot_conversation_triggers WHERE status='PENDING'",
        )
    )["n"] == 1


async def test_confirmed_unsent_retry_keeps_exact_wire_and_original_input_cut(agent_env):
    repository, lease, _, runner, client, _, _ = agent_env
    client.responses = [{"status": "NOT_SENT"}, {"text": "retry succeeded"}]
    actor, selected, context = await begin(agent_env)
    assert (await runner.advance(actor, selected, context)).status == "YIELD"
    await receive(repository, lease, 2, trigger=True)
    actor, selected, context = await resume(agent_env, actor)
    final = await runner.advance(actor, selected, context)
    assert final.status == "FINAL_READY"
    first, second = client.prepared
    assert first.body == second.body and first.request_id == second.request_id
    assert first.transport_attempt_id != second.transport_attempt_id
    assert first.manifest == second.manifest
    reply = await runner.finalize(actor, final, DeliveryStore(repository.store))
    assert reply["input_revision"] == 1
    assert (
        await query(repository, "SELECT count(*) AS n FROM public.bot_model_transport_attempts")
    )["n"] == 2


async def test_unknown_model_blocks_session_without_automatic_dispatch_or_hot_poll(agent_env):
    repository, lease, _, runner, client, _, _ = agent_env
    client.responses = [{"status": "UNKNOWN"}]
    first = await receive(repository, lease, 1, trigger=True)
    invocations = []

    async def advance(*args):
        invocations.append(args[1]["run_id"])
        return await runner.advance(*args)

    scheduler = GroupScheduler(
        repository, lease=lease, execute=advance, finalize_agent=runner.finalize, poll_interval=0.02
    )
    try:
        await scheduler.start()

        async def blocked():
            row = await query(repository, "SELECT phase FROM public.bot_agent_runs")
            return row and row["phase"] == "BLOCKED_UNKNOWN"

        await eventually(blocked)
        await receive(repository, lease, 2, trigger=True)
        await asyncio.sleep(0.3)
        assert len(client.prepared) == 1 and len(invocations) <= 2
        conversation = await query(
            repository,
            "SELECT active_session_id,actor_id FROM public.bot_conversations WHERE conversation_id=%s",
            (first["conversation_id"],),
        )
        assert conversation["active_session_id"] and conversation["actor_id"] is None
        assert (await query(repository, "SELECT count(*) AS n FROM public.bot_conversation_runs"))[
            "n"
        ] == 1
    finally:
        await scheduler.close()


async def test_native_tool_group_closes_before_next_trigger_cut_and_final_revision(
    agent_env, tmp_path
):
    repository, lease, _, runner, client, _, authorized = agent_env
    await install_tool(agent_env, tmp_path)
    actor, selected, context = await begin(agent_env)
    trigger = selected["trigger_ids"][0]
    client.responses = [proposed(trigger), {"text": "all messages through the final trigger"}]
    first = await runner.advance(actor, selected, context)
    assert first.status == "YIELD", await query(
        repository, "SELECT status,result FROM public.bot_agent_tools"
    )
    await receive(repository, lease, 2)
    await receive(repository, lease, 3, trigger=True, user="user-2")
    await receive(repository, lease, 4)
    actor, selected, context = await resume(agent_env, actor)
    final = await runner.advance(actor, selected, context)
    assert final.status == "FINAL_READY"
    assert client.prepared[-1].manifest["input_revision"] == 2
    assert client.prepared[-1].manifest["selected_input_end_seq"] == 3
    body = json.loads(client.prepared[-1].body)
    roles = [entry["role"] for entry in body["messages"]]
    assistant = roles.index("assistant")
    assert roles[assistant + 1] == "tool"
    assert "message 4" not in client.prepared[-1].body.decode()
    assert authorized[0][1] == trigger
    reply = await runner.finalize(actor, final, DeliveryStore(repository.store))
    assert reply["input_revision"] == 2
    assert (
        await query(
            repository, "SELECT count(*) AS n FROM public.bot_context_units WHERE kind='RUN_STEP'"
        )
    )["n"] == 1
    assert (
        await query(
            repository,
            "SELECT count(*) AS n FROM public.bot_calls WHERE component_id='example.counter'",
        )
    )["n"] == 1


async def test_completed_native_tool_receipt_recovers_lost_ack_without_second_effect(
    agent_env, tmp_path, monkeypatch
):
    repository, _, kernel, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    actor, selected, context = await begin(agent_env)
    client.responses = [proposed(selected["trigger_ids"][0], increment=7)]
    invoke = kernel.invoke_with_receipt
    executions = []

    async def lose_return(*args, **kwargs):
        executions.append(kwargs["operation_id"])
        await invoke(*args, **kwargs)
        raise OSError("lost local result acknowledgement")

    monkeypatch.setattr(kernel, "invoke_with_receipt", lose_return)
    result = await runner.advance(actor, selected, context)
    assert result.status == "YIELD", await query(
        repository, "SELECT status,result FROM public.bot_agent_tools"
    )
    assert len(executions) == 1
    tool = await query(repository, "SELECT * FROM public.bot_agent_tools")
    assert tool["result"] is None
    async with repository.store.pool.connection() as conn, conn.transaction():
        tool = await AgentPayloadStore(repository.store).hydrate_tool_in(conn, tool)
    assert tool["status"] == "SUCCEEDED" and tool["result"]["value"] == 7
    assert (await kernel.store.snapshot_state(kernel.lease, "example.counter"))["counter"][
        "value"
    ] == 7


async def test_wait_approval_keeps_one_session_without_tool_execution_or_hot_poll(
    agent_env, tmp_path
):
    repository, lease, _, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path, approval="always")
    first = await receive(repository, lease, 1, trigger=True)
    client.responses = [proposed(first["event_id"])]
    # Trigger IDs are host records, not necessarily platform event IDs.
    trigger = await query(repository, "SELECT trigger_id FROM public.bot_conversation_triggers")
    client.responses = [proposed(trigger["trigger_id"])]
    calls = []

    async def advance(*args):
        calls.append(args[1]["run_id"])
        return await runner.advance(*args)

    scheduler = GroupScheduler(
        repository, lease=lease, execute=advance, finalize_agent=runner.finalize, poll_interval=0.02
    )
    try:
        await scheduler.start()

        async def waiting():
            current = await query(repository, "SELECT phase FROM public.bot_agent_runs")
            return current and current["phase"] == "WAIT_APPROVAL"

        await eventually(waiting)
        await receive(repository, lease, 2, trigger=True)
        await asyncio.sleep(0.3)
        assert len(calls) <= 2 and len(client.prepared) == 1
        assert (await query(repository, "SELECT count(*) AS n FROM public.bot_calls"))["n"] == 0
        assert (await query(repository, "SELECT active_session_id FROM public.bot_conversations"))[
            "active_session_id"
        ]
    finally:
        await scheduler.close()


async def test_tool_cannot_borrow_another_runs_trigger_authority(agent_env, tmp_path):
    repository, _, _, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    actor, selected, context = await begin(agent_env)
    _, other_selected, _ = await begin(agent_env, group="other")
    client.responses = [proposed(other_selected["trigger_ids"][0])]
    result = await runner.advance(actor, selected, context)
    assert result.status == "FAILED" and result.error_code == "AGENT_TOOL_AUTHORITY_INVALID"
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_calls"))["n"] == 0


async def test_materializer_cannot_mutate_input_author_or_append_queued_content(agent_env):
    repository, _, _, runner, client, _, _ = agent_env
    actor, selected, context = await begin(agent_env)

    async def mutate(actor, spec, snapshot):
        snapshot["units"][0]["parts"] = [{"type": "text", "text": "unselected tail"}]
        return snapshot, {}

    runner.materialize_context = mutate
    result = await runner.advance(actor, selected, context)
    assert result.status == "FAILED" and result.error_code == "MODEL_INPUT_CUT_CHANGED"
    assert not client.prepared
    saved = await query(repository, "SELECT * FROM public.bot_agent_steps")
    assert saved["snapshot"] is None
    async with repository.store.pool.connection() as conn, conn.transaction():
        saved = await AgentPayloadStore(repository.store).hydrate_step_in(conn, saved)
    assert saved["snapshot"]["units"][0]["parts"][0]["text"] == "message 1"


async def test_read_only_tool_cannot_write_through_native_component_capability(agent_env, tmp_path):
    repository, _, kernel, runner, client, _, _ = agent_env
    definition = await install_tool(agent_env, tmp_path)
    definition["effect"] = "read"
    actor, selected, context = await begin(agent_env)
    client.responses = [proposed(selected["trigger_ids"][0], increment=9)]
    result = await runner.advance(actor, selected, context)
    assert result.status == "BLOCKED_UNKNOWN"
    tool = await query(repository, "SELECT * FROM public.bot_agent_tools")
    assert tool["result"] is None
    async with repository.store.pool.connection() as conn, conn.transaction():
        tool = await AgentPayloadStore(repository.store).hydrate_tool_in(conn, tool)
    assert tool["status"] == "UNKNOWN" and tool["result"]["error_code"] == "AGENT_TOOL_READ_ONLY"
    assert "counter" not in await kernel.store.snapshot_state(kernel.lease, "example.counter")


async def test_model_cancellation_preserves_unknown_and_never_dispatches_again(agent_env):
    repository, _, _, runner, client, _, _ = agent_env
    actor, selected, context = await begin(agent_env)
    entered = asyncio.Event()

    async def block(_):
        entered.set()
        await asyncio.Event().wait()

    client.responses = [block]
    task = asyncio.create_task(runner.advance(actor, selected, context))
    await asyncio.wait_for(entered.wait(), 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    head = await runner.state.load(actor, selected["run_id"])
    assert head["phase"] == "BLOCKED_UNKNOWN"
    assert head["current_step"]["attempt"]["status"] == "UNKNOWN"
    actor, selected, context = await resume(agent_env, actor)
    assert (await runner.advance(actor, selected, context)).status == "BLOCKED_UNKNOWN"
    assert len(client.prepared) == 1
    assert (await query(repository, "SELECT status FROM public.bot_conversation_runs"))[
        "status"
    ] == "RUNNING"


async def test_tool_backpressure_before_admission_retries_same_operation_once(
    agent_env, tmp_path, monkeypatch
):
    repository, _, kernel, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    actor, selected, context = await begin(agent_env)
    client.responses = [proposed(selected["trigger_ids"][0], increment=4)]
    invoke = kernel.invoke_with_receipt
    operations = []

    async def reject_first(*args, **kwargs):
        operations.append(kwargs["operation_id"])
        if len(operations) == 1:
            raise ForestError("BACKPRESSURE", "Real capacity is unavailable", 503)
        return await invoke(*args, **kwargs)

    monkeypatch.setattr(kernel, "invoke_with_receipt", reject_first)
    assert (await runner.advance(actor, selected, context)).status == "WAIT_TOOL"
    actor, selected, context = await resume(agent_env, actor)
    assert (await runner.advance(actor, selected, context)).status == "YIELD"
    assert len(operations) == 2 and operations[0] == operations[1]
    assert len(client.prepared) == 1
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_calls"))["n"] == 1
    assert (await kernel.store.snapshot_state(kernel.lease, "example.counter"))["counter"][
        "value"
    ] == 4


async def test_scheduler_final_ready_recovery_does_not_reprepare_over_budget_context(agent_env):
    repository, lease, _, runner, client, _, _ = agent_env
    actor, selected, context = await begin(agent_env)
    assert (await runner.advance(actor, selected, context)).status == "FINAL_READY"
    await repository.release_actor(actor)
    repository.context_token_budget = 1
    scheduler = GroupScheduler(
        repository,
        lease=lease,
        execute=runner.advance,
        finalize_agent=runner.finalize,
        poll_interval=0.02,
    )
    try:
        await scheduler.start()

        async def completed():
            row = await query(repository, "SELECT phase FROM public.bot_agent_runs")
            return row and row["phase"] == "COMPLETED"

        await eventually(completed)
        assert len(client.prepared) == 1
        assert (
            await query(repository, "SELECT count(*) AS n FROM public.bot_conversation_replies")
        )["n"] == 1
    finally:
        await scheduler.close()

"""Real database private child leases, immutable evidence, budgets and recovery."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_agent_runner import agent_env as agent_env
from test_agent_store import prepare, query, respond, specification, start
from test_messages import fails, owner, receive
from test_messages import messages as messages

from forest.children import ChildScheduler
from forest.errors import ForestError
from forest.models import (
    ChatCompletionsClient,
    ModelCapabilities,
    ModelResult,
    prepare_model_request,
)
from forest.storage.agent import AgentStore
from forest.storage.children import TOOL_NAME, ChildStore
from forest.storage.delivery import DeliveryStore

pytestmark = pytest.mark.postgres


def spec():
    value = specification()
    value["values"].update(
        children_enabled=True,
        child_concurrency=2,
        max_children=4,
        child_timeout=90,
        child_max_output_tokens=64,
        run_token_budget=20000,
    )
    return value


async def family(messages, count=1, configuration=None):
    agent, actor, head = await start(messages, spec=configuration or spec())
    snapshot, prepared, permit = await prepare(agent, actor, head)
    calls = [
        {
            "id": "child-call-" + str(i),
            "name": TOOL_NAME,
            "arguments": {
                "authority_trigger_id": snapshot["trigger_ids"][0],
                "arguments": {
                    "task": "Analyze evidence " + str(i),
                    "source_unit_ids": [snapshot["units"][0]["unit_id"]],
                },
            },
        }
        for i in range(count)
    ]
    await respond(agent, actor, permit, prepared, text="Analyzing", calls=calls)
    children = ChildStore(agent)
    await children.spawn(actor, head["run_id"])
    return children, actor, head, snapshot


async def acquire(children, host, child_id=None, ttl=120):
    if child_id is None:
        child_id = (await children.pending(host))[0]["child_id"]
    lease = await children.claim(host, child_id, "test-worker", ttl=ttl)
    row = await children.load(lease)
    prepared = prepare_model_request(
        row["snapshot"],
        ModelCapabilities("child", "test", "v1", supports_tools=False),
        request_id=row["child_id"] + ":request",
        transport_attempt_id=row["child_id"] + ":attempt",
        output_tokens=64,
        stream=False,
    )
    attempt_id = await children.prepare(lease, prepared)
    return lease, row, prepared, attempt_id


async def finish(
    children, lease, prepared, attempt_id, text="private candidate", status="RESPONDED"
):
    if status != "NOT_SENT":
        await children.check_permit(lease, attempt_id, prepared)
    return await children.complete(
        lease,
        attempt_id,
        ModelResult(
            status,
            prepared.request_id,
            attempt_id,
            prepared.payload_digest,
            text=text,
            usage_json='{"total_tokens":7}',
        ),
    )


async def test_child_results_need_parent_adoption_and_preserve_input_cut(messages):
    children, actor, head, snapshot = await family(messages, 2)
    ids = [r["child_id"] for r in await children.pending(messages[1])]
    first = await acquire(children, messages[1], ids[0])
    second = await acquire(children, messages[1], ids[1])
    await messages[0].release_actor(actor)
    await receive(messages[0], messages[1], "queued-trigger", trigger=True)
    await receive(messages[0], messages[1], "queued-tail")
    await finish(children, second[0], second[2], second[3], text="second")
    await finish(children, first[0], first[2], first[3], text="first")
    assert await query(children.agent, "SELECT * FROM public.bot_conversation_replies") == []
    assert (
        await query(children.agent, "SELECT * FROM public.bot_context_units WHERE kind='RUN_STEP'")
        == []
    )
    actor2 = await owner(messages[0], messages[1], actor.conversation_id, "new-parent")
    await children.agent.ensure_run(actor2, head["run_id"], spec())
    await children.adopt(actor2, head["run_id"])
    await children.agent.close_tools(actor2, head["run_id"])
    frozen = await children.agent.snapshot(actor2, head["run_id"])
    assert (
        frozen["input_revision"] == snapshot["input_revision"]
        and frozen["selected_input_end_seq"] == 1
    )
    tool_results = [u["parts"][0]["value"]["text"] for u in frozen["units"] if u["role"] == "tool"]
    assert tool_results == ["first", "second"]
    assert all(
        r["status"] == "ACCEPTED"
        for r in await query(children.agent, "SELECT status FROM public.bot_agent_children")
    )


async def test_waiting_children_do_not_hot_poll_or_create_another_foreground(messages):
    children, actor, head, _ = await family(messages)
    await receive(messages[0], messages[1], "new", trigger=True)
    assert await messages[0].pending(messages[1]) == []
    selected = await messages[0].select_input(actor, "busy")
    assert selected["run_id"] == head["run_id"]
    lease, _, prepared, attempt = await acquire(children, messages[1])
    await finish(children, lease, prepared, attempt)
    assert len(await messages[0].pending(messages[1])) == 1


async def test_parent_cancellation_invalidates_children_and_keeps_unknown_cost(messages):
    children, actor, head, _ = await family(messages)
    lease, row, prepared, attempt = await acquire(children, messages[1])
    await children.check_permit(lease, attempt, prepared)
    await children.agent.fail(actor, head["run_id"], "PARENT_CANCELLED", status="CANCELLED")
    await fails(
        "CHILD_PARENT_REVOKED",
        children.complete(
            lease,
            attempt,
            ModelResult(
                "RESPONDED", prepared.request_id, attempt, prepared.payload_digest, text="late"
            ),
        ),
    )
    saved = (await query(children.agent, "SELECT * FROM public.bot_agent_children"))[0]
    assert saved["status"] == "CANCELLED"
    cost = (await query(children.agent, "SELECT * FROM public.bot_agent_tree_budgets"))[0]
    assert cost["reserved_tokens"] == prepared.reserved_tokens and cost["charged_tokens"] == 15


async def test_expired_child_owner_recovers_dispatch_as_unknown(messages):
    children, actor, head, _ = await family(messages)
    lease, row, prepared, attempt = await acquire(children, messages[1], ttl=0.2)
    await children.check_permit(lease, attempt, prepared)
    await asyncio.sleep(0.25)
    assert await children.claim(messages[1], row["child_id"], "replacement") is None
    assert (await children.agent.load(actor, head["run_id"]))["phase"] == "BLOCKED_UNKNOWN"
    assert (
        await query(children.agent, "SELECT status,settled FROM public.bot_child_model_attempts")
    )[0] == {"status": "UNKNOWN", "settled": False}
    await fails(
        "STALE_CHILD",
        children.complete(
            lease,
            attempt,
            ModelResult(
                "RESPONDED", prepared.request_id, attempt, prepared.payload_digest, text="late"
            ),
        ),
    )


async def test_unsent_child_retries_exact_wire_with_new_attempt_and_cannot_recurse(messages):
    children, _, _, _ = await family(messages)
    lease, row, prepared, attempt = await acquire(children, messages[1])
    await finish(children, lease, prepared, attempt, status="NOT_SENT")
    lease2 = await children.claim(messages[1], row["child_id"], "replacement")
    saved = await children.load(lease2)
    retry = children.replay(saved)
    assert retry.body == prepared.body and retry.request_id == prepared.request_id
    assert retry.transport_attempt_id != attempt
    second = await children.prepare(lease2, retry)
    await children.check_permit(lease2, second, retry)
    result = await children.complete(
        lease2,
        second,
        ModelResult(
            "RESPONDED",
            retry.request_id,
            second,
            retry.payload_digest,
            tool_calls_json=json.dumps([{"id": "recursive", "name": TOOL_NAME, "arguments": {}}]),
            usage_json='{"total_tokens":3}',
        ),
    )
    assert result["status"] == "FAILED" and result["error_code"] == "CHILD_TOOL_OR_EMPTY_OUTPUT"
    assert len(await query(children.agent, "SELECT * FROM public.bot_agent_children")) == 1


async def test_child_and_foreground_share_run_tree_budget(messages):
    configuration = spec()
    configuration["values"]["run_token_budget"] = 3000
    children, _, _, _ = await family(messages, 2, configuration)
    ids = [r["child_id"] for r in await children.pending(messages[1])]
    first = await acquire(children, messages[1], ids[0])
    second_lease = await children.claim(messages[1], ids[1], "second")
    second = await children.load(second_lease)
    prepared = prepare_model_request(
        second["snapshot"],
        ModelCapabilities("child", "test", "v1", supports_tools=False),
        request_id="second",
        transport_attempt_id="second-attempt",
        output_tokens=64,
        stream=False,
    )
    await fails("RUN_TOKEN_BUDGET_EXCEEDED", children.prepare(second_lease, prepared))
    account = (await query(children.agent, "SELECT * FROM public.bot_agent_tree_budgets"))[0]
    assert (
        account["charged_tokens"] == 15 and account["reserved_tokens"] == first[2].reserved_tokens
    )


async def test_fake_source_unit_and_disabled_children_rejected(messages):
    agent, actor, head = await start(messages, spec=spec())
    snapshot, prepared, permit = await prepare(agent, actor, head)
    await respond(
        agent,
        actor,
        permit,
        prepared,
        calls=[
            {
                "id": "bad",
                "name": TOOL_NAME,
                "arguments": {
                    "authority_trigger_id": snapshot["trigger_ids"][0],
                    "arguments": {
                        "task": "read another group",
                        "source_unit_ids": ["foreign-source"],
                    },
                },
            }
        ],
    )
    await fails("CHILD_INPUT_INVALID", ChildStore(agent).spawn(actor, head["run_id"]))
    assert await query(agent, "SELECT * FROM public.bot_agent_children") == []


async def test_real_http_child_scheduler_closes_resources_and_parent_alone_sends(messages):
    children, actor, head, _ = await family(messages)
    bodies = []
    handlers = set()

    async def serve(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in header.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            bodies.append(await reader.readexactly(length))
            body = json.dumps(
                {
                    "id": "child-result",
                    "model": "test",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "HTTP private analysis"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7},
                }
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = ChatCompletionsClient(f"http://127.0.0.1:{port}/v1", "test-key")

    async def resolve(lease, row):
        return client, ModelCapabilities("child", "test", "v1", supports_tools=False)

    scheduler = ChildScheduler(children, lambda: messages[1], resolve, poll_interval=0.02)
    try:
        await scheduler.start()
        async with asyncio.timeout(8):
            while (await query(children.agent, "SELECT status FROM public.bot_agent_children"))[0][
                "status"
            ] != "RESULT_READY":
                await asyncio.sleep(0.03)
        assert len(bodies) == 1 and "tools" not in json.loads(bodies[0])
        attempt = (await query(children.agent, "SELECT * FROM public.bot_child_model_attempts"))[0]
        assert bytes(attempt["request_body"]) == bodies[0]
        assert await query(children.agent, "SELECT * FROM public.bot_send_intents") == []
        await children.adopt(actor, head["run_id"])
        await children.agent.close_tools(actor, head["run_id"])
        _, prepared, permit = await prepare(children.agent, actor, head)
        await respond(children.agent, actor, permit, prepared, text="Parent answer")
        await children.agent.commit_final(
            actor, head["run_id"], "final", DeliveryStore(messages[0].store)
        )
        assert len(await query(children.agent, "SELECT * FROM public.bot_send_intents")) == 1
    finally:
        async with asyncio.timeout(5):
            await scheduler.close()
        await client.aclose()
        server.close()
        await server.wait_closed()
        if handlers:
            for task in tuple(handlers):
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)
    assert scheduler.task.done() and not scheduler.workers


async def test_scheduler_close_fences_scan_result_after_dependency_consumes_cancellation():
    entered = asyncio.Event()
    executed = []
    scans = 0

    async def pending(host):
        nonlocal scans
        scans += 1
        if scans == 1:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Model the pool/driver boundary returning a completed result
                # after cancellation was delivered during its internal wait.
                return [{"child_id": "child-returned-during-close"}]
        return []

    async def execute(child_id):
        executed.append(child_id)
        await asyncio.Event().wait()

    scheduler = ChildScheduler(
        SimpleNamespace(pending=pending), lambda: None, None, poll_interval=0.01
    )
    scheduler._execute = execute
    try:
        await scheduler.start()
        await asyncio.wait_for(entered.wait(), 2)
        async with asyncio.timeout(1):
            await scheduler.close()
        assert scheduler.task.done() and not scheduler.workers
        assert scans == 1 and executed == []
    finally:
        # Even the pre-fix implementation creates an unowned late worker. The
        # regression test must reclaim that task when its assertion fails.
        tasks = [scheduler.task, *scheduler.workers.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_child_multimodal_input_uses_selected_immutable_parent_capture(messages):
    from test_media import configure, context, media_server, picture
    from test_media import respond as serve_image

    from forest.storage.media import MediaStore

    content = picture()

    async def serve(writer, header):
        await serve_image(writer, content, extra="Content-Type: image/png\r\n")

    repository, host, _ = messages
    async with media_server(serve) as (url, requests):
        await configure(repository, [url])
        actor, initial = await context(repository, host, url + "/private.png")
        agent = AgentStore(repository)
        head = await agent.ensure_run(actor, initial["run_id"], spec())
        snapshot, prepared, permit = await prepare(agent, actor, head)
        captured = await MediaStore(repository).materialize(actor, snapshot, allowed_origins=[url])
        assert len(captured) == 1 and len(requests) == 1
        await respond(
            agent,
            actor,
            permit,
            prepared,
            calls=[
                {
                    "id": "image-child",
                    "name": TOOL_NAME,
                    "arguments": {
                        "authority_trigger_id": snapshot["trigger_ids"][0],
                        "arguments": {
                            "task": "Describe this image",
                            "source_unit_ids": [snapshot["units"][0]["unit_id"]],
                        },
                    },
                }
            ],
        )
        children = ChildStore(agent)
        await children.spawn(actor, head["run_id"])
        child_id = (await children.pending(host))[0]["child_id"]
        child = await children.claim(host, child_id, "vision")
        row = await children.load(child)
        images = await children.media(child)
        assert images == captured and len(requests) == 1
        vision = ModelCapabilities(
            "child", "vision", "v1", supports_tools=False, input_modalities=("text", "image")
        )
        image_request = prepare_model_request(
            row["snapshot"],
            vision,
            request_id="vision-child",
            transport_attempt_id="vision-attempt",
            output_tokens=64,
            stream=False,
            media=images,
        )
        text_request = prepare_model_request(
            row["snapshot"],
            replace(vision, input_modalities=("text",)),
            request_id="text-child",
            transport_attempt_id="text-attempt",
            output_tokens=64,
            stream=False,
            media=images,
        )
        assert b"data:image/png;base64," in image_request.body
        assert b"data:image/png;base64," not in text_request.body
        assert row["snapshot"]["units"][0]["parts"][0]["type"] == "image"
        await children.prepare(child, image_request)
        async with repository.store.pool.connection() as conn:
            await conn.execute(
                "UPDATE public.bot_message_inbox SET context_disposition='RESTRICTED' WHERE bot_id=%s AND conversation_id=%s",
                (host.bot_id, actor.conversation_id),
            )
        await fails(
            "CHILD_SOURCE_REVOKED",
            children.check_permit(child, image_request.transport_attempt_id, image_request),
        )


async def test_runner_owns_child_proposal_and_resumes_only_after_private_result(agent_env):
    from test_agent_runner import begin, resume

    repository, host, _, runner, client, configuration, _ = agent_env
    configuration["values"].update(spec()["values"])
    configuration["values"]["max_output_tokens"] = 64
    actor, selected, context = await begin(agent_env)

    async def propose(prepared):
        unit_id = context["units"][0]["unit_id"]
        assert TOOL_NAME in prepared.manifest["tool_names"]
        return {
            "tool_calls": [
                {
                    "id": "child-runner",
                    "name": TOOL_NAME,
                    "arguments": {
                        "authority_trigger_id": selected["trigger_ids"][0],
                        "arguments": {"task": "Check the evidence", "source_unit_ids": [unit_id]},
                    },
                }
            ]
        }

    client.responses.append(propose)
    waiting = await runner.advance(actor, selected, context)
    assert waiting.status == "WAIT_TOOL" and len(client.prepared) == 1
    assert await repository.pending(host) == []
    children = ChildStore(runner.state)
    lease, _, prepared, attempt_id = await acquire(children, host)
    await finish(children, lease, prepared, attempt_id)
    actor, selected, context = await resume(agent_env, actor)
    resumed = await runner.advance(actor, selected, context)
    assert resumed.status == "YIELD" and len(client.prepared) == 1
    final = await runner.advance(actor, selected, context)
    assert final.status == "FINAL_READY" and len(client.prepared) == 2
    assert b"private candidate" in client.prepared[-1].body
    assert await query(runner.state, "SELECT * FROM public.bot_send_intents") == []


async def test_global_child_capacity_does_not_hold_foreground_model_slots():
    from forest.agent_runtime import _ScopedClient

    runtime = SimpleNamespace(
        global_child_slots=asyncio.Semaphore(0),
        global_model_slots=asyncio.Semaphore(1),
        model_slots=asyncio.Semaphore(1),
    )
    child = _ScopedClient(runtime, "unused", "unused", child=True)
    parent = _ScopedClient(runtime, "unused", "unused")
    completed = []

    async def dispatch(prepared, **kwargs):
        completed.append(prepared)
        return prepared

    child._dispatch = parent._dispatch = dispatch
    waiting = asyncio.create_task(child.dispatch("child", permitcheck=None, timeout=0.15))
    try:
        await asyncio.sleep(0)
        assert await parent.dispatch("parent", permitcheck=None, timeout=0.1) == "parent"
        with pytest.raises(TimeoutError):
            await waiting
        assert completed == ["parent"]
        assert runtime.global_model_slots._value == runtime.model_slots._value == 1
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)


async def test_concurrent_children_from_different_groups_share_bot_day_budget(messages):
    configuration = spec()
    configuration["values"]["daily_token_budget"] = 3000
    children, actor, first, _ = await family(messages, configuration=configuration)
    second_agent, other_actor, second = await start(
        messages, group="other-group", spec=configuration
    )
    snapshot, request, permit = await prepare(second_agent, other_actor, second)
    await respond(
        second_agent,
        other_actor,
        permit,
        request,
        calls=[
            {
                "id": "another-child",
                "name": TOOL_NAME,
                "arguments": {
                    "authority_trigger_id": snapshot["trigger_ids"][0],
                    "arguments": {
                        "task": "Analyze independently",
                        "source_unit_ids": [snapshot["units"][0]["unit_id"]],
                    },
                },
            }
        ],
    )
    await children.spawn(other_actor, second["run_id"])
    claims = []
    for item in await children.pending(messages[1]):
        lease = await children.claim(messages[1], item["child_id"], item["child_id"])
        row = await children.load(lease)
        prepared = prepare_model_request(
            row["snapshot"],
            ModelCapabilities("child", "test", "v1", supports_tools=False),
            request_id=item["child_id"],
            transport_attempt_id=item["child_id"] + ":attempt",
            output_tokens=64,
            stream=False,
        )
        claims.append((lease, prepared))
    results = await asyncio.gather(
        *(children.prepare(lease, prepared) for lease, prepared in claims), return_exceptions=True
    )
    denied = [r for r in results if isinstance(r, ForestError)]
    assert len(denied) == 1 and denied[0].code == "DAILY_TOKEN_BUDGET_EXCEEDED"
    assert len(await query(children.agent, "SELECT * FROM public.bot_child_model_attempts")) == 1
    day = (await query(children.agent, "SELECT * FROM public.bot_agent_budget_days"))[0]
    assert day["charged_tokens"] == 30 and day["reserved_tokens"] > 0


async def test_child_pre_dispatch_crash_replays_bytes_and_old_owner_cannot_reject_new_owner(
    messages,
):
    children, actor, head, _ = await family(messages)
    lease, row, prepared, attempt = await acquire(children, messages[1], ttl=0.2)
    await asyncio.sleep(0.25)
    replacement = await children.claim(messages[1], row["child_id"], "replacement")
    retry = children.replay(await children.load(replacement))
    assert retry.body == prepared.body and retry.request_id == prepared.request_id
    assert retry.transport_attempt_id != attempt
    await children.reject(messages[1], row["child_id"], "STALE_FAILURE", lease=lease)
    assert (await children.load(replacement))["status"] == "RUNNING"
    await fails("STALE_CHILD", children.check_permit(lease, attempt, prepared))
    await children.prepare(replacement, retry)
    await children.check_permit(replacement, retry.transport_attempt_id, retry)
    result = ModelResult(
        "RESPONDED",
        retry.request_id,
        retry.transport_attempt_id,
        retry.payload_digest,
        text="candidate",
        usage_json='{"total_tokens":7}',
    )
    first = await children.complete(replacement, retry.transport_attempt_id, result)
    replayed = await children.complete(replacement, retry.transport_attempt_id, result)
    assert first["result_digest"] == replayed["result_digest"]
    await fails(
        "CHILD_RESULT_CONFLICT",
        children.complete(
            replacement, retry.transport_attempt_id, replace(result, text="replacement")
        ),
    )
    accepted = await children.adopt(actor, head["run_id"])
    replayed = await children.adopt(actor, head["run_id"])
    assert accepted["checkpoint_seq"] == replayed["checkpoint_seq"]
    account = (await query(children.agent, "SELECT * FROM public.bot_agent_tree_budgets"))[0]
    assert account["reserved_tokens"] == 0 and account["charged_tokens"] == 22

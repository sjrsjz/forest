"""Web configuration through durable Agent steps and real HTTP protocol sockets."""

import asyncio
import html
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from test_web_bindings import configure, get, onebot_server, receive, wait_for
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database

from forest.agent_protocol import AssistantBlock, FunctionCallBlock, ModelTurn, serialize_model_turn
from forest.bootstrap import BootstrapState
from forest.errors import ForestError
from forest.management.app import WebSettings
from forest.packages import PackageCatalog, digest_json
from forest.storage.agent_payloads import AgentPayloadStore

pytestmark = pytest.mark.postgres
MODEL_KEY = "only-a-test-model-key"


def module_scope(root):
    return root.replace("/management/v1/bots/", "/management/v1/scopes/bot/", 1) + "/modules"


def _turn(*blocks):
    return serialize_model_turn(ModelTurn(tuple(blocks), "test-provider-candidate"))


def _available_tools(body):
    content = body["messages"][0]["content"]
    match = re.search(r'<available_tools encoding="json">(.*)</available_tools>\Z', content)
    assert match is not None
    return json.loads(html.unescape(match.group(1)))


@asynccontextmanager
async def model_server():
    state = {
        "requests": [],
        "first_entered": asyncio.Event(),
        "release_first": asyncio.Event(),
        "errors": [],
    }
    tasks = set()

    async def handle(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            lines = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1").split("\r\n")
            headers = dict(line.lower().split(": ", 1) for line in lines[1:] if ": " in line)
            assert lines[0] == "POST /v1/chat/completions HTTP/1.1"
            assert headers["authorization"] == "bearer " + MODEL_KEY
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            assert body.get("stream") is False
            state["requests"].append(body)
            text = json.dumps(body, ensure_ascii=False)
            if "first request" in text and not any(
                message.get("role") == "assistant"
                and (
                    message.get("tool_calls")
                    or 'block_type=\\"FunctionCall\\"' in str(message.get("content", ""))
                )
                for message in body["messages"]
            ):
                state["first_entered"].set()
                await state["release_first"].wait()
                authority = _available_tools(body)[0]["function"]["parameters"]["properties"][
                    "authority_trigger_id"
                ]["enum"][0]
                message = {
                    "role": "assistant",
                    "content": _turn(FunctionCallBlock("increment", json.dumps({
                        "authority_trigger_id": authority,
                        "arguments": {"increment": 3},
                    }, sort_keys=True, separators=(",", ":")))),
                }
                reason = "stop"
            else:
                message = {
                    "role": "assistant",
                    "content": _turn(AssistantBlock(
                        "other-final" if "other request" in text else "model-final"
                    )),
                }
                reason = "stop"
            data = json.dumps(
                {
                    "id": "fake-" + str(len(state["requests"])),
                    "model": "test-model",
                    "choices": [{"index": 0, "finish_reason": reason, "message": message}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
                }
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                + str(len(data)).encode()
                + b"\r\n\r\n"
                + data
            )
            await writer.drain()
        except Exception as exc:
            state["errors"].append(type(exc).__name__)
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    state["endpoint"] = "http://127.0.0.1:" + str(server.sockets[0].getsockname()[1]) + "/v1"
    try:
        yield state
    finally:
        state["release_first"].set()
        server.close()
        await server.wait_closed()
        for task in tuple(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def configure_agent(client, root, endpoint, *, approval="never"):
    plan = await post(
        client,
        module_scope(root) + "/agent.runtime/actions/configuration.plan",
        {
            "expected_revision": 0,
            "api_key": MODEL_KEY,
            "values": {
                "enabled": True,
                "endpoint": endpoint,
                "model": "test-model",
                "stream": False,
                "allowed_tools": [
                    {
                        "name": "increment",
                        "component_id": "example.counter",
                        "service": "counter.increment",
                        "authority": "trigger",
                        "approval": approval,
                        "effect": "idempotent",
                    }
                ],
            },
        },
        "agent-plan",
    )
    assert MODEL_KEY not in json.dumps(plan)
    applied = await post(
        client,
        module_scope(root) + "/agent.runtime/actions/configuration.apply",
        {key: plan[key] for key in ("expected_revision", "plan_id", "plan_digest")},
        "agent-apply",
    )
    assert applied["revision"] == 1
    saved = await get(client, module_scope(root) + "/agent.runtime/queries/configuration")
    assert saved["has_api_key"] and MODEL_KEY not in json.dumps(saved)


async def test_web_approval_resumes_exact_tool_before_next_model_call(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with (
        onebot_server() as onebot,
        model_server() as model,
        running(settings) as (app, client),
    ):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Approval"}, "bot", status=202
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        snapshot = PackageCatalog(tmp_path / "packages").build(
            Path(__file__).resolve().parents[1] / "examples" / "counter"
        )
        await managed.kernel.install(snapshot, grants=frozenset({"state.read", "state.write"}))
        await configure_agent(client, root, model["endpoint"], approval="always")
        binding = await post(
            client,
            root + "/bindings",
            {"self_id": "10001", "display_name": "QQ"},
            "binding",
            status=201,
        )
        await configure(
            client,
            root + "/bindings/" + binding["binding_id"],
            onebot["origin"],
            response_mode="agent",
        )
        model["release_first"].set()
        assert (
            await receive(client, binding["binding_id"], 1, "first request", mention=True)
        ).status_code == 200
        waiting = await wait_for(
            lambda: get(client, module_scope(root) + "/agent.runtime/queries/runs"),
            lambda value: bool(value["approvals"]) and value["runs"][0]["phase"] == "WAIT_APPROVAL",
        )
        original = waiting["runs"][0]
        approval = waiting["approvals"][0]
        async with managed.store.pool.connection() as conn:
            tool = await (
                await conn.execute(
                    "SELECT operation_id,arguments_digest FROM public.bot_agent_tools"
                )
            ).fetchone()
        assert not onebot["sends"] and len(model["requests"]) == 1
        assert (
            await receive(client, binding["binding_id"], 2, "latest trigger", mention=True)
        ).status_code == 200
        decision = {
            "approval_id": approval["approval_id"],
            "decision": "APPROVE",
            "arguments_digest": approval["arguments_digest"],
        }
        accepted = await post(client, module_scope(root) + "/agent.runtime/actions/approval.decide", decision, "approve")
        assert accepted["run_id"] == original["run_id"]
        await wait_for(
            lambda: get(client, root + "/delivery"),
            lambda value: any(part["state"] == "SENT" for part in value["parts"]),
        )
        assert len(model["requests"]) == 2 and not model["errors"]
        assert "latest trigger" in json.dumps(model["requests"][1])
        saved = await get(client, module_scope(root) + "/agent.runtime/queries/runs")
        assert saved["runs"][0]["run_id"] == original["run_id"]
        assert saved["runs"][0]["session_id"] == original["session_id"]
        assert saved["runs"][0]["input_revision"] == 2
        assert saved["runs"][0]["phase"] == "COMPLETED"
        assert await post(client, module_scope(root) + "/agent.runtime/actions/approval.decide", decision, "approve") == accepted
        async with managed.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT operation_id,arguments_digest,status FROM public.bot_agent_tools"
                )
            ).fetchall()
            assert rows == [{**tool, "status": "SUCCEEDED"}]
            call = await (
                await conn.execute(
                    "SELECT operation_id,agent_tool_id,status FROM public.bot_calls WHERE component_id='example.counter'"
                )
            ).fetchall()
            assert call == [
                {
                    "operation_id": tool["operation_id"],
                    "agent_tool_id": tool["operation_id"],
                    "status": "COMPLETED",
                }
            ]
        assert (await managed.store.snapshot_state(managed.kernel._authority(), "example.counter"))[
            "counter"
        ]["value"] == 3


async def test_web_agent_multigroup_and_next_model_trigger_cut(tmp_path, web_bots_database):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with (
        onebot_server() as onebot,
        model_server() as model,
        running(settings) as (app, client),
    ):
        await setup(client, token)
        bot = await post(
            client,
            "/management/v1/bots",
            {"display_name": "Agent integration"},
            "agent-bot",
            status=202,
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        snapshot = PackageCatalog(tmp_path / "packages").build(
            Path(__file__).resolve().parents[1] / "examples" / "counter"
        )
        await managed.kernel.install(snapshot, grants=frozenset({"state.read", "state.write"}))
        await configure_agent(client, root, model["endpoint"])
        binding = await post(
            client,
            root + "/bindings",
            {"self_id": "10001", "display_name": "Agent QQ"},
            "binding",
            status=201,
        )
        binding_id = binding["binding_id"]
        await configure(
            client, root + "/bindings/" + binding_id, onebot["origin"], response_mode="agent"
        )
        assert (
            await receive(client, binding_id, 1, "first request", mention=True)
        ).status_code == 200
        try:
            await asyncio.wait_for(model["first_entered"].wait(), 20)
            assert (
                await receive(client, binding_id, 2, "ordinary between", user_id=20002)
            ).status_code == 200
            assert (
                await receive(client, binding_id, 3, "latest trigger", mention=True, user_id=20003)
            ).status_code == 200
            assert (
                await receive(client, binding_id, 4, "ordinary queued tail", user_id=20004)
            ).status_code == 200
            assert (
                await receive(client, binding_id, 1, "other request", group_id=30002, mention=True)
            ).status_code == 200
            await wait_for(
                lambda: get(client, root + "/delivery"),
                lambda value: any(part["state"] == "SENT" for part in value["parts"]),
            )
            assert len(onebot["sends"]) == 1 and onebot["sends"][0]["group_id"] == 30002
            model["release_first"].set()
            await wait_for(
                lambda: get(client, root + "/delivery"),
                lambda value: sum(part["state"] == "SENT" for part in value["parts"]) == 2,
            )
            assert not model["errors"] and len(model["requests"]) == 3
            final_request = next(
                body
                for body in model["requests"]
                if any(
                    message.get("role") == "assistant"
                    and message.get("tool_calls")
                    for message in body["messages"]
                )
            )
            text = json.dumps(final_request, ensure_ascii=False)
            assert "ordinary between" in text and "latest trigger" in text
            assert "ordinary queued tail" not in text and "other request" not in text
            assert "20002" in text and "20003" in text and "30001" in text
            roles = [message["role"] for message in final_request["messages"]]
            assistant = roles.index("assistant")
            assert roles[assistant + 1] == "tool"
            async with managed.store.pool.connection() as conn:
                rows = await (
                    await conn.execute(
                        "SELECT input_revision FROM public.bot_conversation_replies ORDER BY input_revision"
                    )
                ).fetchall()
                assert [row["input_revision"] for row in rows] == [1, 2]
                assert (
                    await (
                        await conn.execute(
                            "SELECT count(*) AS n FROM public.bot_agent_tools WHERE status='SUCCEEDED'"
                        )
                    ).fetchone()
                )["n"] == 1
            state = await managed.store.snapshot_state(
                managed.kernel._authority(), "example.counter"
            )
            assert state["counter"]["value"] == 3
            await wait_for(
                lambda: get(client, root + "/conversations"),
                lambda value: all(
                    item["context_applied_seq"] == item["next_ingress_seq"] - 1
                    for item in value["conversations"]
                ),
            )
            assert len(model["requests"]) == 3  # Final silent drain calls no model.
        finally:
            model["release_first"].set()


async def test_web_tool_revocation_blocks_already_running_native_worker_write(
    tmp_path, web_bots_database, monkeypatch
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with (
        onebot_server() as onebot,
        model_server() as model,
        running(settings) as (app, client),
    ):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Revoke tool"}, "bot", status=202
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        snapshot = PackageCatalog(tmp_path / "packages").build(
            Path(__file__).resolve().parents[1] / "examples" / "counter"
        )
        await managed.kernel.install(snapshot, grants=frozenset({"state.read", "state.write"}))
        await configure_agent(client, root, model["endpoint"])
        binding = await post(
            client,
            root + "/bindings",
            {"self_id": "10001", "display_name": "QQ"},
            "binding",
            status=201,
        )
        await configure(
            client,
            root + "/bindings/" + binding["binding_id"],
            onebot["origin"],
            response_mode="agent",
        )
        committing, release = asyncio.Event(), asyncio.Event()
        commit_state = managed.store.commit_state
        attempts, denied = [], []

        async def delayed_commit(grant, *args, **kwargs):
            assert grant.agent_tool_id is not None
            attempts.append(grant.agent_tool_id)
            committing.set()
            await release.wait()
            # The real transaction starts after the Web revocation commits.
            try:
                return await commit_state(grant, *args, **kwargs)
            except ForestError as exc:
                denied.append(exc.code)
                raise

        monkeypatch.setattr(managed.store, "commit_state", delayed_commit)
        model["release_first"].set()
        assert (
            await receive(client, binding["binding_id"], 1, "first request", mention=True)
        ).status_code == 200
        try:
            await asyncio.wait_for(committing.wait(), 20)
            settings_before = await get(client, module_scope(root) + "/agent.runtime/queries/configuration")
            values = {**settings_before["values"], "allowed_tools": []}
            plan = await post(
                client,
                module_scope(root) + "/agent.runtime/actions/configuration.plan",
                {"expected_revision": 1, "values": values},
                "revoke-plan",
            )
            await post(
                client,
                module_scope(root) + "/agent.runtime/actions/configuration.apply",
                {key: plan[key] for key in ("expected_revision", "plan_id", "plan_digest")},
                "revoke-apply",
            )
            release.set()
            result = await wait_for(
                lambda: get(client, module_scope(root) + "/agent.runtime/queries/runs"),
                lambda value: value["runs"][0]["phase"] == "BLOCKED_UNKNOWN",
            )
            assert result["unknown_tools"][0]["operation_id"] == attempts[0]
            assert len(attempts) == 1 and len(model["requests"]) == 1
            assert denied == ["AGENT_TOOL_REVOKED"]
            assert not onebot["sends"] and not model["errors"]
            assert "counter" not in await managed.store.snapshot_state(
                managed.kernel._authority(), "example.counter"
            )
            async with managed.store.pool.connection() as conn:
                tool = await (
                    await conn.execute("SELECT status,result FROM public.bot_agent_tools")
                ).fetchone()
                assert tool["status"] == "UNKNOWN"
        finally:
            release.set()

@pytest.mark.parametrize("change", ["disable", "rotate_key"])
async def test_web_revokes_unsent_final_without_retrying_model_or_dropping_saved_output(
    tmp_path, web_bots_database, change
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with (
        onebot_server() as onebot,
        model_server() as model,
        running(settings) as (app, client),
    ):
        await setup(client, token)
        bot = await post(
            client, "/management/v1/bots", {"display_name": "Unsent final"}, "bot", status=202
        )
        await ready(client, bot)
        root = "/management/v1/bots/" + bot["bot_id"]
        managed = await app.state.runtime.fleet.attach(bot["bot_id"])
        snapshot = PackageCatalog(tmp_path / "packages").build(
            Path(__file__).resolve().parents[1] / "examples" / "counter"
        )
        await managed.kernel.install(snapshot, grants=frozenset({"state.read", "state.write"}))
        await configure_agent(client, root, model["endpoint"])
        binding = await post(
            client,
            root + "/bindings",
            {"self_id": "10001", "display_name": "QQ"},
            "binding",
            status=201,
        )
        await configure(
            client,
            root + "/bindings/" + binding["binding_id"],
            onebot["origin"],
            response_mode="agent",
        )
        final_ready, release = asyncio.Event(), asyncio.Event()
        scheduler = managed.platform.module("conversation.scheduler")
        finalize = scheduler.finalize_agent
        commits = []

        async def delayed_final(actor, disposition, delivery):
            commits.append(disposition.run_id)
            final_ready.set()
            await release.wait()
            return await finalize(actor, disposition, delivery)

        scheduler.finalize_agent = delayed_final
        assert (
            await receive(client, binding["binding_id"], 1, "other request", mention=True)
        ).status_code == 200
        try:
            await asyncio.wait_for(final_ready.wait(), 20)
            original = (await get(client, module_scope(root) + "/agent.runtime/queries/runs"))["runs"][0]
            assert original["phase"] == "FINAL_READY"
            payloads = AgentPayloadStore(managed.store)
            async with managed.store.pool.connection() as conn, conn.transaction():
                original_step = await (
                    await conn.execute(
                        "SELECT status,output,output_payload_id,output_digest FROM public.bot_agent_steps WHERE logical_step_id=%s",
                        (original["final_step_id"],),
                    )
                ).fetchone()
                original_output = await payloads.read_output_in(conn, original["final_step_id"])
                assert original_step["status"] == "FINAL_READY" and original_step["output"] is None
                assert original_output["text"] == "other-final"
                assert original_step["output_digest"] == digest_json(original_output)
            saved = await get(client, module_scope(root) + "/agent.runtime/queries/configuration")
            body = {"expected_revision": 1, "values": saved["values"]}
            if change == "disable":
                body["values"] = {**saved["values"], "enabled": False}
            else:
                body["api_key"] = "replacement-test-model-key"
            plan = await post(client, module_scope(root) + "/agent.runtime/actions/configuration.plan", body, "revoke-final-plan")
            await post(
                client,
                module_scope(root) + "/agent.runtime/actions/configuration.apply",
                {key: plan[key] for key in ("expected_revision", "plan_id", "plan_digest")},
                "revoke-final-apply",
            )
            release.set()
            finished = await wait_for(
                lambda: get(client, module_scope(root) + "/agent.runtime/queries/runs"),
                lambda value: value["runs"][0]["phase"] == "FAILED",
            )
            final = finished["runs"][0]
            assert final["run_id"] == original["run_id"]
            assert final["final_step_id"] == original["final_step_id"]
            assert final["error_code"] == "AGENT_CONFIGURATION_REVOKED"
            assert final["final_reply_id"] is None
            assert commits == [original["run_id"]] and len(model["requests"]) == 1
            assert not onebot["sends"]
            async with managed.store.pool.connection() as conn, conn.transaction():
                step = await (
                    await conn.execute(
                        "SELECT status,output,output_payload_id,output_digest FROM public.bot_agent_steps WHERE logical_step_id=%s",
                        (final["final_step_id"],),
                    )
                ).fetchone()
                assert step == original_step and step["output"] is None
                output = await payloads.read_output_in(conn, final["final_step_id"])
                assert output == original_output and output["text"] == "other-final"
                assert digest_json(output) == step["output_digest"]
                run = await (
                    await conn.execute(
                        "SELECT status FROM public.bot_conversation_runs WHERE run_id=%s",
                        (final["run_id"],),
                    )
                ).fetchone()
                assert run["status"] == "FAILED"
        finally:
            release.set()

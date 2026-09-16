"""Actual Agent/Kernel/PG + HTTP retrieval; fake vectors make no semantic claim."""

import asyncio
import base64
import hashlib
import html
import json
from pathlib import Path

import pytest
from retention_upgrade import content
from test_agent_config import apply as apply_agent
from test_agent_config import plan as plan_agent
from test_agent_store import agent_turn
from test_embeddings import send, server
from test_media import media_server, picture, respond
from test_memory_index_runtime import EMBEDDING_KEY, configure, rows, source_ready, wait_state
from test_platform import platform as platform
from test_platform import receive
from test_web_authority import environment as environment

from forest.agent_settings import DEFAULT_AGENT_SETTINGS, AgentSettings
from forest.packages import PackageCatalog, digest_json

pytestmark = pytest.mark.postgres
MODEL_KEY = "agent-memory-local-model-key"


async def eventually(p, check, *, timeout=25):
    try:
        async with asyncio.timeout(timeout):
            while True:
                value = await check()
                if value:
                    return value
                p.runtime.module("memory.graph").notify()
                await asyncio.sleep(0.05)
    except TimeoutError:
        heads = await rows(p, "SELECT run_id,phase,error_code FROM public.bot_agent_runs")
        decisions = await rows(p, "SELECT * FROM public.bot_agent_memory_bindings")
        pytest.fail(f"Agent/memory did not converge: heads={heads!r}, decisions={decisions!r}, "
                    f"scheduler={p.runtime.module('conversation.scheduler').errors!r}, "
                    f"memory={p.runtime.module('memory.graph').errors!r}")


def headers(body, kind):
    result = []
    for message in body["messages"]:
        content = message.get("content")
        if isinstance(content, list) and content and content[0].get("type") == "text":
            item = json.loads(content[0]["text"])
            if item.get("context_data") == kind:
                result.append(item)
    return result


@pytest.mark.parametrize("vision", [False, True], ids=["text-only", "vision"])
async def test_agent_recalls_cross_group_pixels_and_reuses_fixed_query_across_tool_rounds(platform, tmp_path, vision):
    p = platform
    first_model, release_first = asyncio.Event(), asyncio.Event()
    server_errors = []
    recalled_pixels, query_pixels = picture(), picture(size=(4, 3))

    async def media(writer, header):
        await respond(writer, query_pixels if b"/query " in header else recalled_pixels)

    async def embeddings(writer, request):
        try:
            body = json.loads(request["body"])
            assert request["start"] == b"POST /v1/embeddings HTTP/1.1"
            assert request["headers"][b"authorization"] == b"Bearer " + EMBEDDING_KEY.encode()
            assert body["model"] == "jina-clip-v2" and body["dimensions"] == 512
            assert body["truncate"] is False and body.get("task") in {None, "retrieval.query"}
            await send(writer, {"model": "jina-clip-v2", "object": "list", "usage": {"total_tokens": 7},
                                "data": [{"object": "embedding", "index": index,
                                          "embedding": [1.0] + [0.0] * 511}
                                         for index in range(len(body["input"]))]})
        except Exception as exc:
            server_errors.append(repr(exc))
            raise

    async def model(writer, request):
        try:
            assert request["start"] == b"POST /v1/chat/completions HTTP/1.1"
            assert request["headers"][b"authorization"] == b"Bearer " + MODEL_KEY.encode()
            body = json.loads(request["body"])
            assert body["stream"] is False
            previous = any(message.get("tool_calls") for message in body["messages"])
            if not previous:
                first_model.set()
                await release_first.wait()
                catalogue_text = body["messages"][0]["content"].split(
                    '<available_tools encoding="json">', 1
                )[1].split("</available_tools>", 1)[0]
                tool = json.loads(html.unescape(catalogue_text))[0]["function"]
                trigger = tool["parameters"]["properties"]["authority_trigger_id"]["enum"][0]
                message = {
                    "role": "assistant",
                    "content": agent_turn(
                        "",
                        [
                            {
                                "name": "increment",
                                "arguments": {
                                    "authority_trigger_id": trigger,
                                    "arguments": {"increment": 3},
                                },
                            }
                        ],
                    ),
                }
                finish = "stop"
            else:
                message = {
                    "role": "assistant",
                    "content": agent_turn("Retrieved photographer from group 30002."),
                }
                finish = "stop"
            await send(writer, {"id": "model-second" if previous else "model-first", "model": "local-memory-model",
                                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}})
        except Exception as exc:
            server_errors.append(repr(exc))
            raise

    async with (
        server(embeddings) as (embedding_endpoint, embedding_requests, _),
        media_server(media) as (media_origin, downloads),
        server(model) as (model_endpoint, model_requests, _),
    ):
        try:
            counter = PackageCatalog(tmp_path / "counter-package").build(Path(__file__).resolve().parents[1] / "examples" / "counter")
            await p.kernel.install(counter, grants=frozenset({"state.read", "state.write"}))
            await apply_agent(p.env, await plan_agent(p.env, secret=MODEL_KEY, values=AgentSettings.model_validate({
                **DEFAULT_AGENT_SETTINGS, "enabled": True, "vision": vision, "stream": False,
                "endpoint": model_endpoint.removesuffix("/embeddings"), "model": "local-memory-model",
                "model_timeout": 20, "media_allowed_origins": [media_origin],
                "allowed_tools": [{"name": "increment", "component_id": "example.counter",
                                   "service": "counter.increment", "authority": "trigger",
                                   "approval": "never", "effect": "idempotent"}],
            }).model_dump()))
            await configure(p, embedding_endpoint, [media_origin], query_timeout=20, max_result_tokens=12000)
            await p.binding.apply(await p.binding.plan(expected=1, values={
                **p.desired["desired_values"], "response_mode": "agent",
            }))
            p.desired = (await p.runtime.bindings.desired(p.kernel._authority()))[0]
            await p.runtime._replace(p.desired)
            await p.runtime.start()

            # A different group contributes actual silent history and captures
            # original bytes through the normal runtime, before any Agent run.
            remembered = await receive(p, 101, group_id=30002, user_id=20002,
                sender={"user_id": 20002, "nickname": "Photographer", "role": "member"}, message=[
                    {"type": "text", "data": {"text": "Cross-group original bridge photo"}},
                    {"type": "image", "data": {"file": "bridge-photo", "url": media_origin + "/memory"}},
                ])
            await wait_state(p, lambda: source_ready(p, 2))
            assert not model_requests
            assert not await rows(p, "SELECT * FROM public.bot_conversation_runs")

            trigger = await receive(p, 102, group_id=30001, user_id=20001,
                sender={"user_id": 20001, "nickname": "Asker", "role": "member"}, message=[
                    {"type": "at", "data": {"qq": "10001"}},
                    {"type": "text", "data": {"text": "Find the photographer of this bridge"}},
                    {"type": "image", "data": {"file": "query-photo", "url": media_origin + "/query"}},
                ])
            await eventually(p, lambda: asyncio.sleep(0, result=first_model.is_set()))

            # This ordinary queued message arrives while HTTP is outstanding.
            # It is not an input to either query or the next tool/model round.
            tail = await receive(p, 103, group_id=30001, user_id=20003,
                sender={"user_id": 20003, "nickname": "Later speaker", "role": "member"}, message=[
                    {"type": "text", "data": {"text": "QUEUED_NON_TRIGGER_MUST_NOT_ENTER_QUERY"}},
                ])
            release_first.set()

            async def complete():
                heads = await rows(p, "SELECT * FROM public.bot_agent_runs")
                if heads and heads[0]["phase"] == "COMPLETED":
                    return heads
                return False

            heads = await eventually(p, complete)
            assert len(heads) == 1 and len(model_requests) == 2
            assert not server_errors
            requests = [json.loads(item["body"]) for item in model_requests]
            for body in requests:
                evidence = headers(body, "memory_evidence")
                assert evidence, "The actual model must receive independently projected memory"
                found = [item for item in evidence if item["source"]["channel_id"] == "30002"]
                assert len(found) == 1
                assert found[0]["source"]["source_kind"] == "GROUP"
                assert found[0]["source"]["realm_id"] == "qq-global" and found[0]["source"]["identity_schema"] == 1
                assert found[0]["author"]["external_user_id"] == "20002"
                assert found[0]["author"]["display_name"] == "Photographer"
                assert found[0]["relationship_to_current_conversation"] == "CROSS_GROUP"
                serialized = json.dumps(body)
                assert "Cross-group original bridge photo" in serialized
                assert "QUEUED_NON_TRIGGER" not in serialized
                if not vision:
                    assert "image_url" not in serialized and "data:image" not in serialized
                else:
                    evidence_message = next(message for message in body["messages"]
                        if isinstance(message.get("content"), list) and message["content"]
                        and json.loads(message["content"][0]["text"]).get("context_data") == "memory_evidence")
                    pixels = [base64.b64decode(part["image_url"]["url"].split(",", 1)[1])
                              for part in evidence_message["content"] if part["type"] == "image_url"]
                    assert pixels == [recalled_pixels]

            query_rows = await rows(p, "SELECT * FROM public.bot_memory_queries")
            assert len(query_rows) == 1
            query = query_rows[0]
            inbox = {item["event_id"]: item for item in await rows(p, "SELECT event_id,ingress_seq FROM public.bot_message_inbox")}
            assert query["input_cut"] == inbox[trigger["event_id"]]["ingress_seq"] < inbox[tail["event_id"]]["ingress_seq"]
            assert sorted(query["query_modalities"]) == ["image", "text"]
            occurrences = await rows(p, "SELECT event_id,source_unit_id FROM public.bot_memory_source_occurrences")
            source_ids = {item["event_id"]: item["source_unit_id"] for item in occurrences}
            assert query["source_unit_ids"] == [source_ids[trigger["event_id"]]]
            assert source_ids[tail["event_id"]] not in query["source_unit_ids"]
            assert source_ids[remembered["event_id"]] != query["source_unit_ids"][0]
            bindings = await rows(p, "SELECT * FROM public.bot_agent_memory_bindings ORDER BY logical_step_id")
            assert len(bindings) == 2 and all(item["status"] == "BOUND" for item in bindings)
            assert {item["query_id"] for item in bindings} == {query["query_id"]}
            assert len({item["bundle_id"] for item in bindings}) == 1
            assert len(await rows(p, "SELECT * FROM public.bot_memory_evidence_bundles")) == 1
            transports = await rows(p, "SELECT * FROM public.bot_memory_embedding_requests WHERE purpose='query'")
            assert len(transports) == 1
            attempts = await rows(p, "SELECT * FROM public.bot_memory_embedding_attempts WHERE request_id=%s", (query["query_id"],))
            assert len(attempts) == 1 and attempts[0]["state"] == "RESPONDED"
            query_http = [item for item in embedding_requests
                          if "sha256:" + hashlib.sha256(item["body"]).hexdigest() == transports[0]["wire_digest"]]
            assert len(query_http) == 1  # Background document batches have one facet, query has two.
            inputs = json.loads(query_http[0]["body"])["input"]
            assert [item["text"] for item in inputs if "text" in item] == ["Find the photographer of this bridge"]
            assert [base64.b64decode(item["image"], validate=True) for item in inputs if "image" in item] == [query_pixels]

            steps = await rows(p, "SELECT * FROM public.bot_agent_steps ORDER BY step_seq")
            model_attempts = await rows(p, "SELECT m.manifest,m.manifest_payload_id FROM public.bot_model_transport_attempts a "
                "JOIN public.bot_model_request_manifests m USING(bot_id,model_request_id) ORDER BY a.created_at")
            assert len(steps) == len(model_attempts) == 2
            async with p.env.store.pool.connection() as conn:
                for step, attempt in zip(steps, model_attempts, strict=True):
                    assert attempt["manifest"] is None and step["snapshot"] is None
                    manifest = await content(conn, p.env.store.bot_id, attempt["manifest_payload_id"])
                    snapshot = await content(conn, p.env.store.bot_id, step["snapshot_payload_id"])
                    assert manifest["input_digest"] == digest_json(snapshot)
                    assert manifest["memory_evidence"]["query_id"] == query["query_id"]
                    assert "QUEUED_NON_TRIGGER" not in json.dumps(snapshot)
            tool = (await rows(p, "SELECT * FROM public.bot_agent_tools"))[0]
            assert tool["status"] == "SUCCEEDED"
            call = await p.env.store.read_call(tool["operation_id"])
            assert call["status"] == "COMPLETED"

            async def sent_and_drained():
                conversations = await rows(p, "SELECT * FROM public.bot_conversations")
                return any(item[0] == "POST /send_group_msg HTTP/1.1" for item in p.requests) and all(
                    row["context_applied_seq"] == row["next_ingress_seq"] - 1 for row in conversations)

            await eventually(p, sent_and_drained)
            assert len([item for item in p.requests if item[0] == "POST /send_group_msg HTTP/1.1"]) == 1
            assert len(model_requests) == 2
            assert len(downloads) >= 2
        finally:
            release_first.set()
            await p.runtime.close()
        memory = p.runtime.module("memory.graph")
        assert not memory.executions and not memory.workers and not memory.clients

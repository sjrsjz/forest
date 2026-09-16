"""Real platform/Kernel/PG and TCP acceptance for silent multimodal memory."""

import asyncio
import base64
import json

import pytest
from test_agent_config import apply as apply_agent
from test_agent_config import plan as plan_agent
from test_embeddings import send, server
from test_media import media_server, picture, respond
from test_memory_config import apply, plan
from test_platform import platform as platform
from test_platform import receive
from test_web_authority import environment as environment

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.memory_runtime import MemoryRuntime
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS
from forest.platform import BindingRuntime

pytestmark = pytest.mark.postgres
EMBEDDING_KEY = "local-memory-test-key"


async def rows(p, query, parameters=()):
    async with p.env.store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


async def wait_state(p, predicate, *, timeout=20):
    """Public wakeups speed up the actual supervisor; no replacement worker."""
    try:
        async with asyncio.timeout(timeout):
            while True:
                result = await predicate()
                if result:
                    return result
                p.runtime.memory.notify()
                await asyncio.sleep(0.1)
    except TimeoutError:
        facets = await rows(p, "SELECT state,error_code FROM public.bot_memory_index_facets")
        captures = await rows(p, "SELECT status,error_code FROM public.bot_memory_media_heads")
        pytest.fail(
            f"Memory runtime did not converge: {p.runtime.memory.errors!r}; facets={facets!r}; captures={captures!r}"
        )


async def configure(p, endpoint, origins=(), *, revision=0, enabled=True, **changes):
    values = {
        **DEFAULT_MEMORY_SETTINGS,
        "enabled": enabled,
        "endpoint": endpoint,
        "deployment_id": "local-memory-runtime",
        "batch_size": 1,
        "index_concurrency": 2,
        "capture_concurrency": 2,
        "media_allowed_origins": list(origins),
        "media_max_bytes": 512,
        "request_timeout": 15,
        **changes,
    }
    result = await apply(
        p.env,
        await plan(
            p.env, revision=revision, values=values, secret=EMBEDDING_KEY if revision == 0 else None
        ),
    )
    p.runtime.memory.notify()
    return result


async def embedding_response(writer, request):
    body = json.loads(request["body"])
    assert request["start"] == b"POST /v1/embeddings HTTP/1.1"
    assert request["headers"][b"authorization"] == b"Bearer " + EMBEDDING_KEY.encode()
    assert body["model"] == "jina-clip-v2" and body["dimensions"] == 512
    assert body["truncate"] is False and "task" not in body
    await send(
        writer,
        {
            "model": "jina-clip-v2",
            "object": "list",
            "usage": {"total_tokens": 7},
            "data": [
                {"object": "embedding", "index": index, "embedding": [1.0] + [0.0] * 511}
                for index in range(len(body["input"]))
            ],
        },
    )


async def source_ready(p, count):
    state = await rows(
        p, "SELECT state,count(*) AS n FROM public.bot_memory_index_facets GROUP BY state"
    )
    if state != [{"state": "READY", "n": count}]:
        return False
    return len(await rows(p, "SELECT * FROM public.bot_memory_vectors")) == count


async def graph_ready(p, roots):
    entries = await rows(
        p,
        "SELECT DISTINCT n.node_id FROM public.bot_memory_graph_nodes n "
        "JOIN public.bot_memory_graph_scopes s USING(bot_id,scope_id,revision) "
        "WHERE n.metadata->'ref'->>'kind'='TRUNK_ROOT'",
    )
    if len(entries) != roots:
        return False
    graph, lease = p.runtime.memory.graph, p.kernel._authority()
    page = await graph.discover(lease, limit=roots)
    if len(page["scopes"]) != roots or page["next_after"] is not None:
        return False
    captures = await rows(
        p,
        "SELECT source_unit_id,part_id,blob_digest FROM public.bot_memory_media_heads "
        "WHERE status='CAPTURED' ORDER BY source_unit_id,part_id",
    )
    if len(captures) != roots:
        return False
    expected_images = {
        (row["source_unit_id"], row["part_id"]): row["blob_digest"] for row in captures
    }
    published_images = {}
    for scope in page["scopes"]:
        current = (await graph.capture(lease, scope)).metadata
        publications = await rows(
            p,
            "SELECT p.snapshot FROM public.bot_memory_graph_publications p "
            "JOIN public.bot_memory_graph_scopes s USING(bot_id,scope_id,revision) "
            "WHERE p.scope_id=%s",
            (scope.scope_id,),
        )
        if len(publications) != 1:
            return False
        frozen = publications[0]["snapshot"]
        # A text-bearing source can create its root before image capture. Wait
        # for the exact current source/part availability and halo view, not
        # merely for roots or vectors (which have independent supervisors).
        if any(
            frozen[key] != current[key]
            for key in ("sources", "halos", "operation_revision", "operations")
        ):
            return False
        for source in frozen["sources"]:
            for part in source["parts"]:
                if part["kind"] == "image" and part["part_id"] in source["available_parts"]:
                    published_images[(source["source_unit_id"], part["part_id"])] = part[
                        "content_digest"
                    ]
    return page["scopes"] if published_images == expected_images else False


async def test_qiyu_siliconflow_profile_publishes_normalized_vector(platform):
    p = platform

    async def qwen_embeddings(writer, request):
        body = json.loads(request["body"])
        assert body == {
            "model": "Qwen/Qwen3-VL-Embedding-8B",
            "dimensions": 4096,
            "encoding_format": "float",
            "truncate": "right",
            "input": [{"text": "qiyu-vl-profile"}],
        }
        await send(
            writer,
            {
                "model": "Qwen/Qwen3-VL-Embedding-8B",
                "object": "list",
                "usage": {"prompt_tokens": 3, "completion_tokens": 0, "total_tokens": 3},
                "data": [
                    {
                        "object": "embedding",
                        "index": 0,
                        "embedding": [3.0, 4.0] + [0.0] * 4094,
                    }
                ],
            },
        )

    async with server(qwen_embeddings) as (endpoint, requests, _):
        try:
            await configure(
                p,
                endpoint,
                model="Qwen/Qwen3-VL-Embedding-8B",
                dimensions=4096,
                deployment_id="qiyu-siliconflow-qwen3-vl",
                model_revision="qiyu-production-2026-09",
            )
            await p.runtime._replace(p.desired)
            await p.runtime.start()
            await receive(p, 9, message=[{"type": "text", "data": {"text": "qiyu-vl-profile"}}])
            await wait_state(p, lambda: source_ready(p, 1))
            stored = await rows(
                p,
                "SELECT profile_metadata FROM public.bot_memory_embedding_requests",
            )
            vector = await rows(p, "SELECT vector FROM public.bot_memory_vectors")
            assert len(requests) == 1
            assert stored == [
                {
                    "profile_metadata": {
                        "dimensions": 4096,
                        "protocol_revision": "siliconflow-qwen3-vl-embedding-http-2026-09-v1",
                    },
                }
            ]
            assert vector[0]["vector"][:2] == pytest.approx([0.6, 0.8])
            assert len(vector[0]["vector"]) == 4096
        finally:
            await p.runtime.close()


def closed_memory(runtime):
    assert runtime.closed and runtime.task.done()
    assert not runtime.workers and not runtime.executions and not runtime.clients


@pytest.mark.parametrize("agent_enabled", [False, True], ids=["agent-disabled", "text-only-agent"])
async def test_silent_platform_indexes_original_images_and_text_without_llm_or_send(
    platform, agent_enabled
):
    p = platform
    original_runtime = p.runtime
    image = picture()
    active, peak = 0, 0
    concurrent = asyncio.Event()

    async def embeddings(writer, request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active >= 2:
            concurrent.set()
        try:
            await asyncio.wait_for(concurrent.wait(), 10)
            await embedding_response(writer, request)
        finally:
            active -= 1

    async def media(writer, header):
        # Keep both original downloads pending until the two groups' text
        # facets overlap on real HTTP. Images cannot accidentally satisfy the
        # concurrency assertion by indexing two parts of only one group.
        await asyncio.wait_for(concurrent.wait(), 10)
        await respond(writer, image)

    async def unexpected_llm(writer, request):
        await send(writer, {"error": "No model call is expected for silent ingestion"}, status=500)

    restarted = None
    async with (
        server(embeddings) as (endpoint, requests, _),
        media_server(media) as (origin, downloads),
        server(unexpected_llm) as (llm_endpoint, llm_requests, _),
    ):
        try:
            await apply_agent(
                p.env,
                await plan_agent(
                    p.env,
                    values={
                        **DEFAULT_AGENT_SETTINGS,
                        "enabled": agent_enabled,
                        "vision": False,
                        "endpoint": llm_endpoint.removesuffix("/embeddings"),
                        "model": "unused-local-model",
                    },
                    secret="unused-llm-key",
                ),
            )
            await configure(p, endpoint, [origin])
            await p.runtime._replace(p.desired)
            await p.runtime.start()
            assert type(p.runtime.memory) is MemoryRuntime
            assert p.runtime.memory.task in p.runtime._tasks
            await asyncio.gather(
                *(
                    receive(
                        p,
                        index,
                        group_id=30000 + index,
                        user_id=20000 + index,
                        sender={
                            "user_id": 20000 + index,
                            "nickname": "same-name",
                            "role": "member",
                        },
                        message=[
                            {"type": "text", "data": {"text": "silent-source-" + str(index)}},
                            {
                                "type": "image",
                                "data": {
                                    "file": "image-" + str(index),
                                    "url": origin + "/original-" + str(index),
                                },
                            },
                        ],
                    )
                    for index in (1, 2)
                )
            )
            await wait_state(p, lambda: source_ready(p, 4))
            settled_scopes = await wait_state(p, lambda: graph_ready(p, 2))
            assert peak == 2 and len(requests) == 4 and len(downloads) == 2
            assert sorted(
                json.loads(request["body"])["input"][0]["text"] for request in requests[:2]
            ) == ["silent-source-1", "silent-source-2"]
            inputs = [part for request in requests for part in json.loads(request["body"])["input"]]
            assert sorted(value["text"] for value in inputs if "text" in value) == [
                "silent-source-1",
                "silent-source-2",
            ]
            assert [
                base64.b64decode(value["image"], validate=True)
                for value in inputs
                if "image" in value
            ] == [image, image]
            qualified = await rows(
                p,
                "SELECT i.origin,i.trigger_decision,i.author,u.source_id,u.principal_ref,e.input_batch_id,b.kind "
                "FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) "
                "JOIN public.bot_message_inbox i USING(bot_id,event_id) "
                "JOIN public.bot_memory_source_eligibility e USING(bot_id,occurrence_id) "
                "JOIN public.bot_context_input_batches b USING(bot_id,input_batch_id)",
            )
            assert len(qualified) == 2 and len({r["source_id"] for r in qualified}) == 2
            assert len({r["principal_ref"] for r in qualified}) == 2
            assert all(
                r["kind"] == "SILENT_DRAIN" and r["trigger_decision"] == "NO_TRIGGER"
                for r in qualified
            )
            for table in (
                "bot_conversation_runs",
                "bot_conversation_replies",
                "bot_agent_runs",
                "bot_model_transport_attempts",
                "bot_send_intents",
            ):
                assert not await rows(p, "SELECT * FROM public." + table)
            assert not llm_requests
            assert all(request[0] == "POST /get_login_info HTTP/1.1" for request in p.requests)
            captured = await rows(p, "SELECT status,payload_id FROM public.bot_memory_media_heads")
            assert len(captured) == 2 and all(r["status"] == "CAPTURED" for r in captured)

            # Establish that the real graph builder has no remaining publication
            # before testing restart deduplication. Otherwise restart may validly
            # publish the just-captured images for the first time.
            for scope in settled_scopes:
                receipt = await p.runtime.memory.graph.refresh(p.kernel._authority(), scope)
                assert receipt.get("unchanged"), receipt

            # Stop the complete platform, then construct a new actual platform
            # against the same Kernel/store and the same durable memory slots.
            await p.runtime.close()
            closed_memory(p.runtime.memory)
            assert not p.runtime._clients and not p.runtime.connections
            before = await rows(
                p, "SELECT scope_id,revision FROM public.bot_memory_graph_scopes ORDER BY scope_id"
            )
            slots = await rows(
                p,
                "SELECT facet_id,state,vector_payload_id FROM public.bot_memory_index_facets ORDER BY facet_id",
            )
            restarted = BindingRuntime(original_runtime.bot, p.env.vault, original_runtime.packages)
            p.runtime = restarted
            await restarted._replace((await restarted.bindings.desired(p.kernel._authority()))[0])
            await restarted.start()
            for _ in range(8):
                restarted.memory.notify()
                await asyncio.sleep(0.15)
            assert (
                await rows(
                    p,
                    "SELECT scope_id,revision FROM public.bot_memory_graph_scopes ORDER BY scope_id",
                )
                == before
            )
            assert (
                await rows(
                    p,
                    "SELECT facet_id,state,vector_payload_id FROM public.bot_memory_index_facets ORDER BY facet_id",
                )
                == slots
            )
            assert len(requests) == 4 and len(downloads) == 2 and not llm_requests

            # Persisted disable fences newly arriving HTTP work, while Actor
            # ordering and silent context still proceed normally.
            await configure(p, endpoint, [origin], revision=1, enabled=False)
            await receive(
                p,
                3,
                group_id=30003,
                message=[
                    {"type": "text", "data": {"text": "disabled-new-source"}},
                    {
                        "type": "image",
                        "data": {"file": "new-image", "url": origin + "/disabled-image"},
                    },
                ],
            )

            async def drained_disabled():
                records = await rows(
                    p,
                    "SELECT context_applied_seq FROM public.bot_conversations WHERE channel_id='30003'",
                )
                return records and records[0]["context_applied_seq"] == 1

            await wait_state(p, drained_disabled)
            for _ in range(4):
                restarted.memory.notify()
                await asyncio.sleep(0.1)
            assert len(requests) == 4 and len(downloads) == 2 and not llm_requests
            await restarted.close()
            closed_memory(restarted.memory)
        finally:
            if restarted is not None:
                await restarted.close()
            await original_runtime.close()


async def test_platform_close_recovers_all_owned_inflight_embedding_resources(platform):
    p = platform
    started, release = asyncio.Event(), asyncio.Event()

    async def blocked(writer, request):
        started.set()
        await release.wait()
        await embedding_response(writer, request)

    async with server(blocked) as (endpoint, requests, _):
        try:
            await configure(p, endpoint)
            await p.runtime._replace(p.desired)
            await p.runtime.start()
            await receive(p, 1)
            await asyncio.wait_for(started.wait(), 12)
            runtime = p.runtime.memory
            clients, executions, workers = (
                set(runtime.clients),
                set(runtime.executions),
                set(runtime.workers.values()),
            )
            assert len(requests) == 1 and clients and executions and workers
            await asyncio.wait_for(asyncio.gather(p.runtime.close(), p.runtime.close()), 6)
            closed_memory(runtime)
            assert all(task.done() for task in executions | workers)
            assert all(client._closed for client in clients)
            attempts = await rows(p, "SELECT state FROM public.bot_memory_embedding_attempts")
            assert attempts == [{"state": "UNKNOWN"}]
            assert not await rows(p, "SELECT * FROM public.bot_memory_vectors")
        finally:
            release.set()
            await p.runtime.close()

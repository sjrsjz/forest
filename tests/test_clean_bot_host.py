from __future__ import annotations

import asyncio
import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient
from psycopg import sql

from forest.agent_protocol import CORE_PROMPT_SHA256
from forest.clean_bot_host import CleanBotHost, CleanBotSettings
from forest.clean_management_app import create_clean_management_app
from forest.embeddings import EmbeddingInput, EmbeddingResult, EmbeddingVector
from forest.errors import ForestError
from forest.media_capture_io import CapturedImage
from forest.runtime_modules import ModuleLifecycleState
from forest.storage.kernel_audit import AuditEnvelope
from forest.storage.memory_graph_authority import (
    MemoryOccurrence,
    MemoryPart,
    MemoryPayloadRef,
    MemorySourceIdentity,
)
from forest.storage.payload_authority import PayloadSource


class FakeModelClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class FakeEmbeddingClient(FakeModelClient):
    endpoints = []

    def __init__(self, endpoint, _credential, profile, **_kwargs) -> None:
        super().__init__()
        self.profile = profile
        self.endpoints.append(endpoint)

    async def dispatch(self, prepared, *, permitcheck):
        await permitcheck(prepared)
        facets = prepared.manifest["facets"]
        return EmbeddingResult(
            "RESPONDED",
            prepared.request_id,
            prepared.transport_attempt_id,
            prepared.payload_digest,
            prepared.space_id,
            vectors=tuple(
                EmbeddingVector(
                    facet["facet_id"],
                    index,
                    (1.0,) + (0.0,) * (self.profile.dimensions - 1),
                )
                for index, facet in enumerate(facets)
            ),
            usage_json='{"total_tokens":1}',
            http_status=200,
        )


class FakeMediaClient(FakeModelClient):
    fetched = []

    def __init__(self, **_kwargs) -> None:
        super().__init__()

    async def fetch(self, locator):
        self.fetched.append(locator)
        if (
            "retry" in (locator.get("uri") or "")
            and sum("retry" in (item.get("uri") or "") for item in self.fetched) == 1
        ):
            raise ForestError("MEDIA_TRANSPORT_FAILED", "retry fixture", 502)
        return CapturedImage(b"fake-png-bytes", "image/png", 2, 3)


class FakeWebSocketClient:
    def __init__(self, self_id: str) -> None:
        self.self_id = self_id
        self.handler = None
        self.closed = False
        self.failure_code = None

    async def get_login_info(self, *, timeout=None):
        return {"self_id": self.self_id, "nickname": "Forest"}

    def activate_events(self, handler) -> None:
        self.handler = handler

    async def aclose(self) -> None:
        self.closed = True

    def owned_tasks(self):
        return ()


@pytest.mark.asyncio
async def test_approval_decision_wakes_only_its_exact_conversation_blocker():
    class FakeAgent:
        async def approval_context(self, approval_ref):
            assert approval_ref == "approval/tool/one"
            return {"conversation_ref": "group/one"}

    class FakeConversation:
        def __init__(self):
            self.calls = []

        async def resume_deferred(self, conversation_id, **values):
            self.calls.append((conversation_id, values))
            return {"resumed": True}

    conversation = FakeConversation()

    class FakeRegistry:
        def get(self, component_id):
            return {
                "agent.runtime": FakeAgent(),
                "conversation.scheduler": conversation,
            }[component_id]

    host = object.__new__(CleanBotHost)
    host.registry = FakeRegistry()
    await host._approval_decided(
        {
            "approval_id": "approval/tool/one",
            "request_kind": "agent.tool",
        }
    )

    assert conversation.calls == [
        (
            "group/one",
            {
                "reason_code": "AGENT_WAITING_APPROVAL",
                "reason_ref": "approval/tool/one",
            },
        )
    ]


@pytest.mark.asyncio
async def test_runtime_replacement_prewarms_then_drains_before_publication():
    events = []

    class Previous:
        async def drain_owned_work_for_replacement(self, operation_id):
            assert operation_id == "replacement/one"
            events.append("drain-previous")

        async def close(self):
            events.append("close-previous")
            return []

    class Candidate:
        activation_id = "replacement/one"

        async def prewarm(self):
            events.append("prewarm-candidate")

        async def start(self):
            events.append("publish-candidate")

        async def close(self):
            events.append("close-candidate")
            return []

        def get(self, component_id):
            assert component_id == "agent.runtime"
            return self

        async def resume_reload_wait(self, operation_id):
            assert operation_id == self.activation_id
            events.append("resume-runs")
            return ()

        def notify_work(self):
            events.append("notify-work")

    previous = Previous()
    candidate = Candidate()
    host = object.__new__(CleanBotHost)
    host._replacement_lock = asyncio.Lock()
    host._closing = False
    host.registry = previous

    async def assemble():
        events.append("assemble-candidate")
        return candidate, object()

    host._assemble_registry = assemble
    result = await host.replace_runtime_graph()

    assert result is candidate
    assert host.registry is candidate
    assert events == [
        "assemble-candidate",
        "prewarm-candidate",
        "drain-previous",
        "publish-candidate",
        "resume-runs",
        "close-previous",
    ]


@pytest.mark.asyncio
async def test_durable_reload_worker_executes_exact_revision_and_settles_request():
    events = []

    class Runtime:
        async def status(self):
            return {"graph_revision": 7}

        async def finish_graph_replacement(self, lease, **values):
            events.append(("finish", lease, values))

        async def graph_for_activation(self, _activation_id):
            return None

    host = object.__new__(CleanBotHost)
    host._kernel = SimpleNamespace(runtime=Runtime())
    host._lease = "lease/one"
    host._closing = False

    async def replace_runtime_graph(**values):
        events.append(("replace", values))
        return SimpleNamespace(
            graph_identity=SimpleNamespace(
                graph_id="graph/eight",
                graph_revision=8,
            )
        )

    host.replace_runtime_graph = replace_runtime_graph
    await host._execute_graph_replacement(
        {
            "operation_id": "reload/one",
            "activation_id": "00000000-0000-0000-0000-000000000008",
            "expected_graph_revision": 7,
        }
    )

    assert events == [
        (
            "replace",
            {
                "activation_id": "00000000-0000-0000-0000-000000000008",
                "expected_graph_revision": 7,
            },
        ),
        (
            "finish",
            "lease/one",
            {
                "operation_id": "reload/one",
                "graph_id": "graph/eight",
                "graph_revision": 8,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_runtime_replacement_resumes_previous_graph_when_publication_fails():
    events = []

    class Previous:
        async def drain_owned_work_for_replacement(self, operation_id):
            assert operation_id == "replacement/failing"
            events.append("drain-previous")

        async def resume_owned_work_after_failed_replacement(self):
            events.append("resume-previous")

    class Candidate:
        activation_id = "replacement/failing"

        async def prewarm(self):
            events.append("prewarm-candidate")

        async def start(self):
            events.append("publish-candidate")
            raise RuntimeError("publication failed")

        async def close(self):
            events.append("close-candidate")
            return []

    previous = Previous()
    candidate = Candidate()
    host = object.__new__(CleanBotHost)
    host._replacement_lock = asyncio.Lock()
    host._closing = False
    host.registry = previous

    async def assemble():
        return candidate, object()

    host._assemble_registry = assemble
    with pytest.raises(RuntimeError, match="publication failed"):
        await host.replace_runtime_graph()

    assert host.registry is previous
    assert events == [
        "prewarm-candidate",
        "drain-previous",
        "publish-candidate",
        "close-candidate",
        "resume-previous",
    ]


@pytest.mark.asyncio
async def test_reload_recovery_releases_agent_and_exact_conversation_blocker():
    calls = []

    class Agent:
        async def resume_reload_wait(self, operation_id):
            calls.append(("agent", operation_id))
            return (
                {
                    "run_id": "run/one",
                    "conversation_ref": "group/one",
                    "reload_operation_id": operation_id,
                },
            )

    class Conversation:
        async def resume_deferred(self, conversation_id, **values):
            calls.append(("conversation", conversation_id, values))
            return {"resumed": True}

    class Registry:
        def get(self, component_id):
            return {
                "agent.runtime": Agent(),
                "conversation.scheduler": Conversation(),
            }[component_id]

        def notify_work(self):
            calls.append(("notify",))

    resumed = await CleanBotHost._resume_reload_waits(
        Registry(),
        "replacement/one",
    )

    assert resumed[0]["run_id"] == "run/one"
    assert calls == [
        ("agent", "replacement/one"),
        (
            "conversation",
            "group/one",
            {
                "reason_code": "AGENT_RELOAD_WAIT",
                "reason_ref": "replacement/one",
            },
        ),
        ("notify",),
    ]


@pytest.mark.postgres
async def test_clean_bot_host_starts_retires_and_restarts_exact_graph():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_host_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    settings = CleanBotSettings(
        bot_id="bot-production-test",
        dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1",
        model_api_key="test-model-key",
        model="test-model",
        model_deployment_id="provider.chat-completions/test",
        model_context_limit=32768,
        model_max_output_tokens=1024,
        model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B",
        embedding_dimensions=4096,
        onebot_self_id="123456789",
        onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609",),
        group_trigger_mode="MENTION",
        private_enabled=True,
        shell_administrator_ids=("42",),
        agent_system_prompt="You are the production-host integration test Bot.",
    )
    clients = []

    async def create_websocket(_url, _token, **_limits):
        client = FakeWebSocketClient(settings.onebot_self_id)
        client.url = _url
        clients.append(client)
        return client

    try:
        FakeEmbeddingClient.endpoints.clear()
        FakeMediaClient.fetched.clear()
        graph_ids = []
        for index in range(2):
            host = CleanBotHost(
                settings,
                model_client_factory=FakeModelClient,
                embedding_client_factory=FakeEmbeddingClient,
                media_client_factory=FakeMediaClient,
                websocket_client_factory=create_websocket,
            )
            await host.start()
            try:
                assert set(host.registry.states.values()) == {ModuleLifecycleState.ACTIVE}
                assert set(host.registry.graph_identity.generations) == {
                    "kernel.runtime",
                    "kernel.payload",
                    "kernel.authorization",
                    "kernel.effect",
                    "kernel.audit",
                    "provider.chat-completions",
                    "embedding.multimodal",
                    "memory.graph",
                    "media.capture",
                    "agent.runtime",
                    "conversation.trigger-policy",
                    "job.scheduler",
                    "shell.command",
                    "conversation.scheduler",
                    "adapter.onebot",
                    "delivery",
                }
                audit_descriptor = next(
                    item
                    for item in host.registry.descriptors
                    if item.component_id == "kernel.audit"
                )
                assert [item.command_id for item in audit_descriptor.shell_commands] == [
                    "audit.tail"
                ]
                audit_route = host.registry.get("shell.command").command_routes[
                    ("audit.tail", 1)
                ]
                assert audit_route[0] == "kernel.audit"
                assert audit_route[2].route.provider_component_id == "kernel.audit"
                graph_ids.append(host.registry.graph_identity.graph_id)
                assert clients[-1].handler is not None
                if index == 0:
                    jobs = host.registry.get("job.scheduler")
                    queued = await jobs.enqueue(
                        job_type="runtime.health-snapshot",
                        operation_id="host-test:runtime-health",
                        requested_by="test:operator",
                        input_value={},
                    )
                    jobs.notify()
                    async with asyncio.timeout(5):
                        while True:
                            job = await jobs.inspect(queued["job_id"])
                            if job["state"] == "SUCCEEDED":
                                break
                            await asyncio.sleep(0.05)
                    assert job["result_payload_id"]
                    await clients[-1].handler(
                        json.dumps(
                            {
                                "self_id": int(settings.onebot_self_id),
                                "post_type": "message",
                                "message_type": "group",
                                "sub_type": "normal",
                                "time": 1_720_000_000,
                                "message_id": 4242,
                                "user_id": 20001,
                                "group_id": 933179609,
                                "sender": {
                                    "user_id": 20001,
                                    "nickname": "Image sender",
                                    "card": "",
                                    "role": "member",
                                },
                                "message": [
                                    {"type": "text", "data": {"text": "before"}},
                                    {
                                        "type": "image",
                                        "data": {
                                            "file": "onebot-file-id",
                                            "url": "https://media.example/image?private-token=1",
                                        },
                                    },
                                    {"type": "text", "data": {"text": "after"}},
                                ],
                            },
                            separators=(",", ":"),
                        ).encode()
                    )
                    media_runtime = host.registry.get("media.capture")
                    async with asyncio.timeout(5):
                        while True:
                            async with host.store.pool.connection() as conn:
                                capture = await (
                                    await conn.execute(
                                        sql.SQL(
                                            "SELECT * FROM {}.media_captures WHERE bot_id=%s"
                                        ).format(
                                            sql.Identifier(media_runtime.repository.namespace)
                                        ),
                                        (settings.bot_id,),
                                    )
                                ).fetchone()
                            if capture is not None and capture["state"] == "READY":
                                break
                            await asyncio.sleep(0.05)
                    assert FakeMediaClient.fetched == [
                        {
                            "kind": "onebot-source",
                            "uri": "https://media.example/image?private-token=1",
                            "platform_file": "onebot-file-id",
                        }
                    ]
                    event_hash = hashlib.sha256(b"binding/main\x004242").hexdigest()
                    media_ref = (
                        "media:"
                        + hashlib.sha256(
                            ("onebot-event:" + event_hash + "\x001").encode()
                        ).hexdigest()
                    )
                    assets = await media_runtime.repository.asset_rows((media_ref,))
                    assert len(assets) == 1
                    resolved_media = await media_runtime.resolve_context(
                        {
                            "units": [
                                {
                                    "unit_id": "input:image-test",
                                    "parts": [
                                        {
                                            "part_id": "input:image-test:0",
                                            "type": "image",
                                            "media_ref": media_ref,
                                            "media_revision": 1,
                                        }
                                    ],
                                }
                            ]
                        }
                    )
                    image = resolved_media[("input:image-test", "input:image-test:0")]
                    assert image.content == b"fake-png-bytes"
                    assert (image.width, image.height) == (2, 3)
                    async with host.store.pool.connection() as conn:
                        inbox = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT content_payload_id FROM {}.inbox_messages "
                                    "WHERE bot_id=%s AND platform_message_ref='4242'"
                                ).format(
                                    sql.Identifier(
                                        host.registry.get(
                                            "conversation.scheduler"
                                        ).repository.namespace
                                    )
                                ),
                                (settings.bot_id,),
                            )
                        ).fetchone()
                    normalized_body = await host._kernel.payload.read_bytes(
                        inbox["content_payload_id"]
                    )
                    assert b"private-token" not in normalized_body
                    assert media_ref.encode() in normalized_body
                    memory_runtime = host.registry.get("memory.graph")
                    async with host.store.pool.connection() as conn:
                        stored_parts = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT part_kind,part_index FROM {}.source_parts "
                                    "WHERE bot_id=%s AND source_unit_id=%s ORDER BY part_index"
                                ).format(sql.Identifier(memory_runtime.repository.namespace)),
                                (settings.bot_id, capture["source_unit_id"]),
                            )
                        ).fetchall()
                    assert [(part["part_kind"], part["part_index"]) for part in stored_parts] == [
                        ("TEXT", 0),
                        ("IMAGE", 1),
                        ("TEXT", 2),
                    ]
                    permitted = False

                    async def permit_in(_conn, *, request):
                        nonlocal permitted
                        permitted = request.component_id == "embedding.multimodal"

                    result = await memory_runtime.embedding_provider.encode(
                        (
                            EmbeddingInput(
                                "test-facet",
                                text="Forest multimodal route",
                                source_json=(
                                    '{"source_revision":1,"part_id":"part/text",'
                                    '"trunk_id":"trunk/test","author_ref":"qq:test"}'
                                ),
                            ),
                        ),
                        request_id="test-request",
                        transport_attempt_id="test-attempt",
                        purpose="document",
                        permit_in=permit_in,
                    )
                    assert permitted is True and result.status == "RESPONDED"
                    embedding_runtime = host.registry.get("embedding.multimodal")
                    async with host.store.pool.connection() as conn:
                        attempt = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT state,result_payload_id,usage_json FROM {}."
                                    "embedding_transport_attempts WHERE bot_id=%s "
                                    "AND attempt_id=%s"
                                ).format(sql.Identifier(embedding_runtime.repository.namespace)),
                                (settings.bot_id, "test-attempt"),
                            )
                        ).fetchone()
                    assert attempt["state"] == "RESPONDED"
                    assert attempt["result_payload_id"] is not None
                    assert attempt["usage_json"] == '{"total_tokens":1}'
                    payload_source = PayloadSource(
                        "adapter.onebot", "test.event", "test-memory-event", 1
                    )
                    epoch = await host._kernel.payload.epoch()
                    author = await host._kernel.payload.put_bytes(
                        owner_component_id="adapter.onebot",
                        owner_kind="test.author",
                        owner_id="test-memory-author",
                        media_type="application/json",
                        content=b'{"subject":"qq:test"}',
                        sources=(payload_source,),
                        expected_deletion_epoch=epoch,
                    )
                    text = await host._kernel.payload.put_bytes(
                        owner_component_id="adapter.onebot",
                        owner_kind="test.text",
                        owner_id="test-memory-text",
                        media_type="text/plain",
                        content=b"durable multimodal memory",
                        sources=(payload_source,),
                        expected_deletion_epoch=epoch,
                    )
                    await memory_runtime.repository.register_source(
                        operation_id="test-memory-source-register",
                        identity=MemorySourceIdentity(
                            "test-memory-source",
                            "GROUP",
                            platform="onebot11",
                            realm_id="qq:test-bot",
                            external_group_id="933179609",
                            trunk_id="test-memory-trunk",
                        ),
                    )
                    now = datetime.now(timezone.utc)
                    await memory_runtime.repository.capture_source_unit(
                        operation_id="test-memory-capture",
                        source_unit_id="test-memory-unit",
                        source_id="test-memory-source",
                        source_revision=1,
                        unit_kind="GROUP_MESSAGE",
                        author_subject_ref="qq:test",
                        author=MemoryPayloadRef(
                            author.payload_id,
                            author.content_digest,
                            "adapter.onebot",
                            "test.author",
                            "test-memory-author",
                        ),
                        parts=(
                            MemoryPart(
                                "part/text",
                                0,
                                "TEXT",
                                "text/plain",
                                MemoryPayloadRef(
                                    text.payload_id,
                                    text.content_digest,
                                    "adapter.onebot",
                                    "test.text",
                                    "test-memory-text",
                                ),
                                byte_length=25,
                            ),
                        ),
                        occurrence=MemoryOccurrence(
                            "test-memory-occurrence",
                            "binding/main",
                            "onebot:group:933179609",
                            "test-memory-event",
                            1,
                            now,
                        ),
                        occurred_at=now,
                        expected_deletion_epoch=epoch,
                    )
                    await memory_runtime.repository.qualify_occurrence(
                        operation_id="test-memory-qualify",
                        source_unit_id="test-memory-unit",
                        occurrence_id="test-memory-occurrence",
                        input_batch_ref="test-input-batch",
                        context_revision=1,
                        expected_deletion_epoch=epoch,
                        _skip_input_batch_verification=True,
                    )
                    facets = await memory_runtime.retrieval.prepare_source_facets(
                        operation_id="test-memory-facets",
                        source_unit_id="test-memory-unit",
                        expected_deletion_epoch=epoch,
                    )
                    memory_runtime.notify()
                    async with asyncio.timeout(5):
                        while True:
                            async with host.store.pool.connection() as conn:
                                ready = await (
                                    await conn.execute(
                                        sql.SQL(
                                            "SELECT state FROM {}.embedding_facets "
                                            "WHERE bot_id=%s AND facet_id=%s"
                                        ).format(
                                            sql.Identifier(memory_runtime.repository.namespace)
                                        ),
                                        (settings.bot_id, facets["facets"][0]["facet_id"]),
                                    )
                                ).fetchone()
                            if ready["state"] == "READY":
                                break
                            await asyncio.sleep(0.05)
                    query_source = PayloadSource(
                        "adapter.onebot", "test.event", "test-query-event", 1
                    )
                    query_author = await host._kernel.payload.put_bytes(
                        owner_component_id="adapter.onebot",
                        owner_kind="test.author",
                        owner_id="test-query-author",
                        media_type="application/json",
                        content=b'{"subject":"qq:questioner"}',
                        sources=(query_source,),
                        expected_deletion_epoch=epoch,
                    )
                    query_text = await host._kernel.payload.put_bytes(
                        owner_component_id="adapter.onebot",
                        owner_kind="test.text",
                        owner_id="test-query-text",
                        media_type="text/plain",
                        content=b"what durable memory exists?",
                        sources=(query_source,),
                        expected_deletion_epoch=epoch,
                    )
                    await memory_runtime.repository.capture_source_unit(
                        operation_id="test-query-capture",
                        source_unit_id="test-query-unit",
                        source_id="test-memory-source",
                        source_revision=1,
                        unit_kind="GROUP_MESSAGE",
                        author_subject_ref="qq:questioner",
                        author=MemoryPayloadRef(
                            query_author.payload_id,
                            query_author.content_digest,
                            "adapter.onebot",
                            "test.author",
                            "test-query-author",
                        ),
                        parts=(
                            MemoryPart(
                                "part/text",
                                0,
                                "TEXT",
                                "text/plain",
                                MemoryPayloadRef(
                                    query_text.payload_id,
                                    query_text.content_digest,
                                    "adapter.onebot",
                                    "test.text",
                                    "test-query-text",
                                ),
                                byte_length=27,
                            ),
                        ),
                        occurrence=MemoryOccurrence(
                            "test-query-occurrence",
                            "binding/main",
                            "onebot:group:933179609",
                            "test-query-event",
                            2,
                            now,
                        ),
                        occurred_at=now,
                        expected_deletion_epoch=epoch,
                    )
                    await memory_runtime.repository.qualify_occurrence(
                        operation_id="test-query-qualify",
                        source_unit_id="test-query-unit",
                        occurrence_id="test-query-occurrence",
                        input_batch_ref="test-query-input-batch",
                        context_revision=2,
                        expected_deletion_epoch=epoch,
                        _skip_input_batch_verification=True,
                    )
                    recalled = await memory_runtime.recall(
                        recall_id="test-agent-input-revision-2",
                        conversation_id="onebot:group:933179609",
                        input_batch_ref="test-query-input-batch",
                        context_revision=2,
                        input_cut=2,
                        timeout_seconds=5,
                    )
                    assert recalled is not None
                    recalled_by_unit = {
                        item["source_unit_id"]: item for item in recalled.evidence["evidence"]
                    }
                    assert set(recalled_by_unit) == {
                        "test-memory-unit",
                        capture["source_unit_id"],
                    }
                    assert any(
                        part["type"] == "image"
                        for part in recalled_by_unit[capture["source_unit_id"]]["parts"]
                    )
                    assert recalled.media
                    assert recalled.evidence["conversation_id"] == ("onebot:group:933179609")
                    recalled_source = recalled_by_unit["test-memory-unit"]["source"]
                    assert recalled_source["group_source_id"] == ("onebot:group:933179609")
                    assert recalled_source["channel_id"] == "933179609"
                    assert recalled_by_unit["test-memory-unit"]["author"] == {
                        "principal_ref": "qq:test",
                        "identity_origin": "memory_verified",
                    }
                    agent_runtime = host.registry.get("agent.runtime")
                    assert agent_runtime.memory is not None
                    await clients[-1].handler(
                        json.dumps(
                            {
                                "self_id": int(settings.onebot_self_id),
                                "post_type": "message",
                                "message_type": "group",
                                "sub_type": "normal",
                                "time": 1_720_000_001,
                                "message_id": 4243,
                                "user_id": 20002,
                                "group_id": 933179609,
                                "sender": {
                                    "user_id": 20002,
                                    "nickname": "Retry sender",
                                    "card": "",
                                    "role": "member",
                                },
                                "message": [
                                    {
                                        "type": "image",
                                        "data": {
                                            "file": "retry-file-id",
                                            "url": "https://media.example/retry",
                                        },
                                    }
                                ],
                            },
                            separators=(",", ":"),
                        ).encode()
                    )
                    retry_event_hash = hashlib.sha256(b"binding/main\x004243").hexdigest()
                    retry_capture_id = "media-capture:" + retry_event_hash
                    async with asyncio.timeout(8):
                        while True:
                            retry_capture = await media_runtime.repository.load(retry_capture_id)
                            if retry_capture["state"] == "READY":
                                break
                            await asyncio.sleep(0.05)
                    assert retry_capture["attempt_count"] == 2
                    assert (
                        sum("retry" in (item.get("uri") or "") for item in FakeMediaClient.fetched)
                        == 2
                    )
                    duplicate_body = json.dumps(
                        {
                            "self_id": int(settings.onebot_self_id),
                            "post_type": "message",
                            "message_type": "group",
                            "sub_type": "normal",
                            "time": 1_720_000_002,
                            "message_id": 4244,
                            "user_id": 20003,
                            "group_id": 933179609,
                            "sender": {
                                "user_id": 20003,
                                "nickname": "Duplicate sender",
                                "card": "",
                                "role": "member",
                            },
                            "message": [
                                {
                                    "type": "image",
                                    "data": {
                                        "file": "duplicate-file-id",
                                        "url": "https://media.example/duplicate",
                                    },
                                }
                            ],
                        },
                        separators=(",", ":"),
                    ).encode()
                    await asyncio.gather(
                        clients[-1].handler(duplicate_body),
                        clients[-1].handler(duplicate_body),
                    )
                    duplicate_event_hash = hashlib.sha256(b"binding/main\x004244").hexdigest()
                    duplicate_capture_id = "media-capture:" + duplicate_event_hash
                    async with asyncio.timeout(5):
                        while True:
                            duplicate_capture = await media_runtime.repository.load(
                                duplicate_capture_id
                            )
                            if duplicate_capture["state"] == "READY":
                                break
                            await asyncio.sleep(0.05)
                    assert (
                        sum(
                            "duplicate" in (item.get("uri") or "")
                            for item in FakeMediaClient.fetched
                        )
                        == 1
                    )
                    previous_graph = host.registry.graph_identity
                    await host._kernel.audit.append(
                        AuditEnvelope(
                            audit_id="test:audit:web",
                            action="test.audit.visible",
                            producer_component_id="test.fixture",
                            origin_kind="test",
                            actor_kind="principal",
                            actor_id="qq:42",
                            resource_kind="bot",
                            resource_id=settings.bot_id,
                            operation_kind="test.operation",
                            operation_id="web-audit",
                        )
                    )
                    app = create_clean_management_app(
                        host, access_token="test-management-token"
                    )
                    async with AsyncClient(
                        transport=ASGITransport(app=app), base_url="http://forest.test"
                    ) as web:
                        index = await web.get("/")
                        assert index.status_code == 200
                        assert "height:100dvh" in (
                            await web.get("/static/style.css")
                        ).text.replace(" ", "")
                        assert (await web.get("/static/modules/clean-audit.html")).status_code == 200
                        assert (await web.get("/static/clean-audit.js")).status_code == 200
                        assert (
                            await web.get(
                                "/static/modules/clean-authorization-policy.html"
                            )
                        ).status_code == 200
                        assert (
                            await web.get("/static/clean-authorization-policy.js")
                        ).status_code == 200
                        assert (await web.get("/static/modules/clean-jobs.html")).status_code == 200
                        assert (await web.get("/static/clean-jobs.js")).status_code == 200
                        assert (
                            await web.get("/static/modules/clean-delivery.html")
                        ).status_code == 200
                        assert (await web.get("/static/clean-delivery.js")).status_code == 200
                        assert index.headers["x-frame-options"] == "DENY"
                        assert "default-src 'none'" in index.headers["content-security-policy"]
                        unauthorized = await web.get("/management/v1/runtime")
                        assert unauthorized.status_code == 401
                        headers = {
                            "x-forest-management-token": "test-management-token"
                        }
                        runtime_view = await web.get(
                            "/management/v1/runtime", headers=headers
                        )
                        assert runtime_view.status_code == 200
                        embedding_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "embedding.multimodal"
                        )
                        assert embedding_view["management_contributions"][0][
                            "contribution_id"
                        ] == "configuration"
                        agent_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "agent.runtime"
                        )
                        assert [
                            item["contribution_id"]
                            for item in agent_view["management_contributions"]
                        ] == ["agent-profile"]
                        assert {
                            item["query_id"]
                            for item in agent_view["management_contract"]["queries"]
                        } == {"agent.runs.query", "agent.run.read"}
                        assert agent_view["management_contract"]["actions"] == [
                            {"action_id": "agent.run.cancel", "priority": "P0"}
                        ]
                        adapter_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "adapter.onebot"
                        )
                        assert [
                            item["contribution_id"]
                            for item in adapter_view["management_contributions"]
                        ] == ["onebot-bindings"]
                        memory_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "memory.graph"
                        )
                        assert {
                            item["contribution_id"]
                            for item in memory_view["management_contributions"]
                        } == {"memory-explorer", "configuration"}
                        audit_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "kernel.audit"
                        )
                        assert [
                            item["contribution_id"]
                            for item in audit_view["management_contributions"]
                        ] == ["audit-explorer"]
                        authorization_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "kernel.authorization"
                        )
                        assert [
                            item["contribution_id"]
                            for item in authorization_view["management_contributions"]
                        ] == ["web-operation-policy"]
                        assert authorization_view["management_contract"]["configuration"][
                            "schema"
                        ]["properties"]["protocol"]["const"] == (
                            "forest.web-operation-policy/1"
                        )
                        job_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "job.scheduler"
                        )
                        assert [
                            item["contribution_id"]
                            for item in job_view["management_contributions"]
                        ] == ["job-control", "configuration"]
                        assert {
                            item["query_id"]
                            for item in job_view["management_contract"]["queries"]
                        } == {"jobs.types", "jobs.query", "jobs.read"}
                        delivery_view = next(
                            item
                            for item in runtime_view.json()["modules"]
                            if item["component_id"] == "delivery"
                        )
                        assert [
                            item["contribution_id"]
                            for item in delivery_view["management_contributions"]
                        ] == ["delivery-reconciliation", "configuration"]
                        assert {
                            item["query_id"]
                            for item in delivery_view["management_contract"]["queries"]
                        } == {"delivery.unknown.query", "delivery.unknown.read"}
                        assert delivery_view["management_contract"]["actions"] == [
                            {
                                "action_id": "delivery.unknown.resolve",
                                "priority": "P0",
                            }
                        ]
                        job_types = await web.post(
                            "/management/v1/modules/job.scheduler/queries/jobs.types",
                            headers=headers,
                            json={},
                        )
                        assert job_types.status_code == 200
                        assert [item["job_type"] for item in job_types.json()["items"]] == [
                            "runtime.health-snapshot"
                        ]
                        web_job = await web.post(
                            "/management/v1/modules/job.scheduler/actions/jobs.run",
                            headers={**headers, "idempotency-key": "web-health-job"},
                            json={"job_type": "runtime.health-snapshot", "input": {}},
                        )
                        assert web_job.status_code == 200
                        web_job_id = web_job.json()["job_id"]
                        async with asyncio.timeout(5):
                            while True:
                                jobs_page = await web.post(
                                    "/management/v1/modules/job.scheduler/queries/jobs.query",
                                    headers=headers,
                                    json={
                                        "states": ["SUCCEEDED"],
                                        "job_type": "runtime.health-snapshot",
                                        "requested_by": "management.web",
                                        "limit": 25,
                                    },
                                )
                                if any(
                                    item["job_id"] == web_job_id
                                    for item in jobs_page.json()["items"]
                                ):
                                    break
                                await asyncio.sleep(0.05)
                        web_job_detail = await web.post(
                            "/management/v1/modules/job.scheduler/queries/jobs.read",
                            headers=headers,
                            json={"job_id": web_job_id},
                        )
                        assert web_job_detail.status_code == 200
                        assert web_job_detail.json()["result"]["protocol"] == (
                            "forest.runtime-health-snapshot/1"
                        )
                        audits = await web.post(
                            "/management/v1/modules/kernel.audit/queries/audit.records.query",
                            headers=headers,
                            json={
                                "action": "test.audit.visible",
                                "resource_kind": "bot",
                                "resource_id": settings.bot_id,
                                "limit": 25,
                            },
                        )
                        assert audits.status_code == 200
                        assert [
                            item["audit_id"] for item in audits.json()["records"]
                        ] == ["test:audit:web"]
                        malformed_audit = await web.post(
                            "/management/v1/modules/kernel.audit/queries/audit.records.query",
                            headers=headers,
                            json={"resource_kind": "bot"},
                        )
                        assert malformed_audit.status_code == 422
                        trunks = await web.post(
                            "/management/v1/modules/memory.graph/queries/memory.trunks.list",
                            headers=headers,
                            json={},
                        )
                        assert trunks.status_code == 200
                        assert any(
                            row["external_group_id"] == "933179609"
                            for row in trunks.json()
                        )
                        sources = await web.post(
                            "/management/v1/modules/memory.graph/queries/memory.sources.query",
                            headers=headers,
                            json={
                                "trunk_id": "test-memory-trunk",
                                "author_subject_ref": "qq:test",
                                "modality": "TEXT",
                                "limit": 50,
                            },
                        )
                        assert sources.status_code == 200
                        assert [
                            item["source_unit_id"] for item in sources.json()["items"]
                        ] == ["test-memory-unit"]
                        memory_configuration = await web.get(
                            "/management/v1/modules/memory.graph/configuration",
                            headers=headers,
                        )
                        assert memory_configuration.status_code == 200
                        malformed_memory = await web.post(
                            "/management/v1/modules/memory.graph/configuration/plans",
                            headers={
                                **headers,
                                "idempotency-key": "memory-configuration-invalid",
                            },
                            json={
                                "expected_desired_revision": memory_configuration.json()[
                                    "desired"
                                ]["revision"],
                                "value": {"protocol": "forest.memory.graph/1"},
                            },
                        )
                        assert malformed_memory.status_code == 422
                        assert malformed_memory.json()["code"] == "MEMORY_CONFIGURATION_INVALID"
                        current = await web.get(
                            "/management/v1/modules/embedding.multimodal/configuration",
                            headers=headers,
                        )
                        assert current.status_code == 200
                        current = current.json()
                        changed = deepcopy(current["desired"]["value"])
                        immutable = deepcopy(changed)
                        immutable["profile"]["dimensions"] = 2048
                        rejected_plan = await web.post(
                            "/management/v1/modules/embedding.multimodal/configuration/plans",
                            headers={
                                **headers,
                                "idempotency-key": "embedding-profile-invalid",
                            },
                            json={
                                "expected_desired_revision": current["desired"]["revision"],
                                "value": immutable,
                            },
                        )
                        assert rejected_plan.status_code == 409
                        assert rejected_plan.json()["code"] == "EMBEDDING_PROFILE_IMMUTABLE"
                        changed["endpoint"] = (
                            "http://127.0.0.1:9001/v1/embeddings"
                        )
                        plan = await web.post(
                            "/management/v1/modules/embedding.multimodal/configuration/plans",
                            headers={**headers, "idempotency-key": "embedding-endpoint-plan"},
                            json={
                                "expected_desired_revision": current["desired"]["revision"],
                                "value": changed,
                            },
                        )
                        assert plan.status_code == 200
                        applied = await web.post(
                            "/management/v1/modules/embedding.multimodal/configuration/apply",
                            headers={**headers, "idempotency-key": "embedding-endpoint-apply"},
                            json={
                                "plan_id": plan.json()["plan_id"],
                                "expected_desired_revision": current["desired"]["revision"],
                            },
                        )
                        assert applied.status_code == 200
                        assert applied.json()["replaced"] is True
                        agent_configuration = await web.get(
                            "/management/v1/modules/agent.runtime/configuration",
                            headers=headers,
                        )
                        assert agent_configuration.status_code == 200
                        agent_configuration = agent_configuration.json()
                        assert agent_configuration["core_prompt"]["digest"] == CORE_PROMPT_SHA256
                        assert agent_configuration["core_prompt"]["mutable"] is False
                        assert agent_configuration["core_prompt"]["text"].startswith(
                            '<forest_agent_protocol version="1">'
                        )
                        assert agent_configuration["desired"]["value"]["preset_context"] == [
                            settings.agent_system_prompt
                        ]
                        agent_runs = await web.post(
                            "/management/v1/modules/agent.runtime/queries/agent.runs.query",
                            headers=headers,
                            json={"states": ["PREPARING_MODEL"], "limit": 25},
                        )
                        assert agent_runs.status_code == 200
                        assert agent_runs.json()["items"] == []
                        malformed_agent_runs = await web.post(
                            "/management/v1/modules/agent.runtime/queries/agent.runs.query",
                            headers=headers,
                            json={"limit": 1000},
                        )
                        assert malformed_agent_runs.status_code == 422
                        agent_profile = deepcopy(agent_configuration["desired"]["value"])
                        agent_profile["preset_context"] = [
                            "You are Forest's managed single-Agent test persona.",
                            "Keep user identities distinct.",
                        ]
                        agent_profile["max_steps"] = 24
                        agent_plan = await web.post(
                            "/management/v1/modules/agent.runtime/configuration/plans",
                            headers={**headers, "idempotency-key": "agent-profile-plan"},
                            json={
                                "expected_desired_revision": agent_configuration["desired"][
                                    "revision"
                                ],
                                "value": agent_profile,
                            },
                        )
                        assert agent_plan.status_code == 200
                        agent_applied = await web.post(
                            "/management/v1/modules/agent.runtime/configuration/apply",
                            headers={**headers, "idempotency-key": "agent-profile-apply"},
                            json={
                                "plan_id": agent_plan.json()["plan_id"],
                                "expected_desired_revision": agent_configuration["desired"][
                                    "revision"
                                ],
                            },
                        )
                        assert agent_applied.status_code == 200
                        assert agent_applied.json()["replaced"] is True
                        active_agent = host.registry.get("agent.runtime")
                        assert active_agent._profile.preset_context == tuple(
                            agent_profile["preset_context"]
                        )
                        assert active_agent._profile.max_steps == 24
                        bindings = await web.post(
                            "/management/v1/modules/adapter.onebot/queries/onebot.bindings.list",
                            headers=headers,
                            json={},
                        )
                        assert bindings.status_code == 200
                        binding = bindings.json()["items"][0]
                        assert binding["binding_id"] == "binding/main"
                        assert binding["state"] == "ONLINE"
                        assert binding["config"]["websocket_url"] == (
                            "ws://127.0.0.1:3001/"
                        )
                        binding_config = deepcopy(binding["config"])
                        binding_config["websocket_url"] = "ws://127.0.0.1:3002/"
                        binding_plan = await web.post(
                            "/management/v1/modules/adapter.onebot/actions/onebot.binding.plan",
                            headers={**headers, "idempotency-key": "onebot-binding-plan"},
                            json={
                                "binding_id": binding["binding_id"],
                                "expected_revision": binding["desired_revision"],
                                "enabled": binding["desired_enabled"],
                                "receive_enabled": binding["receive_enabled"],
                                "transport": binding["transport"],
                                "config": binding_config,
                                "secret_ref": binding["secret_ref"],
                                "secret_digest": binding["secret_digest"],
                            },
                        )
                        assert binding_plan.status_code == 200
                        binding_applied = await web.post(
                            "/management/v1/modules/adapter.onebot/actions/onebot.binding.apply",
                            headers={**headers, "idempotency-key": "onebot-binding-apply"},
                            json={"plan_id": binding_plan.json()["plan_id"]},
                        )
                        assert binding_applied.status_code == 200
                        assert binding_applied.json()["replaced"] is True
                        assert clients[-1].url == "ws://127.0.0.1:3002/"
                        binding_replay = await web.post(
                            "/management/v1/modules/adapter.onebot/actions/onebot.binding.apply",
                            headers={**headers, "idempotency-key": "onebot-binding-apply"},
                            json={"plan_id": binding_plan.json()["plan_id"]},
                        )
                        assert binding_replay.status_code == 200
                        assert binding_replay.json()["replaced"] is False
                        assert binding_replay.json()["graph_id"] == binding_applied.json()[
                            "graph_id"
                        ]
                    replacement = host.registry
                    assert replacement.graph_identity.graph_id != previous_graph.graph_id
                    assert set(replacement.states.values()) == {ModuleLifecycleState.ACTIVE}
                    assert clients[-2].closed is True
                    assert clients[-1].handler is not None
                    async with host.store.pool.connection() as conn:
                        graph_states = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT graph_id,state FROM {}.runtime_graphs "
                                    "WHERE graph_id IN (%s,%s)"
                                ).format(sql.Identifier(host._kernel.runtime.namespace)),
                                (
                                    previous_graph.graph_id,
                                    replacement.graph_identity.graph_id,
                                ),
                            )
                        ).fetchall()
                    assert {row["graph_id"]: row["state"] for row in graph_states} == {
                        previous_graph.graph_id: "STOPPED",
                        replacement.graph_identity.graph_id: "ACTIVE",
                    }
                else:
                    runtime = host.registry.get("embedding.multimodal")
                    current = await runtime.module_configuration.desired(include_value=True)
                    changed = deepcopy(current.value)
                    changed["profile"]["dimensions"] = 2048
                    plan = await runtime.module_configuration.plan(
                        operation_id="test:embedding:model:plan",
                        expected_desired_revision=current.revision,
                        value=changed,
                        validation_digest=current.validation_digest,
                    )
                    await runtime.module_configuration.apply(
                        operation_id="test:embedding:model:apply",
                        plan_id=plan["plan_id"],
                        expected_desired_revision=current.revision,
                    )
            finally:
                await host.close()
            assert clients[-1].closed is True
        assert graph_ids[0] != graph_ids[1]
        assert FakeEmbeddingClient.endpoints == [
            "http://127.0.0.1:8001/v1/embeddings",
            "http://127.0.0.1:9001/v1/embeddings",
            "http://127.0.0.1:9001/v1/embeddings",
            "http://127.0.0.1:9001/v1/embeddings",
            "http://127.0.0.1:9001/v1/embeddings",
        ]
        rejected = CleanBotHost(
            settings,
            model_client_factory=FakeModelClient,
            embedding_client_factory=FakeEmbeddingClient,
            media_client_factory=FakeMediaClient,
            websocket_client_factory=create_websocket,
        )
        with pytest.raises(ForestError) as caught:
            await rejected.start()
        assert caught.value.code == "EMBEDDING_PROFILE_IMMUTABLE"
    finally:
        assert name.startswith("forest_host_") and len(name) == 44
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()

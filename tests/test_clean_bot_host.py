from __future__ import annotations

import asyncio
import hashlib
import json
import os
from copy import deepcopy
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.clean_bot_host import CleanBotHost, CleanBotSettings
from forest.embeddings import EmbeddingResult, EmbeddingVector
from forest.errors import ForestError
from forest.media_capture_io import CapturedImage
from forest.packages import PackageCatalog
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import ModuleLifecycleState


class FakeModelClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True

    @property
    def is_closed(self):
        return self.closed

    async def freeze_events(self):
        return None

    def resume_events(self):
        return None


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


class FakeMilvusClient:
    search_calls = 0
    fail_search_at = None
    collections = {}
    fail_next_delete = False
    delete_failures = 0

    class Schema:
        def __init__(self):
            self.fields = []

        def add_field(self, name, data_type, **values):
            self.fields.append({"name": name, "type": data_type, **values})

    class Indexes:
        def add_index(self, **_values):
            return None

    def __init__(self, **_values):
        self.closed = False

    async def has_collection(self, *, collection_name):
        return collection_name in self.collections

    def create_schema(self, **_values):
        return self.Schema()

    def prepare_index_params(self):
        return self.Indexes()

    async def create_collection(self, *, collection_name, schema, **_values):
        vector = next(item for item in schema.fields if item["name"] == "embedding")
        self.collections[collection_name] = {
            "dimensions": vector["dim"],
            "rows": {},
        }

    async def describe_collection(self, *, collection_name):
        collection = self.collections[collection_name]
        return {
            "fields": [
                {"name": "embedding", "params": {"dim": collection["dimensions"]}}
            ]
        }

    async def upsert(self, *, collection_name, data, **_values):
        rows = self.collections[collection_name]["rows"]
        for row in data:
            rows[row["ref_id"]] = deepcopy(row)
        return {"upsert_count": len(data)}

    async def get(self, *, collection_name, ids, output_fields, **_values):
        rows = self.collections[collection_name]["rows"]
        return [
            {field: rows[ref][field] for field in output_fields}
            for ref in ids
            if ref in rows
        ]

    async def search(self, *, collection_name, data, limit, output_fields, **_values):
        type(self).search_calls += 1
        if type(self).search_calls == type(self).fail_search_at:
            type(self).fail_search_at = None
            raise RuntimeError("Injected ANN search failure")
        query = data[0]
        rows = self.collections[collection_name]["rows"]
        candidates = []
        for ref_id, row in rows.items():
            if row["purpose"] != "DOCUMENT":
                continue
            score = sum(left * right for left, right in zip(query, row["embedding"], strict=True))
            candidates.append(
                {
                    "ref_id": ref_id,
                    "distance": score,
                    "entity": {field: row[field] for field in output_fields},
                }
            )
        candidates.sort(key=lambda item: (-item["distance"], item["ref_id"]))
        return [candidates[:limit]]

    async def delete(self, *, collection_name, ids, **_values):
        if type(self).fail_next_delete:
            type(self).fail_next_delete = False
            type(self).delete_failures += 1
            raise OSError("transient vector deletion failure")
        rows = self.collections[collection_name]["rows"]
        for ref_id in ids:
            rows.pop(ref_id, None)

    async def close(self):
        self.closed = True


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

    @property
    def is_closed(self) -> bool:
        return self.closed

    async def freeze_events(self) -> None:
        return None

    def resume_events(self) -> None:
        return None

    async def aclose(self) -> None:
        self.closed = True

    def owned_tasks(self):
        return ()


@pytest.mark.asyncio
async def test_bot_host_supervision_propagates_current_module_task_failure():
    async def fail():
        await asyncio.sleep(0)
        raise ForestError("MEMORY_WORKER_TEST_FAILED", "expected worker failure", 503)

    async def keep_running():
        await asyncio.Event().wait()

    failed = asyncio.create_task(fail(), name="memory-owned-test")
    renewal = asyncio.create_task(keep_running(), name="renewal-test")
    replacement = asyncio.create_task(keep_running(), name="replacement-test")
    host = object.__new__(CleanBotHost)
    host._closing = False
    host._replacement_lock = asyncio.Lock()
    host._renewal_task = renewal
    host._replacement_task = replacement
    host.registry = SimpleNamespace(tasks=lambda: (failed,))
    try:
        with pytest.raises(ForestError) as caught:
            await host._supervise_owned_tasks()
        assert caught.value.code == "MEMORY_WORKER_TEST_FAILED"
    finally:
        renewal.cancel()
        replacement.cancel()
        await asyncio.gather(renewal, replacement, return_exceptions=True)


@pytest.mark.asyncio
async def test_bot_host_supervision_ignores_tasks_from_superseded_graph():
    old_release = asyncio.Event()
    new_release = asyncio.Event()

    async def wait_for(event):
        await event.wait()

    old_task = asyncio.create_task(wait_for(old_release), name="old-graph-worker")
    new_task = asyncio.create_task(wait_for(new_release), name="new-graph-worker")
    renewal = asyncio.create_task(wait_for(new_release), name="renewal-test")
    replacement = asyncio.create_task(wait_for(new_release), name="replacement-test")
    host = object.__new__(CleanBotHost)
    host._closing = False
    host._replacement_lock = asyncio.Lock()
    host._renewal_task = renewal
    host._replacement_task = replacement
    host.registry = SimpleNamespace(tasks=lambda: (old_task,))
    supervision = asyncio.create_task(host._supervise_owned_tasks())
    try:
        await asyncio.sleep(0)
        async with host._replacement_lock:
            host.registry = SimpleNamespace(tasks=lambda: (new_task,))
            old_release.set()
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        assert not supervision.done()
    finally:
        host._closing = True
        for task in (supervision, new_task, renewal, replacement):
            task.cancel()
        await asyncio.gather(
            supervision,
            old_task,
            new_task,
            renewal,
            replacement,
            return_exceptions=True,
        )


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
        return candidate

    host._assemble_registry = assemble
    result = await host.replace_runtime_graph()

    assert result is candidate
    assert host.registry is candidate
    assert events == [
        "assemble-candidate",
        "prewarm-candidate",
        "drain-previous",
        "publish-candidate",
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
        return candidate

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


@pytest.mark.postgres
async def test_clean_bot_host_starts_retires_and_restarts_exact_graph(tmp_path):
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
        agent_system_prompt="The builtin Agent production integration fixture.",
    )
    clients = []
    package_catalog = PackageCatalog(tmp_path / "host-packages")

    async def create_websocket(_url, _token, **_limits):
        client = FakeWebSocketClient(settings.onebot_self_id)
        clients.append(client)
        return client

    def create_host():
        return CleanBotHost(
            settings,
            model_client_factory=FakeModelClient,
            embedding_client_factory=FakeEmbeddingClient,
            media_client_factory=FakeMediaClient,
            vector_index_client_factory=FakeMilvusClient,
            websocket_client_factory=create_websocket,
            package_catalog=package_catalog,
        )

    try:
        host = create_host()
        await host.start()
        try:
            assert set(host.registry.states.values()) == {ModuleLifecycleState.ACTIVE}
            async with host.store.pool.connection() as conn:
                grants = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT s.grantee_id,e.capability,e.resource_kind,e.resource_id,"
                            "e.delegable "
                            "FROM {}.grant_sets s JOIN {}.grant_entries e "
                            "USING(bot_id,grant_set_id) WHERE s.bot_id=%s "
                            "AND s.grantee_id IN ('agent.runtime','memory.graph')"
                        ).format(
                            sql.Identifier(host._kernel.authorization.namespace),
                            sql.Identifier(host._kernel.authorization.namespace),
                        ),
                        (settings.bot_id,),
                    )
                ).fetchall()
            assert {
                (row["grantee_id"], row["capability"], row["resource_kind"],
                 row["resource_id"])
                for row in grants
            } >= {
                ("agent.runtime", "provider.invoke", "bot", settings.bot_id),
                ("memory.graph", "embedding.invoke", "bot", settings.bot_id),
            }
            assert all(
                row["delegable"]
                for row in grants
                if row["capability"] in {"provider.invoke", "embedding.invoke"}
            )
            memory_schema = next(
                descriptor.storage.spec.namespace
                for descriptor in host.registry.descriptors
                if descriptor.component_id == "memory.graph"
            )
            async with asyncio.timeout(20):
                while True:
                    async with host.store.pool.connection() as conn:
                        saved_domain = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT d.space_id,d.dimensions,w.state FROM {}.memory_domains d "
                                    "JOIN {}.memory_domain_bind_work w USING(bot_id)"
                                ).format(
                                    sql.Identifier(memory_schema),
                                    sql.Identifier(memory_schema),
                                )
                            )
                        ).fetchone()
                    if saved_domain is not None and saved_domain["state"] == "READY":
                        break
                    await asyncio.sleep(0.1)
            memory_instance = host.registry.get("memory.graph")
            memory_identity = memory_instance.generation_identity
            domain = await memory_instance.runtime_service_handlers()[
                ("memory.domain.ensure", 1)
            ].invoke_routed(
                FrozenServiceRoute(
                    service_id="memory.domain.ensure",
                    version=1,
                    caller_component_id="memory.graph",
                    provider_component_id="memory.graph",
                    provider_generation_id=memory_identity.generation_id,
                    graph_id=memory_identity.graph_id,
                    graph_revision=memory_identity.graph_revision,
                    admission_lane="DATA",
                    owned_work=False,
                ),
                RuntimeServiceInvocation(
                    "memory-domain-host-test", 20, "memory.domain", "bot-production-test"
                ),
                (),
                {
                    "protocol": "forest.memory-domain-ensure/1",
                    "request_id": "memory-domain-host-test",
                },
            )
            assert domain["space_id"] == saved_domain["space_id"]
            assert domain["dimensions"] == saved_domain["dimensions"] == 4096
            embedding_instance = host.registry.get("embedding.multimodal")
            embedding_identity = embedding_instance.generation_identity
            embedding_profile = await embedding_instance.runtime_service_handlers()[
                ("embedding.profile.bind", 1)
            ].invoke_routed(
                FrozenServiceRoute(
                    service_id="embedding.profile.bind",
                    version=1,
                    caller_component_id="memory.graph",
                    provider_component_id="embedding.multimodal",
                    provider_generation_id=embedding_identity.generation_id,
                    graph_id=embedding_identity.graph_id,
                    graph_revision=embedding_identity.graph_revision,
                    admission_lane="DATA",
                    owned_work=False,
                ),
                RuntimeServiceInvocation(
                    "embedding-bind-host-test", 20,
                    "memory.embedding", "bot-production-test",
                ),
                (),
                {
                    "protocol": "forest.embedding-bind-request/1",
                    "request_id": "embedding-materialize-host-test",
                },
            )
            source_bytes = "跨群记忆".encode("utf-8")
            source_payload = await host._kernel.payload.put_bytes(
                owner_component_id="memory.graph",
                owner_kind="embedding.source.test",
                owner_id="source/materialize/test",
                media_type="text/plain",
                content=source_bytes,
                expected_deletion_epoch=await host._kernel.payload.epoch(),
            )
            source_ref = {
                "payload_id": source_payload.payload_id,
                "digest": source_payload.content_digest,
                "owner_component_id": "memory.graph",
                "owner_kind": "embedding.source.test",
                "owner_id": "source/materialize/test",
            }
            materialized = await embedding_instance.runtime_service_handlers()[
                ("embedding.request.materialize", 1)
            ].invoke_routed(
                FrozenServiceRoute(
                    service_id="embedding.request.materialize",
                    version=1,
                    caller_component_id="memory.graph",
                    provider_component_id="embedding.multimodal",
                    provider_generation_id=embedding_identity.generation_id,
                    graph_id=embedding_identity.graph_id,
                    graph_revision=embedding_identity.graph_revision,
                    admission_lane="DATA",
                    owned_work=False,
                ),
                RuntimeServiceInvocation(
                    "embedding-materialize-host-test", 30,
                    "memory.embedding", "bot-production-test",
                ),
                (),
                {
                    "protocol": "forest.embedding-materialize/1",
                    "request_id": "embedding-materialize-host-test",
                    "transport_attempt_id": "embedding-materialize-attempt-test",
                    "component_id": "embedding.multimodal",
                    "generation_id": embedding_identity.generation_id,
                    "profile_ref": embedding_profile["profile_ref"],
                    "space_id": embedding_profile["space_id"],
                    "purpose": "DOCUMENT",
                    "facets": [{
                        "facet_id": "embedding-facet-host-test",
                        "source_unit_id": "source/materialize/test",
                        "source_revision": 1,
                        "part_id": "part/text",
                        "trunk_id": "trunk/group/test",
                        "conversation_ref": "onebot:group:933179609",
                        "author_ref": "onebot:user:42",
                        "modality": "TEXT",
                        "selector_start_char": 0,
                        "selector_end_char": len("跨群记忆"),
                        "input_payload": source_ref,
                        "facet_content_digest": source_payload.content_digest,
                        "input_byte_length": len(source_bytes),
                        "media_type": "text/plain",
                        "width": None,
                        "height": None,
                    }],
                },
            )
            assert materialized["prepared"]["body"]["owner_component_id"] == (
                "embedding.multimodal"
            )
            assert json.loads(await host._kernel.payload.read_bytes(
                materialized["prepared"]["body"]["payload_id"]
            ))["input"] == [{"text": "跨群记忆"}]
            package_ids = {
                descriptor.component_id: descriptor.artifact.package_id
                for descriptor in host.registry.descriptors
            }
            expected_builtin_packages = {
                "agent.runtime": "forest.agent-runtime",
                "memory.graph": "forest.memory-graph",
                "conversation.scheduler": "forest.conversation-scheduler",
                "adapter.onebot": "forest.adapter-onebot",
            }
            assert {
                component_id: package_ids[component_id]
                for component_id in expected_builtin_packages
            } == expected_builtin_packages

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
            event_hash = hashlib.sha256(b"binding/main\x004242").hexdigest()
            memory_namespace = next(
                descriptor.storage.spec.namespace
                for descriptor in host.registry.descriptors
                if descriptor.component_id == "memory.graph"
            )
            conversation_namespace = next(
                descriptor.storage.spec.namespace
                for descriptor in host.registry.descriptors
                if descriptor.component_id == "conversation.scheduler"
            )
            async with asyncio.timeout(10):
                while True:
                    async with host.store.pool.connection() as conn:
                        parts = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT part_kind,part_index FROM {}.source_parts "
                                    "WHERE bot_id=%s AND source_unit_id=%s ORDER BY part_index"
                                ).format(sql.Identifier(memory_namespace)),
                                (settings.bot_id, "onebot-unit:" + event_hash),
                            )
                        ).fetchall()
                        lane = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT context_applied_seq FROM {}.conversation_lanes "
                                    "WHERE bot_id=%s AND conversation_id=%s"
                                ).format(sql.Identifier(conversation_namespace)),
                                (settings.bot_id, "onebot:group:933179609"),
                            )
                        ).fetchone()
                    if len(parts) == 3 and lane is not None and lane["context_applied_seq"] == 1:
                        break
                    await asyncio.sleep(0.05)
            assert [(row["part_kind"], row["part_index"]) for row in parts] == [
                ("TEXT", 0),
                ("IMAGE", 1),
                ("TEXT", 2),
            ]

            async with host.store.pool.connection() as conn:
                occurrence = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT event_ref,input_batch_ref,context_revision "
                            "FROM {}.source_occurrences "
                            "WHERE bot_id=%s AND source_unit_id=%s"
                        ).format(sql.Identifier(memory_namespace)),
                        (settings.bot_id, "onebot-unit:" + event_hash),
                    )
                ).fetchone()
            assert occurrence is not None
            event_ref = occurrence["event_ref"]
            batch_ref = occurrence["input_batch_ref"] or "batch/host-embedding"
            context_revision = occurrence["context_revision"] or 1
            compaction_ref = "compaction/host-embedding"
            archived_selection = {
                "status": "BATCH",
                "operation_id": "conversation/silent-archive/host-embedding",
                "conversation_id": "onebot:group:933179609",
                "classified_through_seq": 1,
                "selected_input_end_seq": 1,
                "mailbox_revision": 1,
                "input_batch_id": batch_ref,
                "boundary": "IDLE",
                "kind": "SILENT_DRAIN",
                "start_seq": 1,
                "end_seq": 1,
                "trigger_cut_seq": None,
                "session_ref": None,
                "run_ref": None,
                "context_revision": context_revision,
                "context_view_revision": context_revision,
                "event_ids": [event_ref],
                "trigger_ids": [],
                "triggers": [],
                "memory_archive": {
                    "compaction_ref": compaction_ref,
                    "event_refs": [event_ref],
                },
            }
            participant = memory_instance.unit_of_work_participant()
            async with host.store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn,
                    "memory.qualify-input-batch",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "agent.runtime",
                            "generation_id": host.registry.get("agent.runtime").generation_identity.generation_id,
                        },
                        "input": {
                            "selection_step_id": "selection",
                            "deletion_epoch_step_id": "payload_epoch",
                        },
                        "prior": {
                            "selection": archived_selection,
                            "payload_epoch": {
                                "protocol": "forest.payload-epoch/1",
                                "deletion_epoch": await host._kernel.payload.epoch(),
                            },
                        },
                    },
                    operation_id="memory/query-admission/host-embedding",
                    causal_root_kind="memory.query",
                    causal_root_id="query/host-embedding",
                    parent_call_id="call/memory/query-admission/host-embedding",
                    timeout_seconds=10,
                )
            query_route = FrozenServiceRoute(
                service_id="memory.query.begin", version=1,
                caller_component_id="memory.graph", provider_component_id="memory.graph",
                provider_generation_id=memory_identity.generation_id,
                graph_id=memory_identity.graph_id,
                graph_revision=memory_identity.graph_revision,
                admission_lane="DATA", owned_work=False,
            )
            query_input = {
                "query_id": "query/host-embedding",
                "conversation_ref": "onebot:group:933179609",
                "input_batch_ref": batch_ref, "context_revision": context_revision,
            }
            query = await memory_instance.runtime_service_handlers()[
                ("memory.query.begin", 1)
            ].invoke_routed(
                query_route,
                RuntimeServiceInvocation(
                    "query-begin-host-embedding", 30, "memory.query", settings.bot_id,
                ),
                (),
                query_input,
            )
            assert query["query_id"] == "query/host-embedding"
            assert query["part_count"] == 3 and query["query_mode"] == "MIXED"
            assert await memory_instance.runtime_service_handlers()[
                ("memory.query.begin", 1)
            ].invoke_routed(
                query_route,
                RuntimeServiceInvocation(
                    "query-begin-host-embedding", 30, "memory.query", settings.bot_id,
                ),
                (), query_input,
            ) == query
            assert await memory_instance.runtime_service_handlers()[
                ("memory.query.begin", 1)
            ].invoke_routed(
                query_route,
                RuntimeServiceInvocation(
                    "query-begin-host-new-caller", 30, "memory.query", settings.bot_id,
                ),
                (), query_input,
            ) == query
            with pytest.raises(ForestError) as caught:
                await memory_instance.runtime_service_handlers()[
                    ("memory.query.begin", 1)
                ].invoke_routed(
                    query_route,
                    RuntimeServiceInvocation(
                        "query-begin-wrong-context", 30, "memory.query", settings.bot_id,
                    ),
                    (),
                    {**query_input, "query_id": "query/wrong-context",
                     "context_revision": context_revision + 1},
                )
            assert caught.value.code == "MEMORY_QUERY_INPUT_UNAVAILABLE"
            query_parts_port = next(
                port for port in memory_instance.component["domain_queries"]
                if port["name"] == "memory.read-query-parts"
            )
            first_page = await memory_instance.invoke_management_query(
                query_parts_port,
                {"query_id": query["query_id"], "after_part_index": -1, "limit": 2},
                timeout_seconds=10,
            )
            assert first_page["has_more"] is True
            assert [part["part_kind"] for part in first_page["parts"]] == ["TEXT", "IMAGE"]
            second_page = await memory_instance.invoke_management_query(
                query_parts_port,
                {"query_id": query["query_id"],
                 "after_part_index": first_page["next_part_index"], "limit": 2},
                timeout_seconds=10,
            )
            assert second_page["has_more"] is False
            assert [part["part_kind"] for part in second_page["parts"]] == ["TEXT"]
            with pytest.raises(ForestError) as caught:
                async with host.store.pool.connection() as conn, conn.transaction():
                    await participant.invoke_in(
                        conn, "memory.complete-query-facets-record",
                        {
                            "protocol": "forest.uow-step/1",
                            "caller": {
                                "component_id": "memory.graph",
                                "generation_id": memory_identity.generation_id,
                            },
                            "input": {
                                "protocol": "forest.memory-query-complete/1",
                                "query_id": query["query_id"], "facet_count": 0,
                                "deletion_epoch_step_id": "payload_epoch",
                            },
                            "prior": {"payload_epoch": {
                                "protocol": "forest.payload-epoch/1",
                                "deletion_epoch": await host._kernel.payload.epoch(),
                            }},
                        },
                        operation_id="query-complete-before-parts",
                        causal_root_kind="memory.query", causal_root_id=query["query_id"],
                        parent_call_id="call/query-complete-before-parts", timeout_seconds=10,
                    )
            assert caught.value.code == "MEMORY_QUERY_PREPARATION_INCOMPLETE"
            prepare_route = FrozenServiceRoute(
                service_id="memory.query.prepare", version=1,
                caller_component_id="memory.graph", provider_component_id="memory.graph",
                provider_generation_id=memory_identity.generation_id,
                graph_id=memory_identity.graph_id,
                graph_revision=memory_identity.graph_revision,
                admission_lane="DATA", owned_work=False,
            )

            async def prepare_query(operation):
                return await memory_instance.runtime_service_handlers()[
                    ("memory.query.prepare", 1)
                ].invoke_routed(
                    prepare_route,
                    RuntimeServiceInvocation(operation, 30, "memory.query", settings.bot_id),
                    (), {"query_id": query["query_id"]},
                )

            prepared_query, concurrent_query = await asyncio.gather(
                prepare_query("query-prepare-host-embedding/a"),
                prepare_query("query-prepare-host-embedding/b"),
            )
            assert concurrent_query == prepared_query
            assert prepared_query == {
                "protocol": "forest.memory-query-facets/1",
                "query_id": query["query_id"], "part_count": 3,
                "facet_count": 3, "status": "PREPARED",
            }
            async with asyncio.timeout(40):
                while True:
                    async with host.store.pool.connection() as conn:
                        query_facets = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT facet_id,state,result_payload_id FROM {}.embedding_facets "
                                    "WHERE bot_id=%s AND query_id='query/host-embedding'"
                                ).format(sql.Identifier(memory_namespace)),
                                (settings.bot_id,),
                            )
                        ).fetchall()
                    if len(query_facets) == 3 and all(row["state"] == "READY" for row in query_facets):
                        break
                    await asyncio.sleep(0.1)
            async with host.store.pool.connection() as conn:
                unit_state = await (
                    await conn.execute(
                        sql.SQL("SELECT eligibility_state FROM {}.source_units WHERE bot_id=%s AND source_unit_id=%s").format(sql.Identifier(memory_namespace)),
                        (settings.bot_id, "onebot-unit:" + event_hash),
                    )
                ).fetchone()
            assert unit_state["eligibility_state"] == "REGISTERED"
            async with host.store.pool.connection() as conn, conn.transaction():
                archive = await participant.invoke_in(
                    conn,
                    "memory.qualify-archive",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "conversation.scheduler",
                            "generation_id": memory_identity.generation_id,
                        },
                        "input": {},
                        "prior": {
                            "prepare": {
                                "status": "WRITE",
                                "commit": {
                                    "conversation_id": "onebot:group:933179609",
                                    "compaction_id": compaction_ref,
                                    "archived_unit_count": 1,
                                    "units": [{
                                        "event_id": event_ref,
                                        "input_batch_id": batch_ref,
                                        "context_revision": context_revision,
                                    }],
                                },
                            },
                            "commit": archived_selection,
                            "payload_epoch": {
                                "protocol": "forest.payload-epoch/1",
                                "deletion_epoch": await host._kernel.payload.epoch(),
                            },
                        },
                    },
                    operation_id="memory/archive/host-embedding",
                    causal_root_kind="conversation.silent-drain",
                    causal_root_id="onebot:group:933179609",
                    parent_call_id="call/memory/archive/host-embedding",
                    timeout_seconds=10,
                )
            assert archive == archived_selection
            try:
                async with asyncio.timeout(40):
                    while True:
                        async with host.store.pool.connection() as conn:
                            facets = await (
                                await conn.execute(
                                    sql.SQL(
                                        "SELECT facet_id,modality,state,result_payload_id,index_ref_id "
                                        "FROM {}.embedding_facets WHERE bot_id=%s "
                                        "AND source_unit_id=%s AND purpose='DOCUMENT'"
                                    ).format(sql.Identifier(memory_namespace)),
                                    (settings.bot_id, "onebot-unit:" + event_hash),
                                )
                            ).fetchall()
                        if len(facets) == 3 and all(
                            facet["state"] == "READY" for facet in facets
                        ):
                            break
                        await asyncio.sleep(0.1)
            except TimeoutError as exc:
                async with host.store.pool.connection() as conn:
                    attempt_rows = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,error_code,claim_id FROM {}.embedding_batch_attempts "
                                "WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_namespace)),
                            (settings.bot_id,),
                        )
                    ).fetchall()
                    facet_work = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,last_error_code FROM {}.facet_preparation_work "
                                "WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_namespace)),
                            (settings.bot_id,),
                        )
                    ).fetchall()
                    claims = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,item_count FROM {}.embedding_claims "
                                "WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_namespace)),
                            (settings.bot_id,),
                        )
                    ).fetchall()
                    settlements = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT input_payload FROM {}.embedding_work_settlements "
                                "WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_namespace)),
                            (settings.bot_id,),
                        )
                    ).fetchall()
                    vector_work = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,attempt_count,claim_payload,next_attempt_at "
                                "FROM {}.vector_publish_batches WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_namespace)),
                            (settings.bot_id,),
                        )
                    ).fetchall()
                    vector_settlements = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT input_payload FROM {}.vector_work_settlements "
                                "WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_namespace)),
                            (settings.bot_id,),
                        )
                    ).fetchall()
                raise AssertionError({
                    "facets": facets,
                    "facet_work": facet_work,
                    "claims": claims,
                    "attempts": attempt_rows,
                    "settlements": settlements,
                    "vector_work": vector_work,
                    "vector_settlements": vector_settlements,
                    "owned_failure": memory_instance._owned_work.failure,
                }) from exc
            assert sorted(facet["modality"] for facet in facets) == [
                "IMAGE", "TEXT", "TEXT"
            ]
            assert all(facet["result_payload_id"] for facet in facets)
            assert all(facet["index_ref_id"] == facet["facet_id"] for facet in facets)
            index_collection = host.registry.get("vector.index")._effect_drivers[
                "vector.index.upsert"
            ].collection_name
            assert set(FakeMilvusClient.collections[index_collection]["rows"]) >= {
                facet["facet_id"] for facet in facets
            }

            async def query_stage(query_id, operation, stage="seeds"):
                service_id = "memory.query." + stage
                return await memory_instance.runtime_service_handlers()[
                    (service_id, 1)
                ].invoke_routed(
                    FrozenServiceRoute(
                        service_id=service_id, version=1,
                        caller_component_id="memory.graph", provider_component_id="memory.graph",
                        provider_generation_id=memory_identity.generation_id,
                        graph_id=memory_identity.graph_id,
                        graph_revision=memory_identity.graph_revision,
                        admission_lane="DATA", owned_work=False,
                    ),
                    RuntimeServiceInvocation(operation, 30, "memory.query", settings.bot_id),
                    (), {"query_id": query_id},
                )

            # This query predates document publication and cannot recall itself.
            original_matches = await query_stage(query["query_id"], "match/original", stage="match")
            assert original_matches["match_count"] == 0
            assert (await query_stage(query["query_id"], "seeds/original"))["seeds"] == []
            empty_record = await query_stage(query["query_id"], "freeze/original", stage="freeze")
            assert empty_record["result_count"] == empty_record["seed_count"] == 0
            await clients[-1].handler(json.dumps({
                "self_id": int(settings.onebot_self_id), "post_type": "message",
                "message_type": "group", "sub_type": "normal", "time": 1_720_000_001,
                "message_id": 4243, "user_id": 20002, "group_id": 933179609,
                "sender": {"user_id": 20002, "nickname": "Recall sender", "card": "", "role": "member"},
                "message": [
                    {"type": "text", "data": {"text": "before"}},
                    {"type": "image", "data": {"file": "recall-file", "url": "https://media.example/recall"}},
                    {"type": "text", "data": {"text": "after"}},
                ],
            }).encode())
            recall_hash = hashlib.sha256(b"binding/main\x004243").hexdigest()
            recall_unit = "onebot-unit:" + recall_hash
            async with asyncio.timeout(20):
                while True:
                    async with host.store.pool.connection() as conn:
                        recall_occurrence = await (await conn.execute(
                            sql.SQL("SELECT * FROM {}.source_occurrences WHERE bot_id=%s AND source_unit_id=%s").format(sql.Identifier(memory_namespace)),
                            (settings.bot_id, recall_unit),
                        )).fetchone()
                    if recall_occurrence is not None:
                        break
                    await asyncio.sleep(0.1)
            recall_batch = recall_occurrence["input_batch_ref"] or "batch/recall-host"
            recall_revision = recall_occurrence["context_revision"] or 1
            async with host.store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn, "memory.qualify-input-batch",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {"component_id": "agent.runtime", "generation_id": host.registry.get("agent.runtime").generation_identity.generation_id},
                        "input": {"selection_step_id": "selection", "deletion_epoch_step_id": "payload_epoch"},
                        "prior": {
                            "selection": {**archived_selection, "input_batch_id": recall_batch,
                                "context_revision": recall_revision,
                                "event_ids": [recall_occurrence["event_ref"]]},
                            "payload_epoch": {"protocol": "forest.payload-epoch/1", "deletion_epoch": await host._kernel.payload.epoch()},
                        },
                    },
                    operation_id="recall-admission-host", causal_root_kind="memory.query",
                    causal_root_id="query/recall-host", parent_call_id="call/recall-admission-host",
                    timeout_seconds=10,
                )
            recall_query = await memory_instance.runtime_service_handlers()[
                ("memory.query.begin", 1)
            ].invoke_routed(
                query_route,
                RuntimeServiceInvocation("query-begin-recall-host", 30, "memory.query", settings.bot_id),
                (), {"query_id": "query/recall-host", "conversation_ref": "onebot:group:933179609",
                     "input_batch_ref": recall_batch, "context_revision": recall_revision},
            )
            await memory_instance.runtime_service_handlers()[("memory.query.prepare", 1)].invoke_routed(
                prepare_route,
                RuntimeServiceInvocation("query-prepare-recall-host", 30, "memory.query", settings.bot_id),
                (), {"query_id": recall_query["query_id"]},
            )
            async with asyncio.timeout(40):
                while True:
                    async with host.store.pool.connection() as conn:
                        ready = await (await conn.execute(
                            sql.SQL("SELECT count(*) AS n FROM {}.embedding_facets WHERE bot_id=%s AND query_id=%s AND state='READY'").format(sql.Identifier(memory_namespace)),
                            (settings.bot_id, recall_query["query_id"]),
                        )).fetchone()
                    if ready["n"] == 3:
                        break
                    await asyncio.sleep(0.1)
            index_rows = FakeMilvusClient.collections[index_collection]["rows"]
            index_rows["stale-ref/no-fact"] = dict(index_rows[facets[0]["facet_id"]])
            FakeMilvusClient.fail_search_at = FakeMilvusClient.search_calls + 2
            with pytest.raises(ForestError):
                await query_stage(recall_query["query_id"], "match/recall-interrupted", stage="match")
            with pytest.raises(ForestError) as incomplete_matches:
                await query_stage(recall_query["query_id"], "seeds/recall-incomplete")
            assert incomplete_matches.value.code == "MEMORY_QUERY_MATCHES_PENDING"
            async with host.store.pool.connection() as conn:
                partial_matching = await (
                    await conn.execute(
                        sql.SQL("SELECT count(*) AS n FROM {}.query_match_receipts "
                                "WHERE bot_id=%s AND query_id=%s").format(sql.Identifier(memory_namespace)),
                        (settings.bot_id, recall_query["query_id"]),
                    )
                ).fetchone()
            assert partial_matching["n"] == 1
            matching = await asyncio.gather(
                query_stage(recall_query["query_id"], "match/recall-one", stage="match"),
                query_stage(recall_query["query_id"], "match/recall-two", stage="match"),
            )
            assert matching[0] == matching[1] == {
                "protocol": "forest.memory-query-matched/1", "query_id": recall_query["query_id"],
                "facet_count": 3, "match_count": 3,
            }
            search_count = FakeMilvusClient.search_calls
            assert await query_stage(
                recall_query["query_id"], "match/recall-replay", stage="match"
            ) == matching[0]
            assert FakeMilvusClient.search_calls == search_count
            seeds = await query_stage(recall_query["query_id"], "seeds/recall-before-forget")
            assert len(seeds["seeds"]) == 1
            assert seeds["seeds"][0]["source_unit_id"] == "onebot-unit:" + event_hash
            assert seeds["seeds"][0]["external_group_id"] == "933179609"
            assert seeds["seeds"][0]["author_subject_ref"] == "qq:20001"
            assert seeds["seeds"][0]["query_modalities"] == ["IMAGE", "TEXT"]
            assert seeds["seeds"][0]["activation"] == 1.0
            region = await query_stage(
                recall_query["query_id"], "region/recall-before-forget", stage="region"
            )
            assert region["policy_revision"] == "qiyu-graph-region/1"
            assert region["read_count"] <= 32
            assert recall_unit not in {node["source_unit_id"] for node in region["nodes"]}
            seed_node = next(node for node in region["nodes"] if node["is_seed"])
            assert seed_node["source_unit_id"] == "onebot-unit:" + event_hash
            assert seed_node["external_group_id"] == "933179609"
            assert seed_node["author_subject_ref"] == "qq:20001"
            assert seed_node["activation"] == 1.0
            assert FakeMilvusClient.search_calls == search_count

            frozen = await asyncio.gather(
                query_stage(recall_query["query_id"], "freeze/recall-one", stage="freeze"),
                query_stage(recall_query["query_id"], "freeze/recall-two", stage="freeze"),
            )
            assert frozen[0] == frozen[1]
            assert frozen[0]["result_count"] == len(region["nodes"])
            assert frozen[0]["seed_count"] == sum(node["is_seed"] for node in region["nodes"])
            assert FakeMilvusClient.search_calls == search_count
            async with host.store.pool.connection() as conn:
                record = await (await conn.execute(
                    sql.SQL("SELECT state,bundle_id FROM {}.memory_queries WHERE bot_id=%s AND query_id=%s").format(sql.Identifier(memory_namespace)),
                    (settings.bot_id, recall_query["query_id"]),
                )).fetchone()
                assert record["state"] == "FROZEN"
                assert record["bundle_id"] == frozen[0]["bundle_id"]
            retrieval_parts_port = next(
                port for port in memory_instance.component["domain_queries"]
                if port["name"] == "memory.read-retrieval-parts"
            )

            async def frozen_parts(after=-1):
                return await memory_instance.invoke_management_query(
                    retrieval_parts_port,
                    {"query_id": recall_query["query_id"], "item_index": 0,
                     "after_part_index": after, "limit": 2},
                    timeout_seconds=10,
                )

            first_parts = await frozen_parts()
            assert first_parts["node"]["source_unit_id"] == seed_node["source_unit_id"]
            assert first_parts["node"]["author_subject_ref"] == "qq:20001"
            assert first_parts["node"]["external_group_id"] == "933179609"
            assert {part["part_kind"] for part in first_parts["parts"]} == {"TEXT", "IMAGE"}
            assert first_parts["has_more"]
            final_parts = await frozen_parts(first_parts["last_part_index"])
            assert len(final_parts["parts"]) == 1 and not final_parts["has_more"]
            assert final_parts["parts"][0]["part_kind"] == "TEXT"

            # The public UoW revokes Payload, Memory eligibility, graph evidence,
            # and vector refs together before the asynchronous Milvus deletion.
            previous_epoch = await host._kernel.payload.epoch()
            async with host.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.relation_contributions("
                        "bot_id,contribution_id,operation_id,input_digest,relation_id,"
                        "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                        "weight_micros,admitted_deletion_epoch) "
                        "VALUES(%s,'forget/evidence','forget/evidence',%s,'forget/relation',"
                        "'ASSOCIATES_WITH','ENTITY','entity/a','ENTITY','entity/b','RULE',"
                        "500000,%s)"
                    ).format(sql.Identifier(memory_namespace)),
                    (settings.bot_id, "sha256:" + "1" * 64, previous_epoch),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.relation_evidence("
                        "bot_id,contribution_id,evidence_index,source_unit_id,"
                        "source_revision,part_id) "
                        "VALUES(%s,'forget/evidence',0,%s,1,'part:text:0')"
                    ).format(sql.Identifier(memory_namespace)),
                    (settings.bot_id, "onebot-unit:" + event_hash),
                )
            previous_delete_failures = FakeMilvusClient.delete_failures
            FakeMilvusClient.fail_next_delete = True
            forget_input = {
                "operation_id": "memory-forget-host-embedding",
                "source_unit_id": "onebot-unit:" + event_hash,
            }
            forget_route = FrozenServiceRoute(
                service_id="memory.source.forget",
                version=1,
                caller_component_id="memory.graph",
                provider_component_id="memory.graph",
                provider_generation_id=memory_identity.generation_id,
                graph_id=memory_identity.graph_id,
                graph_revision=memory_identity.graph_revision,
                admission_lane="DATA",
                owned_work=False,
            )
            forget = await memory_instance.runtime_service_handlers()[
                ("memory.source.forget", 1)
            ].invoke_routed(
                forget_route,
                RuntimeServiceInvocation(
                    "memory-forget-host-embedding", 30,
                    "memory.forget", "bot-production-test",
                ),
                (),
                forget_input,
            )
            assert forget["deletion_epoch"] == previous_epoch + 1
            assert forget["suppressed_source_units"] == 1
            assert forget["vector_deletions"] == len(facets) + len(query_facets)
            assert forget["retracted_relations"] == 1
            assert (await query_stage(recall_query["query_id"], "seeds/recall-after-forget"))["seeds"] == []
            assert (await query_stage(
                recall_query["query_id"], "region/recall-after-forget", stage="region"
            ))["nodes"] == []
            assert await query_stage(
                recall_query["query_id"], "freeze/recall-after-forget", stage="freeze"
            ) == frozen[0]
            assert (await frozen_parts())["node"] is None
            assert (await frozen_parts())["parts"] == []
            index_rows.pop("stale-ref/no-fact")
            with pytest.raises(ForestError) as caught:
                await memory_instance.invoke_management_query(
                    query_parts_port,
                    {"query_id": query["query_id"], "after_part_index": -1, "limit": 2},
                    timeout_seconds=10,
                )
            assert caught.value.code == "MEMORY_QUERY_INPUT_UNAVAILABLE"
            for result_payload_id in {facet["result_payload_id"] for facet in facets + query_facets}:
                with pytest.raises(ForestError) as caught:
                    await host._kernel.payload.read_bytes(result_payload_id)
                assert caught.value.code == "PAYLOAD_REVOKED"
            assert await host._kernel.payload.epoch() == previous_epoch + 1
            assert await memory_instance.runtime_service_handlers()[
                ("memory.source.forget", 1)
            ].invoke_routed(
                forget_route,
                RuntimeServiceInvocation(
                    "memory-forget-host-embedding-replay", 30,
                    "memory.forget", "bot-production-test",
                ),
                (),
                forget_input,
            ) == forget
            async with asyncio.timeout(15):
                while True:
                    async with host.store.pool.connection() as conn:
                        deleted = await (
                            await conn.execute(
                                sql.SQL(
                                    "SELECT state FROM {}.vector_index_deletions "
                                    "WHERE bot_id=%s AND source_unit_id=%s"
                                ).format(sql.Identifier(memory_namespace)),
                                (settings.bot_id, "onebot-unit:" + event_hash),
                            )
                        ).fetchall()
                    if len(deleted) == len(facets) + len(query_facets) and all(
                        row["state"] == "DELETED" for row in deleted
                    ):
                        break
                    await asyncio.sleep(0.1)
            assert not set(FakeMilvusClient.collections[index_collection]["rows"]) & {
                facet["facet_id"] for facet in facets + query_facets
            }
            assert FakeMilvusClient.delete_failures == previous_delete_failures + 1

            previous = host.registry.graph_identity
            replacement_graph = await host.replace_runtime_graph()
            assert replacement_graph.graph_identity.graph_id != previous.graph_id
            assert set(replacement_graph.states.values()) == {ModuleLifecycleState.ACTIVE}
            assert clients[-2].closed is True
            async with host.store.pool.connection() as conn:
                previous_state = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT state FROM {}.runtime_graphs WHERE graph_id=%s"
                        ).format(sql.Identifier(host._kernel.runtime.namespace)),
                        (previous.graph_id,),
                    )
                ).fetchone()
            assert previous_state["state"] == "STOPPED"
            replaced_graph_id = replacement_graph.graph_identity.graph_id
        finally:
            await host.close()
        assert clients[-1].closed is True

        restarted = create_host()
        await restarted.start()
        try:
            assert restarted.registry.graph_identity.graph_id != replaced_graph_id
            assert set(restarted.registry.states.values()) == {ModuleLifecycleState.ACTIVE}
        finally:
            await restarted.close()
        assert clients[-1].closed is True
    finally:
        assert name.startswith("forest_host_") and len(name) == 44
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import os
import sys
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shutil import copytree
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient
from PIL import Image
from psycopg import sql
from pymilvus import AsyncMilvusClient

from forest.adapters.onebot import SendResult
from forest.bot_host import BotGraphCandidate, BotHost, BotRuntimeSettings, BotSettings
from forest.builtin_packages import bootstrap_installed_package
from forest.component_graph_preview import preview_selected_graph
from forest.component_selection import ComponentSelectionEntry, ComponentSelectionSnapshot
from forest.deployment_management_app import create_deployment_management_app
from forest.embeddings import EmbeddingProfile, EmbeddingResult, EmbeddingVector
from forest.errors import ForestError
from forest.kernel_effect_executor_outcome import effect_executor_outcome_service
from forest.kernel_resource_admission import KERNEL_RESOURCE_POLICY_SCHEMA
from forest.management_gateway import ConfigurationApplyInput, ManagementGateway
from forest.media_capture_io import CapturedImage
from forest.model_contract import MediaSnapshot, ModelCapabilities, ModelResult
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RuntimeKernelServiceContext,
    RuntimeServiceInvocation,
)
from forest.runtime_modules import ModuleLifecycleState
from forest.schema_bundles import module_schema_namespace
from forest.shell_command_protocol import ShellCommandExecutionRequest
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_store import DeploymentStore
from forest.storage.deployment_web_identity import (
    AuthenticatedWebSession,
    DeploymentWebIdentityAuthority,
    WebSession,
    WebSubject,
)
from forest.storage.management_asset_authority import ManagementAssetAuthority
from forest.storage.service_operations import KernelServiceOperationLedger
from forest.unit_of_work import (
    ComponentConfigurationApplyScope,
    PostgresComponentConfigurationApplyCoordinator,
    PostgresPackageUnitOfWorkCoordinator,
)
from forest.vector_index_effect_driver import vector_collection_name
from forest.web_authority_ticket import WebAuthorityTicketAuthority
from forest.worker_limits import WorkerLimits

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/agent_runtime/python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/component_worker_host/python"))
from component_worker_host import ComponentWorkerHost
from model_projection import prepare_model_request

_TEST_SELECTION = ComponentSelectionSnapshot(0, (), digest_json({
    "protocol": "forest.component-selection-snapshot/1",
    "revision": 0,
    "entries": [],
}))


@asynccontextmanager
async def _real_web_identity_for_bot_test(dsn):
    name = "forest_host_web_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = DeploymentStore(uri, "deployment/host-web-test")
    try:
        await store.open()
        await DeploymentCatalogRepository(store).initialize()
        secret = bytes(range(32))
        identity = DeploymentWebIdentityAuthority(
            store, active_key_id="master/host-test/1",
            key_resolver=lambda key_id: secret if key_id == "master/host-test/1" else None,
        )
        issued_at = datetime.now(timezone.utc)
        credential = SimpleNamespace(
            setup_id="setup/0123456789abcdef", pairing_code="p" * 48,
            issued_at=issued_at, expires_at=issued_at + timedelta(hours=1),
        )
        await identity.initialize_setup(credential)
        await identity.complete_setup(
            pairing_code=credential.pairing_code,
            username="admin", display_name="Host administrator",
            password="host-web-password-1",
        )
        yield identity
    finally:
        await store.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


class FakeSelectionReader:
    @asynccontextmanager
    async def publication_fence(self, **_kwargs):
        yield _TEST_SELECTION


async def test_worker_host_selection_status_reports_kernel_outcome():
    requested = []

    class Queries:
        async def call(self, name, payload):
            assert name == "component.selection-application.read"
            assert payload == {"request_id": "selection:" + "a" * 32}
            return {
                "protocol": "forest.component-selection-application/1",
                "request_id": payload["request_id"],
                "component_id": "example.counter",
                "state": "SUBMITTED",
            }

    class Services:
        async def call(self, name, payload, **_kwargs):
            requested.append((name, payload))
            return {
                "protocol": "forest.runtime-graph-replacement-status/1",
                "state": "FAILED",
                "error_code": "COMPONENT_SELECTION_CHANGED",
            }

    context = SimpleNamespace(
        queries=Queries(), services=Services(),
        operations=SimpleNamespace(stable=lambda _name: "read-selection-status"),
    )
    status = await object.__new__(ComponentWorkerHost).selection_application_status(
        {"request_id": "selection:" + "a" * 32}, context
    )
    assert status["protocol"] == "forest.component-selection-application-status/1"
    assert status["state"] == "SUBMITTED"
    assert status["kernel"]["state"] == "FAILED"
    assert requested == [(
        "runtime.graph-replacement.status",
        {
            "protocol": "forest.runtime-graph-replacement-status-request/1",
            "operation_id": "component-selection:selection:" + "a" * 32,
        },
    )]


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


class RecordingModelClient(FakeModelClient):
    requests = []
    first_gate = None
    reply_text = None
    reply_factory = None
    not_sent_attempts = 0

    async def dispatch(self, prepared, *, permitcheck):
        await permitcheck(prepared)
        first = not type(self).requests
        type(self).requests.append(prepared)
        if first and type(self).first_gate is not None:
            await type(self).first_gate.wait()
        if len(type(self).requests) <= type(self).not_sent_attempts:
            return ModelResult(
                "NOT_SENT", prepared.request_id, prepared.transport_attempt_id,
                prepared.payload_digest, error_code="TEST_MODEL_NOT_SENT",
            )
        reply_text = (
            type(self).reply_factory(prepared, len(type(self).requests))
            if type(self).reply_factory is not None
            else type(self).reply_text
        )
        if reply_text is not None:
            return ModelResult(
                "RESPONDED", prepared.request_id, prepared.transport_attempt_id,
                prepared.payload_digest, text=reply_text,
                usage_json='{"total_tokens":12}', finish_reason="stop",
                provider_response_id="fake-provider-response",
                provider_model="test-model",
            )
        return ModelResult(
            "UNKNOWN", prepared.request_id, prepared.transport_attempt_id,
            prepared.payload_digest, error_code="TEST_MODEL_RESULT_UNKNOWN",
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
        return CapturedImage(b"fake-png-bytes", "image/png", 2, 3, 6)


class ValidPngMediaClient(FakeMediaClient):
    async def fetch(self, locator):
        capture_delay = float(os.environ.get("FOREST_TEST_MEDIA_CAPTURE_DELAY_SECONDS", "0"))
        if capture_delay:
            await asyncio.sleep(capture_delay)
        self.fetched.append(locator)
        image = Image.new("RGB", (64, 64), (33, 91, 147))
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return CapturedImage(buffer.getvalue(), "image/png", 64, 64, 4096)


class LargePngMediaClient(FakeMediaClient):
    _capture = None

    async def fetch(self, locator):
        self.fetched.append(locator)
        if type(self)._capture is None:
            width, height = 1600, 1200
            pixels = hashlib.shake_256(b"forest-real-vl-large-image").digest(
                width * height * 3
            )
            image = Image.frombytes("RGB", (width, height), pixels)
            buffer = io.BytesIO()
            image.save(buffer, format="PNG")
            content = buffer.getvalue()
            assert 4 * 1024 * 1024 < len(content) <= 8 * 1024 * 1024
            type(self)._capture = CapturedImage(content, "image/png", width, height, width * height)
        return type(self)._capture


def qiyu_embedding_config(path: str) -> dict[str, str]:
    wanted = {
        "EMBEDDING_PROVIDER", "EMBEDDING_MODEL", "EMBEDDING_DIMENSION",
        "EMBEDDING_API_KEY", "EMBEDDING_BASE_URL",
    }
    values = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        key = key.strip()
        if separator and key in wanted:
            values[key] = value.strip().strip('"\'')
    assert values.get("EMBEDDING_PROVIDER") == "siliconflow"
    assert values.get("EMBEDDING_MODEL") == "Qwen/Qwen3-VL-Embedding-8B"
    assert values.get("EMBEDDING_DIMENSION") == "4096"
    assert values.get("EMBEDDING_API_KEY")
    assert values.get("EMBEDDING_BASE_URL", "").startswith("https://")
    return values


class FakeMilvusClient:
    search_calls = 0
    fail_search_at = None
    collections = {}
    fail_after_upsert_once = False
    upsert_ack_losses = 0
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
            "fields": deepcopy(schema.fields),
            "rows": {},
        }

    async def describe_collection(self, *, collection_name):
        collection = self.collections[collection_name]
        return {
            "fields": [
                {"name": field["name"], **(
                    {"params": {"dim": field["dim"]}}
                    if field["name"] == "embedding" else {}
                )}
                for field in collection["fields"]
            ]
        }

    async def upsert(self, *, collection_name, data, **_values):
        rows = self.collections[collection_name]["rows"]
        for row in data:
            rows[row["ref_id"]] = deepcopy(row)
        if type(self).fail_after_upsert_once:
            type(self).fail_after_upsert_once = False
            type(self).upsert_ack_losses += 1
            raise OSError("injected vector upsert acknowledgement loss")
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
        self.sent = []

    async def send(self, prepared, *, timeout=None):
        self.sent.append(prepared)
        return SendResult("SENT", message_id=str(80000 + len(self.sent)))

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
    host = object.__new__(BotHost)
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
    host = object.__new__(BotHost)
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
        states = {"kernel.runtime": ModuleLifecycleState.ACTIVE}
        descriptors = ()

        async def drain_owned_work_for_replacement(
            self, operation_id, *, components, candidate_components
        ):
            assert operation_id == "replacement/one"
            assert components is None
            assert candidate_components == {}
            events.append("drain-previous")

        async def close(self):
            events.append("close-previous")
            return []

    class Candidate:
        activation_id = "replacement/one"
        states = {"kernel.runtime": ModuleLifecycleState.ACTIVE}
        descriptors = ()

        def prepared_replacement_facts(self):
            return {}

        async def prewarm(self):
            events.append("prewarm-candidate")

        async def start(self):
            events.append("publish-candidate")

        def accept_retained_ownership(self):
            pass

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
    host = object.__new__(BotHost)
    host._replacement_lock = asyncio.Lock()
    host._closing = False
    host.registry = previous
    host.selection_reader = FakeSelectionReader()
    host._admit_worker_capacity = AsyncMock()

    async def assemble():
        events.append("assemble-candidate")
        return BotGraphCandidate(candidate, _TEST_SELECTION, (), host.selection_reader)

    host._assemble_candidate = assemble
    with pytest.raises(ForestError) as stale_selection:
        await host.replace_runtime_graph(
            selection_revision=1, selection_digest="sha256:" + "1" * 64
        )
    assert stale_selection.value.code == "COMPONENT_SELECTION_CHANGED"
    assert events == ["assemble-candidate"]
    events.clear()
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

    host = object.__new__(BotHost)
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
                "target_component_id": None,
                "selection_revision": None,
                "selection_digest": None,
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
        states = {"kernel.runtime": ModuleLifecycleState.ACTIVE}
        descriptors = ()

        async def drain_owned_work_for_replacement(
            self, operation_id, *, components, candidate_components
        ):
            assert operation_id == "replacement/failing"
            assert components is None
            assert candidate_components == {}
            events.append("drain-previous")

        async def resume_owned_work_after_failed_replacement(self):
            events.append("resume-previous")

    class Candidate:
        activation_id = "replacement/failing"
        states = {"kernel.runtime": ModuleLifecycleState.ACTIVE}
        descriptors = ()

        def prepared_replacement_facts(self):
            return {}

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
    host = object.__new__(BotHost)
    host._replacement_lock = asyncio.Lock()
    host._closing = False
    host.registry = previous
    host.selection_reader = FakeSelectionReader()
    host._admit_worker_capacity = AsyncMock()

    async def assemble():
        return BotGraphCandidate(candidate, _TEST_SELECTION, (), host.selection_reader)

    host._assemble_candidate = assemble
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
async def test_runtime_replacement_fails_host_after_publication_and_never_resumes_old_graph():
    events = []

    class Previous:
        states = {"kernel.runtime": ModuleLifecycleState.ACTIVE}
        descriptors = ()

        async def drain_owned_work_for_replacement(
            self, _operation_id, *, components, candidate_components
        ):
            assert components is None
            assert candidate_components == {}
            events.append("drain-previous")

        async def resume_owned_work_after_failed_replacement(self):
            events.append("resume-previous")

    class Candidate:
        activation_id = "replacement/activation-failure"
        publication_committed = False
        states = {"kernel.runtime": ModuleLifecycleState.ACTIVE}
        descriptors = ()

        def prepared_replacement_facts(self):
            return {}

        async def prewarm(self):
            events.append("prewarm-candidate")

        async def start(self):
            self.publication_committed = True
            events.append("activate-candidate")
            raise RuntimeError("local activation failed")

        async def close(self):
            events.append("close-candidate")
            return []

    host = object.__new__(BotHost)
    host._replacement_lock = asyncio.Lock()
    host._closing = False
    host._fatal_replacement_error = None
    host.registry = Previous()
    host.selection_reader = FakeSelectionReader()
    host._admit_worker_capacity = AsyncMock()
    candidate = Candidate()

    async def assemble():
        return BotGraphCandidate(candidate, _TEST_SELECTION, (), host.selection_reader)

    host._assemble_candidate = assemble
    with pytest.raises(ForestError) as raised:
        await host.replace_runtime_graph()

    assert raised.value.code == "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE"
    assert host._fatal_replacement_error is raised.value
    assert events == ["prewarm-candidate", "drain-previous", "activate-candidate", "close-candidate"]


@pytest.mark.asyncio
async def test_graph_replacement_worker_records_post_publication_failure():
    events = []

    class Runtime:
        async def claim_graph_replacement(self, _lease, *, lease_seconds):
            assert lease_seconds == 120
            return {"operation_id": "replacement/fatal"}

        async def fail_graph_replacement(self, _lease, *, operation_id, error_code):
            events.append((operation_id, error_code))

        async def graph_for_activation(self, _activation_id):
            raise AssertionError("Published activation must not be settled as success")

    host = object.__new__(BotHost)
    host._closing = False
    host._kernel = SimpleNamespace(runtime=Runtime())
    host._authority = lambda: "lease/one"

    async def execute(_request):
        raise ForestError("RUNTIME_GRAPH_POST_PUBLICATION_FAILURE", "activation failed", 503)

    host._execute_graph_replacement = execute
    with pytest.raises(ForestError) as raised:
        await host._run_graph_replacements()

    assert raised.value.code == "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE"
    assert events == [("replacement/fatal", "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE")]


@pytest.mark.postgres
async def test_worker_host_shell_reload_preserves_unaffected_generations(tmp_path, monkeypatch):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_reload_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    settings = BotSettings(
        bot_id="bot-worker-host-reload-test", dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="test-model-key",
        model="test-model", model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024, model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B", embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609",), group_trigger_mode="MENTION",
        private_enabled=True, shell_administrator_ids=("42",),
        agent_system_prompt="Worker Host reload fixture.",
    )

    async def create_websocket(_url, _token, **_limits):
        return FakeWebSocketClient(settings.onebot_self_id)

    package_catalog = PackageCatalog(tmp_path / "reload-packages")

    async def read_package_registry(query_id, payload):
        if query_id == "packages":
            return await asyncio.to_thread(package_catalog.page, **payload)
        if query_id == "artifact":
            return await asyncio.to_thread(
                package_catalog.artifact, payload["package_digest"]
            )
        raise AssertionError(f"Unexpected package catalog query: {query_id}")

    host = BotHost(
        settings,
        model_client_factory=FakeModelClient,
        embedding_client_factory=FakeEmbeddingClient,
        media_client_factory=FakeMediaClient,
        vector_index_client_factory=FakeMilvusClient,
        websocket_client_factory=create_websocket,
        package_catalog=package_catalog,
        package_registry_reader=read_package_registry,
    )
    try:
        await host.start()
        try:
            previous = host.registry.graph_identity
            affected = set(host.registry.replacement_closure("agent.runtime"))
            assert affected < set(previous.generations)
            worker_host = host.registry.get("component.worker-host")
            worker_host_artifact = worker_host.snapshot
            artifact = await worker_host.management_contract.query(
                "artifact", {"package_digest": worker_host_artifact.digest}
            )
            assert artifact["component_id"] == "component.worker-host"
            assert artifact["package_digest"] == worker_host_artifact.digest
            assert artifact["manifest_digest"] == digest_json(worker_host_artifact.manifest)
            assert any(
                item["name"] == "deployment.package-registry.artifact"
                for item in artifact["requires"]
            )
            selection_before_preview = await worker_host.management_contract.query(
                "selection", {}
            )
            validation_input = {
                    "component_id": "agent.runtime",
                    "source_kind": "builtin",
                    "package_digest": None,
                    "manifest_digest": None,
                    "approved_capabilities": [],
                    "desired_enabled": True,
                    "expected_selection_revision": selection_before_preview["revision"],
                    "expected_graph_revision": previous.graph_revision,
                    "expected_configuration_revision": None,
            }
            preview = await asyncio.wait_for(
                worker_host.management_contract.query("validate", validation_input),
                timeout=50,
            )
            assert preview["protocol"] == "forest.component-candidate-preview/1"
            assert preview["target_component_id"] == "agent.runtime"
            assert "agent.runtime" in preview["impact"]["replace"]
            assert preview["plan_digest"].startswith("sha256:")
            agent_package = next(
                item for item in preview["packages"]
                if item["component_id"] == "agent.runtime"
            )
            assert agent_package["worker_limits"] == WorkerLimits.from_component(
                host.package_catalog.get(agent_package["package_digest"]).component
            ).as_fact()
            capacity = preview["worker_capacity"]
            admission = preview["admission"]
            assert admission["protocol"] == "forest.worker-capacity-admission/1"
            assert admission["policy_revision"] == 1
            assert admission["peak"]["max_workers"] == (
                len(capacity["active"]) + len(capacity["preparing"])
            )
            assert all(
                admission["peak"][key] <= limit
                for key, limit in admission["budget"].items()
            )
            assert len(capacity["active"]) == sum(
                item.worker_limits is not None for item in host.registry.descriptors
            )
            assert {item["component_id"] for item in capacity["preparing"]} == set(
                preview["impact"]["prepare"]
            ) & {item["component_id"] for item in preview["packages"]}
            assert any(
                item["component_id"] == "agent.runtime"
                and item["limits"] == agent_package["worker_limits"]
                for item in capacity["preparing"]
            )
            assert (await worker_host.management_contract.query("selection", {})) == (
                selection_before_preview
            )
            assert host.registry.graph_identity == previous
            selection_preflight = host.resolve_management_action_preflight(
                "runtime.selection-commit",
                component_id="component.worker-host",
                package_digest=worker_host_artifact.digest,
                trust_level="official",
            )
            assert selection_preflight is not None
            command_input = dict(validation_input)
            command_input.update({
                "plan_digest": preview["plan_digest"],
                "requested_by": "subject/host-admin",
            })
            async with selection_preflight(
                command_input, operation_id="selection/preflight"
            ) as verify_in:
                async with host.store.pool.connection() as conn, conn.transaction():
                    await verify_in(conn)
            async with selection_preflight(
                {**command_input, "plan_digest": "sha256:" + "0" * 64},
                operation_id="selection/stale-plan",
            ) as verify_in:
                async with host.store.pool.connection() as conn, conn.transaction():
                    with pytest.raises(ForestError) as stale_plan:
                        await verify_in(conn)
            assert stale_plan.value.code == "COMPONENT_SELECTION_PLAN_CHANGED"
            assert (await worker_host.management_contract.query("selection", {})) == (
                selection_before_preview
            )
            web_request = await worker_host.management_contract.action(
                "module.reload",
                operation_id="web:reload-missing",
                payload={
                    "component_id": "module.missing",
                    "expected_graph_revision": previous.graph_revision,
                },
                authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            assert web_request["state"] == "QUEUED"
            assert web_request["requested_by"] == "subject/host-admin"
            async with asyncio.timeout(20):
                while True:
                    web_status = await worker_host.management_contract.query(
                        "reload", {"request_id": web_request["request_id"]}
                    )
                    if web_status["state"] == "FAILED":
                        break
                    await asyncio.sleep(0.1)
            assert web_status["error_code"] == "COMPONENT_HOST_MODULE_NOT_FOUND"
            assert web_status["kernel"] is None
            handle = next(
                handle for key, handle in host.registry.contract_router._requests.items()
                if key[0] == "shell.command" and key[1] == "shell.component-host.reload"
            )

            async def reload(component_id, revision, operation_id, *, route=handle):
                return await route.invoke(
                    ShellCommandExecutionRequest(
                        command_id="module.reload",
                        command_version=1,
                        arguments={
                            "component_id": component_id,
                            "expected_graph_revision": revision,
                        },
                        request_id=operation_id,
                        principal_ref="qq:42",
                        conversation_ref="onebot:group:933179609",
                    ),
                    operation_id=operation_id,
                    causal_root_kind="shell.request",
                    causal_root_id=operation_id,
                    timeout_seconds=20,
                )

            with pytest.raises(ForestError) as missing:
                await reload("module.missing", previous.graph_revision, "reload/missing")
            assert missing.value.code == "COMPONENT_HOST_MODULE_NOT_FOUND"
            with pytest.raises(ForestError) as stale:
                await reload("agent.runtime", previous.graph_revision + 1, "reload/stale")
            assert stale.value.code == "RUNTIME_GRAPH_REVISION_CONFLICT"
            result = await reload("agent.runtime", previous.graph_revision, "reload/valid")
            assert "agent.runtime" in result.text
            async with asyncio.timeout(180):
                while host.registry.graph_identity.graph_id == previous.graph_id:
                    latest = (await host._kernel.runtime.status())["latest_replacement"]
                    if latest is not None and latest["state"] == "FAILED":
                        pytest.fail(f"Graph replacement failed: {latest}")
                    await asyncio.sleep(0.1)
                while (await host._kernel.runtime.status())["latest_replacement"]["state"] != "SUCCEEDED":
                    await asyncio.sleep(0.1)
            replacement = host.registry.graph_identity
            assert replacement.graph_revision == previous.graph_revision + 1
            assert set(replacement.generations) == set(previous.generations)
            assert all(
                (replacement.generations[component_id] != previous.generations[component_id])
                == (component_id in affected)
                for component_id in previous.generations
            )
            request_id = "reload:" + result.text.split("reload:", 1)[1][:32]
            new_worker_host = host.registry.get("component.worker-host")
            status = await new_worker_host.management_contract.query(
                "reload", {"request_id": request_id}
            )
            assert status["state"] == "SUBMITTED"
            assert status["kernel"]["state"] == "SUCCEEDED"
            assert status["kernel"]["graph_revision"] == replacement.graph_revision
            current_handle = (
                next(
                    route for key, route in host.registry.contract_router._requests.items()
                    if key[0] == "shell.command" and key[1] == "shell.component-host.reload"
                )
                if "shell.command" in affected
                else handle
            )
            replay = await reload(
                "agent.runtime", previous.graph_revision, "reload/valid", route=current_handle
            )
            assert f"Graph r{previous.graph_revision}" in replay.text
            assert host.registry.graph_identity.graph_id == replacement.graph_id
            agent_configuration = host.registry.get("agent.runtime").module_configuration
            current_configuration = await agent_configuration.desired(include_value=True)
            configuration_plan = await agent_configuration.plan(
                operation_id="web:configuration-plan/atomic-test",
                expected_desired_revision=current_configuration.revision,
                value=current_configuration.value,
                validation_digest=current_configuration.validation_digest,
            )
            configuration_scope = ComponentConfigurationApplyScope(
                component_id="agent.runtime",
                generation_id=replacement.generations["agent.runtime"],
                call_id="call:test/configuration",
                operation_id="web:configuration-apply/atomic-test",
            )
            actor = SimpleNamespace(
                target_graph_revision=replacement.graph_revision,
                web_subject_ref="subject/host-admin",
            )

            async def reject_control(*_args, **_kwargs):
                raise ForestError("TEST_CONTROL_REJECTED", "Control participant rejected", 409)

            control = SimpleNamespace(
                component_id="component.worker-host",
                generation_id=replacement.generations["component.worker-host"],
                invoke_in=reject_control,
            )
            rejected_coordinator = PostgresComponentConfigurationApplyCoordinator(
                host._kernel.runtime.store,
                host._kernel.runtime,
                component_id="agent.runtime",
                configuration=agent_configuration,
                control_handle=SimpleNamespace(resolve=lambda: control),
            )
            apply_args = dict(
                operation_id="web:configuration-apply/atomic-test",
                plan_id=configuration_plan["plan_id"],
                expected_desired_revision=current_configuration.revision,
                authorization=actor,
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            with pytest.raises(ForestError) as rejected:
                await rejected_coordinator.execute(configuration_scope, **apply_args)
            assert rejected.value.code == "TEST_CONTROL_REJECTED"
            assert (await agent_configuration.desired()).revision == current_configuration.revision
            gateway = ManagementGateway(
                host,
                ticket_authority=WebAuthorityTicketAuthority(
                    issuer_id="deployment/configuration-test",
                    active_key_id="master/configuration-test/1",
                    key_resolver=lambda _key_id: b"k" * 32,
                ),
            )
            body = ConfigurationApplyInput(
                plan_id=configuration_plan["plan_id"],
                expected_desired_revision=current_configuration.revision,
            )
            web_ticket = await gateway.ticket_authority.issue(
                session=AuthenticatedWebSession(
                    "token-digest", "session/configuration-test", 1, "csrf-digest",
                    WebSubject("subject/host-admin", "admin", "Administrator", "ADMIN", True, 1),
                    datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                target_scope_kind="bot",
                target_scope_id=settings.bot_id,
                target_component_id="agent.runtime",
                target_generation_id=replacement.generations["agent.runtime"],
                target_graph_revision=replacement.graph_revision,
                target_operation_ref=apply_args["operation_id"],
                action_key="configuration.apply",
                input_digest=digest_json(body.model_dump(mode="json")),
                plan_digest=digest_json({"plan_id": body.plan_id}),
            )
            committed = await gateway.apply_configuration(
                "agent.runtime",
                body=body,
                idempotency_key="atomic-test",
                authorization=web_ticket,
                operation_id=apply_args["operation_id"],
            )
            assert committed["configuration"]["revision"] == current_configuration.revision + 1
            assert committed["rebuild"]["target_component_id"] == "agent.runtime"
            request_id = committed["rebuild"]["request_id"]
            async with asyncio.timeout(180):
                while True:
                    try:
                        rebuild = await host.registry.get(
                            "component.worker-host"
                        ).management_contract.query(
                            "configuration-rebuild", {"request_id": request_id}
                        )
                    except ForestError as error:
                        if error.status != 409:
                            raise
                        await asyncio.sleep(0.2)
                        continue
                    if rebuild["state"] in ("EFFECTIVE", "REBUILD_FAILED"):
                        break
                    await asyncio.sleep(0.2)
            assert rebuild["state"] == "EFFECTIVE", rebuild
            assert host.registry.graph_identity.graph_revision > replacement.graph_revision
            active_graph = host.registry.graph_identity
            replay_ticket = await gateway.ticket_authority.issue(
                session=AuthenticatedWebSession(
                    "token-digest", "session/configuration-test", 1, "csrf-digest",
                    WebSubject("subject/host-admin", "admin", "Administrator", "ADMIN", True, 1),
                    datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                target_scope_kind="bot",
                target_scope_id=settings.bot_id,
                target_component_id="agent.runtime",
                target_generation_id=active_graph.generations["agent.runtime"],
                target_graph_revision=active_graph.graph_revision,
                target_operation_ref=apply_args["operation_id"],
                action_key="configuration.apply",
                input_digest=digest_json(body.model_dump(mode="json")),
                plan_digest=digest_json({"plan_id": body.plan_id}),
            )
            replayed = await gateway.apply_configuration(
                "agent.runtime",
                body=body,
                idempotency_key="atomic-test",
                authorization=replay_ticket,
                operation_id=apply_args["operation_id"],
            )
            assert replayed["configuration"] == committed["configuration"]
            assert replayed["rebuild"]["request_id"] == request_id
            assert host.registry.graph_identity.graph_id == active_graph.graph_id
            authority = host.registry.get("kernel.authorization")
            policy = await authority.management_contract.read()
            policy_plan = await authority.management_contract.plan(
                operation_id="web:policy-plan/atomic-test",
                expected_desired_revision=policy["desired"]["revision"],
                value=policy["desired"]["value"],
            )
            policy_body = ConfigurationApplyInput(
                plan_id=policy_plan["plan_id"],
                expected_desired_revision=policy["desired"]["revision"],
            )
            policy_operation = "web:policy-apply/atomic-test"
            policy_ticket = await gateway.ticket_authority.issue(
                session=AuthenticatedWebSession(
                    "token-digest", "session/configuration-test", 1, "csrf-digest",
                    WebSubject("subject/host-admin", "admin", "Administrator", "ADMIN", True, 1),
                    datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                target_scope_kind="bot",
                target_scope_id=settings.bot_id,
                target_component_id="kernel.authorization",
                target_generation_id=active_graph.generations["kernel.authorization"],
                target_graph_revision=active_graph.graph_revision,
                target_operation_ref=policy_operation,
                action_key="configuration.apply",
                input_digest=digest_json(policy_body.model_dump(mode="json")),
                plan_digest=digest_json({"plan_id": policy_body.plan_id}),
            )
            policy_committed = await gateway.apply_configuration(
                "kernel.authorization",
                body=policy_body,
                idempotency_key="policy-atomic-test",
                authorization=policy_ticket,
                operation_id=policy_operation,
            )
            assert policy_committed["configuration"]["revision"] == policy["desired"]["revision"] + 1
            assert policy_committed["rebuild"]["target_component_id"] == "kernel.authorization"
            policy_request_id = policy_committed["rebuild"]["request_id"]
            async with asyncio.timeout(180):
                while True:
                    try:
                        policy_rebuild = await host.registry.get(
                            "component.worker-host"
                        ).management_contract.query(
                            "configuration-rebuild", {"request_id": policy_request_id}
                        )
                    except ForestError as error:
                        if error.status != 409:
                            raise
                        await asyncio.sleep(0.2)
                        continue
                    if policy_rebuild["state"] in ("EFFECTIVE", "REBUILD_FAILED"):
                        break
                    await asyncio.sleep(0.2)
            assert policy_rebuild["state"] == "EFFECTIVE", policy_rebuild
            assert host.registry.graph_identity.graph_revision > active_graph.graph_revision

            source_catalog = PackageCatalog(tmp_path / "counter-source")
            source = source_catalog.build(
                Path(__file__).parents[1] / "examples/counter"
            )
            counter = host.package_catalog.import_archive(
                source_catalog.export_archive(source.digest)
            )
            await bootstrap_installed_package(
                counter.digest, host.package_catalog, host.store,
                host.schema_bundle_installer, kernel=host._kernel,
                initial_configuration={},
                approved_capabilities=("state.read", "state.write"),
            )
            async def select(function, operation_id, selection_revision, graph_revision):
                payload = {
                    "component_id": "example.counter", "source_kind": "installed",
                    "package_digest": counter.digest,
                    "manifest_digest": digest_json(counter.manifest),
                    "approved_capabilities": ["state.read", "state.write"],
                    "desired_enabled": function == "enable",
                    "expected_configuration_revision": (
                        1 if function == "enable" else None
                    ),
                    "expected_selection_revision": selection_revision,
                    "expected_graph_revision": graph_revision,
                }
                contract = host.registry.get("component.worker-host").management_contract
                preview = await contract.query("validate", payload)
                payload["plan_digest"] = preview["plan_digest"]
                result = await contract.action(
                    "module." + function,
                    operation_id=operation_id,
                    payload=payload,
                    authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                    authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
                )
                assert result["desired_enabled"] is (function == "enable")
                assert result["request_id"].startswith("selection:")
                return result

            before_add = host.registry.graph_identity
            enabled = await select(
                "enable", "selection/enable-counter", 0, before_add.graph_revision
            )
            assert enabled["selection_revision"] == 1

            async def await_counter_membership(expected):
                deadline = asyncio.get_running_loop().time() + 120
                while asyncio.get_running_loop().time() < deadline:
                    identity = host.registry.graph_identity
                    if ("example.counter" in identity.generations) is expected:
                        return identity
                    await asyncio.sleep(0.25)
                raise AssertionError(
                    f"Selection task did not publish expected membership: "
                    f"{await host._kernel.runtime.status()}"
                )

            with_counter = await await_counter_membership(True)
            assert "example.counter" in with_counter.generations
            worker_host = host.registry.get("component.worker-host")
            selected = await worker_host.management_contract.query("selection", {})
            assert selected["revision"] == 1
            assert selected["entries"][0]["component_id"] == "example.counter"
            assert selected["entries"][0]["desired_enabled"] is True
            async def await_selection_settled(receipt):
                async with asyncio.timeout(20):
                    while True:
                        application = await worker_host.management_contract.query(
                            "selection-application", {"request_id": receipt["request_id"]}
                        )
                        if application["kernel"] is not None and application["kernel"][
                            "state"
                        ] == "SUCCEEDED":
                            return application
                        await asyncio.sleep(0.1)

            application = await await_selection_settled(enabled)
            assert application["state"] == "SUBMITTED"
            assert application["kernel"]["graph_revision"] == with_counter.graph_revision
            assert all(
                with_counter.generations[component_id] == generation_id
                for component_id, generation_id in before_add.generations.items()
            )
            old_counter = host.registry.get("example.counter")
            disabled = await select(
                "disable", "selection/disable-counter", 1, with_counter.graph_revision
            )
            assert disabled["selection_revision"] == 2
            without_counter = await await_counter_membership(False)
            application = await await_selection_settled(disabled)
            assert application["kernel"]["graph_revision"] == without_counter.graph_revision
            selected = await worker_host.management_contract.query("selection", {})
            assert selected["revision"] == 2
            assert selected["entries"][0]["desired_enabled"] is False
            assert "example.counter" not in without_counter.generations
            assert old_counter.state.value == "STOPPED"
            assert all(
                without_counter.generations[component_id] == generation_id
                for component_id, generation_id in before_add.generations.items()
            )
            worker_host = host.registry.get("component.worker-host")
            selection = await worker_host.management_contract.query("selection", {})
            proposal = {
                "component_id": "agent.runtime",
                "source_kind": "builtin",
                "package_digest": None,
                "manifest_digest": None,
                "approved_capabilities": [],
                "desired_enabled": True,
                "expected_selection_revision": selection["revision"],
                "expected_graph_revision": without_counter.graph_revision,
                "expected_configuration_revision": None,
            }
            accepted = await worker_host.management_contract.query("validate", proposal)
            runtime_config = host._kernel.configurations["kernel.runtime"]
            current_policy = await runtime_config.desired(include_value=True)
            low_policy = deepcopy(current_policy.value)
            low_policy["worker_budget"]["max_workers"] = len(
                accepted["worker_capacity"]["active"]
            )
            runtime_authority = host.registry.get("kernel.runtime")
            invalid_plan = await runtime_authority.management_contract.plan(
                operation_id="web:capacity-plan-invalid",
                expected_desired_revision=current_policy.revision,
                value=low_policy,
            )
            invalid_body = ConfigurationApplyInput(
                plan_id=invalid_plan["plan_id"],
                expected_desired_revision=current_policy.revision,
            )
            invalid_ticket = await gateway.ticket_authority.issue(
                session=AuthenticatedWebSession(
                    "token-digest", "session/capacity-test", 1, "csrf-digest",
                    WebSubject("subject/host-admin", "admin", "Administrator", "ADMIN", True, 1),
                    datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                target_scope_kind="bot",
                target_scope_id=settings.bot_id,
                target_component_id="kernel.runtime",
                target_generation_id=without_counter.generations["kernel.runtime"],
                target_graph_revision=without_counter.graph_revision,
                target_operation_ref="web:capacity-apply-invalid",
                action_key="configuration.apply",
                input_digest=digest_json(invalid_body.model_dump(mode="json")),
                plan_digest=digest_json({"plan_id": invalid_body.plan_id}),
            )
            with pytest.raises(ForestError) as rejected_policy_apply:
                await gateway.apply_configuration(
                    "kernel.runtime",
                    body=invalid_body,
                    idempotency_key="capacity-invalid",
                    authorization=invalid_ticket,
                    operation_id="web:capacity-apply-invalid",
                )
            assert rejected_policy_apply.value.code == "KERNEL_WORKER_CAPACITY_EXCEEDED"
            assert (await runtime_config.desired()).revision == current_policy.revision
            policy_plan = await runtime_config.plan(
                operation_id="fixture:capacity-plan",
                expected_desired_revision=current_policy.revision,
                value=low_policy,
                validation_digest=digest_json(KERNEL_RESOURCE_POLICY_SCHEMA),
            )
            await runtime_config.apply(
                operation_id="fixture:capacity-apply",
                plan_id=policy_plan["plan_id"],
                expected_desired_revision=current_policy.revision,
            )
            with pytest.raises(ForestError) as rejected_preview:
                await worker_host.management_contract.query("validate", proposal)
            assert rejected_preview.value.code == "KERNEL_WORKER_CAPACITY_EXCEEDED"
            preflight = host.resolve_management_action_preflight(
                "runtime.selection-commit",
                component_id="component.worker-host",
                package_digest=worker_host.snapshot.digest,
                trust_level="official",
            )
            async with preflight(
                {**proposal, "plan_digest": accepted["plan_digest"]},
                operation_id="selection/capacity-rejected",
            ) as verify_in:
                async with host.store.pool.connection() as conn, conn.transaction():
                    with pytest.raises(ForestError) as rejected_commit:
                        await verify_in(conn)
            assert rejected_commit.value.code == "KERNEL_WORKER_CAPACITY_EXCEEDED"
            assert await worker_host.management_contract.query("selection", {}) == selection
        finally:
            await host.close()
    finally:
        assert name.startswith("forest_reload_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_ready_bot_rejects_missing_official_config_before_package_install(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_ready_config_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))

    async def resolve_secret(_reference, _purpose):
        return "test-key"

    host = BotHost(
        BotRuntimeSettings(bot_id="bot-ready-config", dsn=uri),
        credential_resolver=resolve_secret,
        package_catalog=PackageCatalog(tmp_path / "ready-config-packages"),
    )
    try:
        with pytest.raises(ForestError) as rejected:
            await host.start()
        assert rejected.value.code == "PACKAGE_CONFIGURATION_REQUIRED"
        async with await psycopg.AsyncConnection.connect(uri) as conn:
            row = await (
                await conn.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname=%s",
                    (module_schema_namespace("forest.component.worker-host"),),
                )
            ).fetchone()
            assert row is None
    finally:
        await host.close()
        assert name.startswith("forest_ready_config_") and len(name) == 52
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_embedding_configuration_can_rotate_its_secret_reference(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_secret_rotation_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    settings = BotSettings(
        bot_id="bot-secret-rotation", dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="",
        model="test-model", model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024, model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B", embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="", allowed_group_ids=("933179609",),
        group_trigger_mode="MENTION", private_enabled=True,
        shell_administrator_ids=("42",), agent_system_prompt="Secret rotation fixture.",
        bootstrap_package_digests=(("embedding.multimodal", "sha256:" + "f" * 64),),
    )
    rotated_ref = "secret://embedding.multimodal/rotated"
    resolved = []

    async def resolve_secret(secret_ref, purpose):
        resolved.append((secret_ref, purpose))
        if purpose == "embedding.multimodal.credential" and secret_ref == rotated_ref:
            return "new-embedding-key"
        return {
            (settings.model_credential_ref, "provider.chat-completions.credential"):
                "old-model-key",
            (settings.embedding_credential_ref, "embedding.multimodal.credential"):
                "old-embedding-key",
            (settings.onebot_secret_ref, "adapter.onebot.access-token"):
                "old-onebot-token",
        }[(secret_ref, purpose)]

    class RecordingEmbeddingClient(FakeEmbeddingClient):
        credentials = []

        def __init__(self, endpoint, credential, profile, **limits):
            super().__init__(endpoint, credential, profile, **limits)
            self.credentials.append(credential)

    async def create_websocket(_url, _token, **_limits):
        assert _token == "old-onebot-token"
        return FakeWebSocketClient(settings.onebot_self_id)

    host = BotHost(
        settings,
        credential_resolver=resolve_secret,
        model_client_factory=FakeModelClient,
        embedding_client_factory=RecordingEmbeddingClient,
        media_client_factory=FakeMediaClient,
        vector_index_client_factory=FakeMilvusClient,
        websocket_client_factory=create_websocket,
        package_catalog=PackageCatalog(tmp_path / "secret-rotation-packages"),
    )
    try:
        await host.start()
        try:
            assert RecordingEmbeddingClient.credentials[-1] == "old-embedding-key"
            graph = host.registry.graph_identity
            embedding = host.registry.get("embedding.multimodal")
            current = await embedding.management_contract.read()
            changed = {
                **current["desired"]["value"],
                "credential_ref": rotated_ref,
            }
            plan = await embedding.management_contract.plan(
                operation_id="test:embedding-secret-plan",
                expected_desired_revision=current["desired"]["revision"],
                value=changed,
            )
            committed = await embedding.management_contract.apply(
                operation_id="test:embedding-secret-apply",
                plan_id=plan["plan_id"],
                expected_desired_revision=current["desired"]["revision"],
                authorization=SimpleNamespace(
                    target_graph_revision=graph.graph_revision,
                    web_subject_ref="subject/test-admin",
                ),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            request_id = committed["rebuild"]["request_id"]
            async with asyncio.timeout(180):
                while True:
                    try:
                        result = await host.registry.get(
                            "component.worker-host"
                        ).management_contract.query(
                            "configuration-rebuild", {"request_id": request_id}
                        )
                    except ForestError as error:
                        if error.status != 409:
                            raise
                        await asyncio.sleep(0.2)
                        continue
                    if result["state"] in {"EFFECTIVE", "REBUILD_FAILED"}:
                        break
                    await asyncio.sleep(0.2)
            assert result["state"] == "EFFECTIVE", result
            assert host.registry.graph_identity.graph_revision > graph.graph_revision
            assert RecordingEmbeddingClient.credentials[-1] == "new-embedding-key"
            assert (rotated_ref, "embedding.multimodal.credential") in resolved
            effective = await host.registry.get(
                "embedding.multimodal"
            ).management_contract.read()
            assert effective["effective"]["value"]["profile"] == (
                current["effective"]["value"]["profile"]
            )
        finally:
            await host.close()
        resumed_host = BotHost(
            BotRuntimeSettings(bot_id=settings.bot_id, dsn=uri),
            credential_resolver=resolve_secret,
            model_client_factory=FakeModelClient,
            embedding_client_factory=RecordingEmbeddingClient,
            media_client_factory=FakeMediaClient,
            vector_index_client_factory=FakeMilvusClient,
            websocket_client_factory=create_websocket,
            package_catalog=PackageCatalog(tmp_path / "secret-rotation-packages"),
        )
        try:
            await resumed_host.start()
            assert RecordingEmbeddingClient.credentials[-1] == "new-embedding-key"
            resumed = await resumed_host.registry.get(
                "embedding.multimodal"
            ).management_contract.read()
            assert resumed["effective"]["value"]["credential_ref"] == rotated_ref
        finally:
            await resumed_host.close()
        pinned_provisioner = BotHost(
            settings,
            package_catalog=PackageCatalog(tmp_path / "secret-rotation-packages"),
        )
        with pytest.raises(ForestError) as rejected:
            await pinned_provisioner.provision()
        assert rejected.value.code == "BOOTSTRAP_PACKAGE_DIGEST_MISMATCH"
    finally:
        assert name.startswith("forest_secret_rotation_") and len(name) == 55
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_worker_host_can_reload_its_own_generation(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_self_reload_" + uuid4().hex
    other_name = "forest_self_reload_other_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(other_name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    other_uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + other_name, "", ""))
    settings = BotSettings(
        bot_id="bot-worker-self-reload", dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="test-model-key",
        model="test-model", model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024, model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B", embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609",), group_trigger_mode="MENTION",
        private_enabled=True, shell_administrator_ids=("42",),
        agent_system_prompt="Worker Host self-reload fixture.",
    )

    async def create_websocket(_url, _token, **_limits):
        return FakeWebSocketClient(settings.onebot_self_id)

    host = BotHost(
        settings,
        model_client_factory=FakeModelClient,
        embedding_client_factory=FakeEmbeddingClient,
        media_client_factory=FakeMediaClient,
        vector_index_client_factory=FakeMilvusClient,
        websocket_client_factory=create_websocket,
        package_catalog=PackageCatalog(tmp_path / "self-reload-packages"),
    )
    try:
        await host.start()
        try:
            before = host.registry.graph_identity
            routing_default = next(
                item for item in host._official_defaults
                if item.component["id"] == "routing.graph"
            )
            routing_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/routing_graph"
            updated_root = tmp_path / "routing-preview-version"
            copytree(routing_root, updated_root)
            manifest_path = updated_root / "module.toml"
            manifest_path.write_text(
                manifest_path.read_text(encoding="utf-8").replace(
                    f'version = "{routing_default.manifest["version"]}"',
                    'version = "99.0.0"', 1,
                ),
                encoding="utf-8",
            )
            updated = host.package_catalog.build_official(updated_root, scope="bot")
            entry = ComponentSelectionEntry(
                "routing.graph", "official", updated.digest,
                digest_json(updated.manifest), (), True, 1,
            )
            frozen = {
                "protocol": "forest.component-selection-snapshot/1",
                "revision": 1,
                "entries": [entry.as_dict()],
            }
            preview = await preview_selected_graph(
                host.registry,
                ComponentSelectionSnapshot(1, (entry,), digest_json(frozen)),
                host._official_descriptors, host.package_catalog, host.store,
                kernel=host._kernel,
                target_component_id="routing.graph",
                expected_graph_revision=before.graph_revision,
                official_defaults=host._official_defaults,
                official_effect_drivers=host._official_effect_drivers,
            )
            assert next(
                item for item in preview.packages
                if item.component_id == "routing.graph"
            ).package_digest == updated.digest
            assert "routing.graph" in preview.impact.replace
            assert host.registry.graph_identity == before
            proposal = {
                "component_id": "routing.graph", "source_kind": "official",
                "package_digest": updated.digest,
                "manifest_digest": digest_json(updated.manifest),
                "approved_capabilities": [], "desired_enabled": True,
                "expected_configuration_revision": None,
                "expected_selection_revision": 0,
                "expected_graph_revision": before.graph_revision,
            }
            contract = host.registry.get("component.worker-host").management_contract
            validated = await contract.query("validate", proposal)
            assert validated["plan_digest"] == preview.plan_digest
            selected = await contract.action(
                "module.enable",
                operation_id="selection/pin-routing-official",
                payload={**proposal, "plan_digest": validated["plan_digest"]},
                authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            assert selected["selection_revision"] == 1
            async with asyncio.timeout(180):
                while host.registry.graph_identity.graph_id == before.graph_id:
                    latest = (await host._kernel.runtime.status())["latest_replacement"]
                    if latest is not None and latest["state"] == "FAILED":
                        pytest.fail(f"Official selection replacement failed: {latest}")
                    await asyncio.sleep(0.1)
                while (await host._kernel.runtime.status())["latest_replacement"]["state"] != "SUCCEEDED":
                    await asyncio.sleep(0.1)
            pinned = host.registry.graph_identity
            assert pinned.generations["routing.graph"] != before.generations["routing.graph"]
            assert next(
                item for item in host.registry.descriptors
                if item.component_id == "routing.graph"
            ).artifact.package_digest == updated.digest
            assert pinned.generations["kernel.runtime"] == before.generations["kernel.runtime"]
            other = BotHost(
                replace(settings, bot_id="bot-worker-self-reload-other", dsn=other_uri),
                model_client_factory=FakeModelClient,
                embedding_client_factory=FakeEmbeddingClient,
                media_client_factory=FakeMediaClient,
                vector_index_client_factory=FakeMilvusClient,
                websocket_client_factory=create_websocket,
                package_catalog=PackageCatalog(tmp_path / "self-reload-packages"),
            )
            try:
                await other.start()
                other_before = other.registry.graph_identity
                other_proposal = {
                    **proposal,
                    "package_digest": routing_default.digest,
                    "manifest_digest": digest_json(routing_default.manifest),
                    "expected_graph_revision": other_before.graph_revision,
                }
                other_contract = other.registry.get(
                    "component.worker-host"
                ).management_contract
                other_validated = await other_contract.query("validate", other_proposal)
                other_selected = await other_contract.action(
                    "module.enable", operation_id="selection/pin-other-routing-official",
                    payload={**other_proposal, "plan_digest": other_validated["plan_digest"]},
                    authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                    authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
                )
                assert other_selected["selection_revision"] == 1
                async with asyncio.timeout(180):
                    while other.registry.graph_identity.graph_id == other_before.graph_id:
                        latest = (await other._kernel.runtime.status())["latest_replacement"]
                        if latest is not None and latest["state"] == "FAILED":
                            pytest.fail(f"Other Bot official selection failed: {latest}")
                        await asyncio.sleep(0.1)
                assert next(
                    item for item in other.registry.descriptors
                    if item.component_id == "routing.graph"
                ).artifact.package_digest == routing_default.digest
                assert host.registry.graph_identity == pinned
                assert next(
                    item for item in host.registry.descriptors
                    if item.component_id == "routing.graph"
                ).artifact.package_digest == updated.digest
            finally:
                await other.close()
            before = pinned
            affected = set(host.registry.replacement_closure("component.worker-host"))
            worker_host = host.registry.get("component.worker-host")
            request = await worker_host.management_contract.action(
                "module.reload",
                operation_id="web:reload-worker-host-itself",
                payload={
                    "component_id": "component.worker-host",
                    "expected_graph_revision": before.graph_revision,
                },
                authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            assert request["state"] == "QUEUED"
            async with asyncio.timeout(180):
                while host.registry.graph_identity.graph_id == before.graph_id:
                    latest = (await host._kernel.runtime.status())["latest_replacement"]
                    if latest is not None and latest["state"] == "FAILED":
                        pytest.fail(f"Worker Host self-reload failed: {latest}")
                    await asyncio.sleep(0.1)
                while (await host._kernel.runtime.status())["latest_replacement"]["state"] != "SUCCEEDED":
                    await asyncio.sleep(0.1)
            after = host.registry.graph_identity
            assert after.graph_revision == before.graph_revision + 1
            assert set(after.generations) == set(before.generations)
            assert all(
                (after.generations[component_id] != before.generations[component_id])
                == (component_id in affected)
                for component_id in before.generations
            )
            new_worker_host = host.registry.get("component.worker-host")
            saved = await new_worker_host.management_contract.query(
                "reload", {"request_id": request["request_id"]}
            )
            assert saved["state"] == "SUBMITTED"
            assert saved["kernel"]["state"] == "SUCCEEDED"
            assert saved["kernel"]["graph_revision"] == after.graph_revision
            modules = await new_worker_host.management_contract.query("modules", {})
            assert len(modules["members"]) == len(after.generations)
        finally:
            await host.close()
        restarted = BotHost(
            settings,
            model_client_factory=FakeModelClient,
            embedding_client_factory=FakeEmbeddingClient,
            media_client_factory=FakeMediaClient,
            vector_index_client_factory=FakeMilvusClient,
            websocket_client_factory=create_websocket,
            package_catalog=PackageCatalog(tmp_path / "self-reload-packages"),
        )
        try:
            await restarted.start()
            assert next(
                item for item in restarted.registry.descriptors
                if item.component_id == "routing.graph"
            ).artifact.package_digest == updated.digest
            selection = await restarted.selection_reader.snapshot()
            assert selection.entries[0].source_kind == "official"
            assert selection.entries[0].package_digest == updated.digest
            audit = next(
                item for item in restarted._official_defaults
                if item.component["id"] == "audit.console"
            )
            before_disable = restarted.registry.graph_identity
            disable = {
                "component_id": "audit.console", "source_kind": "official",
                "package_digest": audit.digest,
                "manifest_digest": digest_json(audit.manifest),
                "approved_capabilities": [], "desired_enabled": False,
                "expected_configuration_revision": None,
                "expected_selection_revision": selection.revision,
                "expected_graph_revision": before_disable.graph_revision,
            }
            contract = restarted.registry.get("component.worker-host").management_contract
            validated = await contract.query("validate", disable)
            disabled = await contract.action(
                "module.disable", operation_id="selection/disable-audit-official",
                payload={**disable, "plan_digest": validated["plan_digest"]},
                authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            assert disabled["selection_revision"] == selection.revision + 1
            async with asyncio.timeout(180):
                while restarted.registry.graph_identity.graph_id == before_disable.graph_id:
                    latest = (await restarted._kernel.runtime.status())["latest_replacement"]
                    if latest is not None and latest["state"] == "FAILED":
                        pytest.fail(f"Official disable replacement failed: {latest}")
                    await asyncio.sleep(0.1)
            assert "audit.console" not in restarted.registry.graph_identity.generations
            assert next(
                item for item in restarted.registry.descriptors
                if item.component_id == "routing.graph"
            ).artifact.package_digest == updated.digest
        finally:
            await restarted.close()
        updated.root.rename(tmp_path / "removed-pinned-routing-artifact")
        missing_artifact = BotHost(
            settings,
            model_client_factory=FakeModelClient,
            embedding_client_factory=FakeEmbeddingClient,
            media_client_factory=FakeMediaClient,
            vector_index_client_factory=FakeMilvusClient,
            websocket_client_factory=create_websocket,
            package_catalog=PackageCatalog(tmp_path / "self-reload-packages"),
        )
        with pytest.raises(ForestError) as missing:
            await missing_artifact.start()
        assert missing.value.code == "PACKAGE_CHANGED"
        assert missing_artifact.registry is None
    finally:
        assert name.startswith("forest_self_reload_") and len(name) == 51
        assert other_name.startswith("forest_self_reload_other_") and len(other_name) == 57
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(other_name)))
        await admin.close()


@pytest.mark.postgres
async def test_bot_host_starts_retires_and_restarts_exact_graph(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_host_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    settings = BotSettings(
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
        return BotHost(
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
            worker_host = host.registry.get("component.worker-host")
            with pytest.raises(ForestError) as unavailable_catalog:
                await worker_host.management_contract.query("packages", {"limit": 32})
            assert unavailable_catalog.value.code == "PACKAGE_CATALOG_UNAVAILABLE"
            agent_member = await worker_host.management_contract.query(
                "module", {"component_id": "agent.runtime"}
            )
            assert agent_member["member"]["component_id"] == "agent.runtime"
            assert agent_member["member"]["generation_id"] == (
                host.registry.graph_identity.generations["agent.runtime"]
            )
            assert agent_member["member"]["package_id"] == "forest.agent-runtime"
            assert agent_member["member"]["package_digest"] == (
                host.registry.get("agent.runtime").snapshot.digest
            )
            assert "conversation.context.prefill" in (
                host.registry.get("agent.runtime")._service_routes
            )
            assert agent_member["member"]["data_admission"] is True
            assert agent_member["member"]["control_admission"] is True
            first_modules = await worker_host.management_contract.query(
                "modules", {"limit": 3}
            )
            assert len(first_modules["members"]) == 3
            assert first_modules["next_after"] == first_modules["members"][-1][
                "component_id"
            ]
            next_modules = await worker_host.management_contract.query(
                "modules",
                {
                    "graph_id": first_modules["graph_id"],
                    "after": first_modules["next_after"],
                    "limit": 3,
                },
            )
            assert first_modules["members"][-1]["component_id"] < next_modules[
                "members"
            ][0]["component_id"]
            assert next(
                descriptor.component_id
                for descriptor in host.registry.descriptors
                if any(command.command_id == "module.list" for command in descriptor.shell_commands)
            ) == "component.worker-host"
            assert {
                command.command_id
                for descriptor in host.registry.descriptors
                if descriptor.component_id == "component.worker-host"
                for command in descriptor.shell_commands
            } >= {"core.status", "core.pause", "core.resume", "core.reload"}
            assert {
                command.command_id
                for descriptor in host.registry.descriptors
                if descriptor.component_id == "audit.console"
                for command in descriptor.shell_commands
            } == {"audit.tail"}

            async def core_command(command_id, arguments):
                service_id = {
                    "audit.tail": "shell.audit-console.execute",
                    "core.status": "shell.component-host.core-status",
                    "core.pause": "shell.component-host.core-maintenance",
                    "core.resume": "shell.component-host.core-maintenance",
                    "core.reload": "shell.component-host.core-reload",
                }[command_id]
                handle = next(
                    handle for key, handle in host.registry.contract_router._requests.items()
                    if key[0] == "shell.command" and key[1] == service_id
                )
                request_id = "core-test:" + uuid4().hex
                return await handle.invoke(
                    ShellCommandExecutionRequest(
                        command_id=command_id,
                        command_version=1,
                        arguments=arguments,
                        request_id=request_id,
                        principal_ref="qq:42",
                        conversation_ref="onebot:group:933179609",
                    ),
                    operation_id=request_id,
                    causal_root_kind="shell.request",
                    causal_root_id=request_id,
                    timeout_seconds=20,
                )

            assert "Graph r" in (await core_command("core.status", {})).text
            assert "Forest audit:" in (
                await core_command("audit.tail", {"limit": 1})
            ).text
            maintenance_epoch = (await host._kernel.runtime.status())["maintenance_epoch"]
            await core_command("core.pause", {"expected_maintenance_epoch": maintenance_epoch})
            assert (await host._kernel.runtime.status())["data_admission"] is False
            await core_command("core.resume", {"expected_maintenance_epoch": maintenance_epoch + 1})
            assert (await host._kernel.runtime.status())["data_admission"] is True
            with pytest.raises(ForestError) as core_reload_stale:
                await core_command(
                    "core.reload", {"expected_graph_revision": first_modules["graph_revision"] + 1}
                )
            assert core_reload_stale.value.code == "RUNTIME_GRAPH_REVISION_CONFLICT"
            audit_before = host.registry.graph_identity
            affected = set(host.registry.replacement_closure("audit.console"))
            assert "audit.console" in affected
            assert "kernel.audit" not in affected
            await host.replace_runtime_graph(
                target_component_id="audit.console",
                expected_graph_revision=audit_before.graph_revision,
            )
            audit_after = host.registry.graph_identity
            assert audit_after.graph_revision == audit_before.graph_revision + 1
            assert set(audit_after.generations) == set(audit_before.generations)
            assert all(
                (audit_after.generations[component_id] != audit_before.generations[component_id])
                == (component_id in affected)
                for component_id in audit_before.generations
            )
            assert "Forest audit:" in (
                await core_command("audit.tail", {"limit": 1})
            ).text
            memory_before = host.registry.graph_identity
            memory_affected = set(host.registry.replacement_closure("memory.graph"))
            assert "memory.graph" in memory_affected
            assert "audit.console" not in memory_affected
            await host.replace_runtime_graph(
                target_component_id="memory.graph",
                expected_graph_revision=memory_before.graph_revision,
            )
            memory_after = host.registry.graph_identity
            assert memory_after.graph_revision == memory_before.graph_revision + 1
            assert set(memory_after.generations) == set(memory_before.generations)
            assert all(
                (memory_after.generations[component_id] != memory_before.generations[component_id])
                == (component_id in memory_affected)
                for component_id in memory_before.generations
            )
            assert memory_after.generations["audit.console"] == memory_before.generations[
                "audit.console"
            ]
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
            try:
                async with asyncio.timeout(20):
                    while True:
                        async with host.store.pool.connection() as conn:
                            saved_domain = await (
                                await conn.execute(
                                    sql.SQL(
                                        "SELECT d.space_id,d.dimensions,d.max_batch_items,w.state "
                                        "FROM {}.memory_domains d "
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
            except TimeoutError:
                async with host.store.pool.connection() as conn:
                    bind_work = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,attempt_count,last_error_code "
                                "FROM {}.memory_domain_bind_work WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_schema)),
                            (settings.bot_id,),
                        )
                    ).fetchone()
                pytest.fail(f"Memory domain bind did not settle: {bind_work}")
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
            assert saved_domain["max_batch_items"] == 8
            embedding_instance = host.registry.get("embedding.multimodal")
            embedding_identity = embedding_instance.generation_identity
            embedding_profile = await embedding_instance.runtime_service_handlers()[
                ("embedding.profile.bind", 2)
            ].invoke_routed(
                FrozenServiceRoute(
                    service_id="embedding.profile.bind",
                    version=2,
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
            assert embedding_profile["max_batch_items"] == 8
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
                "trigger_claim": None,
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
            prior_ack_losses = FakeMilvusClient.upsert_ack_losses
            FakeMilvusClient.fail_after_upsert_once = True
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
            assert FakeMilvusClient.upsert_ack_losses == prior_ack_losses + 1
            async with host.store.pool.connection() as conn:
                replayed_publish = await (await conn.execute(
                    sql.SQL(
                        "SELECT max(attempt_count) AS attempts "
                        "FROM {}.vector_publish_batches WHERE bot_id=%s"
                    ).format(sql.Identifier(memory_namespace)),
                    (settings.bot_id,),
                )).fetchone()
            assert replayed_publish["attempts"] >= 2
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
            progress = await query_stage(query["query_id"], "progress/original", stage="progress")
            assert progress["phase"] == "NEED_MATCHING"
            assert progress["ready_count"] == progress["facet_count"] == 3
            original_matches = await query_stage(query["query_id"], "match/original", stage="match")
            assert original_matches["match_count"] == 0
            assert (await query_stage(query["query_id"], "seeds/original"))["seeds"] == []
            empty_record = await query_stage(query["query_id"], "freeze/original", stage="freeze")
            assert empty_record["result_count"] == empty_record["seed_count"] == 0
            assert (await query_stage(query["query_id"], "progress/frozen", stage="progress"))["phase"] == "NEED_MATERIALIZATION"
            empty_evidence = await query_stage(query["query_id"], "materialize/original", stage="materialize")
            assert empty_evidence["evidence_count"] == 0
            assert (await query_stage(query["query_id"], "progress/empty-ready", stage="progress"))["evidence_ref"] == empty_evidence["evidence_ref"]
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
            assert (await query_stage(recall_query["query_id"], "progress/unprepared", stage="progress"))["phase"] == "NEED_PREPARATION"
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
            progress = await query_stage(recall_query["query_id"], "progress/partial-matches", stage="progress")
            assert progress["phase"] == "NEED_MATCHING" and progress["matched_count"] == 1
            matching = await asyncio.gather(
                query_stage(recall_query["query_id"], "match/recall-one", stage="match"),
                query_stage(recall_query["query_id"], "match/recall-two", stage="match"),
            )
            assert (await query_stage(recall_query["query_id"], "progress/matched", stage="progress"))["phase"] == "NEED_FREEZING"
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
            assert region["policy_revision"] == "qiyu-graph-region/2"
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
            materialized = await asyncio.gather(
                query_stage(recall_query["query_id"], "materialize/recall-one", stage="materialize"),
                query_stage(recall_query["query_id"], "materialize/recall-two", stage="materialize"),
            )
            assert materialized[0] == materialized[1]
            assert materialized[0]["evidence_count"] == 1
            assert materialized[0]["missing_count"] == 0
            document = json.loads(await host._kernel.payload.read_bytes(materialized[0]["evidence_ref"]["payload_id"]))
            assert document["protocol"] == "forest.memory-evidence/2"
            assert document["current_source"]["external_group_id"] == "933179609"
            unit_evidence = document["evidence"][0]
            assert unit_evidence["author_subject_ref"] == "qq:20001"
            assert unit_evidence["external_group_id"] == "933179609"
            assert [part["kind"] for part in unit_evidence["parts"]] == ["text", "image", "text"]
            assert unit_evidence["parts"][0]["text"] == "before"
            assert unit_evidence["parts"][2]["text"] == "after"
            assert unit_evidence["parts"][1]["media_ref"]["payload_id"] == first_parts["parts"][1]["payload"]["payload_id"]
            assert not document["content_truncated"]
            current_group = "onebot:group:933179609"
            model_view = {
                "bot_id": settings.bot_id, "run_id": "run/host-recall-projection",
                "logical_step_id": "step/host-recall-projection",
                "input_revision": 1, "context_revision": recall_revision,
                "context_view_revision": recall_revision, "selected_input_end_seq": 2,
                "trigger_claims": [{
                    "input_batch_ref": recall_batch,
                    "trigger_claim_ref": recall_batch,
                    "trigger_claim_digest": "sha256:" + "2" * 64,
                    "trigger_count": 1,
                    "last_trigger_ref": "trigger/host-recall-projection",
                    "last_trigger_ingress_seq": 2,
                    "source_manifest_digest": "sha256:" + "1" * 64,
                }],
                "input_batch_ids": [recall_batch],
                "conversation": {
                    "conversation_id": current_group, "group_source_id": current_group,
                    "platform": "onebot-v11", "channel_kind": "group",
                    "channel_id": "933179609",
                },
                "units": [{
                    "unit_id": "context/host-recall-question", "role": "user",
                    "source": {
                        "conversation_id": current_group, "group_source_id": current_group,
                        "platform": "onebot-v11", "channel_kind": "group",
                        "channel_id": "933179609",
                    },
                    "author": {"principal_ref": "qq:20002", "identity_origin": "adapter_verified"},
                    "parts": [{"part_id": "question", "type": "text", "text": "What happened?"}],
                }],
            }
            text_projection = prepare_model_request(
                model_view, ModelCapabilities("host-text", "test-model", "host-test/1"),
                request_id="model/host-text-evidence", transport_attempt_id="attempt/host-text-evidence",
                evidence=document,
            )
            recalled_source = next(item for item in text_projection.manifest["source_manifest"]
                                   if item.get("source_unit_id") == unit_evidence["source_unit_id"])
            assert recalled_source["source"]["external_group_id"] == "933179609"
            assert recalled_source["source"]["trunk_id"] == unit_evidence["trunk_id"]
            assert recalled_source["author"]["principal_ref"] == "qq:20001"
            assert recalled_source["retrieval"]["path_edge_id"] == unit_evidence["path_edge_id"]
            assert b"image_url" not in text_projection.body
            assert any(item["reason"] == "MODEL_IMAGE_UNSUPPORTED"
                       for item in text_projection.manifest["omitted_part_refs"])
            picture = unit_evidence["parts"][1]
            picture_ref = picture["media_ref"]
            picture_bytes = await host._kernel.payload.read_bytes(picture_ref["payload_id"])
            image = MediaSnapshot(
                settings.bot_id, picture_ref["payload_id"], unit_evidence["source_revision"],
                unit_evidence["source_unit_id"], picture["part_id"],
                picture["media_type"], picture_bytes, picture_ref["digest"],
                picture["width"], picture["height"],
            )
            visual_projection = prepare_model_request(
                model_view,
                ModelCapabilities("host-visual", "test-visual-model", "host-test/1",
                                  input_modalities=("text", "image")),
                request_id="model/host-visual-evidence",
                transport_attempt_id="attempt/host-visual-evidence",
                evidence=document, media={picture_ref["payload_id"]: image},
            )
            assert b"image_url" in visual_projection.body
            assert visual_projection.manifest["media_transform_refs"][0]["source_unit_id"] == (
                unit_evidence["source_unit_id"]
            )
            assert await query_stage(recall_query["query_id"], "materialize/replay", stage="materialize") == materialized[0]
            ready_progress = await query_stage(recall_query["query_id"], "progress/ready", stage="progress")
            assert ready_progress["phase"] == "READY"
            assert ready_progress["evidence_ref"] == materialized[0]["evidence_ref"]
            assert FakeMilvusClient.search_calls == search_count

            # A management trial consumes real bound text/image assets and the
            # same owned recall chain, while retaining its separate input identity.
            trial_id = "trial/host-mixed-recall"
            trial_actor = "subject/host-admin"
            trial_png = io.BytesIO()
            Image.new("RGB", (2, 3), (12, 34, 56)).save(trial_png, format="PNG")
            trial_inputs = (
                ("part/text", "TEXT", "text/plain", b"before after"),
                ("part/image", "IMAGE", "image/png", trial_png.getvalue()),
            )
            class TrialIdentity:
                active_key_id = "master/host-trial/1"
                token = "s" * 43
                csrf = "c" * 43
                subject = WebSubject(
                    trial_actor, "admin", "Administrator", "ADMIN", True, 1
                )

                @staticmethod
                def key_resolver(key_id):
                    return b"k" * 32 if key_id == "master/host-trial/1" else None

                async def login(self, *, username, password, peer_id):
                    assert (username, password, peer_id) == (
                        "admin", "correct-password", "127.0.0.1"
                    )
                    return WebSession(
                        self.token, self.csrf, "session/host-trial", self.subject,
                        datetime.now(timezone.utc) + timedelta(hours=1),
                    )

                async def authenticate(self, token):
                    if token != self.token:
                        raise ForestError(
                            "MANAGEMENT_AUTHENTICATION_REQUIRED",
                            "Web session is invalid", 401,
                        )
                    return AuthenticatedWebSession(
                        hashlib.sha256(token.encode()).hexdigest(),
                        "session/host-trial", 1,
                        hashlib.sha256(self.csrf.encode()).hexdigest(),
                        self.subject,
                        datetime.now(timezone.utc) + timedelta(hours=1),
                    )

                async def revalidate_session(self, session):
                    if session.subject != self.subject:
                        raise ForestError(
                            "MANAGEMENT_AUTHENTICATION_REQUIRED",
                            "Web session changed", 401,
                        )

            class TrialSupervisor:
                @asynccontextmanager
                async def management_host(self, scope_id):
                    assert scope_id == settings.bot_id
                    yield host

                async def management_scopes(self):
                    return ({"scope_kind": "bot", "scope_id": settings.bot_id},)

            management_app = create_deployment_management_app(
                deployment_id="deployment/host-trial",
                identity_authority=TrialIdentity(),
                allowed_origin="http://forest.test",
                bot_supervisor=TrialSupervisor(),
            )
            trial_base = (
                f"/management/v1/scopes/bot/{settings.bot_id}/modules/memory.graph"
            )
            async with AsyncClient(
                transport=ASGITransport(app=management_app),
                base_url="http://forest.test",
            ) as web:
                login = await web.post(
                    "/management/v1/auth/login",
                    headers={"origin": "http://forest.test"},
                    json={"username": "admin", "password": "correct-password"},
                )
                assert login.status_code == 200, login.text
                headers = {
                    "origin": "http://forest.test",
                    "x-forest-csrf": TrialIdentity.csrf,
                }
                trial_parts = []
                for index, (part_id, modality, media_type, content) in enumerate(
                    trial_inputs
                ):
                    upload_id = f"upload-host-trial-{index}"
                    upload_base = f"{trial_base}/assets/trial.input/uploads"
                    opened = await web.post(
                        upload_base, headers=headers,
                        json={
                            "upload_id": upload_id, "target_ref": trial_id,
                            "part_id": part_id, "media_type": media_type,
                            "byte_length": len(content),
                            "digest": "sha256:" + hashlib.sha256(content).hexdigest(),
                            "chunk_count": 1,
                        },
                    )
                    assert opened.status_code == 200, opened.text
                    chunk = await web.put(
                        f"{upload_base}/{upload_id}/chunks/0",
                        headers={**headers, "content-type": "application/octet-stream"},
                        content=content,
                    )
                    assert chunk.status_code == 200, chunk.text
                    finalized = await web.post(
                        f"{upload_base}/{upload_id}/finalize", headers=headers,
                    )
                    assert finalized.status_code == 200, finalized.text
                    saved = finalized.json()
                    trial_parts.append({
                        "part_id": part_id, "modality": modality,
                        "media_type": media_type, "byte_length": len(content),
                        "width": saved["width"], "height": saved["height"],
                        "payload": saved["payload"],
                    })
                started_trial = await web.post(
                    f"{trial_base}/actions/memory.trial.start",
                    headers={**headers, "idempotency-key": "host-mixed-recall"},
                    json={"query_id": trial_id, "parts": trial_parts},
                )
                assert started_trial.status_code == 200, started_trial.text
                assert started_trial.json()["query_id"] == trial_id

                async def trial_query(name, body):
                    response = await web.post(
                        f"{trial_base}/queries/{name}", headers=headers, json=body,
                    )
                    assert response.status_code == 200, response.text
                    return response.json()

                async with asyncio.timeout(100):
                    while True:
                        trial_state = await trial_query(
                            "memory.trial.get", {"query_id": trial_id}
                        )
                        if trial_state["state"] == "READY":
                            break
                        assert trial_state["state"] != "FAILED", trial_state
                        await asyncio.sleep(0.2)
                trial_page = await trial_query(
                    "memory.trial.result", {"query_id": trial_id, "limit": 4}
                )
                trial_image = await trial_query(
                    "memory.trial.image", {
                        "query_id": trial_id,
                        "source_unit_id": unit_evidence["source_unit_id"],
                        "part_id": picture["part_id"], "offset": 0,
                    },
                )
            image_reads = (await host._kernel.audit.scan(
                action="memory.trial.image", limit=10,
            ))["records"]
            result_reads = (await host._kernel.audit.scan(
                action="memory.trial.result", limit=10,
            ))["records"]
            assert len(image_reads) == len(result_reads) == 1
            for record in (*image_reads, *result_reads):
                assert record["severity"] == "SECURITY"
                assert record["producer_component_id"] == "memory.graph"
                assert record["actor_id"] == "subject/host-admin"
                assert record["operation_kind"] == "web.query"
                assert record["call_id"] is None
                assert record["details_payload_id"] is None
            assert trial_page["total_count"] == 1
            assert trial_page["items"][0]["source_unit_id"] == unit_evidence["source_unit_id"]
            assert trial_page["items"][0]["external_group_id"] == "933179609"
            assert trial_page["items"][0]["author_subject_ref"] == "qq:20001"
            assert base64.b64decode(trial_image["data_base64"]) == picture_bytes
            with pytest.raises(ForestError) as foreign_trial:
                await memory_instance.management_contract.query_authorized(
                    "memory.trial.result", {"query_id": trial_id},
                    authorization=SimpleNamespace(actor_ref="subject/other-admin", web_subject_ref="subject/other-admin"),
                )
            assert foreign_trial.value.code == "MEMORY_TRIAL_NOT_FOUND"

            # The owned Memory worker advances a fresh query from capture through
            # embedding, fixed ANN, region freeze and evidence publication.
            automatic_query = await memory_instance.runtime_service_handlers()[
                ("memory.query.begin", 1)
            ].invoke_routed(
                query_route,
                RuntimeServiceInvocation("query-begin-automatic", 30, "memory.query", settings.bot_id),
                (), {"query_id": "query/automatic-host",
                     "conversation_ref": "onebot:group:933179609",
                     "input_batch_ref": recall_batch, "context_revision": recall_revision},
            )
            automatic_deadline = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
            async def request_automatic(operation, deadline):
                service_id = "memory.query.request"
                return await memory_instance.runtime_service_handlers()[(service_id, 1)].invoke_routed(
                    FrozenServiceRoute(
                        service_id=service_id, version=1,
                        caller_component_id="memory.graph", provider_component_id="memory.graph",
                        provider_generation_id=memory_identity.generation_id,
                        graph_id=memory_identity.graph_id,
                        graph_revision=memory_identity.graph_revision,
                        admission_lane="DATA", owned_work=False,
                    ),
                    RuntimeServiceInvocation(operation, 30, "memory.query", settings.bot_id),
                    (), {"query_id": automatic_query["query_id"], "deadline_at": deadline},
                )
            requested = await request_automatic("request/automatic-a", automatic_deadline)
            assert await request_automatic("request/automatic-b", automatic_deadline) == requested
            with pytest.raises(ForestError) as conflicting_deadline:
                await request_automatic("request/automatic-conflict", (
                    datetime.now(timezone.utc) + timedelta(minutes=4)).isoformat())
            assert conflicting_deadline.value.code == "MEMORY_RECALL_REQUEST_CONFLICT"
            async with asyncio.timeout(100):
                status_poll = 0
                while True:
                    status_poll += 1
                    state = await query_stage(automatic_query["query_id"],
                                              f"status/automatic-{status_poll}", stage="work-status")
                    if state["state"] == "READY":
                        break
                    assert state["state"] != "FAILED", state
                    await asyncio.sleep(0.2)
            assert state["evidence_ref"] is not None
            assert (await query_stage(automatic_query["query_id"],
                                      "status/automatic-replay", stage="work-status")) == state
            assert (await query_stage(automatic_query["query_id"],
                                      "progress/automatic", stage="progress"))["phase"] == "READY"
            cancelled_ready = await memory_instance.runtime_service_handlers()[
                ("memory.query.cancel", 1)
            ].invoke_routed(
                FrozenServiceRoute(
                    service_id="memory.query.cancel", version=1,
                    caller_component_id="memory.graph", provider_component_id="memory.graph",
                    provider_generation_id=memory_identity.generation_id,
                    graph_id=memory_identity.graph_id,
                    graph_revision=memory_identity.graph_revision,
                    admission_lane="DATA", owned_work=False,
                ),
                RuntimeServiceInvocation("cancel/finished-recall", 30, "memory.query", settings.bot_id),
                (), {"query_id": automatic_query["query_id"], "reason_code": "SUPERSEDED"},
            )
            assert cancelled_ready["status"] == "TERMINAL"

            # The public UoW revokes Payload, Memory eligibility, graph evidence,
            # and vector refs together before the asynchronous Milvus deletion.
            previous_epoch = await host._kernel.payload.epoch()
            stale_plan = await memory_instance.management_contract.query(
                "memory.forget.preview",
                {"source_unit_id": "onebot-unit:" + event_hash},
            )
            assert stale_plan["deletion_epoch"] == previous_epoch
            async with host.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.relation_contributions("
                        "bot_id,contribution_id,operation_id,input_digest,relation_id,"
                        "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                        "weight_micros,admitted_deletion_epoch,cross_trunk) "
                        "VALUES(%s,'forget/evidence','forget/evidence',%s,'forget/relation',"
                        "'ASSOCIATES_WITH','ENTITY','entity/a','ENTITY','entity/b','RULE',"
                        "500000,%s,false)"
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
            with pytest.raises(ForestError) as changed_plan:
                await memory_instance.runtime_service_handlers()[
                    ("memory.source.forget", 1)
                ].invoke_routed(
                    forget_route,
                    RuntimeServiceInvocation(
                        "memory-forget-stale-plan", 30,
                        "memory.forget", "bot-production-test",
                    ),
                    (),
                    {**forget_input, "operation_id": "memory-forget-stale-plan", "plan": stale_plan},
                )
            assert changed_plan.value.code == "MEMORY_FORGET_PLAN_CHANGED"
            assert await host._kernel.payload.epoch() == previous_epoch
            stale_request = await memory_instance.management_contract.action(
                "memory.forget.enqueue",
                operation_id="web:memory-forget-stale-queued",
                payload={"source_unit_id": forget_input["source_unit_id"],
                         "plan": stale_plan},
                authorization=SimpleNamespace(actor_ref="subject/host-admin"),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
            async with asyncio.timeout(20):
                while True:
                    stale_work = await memory_instance.management_contract.query(
                        "memory.forget.status",
                        {"request_id": stale_request["request_id"]},
                    )
                    if stale_work["state"] in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
                        break
                    await asyncio.sleep(0.2)
            assert stale_work["state"] == "FAILED", stale_work
            assert stale_work["error_code"] == "MEMORY_FORGET_PLAN_CHANGED"
            assert await host._kernel.payload.epoch() == previous_epoch
            current_plan = await memory_instance.management_contract.query(
                "memory.forget.preview", {"source_unit_id": "onebot-unit:" + event_hash}
            )
            assert current_plan["relation_retraction_count"] == (
                stale_plan["relation_retraction_count"] + 1
            )
            forget_input["plan"] = current_plan
            gateway = ManagementGateway(
                host,
                ticket_authority=WebAuthorityTicketAuthority(
                    issuer_id="deployment/host-test",
                    active_key_id="master/host-test/1",
                    key_resolver=lambda _key_id: b"k" * 32,
                ),
            )
            management_body = {
                "source_unit_id": forget_input["source_unit_id"], "plan": current_plan
            }
            management_operation = gateway.operation_id(
                "memory.graph", "memory.forget.enqueue", "host-forget"
            )
            target_generation_id, target_graph_revision = gateway.authorization_target(
                "memory.graph"
            )
            management_ticket = await gateway.ticket_authority.issue(
                session=AuthenticatedWebSession(
                    "token-digest", "session/host-test", 1, "csrf-digest",
                    WebSubject("subject/host-admin", "admin", "Administrator", "ADMIN", True, 1),
                    datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                target_scope_kind="bot",
                target_scope_id=settings.bot_id,
                target_component_id="memory.graph",
                target_generation_id=target_generation_id,
                target_graph_revision=target_graph_revision,
                target_operation_ref=management_operation,
                action_key="memory.forget.enqueue",
                input_digest=digest_json(management_body),
                plan_digest=digest_json(current_plan),
            )
            queued = await gateway.module_action(
                "memory.graph", "memory.forget.enqueue",
                body=management_body,
                idempotency_key="host-forget",
                authorization=management_ticket,
                operation_id=management_operation,
            )
            request_id = queued["request_id"]
            assert request_id == management_operation
            async with asyncio.timeout(30):
                while True:
                    work = await memory_instance.management_contract.query(
                        "memory.forget.status", {"request_id": request_id}
                    )
                    if work["state"] in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
                        break
                    await asyncio.sleep(0.2)
            assert work["state"] == "SUCCEEDED", work
            forget = work["result"]
            trial_authorization = SimpleNamespace(
                actor_ref=trial_actor, web_subject_ref=trial_actor,
            )
            trial_after_forget = await memory_instance.management_contract.query_authorized(
                "memory.trial.get", {"query_id": trial_id},
                authorization=trial_authorization,
            )
            assert trial_after_forget["state"] == "READY"
            trial_page_after_forget = await memory_instance.management_contract.query_authorized(
                "memory.trial.result", {"query_id": trial_id},
                authorization=trial_authorization,
            )
            assert trial_page_after_forget["total_count"] == 0
            assert trial_page_after_forget["items"] == []
            with pytest.raises(ForestError) as forgotten_trial_image:
                await memory_instance.management_contract.query_authorized(
                    "memory.trial.image", {
                        "query_id": trial_id,
                        "source_unit_id": unit_evidence["source_unit_id"],
                        "part_id": picture["part_id"], "offset": 0,
                    },
                    authorization=trial_authorization,
                )
            assert forgotten_trial_image.value.code == "MEMORY_TRIAL_IMAGE_NOT_FOUND"
            forget_input["operation_id"] = request_id
            assert forget["deletion_epoch"] == previous_epoch + 1
            assert forget["suppressed_source_units"] == 1
            assert forget["vector_deletions"] == len(facets) + len(query_facets)
            assert forget["retracted_relations"] == 1
            async with host.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "UPDATE {}.management_forget_work SET state='RUNNING',"
                        "claim_token='claim:simulated-crash',"
                        "lease_deadline=clock_timestamp()-interval '1 second',"
                        "attempt_count=1,result_payload=NULL,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND request_id=%s"
                    ).format(sql.Identifier(memory_namespace)),
                    (settings.bot_id, request_id),
                )
            async with asyncio.timeout(10):
                while True:
                    recovered_work = await memory_instance.management_contract.query(
                        "memory.forget.status", {"request_id": request_id}
                    )
                    if recovered_work["state"] == "SUCCEEDED":
                        break
                    await asyncio.sleep(0.2)
            assert recovered_work["result"] == forget
            assert await host._kernel.payload.epoch() == previous_epoch + 1
            assert (await query_stage(recall_query["query_id"], "seeds/recall-after-forget"))["seeds"] == []
            with pytest.raises(ForestError) as revoked_work:
                await query_stage(automatic_query["query_id"],
                                  "status/automatic-revoked", stage="work-status")
            assert revoked_work.value.code == "MEMORY_EVIDENCE_UNAVAILABLE"
            with pytest.raises(ForestError) as revoked_progress:
                await query_stage(recall_query["query_id"], "progress/revoked", stage="progress")
            assert revoked_progress.value.code == "MEMORY_EVIDENCE_UNAVAILABLE"
            assert (await query_stage(
                recall_query["query_id"], "region/recall-after-forget", stage="region"
            ))["nodes"] == []
            assert await query_stage(
                recall_query["query_id"], "freeze/recall-after-forget", stage="freeze"
            ) == frozen[0]
            assert (await frozen_parts())["node"] is None
            assert (await frozen_parts())["parts"] == []
            with pytest.raises(ForestError) as unavailable:
                await query_stage(recall_query["query_id"], "materialize/after-forget", stage="materialize")
            assert unavailable.value.code == "MEMORY_EVIDENCE_UNAVAILABLE"
            for ref in (materialized[0]["evidence_ref"], empty_evidence["evidence_ref"]):
                with pytest.raises(ForestError) as revoked:
                    await host._kernel.payload.read_bytes(ref["payload_id"])
                assert revoked.value.code == "PAYLOAD_REVOKED"
            async with asyncio.timeout(20):
                while True:
                    async with host.store.pool.connection() as conn:
                        scrubbed = await (await conn.execute(
                            sql.SQL(
                                "SELECT h.state, b.payload_id IS NULL AS body_removed "
                                "FROM {}.payload_headers h LEFT JOIN {}.payload_bodies b "
                                "USING (bot_id,payload_id) WHERE h.bot_id=%s AND h.payload_id=%s"
                            ).format(
                                sql.Identifier(host._kernel.payload.namespace),
                                sql.Identifier(host._kernel.payload.namespace),
                            ),
                            (settings.bot_id, materialized[0]["evidence_ref"]["payload_id"]),
                        )).fetchone()
                    if scrubbed == {"state": "SCRUBBED", "body_removed": True}:
                        break
                    await asyncio.sleep(0.1)
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
                                    "WHERE bot_id=%s AND index_ref_id=ANY(%s::text[])"
                                ).format(sql.Identifier(memory_namespace)),
                                (settings.bot_id,
                                 [facet["facet_id"] for facet in facets + query_facets]),
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
            replacement_task = asyncio.create_task(host.replace_runtime_graph())
            try:
                replacement_graph = await asyncio.wait_for(
                    asyncio.shield(replacement_task), timeout=180,
                )
            except ForestError as error:
                failures = {
                    component_id: work.failure
                    for component_id, instance in host.registry.instances.items()
                    if (work := getattr(instance, "_owned_work", None)) is not None
                    and work.failure is not None
                }
                raise AssertionError(
                    f"Graph replacement failed: {error.code} {error.details}; "
                    f"owned_failures={failures}"
                ) from error
            except TimeoutError as error:
                owned = {}
                for component_id, instance in host.registry.instances.items():
                    work = getattr(instance, "_owned_work", None)
                    if work is None:
                        continue
                    owned[component_id] = {
                        "inflight": work.inflight,
                        "draining": work.draining,
                        "admission": work.admission.is_set(),
                        "tasks": [
                            (task.done(), [
                                (frame.f_code.co_name, frame.f_lineno)
                                for frame in task.get_stack()
                            ]) for task in work.owned_tasks()
                        ],
                    }
                replacement_task.cancel()
                raise AssertionError(f"Graph replacement stalled: {owned}") from error
            assert replacement_graph.graph_identity.graph_id != previous.graph_id
            assert set(replacement_graph.states.values()) == {ModuleLifecycleState.ACTIVE}
            current_worker_host = host.registry.get("component.worker-host")
            current_module = await current_worker_host.management_contract.query(
                "module", {"component_id": "agent.runtime"}
            )
            assert current_module["graph_revision"] == replacement_graph.graph_identity.graph_revision
            with pytest.raises(ForestError) as stale_page:
                await current_worker_host.management_contract.query(
                    "modules",
                    {
                        "graph_id": first_modules["graph_id"],
                        "after": first_modules["next_after"],
                        "limit": 3,
                    },
                )
            assert stale_page.value.code == "COMPONENT_HOST_GRAPH_CHANGED"
            assert current_worker_host.generation_identity != worker_host.generation_identity
            with pytest.raises(ForestError):
                await worker_host.management_contract.query(
                    "module", {"component_id": "agent.runtime"}
                )
            replaced_work = await host.registry.get("memory.graph").management_contract.query(
                "memory.forget.status", {"request_id": request_id}
            )
            assert replaced_work["state"] == "SUCCEEDED"
            assert replaced_work["result"] == forget
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
            restarted_work = await restarted.registry.get(
                "memory.graph"
            ).management_contract.query(
                "memory.forget.status", {"request_id": request_id}
            )
            assert restarted_work["state"] == "SUCCEEDED"
            assert restarted_work["result"] == forget
            selection_contract = restarted.registry.get(
                "component.worker-host"
            ).management_contract
            selection = await selection_contract.query("selection", {})
            graph_revision = restarted.registry.graph_identity.graph_revision
            candidate = {
                "component_id": "process.executor",
                "source_kind": "builtin",
                "package_digest": None,
                "manifest_digest": None,
                "approved_capabilities": [],
                "desired_enabled": True,
                "expected_configuration_revision": None,
                "expected_selection_revision": selection["revision"],
                "expected_graph_revision": graph_revision,
            }
            preview = await selection_contract.query("validate", candidate)

            async def send_shell(message_id, user_id, command):
                await clients[-1].handler(json.dumps({
                    "self_id": int(settings.onebot_self_id),
                    "post_type": "message", "message_type": "group",
                    "sub_type": "normal", "time": 1_720_000_000 + message_id,
                    "message_id": message_id, "user_id": user_id,
                    "group_id": 933179609,
                    "sender": {"user_id": user_id, "nickname": "Operator", "card": "",
                               "role": "admin" if user_id == 42 else "member"},
                    "message": [{"type": "text", "data": {"text": command}}],
                }, separators=(",", ":")).encode())

            shell_command = (
                "#forest module enable --component-id process.executor "
                f"--plan-digest {preview['plan_digest']} "
                f"--expected-selection-revision {selection['revision']} "
                f"--expected-graph-revision {graph_revision}"
            )
            await send_shell(81001, 42, shell_command)
            async with asyncio.timeout(90):
                while True:
                    selected = await restarted.registry.get(
                        "component.worker-host"
                    ).management_contract.query("selection", {})
                    if selected["revision"] == selection["revision"] + 1:
                        break
                    await asyncio.sleep(0.1)
            assert selected["entries"][-1]["component_id"] == "process.executor"
            assert selected["entries"][-1]["desired_enabled"] is True
            async with asyncio.timeout(90):
                while True:
                    latest = (await restarted._kernel.runtime.status())["latest_replacement"]
                    if (
                        latest is not None and latest["state"] == "SUCCEEDED"
                        and (latest.get("result_graph_revision") or 0) > graph_revision
                    ):
                        break
                    if (
                        latest is not None and latest["state"] == "FAILED"
                        and latest.get("expected_graph_revision") == graph_revision
                    ):
                        pytest.fail(f"Shell selection replacement failed: {latest}")
                    await asyncio.sleep(0.1)
            web_gateway = ManagementGateway(
                restarted, ticket_authority=gateway.ticket_authority,
            )
            stale_body = {**candidate, "plan_digest": preview["plan_digest"]}
            stale_operation = web_gateway.operation_id(
                "component.worker-host", "module.enable", "after-shell-selection"
            )
            current_graph = restarted.registry.graph_identity
            stale_ticket = await web_gateway.ticket_authority.issue(
                session=AuthenticatedWebSession(
                    "token-digest", "session/shell-race", 1, "csrf-digest",
                    WebSubject("subject/host-admin", "admin", "Administrator", "ADMIN", True, 1),
                    datetime.now(timezone.utc) + timedelta(hours=1),
                ),
                target_scope_kind="bot", target_scope_id=settings.bot_id,
                target_component_id="component.worker-host",
                target_generation_id=current_graph.generations["component.worker-host"],
                target_graph_revision=current_graph.graph_revision,
                target_operation_ref=stale_operation, action_key="module.enable",
                input_digest=digest_json(stale_body),
                plan_digest=web_gateway.action_plan_digest(
                    "component.worker-host", "module.enable", stale_body
                ),
            )
            with pytest.raises(ForestError) as stale_web:
                await web_gateway.module_action(
                    "component.worker-host", "module.enable", body=stale_body,
                    idempotency_key="after-shell-selection",
                    authorization=stale_ticket, operation_id=stale_operation,
                )
            assert stale_web.value.code == "COMPONENT_SELECTION_REVISION_CONFLICT"
            assert (await restarted.registry.get(
                "component.worker-host"
            ).management_contract.query("selection", {}))["revision"] == selected["revision"]
            await send_shell(81002, 43, shell_command)
            shell_namespace = next(
                item.storage.spec.namespace for item in restarted.registry.descriptors
                if item.component_id == "shell.command"
            )
            async with asyncio.timeout(20):
                while True:
                    async with restarted.store.pool.connection() as conn:
                        rejected = await (await conn.execute(
                            sql.SQL("SELECT state,rejection_code FROM {}.shell_requests "
                                    "WHERE bot_id=%s AND principal_ref=%s "
                                    "AND command_id=%s").format(sql.Identifier(shell_namespace)),
                            (settings.bot_id, "qq:43", "module.enable"),
                        )).fetchone()
                    if rejected is not None and rejected["state"] == "REJECTED":
                        break
                    await asyncio.sleep(0.1)
            assert rejected["rejection_code"] == "SHELL_PRINCIPAL_DENIED"
            disable_graph_revision = restarted.registry.graph_identity.graph_revision
            disable_input = {
                **candidate,
                "desired_enabled": False,
                "expected_selection_revision": selected["revision"],
                "expected_graph_revision": disable_graph_revision,
            }
            disable_preview = await restarted.registry.get(
                "component.worker-host"
            ).management_contract.query("validate", disable_input)
            await send_shell(
                81003, 42,
                "#forest module disable --component-id process.executor "
                f"--plan-digest {disable_preview['plan_digest']} "
                f"--expected-selection-revision {selected['revision']} "
                f"--expected-graph-revision {disable_graph_revision}",
            )
            async with asyncio.timeout(90):
                while True:
                    disabled = await restarted.registry.get(
                        "component.worker-host"
                    ).management_contract.query("selection", {})
                    if disabled["revision"] == selected["revision"] + 1:
                        break
                    await asyncio.sleep(0.1)
            assert disabled["entries"][-1]["desired_enabled"] is False
            try:
                async with asyncio.timeout(90):
                    while "process.executor" in restarted.registry.graph_identity.generations:
                        latest = (await restarted._kernel.runtime.status())["latest_replacement"]
                        if latest is not None and latest["state"] == "FAILED":
                            pytest.fail(f"Shell disable replacement failed: {latest}")
                        await asyncio.sleep(0.1)
            except TimeoutError:
                latest = (await restarted._kernel.runtime.status())["latest_replacement"]
                replacement_task = restarted._replacement_task
                task_error = None
                if replacement_task is not None and replacement_task.done():
                    task_error = (
                        "cancelled" if replacement_task.cancelled()
                        else replacement_task.exception()
                    )
                pytest.fail(
                    "Shell disable replacement timed out: "
                    f"request={latest}, "
                    f"task_done={replacement_task.done() if replacement_task else None}, "
                    f"task_error={task_error}"
                )

            class SelectionWebSupervisor:
                async def management_scopes(self):
                    return ({"scope_kind": "bot", "scope_id": settings.bot_id},)

                @asynccontextmanager
                async def management_host(self, scope_id):
                    assert scope_id == settings.bot_id
                    yield restarted

            async with _real_web_identity_for_bot_test(dsn) as identity:
                web_app = create_deployment_management_app(
                    deployment_id="deployment/host-web-test",                     identity_authority=identity, allowed_origin="http://forest.test",
                    bot_supervisor=SelectionWebSupervisor(),
                )
                base = (
                    f"/management/v1/scopes/bot/{settings.bot_id}/"
                    "modules/component.worker-host"
                )
                async with AsyncClient(
                    transport=ASGITransport(app=web_app), base_url="http://forest.test",
                ) as web:
                    login = await web.post(
                        "/management/v1/auth/login",
                        headers={"origin": "http://forest.test"},
                        json={"username": "admin", "password": "host-web-password-1"},
                    )
                    assert login.status_code == 200, login.text
                    headers = {
                        "origin": "http://forest.test",
                        "x-forest-csrf": web.cookies["forest_management_csrf"],
                    }
                    current_selection = await restarted.registry.get(
                        "component.worker-host"
                    ).management_contract.query("selection", {})
                    web_candidate = {
                        **candidate,
                        "expected_selection_revision": current_selection["revision"],
                        "expected_graph_revision": (
                            restarted.registry.graph_identity.graph_revision
                        ),
                    }
                    checked = await web.post(
                        f"{base}/queries/validate", headers=headers, json=web_candidate,
                    )
                    assert checked.status_code == 200, checked.text
                    web_preview = checked.json()
                    assert web_preview["target_component_id"] == "process.executor"
                    accepted = await web.post(
                        f"{base}/actions/module.enable",
                        headers={**headers, "idempotency-key": "web-after-shell-disable"},
                        json={**web_candidate, "plan_digest": web_preview["plan_digest"]},
                    )
                    assert accepted.status_code == 200, accepted.text
                    web_request_id = accepted.json()["request_id"]
                    async with asyncio.timeout(90):
                        while True:
                            response = await web.post(
                                f"{base}/queries/selection-application",
                                headers=headers, json={"request_id": web_request_id},
                            )
                            assert response.status_code == 200, response.text
                            web_state = response.json()
                            kernel_state = (web_state.get("kernel") or {}).get("state")
                            if kernel_state == "SUCCEEDED":
                                break
                            assert kernel_state != "FAILED", web_state
                            await asyncio.sleep(0.2)
                    assert "process.executor" in restarted.registry.graph_identity.generations
        finally:
            await restarted.close()
        assert clients[-1].closed is True
    finally:
        FakeMilvusClient.fail_after_upsert_once = False
        assert name.startswith("forest_host_") and len(name) == 44
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
@pytest.mark.parametrize(
    "scenario", ("text", "visual", "cross-group", "success", "tool-continuation", "result-ledger-recovery", "result-payload-recovery", "requeue", "transport-exhausted", "reload", "reload-publish", "reload-full", "reload-lost-receipt", "prepared-crash", "long-model-lease")
)
async def test_group_trigger_recall_reaches_provider_request(tmp_path, scenario, monkeypatch):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    if scenario in {"reload-lost-receipt", "prepared-crash"}:
        monkeypatch.setattr("forest.bot_host._HOST_LEASE_TTL", timedelta(seconds=15))
    name = "forest_recall_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    settings = BotSettings(
        bot_id="bot-agent-recall-test", dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="test-model-key",
        model="test-model", model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024,
        model_input_modalities=("text", "image") if scenario == "visual" else ("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B", embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609", "933179610") if scenario == "cross-group"
        else ("933179609",),
        group_trigger_mode="MENTION", private_enabled=True,
        shell_administrator_ids=("42",),
        agent_system_prompt="The Agent-Memory request integration fixture.",
    )
    qiyu_env_file = os.environ.get("FOREST_TEST_QIYU_ENV_FILE") if scenario == "visual" else None
    real_transport = bool(qiyu_env_file)
    valid_media = real_transport or bool(os.environ.get("FOREST_TEST_MEDIA_CAPTURE_DELAY_SECONDS"))
    if qiyu_env_file:
        qiyu_values = qiyu_embedding_config(qiyu_env_file)
        milvus_uri = os.environ["FOREST_TEST_MILVUS_URI"]
        settings = replace(
            settings,
            bot_id="bot-agent-recall-real-" + uuid4().hex[:12],
            embedding_endpoint=qiyu_values["EMBEDDING_BASE_URL"].rstrip("/") + "/embeddings",
            embedding_api_key=qiyu_values["EMBEDDING_API_KEY"],
            embedding_deployment_id="embedding.multimodal/qiyu-vl/real-host-test",
            embedding_model_revision="operator-declared/real-host-test-1",
            vector_index_endpoint=milvus_uri,
            embedding_timeout_seconds=120,
        )
    clients = []
    RecordingModelClient.requests = []
    RecordingModelClient.first_gate = (
        asyncio.Event() if scenario in {"cross-group", "tool-continuation", "requeue", "reload", "reload-publish", "reload-full", "reload-lost-receipt", "long-model-lease"} else None
    )
    RecordingModelClient.not_sent_attempts = (
        3 if scenario in {"requeue", "transport-exhausted"} else 2 if scenario == "reload-full"
        else 1 if scenario == "reload-lost-receipt"
        else 1 if scenario in {"reload", "reload-publish"} else 0
    )
    RecordingModelClient.reply_text = (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        '<block block_type="Assistant"><content><text>Forest success fixture</text>'
        '</content></block></blocks></payload></valid_agent_protocol_content_block>'
        if scenario in {"success", "result-ledger-recovery", "result-payload-recovery", "cross-group", "prepared-crash"} else None
    )
    RecordingModelClient.reply_factory = None
    if scenario == "tool-continuation":
        def tool_then_answer(prepared, invocation_number):
            if invocation_number == 1:
                trigger_ref = prepared.manifest["trigger_claims"][0]["last_trigger_ref"]
                arguments = json.dumps({
                    "authority_trigger_id": trigger_ref,
                    "arguments": {"source_unit_ids": ["missing-test-source"]},
                }, separators=(",", ":"))
                return (
                    '<valid_agent_protocol_content_block block_type="ModelTurn">'
                    '<payload><blocks><block block_type="FunctionCall">'
                    '<function name="memory_recall_exact"><arguments encoding="json">'
                    + arguments + '</arguments></function></block></blocks></payload>'
                    '</valid_agent_protocol_content_block>'
                )
            return (
                '<valid_agent_protocol_content_block block_type="ModelTurn">'
                '<payload><blocks><block block_type="Assistant"><content><text>'
                'Forest tool continuation fixture</text></content></block></blocks>'
                '</payload></valid_agent_protocol_content_block>'
            )

        RecordingModelClient.reply_factory = tool_then_answer

    async def create_websocket(_url, _token, **_limits):
        client = FakeWebSocketClient(settings.onebot_self_id)
        clients.append(client)
        return client

    def create_host(catalog_name):
        return BotHost(
            settings,
            model_client_factory=RecordingModelClient,
            embedding_client_factory=None if real_transport else FakeEmbeddingClient,
            media_client_factory=ValidPngMediaClient if valid_media else FakeMediaClient,
            vector_index_client_factory=None if real_transport else FakeMilvusClient,
            websocket_client_factory=create_websocket,
            package_catalog=PackageCatalog(tmp_path / catalog_name),
        )

    host = create_host("recall-packages")
    try:
        await host.start()
        memory_schema = next(
            item.storage.spec.namespace for item in host.registry.descriptors
            if item.component_id == "memory.graph"
        )
        async with asyncio.timeout(90 if real_transport else 20):
            while True:
                async with host.store.pool.connection() as conn:
                    domain = await (await conn.execute(
                        sql.SQL("SELECT state FROM {}.memory_domain_bind_work WHERE bot_id=%s")
                        .format(sql.Identifier(memory_schema)), (settings.bot_id,),
                    )).fetchone()
                if domain is not None and domain["state"] == "READY":
                    break
                await asyncio.sleep(0.1)
        prepared_crash = asyncio.Event()
        result_observation_interrupted = asyncio.Event()
        result_ledger_interrupted = asyncio.Event()
        recovery_payloads = []
        recovery_errors = []
        if scenario == "prepared-crash":
            original_execute = PostgresPackageUnitOfWorkCoordinator.execute

            async def crash_after_prepare(coordinator, scope, declaration, payload, **kwargs):
                recovering = declaration["name"] == "agent.recover-prepared-model"
                if recovering:
                    recovery_payloads.append(payload)
                try:
                    result = await original_execute(
                        coordinator, scope, declaration, payload, **kwargs
                    )
                except ForestError as error:
                    if recovering:
                        recovery_errors.append(error)
                    raise
                if (declaration["name"] == "agent.prepare-model-effect"
                        and not prepared_crash.is_set()):
                    victim = host.registry.get("agent.runtime")._worker
                    assert victim.alive
                    victim.process.kill()
                    prepared_crash.set()
                    raise ForestError(
                        "TEST_PREPARED_MODEL_WORKER_CRASH",
                        "Agent Worker died after preparing its Effect", 503,
                    )
                return result

            monkeypatch.setattr(
                PostgresPackageUnitOfWorkCoordinator, "execute", crash_after_prepare
            )
        if scenario == "result-ledger-recovery":
            original_execute = PostgresPackageUnitOfWorkCoordinator.execute

            async def interrupt_first_observation(
                coordinator, scope, declaration, payload, **kwargs
            ):
                if (
                    declaration["name"] == "agent.observe-model-effect"
                    and not result_observation_interrupted.is_set()
                ):
                    result_observation_interrupted.set()
                    raise ForestError(
                        "TEST_MODEL_OBSERVATION_INTERRUPTED",
                        "Provider receipt committed before Agent observation", 503,
                    )
                return await original_execute(
                    coordinator, scope, declaration, payload, **kwargs
                )

            monkeypatch.setattr(
                PostgresPackageUnitOfWorkCoordinator,
                "execute", interrupt_first_observation,
            )
        if scenario == "result-payload-recovery":
            original_succeed = KernelServiceOperationLedger.succeed

            async def interrupt_result_receipt(ledger, scope, route, result):
                if (
                    route.service_id == "provider.model.dispatch"
                    and not result_ledger_interrupted.is_set()
                ):
                    result_ledger_interrupted.set()
                    raise ForestError(
                        "TEST_PROVIDER_RESULT_RECEIPT_INTERRUPTED",
                        "Provider result Payload committed before service receipt", 503,
                    )
                return await original_succeed(ledger, scope, route, result)

            monkeypatch.setattr(
                KernelServiceOperationLedger, "succeed", interrupt_result_receipt
            )
        await clients[-1].handler(json.dumps({
            "self_id": int(settings.onebot_self_id), "post_type": "message",
            "message_type": "group", "sub_type": "normal",
            "time": int(datetime.now(timezone.utc).timestamp()),
            "message_id": 90001, "user_id": 20001, "group_id": 933179609,
            "sender": {"user_id": 20001, "nickname": "Recall tester",
                       "card": "", "role": "member"},
            "message": [
                {"type": "at", "data": {"qq": settings.onebot_self_id}},
                {"type": "text", "data": {"text": "What is in memory?"}},
                *([{"type": "image", "data": {
                    "file": "current-question-image",
                    "url": "https://media.example/current-question-image",
                }}] if scenario == "visual" else []),
            ],
        }).encode())
        if scenario == "prepared-crash":
            async with asyncio.timeout(90):
                await prepared_crash.wait()
            assert RecordingModelClient.requests == []
            agent_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "agent.runtime"
            )
            async with host.store.pool.connection() as conn:
                frozen_request = await (await conn.execute(sql.SQL(
                    "SELECT request_id,state,initial_wire_payload_digest,"
                    "projected_content_digest,source_manifest_digest "
                    "FROM {}.model_requests WHERE bot_id=%s"
                ).format(sql.Identifier(agent_schema)), (settings.bot_id,))).fetchone()
                frozen_attempt = await (await conn.execute(sql.SQL(
                    "SELECT attempt_id,effect_operation_ref,state "
                    "FROM {}.model_transport_attempts WHERE bot_id=%s"
                ).format(sql.Identifier(agent_schema)), (settings.bot_id,))).fetchone()
            assert frozen_request["state"] == "ATTEMPT_PREPARED"
            assert frozen_attempt["state"] == "PREPARED"
            runtime_schema = host._kernel.runtime.namespace
            try:
                await host.close()
            except ForestError as exc:
                assert exc.code == "RUNTIME_MODULE_CLEANUP_FAILED"
                assert exc.details["errors"]
            observer = await psycopg.AsyncConnection.connect(uri)
            try:
                async with asyncio.timeout(180):
                    while True:
                        lease = await (await observer.execute(sql.SQL(
                            "SELECT expires_at<=clock_timestamp() AS expired "
                            "FROM {}.host_lease WHERE bot_id=%s"
                        ).format(sql.Identifier(runtime_schema)),
                            (settings.bot_id,))).fetchone()
                        assert lease is not None
                        if lease[0]:
                            break
                        await asyncio.sleep(0.5)
            finally:
                await observer.close()
            host = create_host("recall-prepared-crash-restarted")
            await host.start()
            # The crashed Worker cannot release its 300-second Run lease.
            # Observe the database fence rather than assuming a wall-clock delay.
            async with asyncio.timeout(420):
                while True:
                    async with host.store.pool.connection() as conn:
                        owner_expiry = await (await conn.execute(sql.SQL(
                            "SELECT owner_expires_at<=clock_timestamp() AS expired "
                            "FROM {}.agent_runs WHERE bot_id=%s"
                        ).format(sql.Identifier(agent_schema)),
                            (settings.bot_id,))).fetchone()
                    assert owner_expiry is not None
                    if owner_expiry["expired"]:
                        break
                    await asyncio.sleep(1)
            try:
                async with asyncio.timeout(90):
                    while not RecordingModelClient.requests and not recovery_errors:
                        await asyncio.sleep(0.5)
                    if recovery_errors:
                        raise TimeoutError
            except TimeoutError as exc:
                conversation_schema = next(
                    item.storage.spec.namespace for item in host.registry.descriptors
                    if item.component_id == "conversation.scheduler"
                )
                async with host.store.pool.connection() as conn:
                    runs = await (await conn.execute(sql.SQL(
                        "SELECT run_id,state,owner_id,owner_epoch,owner_expires_at,"
                    "current_model_request_id,error_code,absolute_deadline,"
                    "clock_timestamp() AS database_now "
                        "FROM {}.agent_runs WHERE bot_id=%s"
                    ).format(sql.Identifier(agent_schema)), (settings.bot_id,))).fetchall()
                    requests = await (await conn.execute(sql.SQL(
                        "SELECT request_id,state,request_revision,current_attempt_id "
                        "FROM {}.model_requests WHERE bot_id=%s ORDER BY request_revision"
                    ).format(sql.Identifier(agent_schema)), (settings.bot_id,))).fetchall()
                    attempts = await (await conn.execute(sql.SQL(
                        "SELECT attempt_id,state,effect_operation_ref "
                        "FROM {}.model_transport_attempts WHERE bot_id=%s"
                    ).format(sql.Identifier(agent_schema)), (settings.bot_id,))).fetchall()
                    lanes = await (await conn.execute(sql.SQL(
                        "SELECT active_run_ref,resume_reason_code,resume_not_before "
                        "FROM {}.conversation_lanes WHERE bot_id=%s"
                    ).format(sql.Identifier(conversation_schema)),
                        (settings.bot_id,))).fetchall()
                    settlements = await (await conn.execute(sql.SQL(
                        "SELECT input_payload->'outcome' AS outcome FROM {}."
                        "conversation_agent_work_settlements "
                        "ORDER BY created_at DESC LIMIT 5"
                    ).format(sql.Identifier(conversation_schema)))).fetchall()
                    predecessor = await (await conn.execute(sql.SQL(
                        "SELECT * FROM {}.model_requests WHERE bot_id=%s "
                        "AND request_id=%s"
                    ).format(sql.Identifier(agent_schema)),
                        (settings.bot_id, frozen_request["request_id"]))).fetchone()
                    prepared_attempt = await (await conn.execute(sql.SQL(
                        "SELECT * FROM {}.model_transport_attempts WHERE bot_id=%s "
                        "AND attempt_id=%s"
                    ).format(sql.Identifier(agent_schema)),
                        (settings.bot_id, frozen_attempt["attempt_id"]))).fetchone()
                failures = {
                    component_id: work.failure
                    for component_id, instance in host.registry.instances.items()
                    if (work := getattr(instance, "_owned_work", None)) is not None
                    and work.failure is not None
                }
                mismatch = []
                if recovery_payloads:
                    successor = recovery_payloads[-1]["successor"]
                    for key in (
                        "provider_component_id", "provider_profile_ref", "model_ref",
                        "instruction_channel", "capability_digest",
                        "output_token_budget", "projected_content_digest",
                        "source_manifest_digest",
                    ):
                        if predecessor[key] != successor[key]:
                            mismatch.append((key, predecessor[key], successor[key]))
                    if predecessor["initial_wire_payload_digest"] != (
                        successor["initial_wire"]["digest"]
                    ):
                        mismatch.append("initial_wire_digest")
                    if prepared_attempt["wire_payload_digest"] != (
                        successor["initial_wire"]["digest"]
                    ):
                        mismatch.append("attempt_wire_digest")
                    if predecessor["attempt_count"] != prepared_attempt["attempt_no"]:
                        mismatch.append("attempt_count")
                    if predecessor["current_attempt_id"] != prepared_attempt["attempt_id"]:
                        mismatch.append("current_attempt_id")
                raise AssertionError(
                    f"Prepared crash did not resume: runs={runs}, requests={requests}, "
                    f"attempts={attempts}, lanes={lanes}, settlements={settlements}, "
                    f"owned_failures={failures}, recovery_errors="
                    f"{[(item.code, item.details) for item in recovery_errors]}, "
                    f"mismatch={mismatch}"
                ) from exc
            resumed = RecordingModelClient.requests[0]
            resumed.verify()
            assert resumed.request_id != frozen_request["request_id"]
            assert resumed.payload_digest == frozen_request["initial_wire_payload_digest"]
            async with host.store.pool.connection() as conn:
                lineage = await (await conn.execute(sql.SQL(
                    "SELECT request_id,predecessor_request_id,request_revision,state,"
                    "initial_wire_payload_digest,projected_content_digest,"
                    "source_manifest_digest FROM {}.model_requests "
                    "WHERE bot_id=%s ORDER BY request_revision"
                ).format(sql.Identifier(agent_schema)), (settings.bot_id,))).fetchall()
                old_effect = await (await conn.execute(sql.SQL(
                    "SELECT o.state AS operation_state,a.state AS attempt_state "
                    "FROM {}.effect_operations o JOIN {}.effect_attempts a "
                    "ON a.bot_id=o.bot_id AND a.effect_operation_id=o.effect_operation_id "
                    "WHERE o.bot_id=%s AND o.effect_operation_id=%s"
                ).format(sql.Identifier(host._kernel.effect.namespace),
                         sql.Identifier(host._kernel.effect.namespace)),
                    (settings.bot_id, frozen_attempt["effect_operation_ref"]))).fetchone()
            assert len(lineage) == 2
            assert lineage[0]["state"] == "ABANDONED"
            assert lineage[1]["predecessor_request_id"] == frozen_request["request_id"]
            assert lineage[1]["request_id"] == resumed.request_id
            assert lineage[1]["request_revision"] == 2
            assert lineage[1]["initial_wire_payload_digest"] == (
                frozen_request["initial_wire_payload_digest"]
            )
            assert lineage[1]["projected_content_digest"] == (
                frozen_request["projected_content_digest"]
            )
            assert lineage[1]["source_manifest_digest"] == (
                frozen_request["source_manifest_digest"]
            )
            assert old_effect == {
                "operation_state": "CANCELLED", "attempt_state": "ABANDONED",
            }
            assert len(RecordingModelClient.requests) == 1
            return
        try:
            async with asyncio.timeout(120 if real_transport else 45):
                while not RecordingModelClient.requests:
                    await asyncio.sleep(0.1)
        except TimeoutError as exc:
            conversation_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "conversation.scheduler"
            )
            media_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "media.capture"
            )
            async with host.store.pool.connection() as conn:
                failures = await (await conn.execute(
                    sql.SQL("SELECT input_payload->'outcome' AS outcome "
                            "FROM {}.conversation_agent_work_settlements "
                            "ORDER BY created_at DESC LIMIT 5")
                    .format(sql.Identifier(conversation_schema))
                )).fetchall()
                recall = await (await conn.execute(sql.SQL(
                    "SELECT state,last_error_code,failure_count FROM {}.query_recall_work"
                ).format(sql.Identifier(memory_schema)))).fetchall()
                facets = await (await conn.execute(sql.SQL(
                    "SELECT purpose,state,count(*) AS n FROM {}.embedding_facets "
                    "GROUP BY purpose,state ORDER BY purpose,state"
                ).format(sql.Identifier(memory_schema)))).fetchall()
                vector_batches = await (await conn.execute(sql.SQL(
                    "SELECT state,attempt_count FROM {}.vector_publish_batches"
                ).format(sql.Identifier(memory_schema)))).fetchall()
                vector_settlements = await (await conn.execute(sql.SQL(
                    "SELECT input_payload->'outcome' AS outcome "
                    "FROM {}.vector_work_settlements ORDER BY claim_token DESC LIMIT 5"
                ).format(sql.Identifier(memory_schema)))).fetchall()
                pending_inputs = await (await conn.execute(sql.SQL(
                    "SELECT state,error_code,input_batch_ref,context_revision "
                    "FROM {}.pending_input_events"
                ).format(sql.Identifier(memory_schema)))).fetchall()
                source_occurrences = await (await conn.execute(sql.SQL(
                    "SELECT eligibility_state,input_batch_ref,context_revision "
                    "FROM {}.source_occurrences"
                ).format(sql.Identifier(memory_schema)))).fetchall()
                media_captures = await (await conn.execute(sql.SQL(
                    "SELECT state,error_code FROM {}.media_captures"
                ).format(sql.Identifier(media_schema)))).fetchall()
            raise AssertionError(
                f"Provider not reached: agent={failures}, recall={recall}, "
                f"facets={facets}, vector_batches={vector_batches}, "
                f"vector_settlements={vector_settlements}, "
                f"pending_inputs={pending_inputs}, occurrences={source_occurrences}, "
                f"media_captures={media_captures}, "
                f"memory_failure={host.registry.get('memory.graph')._owned_work.failure}"
            ) from exc
        prepared = RecordingModelClient.requests[0]
        prepared.verify()
        manifest = prepared.manifest
        assert manifest["memory_evidence"]["status"] == "READY"
        assert manifest["memory_evidence"]["current_source"]["external_group_id"] == "933179609"
        assert any(item["author"]["principal_ref"] == "qq:20001"
                   for item in manifest["source_manifest"])
        if real_transport:
            vector_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "vector.index"
            )
            async with host.store.pool.connection() as conn:
                query_facets = await (await conn.execute(
                    sql.SQL(
                        "SELECT modality,state,index_ref_id FROM {}.embedding_facets "
                        "WHERE bot_id=%s AND purpose='QUERY'"
                    ).format(sql.Identifier(memory_schema)), (settings.bot_id,),
                )).fetchall()
                active_refs = await (await conn.execute(
                    sql.SQL(
                        "SELECT ref_id FROM {}.vector_index_refs "
                        "WHERE bot_id=%s AND state='ACTIVE'"
                    ).format(sql.Identifier(vector_schema)), (settings.bot_id,),
                )).fetchall()
                receipts = await (await conn.execute(
                    sql.SQL("SELECT count(*) AS count FROM {}.query_match_receipts WHERE bot_id=%s")
                    .format(sql.Identifier(memory_schema)), (settings.bot_id,),
                )).fetchone()
            assert {(row["modality"], row["state"]) for row in query_facets} >= {
                ("TEXT", "READY"), ("IMAGE", "READY")
            }
            assert {row["index_ref_id"] for row in query_facets} <= {
                row["ref_id"] for row in active_refs
            }
            assert receipts["count"] >= 2
        if scenario == "visual":
            assert b"image_url" in prepared.body
            assert manifest["media_transform_refs"]
            conversation_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "conversation.scheduler"
            )
            async with host.store.pool.connection() as conn:
                media_cost = await (await conn.execute(
                    sql.SQL("SELECT count(*) AS items,sum(byte_length) AS bytes "
                            "FROM {}.context_media_costs WHERE bot_id=%s")
                    .format(sql.Identifier(conversation_schema)), (settings.bot_id,),
                )).fetchone()
                active_media = await (await conn.execute(
                    sql.SQL("SELECT max(context_view_media_bytes) AS bytes "
                            "FROM {}.conversation_lanes WHERE bot_id=%s")
                    .format(sql.Identifier(conversation_schema)), (settings.bot_id,),
                )).fetchone()
            assert media_cost["items"] >= 1
            assert media_cost["bytes"] >= len(b"fake-png-bytes")
            assert active_media["bytes"] >= len(b"fake-png-bytes")
        else:
            assert b"image_url" not in prepared.body
        if scenario == "tool-continuation":
            for message_id, user_id, body, mention in (
                (90002, 20002, "Ordinary before next trigger", False),
                (90003, 20003, "Queued next trigger", True),
                (90004, 20004, "Ordinary tail after trigger", False),
            ):
                await clients[-1].handler(json.dumps({
                    "self_id": int(settings.onebot_self_id),
                    "post_type": "message", "message_type": "group",
                    "sub_type": "normal",
                    "time": int(datetime.now(timezone.utc).timestamp()),
                    "message_id": message_id, "user_id": user_id,
                    "group_id": 933179609,
                    "sender": {"user_id": user_id, "nickname": f"Member {user_id}",
                               "card": "", "role": "member"},
                    "message": [
                        *([{"type": "at", "data": {"qq": settings.onebot_self_id}}]
                          if mention else []),
                        {"type": "text", "data": {"text": body}},
                    ],
                }).encode())
            conversation_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "conversation.scheduler"
            )
            async with asyncio.timeout(30):
                while True:
                    async with host.store.pool.connection() as conn:
                        lane = await (await conn.execute(
                            sql.SQL("SELECT classified_through_seq FROM {}.conversation_lanes "
                                    "WHERE bot_id=%s").format(sql.Identifier(conversation_schema)),
                            (settings.bot_id,),
                        )).fetchone()
                    if lane is not None and lane["classified_through_seq"] >= 4:
                        break
                    await asyncio.sleep(0.1)
            assert len(RecordingModelClient.requests) == 1
            assert b"Queued next trigger" not in prepared.body
            RecordingModelClient.first_gate.set()
            try:
                async with asyncio.timeout(70):
                    while len(RecordingModelClient.requests) < 2:
                        await asyncio.sleep(0.1)
            except TimeoutError as exc:
                agent_schema = next(
                    item.storage.spec.namespace for item in host.registry.descriptors
                    if item.component_id == "agent.runtime"
                )
                async with host.store.pool.connection() as conn:
                    async def rows(namespace, query):
                        return await (await conn.execute(
                            sql.SQL(query).format(sql.Identifier(namespace)),
                        )).fetchall()

                    diagnostics = {
                        "runs": await rows(agent_schema,
                            "SELECT state,error_code,next_step_seq,input_revision FROM {}.agent_runs"),
                        "requests": await rows(agent_schema,
                            "SELECT state,step_seq,request_kind FROM {}.model_requests"),
                        "turns": await rows(agent_schema,
                            "SELECT step_seq,terminal FROM {}.model_turns"),
                        "tools": await rows(agent_schema,
                            "SELECT state,error_code,tool_name FROM {}.tool_calls"),
                        "protocol_failures": await rows(agent_schema,
                            "SELECT error_code FROM {}.protocol_failures"),
                        "plans": await rows(conversation_schema,
                            "SELECT step_seq,selected_bitmap FROM {}.context_model_view_plans"),
                        "settlements": await rows(conversation_schema,
                            "SELECT input_payload->'outcome' AS outcome "
                            "FROM {}.conversation_agent_work_settlements "
                            "ORDER BY created_at DESC LIMIT 5"),
                    }
                raise AssertionError(f"Tool continuation stalled: {diagnostics}") from exc
            assert len(RecordingModelClient.requests) == 2
            continuation = RecordingModelClient.requests[1]
            continuation.verify()
            assert continuation.manifest["run_id"] == prepared.manifest["run_id"]
            assert continuation.manifest["model_view_plan_ref"] != manifest["model_view_plan_ref"]
            assert b"Ordinary before next trigger" in continuation.body
            assert b"Queued next trigger" in continuation.body
            assert b"Ordinary tail after trigger" not in continuation.body
            sources = continuation.manifest["source_manifest"]
            assert {"qq:20001", "qq:20002", "qq:20003"} <= {
                item["author"]["principal_ref"] for item in sources
            }
            assert any(item["source"]["source_kind"] == "agent.model-turn" for item in sources)
            assert any(item["source"]["source_kind"] == "agent.tool-result" for item in sources)
            async with host.store.pool.connection() as conn:
                plans = await (await conn.execute(
                    sql.SQL("SELECT step_seq,selected_bitmap FROM {}.context_model_view_plans "
                            "WHERE bot_id=%s ORDER BY step_seq")
                    .format(sql.Identifier(conversation_schema)),
                    (settings.bot_id,),
                )).fetchall()
            assert [row["step_seq"] for row in plans] == [1, 2]
            assert all(
                old == "0" or new == "1"
                for old, new in zip(plans[0]["selected_bitmap"],
                                    plans[1]["selected_bitmap"], strict=False)
            )
            async with asyncio.timeout(35):
                while not clients[-1].sent:
                    await asyncio.sleep(0.1)
            assert len(clients[-1].sent) == 1
            assert b"Forest tool continuation fixture" in clients[-1].sent[0].wire_body
            async with asyncio.timeout(35):
                while True:
                    async with host.store.pool.connection() as conn:
                        lane = await (await conn.execute(
                            sql.SQL("SELECT context_applied_seq FROM {}.conversation_lanes "
                                    "WHERE bot_id=%s").format(sql.Identifier(conversation_schema)),
                            (settings.bot_id,),
                        )).fetchone()
                    if lane is not None and lane["context_applied_seq"] >= 4:
                        break
                    await asyncio.sleep(0.1)
            assert len(RecordingModelClient.requests) == 2
        if scenario == "long-model-lease":
            agent_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "agent.runtime"
            )

            async def owner_lease():
                async with host.store.pool.connection() as conn:
                    return await (await conn.execute(
                        sql.SQL("SELECT owner_id,owner_epoch,owner_expires_at "
                                "FROM {}.agent_runs WHERE bot_id=%s")
                        .format(sql.Identifier(agent_schema)), (settings.bot_id,),
                    )).fetchone()

            initial_owner = await owner_lease()
            assert initial_owner["owner_id"] is not None
            async with asyncio.timeout(160):
                while True:
                    renewed_owner = await owner_lease()
                    assert renewed_owner["owner_id"] == initial_owner["owner_id"]
                    assert renewed_owner["owner_epoch"] == initial_owner["owner_epoch"]
                    if renewed_owner["owner_expires_at"] > (
                        initial_owner["owner_expires_at"] + timedelta(seconds=90)
                    ):
                        break
                    await asyncio.sleep(0.2)
            assert len(RecordingModelClient.requests) == 1
            RecordingModelClient.first_gate.set()
        if scenario == "reload":
            old_graph = host.registry
            agent = old_graph.get("agent.runtime")
            signalled = asyncio.Event()
            guard_observed = asyncio.Event()
            begin_replacement = agent.begin_replacement
            validate_replacement = agent.validate_replacement
            agent_schema = next(
                item.storage.spec.namespace for item in old_graph.descriptors
                if item.component_id == "agent.runtime"
            )

            async def tracked_replacement(operation_id):
                await begin_replacement(operation_id)
                signalled.set()

            async def tracked_validation(replaced_components, candidate_components):
                async with host.store.pool.connection() as conn:
                    parked = await (await conn.execute(
                        sql.SQL(
                            "SELECT state,reload_operation_id,reload_generation_id "
                            "FROM {}.agent_runs WHERE bot_id=%s"
                        ).format(sql.Identifier(agent_schema)),
                        (settings.bot_id,),
                    )).fetchone()
                assert parked == {
                    "state": "RELOAD_WAIT",
                    "reload_operation_id": reload_operation_id,
                    "reload_generation_id": agent.generation_identity.generation_id,
                }
                assert len(RecordingModelClient.requests) == 1
                guard_observed.set()
                await validate_replacement(replaced_components, candidate_components)

            agent.begin_replacement = tracked_replacement
            agent.validate_replacement = tracked_validation
            reload_operation_id = "runtime-reload/agent-inflight-test"
            drain = None
            try:
                drain = asyncio.create_task(
                    old_graph.drain_owned_work_for_replacement(
                        reload_operation_id,
                        candidate_components={
                            "provider.chat-completions": {
                                "generation_id": "generation/provider/incompatible",
                                "facts": {
                                    "forest.provider-profile/1": {
                                        "profile_ref": "profile:" + "0" * 64,
                                        "capability_digest": "sha256:" + "0" * 64,
                                        "model_ref": "incompatible",
                                    },
                                },
                            }
                        },
                    )
                )
                async with asyncio.timeout(30):
                    while not signalled.is_set():
                        if drain.done():
                            await drain
                        await asyncio.sleep(0.05)
                assert not drain.done()
                RecordingModelClient.first_gate.set()
                with pytest.raises(ForestError) as rejected:
                    async with asyncio.timeout(60):
                        await drain
                assert rejected.value.code == "RUNTIME_REPLACEMENT_GUARD_REJECTED"
                assert guard_observed.is_set()
                assert host.registry is old_graph
            finally:
                RecordingModelClient.first_gate.set()
                if drain is not None and not drain.done():
                    drain.cancel()
                    await asyncio.gather(drain, return_exceptions=True)
            async with asyncio.timeout(45):
                while len(RecordingModelClient.requests) < 2:
                    await asyncio.sleep(0.1)
            async with host.store.pool.connection() as conn:
                resumed = await (await conn.execute(
                    sql.SQL(
                        "SELECT state,reload_operation_id FROM {}.agent_runs "
                        "WHERE bot_id=%s"
                    ).format(sql.Identifier(agent_schema)),
                    (settings.bot_id,),
                )).fetchone()
            assert resumed["reload_operation_id"] is None
        if scenario in {"reload-publish", "reload-full", "reload-lost-receipt"}:
            old_graph = host.registry
            old_agent = old_graph.get("agent.runtime")
            old_provider_generation = old_graph.get(
                "provider.chat-completions"
            ).generation_identity.generation_id
            signalled = asyncio.Event()
            drained = asyncio.Event()
            publish_gate = asyncio.Event()
            begin_replacement = old_agent.begin_replacement
            drain_owned_work = old_graph.drain_owned_work_for_replacement

            async def tracked_replacement(operation_id):
                await begin_replacement(operation_id)
                signalled.set()

            async def tracked_drain(*args, **kwargs):
                await drain_owned_work(*args, **kwargs)
                drained.set()
                await publish_gate.wait()

            old_agent.begin_replacement = tracked_replacement
            old_graph.drain_owned_work_for_replacement = tracked_drain
            lost_receipt = asyncio.Event()
            if scenario == "reload-lost-receipt":
                original_execute = PostgresPackageUnitOfWorkCoordinator.execute

                async def drop_inheritance_receipt(
                    coordinator, scope, declaration, payload, **kwargs
                ):
                    result = await original_execute(
                        coordinator, scope, declaration, payload, **kwargs
                    )
                    if (declaration["name"] == "agent.inherit-model-request"
                            and not lost_receipt.is_set()):
                        victim = host.registry.get("agent.runtime")._worker
                        assert victim.alive
                        victim.process.kill()
                        lost_receipt.set()
                        raise ForestError(
                            "TEST_INHERITANCE_RECEIPT_LOST",
                            "Agent Worker died after inheritance commit", 503,
                        )
                    return result

                monkeypatch.setattr(
                    PostgresPackageUnitOfWorkCoordinator, "execute",
                    drop_inheritance_receipt,
                )
            replacement_task = asyncio.create_task(
                host.replace_runtime_graph(
                    target_component_id=(
                        "agent.runtime" if scenario == "reload-publish" else None
                    )
                )
            )
            try:
                async with asyncio.timeout(90):
                    while not signalled.is_set():
                        if replacement_task.done():
                            await replacement_task
                        await asyncio.sleep(0.05)
                RecordingModelClient.first_gate.set()
                async with asyncio.timeout(90):
                    while not drained.is_set():
                        if replacement_task.done():
                            await replacement_task
                        await asyncio.sleep(0.05)
                agent_schema = next(
                    item.storage.spec.namespace for item in old_graph.descriptors
                    if item.component_id == "agent.runtime"
                )
                async with host.store.pool.connection() as conn:
                    parked = await (await conn.execute(
                        sql.SQL(
                            "SELECT state,reload_operation_id,reload_generation_id "
                            "FROM {}.agent_runs WHERE bot_id=%s"
                        ).format(sql.Identifier(agent_schema)),
                        (settings.bot_id,),
                    )).fetchone()
                assert parked["state"] == "RELOAD_WAIT"
                assert parked["reload_operation_id"]
                assert parked["reload_generation_id"] == (
                    old_agent.generation_identity.generation_id
                )
                assert len(RecordingModelClient.requests) == 1
                publish_gate.set()
                async with asyncio.timeout(90):
                    replacement = await replacement_task
                assert host.registry is replacement
                assert replacement.get("agent.runtime").generation_identity.generation_id != (
                    parked["reload_generation_id"]
                )
                if scenario in {"reload-full", "reload-lost-receipt"}:
                    assert replacement.get(
                        "provider.chat-completions"
                    ).generation_identity.generation_id != old_provider_generation
                if scenario == "reload-lost-receipt":
                    async with asyncio.timeout(60):
                        await lost_receipt.wait()
                    agent_schema = next(
                        item.storage.spec.namespace for item in replacement.descriptors
                        if item.component_id == "agent.runtime"
                    )
                    async with host.store.pool.connection() as conn:
                        inherited = await (await conn.execute(
                            sql.SQL(
                                "SELECT request_id,state FROM {}.model_requests "
                                "WHERE bot_id=%s ORDER BY request_revision"
                            ).format(sql.Identifier(agent_schema)),
                            (settings.bot_id,),
                        )).fetchall()
                    assert len(inherited) == 2
                    assert [item["state"] for item in inherited] == ["ABANDONED", "READY"]
                    assert len(RecordingModelClient.requests) == 1
                    runtime_schema = host._kernel.runtime.namespace
                    try:
                        await host.close()
                    except ForestError as exc:
                        assert exc.code == "RUNTIME_MODULE_CLEANUP_FAILED"
                        assert exc.details["errors"]
                    # A forced crash leaves the old HostLease fenced until its
                    # database deadline. Observe that deadline; never rewrite it.
                    observer = await psycopg.AsyncConnection.connect(uri)
                    try:
                        async with asyncio.timeout(180):
                            while True:
                                lease = await (await observer.execute(
                                    sql.SQL(
                                        "SELECT expires_at<=clock_timestamp() AS expired "
                                        "FROM {}.host_lease WHERE bot_id=%s"
                                    ).format(sql.Identifier(runtime_schema)),
                                    (settings.bot_id,),
                                )).fetchone()
                                assert lease is not None
                                if lease[0]:
                                    break
                                await asyncio.sleep(0.5)
                    finally:
                        await observer.close()
                    host = create_host("recall-restarted-packages")
                    await host.start()
                    conversation_schema = next(
                        item.storage.spec.namespace for item in host.registry.descriptors
                        if item.component_id == "conversation.scheduler"
                    )
                    async with asyncio.timeout(45):
                        while True:
                            async with host.store.pool.connection() as conn:
                                settled = await (await conn.execute(
                                    sql.SQL(
                                        "SELECT input_payload->'outcome'->'result'->>"
                                        "'agent_status' AS status FROM {}."
                                        "conversation_agent_work_settlements "
                                        "ORDER BY created_at DESC LIMIT 1"
                                    ).format(sql.Identifier(conversation_schema)),
                                )).fetchone()
                            if settled is not None and settled["status"] == "WAIT_OWNER":
                                break
                            await asyncio.sleep(0.2)
                    assert len(RecordingModelClient.requests) == 1
                    async with asyncio.timeout(360):
                        while len(RecordingModelClient.requests) < 2:
                            await asyncio.sleep(0.5)
                    successor = RecordingModelClient.requests[1]
                    async with host.store.pool.connection() as conn:
                        lineage = await (await conn.execute(
                            sql.SQL(
                                "SELECT request_id,predecessor_request_id,"
                                "request_revision,state FROM {}.model_requests "
                                "WHERE bot_id=%s ORDER BY request_revision"
                            ).format(sql.Identifier(agent_schema)),
                            (settings.bot_id,),
                        )).fetchall()
                    assert len(lineage) == 3
                    assert [item["request_revision"] for item in lineage] == [1, 2, 3]
                    assert [item["state"] for item in lineage[:2]] == [
                        "ABANDONED", "ABANDONED"
                    ]
                    assert lineage[1]["request_id"] == inherited[1]["request_id"]
                    assert lineage[2]["predecessor_request_id"] == inherited[1]["request_id"]
                    assert successor.request_id == lineage[2]["request_id"]
                    assert successor.body == prepared.body
                    assert successor.payload_digest == prepared.payload_digest
                    return
                try:
                    async with asyncio.timeout(45):
                        while len(RecordingModelClient.requests) < 2:
                            await asyncio.sleep(0.1)
                except TimeoutError as error:
                    conversation_schema = next(
                        item.storage.spec.namespace for item in replacement.descriptors
                        if item.component_id == "conversation.scheduler"
                    )
                    async with host.store.pool.connection() as conn:
                        run_rows = await (await conn.execute(
                            sql.SQL(
                                "SELECT state,owner_id,reload_operation_id,error_code,"
                                "current_model_request_id FROM {}.agent_runs WHERE bot_id=%s"
                            ).format(sql.Identifier(agent_schema)),
                            (settings.bot_id,),
                        )).fetchall()
                        lanes = await (await conn.execute(
                            sql.SQL(
                                "SELECT active_run_ref,resume_reason_code,resume_not_before "
                                "FROM {}.conversation_lanes WHERE bot_id=%s"
                            ).format(sql.Identifier(conversation_schema)),
                            (settings.bot_id,),
                        )).fetchall()
                        settlements = await (await conn.execute(
                            sql.SQL(
                                "SELECT input_payload->'outcome' AS outcome FROM {}."
                                "conversation_agent_work_settlements "
                                "ORDER BY created_at DESC LIMIT 5"
                            ).format(sql.Identifier(conversation_schema)),
                        )).fetchall()
                    failures = {
                        component_id: work.failure
                        for component_id, instance in replacement.instances.items()
                        if (work := getattr(instance, "_owned_work", None)) is not None
                        and work.failure is not None
                    }
                    raise AssertionError(
                        f"Published Agent did not dispatch: runs={run_rows}, "
                        f"lanes={lanes}, settlements={settlements}, "
                        f"owned_failures={failures}"
                    ) from error
                async with host.store.pool.connection() as conn:
                    resumed = await (await conn.execute(
                        sql.SQL(
                            "SELECT state,reload_operation_id FROM {}.agent_runs "
                            "WHERE bot_id=%s"
                        ).format(sql.Identifier(agent_schema)),
                        (settings.bot_id,),
                    )).fetchone()
                assert resumed["reload_operation_id"] is None
                if scenario == "reload-full":
                    async with asyncio.timeout(45):
                        while len(RecordingModelClient.requests) < 3:
                            await asyncio.sleep(0.1)
                    first, second = RecordingModelClient.requests[:2]
                    third = RecordingModelClient.requests[2]
                    assert first.request_id != second.request_id
                    assert second.request_id == third.request_id
                    assert first.body == second.body
                    assert second.body == third.body
                    assert first.payload_digest == second.payload_digest
                    async with host.store.pool.connection() as conn:
                        lineage = await (await conn.execute(
                            sql.SQL(
                                "SELECT request_id,request_revision,state "
                                "FROM {}.model_requests WHERE bot_id=%s "
                                "ORDER BY request_revision"
                            ).format(sql.Identifier(agent_schema)),
                            (settings.bot_id,),
                        )).fetchall()
                    assert [(row["request_revision"], row["state"]) for row in lineage][:1] == [
                        (1, "ABANDONED")
                    ]
                    assert lineage[1]["request_id"] == second.request_id
            finally:
                RecordingModelClient.first_gate.set()
                publish_gate.set()
                if not replacement_task.done():
                    replacement_task.cancel()
                    await asyncio.gather(replacement_task, return_exceptions=True)
        if scenario == "requeue":
            class RequeueWebSupervisor:
                async def management_scopes(self):
                    return ({"scope_kind": "bot", "scope_id": settings.bot_id},)

                @asynccontextmanager
                async def management_host(self, scope_id):
                    assert scope_id == settings.bot_id
                    yield host

            async with _real_web_identity_for_bot_test(dsn) as identity:
                web_app = create_deployment_management_app(
                    deployment_id="deployment/host-web-test",                     identity_authority=identity, allowed_origin="http://forest.test",
                    bot_supervisor=RequeueWebSupervisor(),
                )
                base = (
                    f"/management/v1/scopes/bot/{settings.bot_id}/"
                    "modules/conversation.scheduler"
                )
                async with AsyncClient(
                    transport=ASGITransport(app=web_app), base_url="http://forest.test",
                ) as web:
                    login = await web.post(
                        "/management/v1/auth/login",
                        headers={"origin": "http://forest.test"},
                        json={"username": "admin", "password": "host-web-password-1"},
                    )
                    assert login.status_code == 200, login.text
                    headers = {
                        "origin": "http://forest.test",
                        "x-forest-csrf": web.cookies["forest_management_csrf"],
                    }
                    candidates = await web.post(
                        f"{base}/queries/conversation.requeue.candidates",
                        headers=headers, json={"limit": 20},
                    )
                    assert candidates.status_code == 200, candidates.text
                    items = candidates.json()["items"]
                    assert len(items) == 1
                    assert items[0]["channel_ref"] == "933179609"
                    assert items[0]["request_state"] is None
                    body = {
                        "conversation_id": items[0]["conversation_id"],
                        "run_ref": items[0]["run_ref"],
                        "reason_code": "operator.retry-unsent",
                    }
                    request_url = f"{base}/actions/conversation.requeue.request"
                    action_headers = {**headers, "idempotency-key": "requeue-unsent-run"}
                    accepted = await web.post(
                        request_url, headers=action_headers, json=body,
                    )
                    assert accepted.status_code == 200, accepted.text
                    replay = await web.post(
                        request_url, headers=action_headers, json=body,
                    )
                    assert replay.status_code == 200, replay.text
                    assert replay.json()["request_id"] == accepted.json()["request_id"]
                    status_url = f"{base}/queries/conversation.requeue.status"
                    status_input = {key: body[key] for key in ("conversation_id", "run_ref")}
                    pending = await web.post(
                        status_url, headers=headers, json=status_input,
                    )
                    assert pending.status_code == 200, pending.text
                    assert pending.json()["state"] == "PENDING"
                    RecordingModelClient.first_gate.set()
                    try:
                        async with asyncio.timeout(35):
                            while True:
                                response = await web.post(
                                    status_url, headers=headers, json=status_input,
                                )
                                assert response.status_code == 200, response.text
                                state = response.json()
                                if state["state"] == "APPLIED":
                                    break
                                assert state["state"] != "REJECTED", state
                                await asyncio.sleep(0.2)
                    except TimeoutError as exc:
                        namespaces = {
                            item.component_id: item.storage.spec.namespace
                            for item in host.registry.descriptors if item.storage is not None
                        }
                        async with host.store.pool.connection() as conn:
                            runs = await (await conn.execute(
                                sql.SQL("SELECT state,error_code,current_model_request_id "
                                        "FROM {}.agent_runs WHERE bot_id=%s")
                                .format(sql.Identifier(namespaces["agent.runtime"])),
                                (settings.bot_id,),
                            )).fetchall()
                            requests = await (await conn.execute(
                                sql.SQL("SELECT state,attempt_count,current_attempt_id "
                                        "FROM {}.model_requests WHERE bot_id=%s")
                                .format(sql.Identifier(namespaces["agent.runtime"])),
                                (settings.bot_id,),
                            )).fetchall()
                            attempts = await (await conn.execute(
                                sql.SQL("SELECT state,attempt_no FROM {}.model_transport_attempts "
                                        "WHERE bot_id=%s ORDER BY attempt_no")
                                .format(sql.Identifier(namespaces["agent.runtime"])),
                                (settings.bot_id,),
                            )).fetchall()
                            settlements = await (await conn.execute(
                                sql.SQL("SELECT input_payload->'outcome' AS outcome FROM {}."
                                        "conversation_agent_work_settlements "
                                        "ORDER BY created_at DESC LIMIT 5")
                                .format(sql.Identifier(namespaces["conversation.scheduler"])),
                            )).fetchall()
                            lanes = await (await conn.execute(
                                sql.SQL("SELECT active_run_ref,resume_reason_code "
                                        "FROM {}.conversation_lanes WHERE bot_id=%s")
                                .format(sql.Identifier(namespaces["conversation.scheduler"])),
                                (settings.bot_id,),
                            )).fetchall()
                        raise AssertionError(
                            f"Requeue did not settle: request={state}, runs={runs}, "
                            f"models={requests}, attempts={attempts}, "
                            f"settlements={settlements}, lanes={lanes}, "
                            f"worker_failure={host.registry.get('conversation.scheduler')._owned_work.failure}"
                        ) from exc
                    assert state["request_id"] == accepted.json()["request_id"]
                    agent_schema = next(
                        item.storage.spec.namespace for item in host.registry.descriptors
                        if item.component_id == "agent.runtime"
                    )
                    async with host.store.pool.connection() as conn:
                        failed = await (await conn.execute(
                            sql.SQL("SELECT state,error_code FROM {}.agent_runs "
                                    "WHERE bot_id=%s AND run_id=%s")
                            .format(sql.Identifier(agent_schema)),
                            (settings.bot_id, body["run_ref"]),
                        )).fetchone()
                    assert failed == {
                        "state": "FAILED", "error_code": "AGENT_MODEL_TRANSPORT_FAILED"
                    }
                    assert len(RecordingModelClient.requests) == 3
                    assert len({item.payload_digest for item in RecordingModelClient.requests}) == 1
                    assert all(
                        item.body == prepared.body
                        for item in RecordingModelClient.requests
                    )
        if scenario == "transport-exhausted":
            agent_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "agent.runtime"
            )
            conversation_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "conversation.scheduler"
            )
            async with asyncio.timeout(50):
                while True:
                    async with host.store.pool.connection() as conn:
                        run = await (await conn.execute(
                            sql.SQL("SELECT state,error_code FROM {}.agent_runs "
                                    "WHERE bot_id=%s ORDER BY created_at DESC LIMIT 1")
                            .format(sql.Identifier(agent_schema)),
                            (settings.bot_id,),
                        )).fetchone()
                        claim = await (await conn.execute(
                            sql.SQL("SELECT claim_state,terminal_status FROM {}."
                                    "trigger_claim_groups WHERE bot_id=%s "
                                    "LIMIT 1")
                            .format(sql.Identifier(conversation_schema)),
                            (settings.bot_id,),
                        )).fetchone()
                        lane = await (await conn.execute(
                            sql.SQL("SELECT active_run_ref FROM {}.conversation_lanes "
                                    "WHERE bot_id=%s")
                            .format(sql.Identifier(conversation_schema)),
                            (settings.bot_id,),
                        )).fetchone()
                    if (run is not None and run["state"] == "FAILED"
                            and claim is not None and claim["claim_state"] == "TERMINAL"):
                        break
                    await asyncio.sleep(0.1)
            assert run["error_code"] == "AGENT_MODEL_TRANSPORT_FAILED"
            assert claim["terminal_status"] == "FAILED"
            assert lane["active_run_ref"] is None
            assert len(RecordingModelClient.requests) == 3
        if scenario == "cross-group":
            await clients[-1].handler(json.dumps({
                "self_id": int(settings.onebot_self_id), "post_type": "message",
                "message_type": "group", "sub_type": "normal",
                "time": int(datetime.now(timezone.utc).timestamp()),
                "message_id": 90003, "user_id": 20003, "group_id": 933179609,
                "sender": {"user_id": 20003, "nickname": "Queued tester",
                           "card": "", "role": "member"},
                "message": [
                    {"type": "at", "data": {"qq": settings.onebot_self_id}},
                    {"type": "text", "data": {"text": "Queued for the same group"}},
                ],
            }).encode())
            await clients[-1].handler(json.dumps({
                "self_id": int(settings.onebot_self_id), "post_type": "message",
                "message_type": "group", "sub_type": "normal",
                "time": int(datetime.now(timezone.utc).timestamp()),
                "message_id": 90002, "user_id": 20002, "group_id": 933179610,
                "sender": {"user_id": 20002, "nickname": "Other group tester",
                           "card": "", "role": "member"},
                "message": [
                    {"type": "at", "data": {"qq": settings.onebot_self_id}},
                    {"type": "text", "data": {"text": "What about this group?"}},
                ],
            }).encode())
            async with asyncio.timeout(35):
                while len(RecordingModelClient.requests) < 2:
                    await asyncio.sleep(0.1)
            second = RecordingModelClient.requests[1]
            second.verify()
            assert len(RecordingModelClient.requests) == 2
            assert b"Queued for the same group" not in prepared.body
            assert second.manifest["memory_evidence"]["current_source"][
                "external_group_id"
            ] == "933179610"
            assert second.manifest["memory_evidence"]["status"] == "READY"
            RecordingModelClient.first_gate.set()
            async with asyncio.timeout(45):
                while len(RecordingModelClient.requests) < 3:
                    await asyncio.sleep(0.1)
            queued = RecordingModelClient.requests[2]
            queued.verify()
            assert queued.manifest["memory_evidence"]["current_source"][
                "external_group_id"
            ] == "933179609"
            assert b"Queued for the same group" in queued.body
            assert any(
                item["author"]["principal_ref"] == "qq:20003"
                for item in queued.manifest["source_manifest"]
            )
        if scenario in {"success", "result-ledger-recovery", "result-payload-recovery"}:
            try:
                async with asyncio.timeout(
                    90 if scenario in {"result-ledger-recovery", "result-payload-recovery"} else 15
                ):
                    while not clients[-1].sent:
                        await asyncio.sleep(0.1)
            except TimeoutError as exc:
                namespaces = {
                    item.component_id: item.storage.spec.namespace
                    for item in host.registry.descriptors
                    if item.storage is not None
                }
                async with host.store.pool.connection() as conn:
                    async def rows(component, query):
                        result = await conn.execute(
                            sql.SQL(query).format(sql.Identifier(namespaces[component]))
                        )
                        return await result.fetchall()

                    diagnostics = {
                        "runs": await rows("agent.runtime", "SELECT state, next_step_seq, current_model_request_id FROM {}.agent_runs"),
                        "requests": await rows("agent.runtime", "SELECT state FROM {}.model_requests"),
                        "attempts": await rows("agent.runtime", "SELECT state FROM {}.model_transport_attempts"),
                        "intents": await rows("delivery", "SELECT state, error_code FROM {}.send_intents"),
                        "settlements": await rows("conversation.scheduler", "SELECT input_payload->'outcome'->'error'->>'code' AS code FROM {}.conversation_agent_work_settlements ORDER BY created_at DESC LIMIT 5"),
                    }
                raise AssertionError(f"Successful model response did not deliver: {diagnostics}") from exc
            assert len(clients[-1].sent) == 1
            assert b"Forest success fixture" in clients[-1].sent[0].wire_body
            if scenario == "result-ledger-recovery":
                assert result_observation_interrupted.is_set()
                assert len(RecordingModelClient.requests) == 1
            if scenario == "result-payload-recovery":
                assert result_ledger_interrupted.is_set()
                assert len(RecordingModelClient.requests) == 1
            agent_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "agent.runtime"
            )
            if scenario == "success":
                conversation_schema = next(
                    item.storage.spec.namespace for item in host.registry.descriptors
                    if item.component_id == "conversation.scheduler"
                )
                try:
                    async with asyncio.timeout(20):
                        while True:
                            async with host.store.pool.connection() as conn:
                                session = await (await conn.execute(
                                    sql.SQL("SELECT state FROM {}.agent_sessions "
                                            "WHERE bot_id=%s ORDER BY created_at DESC LIMIT 1")
                                    .format(sql.Identifier(agent_schema)),
                                    (settings.bot_id,),
                                )).fetchone()
                                lane = await (await conn.execute(
                                    sql.SQL("SELECT active_session_ref FROM {}.conversation_lanes "
                                            "WHERE bot_id=%s")
                                    .format(sql.Identifier(conversation_schema)),
                                    (settings.bot_id,),
                                )).fetchone()
                            if (session is not None and session["state"] == "CLOSED"
                                    and lane is not None
                                    and lane["active_session_ref"] is None):
                                break
                            await asyncio.sleep(0.1)
                except TimeoutError as exc:
                    async with host.store.pool.connection() as conn:
                        settlements = await (await conn.execute(
                            sql.SQL("SELECT input_payload->'outcome' AS outcome "
                                    "FROM {}.conversation_agent_work_settlements "
                                    "ORDER BY created_at DESC LIMIT 5")
                            .format(sql.Identifier(conversation_schema)),
                        )).fetchall()
                    raise AssertionError(
                        f"Session handoff did not close: session={session}, lane={lane}, "
                        f"settlements={settlements}, "
                        f"worker_failure={host.registry.get('conversation.scheduler')._owned_work.failure}"
                    ) from exc
            async with host.store.pool.connection() as conn:
                delivered_attempt = await (await conn.execute(sql.SQL(
                    "SELECT r.request_id,a.attempt_id,a.effect_operation_ref "
                    "FROM {}.model_transport_attempts a JOIN {}.model_requests r "
                    "ON r.bot_id=a.bot_id AND r.request_id=a.request_id "
                    "WHERE a.bot_id=%s AND a.state='SUCCEEDED'"
                ).format(sql.Identifier(agent_schema), sql.Identifier(agent_schema)),
                    (settings.bot_id,))).fetchone()
            assert delivered_attempt is not None
            outcome_query = effect_executor_outcome_service(
                RuntimeKernelServiceContext(
                    host.registry,
                    ("kernel.runtime", "kernel.effect", "kernel.payload"),
                )
            )
            stored_outcome = await outcome_query({
                "protocol": "forest.effect-executor-outcome-request/1",
                "effect_operation_id": delivered_attempt["effect_operation_ref"],
                "attempt_id": delivered_attempt["attempt_id"],
            })
            assert stored_outcome["state"] == (
                "ACTIVE" if scenario == "result-payload-recovery" else "SUCCEEDED"
            )
            assert stored_outcome["request_id"] == delivered_attempt["request_id"]
            if scenario == "result-payload-recovery":
                assert stored_outcome["receipt"] is None
            else:
                assert stored_outcome["receipt"]["status"] == "RESPONDED"
            conversation_schema = next(
                item.storage.spec.namespace for item in host.registry.descriptors
                if item.component_id == "conversation.scheduler"
            )
            async with asyncio.timeout(20):
                while True:
                    async with host.store.pool.connection() as conn:
                        physical = await (
                            await conn.execute(
                                sql.SQL("SELECT count(*) AS total,"
                                        "count(*) FILTER (WHERE status='ANSWERED') AS answered "
                                        "FROM {}.conversation_triggers WHERE bot_id=%s")
                                .format(sql.Identifier(conversation_schema)),
                                (settings.bot_id,),
                            )
                        ).fetchone()
                        pending = await (
                            await conn.execute(
                                sql.SQL("SELECT count(*) AS count FROM {}."
                                        "trigger_settlement_progress WHERE bot_id=%s "
                                        "AND completed_at IS NULL")
                                .format(sql.Identifier(conversation_schema)),
                                (settings.bot_id,),
                            )
                        ).fetchone()
                    if (
                        physical["total"] >= 1
                        and physical["answered"] == physical["total"]
                        and pending["count"] == 0
                    ):
                        break
                    await asyncio.sleep(0.1)
    finally:
        if RecordingModelClient.first_gate is not None:
            RecordingModelClient.first_gate.set()
        RecordingModelClient.first_gate = None
        RecordingModelClient.reply_text = None
        RecordingModelClient.reply_factory = None
        RecordingModelClient.not_sent_attempts = 0
        await host.close()
        if real_transport:
            profile = EmbeddingProfile(
                deployment_id=settings.embedding_deployment_id,
                model_revision=settings.embedding_model_revision,
                model=settings.embedding_model,
                dimensions=settings.embedding_dimensions,
            )
            collection_name = vector_collection_name(
                settings.bot_id, profile.space_id
            )
            client = AsyncMilvusClient(uri=settings.vector_index_endpoint)
            try:
                if await client.has_collection(collection_name=collection_name):
                    await client.drop_collection(collection_name=collection_name)
            finally:
                await client.close()
        assert name.startswith("forest_recall_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_real_vl_large_image_auto_archive_cross_group_recall_and_management_trial(
    tmp_path,
):
    dsn = os.environ.get("FOREST_TEST_DSN")
    qiyu_env_file = os.environ.get("FOREST_TEST_QIYU_ENV_FILE")
    milvus_uri = os.environ.get("FOREST_TEST_MILVUS_URI")
    if not dsn or not qiyu_env_file or not milvus_uri:
        pytest.skip("Private PostgreSQL, QiYu VL credentials and Milvus are required")
    qiyu = qiyu_embedding_config(qiyu_env_file)
    database = "forest_cross_vl_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + database, "", ""))
    settings = BotSettings(
        bot_id="bot-cross-vl-" + uuid4().hex[:16], dsn=uri,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="test-model-key",
        model="test-model", model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024, model_input_modalities=("text", "image"),
        embedding_endpoint=qiyu["EMBEDDING_BASE_URL"].rstrip("/") + "/embeddings",
        embedding_api_key=qiyu["EMBEDDING_API_KEY"],
        embedding_deployment_id="embedding.multimodal/qiyu-vl/cross-group-test",
        embedding_model_revision="operator-declared/cross-group-test-1",
        embedding_model=qiyu["EMBEDDING_MODEL"], embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609", "933179610"),
        group_trigger_mode="MENTION", private_enabled=False,
        shell_administrator_ids=(), agent_system_prompt="Cross-group VL recall fixture.",
        embedding_timeout_seconds=120, vector_index_endpoint=milvus_uri,
    )
    clients = []
    RecordingModelClient.requests = []
    RecordingModelClient.first_gate = None
    RecordingModelClient.reply_text = (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        '<block block_type="Assistant"><content><text>Cross-group recall complete</text>'
        '</content></block></blocks></payload></valid_agent_protocol_content_block>'
    )

    async def create_websocket(_url, _token, **_limits):
        client = FakeWebSocketClient(settings.onebot_self_id)
        clients.append(client)
        return client

    host = BotHost(
        settings, model_client_factory=RecordingModelClient,
        media_client_factory=LargePngMediaClient,
        websocket_client_factory=create_websocket,
        package_catalog=PackageCatalog(tmp_path / "cross-vl-packages"),
    )
    try:
        await host.start()
        conversation = host.registry.get("conversation.scheduler")
        configuration = conversation.module_configuration
        compact_view = {
            "protocol": "forest.conversation.scheduler/2",
            "context_view": {
                "high_watermark_units": 2,
                "retain_units": 1,
                "high_watermark_payload_bytes": 524288,
                "retain_payload_bytes": 262144,
                "high_watermark_media_bytes": 12582912,
                "retain_media_bytes": 6291456,
                "high_watermark_decoded_bytes": 134217728,
                "retain_decoded_bytes": 67108864,
                "high_watermark_estimated_tokens": 24576,
                "retain_estimated_tokens": 16384,
                "model_view_budget_tokens": 32768,
                "model_view_media_bytes": 25165824,
                "model_view_decoded_bytes": 268435456,
                "utf8_bytes_per_estimated_token": 2,
                "unit_overhead_estimated_tokens": 16,
                "policy_revision": "context-view-multimodal-window-v6",
            },
        }
        plan = await configuration.plan(
            operation_id="cross-vl-compact-view-plan",
            expected_desired_revision=1,
            value=compact_view,
            validation_digest=digest_json(conversation.snapshot.schema("schemas/config.json")),
        )
        await configuration.apply(
            operation_id="cross-vl-compact-view-apply",
            plan_id=plan["plan_id"],
            expected_desired_revision=1,
        )
        await host.replace_runtime_graph()
        memory = host.registry.get("memory.graph")
        namespace = next(
            item.storage.spec.namespace for item in host.registry.descriptors
            if item.component_id == "memory.graph"
        )
        async with asyncio.timeout(90):
            while True:
                async with host.store.pool.connection() as conn:
                    domain = await (await conn.execute(
                        sql.SQL("SELECT state FROM {}.memory_domain_bind_work WHERE bot_id=%s")
                        .format(sql.Identifier(namespace)), (settings.bot_id,),
                    )).fetchone()
                if domain is not None and domain["state"] == "READY":
                    break
                await asyncio.sleep(0.1)

        async def ingest(group_id, user_id, message_id, text_value, *, mention, image=True):
            message = []
            if mention:
                message.append({"type": "at", "data": {"qq": settings.onebot_self_id}})
            message.append({"type": "text", "data": {"text": text_value}})
            if image:
                message.append({"type": "image", "data": {
                    "file": "same-test-image", "url": "https://media.example/same-test-image",
                }})
            await clients[-1].handler(json.dumps({
                "self_id": int(settings.onebot_self_id), "post_type": "message",
                "message_type": "group", "sub_type": "normal",
                "time": int(datetime.now(timezone.utc).timestamp()),
                "message_id": message_id, "user_id": user_id, "group_id": group_id,
                "sender": {"user_id": user_id, "nickname": "Test sender", "card": "",
                           "role": "member"},
                "message": message,
            }).encode())

        await ingest(933179609, 20001, 97001, "The blue glass orb is here.", mention=False)
        for index in range(2, 5):
            await ingest(933179609, 20001, 97000 + index,
                         f"Background conversation {index}.", mention=False, image=False)
        document_unit = "onebot-unit:" + hashlib.sha256(b"binding/main\x0097001").hexdigest()
        async with asyncio.timeout(180):
            while True:
                async with host.store.pool.connection() as conn:
                    source = await (await conn.execute(
                        sql.SQL("SELECT eligibility_state FROM {}.source_units "
                                "WHERE bot_id=%s AND source_unit_id=%s")
                        .format(sql.Identifier(namespace)),
                        (settings.bot_id, document_unit),
                    )).fetchone()
                    document_facets = await (await conn.execute(
                        sql.SQL("SELECT modality,state,index_ref_id FROM {}.embedding_facets "
                                "WHERE bot_id=%s AND purpose='DOCUMENT' AND source_unit_id=%s")
                        .format(sql.Identifier(namespace)),
                        (settings.bot_id, document_unit),
                    )).fetchall()
                if (source is not None and source["eligibility_state"] == "ELIGIBLE"
                        and len(document_facets) == 2
                        and all(row["state"] == "READY" for row in document_facets)):
                    break
                await asyncio.sleep(0.2)
        assert {row["modality"] for row in document_facets} == {"TEXT", "IMAGE"}
        assert all(row["index_ref_id"] for row in document_facets)

        await ingest(933179610, 20002, 97102, "Where is the blue glass orb?", mention=True)
        async with asyncio.timeout(180):
            while not RecordingModelClient.requests:
                await asyncio.sleep(0.2)
        prepared = RecordingModelClient.requests[0]
        prepared.verify()
        assert prepared.manifest["memory_evidence"]["status"] == "READY"
        assert prepared.manifest["memory_evidence"]["current_source"]["external_group_id"] == "933179610"
        historical = [
            item for item in prepared.manifest["source_manifest"]
            if item.get("context_data") == "memory_evidence"
        ]
        assert any(
            item["source_unit_id"] == document_unit
            and item["source"]["external_group_id"] == "933179609"
            and item["author"]["principal_ref"] == "qq:20001"
            for item in historical
        )
        async with host.store.pool.connection() as conn:
            matched_modalities = await (await conn.execute(
                sql.SQL(
                    "SELECT q.modality AS query_modality,d.modality AS document_modality "
                    "FROM {m}.query_seed_matches x "
                    "JOIN {m}.embedding_facets q ON q.bot_id=x.bot_id "
                    "AND q.facet_id=x.query_facet_id "
                    "JOIN {m}.embedding_facets d ON d.bot_id=x.bot_id "
                    "AND d.facet_id=x.document_facet_id "
                    "WHERE x.bot_id=%s AND x.source_unit_id=%s"
                ).format(m=sql.Identifier(namespace)),
                (settings.bot_id, document_unit),
            )).fetchall()
        assert ("IMAGE", "IMAGE") in {
            (row["query_modality"], row["document_modality"])
            for row in matched_modalities
        }
        assert b"image_url" in prepared.body

        trigger_unit = "onebot-unit:" + hashlib.sha256(b"binding/main\x0097102").hexdigest()
        async with asyncio.timeout(60):
            while True:
                async with host.store.pool.connection() as conn:
                    journal = await (await conn.execute(
                        sql.SQL(
                            "SELECT trigger_source_unit_id,seed_source_unit_id,state "
                            "FROM {}.association_journal WHERE bot_id=%s"
                        ).format(sql.Identifier(namespace)), (settings.bot_id,),
                    )).fetchall()
                if clients[-1].sent and journal:
                    break
                await asyncio.sleep(0.2)
        assert len(clients[-1].sent) == 1
        assert any(
            item["trigger_source_unit_id"] == trigger_unit
            and item["seed_source_unit_id"] == document_unit
            and item["state"] == "PENDING_ARCHIVE"
            for item in journal
        )

        await ingest(933179610, 20003, 97103, "Conversation after reply.",
                     mention=False, image=False)
        async with asyncio.timeout(90):
            while True:
                async with host.store.pool.connection() as conn:
                    journal = await (await conn.execute(
                        sql.SQL(
                            "SELECT state,contribution_id FROM {}.association_journal "
                            "WHERE bot_id=%s AND trigger_source_unit_id=%s "
                            "AND seed_source_unit_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (settings.bot_id, trigger_unit, document_unit),
                    )).fetchone()
                    relation = await (await conn.execute(
                        sql.SQL(
                            "SELECT relation_kind,left_id,right_id,cross_trunk "
                            "FROM {}.relation_contributions WHERE bot_id=%s "
                            "AND contribution_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (settings.bot_id, journal["contribution_id"] if journal else None),
                    )).fetchone()
                if journal and journal["state"] == "PROMOTED" and relation:
                    break
                await asyncio.sleep(0.2)
        assert relation["relation_kind"] == "ASSOCIATES_WITH"
        assert {relation["left_id"], relation["right_id"]} == {
            trigger_unit, document_unit,
        }
        assert relation["cross_trunk"] is True

        trial_id = "trial/cross-vl-image"
        trial_actor = "subject/cross-vl-admin"
        generation_id = memory.generation_identity.generation_id
        assets = ManagementAssetAuthority(host._kernel.payload)

        async def upload_trial_part(part_id, modality, media_type, content, width, height):
            content_digest = "sha256:" + hashlib.sha256(content).hexdigest()
            owner_id = "trial:" + hashlib.sha256(
                (trial_id + "\0" + part_id).encode()
            ).hexdigest()
            upload_id = "upload/cross-vl/" + part_id
            async with host.store.pool.connection() as conn, conn.transaction():
                await assets.begin_in(
                    conn, upload_id=upload_id, component_id="memory.graph",
                    asset_id="trial.input", actor_subject_ref=trial_actor,
                    generation_id=generation_id, target_ref=trial_id,
                    part_id=part_id, owner_kind="trial.input", owner_id=owner_id,
                    media_type=media_type, byte_length=len(content),
                    content_digest=content_digest, chunk_count=1,
                    allowed_media_types=(media_type,), max_bytes=8388608,
                )
                await assets.put_chunk_in(
                    conn, upload_id=upload_id, component_id="memory.graph",
                    asset_id="trial.input", actor_subject_ref=trial_actor,
                    generation_id=generation_id, chunk_index=0,
                    content=content, content_digest=content_digest,
                )
                saved = await assets.finalize_in(
                    conn, upload_id=upload_id, component_id="memory.graph",
                    asset_id="trial.input", actor_subject_ref=trial_actor,
                    generation_id=generation_id,
                )
            return {
                "part_id": part_id, "modality": modality, "media_type": media_type,
                "byte_length": len(content), "width": width, "height": height,
                "payload": saved["payload"],
            }

        trial_png = io.BytesIO()
        Image.new("RGB", (64, 64), (33, 91, 147)).save(trial_png, format="PNG")
        trial_parts = [
            await upload_trial_part(
                "part/text", "TEXT", "text/plain",
                b"Where is the blue glass orb?", None, None,
            ),
            await upload_trial_part(
                "part/image", "IMAGE", "image/png",
                trial_png.getvalue(), 64, 64,
            ),
        ]
        trial_authorization = SimpleNamespace(
            actor_ref=trial_actor, web_subject_ref=trial_actor,
            target_generation_id=generation_id,
        )

        async def accept_trial_ticket(_conn, *, ticket, **_kwargs):
            assert ticket is trial_authorization

        started = await memory.management_contract.action(
            "memory.trial.start", operation_id="web:cross-vl-trial",
            payload={"query_id": trial_id, "parts": trial_parts},
            authorization=trial_authorization,
            authorization_guard=accept_trial_ticket,
        )
        assert started["query_id"] == trial_id
        async with asyncio.timeout(180):
            while True:
                trial_state = await memory.management_contract.query_authorized(
                    "memory.trial.get", {"query_id": trial_id},
                    authorization=trial_authorization,
                )
                if trial_state["state"] == "READY":
                    break
                assert trial_state["state"] != "FAILED", trial_state
                await asyncio.sleep(0.2)
        trial_page = await memory.management_contract.query_authorized(
            "memory.trial.result", {"query_id": trial_id, "limit": 4},
            authorization=trial_authorization,
        )
        assert any(
            item["source_unit_id"] == document_unit
            and item["external_group_id"] == "933179609"
            and item["author_subject_ref"] == "qq:20001"
            and item["image_count"] >= 1
            for item in trial_page["items"]
        )
        async with host.store.pool.connection() as conn:
            trial_matches = await (await conn.execute(
                sql.SQL(
                    "SELECT q.modality AS query_modality,d.modality AS document_modality "
                    "FROM {m}.query_seed_matches x "
                    "JOIN {m}.embedding_facets q ON q.bot_id=x.bot_id "
                    "AND q.facet_id=x.query_facet_id "
                    "JOIN {m}.embedding_facets d ON d.bot_id=x.bot_id "
                    "AND d.facet_id=x.document_facet_id "
                    "WHERE x.bot_id=%s AND x.query_id=%s AND x.source_unit_id=%s"
                ).format(m=sql.Identifier(namespace)),
                (settings.bot_id, trial_id, document_unit),
            )).fetchall()
        assert ("IMAGE", "IMAGE") in {
            (row["query_modality"], row["document_modality"])
            for row in trial_matches
        }
    finally:
        RecordingModelClient.reply_text = None
        try:
            await host.close()
        finally:
            try:
                profile = EmbeddingProfile(
                    deployment_id=settings.embedding_deployment_id,
                    model_revision=settings.embedding_model_revision,
                    model=settings.embedding_model, dimensions=settings.embedding_dimensions,
                )
                collection_name = vector_collection_name(
                    settings.bot_id, profile.space_id
                )
                client = AsyncMilvusClient(uri=settings.vector_index_endpoint)
                try:
                    if await client.has_collection(collection_name=collection_name):
                        await client.drop_collection(collection_name=collection_name)
                finally:
                    await client.close()
            finally:
                await admin.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
                )
                await admin.close()

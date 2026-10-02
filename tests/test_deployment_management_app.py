from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from forest.deployment_management_app import create_deployment_management_app
from forest.errors import ForestError
from forest.management_gateway import ManagementGateway
from forest.module_management_contract import ModuleConfigurationContract
from forest.packages import digest_json
from forest.storage.deployment_web_identity import (
    AuthenticatedWebSession,
    WebSession,
    WebSubject,
)
from forest.web_authority_ticket import WebAuthorityTicketAuthority
from forest.web_operation_policy import WEB_OPERATION_POLICY_DEFAULT


class _IdentityAuthority:
    session_token = "s" * 43
    csrf_token = "c" * 43
    active_key_id = "master/test/1"

    @staticmethod
    def key_resolver(key_id):
        return b"k" * 32 if key_id == "master/test/1" else None

    def __init__(self):
        self.revoked = False
        self.setup_required = False
        self.subject = WebSubject(
            "web-subject/test", "admin", "Administrator", "ADMIN", True, 1
        )

    async def login(self, *, username, password, peer_id):
        assert peer_id == "127.0.0.1"
        if username == "limited":
            raise ForestError(
                "MANAGEMENT_LOGIN_RATE_LIMITED",
                "Login attempts are temporarily limited",
                429,
                details={"retry_after_seconds": 37},
            )
        if (username, password) != ("admin", "correct-password"):
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Credentials are invalid", 401
            )
        self.revoked = False
        return WebSession(
            self.session_token,
            self.csrf_token,
            "web-session/test",
            self.subject,
            datetime.now(timezone.utc) + timedelta(hours=1),
        )

    async def authenticate(self, token):
        if token != self.session_token or self.revoked:
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Web session is invalid", 401
            )
        return AuthenticatedWebSession(
            hashlib.sha256(token.encode()).hexdigest(),
            "web-session/test",
            1,
            hashlib.sha256(self.csrf_token.encode()).hexdigest(),
            self.subject,
            datetime.now(timezone.utc) + timedelta(hours=1),
        )

    async def revalidate_session(self, session):
        if self.revoked or session.subject != self.subject:
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Web session changed", 401
            )

    async def logout(self, token):
        if token == self.session_token:
            self.revoked = True

    async def setup_status(self):
        return {
            "state": "PENDING" if self.setup_required else "COMPLETED",
            "setup_required": self.setup_required,
            "expires_at": "2030-01-01T00:00:00+00:00" if self.setup_required else None,
            "credential_expired": False,
        }

    async def complete_setup(self, *, pairing_code, username, display_name, password):
        if not self.setup_required or pairing_code != "p" * 48:
            raise ForestError("WEB_SETUP_CREDENTIAL_INVALID", "Pairing credential is invalid", 401)
        self.setup_required = False
        self.subject = WebSubject(
            "web-subject/paired", username, display_name, "ADMIN", True, 1
        )
        return self.subject


class _Configuration:
    component_id = "test.component"

    async def desired(self, *, include_value=False):
        return SimpleNamespace(
            revision=1,
            payload_id="payload/desired",
            payload_digest="sha256:" + "1" * 64,
            validation_digest="sha256:" + "2" * 64,
            value={"enabled": True} if include_value else None,
        )

    async def effective(self, *, include_value=False):
        return await self.desired(include_value=include_value)


class _GenerationAuthority:
    async def status(self):
        return {
            "bot_id": "bot/managed",
            "graph_state": "ACTIVE",
            "graph_id": "graph/managed",
        }


class _PolicyConfiguration:
    def __init__(self, value=WEB_OPERATION_POLICY_DEFAULT):
        self.value = value

    async def effective(self, *, include_value=False):
        return SimpleNamespace(
            revision=1,
            value=self.value if include_value else None,
        )

    async def effective_in(self, _conn, *, include_value=False):
        return await self.effective(include_value=include_value)


async def test_bot_gateway_applies_exact_web_operation_policy_after_signature_check():
    contract = _Contract(
        _Configuration(), schema={"type": "object"}, apply_callback=lambda **_: None
    )
    policy = {
        "protocol": "forest.web-operation-policy/1",
        "rules": [
            {
                "target_component_id": "test.component",
                "action_key": "test.query",
                "enabled": False,
                "allowed_roles": ["ADMIN", "AUDITOR"],
                "allowed_subject_refs": None,
            }
        ],
    }
    host = SimpleNamespace(
        settings=SimpleNamespace(bot_id="bot/managed"),
        registry=_Registry(SimpleNamespace(management_contract=contract)),
        _kernel=SimpleNamespace(
            configurations={"kernel.authorization": _PolicyConfiguration(policy)}
        ),
    )
    signer = WebAuthorityTicketAuthority(
        issuer_id="deployment/test",
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: b"k" * 32,
    )
    session = AuthenticatedWebSession(
        "token-digest",
        "web-session/test",
        1,
        "csrf-digest",
        WebSubject("web-subject/test", "admin", "Administrator", "ADMIN", True, 1),
        datetime.now(timezone.utc) + timedelta(hours=1),
    )
    operation_id = "web-read:test-policy"
    ticket = await signer.issue(
        session=session,
        target_scope_kind="bot",
        target_scope_id="bot-scope-test",
        target_component_id="test.component",
        target_generation_id="generation/test/1",
        target_graph_revision=1,
        target_operation_ref=operation_id,
        action_key="test.query",
        input_digest="sha256:" + hashlib.sha256(b"{}").hexdigest(),
    )
    gateway = ManagementGateway(
        host,
        ticket_authority=signer,
        scope_id="bot-scope-test",
    )
    with pytest.raises(ForestError) as denied:
        await gateway.module_query(
            "test.component",
            "test.query",
            {},
            authorization=ticket,
            operation_id=operation_id,
        )
    assert denied.value.code == "WEB_OPERATION_POLICY_DENIED"


class _Contract(ModuleConfigurationContract):
    transaction = object()

    async def query(self, query_id, payload):
        return {"query_id": query_id, "payload": payload}

    async def action(
        self,
        action_id,
        *,
        operation_id,
        payload,
        authorization=None,
        authorization_guard=None,
    ):
        if authorization_guard is not None:
            await authorization_guard(
                self.transaction,
                ticket=authorization,
                component_id="test.component",
                operation_id=operation_id,
                action_key=action_id,
                input_digest="sha256:" + hashlib.sha256(
                    b'{"enabled":false}'
                ).hexdigest(),
                plan_digest=None,
            )
        return {"action_id": action_id, "operation_id": operation_id, "payload": payload}


class _Registry:
    def __init__(self, module):
        self.module = module
        self.graph_identity = SimpleNamespace(
            graph_id="graph/managed",
            graph_revision=1,
            generations={"test.component": "generation/test/1"},
        )

    def get(self, component_id):
        if component_id != "test.component":
            raise KeyError(component_id)
        return self.module

    def describe(self):
        return [
            {
                "component_id": "test.component",
                "state": "ACTIVE",
                "generation_id": "generation/test/1",
                "management_contributions": [],
                "management_contract": self.module.management_contract.describe(),
            }
        ]


def test_real_browser_loads_memory_and_worker_host_through_authenticated_http():
    """Package UIs must complete their actions through the isolated HTTP bridge."""
    if os.environ.get("FOREST_BROWSER_SMOKE") != "1":
        pytest.skip("Set FOREST_BROWSER_SMOKE=1 for the real browser smoke")
    import uvicorn
    from playwright.sync_api import expect, sync_playwright

    memory_html = (
        Path(__file__).resolve().parents[1]
        / "src/forest/builtin/memory_graph/ui/index.html"
    ).read_bytes()
    digest = hashlib.sha256(memory_html).hexdigest()
    worker_html = (
        Path(__file__).resolve().parents[1]
        / "src/forest/builtin/component_worker_host/ui/index.html"
    ).read_bytes()
    worker_digest = hashlib.sha256(worker_html).hexdigest()

    class MemoryConfiguration(_Configuration):
        component_id = "memory.graph"

        async def desired(self, *, include_value=False):
            result = await super().desired(include_value=include_value)
            result.value = (
                {
                    "embedding": {
                        "deployment_id": "browser-smoke",
                        "model": "fixture-vl",
                        "model_kind": "VL",
                        "dimensions": 3,
                    },
                    "retrieval": {},
                }
                if include_value else None
            )
            return result

    class MemoryContract(_Contract):
        batch_applied = False

        def authorization_plan_digest(self, action_id, payload):
            assert action_id == "memory.forget.batch.enqueue"
            return digest_json(payload["plan"])

        async def action(self, action_id, *, operation_id, payload,
                         authorization=None, authorization_guard=None):
            assert action_id == "memory.forget.batch.enqueue"
            assert operation_id.startswith("web:sha256:")
            assert payload["selection_id"] == "selection/browser-smoke"
            assert payload["after_ordinal"] == 0
            assert payload["batch_size"] == 32
            assert authorization.web_subject_ref == "web-subject/test"
            self.batch_applied = True
            return {"request_id": operation_id}

        async def query(self, query_id, payload):
            if query_id == "memory.trunks.list":
                return {"items": []}
            if query_id == "memory.sources.query":
                return {
                    "items": [],
                    "has_more": False,
                    "snapshot_accepted_at": "2026-09-28T00:00:00Z",
                }
            if query_id == "memory.forget.selection":
                assert payload["selection_id"] == "selection/browser-smoke"
                return {
                    "selection_id": payload["selection_id"],
                    "member_count": 2,
                    "member_digest": "sha256:" + "a" * 64,
                    "snapshot_accepted_at": "2026-09-28T00:00:00Z",
                    "progress_state": "COMPLETE" if self.batch_applied else "FROZEN",
                    "applied_member_count": 2 if self.batch_applied else 0,
                    "applied_batch_count": 1 if self.batch_applied else 0,
                    "next_unapplied_ordinal": None if self.batch_applied else 1,
                    "filters": {"trunk_id": "trunk/example"},
                    "items": [{
                        "ordinal": 1, "source_unit_id": "unit/example",
                        "source_revision": 1, "anchor_payload_id": "payload/example",
                    }],
                    "has_more": False,
                }
            if query_id == "memory.forget.batch.preview":
                assert payload == {
                    "selection_id": "selection/browser-smoke",
                    "after_ordinal": 0, "batch_size": 32,
                }
                return {
                    "first_ordinal": 1, "last_ordinal": 2,
                    "batch_member_count": 2, "source_unit_count": 2,
                    "selected_payload_count": 2, "vector_deletion_count": 1,
                    "relation_retraction_count": 0,
                    "association_journal_count": 0, "deletion_epoch": 3,
                    "plan_digest": "sha256:" + "b" * 64,
                    "batch_source_unit_sample": ["unit/example"],
                }
            if query_id == "memory.forget.batch.status":
                assert payload["request_id"].startswith("web:sha256:")
                return {
                    "state": "SUCCEEDED", "selection_id": "selection/browser-smoke",
                    "after_ordinal": 0, "attempt_count": 1, "error_code": None,
                    "result": {"first_ordinal": 1, "last_ordinal": 2,
                               "suppressed_source_units": 2,
                               "vector_deletions": 1, "retracted_relations": 0},
                }
            raise AssertionError(f"Unexpected Memory UI query: {query_id}")

    contract = MemoryContract(
        MemoryConfiguration(), schema={"type": "object"}, apply_callback=lambda **_: None,
    )
    def read_memory_ui(expected):
        assert expected == digest
        return {"sha256": digest}, memory_html

    module = SimpleNamespace(
        management_contract=contract, management_ui_artifact=read_memory_ui
    )

    class WorkerContract(_Contract):
        selection_applied = False
        reload_reads = 0

        def authorization_plan_digest(self, action_id, payload):
            assert action_id == "module.enable"
            return payload["plan_digest"]

        async def query(self, query_id, payload):
            if query_id == "modules":
                return {
                    "bot_id": "bot/browser-smoke", "graph_id": "graph/browser-smoke",
                    "graph_revision": 1, "next_after": None,
                    "members": [{"component_id": "component.worker-host", "status": "ACTIVE",
                                 "package_id": "forest.component-worker-host",
                                 "package_version": "1.6.20"}],
                }
            if query_id == "module":
                assert payload["component_id"] == "component.worker-host"
                return {
                    "bot_id": "bot/browser-smoke", "graph_id": "graph/browser-smoke",
                    "graph_revision": 1,
                    "member": {"component_id": "component.worker-host",
                               "generation_id": "generation/worker-browser-smoke",
                               "status": "ACTIVE", "config_revision": 1,
                               "package_id": "forest.component-worker-host",
                               "package_version": "1.6.20",
                               "package_digest": "sha256:" + "a" * 64,
                               "data_admission": True, "control_admission": True},
                }
            if query_id == "selection":
                return {"revision": 2 if self.selection_applied else 1,
                        "entries": ([{"component_id": "process.executor",
                                      "desired_enabled": True, "source_kind": "builtin",
                                      "desired_revision": 2, "package_digest": None}]
                                    if self.selection_applied else [])}
            if query_id == "validate":
                assert payload["component_id"] == "process.executor"
                assert payload["desired_enabled"] is True
                assert payload["expected_selection_revision"] == 1
                assert payload["expected_graph_revision"] == 1
                totals = {name: 100 for name in (
                    "max_workers", "max_frame_bytes", "max_inflight", "max_queue_items",
                    "max_state_inflight", "max_owned_tasks", "max_owned_resources"
                )}
                totals["max_frame_bytes"] = 65536
                return {
                    "bot_id": "bot/browser-smoke",
                    "source_graph_id": "graph/browser-smoke",
                    "source_graph_revision": 1, "selection_revision": 1,
                    "plan_digest": "sha256:" + "b" * 64,
                    "impact": {name: [] for name in (
                        "replace", "retain", "remove", "add", "drain", "prepare"
                    )},
                    "worker_capacity": {"active": [], "preparing": []},
                    "admission": {"policy_revision": 1,
                                  "budget": totals, "peak": totals},
                    "packages": [],
                }
            if query_id == "selection-application":
                assert payload["request_id"] == "selection:" + "c" * 32
                return {"request_id": payload["request_id"],
                        "component_id": "process.executor", "desired_enabled": True,
                        "selection_revision": 2, "state": "SUBMITTED",
                        "kernel": {"state": "SUCCEEDED", "graph_revision": 2}}
            if query_id == "reload":
                assert payload == {"request_id": "reload:" + "d" * 32}
                self.reload_reads += 1
                return {
                    "protocol": "forest.component-reload-status/1",
                    "request_id": payload["request_id"],
                    "component_id": "component.worker-host",
                    "expected_graph_revision": 1,
                    "requested_by": "web-subject/test",
                    "state": "SUBMITTED", "attempt_count": 1,
                    "error_code": None,
                    "kernel": {"state": "SUCCEEDED", "graph_revision": 2},
                    "created_at": "2026-10-02T00:00:00Z",
                    "updated_at": "2026-10-02T00:00:01Z",
                }
            raise AssertionError(f"Unexpected Worker Host query: {query_id}")

        async def action(self, action_id, *, operation_id, payload,
                         authorization=None, authorization_guard=None):
            assert action_id == "module.enable"
            assert payload["component_id"] == "process.executor"
            assert payload["plan_digest"] == "sha256:" + "b" * 64
            assert authorization.web_subject_ref == "web-subject/test"
            assert operation_id.startswith("web:sha256:")
            self.selection_applied = True
            return {"request_id": "selection:" + "c" * 32}

    worker_contract = WorkerContract(
        _Configuration(), schema={"type": "object"}, apply_callback=lambda **_: None,
    )

    def read_worker_ui(expected):
        assert expected == worker_digest
        return {"sha256": worker_digest}, worker_html

    worker_module = SimpleNamespace(
        management_contract=worker_contract, management_ui_artifact=read_worker_ui
    )

    class Registry:
        graph_identity = SimpleNamespace(
            graph_id="graph/browser-smoke", graph_revision=1,
            generations={"memory.graph": "generation/browser-smoke",
                         "component.worker-host": "generation/worker-browser-smoke"},
        )

        def get(self, component_id):
            if component_id == "memory.graph":
                return module
            if component_id == "component.worker-host":
                return worker_module
            raise KeyError(component_id)

        def describe(self):
            return [{
                "component_id": "memory.graph",
                "state": "ACTIVE",
                "generation_id": "generation/browser-smoke",
                "artifact": {"package_id": "forest.memory-graph"},
                "management_contract": contract.describe(),
                "management_contributions": [{
                    "contribution_id": "memory.graph.management",
                    "label": "Memory",
                    "surface": {
                        "kind": "sandboxed-package",
                        "artifact": f"/management/v1/modules/memory.graph/management-ui/{digest}",
                        "sha256": digest,
                    },
                }],
            }, {
                "component_id": "component.worker-host",
                "state": "ACTIVE",
                "generation_id": "generation/worker-browser-smoke",
                "artifact": {"package_id": "forest.component-worker-host"},
                "management_contract": worker_contract.describe(),
                "management_contributions": [{
                    "contribution_id": "component.worker-host.management",
                    "label": "Components",
                    "surface": {
                        "kind": "sandboxed-package",
                        "artifact": (
                            "/management/v1/modules/component.worker-host/"
                            f"management-ui/{worker_digest}"
                        ),
                        "sha256": worker_digest,
                    },
                }],
            }]

    host = SimpleNamespace(
        settings=SimpleNamespace(bot_id="bot/browser-smoke"),
        registry=Registry(),
        generation_authority=_GenerationAuthority(),
        _kernel=SimpleNamespace(
            configurations={"kernel.authorization": _PolicyConfiguration()},
        ),
    )

    class Supervisor:
        async def management_scopes(self):
            return ({
                "scope_id": "browser-smoke",
                "scope_kind": "bot",
                "bot_id": "bot/browser-smoke",
                "label": "Browser smoke",
                "available": True,
                "desired_state": "ONLINE",
                "observed_state": "ONLINE",
            },)

        @asynccontextmanager
        async def management_host(self, scope_id):
            assert scope_id == "browser-smoke"
            yield host

    listening = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening.bind(("127.0.0.1", 0))
    listening.listen(128)
    port = listening.getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    identity = _IdentityAuthority()
    app = create_deployment_management_app(
        deployment_id="deployment/browser-smoke",
        components=(),
        identity_authority=identity,
        allowed_origin=origin,
        bot_supervisor=Supervisor(),
    )
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listening]}, daemon=True)
    thread.start()
    previous_policy = asyncio.get_event_loop_policy()
    try:
        deadline = time.monotonic() + 10
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert server.started
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        with sync_playwright() as playwright:
            executable = os.environ.get("FOREST_BROWSER_EXECUTABLE")
            if executable is None and sys.platform == "win32":
                edge = Path("C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe")
                if edge.is_file():
                    executable = str(edge)
            browser = playwright.chromium.launch(
                headless=True,
                **({"executable_path": executable} if executable else {}),
            )
            try:
                page = browser.new_page()
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto(origin + "/")
                expect(page.locator("#login")).to_be_visible()
                page.locator('#login-form [name="username"]').fill("admin")
                page.locator('#login-form [name="password"]').fill("correct-password")
                page.locator("#login-form button").click()
                expect(page.locator("#console")).to_be_visible()
                page.locator("#scope-select").select_option("bot:browser-smoke")
                page.locator("#module-nav button").filter(has_text="memory.graph").click()
                frame = page.frame_locator("iframe.package-ui-frame")
                expect(frame.locator('[data-tab="trial"]')).to_be_visible()
                expect(frame.locator("#revision")).to_contain_text("r1")
                expect(frame.locator("#source-count")).not_to_be_empty()
                frame.locator('[data-tab="forget"]').click()
                frame.locator("#forget details summary").click()
                frame.locator("#batch-selection-id").fill("selection/browser-smoke")
                frame.locator("#batch-load").click()
                expect(frame.locator("#batch-selection-meta")).to_contain_text("冻结成员 2 个")
                expect(frame.locator("#batch-members")).to_contain_text("unit/example")
                frame.locator("#batch-preview").click()
                expect(frame.locator("#batch-impact")).to_contain_text("向量删除 1")
                frame.locator("#batch-confirm").check()
                frame.locator("#batch-enqueue").click()
                expect(frame.locator("#batch-state")).to_contain_text("SUCCEEDED")
                expect(frame.locator("#batch-selection-meta")).to_contain_text("全部完成")
                assert contract.batch_applied
                assert not errors, errors
                page.locator("#back").click()
                expect(page.locator("iframe.package-ui-frame")).to_have_count(0)
                page.locator("#module-nav button").filter(
                    has_text="component.worker-host"
                ).click()
                worker_frame = page.frame_locator("iframe.package-ui-frame")
                expect(worker_frame.locator("#details")).to_be_visible()
                worker_frame.locator("#candidate-preview summary").click()
                worker_frame.locator("#preview-component-id").fill("process.executor")
                worker_frame.locator("#run-preview").click()
                expect(worker_frame.locator("#preview-digest")).to_have_text(
                    "sha256:" + "b" * 64
                )
                worker_frame.locator("#submit-selection").click()
                expect(worker_frame.locator("#selection-result")).to_contain_text(
                    "SUCCEEDED"
                )
                assert worker_contract.selection_applied
                worker_frame.locator("#reload-request-id").fill("reload:" + "d" * 32)
                worker_frame.locator("#refresh-reload").click()
                expect(worker_frame.locator("#reload-result")).to_contain_text("SUCCEEDED")
                assert worker_contract.reload_reads == 1
                page.locator("#back").click()
                page.locator("#module-nav button").filter(
                    has_text="component.worker-host"
                ).click()
                worker_frame = page.frame_locator("iframe.package-ui-frame")
                worker_frame.locator("#reload-request-id").fill("reload:" + "d" * 32)
                worker_frame.locator("#refresh-reload").click()
                expect(worker_frame.locator("#reload-result")).to_contain_text("SUCCEEDED")
                assert worker_contract.reload_reads == 2
                assert not errors, errors
                page.locator("#back").click()
                with page.expect_response("**/management/v1/auth/logout") as logout:
                    page.locator("#logout").click()
                assert logout.value.status == 200
                assert identity.revoked
                expect(page.locator("#login")).to_be_visible()
            finally:
                browser.close()
    finally:
        asyncio.set_event_loop_policy(previous_policy)
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive()


async def test_first_setup_endpoint_requires_exact_origin_and_closes_after_pairing():
    identity = _IdentityAuthority()
    identity.setup_required = True
    app = create_deployment_management_app(
        deployment_id="deployment/test",
        components=(),
        identity_authority=identity,
        allowed_origin="http://forest.test",
    )
    payload = {
        "pairing_code": "p" * 48,
        "username": "admin",
        "display_name": "Administrator",
        "password": "correct-password",
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://forest.test"
    ) as client:
        status = await client.get("/management/v1/setup/status")
        assert status.json()["setup_required"] is True
        denied = await client.post(
            "/management/v1/setup/complete",
            headers={"origin": "http://evil.test"},
            json=payload,
        )
        assert denied.status_code == 403
        completed = await client.post(
            "/management/v1/setup/complete",
            headers={"origin": "http://forest.test"},
            json=payload,
        )
        assert completed.status_code == 200
        assert completed.json()["subject"]["role"] == "ADMIN"
        assert (await client.get("/management/v1/setup/status")).json() == {
            "state": "COMPLETED",
            "setup_required": False,
            "expires_at": None,
            "credential_expired": False,
        }


async def test_deployment_gateway_routes_one_online_bot_scope_without_merging_registries():
    contract = _Contract(
        _Configuration(),
        schema={"type": "object", "properties": {"enabled": {"type": "boolean"}}},
        apply_callback=lambda **_: None,
    )
    accepted_receipts = []
    audit_records = []

    class RuntimeAuthority:
        async def verify_active_generation_in(
            self, conn, *, component_id, generation_id, admission_lane
        ):
            assert conn is _Contract.transaction
            assert (component_id, generation_id, admission_lane) == (
                "test.component",
                "generation/test/1",
                "CONTROL",
            )
            return SimpleNamespace(graph_revision=1)

    class AuthorizationAuthority:
        async def accept_web_authority_ticket_in(self, conn, *, ticket):
            assert conn is _Contract.transaction
            receipt = {"ticket_id": ticket.ticket_id}
            accepted_receipts.append(receipt)
            return receipt

    class AuditAuthority:
        async def append_in(self, conn, envelope):
            assert conn is _Contract.transaction
            audit_records.append(envelope)
            return {"audit_id": envelope.audit_id}

    host = SimpleNamespace(
        settings=SimpleNamespace(bot_id="bot/managed"),
        generation_authority=_GenerationAuthority(),
        _kernel=SimpleNamespace(
            configurations={"kernel.authorization": _PolicyConfiguration()},
            runtime=RuntimeAuthority(),
            authorization=AuthorizationAuthority(),
            audit=AuditAuthority(),
        ),
    )
    host.registry = _Registry(SimpleNamespace(management_contract=contract))

    class Supervisor:
        async def management_scopes(self):
            return (
                {
                    "scope_id": "bot-scope-test",
                    "scope_kind": "bot",
                    "bot_id": "bot/managed",
                    "label": "Managed",
                    "available": True,
                    "desired_state": "ONLINE",
                    "observed_state": "ONLINE",
                },
            )

        @asynccontextmanager
        async def management_host(self, scope_id):
            if scope_id != "bot-scope-test":
                raise ForestError(
                    "BOT_MANAGEMENT_SCOPE_NOT_FOUND", "Bot scope does not exist", 404
                )
            yield host

    app = create_deployment_management_app(
        deployment_id="deployment/test",
        components=(),
        identity_authority=(identity := _IdentityAuthority()),
        allowed_origin="http://forest.test",
        bot_supervisor=Supervisor(),
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://forest.test"
    ) as client:
        status = await client.get("/management/v1/setup/status")
        assert status.json() == {
            "state": "COMPLETED",
            "setup_required": False,
            "expires_at": None,
            "credential_expired": False,
        }
        assert (await client.get("/management/v1/runtime")).status_code == 401

        assert (
            await client.post(
                "/management/v1/auth/login",
                headers={"origin": "http://evil.test"},
                json={"username": "admin", "password": "correct-password"},
            )
        ).status_code == 403
        limited = await client.post(
            "/management/v1/auth/login",
            headers={"origin": "http://forest.test"},
            json={"username": "limited", "password": "incorrect-password"},
        )
        assert limited.status_code == 429
        assert limited.headers["retry-after"] == "37"
        login = await client.post(
            "/management/v1/auth/login",
            headers={"origin": "http://forest.test"},
            json={"username": "admin", "password": "correct-password"},
        )
        assert login.status_code == 200
        headers = {
            "origin": "http://forest.test",
            "x-forest-csrf": _IdentityAuthority.csrf_token,
        }
        oversized_asset = await client.put(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/assets/trial.input/uploads/upload-1/chunks/0",
            headers={**headers, "content-type": "application/octet-stream"},
            content=b"x" * (262144 + 1),
        )
        assert oversized_asset.status_code == 413
        assert oversized_asset.json()["code"] == "MANAGEMENT_ASSET_CHUNK_INVALID"
        deployment = await client.get("/management/v1/runtime")
        assert [scope["scope_id"] for scope in deployment.json()["scopes"]] == [
            "deployment",
            "bot-scope-test",
        ]
        runtime = await client.get(
            "/management/v1/scopes/bot/bot-scope-test/runtime"
        )
        assert runtime.json()["runtime"]["bot_id"] == "bot/managed"
        assert [item["component_id"] for item in runtime.json()["modules"]] == [
            "test.component"
        ]
        configuration = await client.get(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/configuration",
        )
        assert configuration.status_code == 200
        assert configuration.json()["desired"]["value"] == {"enabled": True}
        query = await client.post(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/queries/test.query",
            headers=headers,
            json={"cursor": "next"},
        )
        assert query.json() == {
            "query_id": "test.query",
            "payload": {"cursor": "next"},
        }
        action = await client.post(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/actions/test.action",
            headers={**headers, "idempotency-key": "bot-action-1"},
            json={"enabled": False},
        )
        assert action.status_code == 200
        assert action.json()["action_id"] == "test.action"
        assert action.json()["operation_id"].startswith("web:sha256:")
        assert action.json()["graph_id"] == "graph/managed"
        assert len(accepted_receipts) == 1
        assert len(audit_records) == 1
        audit = audit_records[0]
        assert audit.action == "test.action"
        assert audit.severity == "SECURITY"
        assert audit.producer_component_id == "test.component"
        assert audit.producer_generation_id == "generation/test/1"
        assert audit.origin_kind == "web.management"
        assert (audit.actor_kind, audit.actor_id) == (
            "web.subject",
            "web-subject/test",
        )
        assert (audit.resource_kind, audit.resource_id) == (
            "bot.component",
            "test.component",
        )
        assert (audit.operation_kind, audit.operation_id) == (
            "web.action",
            action.json()["operation_id"],
        )
        assert audit.call_id == accepted_receipts[0]["ticket_id"]
        identity.subject = WebSubject(
            identity.subject.subject_id,
            identity.subject.username,
            identity.subject.display_name,
            "AUDITOR",
            True,
            2,
        )
        denied = await client.post(
            "/management/v1/scopes/bot/bot-scope-test/modules/"
            "test.component/actions/test.action",
            headers={**headers, "idempotency-key": "auditor-action"},
            json={},
        )
        assert denied.status_code == 403
        assert denied.json()["code"] == "MANAGEMENT_AUTHORIZATION_DENIED"
        missing = await client.get(
            "/management/v1/scopes/bot/missing/runtime"
        )
        assert missing.status_code == 404
        assert (
            await client.post("/management/v1/auth/logout", headers=headers)
        ).status_code == 200
        assert (await client.get("/management/v1/runtime")).status_code == 401

async def test_asset_routes_forward_bounded_binary_under_bot_web_tickets(monkeypatch):
    calls = []

    class AssetGateway:
        def __init__(self, host, *, ticket_authority, scope_id):
            assert scope_id == "bot-scope-test"

        def authorization_target(self, component_id):
            assert component_id == "memory.graph"
            return "generation/asset-1", 1

        def asset_operation_id(self, component_id, asset_id, stage, key):
            return "asset:" + hashlib.sha256(
                f"{component_id}:{asset_id}:{stage}:{key}".encode()
            ).hexdigest()

        async def asset_begin(self, component_id, asset_id, body, **kwargs):
            calls.append(("begin", body, kwargs["authorization"]))
            return {"state": "OPEN", "upload_id": body["upload_id"]}

        async def asset_chunk(self, component_id, asset_id, upload_id, index, content, **kwargs):
            calls.append(("chunk", bytes(content), kwargs["authorization"]))
            return {"upload_id": upload_id, "chunk_index": index}

        async def asset_finalize(self, component_id, asset_id, upload_id, **kwargs):
            calls.append(("finalize", upload_id, kwargs["authorization"]))
            return {"state": "FINALIZED", "upload_id": upload_id}

    monkeypatch.setattr("forest.management_scopes.ManagementGateway", AssetGateway)

    class Supervisor:
        async def management_scopes(self):
            return ()

        @asynccontextmanager
        async def management_host(self, scope_id):
            assert scope_id == "bot-scope-test"
            yield object()

    app = create_deployment_management_app(
        deployment_id="deployment/test", components=(),
        identity_authority=_IdentityAuthority(), allowed_origin="http://forest.test",
        bot_supervisor=Supervisor(),
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://forest.test"
    ) as client:
        login = await client.post(
            "/management/v1/auth/login", headers={"origin": "http://forest.test"},
            json={"username": "admin", "password": "correct-password"},
        )
        assert login.status_code == 200
        headers = {"origin": "http://forest.test",
                   "x-forest-csrf": _IdentityAuthority.csrf_token}
        base = ("/management/v1/scopes/bot/bot-scope-test/modules/"
                "memory.graph/assets/trial.input/uploads")
        body = {"upload_id": "asset-1", "target_ref": "trial-1",
                "part_id": "image-0", "media_type": "image/png",
                "byte_length": 4, "digest": "sha256:" + "a" * 64,
                "chunk_count": 1}
        assert (await client.post(base, headers=headers, json=body)).json()["state"] == "OPEN"
        binary = b"\x00\x01\x02\xff"
        uploaded = await client.put(
            base + "/asset-1/chunks/0",
            headers={**headers, "content-type": "application/octet-stream"},
            content=binary,
        )
        assert uploaded.status_code == 200
        assert uploaded.json()["chunk_index"] == 0
        assert (await client.post(base + "/asset-1/finalize", headers=headers)).json()["state"] == "FINALIZED"
        assert [item[0] for item in calls] == ["begin", "chunk", "finalize"]
        assert calls[1][1] == binary
        assert all(item[2].web_subject_ref == "web-subject/test" for item in calls)
        assert all(item[2].target_generation_id == "generation/asset-1" for item in calls)

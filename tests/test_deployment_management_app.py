from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from forest.clean_management_app import CleanManagementGateway
from forest.clean_management_contract import (
    CleanManagementActionResult,
    CleanModuleConfigurationContract,
)
from forest.deployment_management_app import create_deployment_management_app
from forest.errors import ForestError
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
    contract = _Contract(_Configuration(), schema={"type": "object"})
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
    gateway = CleanManagementGateway(
        host,
        "management-token-long-enough",
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


class _Contract(CleanModuleConfigurationContract):
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
        return CleanManagementActionResult(
            {"action_id": action_id, "operation_id": operation_id, "payload": payload}
        )


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
        def __init__(self, host, token, *, ticket_authority, scope_id):
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

    monkeypatch.setattr("forest.management_scopes.CleanManagementGateway", AssetGateway)

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

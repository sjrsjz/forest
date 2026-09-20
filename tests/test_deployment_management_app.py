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


async def test_deployment_gateway_routes_one_online_bot_scope_without_merging_registries():
    contract = _Contract(
        _Configuration(),
        schema={"type": "object", "properties": {"enabled": {"type": "boolean"}}},
    )
    host = SimpleNamespace(
        settings=SimpleNamespace(bot_id="bot/managed"),
        generation_authority=_GenerationAuthority(),
        _kernel=SimpleNamespace(
            configurations={"kernel.authorization": _PolicyConfiguration()}
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

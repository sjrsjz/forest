from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.clean_messaging_runtime import bootstrap_clean_messaging
from forest.core_runtime import bootstrap_clean_kernel
from forest.runtime_modules import ModuleLifecycleState, RuntimeModuleRegistry
from forest.storage.adapter_onebot_authority import OneBotPayloadRef
from forest.storage.payload_authority import PayloadSource


class FakeWebSocketClient:
    def __init__(self):
        self.handler = None
        self.closed = False
        self.failure_code = None

    async def get_login_info(self, *, timeout=None):
        assert timeout == 5
        return {"self_id": "123456789", "nickname": "Forest"}

    def activate_events(self, handler):
        assert self.handler is None
        self.handler = handler

    async def emit(self, body):
        return await self.handler(body)

    async def send(self, _prepared, *, timeout=None):
        raise AssertionError(("unexpected send", timeout))

    async def aclose(self):
        self.closed = True

    def owned_tasks(self):
        return ()


async def _payload(payloads, *, owner_kind, owner_id, value) -> OneBotPayloadRef:
    epoch = await payloads.epoch()
    saved = await payloads.put_bytes(
        owner_component_id="adapter.onebot",
        owner_kind=owner_kind,
        owner_id=owner_id,
        media_type="application/json",
        content=json.dumps(value, separators=(",", ":")).encode(),
        sources=(
            PayloadSource(
                source_component_id="adapter.onebot",
                source_kind="configuration",
                source_id=owner_id,
                source_revision=1,
            ),
        ),
        expected_deletion_epoch=epoch,
    )
    return OneBotPayloadRef(
        saved.payload_id,
        saved.content_digest,
        "adapter.onebot",
        owner_kind,
        owner_id,
    )


@pytest.mark.postgres
async def test_kernel_and_messaging_publish_one_exact_runtime_graph(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    clients = []

    async def create_client(_url, _token, **_limits):
        client = FakeWebSocketClient()
        clients.append(client)
        return client

    async def resolve_secret(*, secret_ref, expected_digest):
        assert secret_ref == "secret://onebot/main/1"
        assert expected_digest == "sha256:" + "a" * 64
        return {"access_token": "test-access-token"}

    messaging = await bootstrap_clean_messaging(
        store,
        installer,
        kernel=kernel,
        secret_resolver=resolve_secret,
        websocket_client_factory=create_client,
    )
    display = await _payload(
        kernel.payload,
        owner_kind="binding.display",
        owner_id="binding/main",
        value={"display_name": "Forest"},
    )
    await messaging.adapter.register_binding(
        operation_id="binding/register/main",
        binding_id="binding/main",
        self_id="123456789",
        display=display,
    )
    config = await _payload(
        kernel.payload,
        owner_kind="binding.config",
        owner_id="binding/config/1",
        value={
            "transport": "FORWARD_WEBSOCKET",
            "websocket_url": "ws://127.0.0.1:3001/",
        },
    )
    await messaging.adapter.plan_configuration(
        plan_id="binding/config/1",
        binding_id="binding/main",
        expected_revision=0,
        enabled=True,
        receive_enabled=True,
        transport="FORWARD_WEBSOCKET",
        config=config,
        secret_ref="secret://onebot/main/1",
        secret_digest="sha256:" + "a" * 64,
        actor_principal_ref="admin/root",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    await messaging.adapter.apply_configuration(
        operation_id="binding/apply/1", plan_id="binding/config/1"
    )

    lease = await kernel.runtime.claim_host("host-a", "inc-a")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(host, kernel.descriptors + messaging.descriptors)
    await registry.start()
    try:
        assert set(registry.states.values()) == {ModuleLifecycleState.ACTIVE}
        assert set(registry.graph_identity.generations) == {
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "kernel.audit",
            "conversation.scheduler",
            "adapter.onebot",
        }
        adapter_runtime = registry.get("adapter.onebot")
        conversation_runtime = registry.get("conversation.scheduler")
        assert adapter_runtime.generation_identity.generation_id == (
            registry.graph_identity.generations["adapter.onebot"]
        )
        assert conversation_runtime.active is True
        assert len(clients) == 1 and clients[0].handler is not None

        body = json.dumps(
            {
                "time": 1700000000,
                "self_id": 123456789,
                "post_type": "message",
                "message_type": "group",
                "sub_type": "normal",
                "message_id": 88,
                "group_id": 933179609,
                "user_id": 42,
                "sender": {"user_id": 42, "nickname": "Alice", "role": "member"},
                "message": [{"type": "text", "data": {"text": "hello graph"}}],
            },
            separators=(",", ":"),
        ).encode()
        await clients[0].emit(body)
        async with store.pool.connection() as conn:
            inbox = await (
                await conn.execute(
                    sql.SQL("SELECT conversation_id,author_subject_ref FROM {}.inbox_messages").format(
                        sql.Identifier(
                            messaging.conversation.namespace
                        )
                    )
                )
            ).fetchone()
        assert inbox == {
            "conversation_id": "onebot:group:933179609",
            "author_subject_ref": "qq:42",
        }
    finally:
        assert await registry.close() == []
    assert clients[0].closed is True
    assert set(registry.states.values()) == {ModuleLifecycleState.STOPPED}

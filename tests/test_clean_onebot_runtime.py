from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from psycopg import sql
from test_clean_adapter_onebot import _configured
from test_clean_adapter_onebot import clean_onebot as clean_onebot

from forest.adapter_onebot_runtime import CleanOneBotRuntime
from forest.adapters.onebot import SendResult
from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity
from forest.storage.ingress_unit_of_work import OneBotIngressUnitOfWork
from forest.storage.module_configuration import ModuleConfigurationRepository


class FakeWebSocketClient:
    def __init__(self, self_id: str) -> None:
        self.self_id = self_id
        self.handler = None
        self.closed = False
        self.sent = []
        self.failure_code = None

    async def get_login_info(self, *, timeout=None):
        assert timeout == 5
        return {"self_id": self.self_id, "nickname": "Forest"}

    def activate_events(self, handler):
        assert self.handler is None
        self.handler = handler

    async def emit(self, body: bytes):
        assert self.handler is not None
        return await self.handler(body)

    async def send(self, prepared, *, timeout=None):
        self.sent.append((prepared, timeout))
        return SendResult("SENT", message_id="9001")

    async def aclose(self):
        self.closed = True

    def owned_tasks(self):
        return ()


@pytest.mark.postgres
async def test_clean_onebot_generation_buffers_then_admits_and_quiesces(clean_onebot):
    store, storage, scheduler_storage, payloads, scheduler, adapter = clean_onebot
    await _configured(clean_onebot)
    configuration = ModuleConfigurationRepository(
        store,
        component_id="adapter.onebot",
        namespace=storage.spec.namespace,
        payloads=payloads,
    )
    await configuration.initialize(
        operation_id="adapter.onebot/configuration/bootstrap",
        value={"protocol": "forest.adapter-onebot/1"},
        validation_digest="sha256:" + "9" * 64,
    )
    clients = []

    async def create_client(url, token, **limits):
        assert url == "ws://127.0.0.1:3001/"
        assert token == "test-access-token"
        assert limits == {
            "timeout_seconds": 15,
            "event_capacity": 256,
            "pending_capacity": 64,
        }
        client = FakeWebSocketClient("123456789")
        clients.append(client)
        return client

    async def resolve_secret(*, secret_ref, expected_digest):
        assert secret_ref == "secret://onebot/main/1"
        assert expected_digest == "sha256:" + "a" * 64
        return {"access_token": "test-access-token"}

    async def classify(*, normalized):
        assert normalized["channel_id"] == "933179609"
        return {
            "decision": "NO_TRIGGER",
            "classifier_component_id": "conversation.trigger-policy",
            "classifier_generation_id": "generation/trigger-policy/1",
            "rule_revision": "trigger-policy:1:test",
            "reason_code": "mention_required",
        }

    async def inspect_shell(**_values):
        return None

    ingress = OneBotIngressUnitOfWork(store, adapter=adapter, conversation=scheduler)
    runtime = CleanOneBotRuntime(
        repository=adapter,
        module_configuration=configuration,
        grants=SimpleNamespace(grant_set_digest="sha256:" + "8" * 64),
        payloads=payloads,
        secret_resolver=resolve_secret,
        ingress_port=ingress,
        trigger_policy=classify,
        shell_inspector=inspect_shell,
        websocket_client_factory=create_client,
    )
    identity = BoundGenerationIdentity(
        component_id="adapter.onebot",
        generation_id="generation/adapter/1",
        graph_id="runtime-graph/1",
        graph_revision=1,
        graph_digest="sha256:" + "7" * 64,
        activation_id="activation-1",
    )
    await runtime.bind_generation(identity)
    await runtime.start()
    assert len(clients) == 1 and clients[0].handler is None
    head = await adapter.load_binding("binding/main")
    assert head["state"] == "DISABLED" and head["binding_epoch"] == 0

    async with store.pool.connection() as conn, conn.transaction():
        await runtime.publish_generation_in(
            conn,
            generation_id=identity.generation_id,
            graph_id=identity.graph_id,
        )
    runtime.activate_generation()
    head = await adapter.load_binding("binding/main")
    assert head["state"] == "ONLINE" and head["binding_epoch"] == 1
    assert clients[0].handler is not None

    body = json.dumps(
        {
            "time": 1700000000,
            "self_id": 123456789,
            "post_type": "message",
            "message_type": "group",
            "sub_type": "normal",
            "message_id": 77,
            "group_id": 933179609,
            "user_id": 42,
            "sender": {"user_id": 42, "nickname": "Alice", "role": "member"},
            "message": [{"type": "text", "data": {"text": "hello"}}],
        },
        separators=(",", ":"),
    ).encode()
    await clients[0].emit(body)
    async with store.pool.connection() as conn:
        event_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.platform_events").format(
                    sql.Identifier(storage.spec.namespace)
                )
            )
        ).fetchone()
        inbox_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.inbox_messages").format(
                    sql.Identifier(scheduler_storage.spec.namespace)
                )
            )
        ).fetchone()
    assert event_count["count"] == inbox_count["count"] == 1

    async def verify_handoff_generation(_conn, *, component_id, generation_id, admission_lane):
        assert component_id == "adapter.onebot"
        assert generation_id in {"generation/adapter/1", "generation/adapter/2"}
        assert admission_lane == "DATA"
        return {"active": True}

    adapter.generation_verifier = verify_handoff_generation
    successor = CleanOneBotRuntime(
        repository=adapter,
        module_configuration=configuration,
        grants=SimpleNamespace(grant_set_digest="sha256:" + "8" * 64),
        payloads=payloads,
        secret_resolver=resolve_secret,
        ingress_port=ingress,
        trigger_policy=classify,
        shell_inspector=inspect_shell,
        websocket_client_factory=create_client,
    )
    successor_identity = BoundGenerationIdentity(
        component_id="adapter.onebot",
        generation_id="generation/adapter/2",
        graph_id="runtime-graph/2",
        graph_revision=2,
        graph_digest="sha256:" + "6" * 64,
        activation_id="activation-2",
    )
    await successor.bind_generation(successor_identity)
    await successor.start()
    assert len(clients) == 2 and clients[1].handler is None
    async with store.pool.connection() as conn, conn.transaction():
        await successor.publish_generation_in(
            conn,
            generation_id=successor_identity.generation_id,
            graph_id=successor_identity.graph_id,
        )
    successor.activate_generation()
    head = await adapter.load_binding("binding/main")
    assert head["state"] == "ONLINE" and head["binding_epoch"] == 2
    assert head["current_client_incarnation"].startswith("client:activation-2:")

    with pytest.raises(ForestError) as caught:
        await clients[0].emit(body)
    assert caught.value.code == "STALE_BINDING"
    await runtime.quiesce()
    await runtime.close()
    assert clients[0].closed is True
    assert (await adapter.load_binding("binding/main"))["state"] == "ONLINE"

    successor_body = json.loads(body)
    successor_body["message_id"] = 78
    await clients[1].emit(json.dumps(successor_body, separators=(",", ":")).encode())
    async with store.pool.connection() as conn:
        event_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.platform_events").format(
                    sql.Identifier(storage.spec.namespace)
                )
            )
        ).fetchone()
    assert event_count["count"] == 2

    await successor.quiesce()
    await successor.close()
    assert clients[1].closed is True
    assert (await adapter.load_binding("binding/main"))["state"] == "OFFLINE"

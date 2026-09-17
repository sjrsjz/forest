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

    ingress = OneBotIngressUnitOfWork(store, adapter=adapter, conversation=scheduler)
    runtime = CleanOneBotRuntime(
        repository=adapter,
        module_configuration=configuration,
        grants=SimpleNamespace(grant_set_digest="sha256:" + "8" * 64),
        payloads=payloads,
        secret_resolver=resolve_secret,
        ingress_port=ingress,
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

    await runtime.activate_generation()
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

    await runtime.quiesce()
    with pytest.raises(ForestError) as caught:
        await clients[0].emit(body)
    assert caught.value.code == "STALE_BINDING"
    await runtime.close()
    assert clients[0].closed is True
    assert (await adapter.load_binding("binding/main"))["state"] == "OFFLINE"

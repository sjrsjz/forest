from __future__ import annotations

import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.clean_bot_host import CleanBotHost, CleanBotSettings
from forest.runtime_modules import ModuleLifecycleState


class FakeModelClient:
    def __init__(self, *_args, **_kwargs) -> None:
        self.closed = False

    async def aclose(self) -> None:
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

    async def aclose(self) -> None:
        self.closed = True

    def owned_tasks(self):
        return ()


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
        onebot_self_id="123456789",
        onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609",),
        group_trigger_mode="MENTION",
        private_enabled=True,
        agent_system_prompt="You are the production-host integration test Bot.",
    )
    clients = []

    async def create_websocket(_url, _token, **_limits):
        client = FakeWebSocketClient(settings.onebot_self_id)
        clients.append(client)
        return client

    try:
        graph_ids = []
        for _ in range(2):
            host = CleanBotHost(
                settings,
                model_client_factory=FakeModelClient,
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
                    "memory.graph",
                    "agent.runtime",
                    "conversation.trigger-policy",
                    "conversation.scheduler",
                    "adapter.onebot",
                    "delivery",
                }
                graph_ids.append(host.registry.graph_identity.graph_id)
                assert clients[-1].handler is not None
            finally:
                await host.close()
            assert clients[-1].closed is True
        assert graph_ids[0] != graph_ids[1]
    finally:
        assert name.startswith("forest_host_") and len(name) == 44
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()

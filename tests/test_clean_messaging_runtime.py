from __future__ import annotations

import asyncio
import json
import traceback
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.adapters.onebot import SendResult
from forest.agent_model_executor import (
    AgentModelStep,
    AgentToolBinding,
    DurableAgentModelExecutor,
)
from forest.agent_tool_executor import (
    DurableAgentToolExecutor,
    ToolAuthorizationResult,
    ToolDispatchResult,
)
from forest.clean_agent_module import bootstrap_clean_agent
from forest.clean_delivery_module import bootstrap_clean_delivery
from forest.clean_memory_module import bootstrap_clean_memory
from forest.clean_messaging_runtime import bootstrap_clean_messaging
from forest.clean_provider_module import bootstrap_clean_provider
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.models import ModelResult
from forest.runtime_modules import ModuleLifecycleState, RuntimeModuleRegistry
from forest.storage.adapter_onebot_authority import OneBotPayloadRef
from forest.storage.agent_runtime_authority import (
    AgentInputBatch,
    AgentPayloadRef,
    AgentTriggerInput,
)
from forest.storage.delivery_authority import DeliveryPart, DeliveryPayloadRef
from forest.storage.payload_authority import PayloadSource


class FakeWebSocketClient:
    def __init__(self):
        self.handler = None
        self.closed = False
        self.failure_code = None
        self.sends = []

    async def get_login_info(self, *, timeout=None):
        assert timeout == 5
        return {"self_id": "123456789", "nickname": "Forest"}

    def activate_events(self, handler):
        assert self.handler is None
        self.handler = handler

    async def emit(self, body):
        return await self.handler(body)

    async def send(self, prepared, *, timeout=None):
        self.sends.append((prepared, timeout))
        return SendResult("SENT", message_id="991")

    async def aclose(self):
        self.closed = True

    def owned_tasks(self):
        return ()


class FakeModelClient:
    def __init__(self, _url, api_key, *, deployment_id, **_limits):
        assert api_key == "test-model-key"
        self.deployment_id = deployment_id
        self.closed = False
        self.permitted = False

    async def dispatch(self, prepared, *, permitcheck):
        await permitcheck(prepared)
        self.permitted = True
        if "tool" in prepared.request_id:
            text = (
                '<valid_agent_protocol_content_block block_type="ModelTurn">'
                '<payload><blocks><block block_type="FunctionCall">'
                '<function name="lookup"><arguments encoding="json">'
                '{"authority_trigger_id":"trigger:tool:1","arguments":{"query":"x"}}'
                '</arguments></function></block></blocks></payload>'
                '</valid_agent_protocol_content_block>'
            )
        else:
            text = (
                '<valid_agent_protocol_content_block block_type="ModelTurn">'
                '<payload><blocks><block block_type="Assistant"><content>'
                '<text>ok</text></content></block></blocks></payload>'
                '</valid_agent_protocol_content_block>'
            )
        return ModelResult(
            "RESPONDED",
            prepared.request_id,
            prepared.transport_attempt_id,
            prepared.payload_digest,
            text=text,
        )

    async def aclose(self):
        self.closed = True


class FakeToolAuthorization:
    def __init__(self):
        self.ready = False
        self.audience_override = None
        self.requests = []

    async def authorize(self, request, *, approval_required):
        self.requests.append((request, approval_required))
        if not self.ready:
            return ToolAuthorizationResult("WAITING", approval_ref="approval:tool:test")
        return ToolAuthorizationResult(
            "AUTHORIZED",
            authorization_ticket_id="authorization-ticket:tool:test",
            approval_ref="approval:tool:test",
            input_digest=request.input_digest,
            audience_component_id=request.executor_component_id,
            audience_generation_id=(
                self.audience_override or request.executor_generation_id
            ),
        )


class CrashAfterToolHandoff:
    def __init__(self, store, payloads):
        self.store = store
        self.payloads = payloads
        self.dispatches = []
        self.queries = []

    async def dispatch(self, request, *, permit_in):
        self.dispatches.append(request)
        async with self.store.pool.connection() as conn, conn.transaction():
            await permit_in(conn, effect_observation_ref="effect-attempt:tool:test")
        raise ConnectionError("tool result was lost after handoff")

    async def query(self, request):
        self.queries.append(request)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id=request.executor_component_id,
            owner_kind="tool.result",
            owner_id=request.tool_call_id,
            media_type="application/json",
            content=json.dumps(
                {"value": "reconciled"},
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            expected_deletion_epoch=epoch,
        )
        return ToolDispatchResult(
            "SUCCEEDED",
            result=AgentPayloadRef(
                saved.payload_id,
                saved.content_digest,
                request.executor_component_id,
                "tool.result",
                request.tool_call_id,
            ),
            observation_ref="effect-observation:tool:test",
        )


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


async def _agent_payload(payloads, *, owner_kind, owner_id, value) -> AgentPayloadRef:
    epoch = await payloads.epoch()
    saved = await payloads.put_bytes(
        owner_component_id="agent.runtime",
        owner_kind=owner_kind,
        owner_id=owner_id,
        media_type="application/json",
        content=json.dumps(value, separators=(",", ":")).encode(),
        sources=(
            PayloadSource(
                source_component_id="agent.runtime",
                source_kind="configuration",
                source_id=owner_id,
                source_revision=1,
            ),
        ),
        expected_deletion_epoch=epoch,
    )
    return AgentPayloadRef(
        saved.payload_id,
        saved.content_digest,
        "agent.runtime",
        owner_kind,
        owner_id,
    )


@pytest.mark.postgres
async def test_kernel_and_messaging_publish_one_exact_runtime_graph(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    model_clients = []

    def create_model_client(*args, **kwargs):
        client = FakeModelClient(*args, **kwargs)
        model_clients.append(client)
        return client

    provider = await bootstrap_clean_provider(
        store,
        installer,
        kernel=kernel,
        credential_resolver=lambda _revision: "test-model-key",
        client_factory=create_model_client,
    )
    memory = await bootstrap_clean_memory(store, installer, kernel=kernel)
    agent = await bootstrap_clean_agent(store, installer, kernel=kernel)
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
    delivery = await bootstrap_clean_delivery(
        store,
        installer,
        kernel=kernel,
        adapter=messaging.adapter,
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
    registry = RuntimeModuleRegistry(
        host,
        kernel.descriptors
        + (provider.descriptor, memory.descriptor, agent.descriptor)
        + messaging.descriptors
        + (delivery.descriptor,),
    )
    await registry.start()
    try:
        assert set(registry.states.values()) == {ModuleLifecycleState.ACTIVE}
        assert set(registry.graph_identity.generations) == {
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "kernel.audit",
            "provider.chat-completions",
            "memory.graph",
            "agent.runtime",
            "conversation.scheduler",
            "adapter.onebot",
            "delivery",
        }
        adapter_runtime = registry.get("adapter.onebot")
        conversation_runtime = registry.get("conversation.scheduler")
        agent_runtime = registry.get("agent.runtime")
        provider_runtime = registry.get("provider.chat-completions")
        memory_runtime = registry.get("memory.graph")
        delivery_runtime = registry.get("delivery")
        assert adapter_runtime.generation_identity.generation_id == (
            registry.graph_identity.generations["adapter.onebot"]
        )
        assert conversation_runtime.active is True
        assert agent_runtime.active is True
        assert provider_runtime.active is True
        assert memory_runtime.active is True
        assert memory_runtime.profile.embedding.model == "Qwen/Qwen3-VL-Embedding-8B"
        assert memory_runtime.profile.embedding.dimensions == 4096
        assert await memory_runtime.list_trunks() == []
        assert delivery_runtime.active is True
        assert agent_runtime.provider is not provider_runtime
        provider_route = agent_runtime.provider._project.route
        assert provider_route.caller_component_id == "agent.runtime"
        assert provider_route.provider_component_id == "provider.chat-completions"
        assert provider_route.provider_generation_id == (
            registry.graph_identity.generations["provider.chat-completions"]
        )
        assert provider_route.graph_id == registry.graph_identity.graph_id
        assert provider_route.owned_work is True
        agent_route = conversation_runtime.agent_execution._execute.route
        assert agent_route.provider_component_id == "agent.runtime"
        assert agent_route.owned_work is True
        reply_uow_route = conversation_runtime.completion_port._commit.route
        assert reply_uow_route.provider_component_id == "kernel.runtime"
        assert reply_uow_route.owned_work is True
        input_uow_route = conversation_runtime.input_selection_port._select_input.route
        assert input_uow_route.provider_component_id == "kernel.runtime"
        assert input_uow_route.owned_work is True
        assert adapter_runtime.ingress_port is not messaging.conversation
        ingress_route = adapter_runtime.ingress_port._admit.route
        assert ingress_route.provider_component_id == "kernel.runtime"
        assert ingress_route.owned_work is False
        assert delivery_runtime.adapter is not adapter_runtime
        delivery_route = delivery_runtime.adapter._dispatch.route
        assert delivery_route.provider_component_id == "adapter.onebot"
        assert delivery_route.owned_work is True
        assert len(clients) == 1 and clients[0].handler is not None

        reply = await _agent_payload(
            kernel.payload,
            owner_kind="final.reply",
            owner_id="reply:delivery-worker:test",
            value={"text": "delivery worker reply"},
        )
        await delivery.repository.create_intent(
            operation_id="intent:create:delivery-worker:test",
            intent_id="intent:delivery-worker:test",
            binding_ref="binding/main",
            lane_kind="CHAT",
            ordering_key="onebot:group:933179609",
            conversation_ref="onebot:group:933179609",
            channel_kind="GROUP",
            channel_ref="933179609",
            cause_component_id="conversation.scheduler",
            cause_kind="context.reply",
            cause_id="reply:delivery-worker:test",
            delivery_generation_id=delivery_runtime.generation_identity.generation_id,
            reply_ref="reply:delivery-worker:test",
            parts=(
                DeliveryPart(
                    "part:delivery-worker:test",
                    "effect:delivery-worker:test",
                    DeliveryPayloadRef(
                        reply.payload_id,
                        reply.digest,
                        reply.owner_component_id,
                        reply.owner_kind,
                        reply.owner_id,
                    ),
                ),
            ),
        )
        delivery_runtime.worker.wake()
        delivery_state = None
        for _ in range(500):
            delivery_state = (
                await delivery.repository.load_intent("intent:delivery-worker:test")
            )["state"]
            if delivery_state == "SENT":
                break
            await asyncio.sleep(0.01)
        assert len(clients[0].sends) == 1, (
            delivery_runtime.worker.failure,
            (await delivery.repository.load_intent("intent:delivery-worker:test"))["state"],
            await delivery.repository.recoverable_attempts(),
        )
        prepared_send, timeout = clients[0].sends[0]
        assert timeout is None
        assert prepared_send.action == "send_group_msg"
        assert b"delivery worker reply" in prepared_send.wire_body
        assert delivery_state == "SENT"

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
                    sql.SQL(
                        "SELECT event_id,conversation_id,author_subject_ref FROM {}.inbox_messages"
                    ).format(
                        sql.Identifier(
                            messaging.conversation.namespace
                        )
                    )
                )
            ).fetchone()
        assert inbox["event_id"].startswith("onebot:onebot-event:")
        assert inbox["conversation_id"] == "onebot:group:933179609"
        assert inbox["author_subject_ref"] == "qq:42"
        trunks = await memory_runtime.list_trunks()
        assert len(trunks) == 1
        assert trunks[0]["external_group_id"] == "933179609"
        assert trunks[0]["registered_sources"] == 1
        assert trunks[0]["eligible_sources"] == 0
        async with store.pool.connection() as conn, conn.transaction():
            classified = await conversation_runtime.complete_classification_in(
                conn,
                operation_id="classify:onebot:88",
                conversation_id="onebot:group:933179609",
                event_id=inbox["event_id"],
                decision="TRIGGER",
                classifier_component_id="conversation.classifier",
                classifier_generation_id="generation:classifier:test",
                rule_revision="rules:test/1",
                reason_code="test_trigger",
                route_ref=None,
                trigger_id="trigger:onebot:88",
                principal_ref="qq:42",
            )
        assert classified["classified_through_seq"] == 1
        for _ in range(500):
            remembered = await memory_runtime.query_sources()
            if remembered["items"]:
                break
            await asyncio.sleep(0.01)
        assert len(remembered["items"]) == 1
        assert remembered["items"][0]["external_group_id"] == "933179609"
        assert remembered["items"][0]["author_subject_ref"] == "qq:42"
        assert remembered["items"][0]["parts"][0]["part_kind"] == "TEXT"
        assert await conversation_runtime.claimable_conversations() == (
            "onebot:group:933179609",
        )
        agent_config = await _agent_payload(
            kernel.payload,
            owner_kind="agent.config",
            owner_id="agent/config/test",
            value={"model": "test"},
        )
        preset = await _agent_payload(
            kernel.payload,
            owner_kind="preset.context",
            owner_id="agent/preset/test",
            value={"system": ["test"]},
        )
        started = await agent_runtime.start_run(
            operation_id="agent/start/test",
            conversation_ref="onebot:group:manual-model-test",
            session_id="agent/session/test",
            run_id="agent/run/test",
            input_batch=AgentInputBatch(
                input_batch_ref="batch:test",
                input_batch_digest="sha256:" + "1" * 64,
                selected_input_end_seq=1,
                context_revision=1,
                context_view_revision=0,
                source_manifest_digest="sha256:" + "2" * 64,
                triggers=(AgentTriggerInput("trigger:onebot:88", "qq:42", 1),),
            ),
            agent_spec_revision="agent-spec:test/1",
            config=agent_config,
            preset_revision="preset:test/1",
            preset=preset,
            absolute_deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        assert started["run_id"] == "agent/run/test"
        assert started["core_prompt_digest"].startswith("sha256:")

        model_snapshot = {
                "bot_id": store.bot_id,
                "run_id": "agent/run/test",
                "logical_step_id": "agent/step/test",
                "input_revision": 1,
                "context_revision": 1,
                "context_view_revision": 0,
                "selected_input_end_seq": 1,
                "trigger_ids": ["trigger:onebot:88"],
                "input_batch_ids": ["batch:test"],
                "units": [
                    {
                        "unit_id": "message:88",
                        "role": "user",
                        "source": {
                            "conversation_id": "onebot:group:manual-model-test",
                            "group_source_id": "qq-group:manual-model-test",
                            "platform": "onebot-v11",
                            "channel_kind": "group",
                            "channel_id": "manual-model-test",
                        },
                        "author": {
                            "principal_ref": "qq:42",
                            "external_user_id": "42",
                            "display_name": "Alice",
                        },
                        "parts": [
                            {"type": "text", "text": "look"},
                            {
                                "type": "image",
                                "media_ref": "media:88",
                                "url": "https://secret.invalid/image-token",
                            },
                        ],
                    }
                ],
            }
        request = await agent_runtime.provider.prepare_request(
            model_snapshot,
            request_id="model-request:test",
            transport_attempt_id="model-attempt:test",
            stream=False,
        )
        assert request.component_id == "provider.chat-completions"
        assert request.generation_id == registry.graph_identity.generations[
            "provider.chat-completions"
        ]
        assert "secret.invalid" not in request.prepared.body.decode()
        permitted = []

        async def permit(_conn, *, request):
            permitted.append(request.prepared.request_id)

        response = await agent_runtime.provider.dispatch(request, permit_in=permit)
        assert response.status == "RESPONDED"
        assert permitted == ["model-request:test"]

        owner = await agent_runtime.acquire_run("agent/run/test", "agent-worker/test")
        executor = DurableAgentModelExecutor(
            agent=agent_runtime,
            provider=agent_runtime.provider,
            payloads=kernel.payload,
        )
        with pytest.raises(ForestError) as caught:
            await executor.execute(
                owner,
                AgentModelStep(
                    request_id="model-request:stale:test",
                    attempt_id="model-attempt:stale:test",
                    effect_operation_ref="effect:model:stale:test",
                    turn_id="model-turn:stale:test",
                    final_reply_ref="reply:stale:test",
                ),
                {**model_snapshot, "selected_input_end_seq": 2},
                stream=False,
            )
        assert caught.value.code == "AGENT_MODEL_INPUT_FENCE_MISMATCH"

        model_step = await executor.execute(
            owner,
            AgentModelStep(
                request_id="model-request:agent:test",
                attempt_id="model-attempt:agent:test",
                effect_operation_ref="effect:model:agent:test",
                turn_id="model-turn:agent:test",
                final_reply_ref="reply:agent:test",
            ),
            model_snapshot,
            stream=False,
        )
        assert model_step.status == "FINAL_READY"
        assert model_step.text == "ok"
        durable_run = await agent_runtime.load("agent/run/test")
        assert durable_run["state"] == "FINAL_READY"
        assert durable_run["final_candidate_payload_id"] == (
            model_step.final_candidate.payload_id
        )

        await agent_runtime.start_run(
            operation_id="agent/start/tool",
            conversation_ref="onebot:group:tool",
            session_id="agent/session/tool",
            run_id="agent/run/tool",
            input_batch=AgentInputBatch(
                input_batch_ref="batch:tool",
                input_batch_digest="sha256:" + "3" * 64,
                selected_input_end_seq=1,
                context_revision=1,
                context_view_revision=0,
                source_manifest_digest="sha256:" + "4" * 64,
                triggers=(AgentTriggerInput("trigger:tool:1", "qq:43", 1),),
            ),
            agent_spec_revision="agent-spec:test/1",
            config=agent_config,
            preset_revision="preset:test/1",
            preset=preset,
            absolute_deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        tool_owner = await agent_runtime.acquire_run("agent/run/tool", "agent-worker/tool")
        tool_snapshot = {
            "bot_id": store.bot_id,
            "run_id": "agent/run/tool",
            "logical_step_id": "agent/step/tool",
            "input_revision": 1,
            "context_revision": 1,
            "context_view_revision": 0,
            "selected_input_end_seq": 1,
            "trigger_ids": ["trigger:tool:1"],
            "input_batch_ids": ["batch:tool"],
            "units": [
                {
                    "unit_id": "message:tool:1",
                    "role": "user",
                    "source": {
                        "conversation_id": "onebot:group:tool",
                        "group_source_id": "qq-group:tool",
                        "platform": "onebot-v11",
                        "channel_kind": "group",
                        "channel_id": "tool",
                    },
                    "author": {
                        "principal_ref": "qq:43",
                        "external_user_id": "43",
                        "display_name": "Bob",
                    },
                    "parts": [{"type": "text", "text": "lookup x"}],
                }
            ],
        }
        tool_declaration = {
            "name": "lookup",
            "description": "Read a bounded value",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": False,
            },
        }
        tool_binding = AgentToolBinding(
            "lookup",
            "adapter.onebot",
            adapter_runtime.generation_identity.generation_id,
            "lookup.read",
            "sha256:" + "5" * 64,
            supports_query=True,
        )
        tool_step = await executor.execute(
            tool_owner,
            AgentModelStep(
                request_id="model-request:tool:test",
                attempt_id="model-attempt:tool:test",
                effect_operation_ref="effect:model:tool:test",
                turn_id="model-turn:tool:test",
                final_reply_ref="reply:tool:test",
            ),
            tool_snapshot,
            tools=(tool_declaration,),
            tool_bindings=(tool_binding,),
            stream=False,
        )
        assert tool_step.status == "TOOL_PROPOSED"
        tool_run = await agent_runtime.load_recovery("agent/run/tool")
        assert tool_run["state"] == "WAIT_TOOL"
        assert tool_run["current_tool"]["tool_call_id"] == tool_step.tool_call_id
        assert tool_run["current_tool"]["authority_principal_ref"] == "qq:43"

        tool_executor = DurableAgentToolExecutor(
            agent=agent_runtime,
            payloads=kernel.payload,
        )
        tool_authorization = FakeToolAuthorization()
        tool_port = CrashAfterToolHandoff(store, kernel.payload)
        waiting = await tool_executor.execute(
            tool_owner,
            tool_binding,
            tool_authorization,
            tool_port,
        )
        assert waiting.status == "WAIT_APPROVAL"
        assert tool_port.dispatches == []
        tool_authorization.ready = True
        tool_authorization.audience_override = "generation:wrong"
        with pytest.raises(ForestError) as wrong_audience:
            await tool_executor.execute(
                tool_owner,
                tool_binding,
                tool_authorization,
                tool_port,
            )
        assert wrong_audience.value.code == "AGENT_TOOL_AUTHORIZATION_CONFLICT"
        assert tool_port.dispatches == []
        tool_authorization.audience_override = None
        with pytest.raises(ConnectionError, match="lost after handoff"):
            await tool_executor.execute(
                tool_owner,
                tool_binding,
                tool_authorization,
                tool_port,
            )
        handed_off = await agent_runtime.load_recovery("agent/run/tool")
        assert handed_off["state"] == "WAIT_TOOL"
        assert handed_off["current_tool"]["state"] == "DISPATCHING"
        await agent_runtime.release_run(tool_owner)
        recovered_owner = await agent_runtime.acquire_run(
            "agent/run/tool", "agent-worker/tool-recovery"
        )
        reconciled = await tool_executor.reconcile(
            recovered_owner,
            tool_binding,
            tool_port,
        )
        assert reconciled.status == "SUCCEEDED"
        assert len(tool_port.dispatches) == 1
        assert len(tool_port.queries) == 1
        recovered = await agent_runtime.load_recovery("agent/run/tool")
        assert recovered["state"] == "PREPARING_MODEL"
        assert recovered["current_tool"] is None

        conversation_runtime.notify()
        for _ in range(1000):
            if len(clients[0].sends) >= 2:
                break
            await asyncio.sleep(0.01)
        assert conversation_runtime.worker is not None
        assert conversation_runtime.worker.failures == {}, "".join(
            traceback.format_exception(
                next(iter(conversation_runtime.worker.failures.values()))
            )
        )
        assert len(clients[0].sends) == 2
        automatic_send, automatic_timeout = clients[0].sends[1]
        assert automatic_timeout is None
        assert automatic_send.action == "send_group_msg"
        assert b"ok" in automatic_send.wire_body
        async with store.pool.connection() as conn:
            lane = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT active_session_ref,active_run_ref,context_revision "
                        "FROM {}.conversation_lanes WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(messaging.conversation.namespace)),
                    (store.bot_id, "onebot:group:933179609"),
                )
            ).fetchone()
        assert lane["active_session_ref"] is None
        assert lane["active_run_ref"] is None
        assert lane["context_revision"] >= 2
    finally:
        assert await registry.close() == []
    assert clients[0].closed is True
    assert model_clients[0].closed is True
    assert set(registry.states.values()) == {ModuleLifecycleState.STOPPED}
    with pytest.raises(ForestError) as caught:
        await conversation_runtime.claimable_conversations()
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    with pytest.raises(ForestError) as caught:
        await agent_runtime.load("run/missing")
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    with pytest.raises(ForestError) as caught:
        await memory_runtime.list_trunks()
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    with pytest.raises(ForestError) as caught:
        await agent_runtime.provider.prepare_request(
            model_snapshot,
            request_id="model-request:after-close",
            transport_attempt_id="model-attempt:after-close",
            stream=False,
        )
    assert caught.value.code == "RUNTIME_SERVICE_CALLER_NOT_ACTIVE"

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.runtime_modules import official_storage_bundle
from forest.storage.adapter_onebot_authority import OneBotAdapterRepository, OneBotPayloadRef
from forest.storage.authorization_authority import CapabilityGrant, KernelAuthorizationRepository
from forest.storage.bot import BotStore
from forest.storage.conversation_scheduler import (
    ConversationSchedulerRepository,
    SchedulerContextFact,
    SchedulerPayloadRef,
)
from forest.storage.delivery_authority import (
    DeliveryPart,
    DeliveryPayloadRef,
    DeliveryRepository,
)
from forest.storage.effect_authority import KernelEffectRepository
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller

PART_IDS = (
    "part/reply/1",
    "part/reply/2",
    "part/control/1",
    "part/retry/1",
    "part/race/1",
)


@pytest_asyncio.fixture
async def clean_delivery():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_delivery_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-delivery")
    await store.open()
    installer = SchemaBundleInstaller(store)
    await installer.initialize_kernel()
    specs = [
        official_storage_bundle(
            "kernel.payload",
            "forest.kernel.payload",
            "forest.kernel-payload.repository/1",
            "storage/official/kernel_payload.sql",
        ),
        official_storage_bundle(
            "kernel.authorization",
            "forest.kernel.authorization",
            "forest.kernel-authorization.repository/1",
            "storage/official/kernel_authorization.sql",
        ),
        official_storage_bundle(
            "kernel.effect",
            "forest.kernel.effect",
            "forest.kernel-effect.repository/1",
            "storage/official/kernel_effect.sql",
        ),
        official_storage_bundle(
            "conversation.scheduler",
            "forest.conversation.scheduler",
            "forest.conversation-scheduler.repository/1",
            "storage/official/conversation_scheduler.sql",
        ),
        official_storage_bundle(
            "adapter.onebot",
            "forest.adapter.onebot",
            "forest.adapter-onebot.repository/1",
            "storage/official/adapter_onebot.sql",
        ),
        official_storage_bundle(
            "delivery",
            "forest.delivery",
            "forest.delivery.repository/1",
            "storage/official/delivery.sql",
        ),
    ]
    for storage in specs:
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    storage = {item.spec.component_id: item for item in specs}
    payloads = KernelPayloadRepository(store, namespace=storage["kernel.payload"].spec.namespace)
    authorization = KernelAuthorizationRepository(
        store, namespace=storage["kernel.authorization"].spec.namespace
    )

    async def verify_effect_fence(_conn, **_identity):
        return None

    effects = KernelEffectRepository(
        store,
        namespace=storage["kernel.effect"].spec.namespace,
        payload_verifier=payloads.verify_in,
        authorization_consumer=authorization.consume_ticket_in,
        fence_verifier=verify_effect_fence,
    )
    scheduler = ConversationSchedulerRepository(
        store,
        namespace=storage["conversation.scheduler"].spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    adapter = OneBotAdapterRepository(
        store,
        namespace=storage["adapter.onebot"].spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    delivery = DeliveryRepository(
        store,
        namespace=storage["delivery"].spec.namespace,
        payload_verifier=payloads.verify_in,
        binding_fence_verifier=adapter.verify_transport_fence_in,
        effect_admitter=effects.admit_in,
        effect_permitter=effects.permit_in,
        effect_handoff=effects.mark_handoff_in,
        effect_observer=effects.observe_in,
        effect_abandoner=effects.abandon_prepared_in,
    )
    grant = await authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="delivery",
        entries=tuple(CapabilityGrant("delivery.send", "delivery-part", item) for item in PART_IDS),
        expected_policy_revision=1,
        audience_component_id="adapter.onebot",
        audience_generation_id="generation/adapter/1",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )
    try:
        yield store, storage, payloads, authorization, effects, scheduler, adapter, delivery, grant
    finally:
        await store.close()
        assert name.startswith("forest_delivery_") and len(name) == 48
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _put(
    payloads,
    *,
    component: str,
    kind: str,
    owner_id: str,
    body: bytes,
    source_id: str,
):
    epoch = await payloads.epoch()
    saved = await payloads.put_bytes(
        owner_component_id=component,
        owner_kind=kind,
        owner_id=owner_id,
        media_type="application/json",
        content=body,
        sources=(
            PayloadSource(
                source_component_id=component,
                source_kind="test.source",
                source_id=source_id,
                source_revision=1,
            ),
        ),
        expected_deletion_epoch=epoch,
    )
    return saved


def _delivery_ref(saved, component: str, kind: str, owner_id: str):
    return DeliveryPayloadRef(saved.payload_id, saved.content_digest, component, kind, owner_id)


async def _active_binding(payloads, adapter):
    display = await _put(
        payloads,
        component="adapter.onebot",
        kind="binding.display",
        owner_id="binding/main",
        body=b'{"display_name":"Forest"}',
        source_id="binding/display",
    )
    await adapter.register_binding(
        operation_id="binding/register",
        binding_id="binding/main",
        self_id="123456789",
        display=OneBotPayloadRef(
            display.payload_id,
            display.content_digest,
            "adapter.onebot",
            "binding.display",
            "binding/main",
        ),
    )
    config = await _put(
        payloads,
        component="adapter.onebot",
        kind="binding.config",
        owner_id="binding/plan/1",
        body=b'{"transport":"FORWARD_WEBSOCKET"}',
        source_id="binding/config",
    )
    await adapter.plan_configuration(
        plan_id="binding/plan/1",
        binding_id="binding/main",
        expected_revision=0,
        enabled=True,
        receive_enabled=True,
        transport="FORWARD_WEBSOCKET",
        config=OneBotPayloadRef(
            config.payload_id,
            config.content_digest,
            "adapter.onebot",
            "binding.config",
            "binding/plan/1",
        ),
        secret_ref="secret://onebot/main",
        secret_digest="sha256:" + "a" * 64,
        actor_principal_ref="admin/root",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    revision = await adapter.apply_configuration(
        operation_id="binding/apply/1", plan_id="binding/plan/1"
    )
    await adapter.prepare_client(
        client_incarnation="client/main/1",
        binding_id="binding/main",
        revision=revision["revision"],
        expected_binding_epoch=0,
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest="sha256:" + "b" * 64,
    )
    return await adapter.publish_client(
        operation_id="binding/publish/1",
        binding_id="binding/main",
        client_incarnation="client/main/1",
        revision=revision["revision"],
        expected_binding_epoch=0,
    )


async def _create_intent(
    payloads,
    delivery,
    *,
    suffix: str,
    part_id: str,
    lane: str = "CHAT",
    ordering_key: str = "conversation/main",
):
    saved = await _put(
        payloads,
        component="agent.runtime" if lane == "CHAT" else "shell.command",
        kind="final.reply" if lane == "CHAT" else "control.reply",
        owner_id="payload/" + suffix,
        body=(f'{{"text":"{suffix}"}}').encode(),
        source_id="reply/" + suffix,
    )
    part = DeliveryPart(
        part_operation_id=part_id,
        effect_operation_ref="effect/" + suffix,
        payload=_delivery_ref(
            saved,
            "agent.runtime" if lane == "CHAT" else "shell.command",
            "final.reply" if lane == "CHAT" else "control.reply",
            "payload/" + suffix,
        ),
    )
    return await delivery.create_intent(
        operation_id="intent/create/" + suffix,
        intent_id="intent/" + suffix,
        binding_ref="binding/main",
        lane_kind=lane,
        ordering_key=ordering_key,
        conversation_ref=ordering_key if lane == "CHAT" else None,
        channel_kind="GROUP",
        channel_ref="933179609",
        cause_component_id="conversation.scheduler" if lane == "CHAT" else "shell.command",
        cause_kind="context.reply" if lane == "CHAT" else "shell.reply",
        cause_id="reply/" + suffix if lane == "CHAT" else "cause/" + suffix,
        delivery_generation_id="generation/delivery/1",
        reply_ref="reply/" + suffix if lane == "CHAT" else None,
        parts=(part,),
    )


async def _prepare(
    payloads,
    authorization,
    delivery,
    grant,
    claim,
    suffix: str,
    *,
    handoff: bool = True,
):
    attempt_id = "attempt/" + suffix
    prepare_operation = "prepare/" + suffix
    wire = await _put(
        payloads,
        component="kernel.effect",
        kind="effect.wire",
        owner_id=attempt_id,
        body=(f'{{"action":"send_group_msg","suffix":"{suffix}"}}').encode(),
        source_id="wire/" + suffix,
    )
    deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
    ticket_id = "ticket/" + suffix
    await authorization.issue_ticket(
        ticket_id=ticket_id,
        issuer_kind="kernel",
        issuer_id="kernel.authorization",
        subject_kind="component",
        subject_id="delivery",
        action="delivery.send",
        resource_kind="delivery-part",
        resource_id=claim.part_operation_id,
        operation_id=prepare_operation,
        input_digest=claim.semantic_digest,
        plan_digest=None,
        grant_set=grant,
        expires_at=deadline,
    )
    prepared = await delivery.prepare_attempt(
        claim,
        operation_id=prepare_operation,
        attempt_id=attempt_id,
        transport_action="send_group_msg",
        wire=DeliveryPayloadRef(
            wire.payload_id,
            wire.content_digest,
            "kernel.effect",
            "effect.wire",
            attempt_id,
        ),
        binding_epoch=1,
        connection_epoch=1,
        client_incarnation="client/main/1",
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest="sha256:" + "b" * 64,
        authorization_ticket_ref=ticket_id,
        authorization_plan_digest=None,
        deadline_at=deadline,
    )
    if handoff:
        await delivery.mark_handoff(
            claim,
            attempt_id=attempt_id,
            handoff_operation_id="handoff/" + suffix,
        )
    return prepared


async def _observe_sent(payloads, delivery, attempt_id: str, suffix: str, *, evidence="EXECUTOR"):
    effect_operation = "effect-observe/" + suffix
    receipt = await _put(
        payloads,
        component="kernel.effect",
        kind="effect.evidence",
        owner_id=effect_operation,
        body=(f'{{"message_id":"{suffix}"}}').encode(),
        source_id="receipt/" + suffix,
    )
    return await delivery.observe(
        observation_operation_id="delivery-observe/" + suffix,
        effect_observation_operation_id=effect_operation,
        attempt_id=attempt_id,
        observed_status="SENT",
        evidence_kind=evidence,
        receipt=DeliveryPayloadRef(
            receipt.payload_id,
            receipt.content_digest,
            "kernel.effect",
            "effect.evidence",
            effect_operation,
        ),
    )


@pytest.mark.postgres
async def test_clean_delivery_schema_has_no_adapter_socket_context_or_inline_body(clean_delivery):
    _store, storage, *_rest = clean_delivery
    lowered = storage["delivery"].baseline.lower()
    for forbidden in (
        b"jsonb",
        b"access_token",
        b"webhook_secret",
        b"socket",
        b"context_units",
        b"agent_run",
        b"onebot_action",
    ):
        assert forbidden not in lowered
    assert b"delivery_lanes" in lowered
    assert b"delivery_observations" in lowered


@pytest.mark.postgres
async def test_chat_intent_rejects_agent_as_direct_cause(clean_delivery):
    _store, _storage, payloads, _authorization, _effects, _scheduler, _adapter, delivery, _grant = (
        clean_delivery
    )
    reply = await _put(
        payloads,
        component="agent.runtime",
        kind="final.reply",
        owner_id="reply/direct-agent",
        body=b'{"text":"pong"}',
        source_id="reply/direct-agent",
    )
    with pytest.raises(ForestError) as caught:
        await delivery.create_intent(
            operation_id="intent/create/direct-agent",
            intent_id="intent/direct-agent",
            binding_ref="binding/main",
            lane_kind="CHAT",
            ordering_key="conversation/main",
            conversation_ref="conversation/main",
            channel_kind="GROUP",
            channel_ref="933179609",
            cause_component_id="agent.runtime",
            cause_kind="final.reply",
            cause_id="reply/direct-agent",
            delivery_generation_id="generation/delivery/1",
            reply_ref="reply/direct-agent",
            parts=(
                DeliveryPart(
                    "part/direct-agent/1",
                    "effect/direct-agent",
                    _delivery_ref(
                        reply,
                        "agent.runtime",
                        "final.reply",
                        "reply/direct-agent",
                    ),
                ),
            ),
        )
    assert caught.value.code == "DELIVERY_CAUSE_INVALID"


@pytest.mark.postgres
async def test_final_reply_context_and_send_intent_share_one_unit_of_work(clean_delivery):
    store, storage, payloads, _authorization, _effects, scheduler, _adapter, delivery, _grant = (
        clean_delivery
    )
    message = await _put(
        payloads,
        component="adapter.test",
        kind="normalized.message",
        owner_id="message/1",
        body=b'{"parts":[{"type":"text","text":"ping"}]}',
        source_id="message/1",
    )
    author = await _put(
        payloads,
        component="adapter.test",
        kind="author.snapshot",
        owner_id="author/1",
        body=b'{"subject":"user/1"}',
        source_id="author/1",
    )
    now = datetime.now(timezone.utc)
    await scheduler.admit_message(
        operation_id="message/admit/1",
        conversation_id="conversation/main",
        source_component_id="adapter.test",
        binding_ref="binding/main",
        platform="test",
        channel_ref="933179609",
        thread_ref=None,
        event_id="message/1",
        source_event_ref="source/message/1",
        event_kind="group.message",
        platform_message_ref="1",
        author_subject_ref="user/1",
        content=SchedulerPayloadRef(
            message.payload_id,
            message.content_digest,
            "adapter.test",
            "normalized.message",
            "message/1",
        ),
        author=SchedulerPayloadRef(
            author.payload_id,
            author.content_digest,
            "adapter.test",
            "author.snapshot",
            "author/1",
        ),
        received_at=now,
        classification_deadline=now + timedelta(minutes=2),
    )
    await scheduler.complete_classification(
        operation_id="message/classify/1",
        conversation_id="conversation/main",
        event_id="message/1",
        decision="TRIGGER",
        classifier_component_id="router.test",
        classifier_generation_id="generation/router/1",
        trigger_id="trigger/1",
        principal_ref="user/1",
    )
    actor = await scheduler.acquire_actor("conversation/main", "conversation-worker")
    await scheduler.select_input(
        actor,
        "select/1",
        boundary="IDLE",
        session_ref="session/1",
        run_ref="run/1",
    )
    reply = await _put(
        payloads,
        component="agent.runtime",
        kind="final.reply",
        owner_id="reply/atomic",
        body=b'{"text":"pong"}',
        source_id="reply/atomic",
    )
    reply_scheduler = SchedulerPayloadRef(
        reply.payload_id,
        reply.content_digest,
        "agent.runtime",
        "final.reply",
        "reply/atomic",
    )
    part = DeliveryPart(
        "part/reply/1",
        "effect/reply/atomic",
        _delivery_ref(reply, "agent.runtime", "final.reply", "reply/atomic"),
    )

    async def fail_effect(_conn, **_values):
        raise ForestError("TEST_EFFECT_FAILED", "forced", 503)

    failing = DeliveryRepository(
        store,
        namespace=storage["delivery"].spec.namespace,
        payload_verifier=payloads.verify_in,
        effect_admitter=fail_effect,
    )
    with pytest.raises(ForestError) as caught:
        async with store.pool.connection() as conn, conn.transaction():
            await scheduler.append_context_in(
                conn,
                actor,
                operation_id="final/context/1",
                facts=(
                    SchedulerContextFact(
                        "unit/reply/1",
                        "REPLY",
                        "agent.runtime",
                        "final.reply",
                        "reply/atomic",
                        reply_scheduler,
                    ),
                ),
                session_ref="session/1",
                run_ref="run/1",
                answer_trigger_ids=("trigger/1",),
                reply_ref="reply/atomic",
            )
            await failing.create_intent_in(
                conn,
                operation_id="intent/create/atomic",
                intent_id="intent/atomic",
                binding_ref="binding/main",
                lane_kind="CHAT",
                ordering_key="conversation/main",
                conversation_ref="conversation/main",
                channel_kind="GROUP",
                channel_ref="933179609",
                cause_component_id="conversation.scheduler",
                cause_kind="context.reply",
                cause_id="reply/atomic",
                delivery_generation_id="generation/delivery/1",
                reply_ref="reply/atomic",
                parts=(part,),
            )
    assert caught.value.code == "TEST_EFFECT_FAILED"
    async with store.pool.connection() as conn:
        trigger = await (
            await conn.execute(
                sql.SQL("SELECT status FROM {}.conversation_triggers WHERE bot_id=%s "
                        "AND trigger_id='trigger/1'").format(
                    sql.Identifier(storage["conversation.scheduler"].spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
        assert trigger["status"] == "CLAIMED"
    async with store.pool.connection() as conn, conn.transaction():
        await scheduler.append_context_in(
            conn,
            actor,
            operation_id="final/context/1",
            facts=(
                SchedulerContextFact(
                    "unit/reply/1",
                    "REPLY",
                    "agent.runtime",
                    "final.reply",
                    "reply/atomic",
                    reply_scheduler,
                ),
            ),
            session_ref="session/1",
            run_ref="run/1",
            answer_trigger_ids=("trigger/1",),
            reply_ref="reply/atomic",
        )
        await delivery.create_intent_in(
            conn,
            operation_id="intent/create/atomic",
            intent_id="intent/atomic",
            binding_ref="binding/main",
            lane_kind="CHAT",
            ordering_key="conversation/main",
            conversation_ref="conversation/main",
            channel_kind="GROUP",
            channel_ref="933179609",
            cause_component_id="conversation.scheduler",
            cause_kind="context.reply",
            cause_id="reply/atomic",
            delivery_generation_id="generation/delivery/1",
            reply_ref="reply/atomic",
            parts=(part,),
        )
    assert (await delivery.load_intent("intent/atomic"))["state"] == "QUEUED"


@pytest.mark.postgres
async def test_control_priority_and_unknown_chat_block_only_its_lane(clean_delivery):
    _store, _storage, payloads, authorization, _effects, _scheduler, adapter, delivery, grant = (
        clean_delivery
    )
    await _active_binding(payloads, adapter)
    await _create_intent(
        payloads, delivery, suffix="chat-one", part_id="part/reply/1"
    )
    await _create_intent(
        payloads, delivery, suffix="chat-two", part_id="part/reply/2"
    )
    await _create_intent(
        payloads,
        delivery,
        suffix="control-one",
        part_id="part/control/1",
        lane="CONTROL",
        ordering_key="control/admin",
    )
    control = await delivery.claim(operation_id="claim/control", worker_id="worker/control")
    assert control.lane_kind == "CONTROL"
    await _prepare(payloads, authorization, delivery, grant, control, "control-one")
    await _observe_sent(payloads, delivery, "attempt/control-one", "control-one")

    first = await delivery.claim(operation_id="claim/chat-one", worker_id="worker/chat")
    assert first.intent_id == "intent/chat-one"
    await _prepare(payloads, authorization, delivery, grant, first, "chat-one")
    unknown = await delivery.observe(
        observation_operation_id="delivery-observe/chat-one/unknown",
        effect_observation_operation_id="effect-observe/chat-one/unknown",
        attempt_id="attempt/chat-one",
        observed_status="UNKNOWN",
        evidence_kind="EXECUTOR",
        error_code="RECEIPT_LOST",
    )
    assert unknown["accepted"] is True
    assert await delivery.claim(operation_id="claim/blocked", worker_id="worker/other") is None
    await _observe_sent(
        payloads,
        delivery,
        "attempt/chat-one",
        "chat-one/reconciled",
        evidence="RECONCILIATION",
    )
    second = await delivery.claim(operation_id="claim/chat-two", worker_id="worker/chat")
    assert second.intent_id == "intent/chat-two"


@pytest.mark.postgres
async def test_retryable_not_applied_reuses_part_effect_with_new_attempt(clean_delivery):
    _store, _storage, payloads, authorization, _effects, _scheduler, adapter, delivery, grant = (
        clean_delivery
    )
    await _active_binding(payloads, adapter)
    await _create_intent(payloads, delivery, suffix="retry", part_id="part/retry/1")
    first = await delivery.claim(operation_id="claim/retry/1", worker_id="worker/one")
    await _prepare(payloads, authorization, delivery, grant, first, "retry/1")
    result = await delivery.observe(
        observation_operation_id="delivery-observe/retry/1",
        effect_observation_operation_id="effect-observe/retry/1",
        attempt_id="attempt/retry/1",
        observed_status="RETRYABLE_NOT_APPLIED",
        evidence_kind="EXECUTOR",
        error_code="CONNECTION_CLOSED_BEFORE_WRITE",
    )
    assert result["accepted"] is True
    second = await delivery.claim(operation_id="claim/retry/2", worker_id="worker/two")
    assert second.part_operation_id == first.part_operation_id
    assert second.effect_operation_ref == first.effect_operation_ref
    assert second.owner_epoch == first.owner_epoch + 1
    prepared = await _prepare(payloads, authorization, delivery, grant, second, "retry/2")
    assert prepared["attempt_no"] == 2
    late = await _observe_sent(
        payloads,
        delivery,
        "attempt/retry/1",
        "retry/1/late-after-retry",
    )
    assert late["accepted"] is False
    assert late["previous_state"] == late["resulting_state"] == "NOT_APPLIED"


@pytest.mark.postgres
async def test_unknown_opposite_reconciliation_keeps_one_delivery_result(clean_delivery):
    _store, _storage, payloads, authorization, _effects, _scheduler, adapter, delivery, grant = (
        clean_delivery
    )
    await _active_binding(payloads, adapter)
    await _create_intent(payloads, delivery, suffix="race", part_id="part/race/1")
    claim = await delivery.claim(operation_id="claim/race", worker_id="worker/race")
    await _prepare(payloads, authorization, delivery, grant, claim, "race")
    await delivery.observe(
        observation_operation_id="delivery-observe/race/unknown",
        effect_observation_operation_id="effect-observe/race/unknown",
        attempt_id="attempt/race",
        observed_status="UNKNOWN",
        evidence_kind="EXECUTOR",
        error_code="RECEIPT_LOST",
    )

    async def sent():
        return await _observe_sent(
            payloads,
            delivery,
            "attempt/race",
            "race/sent",
            evidence="RECONCILIATION",
        )

    async def not_applied():
        return await delivery.observe(
            observation_operation_id="delivery-observe/race/not-applied",
            effect_observation_operation_id="effect-observe/race/not-applied",
            attempt_id="attempt/race",
            observed_status="RETRYABLE_NOT_APPLIED",
            evidence_kind="RECONCILIATION",
            error_code="CONFIRMED_NOT_APPLIED",
        )

    results = await asyncio.gather(sent(), not_applied())
    assert sorted(item["accepted"] for item in results) == [False, True]
    final = await delivery.load_intent("intent/race")
    accepted = next(item for item in results if item["accepted"])
    assert final["state"] == (
        "SENT" if accepted["observed_status"] == "SENT" else "QUEUED"
    )


@pytest.mark.postgres
async def test_cancel_abandons_only_prepared_effect_and_cannot_erase_handoff(clean_delivery):
    _store, storage, payloads, authorization, _effects, _scheduler, adapter, delivery, grant = (
        clean_delivery
    )
    await _active_binding(payloads, adapter)
    await _create_intent(
        payloads, delivery, suffix="cancel-prepared", part_id="part/reply/1"
    )
    prepared_claim = await delivery.claim(
        operation_id="claim/cancel-prepared", worker_id="worker/cancel"
    )
    await _prepare(
        payloads,
        authorization,
        delivery,
        grant,
        prepared_claim,
        "cancel-prepared",
        handoff=False,
    )
    cancelled = await delivery.cancel_intent(
        operation_id="cancel/prepared",
        intent_id="intent/cancel-prepared",
        reason_code="ADMIN_CANCELLED",
    )
    assert cancelled["resulting_state"] == "CANCELLED"
    assert (await delivery.load_intent("intent/cancel-prepared"))["state"] == "CANCELLED"
    async with _store.pool.connection() as conn:
        attempt = await (
            await conn.execute(
                sql.SQL("SELECT state FROM {}.transport_attempts WHERE bot_id=%s "
                        "AND attempt_id='attempt/cancel-prepared'").format(
                    sql.Identifier(storage["delivery"].spec.namespace)
                ),
                (_store.bot_id,),
            )
        ).fetchone()
    assert attempt["state"] == "ABANDONED"

    await _create_intent(
        payloads, delivery, suffix="cancel-dispatched", part_id="part/reply/2"
    )
    dispatched_claim = await delivery.claim(
        operation_id="claim/cancel-dispatched", worker_id="worker/cancel"
    )
    await _prepare(
        payloads,
        authorization,
        delivery,
        grant,
        dispatched_claim,
        "cancel-dispatched",
    )
    uncertain = await delivery.cancel_intent(
        operation_id="cancel/dispatched",
        intent_id="intent/cancel-dispatched",
        reason_code="ADMIN_CANCELLED",
    )
    assert uncertain["resulting_state"] == "UNKNOWN"
    assert (await delivery.load_intent("intent/cancel-dispatched"))["state"] == "SENDING"

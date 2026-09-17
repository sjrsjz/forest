from __future__ import annotations

import asyncio
import json
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
from forest.storage.adapter_onebot_authority import (
    OneBotAdapterRepository,
    OneBotPayloadRef,
    OneBotPlatformEvent,
)
from forest.storage.bot import BotStore
from forest.storage.conversation_scheduler import ConversationSchedulerRepository
from forest.storage.ingress_unit_of_work import OneBotIngressUnitOfWork
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller


@pytest_asyncio.fixture
async def clean_onebot():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_onebot_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-onebot")
    await store.open()
    installer = SchemaBundleInstaller(store)
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    scheduler_storage = official_storage_bundle(
        "conversation.scheduler",
        "forest.conversation.scheduler",
        "forest.conversation-scheduler.repository/1",
        "storage/official/conversation_scheduler.sql",
    )
    onebot_storage = official_storage_bundle(
        "adapter.onebot",
        "forest.adapter.onebot",
        "forest.adapter-onebot.repository/1",
        "storage/official/adapter_onebot.sql",
    )
    for storage in (payload_storage, scheduler_storage, onebot_storage):
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    scheduler = ConversationSchedulerRepository(
        store,
        namespace=scheduler_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    adapter = OneBotAdapterRepository(
        store,
        namespace=onebot_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    try:
        yield store, onebot_storage, scheduler_storage, payloads, scheduler, adapter
    finally:
        await store.close()
        assert name.startswith("forest_onebot_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _payload(
    payloads,
    *,
    owner_kind: str,
    owner_id: str,
    body: bytes,
    source_id: str,
) -> OneBotPayloadRef:
    epoch = await payloads.epoch()
    owner = ("adapter.onebot", owner_kind, owner_id)
    saved = await payloads.put_bytes(
        owner_component_id=owner[0],
        owner_kind=owner[1],
        owner_id=owner[2],
        media_type="application/json",
        content=body,
        sources=(
            PayloadSource(
                source_component_id="adapter.onebot",
                source_kind="onebot.source",
                source_id=source_id,
                source_revision=1,
                observation_binding_id="binding/main",
            ),
        ),
        expected_deletion_epoch=epoch,
    )
    return OneBotPayloadRef(saved.payload_id, saved.content_digest, *owner)


async def _configured(clean_onebot, *, enabled: bool = True):
    _store, _storage, _scheduler_storage, payloads, _scheduler, adapter = clean_onebot
    display = await _payload(
        payloads,
        owner_kind="binding.display",
        owner_id="binding/main",
        body=b'{"display_name":"Forest"}',
        source_id="binding/main/display",
    )
    await adapter.register_binding(
        operation_id="binding/register/main",
        binding_id="binding/main",
        self_id="123456789",
        display=display,
    )
    config = await _payload(
        payloads,
        owner_kind="binding.config",
        owner_id="binding/config/1",
        body=json.dumps(
            {
                "websocket_url": "ws://127.0.0.1:3001/",
                "transport": "FORWARD_WEBSOCKET",
            },
            separators=(",", ":"),
        ).encode(),
        source_id="binding/main/config/1",
    )
    await adapter.plan_configuration(
        plan_id="binding/config/1",
        binding_id="binding/main",
        expected_revision=0,
        enabled=enabled,
        receive_enabled=enabled,
        transport="FORWARD_WEBSOCKET",
        config=config,
        secret_ref="secret://onebot/main/1",
        secret_digest="sha256:" + "a" * 64,
        actor_principal_ref="admin/root",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    revision = await adapter.apply_configuration(
        operation_id="binding/apply/1",
        plan_id="binding/config/1",
    )
    return payloads, adapter, revision


async def _active(clean_onebot):
    payloads, adapter, revision = await _configured(clean_onebot)
    candidate = await adapter.prepare_client(
        client_incarnation="client/main/1",
        binding_id="binding/main",
        revision=revision["revision"],
        expected_binding_epoch=0,
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest="sha256:" + "b" * 64,
    )
    publication = await adapter.publish_client(
        operation_id="binding/publish/1",
        binding_id="binding/main",
        client_incarnation=candidate["client_incarnation"],
        revision=revision["revision"],
        expected_binding_epoch=0,
    )
    return payloads, adapter, publication


async def _event(payloads, number: int = 1) -> OneBotPlatformEvent:
    event_id = f"event/{number}"
    raw, normalized, author = await asyncio.gather(
        _payload(
            payloads,
            owner_kind="onebot.raw-event",
            owner_id=event_id,
            body=(f'{{"message_id":{number},"raw_message":"hello"}}').encode(),
            source_id=event_id + "/raw",
        ),
        _payload(
            payloads,
            owner_kind="onebot.normalized-event",
            owner_id=event_id,
            body=b'{"parts":[{"type":"text","text":"hello"}]}',
            source_id=event_id + "/normalized",
        ),
        _payload(
            payloads,
            owner_kind="onebot.author-snapshot",
            owner_id=event_id,
            body=b'{"subject":"qq:42","display_name":"Alice"}',
            source_id=event_id + "/author",
        ),
    )
    now = datetime.now(timezone.utc)
    return OneBotPlatformEvent(
        event_id=event_id,
        platform_message_ref=str(number),
        event_kind="group.message",
        channel_kind="GROUP",
        channel_ref="933179609",
        conversation_ref="onebot:group:933179609",
        author_subject_ref="qq:42",
        raw=raw,
        normalized=normalized,
        author=author,
        sent_at=now - timedelta(milliseconds=10),
        received_at=now,
        classification_deadline=now + timedelta(minutes=2),
    )


@pytest.mark.postgres
async def test_clean_onebot_schema_excludes_conversation_delivery_shell_and_secrets(clean_onebot):
    _store, storage, _scheduler_storage, _payloads, _scheduler, _adapter = clean_onebot
    lowered = storage.baseline.lower()
    for forbidden in (
        b"jsonb",
        b"send_intent",
        b"delivery_attempt",
        b"conversation_lane",
        b"context_unit",
        b"scheduler_ingress_seq",
        b"shell_command",
        b"access_token",
        b"webhook_secret",
        b"agent_run",
    ):
        assert forbidden not in lowered
    assert b"secret_ref" in lowered
    assert b"platform_events" in lowered


@pytest.mark.postgres
async def test_binding_registration_and_config_revision_are_exact_and_convergent(clean_onebot):
    store, storage, _scheduler_storage, payloads, _scheduler, adapter = clean_onebot
    display = await _payload(
        payloads,
        owner_kind="binding.display",
        owner_id="binding/main",
        body=b'{"display_name":"Forest"}',
        source_id="binding/main/display",
    )
    first, replay = await asyncio.gather(
        adapter.register_binding(
            operation_id="binding/register/main",
            binding_id="binding/main",
            self_id="123456789",
            display=display,
        ),
        adapter.register_binding(
            operation_id="binding/register/main",
            binding_id="binding/main",
            self_id="123456789",
            display=display,
        ),
    )
    assert first == replay
    config_left, config_right = await asyncio.gather(
        _payload(
            payloads,
            owner_kind="binding.config",
            owner_id="config/left",
            body=b'{"endpoint":"ws://left/"}',
            source_id="config/left",
        ),
        _payload(
            payloads,
            owner_kind="binding.config",
            owner_id="config/right",
            body=b'{"endpoint":"ws://right/"}',
            source_id="config/right",
        ),
    )

    async def plan(plan_id, config, secret):
        return await adapter.plan_configuration(
            plan_id=plan_id,
            binding_id="binding/main",
            expected_revision=0,
            enabled=True,
            receive_enabled=True,
            transport="FORWARD_WEBSOCKET",
            config=config,
            secret_ref=secret,
            secret_digest="sha256:" + ("1" if "left" in plan_id else "2") * 64,
            actor_principal_ref="admin/root",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        )

    await asyncio.gather(
        plan("config/left", config_left, "secret://left"),
        plan("config/right", config_right, "secret://right"),
    )

    async def apply(operation, plan_id):
        try:
            return await adapter.apply_configuration(
                operation_id=operation,
                plan_id=plan_id,
            )
        except ForestError as exc:
            return exc.code

    left, right = await asyncio.gather(
        apply("apply/left", "config/left"),
        apply("apply/right", "config/right"),
    )
    assert [isinstance(value, dict) for value in (left, right)].count(True) == 1
    assert "BINDING_REVISION_CONFLICT" in {
        value for value in (left, right) if isinstance(value, str)
    }
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL("SELECT secret_ref,config_payload_id FROM {}.binding_revisions "
                        "WHERE bot_id=%s").format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id,),
            )
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["secret_ref"].startswith("secret://")


@pytest.mark.postgres
async def test_connection_publication_retirement_and_late_observation_are_one_way(clean_onebot):
    store, storage, _scheduler_storage, _payloads, _scheduler, _adapter = clean_onebot
    _payloads, adapter, publication = await _active(clean_onebot)
    assert publication["resulting_binding_epoch"] == 1
    head = await adapter.load_binding("binding/main")
    assert (head["state"], head["binding_epoch"], head["connection_epoch"]) == (
        "ONLINE",
        1,
        1,
    )
    retiring = await adapter.observe_client(
        operation_id="client/retire/1",
        client_incarnation="client/main/1",
        observed_state="RETIRING",
    )
    assert retiring["accepted"] is True
    closed = await adapter.observe_client(
        operation_id="client/close/1",
        client_incarnation="client/main/1",
        observed_state="CLOSED",
    )
    assert closed["accepted"] is True
    late = await adapter.observe_client(
        operation_id="client/late-failure/1",
        client_incarnation="client/main/1",
        observed_state="FAILED",
        error_code="SOCKET_CLOSED",
    )
    assert late["accepted"] is False
    assert late["previous_state"] == late["resulting_state"] == "CLOSED"
    assert late["error_code"] == "SOCKET_CLOSED"
    assert (await adapter.load_binding("binding/main"))["state"] == "OFFLINE"
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.binding_clients SET state='ACTIVE' WHERE bot_id=%s "
                        "AND client_incarnation='client/main/1'").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )


@pytest.mark.postgres
async def test_platform_event_and_conversation_inbox_commit_atomically_and_dedupe(clean_onebot):
    store, storage, scheduler_storage, _payloads, scheduler, _adapter = clean_onebot
    payloads, adapter, publication = await _active(clean_onebot)
    ingress = OneBotIngressUnitOfWork(store, adapter=adapter, conversation=scheduler)
    event = await _event(payloads)
    one, two = await asyncio.gather(
        ingress.admit(
            operation_id="event/admit/a",
            binding_id="binding/main",
            expected_binding_epoch=publication["resulting_binding_epoch"],
            client_incarnation="client/main/1",
            event=event,
        ),
        ingress.admit(
            operation_id="event/admit/b",
            binding_id="binding/main",
            expected_binding_epoch=publication["resulting_binding_epoch"],
            client_incarnation="client/main/1",
            event=event,
        ),
    )
    assert {one["adapter"]["duplicate"], two["adapter"]["duplicate"]} == {False, True}
    assert one["conversation"]["ingress_seq"] == two["conversation"]["ingress_seq"] == 1
    async with store.pool.connection() as conn:
        adapter_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.platform_events WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
        inbox_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.inbox_messages WHERE bot_id=%s").format(
                    sql.Identifier(scheduler_storage.spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
    assert adapter_count["count"] == inbox_count["count"] == 1


@pytest.mark.postgres
async def test_failed_conversation_participant_rolls_back_adapter_event(clean_onebot):
    store, storage, _scheduler_storage, payloads, _scheduler, adapter = clean_onebot
    payloads, adapter, publication = await _active(clean_onebot)
    event = await _event(payloads, 2)

    class FailingConversation:
        @staticmethod
        async def admit_message_in(_conn, **_values):
            raise ForestError("TEST_PARTICIPANT_FAILED", "forced", 503)

    ingress = OneBotIngressUnitOfWork(
        store, adapter=adapter, conversation=FailingConversation()
    )
    with pytest.raises(ForestError) as caught:
        await ingress.admit(
            operation_id="event/admit/fail",
            binding_id="binding/main",
            expected_binding_epoch=publication["resulting_binding_epoch"],
            client_incarnation="client/main/1",
            event=event,
        )
    assert caught.value.code == "TEST_PARTICIPANT_FAILED"
    async with store.pool.connection() as conn:
        count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.platform_events WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
    assert count["count"] == 0

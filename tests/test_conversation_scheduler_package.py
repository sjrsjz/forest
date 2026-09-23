from __future__ import annotations

from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.packages import PackageCatalog
from forest.runtime_modules import RuntimeModuleRegistry


async def _payload(payloads, *, owner_kind: str, event_id: str, content: bytes) -> dict:
    saved = await payloads.put_bytes(
        owner_component_id="adapter.onebot",
        owner_kind=owner_kind,
        owner_id=event_id,
        media_type="application/json",
        content=content,
        expected_deletion_epoch=await payloads.epoch(),
    )
    return {
        "payload_id": saved.payload_id,
        "digest": saved.content_digest,
        "owner_component_id": "adapter.onebot",
        "owner_kind": owner_kind,
        "owner_id": event_id,
    }


@pytest.mark.postgres
async def test_conversation_package_owns_ingress_and_classification_participants(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/conversation_scheduler"))),
        PackageCatalog(tmp_path / "conversation-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={
            "protocol": "forest.conversation.scheduler/2",
            "context_view": {
                "high_watermark_units": 512,
                "retain_units": 256,
                "policy_revision": "context-view-unit-window-v1",
            },
        },
    )
    lease = await kernel.runtime.claim_host("conversation-host", "conversation-incarnation")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(host, (*kernel.descriptors, package.descriptor))
    await registry.start()
    try:
        instance = registry.get("conversation.scheduler")
        participant = instance.unit_of_work_participant()
        event_id = "event/onebot/1"
        content = await _payload(
            kernel.payload,
            owner_kind="onebot.normalized-event",
            event_id=event_id,
            content=b'{"segments":[{"type":"text","text":"A"}]}',
        )
        author = await _payload(
            kernel.payload,
            owner_kind="onebot.author-snapshot",
            event_id=event_id,
            content=b'{"subject_ref":"qq:42","display_name":"tester"}',
        )
        received_at = datetime.now(timezone.utc)
        admission = {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "adapter.onebot",
                "generation_id": "generation/adapter/1",
            },
            "input": {
                "conversation_id": "conversation/onebot/group/933179609",
                "source_component_id": "adapter.onebot",
                "binding_ref": "binding/main",
                "platform": "onebot-v11",
                "channel_kind": "GROUP",
                "channel_ref": "933179609",
                "thread_ref": None,
                "event_id": event_id,
                "source_event_ref": "onebot-message/1",
                "event_kind": "message.group",
                "platform_message_ref": "message/1",
                "author_subject_ref": "qq:42",
                "content": content,
                "author": author,
                "received_at": received_at.isoformat(),
                "classification_deadline": (
                    received_at + timedelta(seconds=30)
                ).isoformat(),
                "sent_at": received_at.isoformat(),
                "context_disposition": "MATERIALIZE",
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            first = await participant.invoke_in(
                conn,
                "conversation.admit-message",
                admission,
                operation_id="uow-step:conversation-admit/1",
                causal_root_kind="onebot.ingress",
                causal_root_id=event_id,
                parent_call_id="call:ingress/1",
                timeout_seconds=10,
            )
        async with store.pool.connection() as conn, conn.transaction():
            replay = await participant.invoke_in(
                conn,
                "conversation.admit-message",
                admission,
                operation_id="uow-step:conversation-admit/1",
                causal_root_kind="onebot.ingress",
                causal_root_id=event_id,
                parent_call_id="call:ingress/replay",
                timeout_seconds=10,
            )
        assert first == {
            "conversation_id": "conversation/onebot/group/933179609",
            "event_id": event_id,
            "ingress_seq": 1,
            "duplicate": False,
        }
        assert replay == {**first, "duplicate": True}

        classification = {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "adapter.onebot",
                "generation_id": "generation/adapter/1",
            },
            "input": {
                "conversation_id": first["conversation_id"],
                "event_id": event_id,
                "decision": "TRIGGER",
                "classifier_component_id": "conversation.trigger-policy",
                "classifier_generation_id": "generation/trigger/1",
                "rule_revision": "rule/1",
                "reason_code": "mention",
                "route_ref": None,
                "trigger_id": "trigger/1",
                "principal_ref": "qq:42",
            },
            "prior": {"admit": first},
        }
        async with store.pool.connection() as conn, conn.transaction():
            result = await participant.invoke_in(
                conn,
                "conversation.complete-classification",
                classification,
                operation_id="uow-step:conversation-classify/1",
                causal_root_kind="onebot.ingress",
                causal_root_id=event_id,
                parent_call_id="call:ingress/1",
                timeout_seconds=10,
            )
        assert result == {
            "event_id": event_id,
            "ingress_seq": 1,
            "decision": "TRIGGER",
            "classified_through_seq": 1,
            "duplicate": False,
        }
        actor = {
            "conversation_id": first["conversation_id"],
            "owner_id": "claim/conversation/1",
            "epoch": 1,
        }
        namespace = package.descriptor.storage.spec.namespace
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET actor_owner_id=%s,actor_epoch=%s,"
                    "actor_expires_at=clock_timestamp()+interval '5 minutes' "
                    "WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (
                    actor["owner_id"],
                    actor["epoch"],
                    store.bot_id,
                    actor["conversation_id"],
                ),
            )
        selection = {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "conversation.scheduler",
                "generation_id": participant.generation_id,
            },
            "input": {
                "actor": actor,
                "boundary": "IDLE",
                "session_ref": "session/1",
                "run_ref": "run/1",
                "next_run_ref": None,
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            selected = await participant.invoke_in(
                conn,
                "conversation.select-input",
                selection,
                operation_id="uow-step:conversation-select/1",
                causal_root_kind="conversation.actor",
                causal_root_id=actor["conversation_id"],
                parent_call_id="call:conversation/1",
                timeout_seconds=10,
            )
        async with store.pool.connection() as conn, conn.transaction():
            selected_replay = await participant.invoke_in(
                conn,
                "conversation.select-input",
                selection,
                operation_id="uow-step:conversation-select/1",
                causal_root_kind="conversation.actor",
                causal_root_id=actor["conversation_id"],
                parent_call_id="call:conversation/replay",
                timeout_seconds=10,
            )
        assert selected_replay == selected
        assert selected["status"] == "BATCH"
        assert selected["kind"] == "TRIGGER_INPUT"
        assert selected["event_ids"] == [event_id]
        assert selected["trigger_ids"] == ["trigger/1"]
        assert selected["session_ref"] == "session/1"
        assert selected["run_ref"] == "run/1"
        assert selected["selected_input_end_seq"] == 1
        async with store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT status,principal_ref FROM {}.conversation_triggers "
                        "WHERE bot_id=%s AND trigger_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "trigger/1"),
                )
            ).fetchone()
            context = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT event_id,author_subject_ref,trigger_decision "
                        "FROM {}.context_units WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"]),
                )
            ).fetchone()
        assert dict(row) == {"status": "CLAIMED", "principal_ref": "qq:42"}
        assert dict(context) == {
            "event_id": event_id,
            "author_subject_ref": "qq:42",
            "trigger_decision": "TRIGGER",
        }
    finally:
        assert await registry.close() == []

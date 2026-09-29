from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.package_capabilities import PackageCallScope
from forest.packages import PackageCatalog
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
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
                "high_watermark_units": 2,
                "retain_units": 1,
                "policy_revision": "context-view-unit-window-v1",
            },
        },
        resolved_contracts={},
    )
    memory = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/memory_graph"))),
        PackageCatalog(tmp_path / "memory-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={
            "protocol": "forest.memory.graph/2",
            "embedding": {
                "model_kind": "TEXT_IMAGE",
                "deployment_id": "embedding.multimodal/test",
                "model_revision": "test/1",
                "model": "Qwen/Qwen3-VL-Embedding-8B",
                "dimensions": 4096,
                "normalization": "L2",
            },
            "fusion_policy_revision": "independent-facets/1",
            "retrieval": {
                "policy_revision": "qiyu-graph-region/1",
                "seed_limit": 4,
                "candidate_limit_per_facet": 64,
                "min_similarity": 0.35,
                "min_graph_relevance": 0.05,
                "max_graph_nodes": 32,
                "max_association_edges_per_node": 32,
                "max_retrieval_chars": 24000,
                "sequence_weight": 0.55,
                "association_weight": 0.9,
            },
            "embedding_budget": {
                "policy_revision": "daily-cost-cap/1",
                "daily_request_limit": 1000,
                "daily_token_limit": 10000000,
                "reservation_tokens": 32768,
            },
        },
        resolved_contracts={},
    )
    lease = await kernel.runtime.claim_host("conversation-host", "conversation-incarnation")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(
        host, (*kernel.descriptors, memory.descriptor, package.descriptor)
    )
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
                "channel_kind": "SYSTEM",
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
                "selection_mode": "ANY",
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
        assert selected["trigger_claim"]["count"] == 1
        assert selected["trigger_claim"]["last_trigger_ref"] == "trigger/1"
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

        authority_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "agent.runtime", "generation_id": "agent/test"},
            "input": {"run_ref": "run/1", "trigger_ref": "trigger/1"},
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            authorized = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_authorize_run_trigger(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace)
                    ),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(authority_envelope)),
                )
            ).fetchone()
            wrong_run = copy.deepcopy(authority_envelope)
            wrong_run["input"]["run_ref"] = "run/other"
            rejected = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_authorize_run_trigger(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace)
                    ),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(wrong_run)),
                )
            ).fetchone()
        assert authorized["result"]["value"] == {
            "protocol": "forest.conversation-run-trigger-authority/1",
            "run_ref": "run/1",
            "trigger_ref": "trigger/1",
            "session_ref": "session/1",
            "conversation_ref": actor["conversation_id"],
            "principal_ref": "qq:42",
        }
        assert rejected["result"]["error"]["code"] == "AGENT_TOOL_AUTHORITY_INVALID"

        terminal = {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "conversation.scheduler",
                "generation_id": participant.generation_id,
            },
            "input": {
                "actor": actor,
                "session_ref": "session/1",
                "run_ref": "run/1",
                "terminal_status": "FAILED",
                "reason_code": "AGENT_PROTOCOL_FAILED",
            },
            "prior": {},
        }
        class RollbackProbe(Exception):
            pass

        with pytest.raises(RollbackProbe):
            async with store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.inbox_messages SELECT "
                        "(jsonb_populate_record(NULL::{}.inbox_messages,to_jsonb(m)||"
                        "jsonb_build_object('event_id','event/other-run',"
                        "'ingress_seq',2,'admission_operation_id','admit/other-run',"
                        "'classification_operation_id','classify/other-run'))).* "
                        "FROM {}.inbox_messages m WHERE m.bot_id=%s AND m.event_id=%s"
                    ).format(*(sql.Identifier(namespace) for _ in range(3))),
                    (store.bot_id, event_id),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.conversation_triggers("
                        "bot_id,conversation_id,trigger_id,event_id,ingress_seq,"
                        "principal_ref,input_digest,status,claimed_session_ref,"
                        "claimed_run_ref,input_batch_id) VALUES "
                        "(%s,%s,'trigger/other-run','event/other-run',2,'qq:other',"
                        "%s,'CLAIMED','session/1','run/other',%s)"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"],
                     "sha256:" + "0" * 64, selected["input_batch_id"]),
                )
                guarded = await participant.invoke_in(
                    conn, "conversation.terminalize-run", terminal,
                    operation_id="uow-step:conversation-terminal/probe",
                    causal_root_kind="conversation.agent-run",
                    causal_root_id="run/1",
                    parent_call_id="call:conversation/terminal-probe",
                    timeout_seconds=10,
                )
                assert guarded["trigger_count"] == 1
                other = await (
                    await conn.execute(
                        sql.SQL("SELECT status FROM {}.conversation_triggers "
                                "WHERE bot_id=%s AND trigger_id='trigger/other-run'")
                        .format(sql.Identifier(namespace)),
                        (store.bot_id,),
                    )
                ).fetchone()
                assert other["status"] == "CLAIMED"
                raise RollbackProbe
        async with store.pool.connection() as conn, conn.transaction():
            terminal_result = await participant.invoke_in(
                conn,
                "conversation.terminalize-run",
                terminal,
                operation_id="uow-step:conversation-terminal/1",
                causal_root_kind="conversation.agent-run",
                causal_root_id="run/1",
                parent_call_id="call:conversation/terminal-1",
                timeout_seconds=10,
            )
        async with store.pool.connection() as conn, conn.transaction():
            terminal_replay = await participant.invoke_in(
                conn,
                "conversation.terminalize-run",
                terminal,
                operation_id="uow-step:conversation-terminal/1",
                causal_root_kind="conversation.agent-run",
                causal_root_id="run/1",
                parent_call_id="call:conversation/terminal-replay",
                timeout_seconds=10,
            )
        assert terminal_replay == terminal_result == {
            "protocol": "forest.conversation-run-terminal/1",
            "conversation_id": actor["conversation_id"],
            "session_ref": "session/1",
            "run_ref": "run/1",
            "terminal_status": "FAILED",
            "reason_code": "AGENT_PROTOCOL_FAILED",
            "trigger_count": 1,
        }
        async with store.pool.connection() as conn:
            terminal_state = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT t.status,t.terminal_reason_code,l.active_session_ref,"
                        "l.active_run_ref FROM {}.conversation_triggers t JOIN "
                        "{}.conversation_lanes l ON l.bot_id=t.bot_id AND "
                        "l.conversation_id=t.conversation_id WHERE t.bot_id=%s "
                        "AND t.trigger_id=%s"
                    ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                    (store.bot_id, "trigger/1"),
                )
            ).fetchone()
        assert dict(terminal_state) == {
            "status": "FAILED",
            "terminal_reason_code": "AGENT_PROTOCOL_FAILED",
            "active_session_ref": None,
            "active_run_ref": None,
        }

        async with store.pool.connection() as conn, conn.transaction():
            for revision in (2, 3):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.context_units (bot_id,conversation_id,unit_id,"
                        "context_revision,unit_index,kind,source_component_id,source_kind,"
                        "source_id,payload_id,payload_digest,payload_owner_component_id,"
                        "payload_owner_kind,payload_owner_id) "
                        "SELECT bot_id,conversation_id,%s,%s,0,'CONTROL',"
                        "source_component_id,'context.control',%s,payload_id,payload_digest,"
                        "payload_owner_component_id,payload_owner_kind,payload_owner_id "
                        "FROM {}.context_units WHERE bot_id=%s AND conversation_id=%s "
                        "AND event_id=%s"
                    ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                    (
                        f"control/pre-run/{revision}", revision,
                        f"control/pre-run/{revision}", store.bot_id,
                        actor["conversation_id"], event_id,
                    ),
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.active_context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index) "
                    "SELECT bot_id,conversation_id,unit_id,context_revision,unit_index "
                    "FROM {}.context_units WHERE bot_id=%s AND conversation_id=%s "
                    "AND context_revision IN (2,3)"
                ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                (store.bot_id, actor["conversation_id"]),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET context_revision=3,"
                    "context_view_unit_count=3 WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, actor["conversation_id"]),
            )
        compact_plan = next(
            item for item in instance.component["unit_of_work_plans"]
            if item["name"] == "conversation.compact-before-run"
        )
        binding = instance._prepared_binding
        pre_run_input = {
            "anchor": {
                "protocol": "forest.conversation-pre-run-anchor/1",
                "conversation_id": actor["conversation_id"],
                "actor": actor,
            },
            "context_view": {
                "high_watermark_units": 2,
                "retain_units": 1,
                "policy_revision": "context-view-unit-window-v1",
            },
            "payload": {
                "protocol": "forest.payload-json-from-prior/1",
                "prior_step_id": "prepare",
            },
            "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
            "memory": {},
        }
        pre_run = await instance._unit_of_work_coordinator.execute(
            PackageCallScope(
                bot_id=store.bot_id,
                component_id="conversation.scheduler",
                generation_id=participant.generation_id,
                instance_id=binding.instance_id,
                worker_incarnation=binding.worker_incarnation,
                call_id="call:conversation/pre-run",
                call_operation_id="conversation/pre-run/test",
                causal_root_kind="conversation.agent-run",
                causal_root_id="run/pre-run/test",
            ),
            compact_plan,
            pre_run_input,
            parent_call_id="call:conversation/pre-run/root",
            timeout_seconds=20,
        )
        assert pre_run["protocol"] == "forest.context-pre-run-compaction/1"
        assert pre_run["status"] == "COMPACTED"
        assert pre_run["context_view_revision"] == 1
        assert pre_run["memory_archive"]["event_refs"] == [event_id]
        async with store.pool.connection() as conn:
            view = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT context_view_revision,context_view_unit_count,"
                        "context_archived_unit_count FROM {}.conversation_lanes "
                        "WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"]),
                )
            ).fetchone()
        assert view == {
            "context_view_revision": 1,
            "context_view_unit_count": 1,
            "context_archived_unit_count": 2,
        }
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET active_session_ref=%s,"
                    "active_run_ref=%s WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                ("session/pre-run", "run/pre-run", store.bot_id, actor["conversation_id"]),
            )
        replayed_pre_run = await instance._unit_of_work_coordinator.execute(
            PackageCallScope(
                bot_id=store.bot_id,
                component_id="conversation.scheduler",
                generation_id=participant.generation_id,
                instance_id=binding.instance_id,
                worker_incarnation=binding.worker_incarnation,
                call_id="call:conversation/pre-run/replay",
                call_operation_id="conversation/pre-run/test",
                causal_root_kind="conversation.agent-run",
                causal_root_id="run/pre-run/test",
            ),
            compact_plan,
            pre_run_input,
            parent_call_id="call:conversation/pre-run/replay-root",
            timeout_seconds=20,
        )
        assert replayed_pre_run == pre_run

        silent_event_id = "event/onebot/silent-1"
        silent_content = await _payload(
            kernel.payload,
            owner_kind="onebot.normalized-event",
            event_id=silent_event_id,
            content=b'{"segments":[{"type":"text","text":"ordinary"}]}',
        )
        silent_author = await _payload(
            kernel.payload,
            owner_kind="onebot.author-snapshot",
            event_id=silent_event_id,
            content=b'{"subject_ref":"qq:43","display_name":"ordinary"}',
        )
        silent_admission = copy.deepcopy(admission)
        silent_admission["input"].update(
            {
                "conversation_id": "conversation/onebot/group/silent",
                "channel_kind": "GROUP",
                "channel_ref": "silent",
                "event_id": silent_event_id,
                "source_event_ref": "onebot-message/silent-1",
                "platform_message_ref": "message/silent-1",
                "author_subject_ref": "qq:43",
                "content": silent_content,
                "author": silent_author,
            }
        )
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn,
                "conversation.admit-message",
                silent_admission,
                operation_id="uow-step:conversation-admit/silent-1",
                causal_root_kind="onebot.ingress",
                causal_root_id=silent_event_id,
                parent_call_id="call:ingress/silent-1",
                timeout_seconds=10,
            )
        silent_classification = copy.deepcopy(classification)
        silent_classification["input"].update(
            {
                "conversation_id": "conversation/onebot/group/silent",
                "event_id": silent_event_id,
                "decision": "NO_TRIGGER",
                "reason_code": "ordinary",
                "trigger_id": None,
                "principal_ref": None,
            }
        )
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn,
                "conversation.complete-classification",
                silent_classification,
                operation_id="uow-step:conversation-classify/silent-1",
                causal_root_kind="onebot.ingress",
                causal_root_id=silent_event_id,
                parent_call_id="call:ingress/silent-1",
                timeout_seconds=10,
            )
        silent_state = None
        for _attempt in range(100):
            async with store.pool.connection() as conn:
                silent_state = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT context_applied_seq,actor_owner_id,active_session_ref "
                            "FROM {}.conversation_lanes WHERE bot_id=%s "
                            "AND conversation_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id, "conversation/onebot/group/silent"),
                    )
                ).fetchone()
            if silent_state["context_applied_seq"] == 1 and silent_state["actor_owner_id"] is None:
                break
            await asyncio.sleep(0.05)
        assert dict(silent_state) == {
            "context_applied_seq": 1,
            "actor_owner_id": None,
            "active_session_ref": None,
        }
        async with store.pool.connection() as conn:
            silent_unit = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT event_id,author_subject_ref,trigger_decision "
                        "FROM {}.context_units WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchone()
            settlements = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS count FROM "
                        "{}.conversation_silent_drain_settlements"
                    ).format(sql.Identifier(namespace))
                )
            ).fetchone()
        assert dict(silent_unit) == {
            "event_id": silent_event_id,
            "author_subject_ref": "qq:43",
            "trigger_decision": "NO_TRIGGER",
        }
        assert settlements["count"] == 1

        for index in (2, 3):
            next_event_id = f"event/onebot/silent-{index}"
            next_content = await _payload(
                kernel.payload,
                owner_kind="onebot.normalized-event",
                event_id=next_event_id,
                content=(f'{{"segments":[{{"type":"text","text":"ordinary-{index}"}}]}}').encode(),
            )
            next_author = await _payload(
                kernel.payload,
                owner_kind="onebot.author-snapshot",
                event_id=next_event_id,
                content=b'{"subject_ref":"qq:43","display_name":"ordinary"}',
            )
            next_admission = copy.deepcopy(silent_admission)
            next_admission["input"].update(
                {
                    "event_id": next_event_id,
                    "source_event_ref": f"onebot-message/silent-{index}",
                    "platform_message_ref": f"message/silent-{index}",
                    "content": next_content,
                    "author": next_author,
                }
            )
            async with store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn,
                    "conversation.admit-message",
                    next_admission,
                    operation_id=f"uow-step:conversation-admit/silent-{index}",
                    causal_root_kind="onebot.ingress",
                    causal_root_id=next_event_id,
                    parent_call_id=f"call:ingress/silent-{index}",
                    timeout_seconds=10,
                )
            next_classification = copy.deepcopy(silent_classification)
            next_classification["input"].update({"event_id": next_event_id})
            async with store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn,
                    "conversation.complete-classification",
                    next_classification,
                    operation_id=f"uow-step:conversation-classify/silent-{index}",
                    causal_root_kind="onebot.ingress",
                    causal_root_id=next_event_id,
                    parent_call_id=f"call:ingress/silent-{index}",
                    timeout_seconds=10,
                )

        compacted = None
        for _attempt in range(100):
            async with store.pool.connection() as conn:
                compacted = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT context_applied_seq,context_view_revision,"
                            "context_view_unit_count,context_archived_unit_count,actor_owner_id "
                            "FROM {}.conversation_lanes WHERE bot_id=%s "
                            "AND conversation_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id, "conversation/onebot/group/silent"),
                    )
                ).fetchone()
            if (
                compacted["context_applied_seq"] == 3
                and compacted["context_view_revision"] == 1
                and compacted["actor_owner_id"] is None
            ):
                break
            await asyncio.sleep(0.05)
        assert dict(compacted) == {
            "context_applied_seq": 3,
            "context_view_revision": 1,
            "context_view_unit_count": 1,
            "context_archived_unit_count": 2,
            "actor_owner_id": None,
        }
        async with store.pool.connection() as conn:
            archive = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT compaction_id,archive_payload_id,archive_payload_digest,"
                        "archived_unit_count,resulting_view_unit_count "
                        "FROM {}.context_compactions WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchone()
            archived_units = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS count FROM {}.context_compaction_units "
                        "WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchone()
            original_authors = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT array_agg(author_subject_ref ORDER BY ingress_seq) AS authors "
                        "FROM {}.context_units WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchone()
        assert archive["archived_unit_count"] == 2
        assert archive["resulting_view_unit_count"] == 1
        assert archived_units["count"] == 2
        assert original_authors["authors"] == ["qq:43", "qq:43", "qq:43"]
        async with store.pool.connection() as conn, conn.transaction():
            verified = await kernel.payload.verify_in(
                conn,
                payload_id=archive["archive_payload_id"],
                digest=archive["archive_payload_digest"],
                owner_component_id="conversation.scheduler",
                owner_kind="context.archive",
                owner_id=archive["compaction_id"],
            )
        assert verified.content_digest == archive["archive_payload_digest"]
        async with store.pool.connection() as conn, conn.transaction():
            snapshot_actor = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {}.conversation_lanes SET actor_owner_id=%s,"
                        "actor_epoch=actor_epoch+1,"
                        "actor_expires_at=clock_timestamp()+interval '5 minutes' "
                        "WHERE bot_id=%s AND conversation_id=%s "
                        "RETURNING conversation_id,actor_owner_id,actor_epoch"
                    ).format(sql.Identifier(namespace)),
                    (
                        "claim/context-snapshot",
                        store.bot_id,
                        "conversation/onebot/group/silent",
                    ),
                )
            ).fetchone()
        endpoint = instance.runtime_service_handlers()[("conversation.context.snapshot", 2)]
        route = FrozenServiceRoute(
            service_id="conversation.context.snapshot",
            version=2,
            caller_component_id="kernel.runtime",
            provider_component_id="conversation.scheduler",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=False,
        )
        snapshot = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "conversation:snapshot:1",
                10,
                "test.conversation",
                "conversation/onebot/group/silent",
            ),
            (
                {
                    "actor": {
                        "conversation_id": snapshot_actor["conversation_id"],
                        "owner_id": snapshot_actor["actor_owner_id"],
                        "epoch": snapshot_actor["actor_epoch"],
                    }
                },
            ),
            {},
        )
        assert snapshot["protocol"] == "forest.conversation-context-snapshot/2"
        assert snapshot["context_view_revision"] == 1
        assert snapshot["unit_count"] == 2
        manifest = json.loads(
            await kernel.payload.read_bytes(snapshot["snapshot"]["payload_id"])
        )
        assert manifest["archive"]["compaction_id"] == archive["compaction_id"]
        assert manifest["tree_depth"] == 0
        assert len(manifest["roots"]) == 1
        page = json.loads(
            await kernel.payload.read_bytes(manifest["roots"][0]["payload_id"])
        )
        assert [unit["source_kind"] for unit in page["units"]] == [
            "context.archive",
            "conversation.input",
        ]
        assert page["units"][1]["author_subject_ref"] == "qq:43"

        instance.freeze_owned_admission()
        await instance.drain_owned_work()
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET actor_owner_id=NULL,"
                    "actor_expires_at=NULL WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, "conversation/onebot/group/silent"),
            )
        for index in range(4, 69):
            next_event_id = f"event/onebot/silent-{index}"
            next_content = await _payload(
                kernel.payload,
                owner_kind="onebot.normalized-event",
                event_id=next_event_id,
                content=b'{"segments":[{"type":"text","text":"backlog"}]}',
            )
            next_author = await _payload(
                kernel.payload,
                owner_kind="onebot.author-snapshot",
                event_id=next_event_id,
                content=b'{"subject_ref":"qq:43","display_name":"ordinary"}',
            )
            next_admission = copy.deepcopy(silent_admission)
            now = datetime.now(timezone.utc)
            next_admission["input"].update(
                {
                    "event_id": next_event_id,
                    "source_event_ref": f"onebot-message/silent-{index}",
                    "platform_message_ref": f"message/silent-{index}",
                    "content": next_content,
                    "author": next_author,
                    "received_at": now.isoformat(),
                    "classification_deadline": (now + timedelta(seconds=30)).isoformat(),
                    "sent_at": now.isoformat(),
                }
            )
            next_classification = copy.deepcopy(silent_classification)
            next_classification["input"]["event_id"] = next_event_id
            async with store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn, "conversation.admit-message", next_admission,
                    operation_id=f"uow-step:conversation-admit/silent-{index}",
                    causal_root_kind="onebot.ingress", causal_root_id=next_event_id,
                    parent_call_id=f"call:ingress/silent-{index}", timeout_seconds=10,
                )
                await participant.invoke_in(
                    conn, "conversation.complete-classification", next_classification,
                    operation_id=f"uow-step:conversation-classify/silent-{index}",
                    causal_root_kind="onebot.ingress", causal_root_id=next_event_id,
                    parent_call_id=f"call:ingress/silent-{index}", timeout_seconds=10,
                )
        instance.open_owned_admission()
        async with asyncio.timeout(60):
            while True:
                async with store.pool.connection() as conn:
                    lane = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT context_applied_seq,classified_through_seq,"
                                "actor_owner_id FROM {}.conversation_lanes "
                                "WHERE bot_id=%s AND conversation_id=%s"
                            ).format(sql.Identifier(namespace)),
                            (store.bot_id, "conversation/onebot/group/silent"),
                        )
                    ).fetchone()
                if lane["context_applied_seq"] == 68 and lane["actor_owner_id"] is None:
                    break
                await asyncio.sleep(0.1)
        assert lane["classified_through_seq"] == 68
        async with store.pool.connection() as conn:
            batches = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT start_seq,end_seq FROM {}.input_batches "
                        "WHERE bot_id=%s AND conversation_id=%s "
                        "AND kind='SILENT_DRAIN' AND start_seq>=4 ORDER BY start_seq"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchall()
        assert [(row["start_seq"], row["end_seq"]) for row in batches] == [
            (4, 67), (68, 68)
        ]

        instance.freeze_owned_admission()
        await instance.drain_owned_work()
        async with store.pool.connection() as conn, conn.transaction():
            prior = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT context_revision,context_view_revision,"
                        "context_view_unit_count,context_archived_unit_count "
                        "FROM {}.conversation_lanes WHERE bot_id=%s AND conversation_id=%s "
                        "FOR UPDATE"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchone()
            assert prior["context_view_unit_count"] == 2
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {m}.context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index,kind,"
                    "source_component_id,source_kind,source_id,payload_id,payload_digest,"
                    "payload_owner_component_id,payload_owner_kind,payload_owner_id) "
                    "SELECT %s,%s,'maintenance-control:'||g::text,%s,g-1,'CONTROL',"
                    "'conversation.scheduler','context.maintenance-fixture',"
                    "'maintenance-control:'||g::text,%s,%s,%s,%s,%s "
                    "FROM generate_series(1,67) AS g"
                ).format(m=sql.Identifier(namespace)),
                (
                    store.bot_id, "conversation/onebot/group/silent",
                    prior["context_revision"] + 1,
                    silent_content["payload_id"], silent_content["digest"],
                    silent_content["owner_component_id"], silent_content["owner_kind"],
                    silent_content["owner_id"],
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {m}.active_context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index) "
                    "SELECT bot_id,conversation_id,unit_id,context_revision,unit_index "
                    "FROM {m}.context_units WHERE bot_id=%s AND conversation_id=%s "
                    "AND context_revision=%s"
                ).format(m=sql.Identifier(namespace)),
                (store.bot_id, "conversation/onebot/group/silent",
                 prior["context_revision"] + 1),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET context_revision=context_revision+1,"
                    "context_view_unit_count=context_view_unit_count+67,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, "conversation/onebot/group/silent"),
            )
        instance.open_owned_admission()
        async with asyncio.timeout(90):
            while True:
                async with store.pool.connection() as conn:
                    maintained = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT context_applied_seq,context_view_unit_count,"
                                "context_archived_unit_count,actor_owner_id "
                                "FROM {}.conversation_lanes WHERE bot_id=%s "
                                "AND conversation_id=%s"
                            ).format(sql.Identifier(namespace)),
                            (store.bot_id, "conversation/onebot/group/silent"),
                        )
                    ).fetchone()
                if (maintained["context_view_unit_count"] == 1
                        and maintained["actor_owner_id"] is None):
                    break
                await asyncio.sleep(0.1)
        assert maintained["context_applied_seq"] == 68
        assert maintained["context_archived_unit_count"] == (
            prior["context_archived_unit_count"] + 68
        )
        async with store.pool.connection() as conn:
            maintenance_archives = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT archived_unit_count FROM {}.context_compactions "
                        "WHERE bot_id=%s AND conversation_id=%s "
                        "AND published_view_revision>%s ORDER BY published_view_revision"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent",
                     prior["context_view_revision"]),
                )
            ).fetchall()
            active_count = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS n FROM {}.active_context_units "
                        "WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "conversation/onebot/group/silent"),
                )
            ).fetchone()
        assert [row["archived_unit_count"] for row in maintenance_archives] == [64, 4]
        assert active_count["n"] == maintained["context_view_unit_count"] == 1
        # A trigger at the start of a long classified prefix must not make
        # Conversation claim all sources or triggers in one transaction.
        prefill_conversation = "conversation/onebot/system/prefill"
        digest = "sha256:" + "0" * 64
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_lanes("
                    "bot_id,conversation_id,conversation_key_digest,source_component_id,"
                    "binding_ref,platform,channel_kind,channel_ref,next_ingress_seq,"
                    "classified_through_seq,actor_owner_id,actor_epoch,actor_expires_at) "
                    "VALUES(%s,%s,%s,'adapter.onebot','binding/test','onebot-v11',"
                    "'SYSTEM','prefill',131,130,'prefill-owner',1,"
                    "clock_timestamp()+interval '1 hour')"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, prefill_conversation, digest),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.inbox_messages("
                    "bot_id,conversation_id,event_id,ingress_seq,admission_operation_id,"
                    "input_digest,source_component_id,source_event_ref,event_kind,"
                    "author_subject_ref,content_payload_id,content_payload_digest,"
                    "content_owner_component_id,content_owner_kind,content_owner_id,"
                    "author_payload_id,author_payload_digest,author_owner_component_id,"
                    "author_owner_kind,author_owner_id,context_disposition,"
                    "classification_state,trigger_decision,classification_operation_id,"
                    "classification_input_digest,classifier_component_id,"
                    "classifier_generation_id,received_at,classification_deadline,"
                    "classified_at) "
                    "SELECT %s,%s,'prefill-event/'||n,n,'prefill-admit/'||n,%s,"
                    "'adapter.onebot','prefill-source/'||n,'message.group',"
                    "'qq:'||n,'payload/content/'||n,%s,'adapter.onebot',"
                    "'onebot.normalized-event','prefill-event/'||n,"
                    "'payload/author/'||n,%s,'adapter.onebot',"
                    "'onebot.author-snapshot','prefill-event/'||n,'MATERIALIZE',"
                    "'FINAL',CASE WHEN n IN (30,130) THEN 'TRIGGER' "
                    "ELSE 'NO_TRIGGER' END,'prefill-classify/'||n,%s,"
                    "'conversation.scheduler','generation/test',clock_timestamp(),"
                    "clock_timestamp()+interval '1 hour',clock_timestamp() "
                    "FROM generate_series(1,130) AS n"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, prefill_conversation, digest, digest, digest, digest),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_triggers("
                    "bot_id,conversation_id,trigger_id,event_id,ingress_seq,"
                    "principal_ref,input_digest) "
                    "SELECT %s,%s,'prefill-trigger/'||n,'prefill-event/'||n,n,"
                    "'qq:'||n,%s FROM (VALUES (30),(130)) AS t(n)"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, prefill_conversation, digest),
            )
        prefill_actor = {
            "conversation_id": prefill_conversation,
            "owner_id": "prefill-owner",
            "epoch": 1,
        }
        prefill_endpoint = instance.runtime_service_handlers()[
            ("conversation.context.prefill", 1)
        ]
        prefill_route = FrozenServiceRoute(
            service_id="conversation.context.prefill",
            version=1,
            caller_component_id="kernel.runtime",
            provider_component_id="conversation.scheduler",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=False,
        )
        for index, expected_range in enumerate(((1, 64), (65, 128))):
            envelope = {
                "protocol": "forest.uow-step/1",
                "caller": {
                    "component_id": "conversation.scheduler",
                    "generation_id": participant.generation_id,
                },
                "input": {
                    "actor": prefill_actor,
                    "boundary": "IDLE",
                    "selection_mode": "PREFILL_ONLY",
                    "session_ref": None,
                    "run_ref": None,
                    "next_run_ref": None,
                },
                "prior": {},
            }
            if index == 0:
                prefilled = await prefill_endpoint.invoke_routed(
                    prefill_route,
                    RuntimeServiceInvocation(
                        "conversation:prefill:first", 30,
                        "test.conversation", prefill_conversation,
                    ),
                    ({
                        "protocol": "forest.conversation-context-prefill/1",
                        "request_id": "prefill-first",
                        "actor": prefill_actor,
                        "boundary": "IDLE",
                        "session_ref": None,
                        "run_ref": None,
                    },),
                    {},
                )
                replay = await prefill_endpoint.invoke_routed(
                    prefill_route,
                    RuntimeServiceInvocation(
                        "conversation:prefill:first", 30,
                        "test.conversation", prefill_conversation,
                    ),
                    ({
                        "protocol": "forest.conversation-context-prefill/1",
                        "request_id": "prefill-first",
                        "actor": prefill_actor,
                        "boundary": "IDLE",
                        "session_ref": None,
                        "run_ref": None,
                    },),
                    {},
                )
                assert replay == prefilled
            else:
                async with store.pool.connection() as conn, conn.transaction():
                    prefilled = await participant.invoke_in(
                        conn, "conversation.select-input", envelope,
                        operation_id=f"uow-step:prefill/{index}",
                        causal_root_kind="conversation.actor",
                        causal_root_id=prefill_conversation,
                        parent_call_id=f"call:prefill/{index}",
                        timeout_seconds=10,
                    )
            assert prefilled["kind"] == "CONTEXT_PREFILL"
            assert (prefilled["start_seq"], prefilled["end_seq"]) == expected_range
            assert len(prefilled["event_ids"]) == 64
            assert prefilled["trigger_claim"] is None
        envelope["input"].update(
            selection_mode="ANY", session_ref="prefill-session", run_ref="prefill-run"
        )
        async with store.pool.connection() as conn, conn.transaction():
            final = await participant.invoke_in(
                conn, "conversation.select-input", envelope,
                operation_id="uow-step:prefill/final",
                causal_root_kind="conversation.actor",
                causal_root_id=prefill_conversation,
                parent_call_id="call:prefill/final",
                timeout_seconds=10,
            )
        assert final["kind"] == "TRIGGER_INPUT"
        assert final["event_ids"] == ["prefill-event/129", "prefill-event/130"]
        assert final["trigger_claim"]["count"] == 2
        assert final["trigger_claim"]["last_trigger_ref"] == "prefill-trigger/130"
        assert final["trigger_claim"]["last_trigger_ingress_seq"] == 130
        reply_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "conversation.scheduler",
                "generation_id": participant.generation_id,
            },
            "input": {
                "actor": prefill_actor,
                "session_ref": "prefill-session",
                "run_ref": "prefill-run",
                "reply": {
                    "payload_id": "prefill-reply/payload",
                    "digest": digest,
                    "owner_component_id": "agent.runtime",
                    "owner_kind": "final.reply",
                    "owner_id": "prefill-reply",
                },
                "unit_id": "prefill-reply/unit",
            },
            "prior": {"agent": {
                "run_id": "prefill-run",
                "session_id": "prefill-session",
                "conversation_ref": prefill_conversation,
                "state": "COMPLETED",
                "final_candidate": {
                    "payload_id": "prefill-reply/payload", "digest": digest,
                },
                "learning": {
                    "protocol": "forest.agent-learning/1",
                    "proposals": [{"trigger_ref": "prefill-trigger/130"}],
                },
            }},
        }
        async def append_reply(operation_id):
            async with store.pool.connection() as conn, conn.transaction():
                row = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_append_final_reply(%s::jsonb,%s::jsonb) "
                                "AS result").format(sql.Identifier(namespace)),
                        (Jsonb({
                            "bot_id": store.bot_id,
                            "operation_id": operation_id,
                            "participant_input_digest": digest,
                        }), Jsonb(reply_envelope)),
                    )
                ).fetchone()
            return row["result"]

        async with store.pool.connection() as conn:
            claimed = await (
                await conn.execute(
                    sql.SQL("SELECT status FROM {}.conversation_triggers "
                            "WHERE bot_id=%s AND conversation_id=%s ORDER BY ingress_seq")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, prefill_conversation),
                )
            ).fetchall()
        assert [row["status"] for row in claimed] == ["CLAIMED", "CLAIMED"]
        accepted = await append_reply("prefill-reply/accepted")
        assert accepted["ok"] is True
        assert accepted["value"]["answered_trigger_count"] == 2
        assert accepted["value"]["answered_events"] == [{
            "trigger_ref": "prefill-trigger/130",
            "event_ref": "prefill-event/130",
        }]
        assert await append_reply("prefill-reply/accepted") == accepted
        async with store.pool.connection() as conn:
            answered = await (
                await conn.execute(
                    sql.SQL("SELECT status FROM {}.conversation_triggers "
                            "WHERE bot_id=%s AND conversation_id=%s ORDER BY ingress_seq")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, prefill_conversation),
                )
            ).fetchall()
        assert [row["status"] for row in answered] == ["ANSWERED", "ANSWERED"]
        drained = await prefill_endpoint.invoke_routed(
            prefill_route,
            RuntimeServiceInvocation(
                "conversation:prefill:done", 30, "test.conversation", prefill_conversation
            ),
            ({
                "protocol": "forest.conversation-context-prefill/1",
                "request_id": "prefill-done",
                "actor": prefill_actor,
                "boundary": "MODEL_SAFE_POINT",
                "session_ref": "prefill-session",
                "run_ref": "prefill-run",
            },),
            {},
        )
        assert drained["status"] == "CONTINUE"
    finally:
        assert await registry.close() == []

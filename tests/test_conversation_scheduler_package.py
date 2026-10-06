from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.errors import ForestError
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
    kernel = await bootstrap_kernel(store, installer)
    view_config = {
        "high_watermark_units": 2,
        "retain_units": 1,
        "high_watermark_payload_bytes": 524288,
        "retain_payload_bytes": 262144,
        "high_watermark_media_bytes": 12582912,
        "retain_media_bytes": 6291456,
        "high_watermark_decoded_bytes": 134217728,
        "retain_decoded_bytes": 67108864,
        "high_watermark_estimated_tokens": 24576,
        "retain_estimated_tokens": 16384,
        "utf8_bytes_per_estimated_token": 2,
        "unit_overhead_estimated_tokens": 16,
        "policy_revision": "context-view-multimodal-window-v5",
    }
    package = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/conversation_scheduler"))),
        PackageCatalog(tmp_path / "conversation-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={
            "protocol": "forest.conversation.scheduler/2",
            "context_view": view_config,
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
                "policy_revision": "qiyu-graph-region/2",
                "seed_limit": 4,
                "candidate_limit_per_facet": 64,
                "min_similarity": 0.35,
                "min_graph_relevance": 0.05,
                "max_graph_nodes": 32,
                "max_association_edges_per_node": 32,
                "max_retrieval_chars": 24000,
                "max_retrieval_media_bytes": 12582912,
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
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    # This focused fixture omits Worker Host, Agent and Delivery; the full Bot
    # Host verifies their retention edges and component lifecycles.
    omitted = {"component.worker-host", "agent.runtime", "delivery"}
    descriptors = tuple(
        replace(descriptor, retention_requires=tuple(
            component_id for component_id in descriptor.retention_requires
            if component_id not in omitted
        ))
        for descriptor in (*kernel.descriptors, memory.descriptor, package.descriptor)
    )
    registry = RuntimeModuleRegistry(host, descriptors)
    await registry.start()
    try:
        instance = registry.get("conversation.scheduler")
        participant = instance.unit_of_work_participant()
        event_id = "event/onebot/1"
        content_bytes = b'{"segments":[{"type":"text","text":"' + b"A" * 1024 + b'"}]}'
        content = await _payload(
            kernel.payload,
            owner_kind="onebot.normalized-event",
            event_id=event_id,
            content=content_bytes,
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
                "content_byte_length": len(content_bytes),
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
        wrong_length = copy.deepcopy(admission)
        wrong_length["input"]["content_byte_length"] += 1
        with pytest.raises(ForestError) as rejected_length:
            async with store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn,
                    "conversation.admit-message",
                    wrong_length,
                    operation_id="uow-step:conversation-admit/wrong-length",
                    causal_root_kind="onebot.ingress",
                    causal_root_id=event_id,
                    parent_call_id="call:ingress/wrong-length",
                    timeout_seconds=10,
                )
        assert rejected_length.value.code == "DOMAIN_PAYLOAD_METADATA_CONFLICT"
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
        async def settle_one_segment(claim_ref: str, token: str) -> dict | None:
            async with store.pool.connection() as conn, conn.transaction():
                claimed = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_claim_trigger_settlement("
                                "%s::jsonb,%s::jsonb) AS result")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id}), Jsonb({
                            "protocol": "forest.owned-work-claim-request/1",
                            "claim_token": token, "lease_seconds": 60,
                        })),
                    )
                ).fetchone()
            assert claimed["result"]["ok"] is True
            claim = claimed["result"]["value"]
            if claim is None:
                return None
            assert claim["work_id"] == claim_ref
            async with store.pool.connection() as conn, conn.transaction():
                settled = await participant.invoke_in(
                    conn, "conversation.settle-trigger-segment-record",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "conversation.scheduler",
                            "generation_id": participant.generation_id,
                        },
                        "input": claim["payload"], "prior": {},
                    },
                    operation_id=f"uow-step:trigger-settle/{token}",
                    causal_root_kind="conversation.trigger-settlement",
                    causal_root_id=claim_ref,
                    parent_call_id=f"call:trigger-settle/{token}",
                    timeout_seconds=10,
                )
            async with store.pool.connection() as conn, conn.transaction():
                acknowledged = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_settle_trigger_owned_work("
                                "%s::jsonb,%s::jsonb) AS result")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id}), Jsonb({
                            "protocol": "forest.owned-work-settle-request/1",
                            "work_id": claim_ref,
                            "claim_token": token,
                            "attempt": claim["attempt"],
                            "outcome": {"status": "SUCCEEDED", "result": settled},
                        })),
                    )
                ).fetchone()
            assert acknowledged["result"]["ok"] is True
            return settled

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
        run_view_context = {
            "bot_id": store.bot_id,
            "component_id": "conversation.scheduler",
            "port_kind": "command",
            "port_name": "conversation.run-view.bind",
        }
        original_view = dict(view_config)
        changed_view = {
            **original_view,
            "high_watermark_estimated_tokens": 4096,
            "retain_estimated_tokens": 2048,
        }
        async with store.pool.connection() as conn, conn.transaction():
            async def bind_run_view(run_ref: str, profile: dict) -> dict:
                row = await (await conn.execute(
                    sql.SQL("SELECT {}.conversation_bind_run_view_profile("
                            "%s::jsonb,%s::jsonb) AS value")
                    .format(sql.Identifier(namespace)),
                    (Jsonb(run_view_context), Jsonb({
                        "actor": actor, "session_ref": "session/1",
                        "run_ref": run_ref, "profile": profile,
                    })),
                )).fetchone()
                return row["value"]

            first_view = await bind_run_view("run/1", original_view)
            replay_view = await bind_run_view("run/1", changed_view)
            oversized_view = await bind_run_view(
                "run/1", {**original_view, "high_watermark_units": 4097,
                          "retain_units": 2048}
            )
            fenced_view = await bind_run_view("run/other", original_view)
        assert first_view["ok"] is True, first_view
        assert first_view["value"]["profile"] == original_view
        assert replay_view == first_view
        assert oversized_view["error"]["code"] == "CONTEXT_RUN_VIEW_INVALID"
        assert fenced_view["error"]["code"] == "CONTEXT_RUN_VIEW_FENCE_CHANGED"
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
        async with store.pool.connection() as conn:
            page = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_context_page(%s::jsonb,%s::jsonb) AS value")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "actor": actor, "run_ref": "run/1", "offset": 0, "limit": 32,
                    })),
                )
            ).fetchone()
            stale = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_context_page(%s::jsonb,%s::jsonb) AS value")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "actor": actor, "run_ref": "run/other", "offset": 0, "limit": 32,
                    })),
                )
            ).fetchone()
        assert page["value"]["value"]["claimed_trigger_count"] == 1
        assert page["value"]["value"]["units"][0]["required_for_run"] is True
        assert stale["value"]["error"]["code"] == "CONTEXT_SNAPSHOT_RUN_FENCE_CHANGED"

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

        class RollbackProbe(Exception):
            pass

        with pytest.raises(RollbackProbe):
            async with store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.inbox_messages SELECT "
                        "(jsonb_populate_record(NULL::{}.inbox_messages,to_jsonb(m)||"
                        "jsonb_build_object('event_id','event/forged-member',"
                        "'ingress_seq',3,'admission_operation_id','admit/forged-member',"
                        "'classification_operation_id','classify/forged-member'))).* "
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
                        "(%s,%s,'trigger/forged-member','event/forged-member',"
                        "3,'qq:forged',%s,'CLAIMED','session/1','run/1',%s)"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"],
                     "sha256:" + "0" * 64, selected["input_batch_id"]),
                )
                forged = copy.deepcopy(authority_envelope)
                forged["input"]["trigger_ref"] = "trigger/forged-member"
                forged_authority = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_authorize_run_trigger("
                                "%s::jsonb,%s::jsonb) AS result")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id}), Jsonb(forged)),
                    )
                ).fetchone()
                assert forged_authority["result"]["error"]["code"] == (
                    "AGENT_TOOL_AUTHORITY_INVALID"
                )
                raise RollbackProbe

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
            "prior": {"agent": {
                "protocol": "forest.agent-terminal-proof/1",
                "conversation_id": actor["conversation_id"],
                "session_ref": "session/1",
                "run_ref": "run/1",
                "terminal_status": "FAILED",
                "reason_code": "AGENT_PROTOCOL_FAILED",
            }},
        }
        with pytest.raises(ForestError) as wrong_terminal_proof:
            async with store.pool.connection() as conn, conn.transaction():
                await participant.invoke_in(
                    conn, "conversation.terminalize-run",
                    {**terminal, "prior": {"agent": {
                        **terminal["prior"]["agent"],
                        "run_ref": "run/another",
                    }}},
                    operation_id="uow-step:conversation-terminal/wrong-proof",
                    causal_root_kind="conversation.agent-run",
                    causal_root_id="run/1",
                    parent_call_id="call:conversation/terminal-wrong-proof",
                    timeout_seconds=10,
                )
        assert wrong_terminal_proof.value.code == "CONVERSATION_TERMINAL_INPUT_INVALID"
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
                requested = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_request_requeue("
                                "%s::jsonb,%s::jsonb) AS result")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id,
                                "operation_id": "requeue-request/terminal-probe"}),
                         Jsonb({"conversation_id": actor["conversation_id"],
                                "run_ref": "run/1", "reason_code": "TEST_RETRY",
                                "requested_by": "subject/package-admin"})),
                    )
                ).fetchone()
                assert requested["result"]["ok"] is True
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
                request_state = await (
                    await conn.execute(
                        sql.SQL("SELECT state,disposition_code FROM {}."
                                "conversation_requeue_requests WHERE bot_id=%s "
                                "AND request_id=%s")
                        .format(sql.Identifier(namespace)),
                        (store.bot_id, "requeue-request/terminal-probe"),
                    )
                ).fetchone()
                assert dict(request_state) == {
                    "state": "REJECTED", "disposition_code": "RUN_TERMINALIZED"
                }
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
                        "SELECT t.status,t.terminal_reason_code,"
                        "{}.conversation_trigger_effective_status(%s,%s) AS effective_status,"
                        "l.active_session_ref,"
                        "l.active_run_ref FROM {}.conversation_triggers t JOIN "
                        "{}.conversation_lanes l ON l.bot_id=t.bot_id AND "
                        "l.conversation_id=t.conversation_id WHERE t.bot_id=%s "
                        "AND t.trigger_id=%s"
                    ).format(*(sql.Identifier(namespace) for _ in range(3))),
                    (store.bot_id, "trigger/1", store.bot_id, "trigger/1"),
                )
            ).fetchone()
        assert dict(terminal_state) == {
            "status": "CLAIMED",
            "terminal_reason_code": None,
            "effective_status": "FAILED",
            "active_session_ref": None,
            "active_run_ref": None,
        }
        async with store.pool.connection() as conn:
            revoked = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_authorize_run_trigger("
                            "%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(authority_envelope)),
                )
            ).fetchone()
        assert revoked["result"]["error"]["code"] == "AGENT_TOOL_AUTHORITY_INVALID"
        failed_ref = selected["trigger_claim"]["ref"]
        async with store.pool.connection() as conn, conn.transaction():
            stale_claim = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_claim_trigger_settlement("
                            "%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "protocol": "forest.owned-work-claim-request/1",
                        "claim_token": "claim:stale-failed-segment", "lease_seconds": 3,
                    })),
                )
            ).fetchone()
        stale_payload = stale_claim["result"]["value"]["payload"]
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("UPDATE {}.trigger_settlement_progress SET "
                        "lease_expires_at=clock_timestamp()-interval '1 second' "
                        "WHERE bot_id=%s AND claim_ref=%s")
                .format(sql.Identifier(namespace)),
                (store.bot_id, failed_ref),
            )
            fresh_claim = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_claim_trigger_settlement("
                            "%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "protocol": "forest.owned-work-claim-request/1",
                        "claim_token": "claim:fresh-failed-segment", "lease_seconds": 60,
                    })),
                )
            ).fetchone()
            stale_result = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_settle_trigger_segment("
                            "%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "conversation.scheduler",
                            "generation_id": participant.generation_id,
                        },
                        "input": stale_payload, "prior": {},
                    })),
                )
            ).fetchone()
        assert fresh_claim["result"]["value"]["attempt"] == (
            stale_claim["result"]["value"]["attempt"]+1
        )
        assert stale_result["result"]["error"]["code"] == (
            "STALE_TRIGGER_SETTLEMENT_LEASE"
        )
        failed_segment = await settle_one_segment(
            failed_ref, "claim:fresh-failed-segment"
        )
        assert failed_segment is not None
        assert failed_segment["settled_count"] == 1
        async with store.pool.connection() as conn:
            physically_failed = await (
                await conn.execute(
                    sql.SQL("SELECT status,terminal_reason_code FROM {}.conversation_triggers "
                            "WHERE bot_id=%s AND trigger_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "trigger/1"),
                )
            ).fetchone()
        assert dict(physically_failed) == {
            "status": "FAILED", "terminal_reason_code": "AGENT_PROTOCOL_FAILED"
        }

        async with store.pool.connection() as conn, conn.transaction():
            for revision in (2, 3):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.context_units (bot_id,conversation_id,unit_id,"
                        "context_revision,unit_index,kind,source_component_id,source_kind,"
                        "source_id,payload_id,payload_digest,payload_byte_length,payload_owner_component_id,"
                        "payload_owner_kind,payload_owner_id) "
                        "SELECT bot_id,conversation_id,%s,%s,0,'CONTROL',"
                        "source_component_id,'context.control',%s,payload_id,payload_digest,"
                        "payload_byte_length,"
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
                    "context_view_unit_count=3,"
                    "context_view_payload_bytes=context_view_payload_bytes+2*%s "
                    "WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (len(content_bytes), store.bot_id, actor["conversation_id"]),
            )
        media_ref = "media:" + hashlib.sha256(b"onebot-message/1\0" + b"0").hexdigest()
        saved_media = await kernel.payload.put_bytes(
            owner_component_id="media.capture",
            owner_kind="media.asset",
            owner_id=media_ref,
            media_type="image/png",
            content=b"x" * 2048,
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        media_fact = {
            "conversation_id": actor["conversation_id"],
            "event_id": event_id,
            "ingress_seq": 1,
            "part_id": "part:image:0",
            "part_index": 0,
            "media_ref": media_ref,
            "media_revision": 1,
            "payload": {
                "payload_id": saved_media.payload_id,
                "digest": saved_media.content_digest,
                "owner_component_id": "media.capture",
                "owner_kind": "media.asset",
                "owner_id": media_ref,
            },
            "byte_length": 2048,
            "width": 1024,
            "height": 1024,
            "decoded_pixels": 1048576,
        }
        async with store.pool.connection() as conn, conn.transaction():
            fact_result = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_record_media_cost("
                            "%s::jsonb,%s::jsonb) AS value")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({
                        "bot_id": store.bot_id,
                        "component_id": "conversation.scheduler",
                        "port_kind": "command",
                        "port_name": "conversation.media-cost.write",
                    }), Jsonb(media_fact)),
                )
            ).fetchone()
        assert fact_result["value"]["value"] == {"recorded": True}
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
                "high_watermark_units": 512,
                "retain_units": 256,
                "high_watermark_payload_bytes": 1024,
                "retain_payload_bytes": 512,
                "high_watermark_media_bytes": 12582912,
                "retain_media_bytes": 6291456,
                "high_watermark_decoded_bytes": 134217728,
                "retain_decoded_bytes": 67108864,
                "high_watermark_estimated_tokens": 24576,
                "retain_estimated_tokens": 16384,
                "utf8_bytes_per_estimated_token": 2,
                "unit_overhead_estimated_tokens": 16,
                "policy_revision": "context-view-multimodal-window-v5",
            },
            "payload": {
                "protocol": "forest.payload-json-from-prior/1",
                "prior_step_id": "prepare",
            },
            "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
            "memory": {},
        }
        media_only_input = copy.deepcopy(pre_run_input)
        media_only_input["context_view"].update({
            "high_watermark_payload_bytes": 524288,
            "retain_payload_bytes": 262144,
            "high_watermark_media_bytes": 1024,
            "retain_media_bytes": 512,
        })
        token_only_input = copy.deepcopy(pre_run_input)
        token_only_input["context_view"].update({
            "high_watermark_payload_bytes": 524288,
            "retain_payload_bytes": 262144,
            "high_watermark_estimated_tokens": 1024,
            "retain_estimated_tokens": 512,
        })
        with pytest.raises(RollbackProbe):
            async with store.pool.connection() as conn, conn.transaction():
                token_plan = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_prepare_context_compaction("
                                "%s::jsonb,%s::jsonb) AS result")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({
                            "bot_id": store.bot_id,
                            "operation_id": "conversation/pre-run/token-only",
                            "participant_input_digest": "sha256:" + "0" * 64,
                        }), Jsonb({
                            "protocol": "forest.uow-step/1",
                            "input": token_only_input,
                            "prior": {},
                        })),
                    )
                ).fetchone()
                assert token_plan["result"]["ok"] is True
                assert [unit["context_revision"] for unit in
                        token_plan["result"]["value"]["commit"]["units"]] == [1, 2]
                raise RollbackProbe
        with pytest.raises(RollbackProbe):
            async with store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL("UPDATE {}.conversation_lanes SET active_session_ref=%s,"
                            "active_run_ref=%s WHERE bot_id=%s AND conversation_id=%s")
                    .format(sql.Identifier(namespace)),
                    ("session/handoff", "run/handoff", store.bot_id,
                     actor["conversation_id"]),
                )

                async def prepare(anchor, prior, operation_id):
                    return (await (await conn.execute(
                        sql.SQL("SELECT {}.conversation_prepare_context_compaction("
                                "%s::jsonb,%s::jsonb) AS result")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({
                            "bot_id": store.bot_id,
                            "operation_id": operation_id,
                            "participant_input_digest": "sha256:" + "0" * 64,
                        }), Jsonb({
                            "protocol": "forest.uow-step/1",
                            "input": {**pre_run_input, "anchor": anchor},
                            "prior": prior,
                        })),
                    )).fetchone())["result"]

                assert (await prepare(
                    pre_run_input["anchor"], {}, "conversation/pre-run/active"
                ))["value"]["status"] == "SKIP"
                anchor = {
                    **pre_run_input["anchor"],
                    "protocol": "forest.conversation-handoff-compaction-anchor/1",
                    "session_ref": "session/handoff", "run_ref": "run/handoff",
                }
                agent_proof = {
                    "protocol": "forest.agent-completed-compaction/1",
                    "conversation_id": actor["conversation_id"],
                    "session_ref": "session/handoff", "run_ref": "run/handoff",
                }
                handoff_plan = await prepare(
                    anchor, {"agent": agent_proof}, "conversation/handoff/1"
                )
                assert handoff_plan["ok"] is True
                assert [unit["context_revision"] for unit in
                        handoff_plan["value"]["commit"]["units"]] == [1, 2]
                stale_proof = await prepare(
                    anchor, {"agent": {**agent_proof, "run_ref": "run/other"}},
                    "conversation/handoff/stale-proof",
                )
                assert stale_proof["error"]["code"] == (
                    "CONTEXT_COMPACTION_INPUT_INVALID"
                )
                compaction_id = handoff_plan["value"]["commit"]["compaction_id"]
                archived_payload = await kernel.payload.put_bytes_in(
                    conn,
                    owner_component_id="conversation.scheduler",
                    owner_kind="context.archive",
                    owner_id=compaction_id,
                    media_type="application/json",
                    content=b'{"protocol":"forest.context-archive-reference/1"}',
                    expected_deletion_epoch=await kernel.payload.epoch_in(conn),
                )
                committed_handoff = await (await conn.execute(
                    sql.SQL("SELECT {}.conversation_commit_context_compaction("
                            "%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "protocol": "forest.uow-step/1",
                        "input": {**pre_run_input, "anchor": anchor},
                        "prior": {
                            "agent": agent_proof,
                            "prepare": handoff_plan["value"],
                            "payload": {
                                "protocol": "forest.payload-json-put/1",
                                "status": "WRITTEN",
                                "payload": {
                                    "payload_id": archived_payload.payload_id,
                                    "digest": archived_payload.content_digest,
                                    "owner_component_id": "conversation.scheduler",
                                    "owner_kind": "context.archive",
                                    "owner_id": compaction_id,
                                },
                            },
                        },
                    })),
                )).fetchone()
                assert committed_handoff["result"]["ok"] is True
                assert committed_handoff["result"]["value"]["status"] == "COMPACTED"
                active = await (await conn.execute(
                    sql.SQL("SELECT context_view_unit_count,context_archived_unit_count "
                            "FROM {}.conversation_lanes WHERE bot_id=%s "
                            "AND conversation_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"]),
                )).fetchone()
                assert active == {
                    "context_view_unit_count": 1,
                    "context_archived_unit_count": 2,
                }
                raise RollbackProbe
        async with store.pool.connection() as conn, conn.transaction():
            media_only_plan = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_prepare_context_compaction("
                            "%s::jsonb,%s::jsonb) AS value")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({
                        "bot_id": store.bot_id,
                        "operation_id": "conversation/pre-run/media-only",
                        "participant_input_digest": "sha256:" + "0" * 64,
                    }), Jsonb({
                        "protocol": "forest.uow-step/1",
                        "input": media_only_input,
                        "prior": {},
                    })),
                )
            ).fetchone()
        assert media_only_plan["value"]["value"]["commit"]["archived_unit_count"] == 1
        assert media_only_plan["value"]["value"]["commit"]["archived_media_bytes"] == 2048
        assert media_only_plan["value"]["value"]["commit"]["archived_decoded_bytes"] == 4194304
        decoded_only_input = copy.deepcopy(pre_run_input)
        decoded_only_input["context_view"].update({
            "high_watermark_payload_bytes": 524288,
            "retain_payload_bytes": 262144,
            "high_watermark_decoded_bytes": 1024,
            "retain_decoded_bytes": 512,
        })
        async with store.pool.connection() as conn, conn.transaction():
            decoded_only_plan = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_prepare_context_compaction("
                            "%s::jsonb,%s::jsonb) AS value")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({
                        "bot_id": store.bot_id,
                        "operation_id": "conversation/pre-run/decoded-only",
                        "participant_input_digest": "sha256:" + "0" * 64,
                    }), Jsonb({
                        "protocol": "forest.uow-step/1",
                        "input": decoded_only_input,
                        "prior": {},
                    })),
                )
            ).fetchone()
        assert decoded_only_plan["value"]["value"]["commit"]["archived_decoded_bytes"] == 4194304
        # An unresolved trigger in the bounded oldest window pins the archive
        # prefix.  Later eligible units must stay in the active view.
        with pytest.raises(RollbackProbe):
            async with store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {0}.inbox_messages SELECT "
                        "(jsonb_populate_record(NULL::{0}.inbox_messages,"
                        "to_jsonb(i)||jsonb_build_object('event_id','event/blocked-prefix',"
                        "'ingress_seq',2,'admission_operation_id','admit/blocked-prefix',"
                        "'source_event_ref','onebot-message/blocked-prefix',"
                        "'platform_message_ref','message/blocked-prefix'))).* "
                        "FROM {0}.inbox_messages i WHERE i.bot_id=%s AND i.event_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, event_id),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.conversation_triggers("
                        "bot_id,conversation_id,trigger_id,event_id,ingress_seq,"
                        "principal_ref,input_digest) SELECT bot_id,conversation_id,"
                        "'trigger/blocked-prefix',event_id,ingress_seq,"
                        "author_subject_ref,input_digest FROM {}.inbox_messages "
                        "WHERE bot_id=%s AND event_id='event/blocked-prefix'"
                    ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                    (store.bot_id,),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {0}.context_units SELECT "
                        "(jsonb_populate_record(NULL::{0}.context_units,"
                        "to_jsonb(u)||jsonb_build_object('unit_id','input/blocked-prefix',"
                        "'context_revision',4,'event_id','event/blocked-prefix',"
                        "'ingress_seq',2))).* FROM {0}.context_units u "
                        "WHERE u.bot_id=%s AND u.event_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, event_id),
                )
                for revision in (5, 6):
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {0}.context_units SELECT "
                            "(jsonb_populate_record(NULL::{0}.context_units,"
                            "to_jsonb(u)||jsonb_build_object('unit_id',%s::text,"
                            "'context_revision',%s::bigint,'source_id',%s::text))).* "
                            "FROM {0}.context_units u WHERE u.bot_id=%s "
                            "AND u.unit_id='control/pre-run/3'"
                        ).format(sql.Identifier(namespace)),
                        (f"control/after-block/{revision}", revision,
                         f"control/after-block/{revision}", store.bot_id),
                    )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {0}.active_context_units("
                        "bot_id,conversation_id,unit_id,context_revision,unit_index) "
                        "SELECT bot_id,conversation_id,unit_id,context_revision,unit_index "
                        "FROM {0}.context_units WHERE bot_id=%s "
                        "AND conversation_id=%s AND context_revision IN (4,5,6)"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"]),
                )
                await conn.execute(
                    sql.SQL(
                        "UPDATE {}.conversation_lanes SET context_revision=6,"
                        "context_view_unit_count=6,"
                        "context_view_payload_bytes=context_view_payload_bytes+3*%s "
                        "WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (len(content_bytes), store.bot_id, actor["conversation_id"]),
                )
                blocked_plan = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.conversation_prepare_context_compaction("
                            "%s::jsonb,%s::jsonb) AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "conversation/pre-run/blocked-prefix",
                                "participant_input_digest": "sha256:" + "0" * 64,
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "input": pre_run_input,
                                "prior": {},
                            }),
                        ),
                    )
                ).fetchone()
                assert blocked_plan["result"]["ok"] is True
                assert [unit["context_revision"] for unit in
                        blocked_plan["result"]["value"]["commit"]["units"]] == [1, 2, 3]
                raise RollbackProbe
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
                        "context_view_payload_bytes,context_view_media_bytes,"
                        "context_archived_unit_count "
                        "FROM {}.conversation_lanes "
                        "WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, actor["conversation_id"]),
                )
            ).fetchone()
        assert view == {
            "context_view_revision": 1,
            "context_view_unit_count": 1,
            "context_view_payload_bytes": len(content_bytes),
            "context_view_media_bytes": 0,
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
                "content_byte_length": len(b'{"segments":[{"type":"text","text":"ordinary"}]}'),
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
            next_content_bytes = (
                json.dumps({"parts": [{
                    "type": "image",
                    "media_ref": "media:" + hashlib.sha256(
                        f"onebot-message/silent-{index}\0".encode() + b"0"
                    ).hexdigest(),
                    "media_revision": 1,
                    "state": "PENDING_CAPTURE",
                }]}, separators=(",", ":")).encode()
                if index == 3
                else (f'{{"segments":[{{"type":"text","text":"ordinary-{index}"}}]}}').encode()
            )
            next_content = await _payload(
                kernel.payload,
                owner_kind="onebot.normalized-event",
                event_id=next_event_id,
                content=next_content_bytes,
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
                    "content_byte_length": len(next_content_bytes),
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
            if index == 2:
                early_media_ref = "media:" + hashlib.sha256(
                    b"onebot-message/silent-2\0" + b"0"
                ).hexdigest()
                early_media = await kernel.payload.put_bytes(
                    owner_component_id="media.capture",
                    owner_kind="media.asset",
                    owner_id=early_media_ref,
                    media_type="image/png",
                    content=b"x" * 2048,
                    expected_deletion_epoch=await kernel.payload.epoch(),
                )
                async with store.pool.connection() as conn, conn.transaction():
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_record_media_cost("
                                "%s::jsonb,%s::jsonb)")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({
                            "bot_id": store.bot_id,
                            "component_id": "conversation.scheduler",
                            "port_kind": "command",
                            "port_name": "conversation.media-cost.write",
                        }), Jsonb({
                            "conversation_id": "conversation/onebot/group/silent",
                            "event_id": next_event_id,
                            "ingress_seq": 2,
                            "part_id": "part:image:0",
                            "part_index": 0,
                            "media_ref": early_media_ref,
                            "media_revision": 1,
                            "payload": {
                                "payload_id": early_media.payload_id,
                                "digest": early_media.content_digest,
                                "owner_component_id": "media.capture",
                                "owner_kind": "media.asset",
                                "owner_id": early_media_ref,
                            },
                            "byte_length": 2048,
                            "width": 1,
                            "height": 1,
                            "decoded_pixels": 1,
                        })),
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
            if index == 2:
                for _attempt in range(100):
                    async with store.pool.connection() as conn:
                        early_lane = await (
                            await conn.execute(
                                sql.SQL("SELECT context_applied_seq,"
                                        "context_view_media_bytes,"
                                        "context_view_decoded_bytes FROM {}.conversation_lanes "
                                        "WHERE bot_id=%s AND conversation_id=%s")
                                .format(sql.Identifier(namespace)),
                                (store.bot_id, "conversation/onebot/group/silent"),
                            )
                        ).fetchone()
                    if early_lane["context_applied_seq"] == 2:
                        break
                    await asyncio.sleep(0.05)
                assert early_lane["context_view_media_bytes"] == 2048
                assert early_lane["context_view_decoded_bytes"] == 4

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
        # A late image for an archived input is recorded without charging the
        # active window.  The active input's cost is charged once on replay.
        for index, expected_active_bytes in ((1, 0), (3, 2048)):
            raw_event = f"onebot-message/silent-{index}"
            media_ref = "media:" + hashlib.sha256(
                (raw_event + "\0" + "0").encode()
            ).hexdigest()
            saved = await kernel.payload.put_bytes(
                owner_component_id="media.capture",
                owner_kind="media.asset",
                owner_id=media_ref,
                media_type="image/png",
                content=b"x" * 2048,
                expected_deletion_epoch=await kernel.payload.epoch(),
            )
            fact = {
                "conversation_id": "conversation/onebot/group/silent",
                "event_id": f"event/onebot/silent-{index}",
                "ingress_seq": index,
                "part_id": "part:image:0",
                "part_index": 0,
                "media_ref": media_ref,
                "media_revision": 1,
                "payload": {
                    "payload_id": saved.payload_id,
                    "digest": saved.content_digest,
                    "owner_component_id": "media.capture",
                    "owner_kind": "media.asset",
                    "owner_id": media_ref,
                },
                "byte_length": 2048,
                "width": 1,
                "height": 1,
                "decoded_pixels": 1,
            }
            command_context = {
                "bot_id": store.bot_id,
                "component_id": "conversation.scheduler",
                "port_kind": "command",
                "port_name": "conversation.media-cost.write",
            }
            async with store.pool.connection() as conn, conn.transaction():
                for _ in range(2):
                    recorded = await (
                        await conn.execute(
                            sql.SQL("SELECT {}.conversation_record_media_cost("
                                    "%s::jsonb,%s::jsonb) AS value")
                            .format(sql.Identifier(namespace)),
                            (Jsonb(command_context), Jsonb(fact)),
                        )
                    ).fetchone()
                    assert recorded["value"]["value"] == {"recorded": True}
                lane_cost = await (
                    await conn.execute(
                        sql.SQL("SELECT context_view_media_bytes,"
                                "context_view_decoded_bytes,media_cost_revision "
                                "FROM {}.conversation_lanes WHERE bot_id=%s "
                                "AND conversation_id=%s")
                        .format(sql.Identifier(namespace)),
                        (store.bot_id, fact["conversation_id"]),
                    )
                ).fetchone()
            assert lane_cost["context_view_media_bytes"] == expected_active_bytes
            assert lane_cost["context_view_decoded_bytes"] == (4 if index == 3 else 0)
            assert lane_cost["media_cost_revision"] == index // 2 + 2
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
        endpoint = instance.runtime_service_handlers()[("conversation.context.snapshot", 7)]
        route = FrozenServiceRoute(
            service_id="conversation.context.snapshot",
            version=7,
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
        assert snapshot["protocol"] == "forest.conversation-context-snapshot/7"
        assert snapshot["run_ref"] is None
        assert snapshot["view_profile_digest"] is None
        assert snapshot["claimed_trigger_count"] == 0
        assert snapshot["context_view_media_bytes"] == 2048
        assert snapshot["context_view_decoded_bytes"] == 4
        assert snapshot["context_view_revision"] == 1
        assert snapshot["unit_count"] == 2
        manifest = json.loads(
            await kernel.payload.read_bytes(snapshot["snapshot"]["payload_id"])
        )
        assert manifest["archive"]["compaction_id"] == archive["compaction_id"]
        assert manifest["required_trigger_unit_ids"] == []
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
        assert page["units"][1]["media_parts"][0]["byte_length"] == 2048
        estimator = snapshot["token_estimator"]
        assert manifest["token_estimator"] == estimator
        assert estimator["revision"] == "utf8-payload-plus-unit-v1"
        assert estimator["estimated_history_tokens"] == (
            (page["units"][1]["payload_byte_length"] + 1) // 2 + 16
        )

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
                    "content_byte_length": len(b'{"segments":[{"type":"text","text":"backlog"}]}'),
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
                    "payload_byte_length,"
                    "payload_owner_component_id,payload_owner_kind,payload_owner_id) "
                    "SELECT %s,%s,'maintenance-control:'||g::text,%s,g-1,'CONTROL',"
                    "'conversation.scheduler','context.maintenance-fixture',"
                    "'maintenance-control:'||g::text,%s,%s,1,%s,%s,%s "
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
                    "context_view_payload_bytes=context_view_payload_bytes+67,"
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
                    "content_byte_length,"
                    "content_owner_component_id,content_owner_kind,content_owner_id,"
                    "author_payload_id,author_payload_digest,author_owner_component_id,"
                    "author_owner_kind,author_owner_id,context_disposition,"
                    "classification_state,trigger_decision,classification_operation_id,"
                    "classification_input_digest,classifier_component_id,"
                    "classifier_generation_id,received_at,classification_deadline,"
                    "classified_at) "
                    "SELECT %s,%s,'prefill-event/'||n,n,'prefill-admit/'||n,%s,"
                    "'adapter.onebot','prefill-source/'||n,'message.group',"
                    "'qq:'||n,'payload/content/'||n,%s,1,'adapter.onebot',"
                    "'onebot.normalized-event','prefill-event/'||n,"
                    "'payload/author/'||n,%s,'adapter.onebot',"
                    "'onebot.author-snapshot','prefill-event/'||n,'MATERIALIZE',"
                    "'FINAL','TRIGGER','prefill-classify/'||n,%s,"
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
                    "'qq:'||n,%s FROM generate_series(1,130) AS n"
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
                    "session_ref": "prefill-session",
                    "run_ref": "prefill-run",
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
                        "session_ref": "prefill-session",
                        "run_ref": "prefill-run",
                        "next_run_ref": None,
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
                        "session_ref": "prefill-session",
                        "run_ref": "prefill-run",
                        "next_run_ref": None,
                    },),
                    {},
                )
                assert replay == prefilled
                wrong_selection = {
                    **envelope,
                    "input": {
                        **envelope["input"],
                        "session_ref": "unrelated-session",
                        "run_ref": "unrelated-run",
                    },
                }
                async with store.pool.connection() as conn, conn.transaction():
                    rejected = await (
                        await conn.execute(
                            sql.SQL("SELECT {}.conversation_select_input(%s::jsonb,%s::jsonb) "
                                    "AS result").format(sql.Identifier(namespace)),
                            (Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "prefill-wrong-target",
                                "participant_input_digest": digest,
                            }), Jsonb(wrong_selection)),
                        )
                    ).fetchone()
                assert rejected["result"]["error"]["code"] == (
                    "CONVERSATION_PREFILL_TARGET_CONFLICT"
                )
                async with store.pool.connection() as conn, conn.transaction():
                    await conn.execute(
                        sql.SQL("UPDATE {}.conversation_lanes SET channel_kind='GROUP',"
                                "actor_expires_at=clock_timestamp()-interval '1 second' "
                                "WHERE bot_id=%s AND conversation_id=%s")
                        .format(sql.Identifier(namespace)),
                        (store.bot_id, prefill_conversation),
                    )
                    reclaimed = await (
                        await conn.execute(
                            sql.SQL("SELECT {}.conversation_claim_agent_run(%s::jsonb,%s::jsonb) "
                                    "AS result").format(sql.Identifier(namespace)),
                            (Jsonb({"bot_id": store.bot_id}), Jsonb({
                                "protocol": "forest.owned-work-claim-request/1",
                                "claim_token": "prefill-recovered-owner",
                                "lease_seconds": 3600,
                            })),
                        )
                    ).fetchone()
                assert reclaimed["result"]["ok"] is True
                recovered = reclaimed["result"]["value"]["payload"]
                assert recovered["session_ref"] == "prefill-session"
                assert recovered["run_ref"] == "prefill-run"
                assert recovered["resume"] is False
                prefill_actor.update(owner_id="prefill-recovered-owner", epoch=2)
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
            if index == 0:
                assert prefilled["trigger_claim"]["count"] == 64
                assert prefilled["trigger_claim"]["last_trigger_ref"] == "prefill-trigger/64"
                claim_ref = prefilled["trigger_claim"]["ref"]
                class _RollbackOrphan(Exception):
                    pass

                with pytest.raises(_RollbackOrphan):
                    async with store.pool.connection() as conn, conn.transaction():
                        async def orphan_call(function, context, body):
                            row = await (
                                await conn.execute(
                                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result")
                                    .format(sql.Identifier(namespace), sql.Identifier(function)),
                                    (Jsonb(context), Jsonb(body)),
                                )
                            ).fetchone()
                            return row["result"]

                        candidates = await orphan_call(
                            "conversation_list_requeue_candidates",
                            {"bot_id": store.bot_id},
                            {"conversation_id": prefill_conversation, "limit": 1},
                        )
                        assert candidates["value"]["items"][0]["run_ref"] == "prefill-run"
                        requested = await orphan_call(
                            "conversation_request_requeue",
                            {"bot_id": store.bot_id,
                             "operation_id": "prefill-orphan-request"},
                            {"conversation_id": prefill_conversation,
                             "run_ref": "prefill-run",
                             "reason_code": "TEST_PREFILL_ABANDONED",
                             "requested_by": "subject/package-admin"},
                        )
                        assert requested["ok"] is True
                        requeued = await orphan_call(
                            "conversation_requeue_run",
                            {"bot_id": store.bot_id,
                             "operation_id": "prefill-orphan-requeue",
                             "participant_input_digest": digest},
                            {"protocol": "forest.uow-step/1",
                             "caller": {"component_id": "conversation.scheduler",
                                        "generation_id": participant.generation_id},
                             "input": {
                                 "protocol": "forest.conversation-run-requeue/1",
                                 "actor": prefill_actor,
                                 "request_id": "prefill-orphan-request",
                                 "session_ref": "prefill-session",
                                 "run_ref": "prefill-run",
                                 "claim_ref": claim_ref,
                                 "claim_digest": prefilled["trigger_claim"]["digest"],
                                 "reason_code": "TEST_PREFILL_ABANDONED",
                             }, "prior": {}},
                        )
                        assert requeued["ok"] is True
                        assert requeued["value"]["trigger_count"] == 64
                        assert (await orphan_call(
                            "conversation_read_requeue_request",
                            {"bot_id": store.bot_id},
                            {"conversation_id": prefill_conversation,
                             "run_ref": "prefill-run"},
                        ))["value"]["state"] == "APPLIED"
                        raise _RollbackOrphan
                with pytest.raises(_RollbackOrphan):
                    async with store.pool.connection() as conn, conn.transaction():
                        await conn.execute(
                            sql.SQL("INSERT INTO {}.trigger_claim_groups("
                                    "bot_id,conversation_id,claim_ref,session_ref,run_ref,"
                                    "trigger_count,segment_count,last_trigger_ref,"
                                    "last_trigger_ingress_seq,claim_digest) "
                                    "VALUES (%s,%s,%s,%s,%s,1,1,%s,1,%s)")
                            .format(sql.Identifier(namespace)),
                            (store.bot_id, prefill_conversation, "claim:" + "f" * 64,
                             "orphan-session", "orphan-run", "orphan-trigger",
                             "sha256:" + "f" * 64),
                        )

                        async def anomaly_page(after=None):
                            payload = {"conversation_id": prefill_conversation, "limit": 1}
                            if after is not None:
                                payload.update(after_conversation_id=after["conversation_id"],
                                               after_claim_ref=after["claim_ref"])
                            row = await (
                                await conn.execute(
                                    sql.SQL("SELECT {}.conversation_list_claim_anomalies("
                                            "%s::jsonb,%s::jsonb) AS result")
                                    .format(sql.Identifier(namespace)),
                                    (Jsonb({"bot_id": store.bot_id}), Jsonb(payload)),
                                )
                            ).fetchone()
                            return row["result"]["value"]

                        first_page = await anomaly_page()
                        assert first_page["has_more"] is True
                        second_page = await anomaly_page(first_page["next_cursor"])
                        assert second_page["has_more"] is False
                        assert {item["claim_ref"] for item in
                                first_page["items"] + second_page["items"]} == {
                            claim_ref, "claim:" + "f" * 64,
                        }
                        assert all(item["anomaly_code"] == "MULTIPLE_ACTIVE_GROUPS"
                                   for item in first_page["items"] + second_page["items"])
                        await conn.execute(
                            sql.SQL("UPDATE {}.conversation_lanes SET "
                                    "active_session_ref=%s WHERE bot_id=%s "
                                    "AND conversation_id=%s")
                            .format(sql.Identifier(namespace)),
                            ("unrelated-session", store.bot_id, prefill_conversation),
                        )
                        mismatched = await (
                            await conn.execute(
                                sql.SQL("SELECT {}.conversation_list_claim_anomalies("
                                        "%s::jsonb,%s::jsonb) AS result")
                                .format(sql.Identifier(namespace)),
                                (Jsonb({"bot_id": store.bot_id}),
                                 Jsonb({"conversation_id": prefill_conversation,
                                        "limit": 20})),
                            )
                        ).fetchone()
                        assert len(mismatched["result"]["value"]["items"]) == 2
                        assert all(item["anomaly_code"] == "LANE_CLAIM_MISMATCH"
                                   for item in mismatched["result"]["value"]["items"])
                        raise _RollbackOrphan
            else:
                assert prefilled["trigger_claim"]["count"] == 128
                assert prefilled["trigger_claim"]["last_trigger_ref"] == "prefill-trigger/128"
                assert prefilled["trigger_claim"]["ref"] == claim_ref
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
        assert final["trigger_claim"]["count"] == 130
        assert final["trigger_claim"]["ref"] == claim_ref
        assert final["trigger_claim"]["last_trigger_ref"] == "prefill-trigger/130"
        assert final["trigger_claim"]["last_trigger_ingress_seq"] == 130
        async with store.pool.connection() as conn:
            pages = []
            for offset in range(0, 130, 32):
                row = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_context_page(%s::jsonb,%s::jsonb) AS value")
                        .format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id}), Jsonb({
                            "actor": prefill_actor, "run_ref": "prefill-run",
                            "offset": offset, "limit": 32,
                        })),
                    )
                ).fetchone()
                assert row["value"]["ok"] is True
                pages.append(row["value"]["value"])
        assert all(page["claimed_trigger_count"] == 130 for page in pages)
        assert sum(unit["required_for_run"] for page in pages for unit in page["units"]) == 130
        async with store.pool.connection() as conn:
            segments = await (
                await conn.execute(
                    sql.SQL("SELECT segment_index,trigger_count FROM {}.trigger_claim_segments "
                            "WHERE bot_id=%s AND claim_ref=%s ORDER BY segment_index")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, claim_ref),
                )
            ).fetchall()
        assert [(row["segment_index"], row["trigger_count"]) for row in segments] == [
            (0, 64), (1, 64), (2, 2)
        ]
        async with store.pool.connection() as conn:
            candidates = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_list_requeue_candidates("
                            "%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}),
                     Jsonb({"conversation_id": prefill_conversation, "limit": 1})),
                )
            ).fetchone()
        assert candidates["result"]["value"]["items"][0]["run_ref"] == "prefill-run"
        assert candidates["result"]["value"]["has_more"] is False
        healthy = await instance.management_contract.query(
            "conversation.claim.anomalies",
            {"conversation_id": prefill_conversation, "limit": 20},
        )
        assert healthy["items"] == []
        authorization = SimpleNamespace(
            actor_ref="subject/package-admin",
            web_subject_ref="subject/package-admin",
            target_generation_id=instance.generation_identity.generation_id,
            target_graph_revision=instance.generation_identity.graph_revision,
        )

        async def accept_requeue_ticket(_conn, *, ticket, **_kwargs):
            assert ticket is authorization

        action_payload = {
            "conversation_id": prefill_conversation,
            "run_ref": "prefill-run",
            "reason_code": "TEST_ABANDONED_RUN",
        }
        async def request_managed_requeue():
            return await instance.management_contract.action(
                "conversation.requeue.request",
                operation_id="requeue-request/managed",
                payload=action_payload,
                authorization=authorization,
                authorization_guard=accept_requeue_ticket,
            )

        requested, concurrent = await asyncio.gather(
            request_managed_requeue(), request_managed_requeue()
        )
        assert requested == {
            "protocol": "forest.conversation-requeue-requested/1",
            "request_id": "requeue-request/managed",
        }
        assert concurrent == requested
        assert await request_managed_requeue() == requested
        managed_state = await instance.management_contract.query(
            "conversation.requeue.status",
            {"conversation_id": prefill_conversation, "run_ref": "prefill-run"},
        )
        assert managed_state["state"] == "PENDING"
        assert managed_state["requested_by"] == "subject/package-admin"
        # An abandoned Run may return an already committed Context prefix to
        # PENDING. Its old claim settles in bounded pieces, and the new Run
        # claims that same prefix without duplicating Context units.
        class _RollbackRequeue(Exception):
            pass

        with pytest.raises(_RollbackRequeue):
            async with store.pool.connection() as conn, conn.transaction():
                async def call(function, context, body):
                    row = await (
                        await conn.execute(
                            sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result")
                            .format(sql.Identifier(namespace), sql.Identifier(function)),
                            (Jsonb(context), Jsonb(body)),
                        )
                    ).fetchone()
                    return row["result"]

                requeue_context = {
                    "bot_id": store.bot_id,
                    "operation_id": "prefill-requeue/one",
                    "participant_input_digest": digest,
                }
                requeue_envelope = {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": participant.generation_id,
                    },
                    "input": {
                        "protocol": "forest.conversation-run-requeue/1",
                        "actor": prefill_actor,
                        "request_id": "requeue-request/managed",
                        "session_ref": "prefill-session",
                        "run_ref": "prefill-run",
                        "claim_ref": claim_ref,
                        "claim_digest": final["trigger_claim"]["digest"],
                        "reason_code": "TEST_ABANDONED_RUN",
                    },
                    "prior": {},
                }
                requeued = await call(
                    "conversation_requeue_run", requeue_context, requeue_envelope
                )
                assert requeued["ok"] is True
                assert requeued["value"]["trigger_count"] == 130
                requeue_status = await call(
                    "conversation_read_requeue_request",
                    {"bot_id": store.bot_id},
                    {"conversation_id": prefill_conversation,
                     "run_ref": "prefill-run"},
                )
                assert requeue_status["value"]["state"] == "APPLIED"
                assert await call(
                    "conversation_requeue_run", requeue_context, requeue_envelope
                ) == requeued
                effective = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.conversation_trigger_effective_status(%s,%s) "
                                "AS status").format(sql.Identifier(namespace)),
                        (store.bot_id, "prefill-trigger/1"),
                    )
                ).fetchone()
                assert effective["status"] == "PENDING"
                blocked = await call(
                    "conversation_select_input",
                    {"bot_id": store.bot_id, "operation_id": "requeue-before-settlement",
                     "participant_input_digest": digest},
                    {"protocol": "forest.uow-step/1", "input": {
                        "actor": prefill_actor, "boundary": "IDLE",
                        "selection_mode": "PREFILL_ONLY",
                        "session_ref": "replacement-session",
                        "run_ref": "replacement-run", "next_run_ref": None,
                    }},
                )
                assert blocked["error"]["code"] == "CONVERSATION_REQUEUE_SETTLEMENT_PENDING"
                for index, expected in enumerate((64, 64, 2)):
                    token = f"requeue-settlement/{index}"
                    claimed = await call(
                        "conversation_claim_trigger_settlement",
                        {"bot_id": store.bot_id},
                        {"protocol": "forest.owned-work-claim-request/1",
                         "claim_token": token, "lease_seconds": 30},
                    )
                    assert claimed["ok"] is True
                    payload = claimed["value"]["payload"]
                    assert payload["segment_index"] == index
                    settled = await call(
                        "conversation_settle_trigger_segment",
                        {"bot_id": store.bot_id},
                        {"protocol": "forest.uow-step/1",
                         "caller": {"component_id": "conversation.scheduler"},
                         "input": payload},
                    )
                    assert settled["ok"] is True
                    assert settled["value"]["settled_count"] == expected
                status_row = await (
                    await conn.execute(
                        sql.SQL("SELECT count(*) FILTER (WHERE status='PENDING') AS pending,"
                                "count(*) AS total FROM {}.conversation_triggers "
                                "WHERE bot_id=%s AND conversation_id=%s")
                        .format(sql.Identifier(namespace)),
                        (store.bot_id, prefill_conversation),
                    )
                ).fetchone()
                assert (status_row["pending"], status_row["total"]) == (130, 130)
                for index, expected_count in enumerate((64, 128)):
                    selected = await call(
                        "conversation_select_input",
                        {"bot_id": store.bot_id,
                         "operation_id": f"recovery-prefill/{index}",
                         "participant_input_digest": digest},
                        {"protocol": "forest.uow-step/1", "input": {
                            "actor": prefill_actor, "boundary": "IDLE",
                            "selection_mode": "PREFILL_ONLY",
                            "session_ref": "replacement-session",
                            "run_ref": "replacement-run", "next_run_ref": None,
                        }},
                    )
                    assert selected["ok"] is True
                    assert selected["value"]["kind"] == "RECOVERY_PREFILL"
                    assert selected["value"]["event_ids"] == []
                    assert selected["value"]["trigger_claim"]["count"] == expected_count
                recovered = await call(
                    "conversation_select_input",
                    {"bot_id": store.bot_id, "operation_id": "recovery-final",
                     "participant_input_digest": digest},
                    {"protocol": "forest.uow-step/1", "input": {
                        "actor": prefill_actor, "boundary": "IDLE",
                        "selection_mode": "ANY",
                        "session_ref": "replacement-session",
                        "run_ref": "replacement-run", "next_run_ref": None,
                    }},
                )
                assert recovered["ok"] is True
                assert recovered["value"]["kind"] == "RECOVERY_CLAIM"
                assert recovered["value"]["trigger_claim"]["count"] == 130
                raise _RollbackRequeue
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
                "reply_byte_length": 1,
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
        assert len(claimed) == 130
        assert {row["status"] for row in claimed} == {"CLAIMED"}
        async with store.pool.connection() as conn:
            with pytest.raises(Exception, match="Illegal trigger transition"):
                async with conn.transaction():
                    await conn.execute(
                        sql.SQL("UPDATE {}.conversation_triggers SET status='PENDING', "
                                "claimed_session_ref=NULL,claimed_run_ref=NULL,"
                                "input_batch_id=NULL WHERE bot_id=%s AND trigger_id=%s")
                        .format(sql.Identifier(namespace)),
                        (store.bot_id, "prefill-trigger/1"),
                    )
        accepted = await append_reply("prefill-reply/accepted")
        assert accepted["ok"] is True
        assert accepted["value"]["answered_trigger_count"] == 130
        assert accepted["value"]["answered_events"] == [{
            "trigger_ref": "prefill-trigger/130",
            "event_ref": "prefill-event/130",
        }]
        async with store.pool.connection() as conn:
            group_state = await (
                await conn.execute(
                    sql.SQL("SELECT claim_state FROM {}.trigger_claim_groups "
                            "WHERE bot_id=%s AND claim_ref=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, claim_ref),
                )
            ).fetchone()
        assert group_state["claim_state"] == "TERMINAL"
        assert await append_reply("prefill-reply/accepted") == accepted
        async with store.pool.connection() as conn:
            answered = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS total,"
                            "count(*) FILTER (WHERE status='CLAIMED') AS raw_claimed,"
                            "count(*) FILTER (WHERE {}.conversation_trigger_effective_status("
                            "bot_id,trigger_id)='ANSWERED') AS effective_answered "
                            "FROM {}.conversation_triggers "
                            "WHERE bot_id=%s AND conversation_id=%s")
                    .format(sql.Identifier(namespace), sql.Identifier(namespace)),
                    (store.bot_id, prefill_conversation),
                )
            ).fetchone()
        assert answered["total"] == answered["effective_answered"] == 130
        async with asyncio.timeout(15):
            for attempt in range(60):
                async with store.pool.connection() as conn:
                    progress = await (
                        await conn.execute(
                            sql.SQL("SELECT next_segment_index,completed_at FROM {}."
                                    "trigger_settlement_progress WHERE bot_id=%s "
                                    "AND claim_ref=%s")
                            .format(sql.Identifier(namespace)),
                            (store.bot_id, claim_ref),
                        )
                    ).fetchone()
                if progress["completed_at"] is not None:
                    break
                await settle_one_segment(claim_ref, f"claim:reply-segment/{attempt}")
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("Trigger settlement did not complete")
        async with store.pool.connection() as conn:
            receipts = await (
                await conn.execute(
                    sql.SQL("SELECT segment_index,settled_count FROM {}."
                            "trigger_segment_settlements WHERE bot_id=%s AND claim_ref=%s "
                            "ORDER BY segment_index")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, claim_ref),
                )
            ).fetchall()
            physical = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS answered FROM {}.conversation_triggers "
                            "WHERE bot_id=%s AND conversation_id=%s "
                            "AND status='ANSWERED'")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, prefill_conversation),
                )
            ).fetchone()
        assert [(row["segment_index"], row["settled_count"]) for row in receipts] == [
            (0, 64), (1, 64), (2, 2)
        ]
        assert physical["answered"] == 130
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
                "next_run_ref": None,
            },),
            {},
        )
        assert drained["status"] == "CONTINUE"
        async with store.pool.connection() as conn, conn.transaction():
            replay_key = await (
                await conn.execute(
                    sql.SQL("SELECT operation_id,input_digest FROM {}.selection_operations "
                            "WHERE bot_id=%s AND conversation_id=%s AND result_status='CONTINUE' "
                            "ORDER BY operation_id DESC LIMIT 1")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, prefill_conversation),
                )
            ).fetchone()
            assert replay_key is not None
            selected_again = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_select_input(%s::jsonb,%s::jsonb) AS result")
                    .format(sql.Identifier(namespace)),
                    (
                        Jsonb({
                            "bot_id": store.bot_id,
                            "operation_id": replay_key["operation_id"],
                            "participant_input_digest": replay_key["input_digest"],
                        }),
                        Jsonb({
                            "protocol": "forest.uow-step/1",
                            "input": {
                                "actor": prefill_actor,
                                "boundary": "MODEL_SAFE_POINT",
                                "selection_mode": "PREFILL_ONLY",
                                "session_ref": "prefill-session",
                                "run_ref": "prefill-run",
                                "next_run_ref": None,
                            },
                        }),
                    ),
                )
            ).fetchone()
        assert selected_again["result"]["ok"] is True
        assert selected_again["result"]["value"]["status"] == "CONTINUE"
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET next_ingress_seq=261,"
                    "classified_through_seq=260 WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, prefill_conversation),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.inbox_messages("
                    "bot_id,conversation_id,event_id,ingress_seq,admission_operation_id,"
                    "input_digest,source_component_id,source_event_ref,event_kind,"
                    "author_subject_ref,content_payload_id,content_payload_digest,"
                    "content_byte_length,"
                    "content_owner_component_id,content_owner_kind,content_owner_id,"
                    "author_payload_id,author_payload_digest,author_owner_component_id,"
                    "author_owner_kind,author_owner_id,context_disposition,"
                    "classification_state,trigger_decision,classification_operation_id,"
                    "classification_input_digest,classifier_component_id,"
                    "classifier_generation_id,received_at,classification_deadline,"
                    "classified_at) "
                    "SELECT %s,%s,'prefill-event/'||n,n,'prefill-admit/'||n,%s,"
                    "'adapter.onebot','prefill-source/'||n,'message.group',"
                    "'qq:'||n,'payload/content/'||n,%s,1,'adapter.onebot',"
                    "'onebot.normalized-event','prefill-event/'||n,"
                    "'payload/author/'||n,%s,'adapter.onebot',"
                    "'onebot.author-snapshot','prefill-event/'||n,'MATERIALIZE',"
                    "'FINAL','TRIGGER','prefill-classify/'||n,%s,"
                    "'conversation.scheduler','generation/test',clock_timestamp(),"
                    "clock_timestamp()+interval '1 hour',clock_timestamp() "
                    "FROM generate_series(131,260) AS n"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, prefill_conversation, digest, digest, digest, digest),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_triggers("
                    "bot_id,conversation_id,trigger_id,event_id,ingress_seq,"
                    "principal_ref,input_digest) "
                    "SELECT %s,%s,'prefill-trigger/'||n,'prefill-event/'||n,n,"
                    "'qq:'||n,%s FROM generate_series(131,260) AS n"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, prefill_conversation, digest),
            )
        successor_prefill = await prefill_endpoint.invoke_routed(
            prefill_route,
            RuntimeServiceInvocation(
                "conversation:prefill:successor", 30,
                "test.conversation", prefill_conversation,
            ),
            ({
                "protocol": "forest.conversation-context-prefill/1",
                "request_id": "prefill-successor",
                "actor": prefill_actor,
                "boundary": "SESSION_END",
                "session_ref": "prefill-session",
                "run_ref": "prefill-run",
                "next_run_ref": "prefill-successor",
            },),
            {},
        )
        assert successor_prefill["kind"] == "CONTEXT_PREFILL"
        assert successor_prefill["trigger_claim"]["count"] == 64
        assert successor_prefill["run_ref"] == "prefill-successor"
        async with store.pool.connection() as conn:
            lane_after = await (
                await conn.execute(
                    sql.SQL("SELECT active_run_ref FROM {}.conversation_lanes "
                            "WHERE bot_id=%s AND conversation_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, prefill_conversation),
                )
            ).fetchone()
        assert lane_after["active_run_ref"] == "prefill-run"
        managed_state = await instance.management_contract.query(
            "conversation.requeue.status",
            {"conversation_id": prefill_conversation, "run_ref": "prefill-run"},
        )
        assert managed_state["state"] == "REJECTED"
        assert managed_state["disposition_code"] == "RUN_ANSWERED"

        oversized_content = json.dumps({"text": "x" * 524288}).encode()
        oversized = await kernel.payload.put_bytes(
            owner_component_id="conversation.scheduler",
            owner_kind="context.control",
            owner_id="context/payload-boundary",
            media_type="application/json",
            content=oversized_content,
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        async with store.pool.connection() as conn, conn.transaction():
            added = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.context_units (bot_id,conversation_id,unit_id,"
                        "context_revision,unit_index,kind,source_component_id,source_kind,"
                        "source_id,payload_id,payload_digest,payload_byte_length,"
                        "payload_owner_component_id,payload_owner_kind,payload_owner_id) "
                        "SELECT u.bot_id,u.conversation_id,%s,l.context_revision+1,0,"
                        "'CONTROL','conversation.scheduler','context.control',%s,%s,%s,%s,"
                        "'conversation.scheduler','context.control',%s "
                        "FROM {}.context_units u JOIN {}.conversation_lanes l "
                        "USING (bot_id,conversation_id) WHERE u.bot_id=%s "
                        "AND u.conversation_id=%s AND u.event_id=%s "
                        "RETURNING context_revision"
                    ).format(
                        sql.Identifier(namespace), sql.Identifier(namespace),
                        sql.Identifier(namespace),
                    ),
                    (
                        "unit/payload-boundary", "context/payload-boundary",
                        oversized.payload_id, oversized.content_digest,
                        len(oversized_content), "context/payload-boundary", store.bot_id,
                        "conversation/onebot/group/silent", "event/onebot/silent-3",
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.active_context_units(bot_id,conversation_id,unit_id,"
                    "context_revision,unit_index) VALUES (%s,%s,%s,%s,0)"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "conversation/onebot/group/silent",
                    "unit/payload-boundary", added["context_revision"],
                ),
            )
            actor_after = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {}.conversation_lanes SET context_revision=%s,"
                        "context_view_unit_count=context_view_unit_count+1,"
                        "context_view_payload_bytes=context_view_payload_bytes+%s,"
                        "actor_owner_id=%s,actor_epoch=actor_epoch+1,"
                        "actor_expires_at=clock_timestamp()+interval '5 minutes' "
                        "WHERE bot_id=%s AND conversation_id=%s "
                        "RETURNING conversation_id,actor_owner_id,actor_epoch"
                    ).format(sql.Identifier(namespace)),
                    (
                        added["context_revision"], len(oversized_content),
                        "claim/payload-boundary", store.bot_id,
                        "conversation/onebot/group/silent",
                    ),
                )
            ).fetchone()
        with pytest.raises(ForestError) as over_budget:
            await endpoint.invoke_routed(
                route,
                RuntimeServiceInvocation(
                    "conversation:snapshot:over-budget", 10,
                    "test.conversation", "conversation/onebot/group/silent",
                ),
                ({"actor": {
                    "conversation_id": actor_after["conversation_id"],
                    "owner_id": actor_after["actor_owner_id"],
                    "epoch": actor_after["actor_epoch"],
                }},),
                {},
            )
        assert over_budget.value.code == "CONTEXT_VIEW_HARD_LIMIT"
    finally:
        assert await registry.close() == []

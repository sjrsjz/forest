"""Consecutive real forget batches retain bounded work in a large selection."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from psycopg import sql
from test_memory_forget_process_crash import (
    _ACTOR,
    _DIGEST,
    _SELECTION,
    _execute_batch,
    _invoke,
    _preview,
)
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog
from forest.storage.payload_authority import PayloadSource


@pytest.mark.postgres
async def test_three_forget_batches_in_large_selection(clean_authority, tmp_path):
    filler_count = int(os.environ.get("FOREST_TEST_MEMORY_FILLER_COUNT", "100000"))
    assert 100000 <= filler_count <= 999808
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "forget-scale-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace
    ns = sql.Identifier(namespace)
    first_epoch = await kernel.payload.epoch()

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources (bot_id,source_id,registration_operation_id,"
            "input_digest,source_kind,identity_incarnation,identity_digest) "
            "VALUES (%s,'source/scale','register/scale',%s,'DIRECT',1,%s)"
        ).format(ns), (store.bot_id, _DIGEST, _DIGEST))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_units (bot_id,source_unit_id,capture_operation_id,"
            "input_digest,source_id,source_order_seq,source_revision,unit_kind,modality,"
            "part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
            "author_payload_digest,author_owner_component_id,author_owner_kind,"
            "author_owner_id,accepted_deletion_epoch,eligibility_state,eligible_at,"
            "eligible_xid,occurred_at) "
            "SELECT %s,'unit/filler/'||n,'capture/filler/'||n,%s,"
            "'source/scale',n,1,'MESSAGE','TEXT',1,%s,%s,"
            "'payload/filler/'||n,%s,'memory.graph','AUTHOR','author/filler/'||n,"
            "%s,'ELIGIBLE',clock_timestamp(),pg_current_xact_id(),"
            "TIMESTAMPTZ '2025-01-01' + n*INTERVAL '1 second' "
            "FROM generate_series(1,%s) n"
        ).format(ns), (store.bot_id, _DIGEST, _DIGEST, _ACTOR,
                       _DIGEST, first_epoch, filler_count))

    # The newest 192 members have actual Payload anchors; the older members
    # members make selection/progress/closure operate in a long-lived Bot.
    for number in range(1, 193):
        root = await kernel.payload.put_bytes(
            owner_component_id="conversation.scheduler", owner_kind="context.input",
            owner_id=f"input/scale/{number}", media_type="text/plain",
            content=f"message {number}".encode(),
            sources=(PayloadSource("conversation.scheduler", "group.message",
                                   f"message/scale/{number}", 1),),
            expected_deletion_epoch=first_epoch,
        )
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units (bot_id,source_unit_id,capture_operation_id,"
                "input_digest,source_id,source_order_seq,source_revision,unit_kind,modality,"
                "part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,eligible_at,"
                "eligible_xid,occurred_at) VALUES "
                "(%s,%s,%s,%s,'source/scale',%s,1,'MESSAGE','TEXT',1,%s,%s,%s,%s,"
                "'conversation.scheduler','context.input',%s,%s,'ELIGIBLE',"
                "clock_timestamp(),pg_current_xact_id(),"
                "TIMESTAMPTZ '2026-01-01' + %s*INTERVAL '1 second')"
            ).format(ns), (
                store.bot_id, f"unit/real/{number}", f"capture/real/{number}",
                _DIGEST, filler_count + number, _DIGEST, _ACTOR,
                root.payload_id, root.content_digest, f"input/scale/{number}",
                first_epoch, number,
            ))

    created = await _invoke(
        store, namespace, "memory_freeze_forget_selection",
        {"bot_id": store.bot_id, "operation_id": "selection/create-scale"},
        {"selection_id": _SELECTION, "requested_by": _ACTOR,
         "filters": {"author_subject_ref": _ACTOR}},
    )
    assert created["ok"] and created["value"]["member_count"] == filler_count + 192

    for index in range(3):
        after = index * 64
        request, plan = await _preview(
            store, namespace, kernel.payload, after, batch_size=64,
        )
        operation_id = f"batch/scale/{index}"
        queued = await _invoke(
            store, namespace, "memory_enqueue_management_forget_batch",
            {"bot_id": store.bot_id, "operation_id": operation_id},
            {**request, "plan": plan},
        )
        assert queued["ok"], queued
        claimed = await _invoke(
            store, namespace, "memory_claim_management_forget",
            {"bot_id": store.bot_id},
            {"protocol": "forest.owned-work-claim-request/1",
             "claim_token": f"claim/scale/{index}", "lease_seconds": 60},
        )
        assert claimed["ok"] and claimed["value"]["work_id"] == operation_id
        result = await _execute_batch(
            store, namespace, kernel.payload, request, operation_id, plan,
        )
        assert result["last_ordinal"] == after + 64
        settled = await _invoke(
            store, namespace, "memory_settle_management_forget",
            {"bot_id": store.bot_id},
            {"protocol": "forest.owned-work-settle-request/1",
             "work_id": operation_id, "claim_token": f"claim/scale/{index}",
             "attempt": 1, "outcome": {"status": "SUCCEEDED", "result": result}},
        )
        assert settled["ok"], settled
        progress = await _invoke(
            store, namespace, "memory_query_forget_selection",
            {"bot_id": store.bot_id},
            {"selection_id": _SELECTION, "requested_by": _ACTOR},
        )
        assert progress["ok"], progress
        assert progress["value"]["progress_state"] == "PARTIAL"
        assert progress["value"]["applied_batch_count"] == index + 1
        assert progress["value"]["applied_member_count"] == after + 64
        assert progress["value"]["next_unapplied_ordinal"] == after + 65

    assert await kernel.payload.epoch() == first_epoch + 3
    async with store.pool.connection() as conn:
        counts = await (await conn.execute(sql.SQL(
            "SELECT count(*) FILTER (WHERE eligibility_state='SUPPRESSED') AS forgotten,"
            "count(*) FILTER (WHERE eligibility_state='ELIGIBLE') AS eligible "
            "FROM {}.source_units WHERE bot_id=%s"
        ).format(ns), (store.bot_id,))).fetchone()
    assert counts == {"forgotten": 192, "eligible": filler_count}

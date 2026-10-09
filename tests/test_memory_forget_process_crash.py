"""A committed second forget batch survives its executing process dying before settlement."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration, _isolated_memory_descriptors
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.kernel_payload_participant import KernelPayloadUnitOfWorkParticipant
from forest.packages import PackageCatalog
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.bot_store import BotStore
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource

_DIGEST = "sha256:" + "a" * 64
_INPUT_DIGEST = "sha256:" + "b" * 64
_SELECTION = "selection/process-crash"
_ACTOR = "admin/process-crash"
_STEP = {"protocol": "forest.uow-step/1", "caller": {
    "component_id": "memory.graph", "generation_id": "memory/process-crash",
}}


def _participant(payload):
    runtime = SimpleNamespace(generation_identity=SimpleNamespace(
        generation_id="payload/process-crash",
    ))
    return KernelPayloadUnitOfWorkParticipant(runtime, payload)


async def _call(conn, namespace, function, context, body):
    row = await (await conn.execute(
        sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
            sql.Identifier(namespace), sql.Identifier(function),
        ), (Jsonb(context), Jsonb(body)),
    )).fetchone()
    return row["result"]


async def _invoke(store, namespace, function, context, body):
    async with store.pool.connection() as conn, conn.transaction():
        return await _call(conn, namespace, function, context, body)


async def _preview(store, namespace, payload, after_ordinal):
    request = {"selection_id": _SELECTION, "requested_by": _ACTOR,
               "after_ordinal": after_ordinal, "batch_size": 2}
    async with store.pool.connection() as conn, conn.transaction():
        prepared = await _call(conn, namespace, "memory_prepare_forget_batch",
                               {"bot_id": store.bot_id},
                               {**_STEP, "input": request, "prior": {}})
        assert prepared["ok"], prepared
        preview = await _participant(payload).invoke_in(
            conn, "payload.preview-forget-anchors-from-prior", {
                **_STEP, "input": {
                    "protocol": "forest.payload-forget-anchors-preview/1",
                    "prior_step_id": "prepare",
                }, "prior": {"prepare": prepared["value"]},
            },
        )
        planned = await _call(conn, namespace, "memory_preview_forget_batch",
                              {"bot_id": store.bot_id}, {
                                  **_STEP, "input": {**request,
                                      "prepare_step_id": "prepare",
                                      "preview_step_id": "payload"},
                                  "prior": {"prepare": prepared["value"],
                                            "payload": preview},
                              })
        assert planned["ok"], planned
        return request, planned["value"]


async def _execute_batch(store, namespace, payload, request, operation_id, plan):
    body = {**request, "operation_id": operation_id}
    async with store.pool.connection() as conn, conn.transaction():
        prepared = await _call(conn, namespace, "memory_prepare_forget_batch",
                               {"bot_id": store.bot_id},
                               {**_STEP, "input": body, "prior": {}})
        assert prepared["ok"], prepared
        forgotten = await _participant(payload).invoke_in(
            conn, "payload.forget-anchors-from-prior", {
                **_STEP, "input": {
                    "protocol": "forest.payload-forget-anchors/1",
                    "prior_step_id": "prepare", "operation_id": operation_id,
                    "expected_deletion_epoch": plan["deletion_epoch"],
                    "expected_selection_digest": plan["selection_digest"],
                }, "prior": {"prepare": prepared["value"]},
            },
        )
        applied = await _call(conn, namespace, "memory_apply_forget_batch",
                              {"bot_id": store.bot_id,
                               "participant_input_digest": _INPUT_DIGEST}, {
                                  **_STEP, "input": {**body,
                                      "prepare_step_id": "prepare",
                                      "forget_step_id": "payload", "plan": plan},
                                  "prior": {"prepare": prepared["value"],
                                            "payload": forgotten},
                              })
        assert applied["ok"], applied
        return applied["value"]


async def _child(spec_path):
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    parsed = urlsplit(os.environ["FOREST_TEST_DSN"])
    uri = urlunsplit((parsed.scheme, parsed.netloc,
                      "/" + spec["database"], "", ""))
    store = BotStore(uri, spec["bot_id"])
    await store.open()
    claim = await _invoke(store, spec["namespace"],
                          "memory_claim_management_forget",
                          {"bot_id": store.bot_id}, {
                              "protocol": "forest.owned-work-claim-request/1",
                              "claim_token": "claim/crashed-worker",
                              "lease_seconds": 5,
                          })
    assert claim["ok"] and claim["value"]["work_id"] == spec["operation_id"]
    assert claim["value"]["attempt"] == 1
    result = await _execute_batch(
        store, spec["namespace"], KernelPayloadRepository(store),
        spec["request"], spec["operation_id"], spec["plan"],
    )
    assert result["last_ordinal"] == 4
    # The transaction has committed; the process dies before owned-work settlement.
    os._exit(137)


@pytest.mark.postgres
async def test_second_forget_batch_recovers_after_real_process_death(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "forget-process-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources (bot_id,source_id,registration_operation_id,"
            "input_digest,source_kind,identity_incarnation,identity_digest) "
            "VALUES (%s,'source/process','register/process',%s,'DIRECT',1,%s)"
        ).format(sql.Identifier(namespace)), (store.bot_id, _DIGEST, _DIGEST))
    starting_epoch = await kernel.payload.epoch()
    for number in range(1, 5):
        root = await kernel.payload.put_bytes(
            owner_component_id="conversation.scheduler", owner_kind="context.input",
            owner_id=f"input/process/{number}", media_type="text/plain",
            content=f"message {number}".encode(),
            sources=(PayloadSource("conversation.scheduler", "group.message",
                                   f"message/process/{number}", 1),),
            expected_deletion_epoch=starting_epoch,
        )
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units (bot_id,source_unit_id,capture_operation_id,"
                "input_digest,source_id,source_order_seq,source_revision,unit_kind,modality,"
                "part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,eligible_at,"
                "eligible_xid,occurred_at) VALUES "
                "(%s,%s,%s,%s,'source/process',%s,1,'MESSAGE','TEXT',1,%s,%s,%s,%s,"
                "'conversation.scheduler','context.input',%s,%s,'ELIGIBLE',"
                "clock_timestamp(),pg_current_xact_id(),%s::timestamptz)"
            ).format(sql.Identifier(namespace)), (
                store.bot_id, f"unit/{number}", f"capture/{number}", _DIGEST,
                number, _DIGEST, _ACTOR, root.payload_id, root.content_digest,
                f"input/process/{number}", starting_epoch,
                f"2026-01-{number:02d}T00:00:00Z",
            ))
    created = await _invoke(store, namespace, "memory_freeze_forget_selection",
                            {"bot_id": store.bot_id,
                             "operation_id": "selection/create-process"}, {
                                 "selection_id": _SELECTION, "requested_by": _ACTOR,
                                 "filters": {"author_subject_ref": _ACTOR},
                             })
    assert created["ok"] and created["value"]["member_count"] == 4
    first_request, first_plan = await _preview(store, namespace, kernel.payload, 0)
    first_operation = "batch/process-first"
    first_queue = await _invoke(store, namespace,
                                "memory_enqueue_management_forget_batch",
                                {"bot_id": store.bot_id,
                                 "operation_id": first_operation},
                                {**first_request, "plan": first_plan})
    assert first_queue["ok"], first_queue
    first_claim = await _invoke(store, namespace, "memory_claim_management_forget",
                                {"bot_id": store.bot_id}, {
                                    "protocol": "forest.owned-work-claim-request/1",
                                    "claim_token": "claim/first", "lease_seconds": 30,
                                })
    assert first_claim["ok"] and first_claim["value"]["work_id"] == first_operation
    first_result = await _execute_batch(store, namespace, kernel.payload,
                                        first_request, first_operation, first_plan)
    settled = await _invoke(store, namespace, "memory_settle_management_forget",
                            {"bot_id": store.bot_id}, {
                                "protocol": "forest.owned-work-settle-request/1",
                                "work_id": first_operation, "claim_token": "claim/first",
                                "attempt": 1,
                                "outcome": {"status": "SUCCEEDED", "result": first_result},
                            })
    assert settled["ok"], settled
    assert await kernel.payload.epoch() == starting_epoch + 1
    partial = await _invoke(store, namespace, "memory_query_forget_selection",
                            {"bot_id": store.bot_id}, {
                                "selection_id": _SELECTION, "requested_by": _ACTOR,
                            })
    assert partial["value"]["progress_state"] == "PARTIAL"
    assert partial["value"]["applied_batch_count"] == 1
    assert partial["value"]["applied_member_count"] == 2
    assert partial["value"]["next_unapplied_ordinal"] == 3

    second_request, second_plan = await _preview(store, namespace, kernel.payload, 2)
    second_operation = "batch/process-second"
    second_queue = await _invoke(store, namespace,
                                 "memory_enqueue_management_forget_batch",
                                 {"bot_id": store.bot_id,
                                  "operation_id": second_operation},
                                 {**second_request, "plan": second_plan})
    assert second_queue["ok"], second_queue
    async with store.pool.connection() as conn:
        current_database = await (await conn.execute(
            "SELECT current_database() AS name"
        )).fetchone()
    spec = {"database": current_database["name"], "bot_id": store.bot_id,
            "namespace": namespace, "request": second_request,
            "operation_id": second_operation, "plan": second_plan}
    spec_path = tmp_path / "second-batch.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    child = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(Path(__file__).resolve()), "--crash-child", str(spec_path)],
        capture_output=True, text=True, timeout=60, check=False,
    )
    assert child.returncode == 137, (child.returncode, child.stdout, child.stderr)
    assert await kernel.payload.epoch() == starting_epoch + 2
    async with store.pool.connection() as conn:
        row = await (await conn.execute(sql.SQL(
            "SELECT state,attempt_count,lease_deadline FROM {}.management_forget_work "
            "WHERE bot_id=%s AND request_id=%s"
        ).format(sql.Identifier(namespace)), (store.bot_id, second_operation))).fetchone()
    assert row["state"] == "RUNNING" and row["attempt_count"] == 1
    delay = (row["lease_deadline"] - datetime.now(timezone.utc)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay + 0.1)
    lease = await kernel.runtime.claim_host(
        "host/forget-crash-recovery", "incarnation/forget-crash-recovery",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot", bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer, generation_authority=kernel.runtime,
        authority_lease=lambda **_kwargs: lease,
    )
    registry = RuntimeModuleRegistry(host, _isolated_memory_descriptors(kernel, package))
    await registry.start()
    try:
        async with asyncio.timeout(30):
            while True:
                status = await _invoke(
                    store, namespace, "memory_query_management_forget_batch",
                    {"bot_id": store.bot_id}, {
                        "request_id": second_operation, "requested_by": _ACTOR,
                    },
                )
                assert status["ok"], status
                if status["value"]["state"] == "SUCCEEDED":
                    break
                await asyncio.sleep(0.1)
    finally:
        assert await registry.close() == []
    assert status["value"]["state"] == "SUCCEEDED"
    assert status["value"]["attempt_count"] == 1
    assert status["value"]["result"]["last_ordinal"] == 4
    stale = await _invoke(store, namespace, "memory_settle_management_forget",
                          {"bot_id": store.bot_id}, {
                              "protocol": "forest.owned-work-settle-request/1",
                              "work_id": second_operation,
                              "claim_token": "claim/crashed-worker", "attempt": 1,
                              "outcome": {"status": "SUCCEEDED",
                                          "result": status["value"]["result"]},
                          })
    assert stale["error"]["code"] == "MEMORY_FORGET_CLAIM_LOST"
    assert await kernel.payload.epoch() == starting_epoch + 2
    complete = await _invoke(store, namespace, "memory_query_forget_selection",
                             {"bot_id": store.bot_id}, {
                                 "selection_id": _SELECTION, "requested_by": _ACTOR,
                                 "limit": 4,
                             })
    assert complete["value"]["progress_state"] == "COMPLETE"
    assert complete["value"]["applied_batch_count"] == 2
    assert complete["value"]["applied_member_count"] == 4
    assert complete["value"]["next_unapplied_ordinal"] is None
    assert all(item["forget_state"] == "FORGOTTEN_BY_SELECTION"
               for item in complete["value"]["items"])


if __name__ == "__main__" and len(sys.argv) == 3 and sys.argv[1] == "--crash-child":
    asyncio.run(_child(Path(sys.argv[2])))

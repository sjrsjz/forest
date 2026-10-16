import asyncio
from pathlib import Path

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_graph_rebuild_work_requeues_progress_and_settles_publication(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "graph-rebuild-work-packages"),
        store, installer, kernel=kernel,
        initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    base = {"bot_id": store.bot_id}

    async def call(conn, name, context, body):
        row = await (await conn.execute(
            sql.SQL("SELECT {}.{}(%s,%s) AS result").format(
                namespace, sql.Identifier(name)
            ),
            (Jsonb(context), Jsonb(body)),
        )).fetchone()
        return row["result"]

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions("
            "bot_id,contribution_id,operation_id,input_digest,relation_id,"
            "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
            "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
            "(%s,'graph-work/edge','graph-work/create',%s,'graph-work/relation',"
            "'ASSOCIATES_WITH','ENTITY','left','ENTITY','right','RULE',"
            "500000,0,false)"
        ).format(namespace), (store.bot_id, "sha256:" + "a" * 64))
        requested = await call(
            conn, "memory_enqueue_graph_rebuild",
            {**base, "operation_id": "graph-work/start"},
            {"requested_by": "admin"},
        )
        assert requested["ok"]
        generation_id = requested["value"]["generation_id"]
        assert requested["value"]["state"] == "QUEUED"
        assert await call(
            conn, "memory_enqueue_graph_rebuild",
            {**base, "operation_id": "graph-work/start"},
            {"requested_by": "admin"},
        ) == requested
        competing = await call(
            conn, "memory_enqueue_graph_rebuild",
            {**base, "operation_id": "graph-work/other"},
            {"requested_by": "admin"},
        )
        assert competing["error"]["code"] == "MEMORY_GRAPH_REBUILD_IN_PROGRESS"

    def claim(token):
        return {
            "protocol": "forest.owned-work-claim-request/1",
            "claim_token": token, "lease_seconds": 30,
        }

    def settle(token, attempt, published=False):
        return {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": generation_id, "claim_token": token,
            "attempt": attempt,
            "outcome": {
                "status": "SUCCEEDED",
                "result": {
                    "generation_id": generation_id,
                    "replayed_through": 1,
                    "published": published,
                },
            },
        }

    async with store.pool.connection() as conn, conn.transaction():
        first = await call(conn, "memory_claim_graph_rebuild", base, claim("claim/first"))
        assert first["value"]["payload"] == {
            "generation_id": generation_id, "requested_by": "admin",
        }
        assert first["value"]["attempt"] == 1
        replay = await call(
            conn, "memory_replay_graph_revision", base,
            {"generation_id": generation_id, "revision": 1},
        )
        assert replay["value"]["replayed_through"] == 1
        first_settlement = await call(
            conn, "memory_settle_graph_rebuild", base,
            settle("claim/first", 1),
        )
        assert first_settlement["ok"]
        assert await call(
            conn, "memory_settle_graph_rebuild", base,
            settle("claim/first", 1),
        ) == first_settlement
        progress = await call(
            conn, "memory_query_graph_rebuild", base,
            {"generation_id": generation_id, "requested_by": "admin"},
        )
        assert progress["value"]["state"] == "QUEUED"
        assert progress["value"]["replayed_through"] == 1
        assert progress["value"]["active_revision"] == 1
        latest = await call(
            conn, "memory_query_graph_rebuild", base,
            {"requested_by": "admin"},
        )
        assert latest["value"]["generation_id"] == generation_id
        hidden = await call(
            conn, "memory_query_graph_rebuild", base,
            {"requested_by": "someone-else"},
        )
        assert hidden["error"]["code"] == "MEMORY_GRAPH_REBUILD_NOT_FOUND"

    async with store.pool.connection() as conn, conn.transaction():
        second = await call(
            conn, "memory_claim_graph_rebuild", base, claim("claim/second")
        )
        assert second["value"]["attempt"] == 2
        envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "memory.graph", "generation_id": "test"},
            "input": {
                "protocol": "forest.memory-graph-publish/1",
                "deletion_epoch_step_id": "payload_epoch",
                "generation_id": generation_id,
                "expected_active_generation_id": "primary",
                "expected_active_revision": 1,
            },
            "prior": {
                "payload_epoch": {
                    "protocol": "forest.payload-epoch/1", "deletion_epoch": 0,
                },
            },
        }
        published = await call(
            conn, "memory_publish_graph_rebuild",
            {**base, "operation_id": "graph-work/publish"}, envelope,
        )
        assert published["ok"]
        final_settlement = await call(
            conn, "memory_settle_graph_rebuild", base,
            settle("claim/second", 2, published=True),
        )
        assert final_settlement["ok"]
        final = await call(
            conn, "memory_query_graph_rebuild", base,
            {"generation_id": generation_id, "requested_by": "admin"},
        )
        assert final["value"]["state"] == "SUCCEEDED"
        assert final["value"]["published_revision"] == 1
        idle = await call(
            conn, "memory_claim_graph_rebuild", base, claim("claim/idle")
        )
        assert idle["value"] is None

    async with store.pool.connection() as conn, conn.transaction():
        another = await call(
            conn, "memory_enqueue_graph_rebuild",
            {**base, "operation_id": "graph-work/recovery"},
            {"requested_by": "admin"},
        )
        recovery_id = another["value"]["generation_id"]
        latest = await call(
            conn, "memory_query_graph_rebuild", base,
            {"requested_by": "admin"},
        )
        assert latest["value"]["generation_id"] == recovery_id
        earlier = await call(
            conn, "memory_query_graph_rebuild", base,
            {"generation_id": generation_id, "requested_by": "admin"},
        )
        assert earlier["value"]["generation_id"] == generation_id
        expired = await call(
            conn, "memory_claim_graph_rebuild", base,
            {**claim("claim/expired"), "lease_seconds": 3},
        )
        assert expired["value"]["attempt"] == 1
    await asyncio.sleep(3.2)
    async with store.pool.connection() as conn, conn.transaction():
        recovered = await call(
            conn, "memory_claim_graph_rebuild", base, claim("claim/recovered")
        )
        assert recovered["value"]["attempt"] == 2
        stale = await call(
            conn, "memory_settle_graph_rebuild", base,
            {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": recovery_id,
                "claim_token": "claim/expired",
                "attempt": 1,
                "outcome": {"status": "SUCCEEDED", "result": {}},
            },
        )
        assert stale["error"]["code"] == "MEMORY_GRAPH_REBUILD_CLAIM_LOST"

        def failed(token, attempt):
            return {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": recovery_id, "claim_token": token,
                "attempt": attempt,
                "outcome": {
                    "status": "FAILED",
                    "error": {"code": "GRAPH_REBUILD_TEST_FAILURE"},
                },
            }

        assert (await call(
            conn, "memory_settle_graph_rebuild", base,
            failed("claim/recovered", 2),
        ))["ok"]
    await asyncio.sleep(4.2)
    async with store.pool.connection() as conn, conn.transaction():
        third = await call(
            conn, "memory_claim_graph_rebuild", base, claim("claim/third")
        )
        assert third["value"]["attempt"] == 3
        assert (await call(
            conn, "memory_settle_graph_rebuild", base,
            failed("claim/third", 3),
        ))["ok"]
        failed_state = await call(
            conn, "memory_query_graph_rebuild", base,
            {"generation_id": recovery_id, "requested_by": "admin"},
        )
        assert failed_state["value"]["state"] == "FAILED"
        retry_context = {**base, "operation_id": "graph-work/retry"}
        retry_input = {"generation_id": recovery_id, "requested_by": "admin"}
        retry = await call(
            conn, "memory_retry_graph_rebuild", retry_context, retry_input,
        )
        assert retry["value"]["state"] == "QUEUED"
        assert await call(
            conn, "memory_retry_graph_rebuild", retry_context, retry_input,
        ) == retry
        conflicting_retry = await call(
            conn, "memory_retry_graph_rebuild", retry_context,
            {"generation_id": recovery_id, "requested_by": "other"},
        )
        assert conflicting_retry["error"]["code"] == (
            "MEMORY_GRAPH_REBUILD_RETRY_CONFLICT"
        )
        fourth = await call(
            conn, "memory_claim_graph_rebuild", base, claim("claim/fourth")
        )
        assert fourth["value"]["attempt"] == 4
        assert fourth["value"]["payload"]["generation_id"] == recovery_id
        assert await call(
            conn, "memory_retry_graph_rebuild", retry_context, retry_input,
        ) == retry
        for attempt in range(4, 8):
            token = "claim/fourth" if attempt == 4 else f"claim/success-{attempt}"
            settled = await call(
                conn, "memory_settle_graph_rebuild", base,
                {
                    "protocol": "forest.owned-work-settle-request/1",
                    "work_id": recovery_id, "claim_token": token,
                    "attempt": attempt,
                    "outcome": {
                        "status": "SUCCEEDED",
                        "result": {
                            "generation_id": recovery_id,
                            "replayed_through": 0,
                            "published": False,
                        },
                    },
                },
            )
            assert settled["ok"]
            continuation = await call(
                conn, "memory_claim_graph_rebuild", base,
                claim(f"claim/success-{attempt + 1}"),
            )
            assert continuation["value"]["attempt"] == attempt + 1
        one_failure = await call(
            conn, "memory_settle_graph_rebuild", base,
            failed("claim/success-8", 8),
        )
        assert one_failure["ok"]
        after_failure = await call(
            conn, "memory_query_graph_rebuild", base,
            {"generation_id": recovery_id, "requested_by": "admin"},
        )
        assert after_failure["value"]["state"] == "QUEUED"

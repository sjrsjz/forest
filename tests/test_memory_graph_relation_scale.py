import time
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
async def test_memory_relation_page_scale(clean_authority, tmp_path):
    """Exercise keyset reads and rare cross-trunk filters over 100k facts."""
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "benchmark-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn:
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions (bot_id,contribution_id,operation_id,"
            "input_digest,relation_id,relation_kind,left_kind,left_id,right_kind,right_id,"
            "origin_kind,weight_micros,admitted_deletion_epoch,cross_trunk) "
            "SELECT %s,'c/'||lpad(n::text,6,'0'),'op/'||lpad(n::text,6,'0'),%s,"
            "'rel/'||lpad(n::text,6,'0'),'ASSOCIATES_WITH','ENTITY','left',"
            "'ENTITY','right/'||lpad(n::text,6,'0'),'RULE',500000,0,false "
            "FROM generate_series(1,100000) n"
        ).format(namespace), (store.bot_id, digest))
        await conn.commit()
        await conn.execute(sql.SQL("ANALYZE {}.relation_contributions").format(namespace))
        await conn.execute("SET enable_seqscan=off")
        plan = await (await conn.execute(sql.SQL(
            "EXPLAIN (COSTS OFF) SELECT contribution_id "
            "FROM {}.relation_contributions WHERE bot_id=%s AND active "
            "AND cross_trunk=true ORDER BY relation_id,contribution_id LIMIT 20"
        ).format(namespace), (store.bot_id,))).fetchall()
        await conn.execute("RESET enable_seqscan")
        assert "relation_cross_trunk_browse_idx" in "\n".join(
            row["QUERY PLAN"] for row in plan
        )

        async def query(values):
            started = time.perf_counter()
            row = await (await conn.execute(
                sql.SQL("SELECT {}.memory_search_relations(%s::jsonb,%s::jsonb) AS result")
                .format(namespace), (Jsonb({"bot_id": store.bot_id}), Jsonb(values))
            )).fetchone()
            elapsed = time.perf_counter() - started
            assert row["result"]["ok"]
            print(f"relation page: {values} -> {elapsed:.3f}s")
            return row["result"]["value"]

        first = await query({"limit": 20})
        assert len(first["items"]) == 20 and first["has_more"]
        late = await query({
            "limit": 20, "snapshot_xid": first["snapshot_xid"],
            "after_relation_id": "rel/090000", "after_contribution_id": "c/090000",
            "cursor_filter_digest": first["filter_digest"],
        })
        assert late["items"][0]["relation_id"] == "rel/090001"
        filtered = await query({"limit": 20, "cross_trunk": False})
        assert len(filtered["items"]) == 20
        absent = await query({"limit": 20, "cross_trunk": True})
        assert absent["items"] == []

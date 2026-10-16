import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog
from forest.runtime_modules import RuntimeModuleRegistry


@pytest.mark.postgres
async def test_graph_rebuild_inflight_chunk_hands_off_to_new_generation(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "graph-reload-packages"),
        store, installer, kernel=kernel,
        initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64

    for revision in range(1, 41):
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.relation_contributions("
                "bot_id,contribution_id,operation_id,input_digest,relation_id,"
                "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                "(%s,%s,%s,%s,%s,'ASSOCIATES_WITH','ENTITY','left',"
                "'ENTITY',%s,'RULE',500000,0,false)"
            ).format(namespace), (
                store.bot_id, f"reload/edge/{revision}",
                f"reload/create/{revision}", digest,
                f"reload/relation/{revision}", f"right/{revision}",
            ))
    async with store.pool.connection() as conn, conn.transaction():
        queued = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_enqueue_graph_rebuild(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "operation_id": "reload/rebuild"}),
            Jsonb({"requested_by": "admin"}),
        ))).fetchone())["result"]
    assert queued["ok"], queued
    generation_id = queued["value"]["generation_id"]

    lease = await kernel.runtime.claim_host(
        "graph-reload-host", "graph-reload-incarnation",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot", bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    descriptors = (*kernel.descriptors, package.descriptor)
    available = {item.component_id for item in descriptors}
    isolated = tuple(
        replace(
            item,
            retention_requires=tuple(
                dependency for dependency in item.retention_requires
                if dependency in available
            ),
        ) for item in descriptors
    )

    async def work_state():
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT w.state,w.attempt_count,b.replayed_through,"
                "b.published_revision FROM {}.graph_rebuild_work w "
                "JOIN {}.graph_generation_builds b USING(bot_id,generation_id) "
                "WHERE w.bot_id=%s AND w.generation_id=%s"
            ).format(namespace, namespace), (
                store.bot_id, generation_id,
            ))).fetchone()
            return row

    previous = RuntimeModuleRegistry(host, isolated)
    candidate = None
    drain = None
    try:
        async with store.pool.connection() as blocker:
            async with blocker.transaction():
                await blocker.execute(sql.SQL(
                    "SELECT 1 FROM {}.graph_generation_builds "
                    "WHERE bot_id=%s AND generation_id=%s FOR UPDATE"
                ).format(namespace), (store.bot_id, generation_id))
                await previous.start()
                async with asyncio.timeout(60):
                    while (await work_state())["state"] != "CLAIMED":
                        await asyncio.sleep(0.05)
                candidate = RuntimeModuleRegistry(host, isolated)
                await candidate.prewarm()
                drain = asyncio.create_task(
                    previous.drain_owned_work_for_replacement(
                        candidate.activation_id
                    )
                )
                await asyncio.sleep(0.2)
                assert not drain.done()
        await asyncio.wait_for(drain, 180)
        between = await work_state()
        assert between["state"] == "QUEUED"
        assert between["replayed_through"] == 32
        assert between["published_revision"] is None

        await candidate.start()
        candidate.accept_retained_ownership()
        assert await previous.close() == []
        async with asyncio.timeout(180):
            while (await work_state())["state"] != "SUCCEEDED":
                await asyncio.sleep(0.1)
        completed = await work_state()
        assert completed["attempt_count"] >= 2
        assert completed["replayed_through"] == 40
        assert completed["published_revision"] == 40
        async with store.pool.connection() as conn:
            edges = await (await conn.execute(sql.SQL(
                "SELECT count(*) AS total,count(DISTINCT contribution_id) AS distinct_ids "
                "FROM {}.graph_edge_versions "
                "WHERE bot_id=%s AND generation_id=%s AND to_revision IS NULL"
            ).format(namespace), (store.bot_id, generation_id))).fetchone()
        assert edges == {"total": 40, "distinct_ids": 40}
        assert await candidate.close() == []
    finally:
        if drain is not None and not drain.done():
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
        if candidate is not None:
            await candidate.close()
        await previous.close()

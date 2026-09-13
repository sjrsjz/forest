"""SQL publication hydrates current payloads without reviving cleared config."""

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_bot_store import candidate
from test_bot_store import database as database
from test_bot_store import store as store

from forest.memory_types import SourceRef
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def read_configuration(store):
    async with store.pool.connection() as conn:
        return (await (await conn.execute(
            "SELECT public.forest_generation_configuration(%s,'gen-1') AS config",
            (store.bot_id,),
        )).fetchone())["config"]


@pytest.mark.parametrize("suppression_only", [False, True])
async def test_sql_config_reader_rejects_actual_deletion_and_unmaterialized_suppression(
    store, suppression_only,
):
    lease = await store.acquire_host("host", "incarnation", ttl_seconds=120)
    await candidate(store, lease)
    assert await read_configuration(store) == {"initial": 0}
    async with store.pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT g.config,g.state_value_schema,g.config_payload_id,d.source_unit_id "
            "FROM public.bot_generations g JOIN public.bot_payload_domains d "
            "ON d.bot_id=g.bot_id AND d.owner_id=g.generation_id AND d.kind='GENERATION_CONFIG' "
            "WHERE g.bot_id=%s AND g.generation_id='gen-1'", (store.bot_id,),
        )).fetchone()
    assert row["config"] is None and row["state_value_schema"] is None
    source = SourceRef(row["source_unit_id"])
    payloads = PayloadStore(store)
    if suppression_only:
        # A durable suppression is authoritative even before a reader's header
        # has been marked REVOKED (e.g. bounded late-link reconciliation).
        # Keep the real header/body ACTIVE to isolate this SQL guard explicitly.
        async with store.pool.connection() as conn, conn.transaction():
            await store._host(conn, lease)
            await payloads.lock_in(conn)
            await conn.execute(
                "INSERT INTO public.bot_payload_forget_requests(bot_id,operation_id,input_digest,"
                "targets,deletion_epoch) VALUES (%s,'config-suppressed','fixed-test-decision',%s,1)",
                (store.bot_id, Jsonb([{"source_unit_id": source.source_unit_id, "part_id": "*"}])),
            )
            await conn.execute(
                "INSERT INTO public.bot_payload_suppression(bot_id,source_unit_id,part_id,operation_id,"
                "deletion_epoch) VALUES (%s,%s,'*','config-suppressed',1)",
                (store.bot_id, source.source_unit_id),
            )
            await conn.execute(
                "UPDATE public.bot_memory_clock SET deletion_epoch=1,fact_revision=fact_revision+1 "
                "WHERE bot_id=%s", (store.bot_id,),
            )
            head = await (await conn.execute(
                "SELECT h.state,b.content FROM public.bot_payload_headers h "
                "JOIN public.bot_payload_bodies b USING(bot_id,payload_id) "
                "WHERE h.bot_id=%s AND h.payload_id=%s", (store.bot_id, row["config_payload_id"]),
            )).fetchone()
            assert head["state"] == "ACTIVE" and head["content"]
    else:
        await payloads.forget_sources(lease, "forget-config", (source,), expected_deletion_epoch=0)
        assert (await payloads.scrub(lease))["removed"] == 1
    with pytest.raises(psycopg.errors.RaiseException, match="GENERATION_CONFIG_UNAVAILABLE"):
        await read_configuration(store)

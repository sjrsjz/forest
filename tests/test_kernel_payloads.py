"""Kernel content reference and deletion invariants on the clean baseline."""

import pytest
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.kernel_payloads import KernelPayloadStore

pytestmark = pytest.mark.postgres

VALUE = {"message": "KERNEL-PAYLOAD-CANARY", "image": {"description": "test pixels"}}

async def test_new_payload_closure_null_and_duplicate_receipts_never_revive(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("host", "incarnation", ttl_seconds=120)
        helper = KernelPayloadStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            await store._host(conn, lease)
            await helper.payloads.lock_in(conn)
            input_ref = await helper.put_call_input_in(
                conn, "call", VALUE, sources=(SourceRef("input-a"),), epoch=0
            )
            state_ref = await helper.put_state_in(
                conn, "counter", "key", 1, None, parents=(input_ref.payload_id,), epoch=0
            )
            output_ref = await helper.put_call_result_in(
                conn, "call", None, parents=(state_ref.payload_id,), epoch=0
            )
            source_ref = await helper.put_migration_source_in(
                conn, "migration", {"key": None}, parents=(state_ref.payload_id,), epoch=0
            )
            candidate = await helper.put_migration_result_in(
                conn, "migration", {"key": None}, parents=(source_ref.payload_id,), epoch=0
            )
            assert await helper.read_in(conn, output_ref.payload_id, epoch=0) is None
            assert (
                await helper.put_call_result_in(
                    conn, "call", None, parents=(state_ref.payload_id,), epoch=0
                )
            ).payload_id == output_ref.payload_id
            row = {
                "status": "COMPLETED",
                "result_payload_id": output_ref.payload_id,
                "result_digest": output_ref.digest,
            }
            assert (await helper.hydrate_call_in(conn, row, epoch=0))["payload_state"] == "ACTIVE"
            with pytest.raises(ForestError) as error:
                await helper.hydrate_call_in(
                    conn, {**row, "result_digest": digest_json("wrong")}, epoch=0
                )
            assert error.value.code == "KERNEL_PAYLOAD_IDENTITY_CONFLICT"
        await helper.payloads.forget_sources(
            lease, "forget", (SourceRef("input-a"),), expected_deletion_epoch=0
        )
        await helper.payloads.scrub(lease)
        async with store.pool.connection() as conn, conn.transaction():
            await store._host(conn, lease)
            await helper.payloads.lock_in(conn)
            assert (await helper.hydrate_call_in(conn, row, epoch=1, required=False))[
                "payload_state"
            ] == "UNAVAILABLE"
            with pytest.raises(ForestError) as error:
                await helper.put_call_result_in(
                    conn, "call", None, parents=(state_ref.payload_id,), epoch=1
                )
            assert error.value.code == "KERNEL_CONTENT_REVOKED"
            assert await helper.read_in(conn, candidate.payload_id, epoch=1, required=False) is None
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=ANY(%s)",
                    (
                        [
                            input_ref.payload_id,
                            state_ref.payload_id,
                            output_ref.payload_id,
                            source_ref.payload_id,
                            candidate.payload_id,
                        ],
                    ),
                )
            ).fetchone()
    finally:
        await store.close()

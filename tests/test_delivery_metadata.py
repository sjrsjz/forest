"""Delivery metadata cannot become an undeletable nested body container."""

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_delivery import delivery as delivery
from test_delivery import enqueue, permit

from forest.storage.data_migrations.delivery_payloads_v1 import public_result

pytestmark = pytest.mark.postgres


async def test_receipt_metadata_shape_is_enforced_by_database_and_full_body_is_managed(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "sender")
    attempt = await permit(d, lease, claim)
    full = {
        "message_id": "123",
        "status": "UNKNOWN",
        "retcode": -1,
        "debug": {"body": "RECEIPT-CANARY"},
    }
    observed = await d.complete(attempt, "UNKNOWN", full)
    assert observed["receipt"] == {"message_id": "123", "status": "UNKNOWN", "retcode": -1}
    bad_values = [
        {"message_id": {"body": "CANARY"}},
        {"status": ["CANARY"]},
        {"status": "CANARY"},
        {"error_code": {"body": "CANARY"}},
        {"retcode": True},
        {"retcode": 0.5},
        {"retcode": 2**31},
        {"message_id": "x" * 257},
        {"message_id": None},
        {"error_code": "line\nbreak"},
        {"body": "CANARY"},
        [],
        "CANARY",
        None,
    ]
    async with d.store.pool.connection() as conn:
        row = await (
            await conn.execute("SELECT receipt_payload_id FROM public.bot_send_attempts")
        ).fetchone()
        async with conn.transaction():
            assert (
                await d.contents.read_receipt_in(
                    conn, row["receipt_payload_id"], expected_deletion_epoch=0
                )
                == full
            )
        for table in ("bot_send_parts", "bot_send_attempts"):
            for value in bad_values:
                with pytest.raises(psycopg.errors.CheckViolation):
                    async with conn.transaction():
                        await conn.execute(
                            sql.SQL("UPDATE public.{} SET receipt=%s").format(
                                sql.Identifier(table)
                            ),
                            (Jsonb(value),),
                        )
        assert (
            await (await conn.execute("SELECT receipt FROM public.bot_send_attempts")).fetchone()
        )["receipt"] == observed["receipt"]


async def test_resolution_metadata_has_closed_scalar_shape_and_legacy_projection_is_bounded(
    delivery,
):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "sender")
    observed = await d.complete(await permit(d, lease, claim), "UNKNOWN", {"status": "UNKNOWN"})
    result = await d.reconcile(
        lease,
        claim.intent_id,
        0,
        operation_id="valid",
        decision="SKIP",
        expected_revision=observed["revision"],
        reason="FULL-REASON-CANARY",
    )
    assert result["state"] == "SKIPPED" and result["receipt"] == {"status": "UNKNOWN"}
    corruptions = [
        {"intent_id": {"body": "CANARY"}},
        {"part_index": {"body": "CANARY"}},
        {"part_index": -1},
        {"part_index": True},
        {"revision": 0},
        {"revision": 2**63},
        {"attempt_count": 0.1},
        {"state": "CANARY"},
        {"attempt_id": ["CANARY"]},
        {"error_code": {"body": "CANARY"}},
        {"receipt": {"message_id": {"body": "CANARY"}}},
        {"receipt": "CANARY"},
        {"body": "CANARY"},
    ]
    async with d.store.pool.connection() as conn:
        for index, corrupted in enumerate(corruptions):
            candidate = {**result, **corrupted}
            with pytest.raises(psycopg.errors.CheckViolation):
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO public.bot_delivery_resolutions(bot_id,operation_id,intent_id,part_index,input_digest,decision,result_metadata) VALUES (%s,%s,%s,0,'digest','SKIP',%s)",
                        (
                            d.store.bot_id,
                            "invalid:" + str(index),
                            claim.intent_id,
                            Jsonb(candidate),
                        ),
                    )
            safe = public_result(candidate)
            assert (
                await (
                    await conn.execute(
                        "SELECT public.forest_delivery_result_metadata_valid(%s) AS valid",
                        (Jsonb(safe),),
                    )
                ).fetchone()
            )["valid"]
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_delivery_resolutions WHERE operation_id='valid'"
            )
        ).fetchone()
        assert row["result_metadata"] == result and row["reason"] is row["result"] is None
        async with conn.transaction():
            body = await d.contents.read_receipt_in(
                conn, row["payload_id"], expected_deletion_epoch=0
            )
            assert body["reason"] == "FULL-REASON-CANARY"
    assert public_result(None) == {}

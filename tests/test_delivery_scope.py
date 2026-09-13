"""Real delivery permits reject overflowing lineage before any HTTP handoff."""

import pytest
from test_delivery import delivery as delivery
from test_delivery import route
from test_onebot import response, socket_api

from forest.adapters.onebot import OneBotHTTPClient
from forest.delivery import DeliveryWorker, PreparedDelivery
from forest.memory_types import SourceRef
from forest.storage import payloads as payloads_module

pytestmark = pytest.mark.postgres


async def configuration_source_limit(delivery):
    async with delivery.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_bindings b "
                "JOIN public.forest_payload_effective_sources s "
                "ON s.bot_id=b.bot_id AND s.payload_id=b.config_payload_id "
                "WHERE b.bot_id=%s AND b.binding_id='binding-a'",
                (delivery.store.bot_id,),
            )
        ).fetchone()
    assert row["n"] > 0
    return row["n"]


@pytest.mark.parametrize("boundary", ["before_permit", "before_mark_started"])
async def test_lineage_overflow_is_definitely_unsent_and_does_not_block_following_delivery(
    delivery, monkeypatch, boundary
):
    d, lease = delivery
    source_limit = await configuration_source_limit(d)
    body = await d.contents.payloads.put_json(
        lease,
        owner_kind="context.parts",
        owner_id="overflow-source-input",
        value={"text": "SOURCE-CANARY"},
        sources=tuple(SourceRef(f"overflow-source-{index}") for index in range(source_limit + 1)),
        expected_deletion_epoch=0,
    )
    first = await d.enqueue(
        lease,
        operation_id="overflow",
        route=route(),
        parts=[[{"type": "text", "text": "UNSENT-CANARY"}]],
        conversation_id="conversation",
        response_seq=1,
        parents=(body.payload_id,),
    )
    # A separate host notification in the same order chain must remain usable.
    await d.enqueue(
        lease,
        operation_id="following",
        route=route(),
        parts=[[{"type": "text", "text": "FOLLOWING-NOTIFICATION"}]],
        conversation_id="conversation",
        response_seq=2,
    )
    started = d.mark_started

    async def lower_limit_before_start(permit):
        if permit.intent_id == first["intent_id"]:
            monkeypatch.setattr(payloads_module, "MAX_SOURCES", source_limit)
        await started(permit)

    async def reply(writer):
        writer.write(response({"status": "ok", "retcode": 0, "data": {"message_id": 123}}))
        await writer.drain()

    async with socket_api(reply) as (origin, requests, _):
        client = OneBotHTTPClient(origin, "private-test-token")

        async def prepare(claim):
            prepared = client.prepare(claim.route, claim.payload)
            if boundary == "before_permit" and claim.intent_id == first["intent_id"]:
                # Exercise the actual SQL source expansion with a smaller read
                # budget; neither repository method nor error is fabricated.
                monkeypatch.setattr(payloads_module, "MAX_SOURCES", source_limit)
            return PreparedDelivery(prepared, client.send, {"driver": "onebot-v11:1"})

        if boundary == "before_mark_started":
            monkeypatch.setattr(d, "mark_started", lower_limit_before_start)
        worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
        try:
            assert await worker.run_once()
            assert requests == []
            saved = next(
                row for row in await d.list(lease) if row["intent_id"] == first["intent_id"]
            )
            assert saved["state"] == "FAILED"
            async with d.store.pool.connection() as conn:
                attempts = await (
                    await conn.execute(
                        "SELECT state,started_at,receipt,receipt_payload_id FROM public.bot_send_attempts WHERE intent_id=%s",
                        (first["intent_id"],),
                    )
                ).fetchall()
            if boundary == "before_permit":
                assert saved["error_code"] == "SOURCE_LINK_SCOPE_LIMIT"
                assert attempts == []
            else:
                assert attempts == [
                    {
                        "state": "NOT_SENT",
                        "started_at": None,
                        "receipt": {"error_code": "DELIVERY_CONTENT_REMOVED"},
                        "receipt_payload_id": None,
                    }
                ]
            assert await worker.run_once()
            assert len(requests) == 1 and b"FOLLOWING-NOTIFICATION" in requests[0][2]
            assert b"UNSENT-CANARY" not in requests[0][2]
            assert not any(row["state"] == "UNKNOWN" for row in await d.list(lease))
            assert not await worker.run_once()
        finally:
            await worker.close()
            await client.aclose()

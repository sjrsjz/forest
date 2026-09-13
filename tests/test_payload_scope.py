"""Retained exact dependencies can exceed a reader's bounded source budget."""

import pytest
from test_delivery import delivery as delivery
from test_delivery import permit, route
from test_messages import messages as messages
from test_messages import owner, receive

from forest.memory_types import SourceRef
from forest.storage import payloads as payloads_module
from forest.storage.delivery import DeliveryStore
from forest.storage.input_payloads import InputContentStore

pytestmark = pytest.mark.postgres


async def test_overflow_keeps_input_identity_and_context_positions_without_exposing_body(
    messages, monkeypatch
):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, group="42", text="question", trigger=True)
    actor = await owner(repository, lease, first["conversation_id"])
    batch = await repository.select_input(actor, "initial")
    sender = DeliveryStore(repository.store)
    await repository.finalize_reply(
        actor, "answer", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "PRIVATE-OUTPUT"}]], delivery=sender,
    )
    claim = await sender.claim(lease, "sender")
    attempt = await permit(sender, lease, claim)
    await sender.mark_started(attempt)
    await sender.complete(attempt, "SENT", {"message_id": "999"})
    echo = await receive(repository, lease, 999, group="42", user="1000", text="PRIVATE-OUTPUT")
    independent = await receive(repository, lease, 1000, group="42", user="other", text="independent")
    await repository.select_input(actor, "drain")
    before = await repository.context(actor)
    contents = InputContentStore(repository.store)
    async with repository.store.pool.connection() as conn, conn.transaction():
        binding, _ = await repository._actor(conn, actor)
        original = await contents.read_input_in(conn, echo["event_id"], expected_deletion_epoch=0)
        assert original["payload_state"] == "ACTIVE"
        assert original["segments"] == [{"type": "text", "text": "PRIVATE-OUTPUT"}]
        await repository._actor_final(conn, actor, binding)
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    async with repository.store.pool.connection() as conn, conn.transaction():
        binding, _ = await repository._actor(conn, actor)
        blocked = await contents.read_input_in(conn, echo["event_id"], expected_deletion_epoch=0)
        kept = await contents.read_input_in(conn, independent["event_id"], expected_deletion_epoch=0)
        assert blocked["payload_state"] == "UNAVAILABLE"
        assert blocked["content_error"] == "SOURCE_LINK_SCOPE_LIMIT"
        assert blocked["author"]["principal_ref"] == original["author"]["principal_ref"]
        assert "PRIVATE-OUTPUT" not in str(blocked)
        assert kept["segments"] == [{"type": "text", "text": "independent"}]
        await repository._actor_final(conn, actor, binding)
    after = await repository.context(actor)
    assert "PRIVATE-OUTPUT" not in str(after)
    assert "independent" in str(after)
    assert [unit["unit_id"] for unit in after["units"]] == [
        unit["unit_id"] for unit in before["units"]
    ]


@pytest.mark.parametrize("outcome", ["SENT", "UNKNOWN"])
async def test_late_delivery_scope_overflow_preserves_effect_without_receipt_body(
    delivery, monkeypatch, outcome
):
    sender, lease = delivery
    source = await sender.contents.payloads.put_json(
        lease, owner_kind="context.parts", owner_id="two-inputs", value={"text": "question"},
        sources=(SourceRef("source-a"), SourceRef("source-b")), expected_deletion_epoch=0,
    )
    await sender.enqueue(
        lease, operation_id="send", route=route(), conversation_id="conversation",
        response_seq=1, parts=[[{"type": "text", "text": "answer"}]],
        parents=(source.payload_id,),
    )
    claim = await sender.claim(lease, "sender")
    attempt = await permit(sender, lease, claim)
    await sender.mark_started(attempt)
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    observed = await sender.complete(
        attempt, outcome, {"message_id": "999", "debug": {"body": "LATE-PRIVATE-RECEIPT"}},
    )
    assert observed["state"] == outcome
    assert observed["receipt"] == {"message_id": "999"}
    async with sender.store.pool.connection() as conn:
        fact = await (await conn.execute(
            "SELECT state,receipt_payload_id,receipt FROM public.bot_send_attempts"
        )).fetchone()
        assert fact == {"state": outcome, "receipt_payload_id": None, "receipt": {"message_id": "999"}}
        assert not await (await conn.execute(
            "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
            (b"LATE-PRIVATE-RECEIPT",),
        )).fetchone()

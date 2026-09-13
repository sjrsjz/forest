"""Canonical history survives erasure as positions, never as hidden old bodies."""

import json

import psycopg
import pytest
from test_delivery import permit as delivery_permit
from test_messages import event, owner, receive
from test_messages import messages as messages

from forest.storage.context_payloads import ContextPayloadStore
from forest.storage.delivery import DeliveryStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def rows(repository, query, parameters=()):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


async def forget_event(repository, lease, receipt, operation="forget", epoch=0):
    source = (await rows(repository,
        "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
        (receipt["event_id"],),
    ))[0]
    from forest.memory_types import SourceRef

    return await PayloadStore(repository.store).forget_sources(
        lease, operation, (SourceRef(MemorySourceStore(repository).unit_id(source["source_event_id"])),),
        expected_deletion_epoch=epoch,
    )


async def test_erased_reply_and_input_are_unavailable_in_next_run_without_losing_positions(messages):
    repository, lease, _ = messages
    first = event(1, text="SOURCE-CANARY-a", user="first-author")
    first["author"]["display_name"] = "DISPLAY-CANARY-a"
    receipt = await repository.ingest(lease, "binding-a", first, decision="TRIGGER")
    other = await receive(repository, lease, 2, group="group-other", text="independent-b")
    actor = await owner(repository, lease, receipt["conversation_id"])
    batch = await repository.select_input(actor, "first")
    reply = await repository.finalize_reply(
        actor, "answer", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "DERIVED-CANARY-a"}]],
        delivery=DeliveryStore(repository.store),
    )
    before = await repository.context(actor)
    assert "SOURCE-CANARY-a" in str(before) and "DERIVED-CANARY-a" in str(before)
    assert len(before["payload_ids"]) == 2
    facts = await rows(repository,
        "SELECT event_id,ingress_seq,input_digest,segments,payload_id FROM public.bot_message_inbox ORDER BY event_id",
    )
    assert all(row["segments"] is None and row["payload_id"] for row in facts)
    stored_reply = (await rows(repository,
        "SELECT reply_id,input_digest,response_seq,parts,payload_id FROM public.bot_conversation_replies",
    ))[0]
    assert stored_reply["parts"] is None and stored_reply["payload_id"] == reply["payload_id"]

    erased = await forget_event(repository, lease, receipt)
    assert erased["deletion_epoch"] == 1
    payloads = PayloadStore(repository.store)
    while (await payloads.scrub(lease))["remaining"]:
        pass
    after = await repository.context(actor)
    assert [row["unit_id"] for row in after["units"]] == [row["unit_id"] for row in before["units"]]
    assert after["context_applied_seq"] == before["context_applied_seq"]
    assert all(row["payload_state"] != "ACTIVE" for row in after["units"])
    assert after["payload_ids"] == []
    assert "CANARY" not in json.dumps(after, ensure_ascii=False, default=str)
    assert after["units"][0]["author"]["principal_ref"] == before["units"][0]["author"]["principal_ref"]
    assert (await repository.ingest(lease, "binding-a", first, decision="TRIGGER"))["duplicate"]
    repeated = await repository.finalize_reply(
        actor, "answer", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "DERIVED-CANARY-a"}]],
        delivery=DeliveryStore(repository.store),
    )
    assert repeated["reply_id"] == reply["reply_id"] and repeated["parts"] is None
    assert await rows(repository,
        "SELECT reply_id,input_digest,response_seq,parts,payload_id FROM public.bot_conversation_replies",
    ) == [stored_reply]
    assert await rows(repository,
        "SELECT event_id,ingress_seq,input_digest,segments,payload_id FROM public.bot_message_inbox ORDER BY event_id",
    ) == facts

    await receive(repository, lease, 3, text="new-trigger", user="second-author", trigger=True)
    await receive(repository, lease, 4, text="queued-tail")
    selection = await repository.select_input(actor, "next")
    view = await repository.context(actor, run_id=selection["run_id"])
    assert selection["run_id"] != batch["run_id"] and selection["selected_input_end_seq"] == 2
    assert "CANARY" not in str(view) and "queued-tail" not in str(view)
    assert "new-trigger" in str(view) and len(view["payload_ids"]) == 1
    other_actor = await owner(repository, lease, other["conversation_id"])
    await repository.select_input(other_actor, "other")
    assert "independent-b" in str(await repository.context(other_actor))


@pytest.mark.parametrize("forget_before_alias", [False, True])
async def test_control_echo_cannot_publish_deleted_command_content(messages, forget_before_alias):
    repository, lease, _ = messages
    command = await receive(repository, lease, 1, group="42", text="#forest help")
    delivery = DeliveryStore(repository.store)
    claim = await repository.claim_control(lease, command["command_id"], "core")
    await repository.complete_control(
        lease, claim, "control", reply_parts=[[{"type": "text", "text": "CONTROL-CANARY"}]],
        outcome={"code": "HELP"}, delivery=delivery,
    )
    sending = await delivery.claim(lease, "sender")
    permit = await delivery_permit(delivery, lease, sending)
    await delivery.mark_started(permit)
    await receive(repository, lease, 999, group="42", user="1000", text="CONTROL-CANARY")
    actor = await owner(repository, lease, command["conversation_id"])
    await repository.select_input(actor, "before-ack")
    await delivery.complete(permit, "SENT", {"message_id": "999"})
    if forget_before_alias:
        await forget_event(repository, lease, command)
    assert await repository.reconcile_echoes(lease) == 1
    await repository.select_input(actor, "alias")
    if not forget_before_alias:
        assert "CONTROL-CANARY" in str(await repository.context(actor))
        await forget_event(repository, lease, command)
    view = await repository.context(actor)
    assert [unit["kind"] for unit in view["units"]] == ["INPUT", "CONTROL"]
    assert "CONTROL-CANARY" not in str(view)
    control = view["units"][1]
    assert control["payload_state"] != "ACTIVE" and control["context_role"] == "control_data"
    assert control["control_command_id"] == command["command_id"]
    assert (await rows(repository,
        "SELECT control_parts,step_parts,payload_id FROM public.bot_context_units WHERE kind='CONTROL'",
    )) == [{"control_parts": None, "step_parts": None, "payload_id": control["payload_id"]}]


async def test_canonical_heads_reject_plaintext_writes_and_changed_payload_identity(messages):
    repository, lease, _ = messages
    receipt = await receive(repository, lease, 1, trigger=True)
    actor = await owner(repository, lease, receipt["conversation_id"])
    batch = await repository.select_input(actor, "start")
    reply = await repository.finalize_reply(
        actor, "answer", run_id=batch["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "original"}]], delivery=DeliveryStore(repository.store),
    )
    for statement in (
        "UPDATE public.bot_conversation_replies SET parts='[]'::jsonb",
        "UPDATE public.bot_context_units SET step_parts='[]'::jsonb WHERE kind='REPLY'",
        "UPDATE public.bot_context_units SET payload_id=NULL WHERE kind='REPLY'",
        "DELETE FROM public.bot_conversation_replies",
    ):
        async with repository.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error):
                await conn.execute(statement)
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        hydrated = await ContextPayloadStore(repository.store).read_reply_in(
            conn, reply, expected_deletion_epoch=0, required=True,
        )
    assert hydrated["parts"] == [[{"type": "text", "text": "original"}]]

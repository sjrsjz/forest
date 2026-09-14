"""Forgotten platform sources converge on immutable metadata-only receipts."""

import asyncio
import json

import psycopg
import pytest
from test_messages import event
from test_messages import messages as messages
from test_onebot import encoded
from test_onebot import event as onebot_event

from forest.adapters.onebot import parse_event
from forest.errors import ForestError
from forest.storage.input_payloads import InputContentStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def counts(repository):
    tables = (
        "bot_conversations",
        "bot_message_inbox",
        "bot_memory_source_units",
        "bot_memory_source_occurrences",
        "bot_inbound_media_refs",
        "bot_shell_requests",
        "bot_payload_headers",
        "bot_payload_bodies",
        "bot_context_units",
    )
    async with repository.store.pool.connection() as conn:
        return {
            table: (
                await (
                    await conn.execute(
                        f"SELECT count(*) AS count FROM public.{table} "
                        "WHERE bot_id=%s",
                        (repository.store.bot_id,),
                    )
                ).fetchone()
            )["count"]
            for table in tables
        }


async def forget(repository, lease, source_event_id, operation_id):
    payloads = PayloadStore(repository.store)
    source = InputContentStore(repository.store).source_ref(source_event_id)
    return await payloads.forget_sources(
        lease,
        operation_id,
        (source,),
        expected_deletion_epoch=await payloads.epoch(lease),
    )


async def test_concurrent_late_occurrences_have_one_closed_receipt(messages):
    repository, lease, _ = messages
    first = await repository.ingest(
        lease, "binding-a", event(1), decision="NO_TRIGGER"
    )
    async with repository.store.pool.connection() as conn:
        inbox = await (
            await conn.execute(
                "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
                (first["event_id"],),
            )
        ).fetchone()
    await forget(repository, lease, inbox["source_event_id"], "forget-source-one")
    before = await counts(repository)

    late = event(1)
    late["thread_id"] = "late-thread"
    results = await asyncio.gather(
        *(
            repository.ingest(
                lease, "binding-a", late, decision="TRIGGER"
            )
            for _ in range(8)
        )
    )
    receipt_ids = {item["receipt_id"] for item in results}
    assert len(receipt_ids) == 1
    assert {item["kind"] for item in results} == {"SUPPRESSED_OCCURRENCE"}
    assert sum(not item["duplicate"] for item in results) == 1
    assert await counts(repository) == before

    changed = {**late, "segments": [{"type": "text", "text": "changed secret"}]}
    with pytest.raises(ForestError) as caught:
        await repository.ingest(
            lease, "binding-a", changed, decision="NO_TRIGGER"
        )
    assert caught.value.code == "INGEST_CONFLICT"

    async with repository.store.pool.connection() as conn:
        receipts = await (
            await conn.execute(
                "SELECT * FROM public.bot_suppressed_input_occurrences"
            )
        ).fetchall()
        columns = await (
            await conn.execute(
                "SELECT column_name,data_type FROM information_schema.columns "
                "WHERE table_schema='public' "
                "AND table_name='bot_suppressed_input_occurrences'"
            )
        ).fetchall()
    assert len(receipts) == 1
    names = {column["column_name"] for column in columns}
    assert not names & {
        "author",
        "display_name",
        "content",
        "segments",
        "parts",
        "media_ref",
        "uri",
        "raw",
        "payload",
        "arguments",
    }
    assert not {column["data_type"] for column in columns} & {"json", "jsonb"}
    assert "changed secret" not in json.dumps(receipts, default=str)

    async with repository.store.pool.connection() as conn:
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState,
            match="SUPPRESSED_OCCURRENCE_PROOF_INVALID",
        ):
            await conn.execute(
                "INSERT INTO public.bot_suppressed_input_occurrences "
                "SELECT bot_id,receipt_id||'-forged',event_id||'-forged',"
                "occurrence_id||'-forged',source_event_id||'-forged',"
                "%s,conversation_id||'-forged',binding_id,first_binding_epoch,"
                "platform,channel_kind,channel_id,thread_id,platform_message_id,"
                "input_digest,observed_at,suppression_source_unit_id,"
                "suppression_part_id,suppression_operation_id,"
                "suppression_deletion_epoch,accepted_at "
                "FROM public.bot_suppressed_input_occurrences",
                ("source-unit:sha256:" + "0" * 64,),
            )
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute(
                "UPDATE public.bot_suppressed_input_occurrences "
                "SET accepted_at=clock_timestamp()"
            )


async def test_forgotten_normal_inbox_replay_never_reads_or_restores_body(messages):
    repository, lease, _ = messages
    original = event(2)
    first = await repository.ingest(
        lease, "binding-a", original, decision="NO_TRIGGER"
    )
    async with repository.store.pool.connection() as conn:
        inbox = await (
            await conn.execute(
                "SELECT source_event_id,payload_id FROM public.bot_message_inbox "
                "WHERE event_id=%s",
                (first["event_id"],),
            )
        ).fetchone()
    await forget(repository, lease, inbox["source_event_id"], "forget-source-two")
    payloads = PayloadStore(repository.store)
    while (await payloads.scrub(lease))["remaining"]:
        pass
    before = await counts(repository)
    duplicate = await repository.ingest(
        lease, "binding-a", original, decision="NO_TRIGGER"
    )
    assert duplicate["duplicate"] and duplicate["event_id"] == first["event_id"]
    assert await counts(repository) == before
    async with repository.store.pool.connection() as conn:
        assert not await (
            await conn.execute(
                "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
                (inbox["payload_id"],),
            )
        ).fetchone()


async def test_stage_and_final_ingest_each_close_the_forget_race(messages):
    repository, lease, _ = messages
    body = encoded(
        onebot_event(
            self_id=1000,
            group_id=30001,
            message_id=91,
            user_id=20001,
            sender={"user_id": 20001, "nickname": "late user", "role": "member"},
            message=[{"type": "text", "data": {"text": "never revive"}}],
        )
    )
    parsed = repository._normalized(parse_event(body, self_id="1000").as_dict())
    source_event_id = repository.platform_source_event_id(parsed)
    async with repository.store.pool.connection() as conn:
        binding = await (
            await conn.execute(
                "SELECT binding_epoch FROM public.bot_bindings "
                "WHERE bot_id=%s AND binding_id='binding-a'",
                (repository.store.bot_id,),
            )
        ).fetchone()

    staged = await repository.stage_platform_input(
        lease,
        "binding-a",
        body,
        expected_binding_epoch=binding["binding_epoch"],
    )
    assert staged.kind == "NEW" and staged.origin_payload_id
    await forget(repository, lease, source_event_id, "forget-staged-source")
    receipt = await repository.ingest(
        lease, "binding-a", staged.event, decision="TRIGGER"
    )
    assert receipt["kind"] == "SUPPRESSED_OCCURRENCE"
    assert not receipt["duplicate"]

    payloads = PayloadStore(repository.store)
    while (await payloads.scrub(lease))["remaining"]:
        pass
    async with repository.store.pool.connection() as conn:
        header = await (
            await conn.execute(
                "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
                (staged.origin_payload_id,),
            )
        ).fetchone()
        assert header["state"] == "SCRUBBED"
        assert not await (
            await conn.execute(
                "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
                (staged.origin_payload_id,),
            )
        ).fetchone()
        assert not await (
            await conn.execute(
                "SELECT 1 FROM public.bot_message_inbox WHERE event_id=%s",
                (receipt["event_id"],),
            )
        ).fetchone()
        assert not await (
            await conn.execute(
                "SELECT 1 FROM public.bot_conversations WHERE conversation_id=%s",
                (repository._platform_ids("binding-a", parsed)["conversation_id"],),
            )
        ).fetchone()

    other_body = encoded(
        onebot_event(
            self_id=1000,
            group_id=30001,
            message_id=92,
            user_id=20001,
            sender={"user_id": 20001, "nickname": "late user", "role": "member"},
            message=[{"type": "text", "data": {"text": "also never revive"}}],
        )
    )
    other = repository._normalized(parse_event(other_body, self_id="1000").as_dict())
    await forget(
        repository,
        lease,
        repository.platform_source_event_id(other),
        "forget-before-stage",
    )
    before = await counts(repository)
    suppressed = await repository.stage_platform_input(
        lease,
        "binding-a",
        other_body,
        expected_binding_epoch=binding["binding_epoch"],
    )
    replay = await repository.stage_platform_input(
        lease,
        "binding-a",
        other_body,
        expected_binding_epoch=binding["binding_epoch"],
    )
    assert suppressed.kind == replay.kind == "SUPPRESSED_OCCURRENCE"
    assert suppressed.receipt["receipt_id"] == replay.receipt["receipt_id"]
    assert not suppressed.receipt["duplicate"] and replay.receipt["duplicate"]
    assert await counts(repository) == before

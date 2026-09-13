"""Exact platform receipt aliases carry deletion to copies made before discovery."""

import pytest
from legacy_022 import DeliveryStore as FrozenDeliveryStore
from legacy_022 import FrozenPayloadStore
from legacy_022 import MessageStore as FrozenMessageStore
from psycopg.types.json import Jsonb
from test_agent_store import prepare as model_prepare
from test_agent_store import respond as model_respond
from test_agent_store import specification, start
from test_delivery import permit as delivery_permit
from test_memory_pipeline_upgrade import old_schema
from test_messages import messages as messages
from test_messages import owner, receive
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage import payloads as payloads_module
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore
from forest.storage.input_payloads import InputContentStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


async def rows(repository, query, parameters=()):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


@pytest.mark.parametrize("confirm_before_forget", [False, True])
async def test_exact_echo_and_early_copy_follow_original_output_deletion(
    messages, confirm_before_forget
):
    repository, lease, _ = messages
    original = await receive(
        repository, lease, 1, group="42", text="original question", trigger=True
    )
    actor = await owner(repository, lease, original["conversation_id"])
    batch = await repository.select_input(actor, "initial")
    delivery = DeliveryStore(repository.store)
    await repository.finalize_reply(
        actor,
        "answer",
        run_id=batch["run_id"],
        expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "ECHO-PRIVATE-CANARY"}]],
        delivery=delivery,
    )
    claim = await delivery.claim(lease, "sender")
    permit = await delivery_permit(delivery, lease, claim)
    await delivery.mark_started(permit)
    echo = await receive(
        repository, lease, 999, group="42", user="1000", text="ECHO-PRIVATE-CANARY"
    )
    independent = await receive(
        repository, lease, 1000, group="42", user="another-user", text="ECHO-PRIVATE-CANARY"
    )
    await repository.select_input(actor, "drain-echo")
    contents = InputContentStore(repository.store)
    async with repository.store.pool.connection() as conn, conn.transaction():
        binding, _ = await repository._actor(conn, actor)
        view = await repository._context_in(conn, actor)
        echo_unit = next(unit for unit in view["units"] if unit["event_id"] == echo["event_id"])
        early = await contents.payloads.put_json_in(
            conn,
            owner_kind="model.snapshot",
            owner_id="early-echo-projection",
            value={
                "unit_id": echo_unit["unit_id"],
                "source": echo_unit["source"],
                "author": echo_unit["author"],
                "parts": echo_unit["segments"],
            },
            parents=(echo_unit["payload_id"],),
            expected_deletion_epoch=0,
        )
        await repository._actor_final(conn, actor, binding)
    before = await rows(
        repository,
        "SELECT payload_id,manifest,input_digest FROM public.bot_payload_headers ORDER BY payload_id",
    )
    source = (
        await rows(
            repository,
            "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
            (original["event_id"],),
        )
    )[0]
    if confirm_before_forget:
        await delivery.complete(permit, "SENT", {"message_id": "999"})
        assert await repository.reconcile_exact_echo(lease, echo["event_id"])
    await contents.payloads.forget_sources(
        lease,
        "forget-original",
        (contents.source_ref(source["source_event_id"]),),
        expected_deletion_epoch=0,
    )
    old_request = (
        await rows(
            repository,
            "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='forget-original'",
        )
    )[0]
    if not confirm_before_forget:
        await delivery.complete(permit, "SENT", {"message_id": "999"})
        assert await repository.reconcile_exact_echo(lease, echo["event_id"])
    assert not await repository.reconcile_exact_echo(lease, echo["event_id"])
    while (await contents.payloads.scrub(lease))["remaining"]:
        pass
    async with repository.store.pool.connection() as conn, conn.transaction():
        clock = await contents.payloads.lock_in(conn)
        erased = await contents.read_input_in(
            conn, echo["event_id"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        assert erased["payload_state"] == "UNAVAILABLE"
        kept = await contents.read_input_in(
            conn,
            independent["event_id"],
            expected_deletion_epoch=clock["deletion_epoch"],
            required=True,
        )
        assert kept["segments"] == [{"type": "text", "text": "ECHO-PRIVATE-CANARY"}]
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
        (early.payload_id,),
    )
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_memory_source_occurrences o JOIN public.bot_memory_source_units u USING(bot_id,source_unit_id) JOIN public.bot_payload_bodies b USING(bot_id,payload_id) WHERE o.event_id=%s",
        (echo["event_id"],),
    )
    after = {
        row["payload_id"]: row
        for row in await rows(
            repository,
            "SELECT payload_id,manifest,input_digest FROM public.bot_payload_headers ORDER BY payload_id",
        )
    }
    assert all(after[row["payload_id"]] == row for row in before)
    assert (
        await rows(
            repository,
            "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='forget-original'",
        )
    )[0] == old_request


async def answer(repository, lease, actor, *, operation, text, delivery):
    batch = await repository.select_input(actor, operation + ":input")
    return await repository.finalize_reply(
        actor,
        operation,
        run_id=batch["run_id"],
        expected_input_revision=batch["input_revision"],
        reply_parts=[[{"type": "text", "text": text}]],
        delivery=delivery,
    )


async def test_already_sent_late_echo_is_revoked_in_its_admission_transaction(messages):
    repository, lease, _ = messages
    original = await receive(repository, lease, 1, group="42", trigger=True)
    actor = await owner(repository, lease, original["conversation_id"])
    delivery = DeliveryStore(repository.store)
    await answer(repository, lease, actor, operation="answer", text="LATE-COPY", delivery=delivery)
    claim = await delivery.claim(lease, "sender")
    permit = await delivery_permit(delivery, lease, claim)
    await delivery.mark_started(permit)
    await delivery.complete(permit, "SENT", {"message_id": "999"})
    contents = InputContentStore(repository.store)
    original_row = (
        await rows(
            repository,
            "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
            (original["event_id"],),
        )
    )[0]
    await contents.payloads.forget_sources(
        lease,
        "forget",
        (contents.source_ref(original_row["source_event_id"]),),
        expected_deletion_epoch=0,
    )
    echo = await receive(repository, lease, 999, group="42", user="1000", text="LATE-COPY")
    # No scanner/Actor/reconciliation call is allowed before this assertion.
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        clock = await contents.payloads.lock_in(conn)
        value = await contents.read_input_in(
            conn, echo["event_id"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        assert value["origin"] == "chat_output" and value["payload_state"] == "UNAVAILABLE"
        assert clock["deletion_epoch"] == 2
    assert not await repository.reconcile_exact_echo(lease, echo["event_id"])
    assert (await receive(repository, lease, 999, group="42", user="1000", text="LATE-COPY"))[
        "duplicate"
    ]
    audit = await rows(
        repository,
        "SELECT details FROM public.bot_audit_records WHERE action='memory.exact_echo_revoked'",
    )
    assert len(audit) == 1 and audit[0]["details"]["cause_operation_id"] == "forget"
    assert "LATE-COPY" not in str(audit)
    while (await contents.payloads.scrub(lease))["remaining"]:
        pass
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
        (b"LATE-COPY",),
    )


async def test_real_echo_chain_is_transitive_and_capacity_failure_rolls_back(messages, monkeypatch):
    repository, lease, _ = messages
    original = await receive(repository, lease, 1, group="42", trigger=True)
    actor = await owner(repository, lease, original["conversation_id"])
    delivery = DeliveryStore(repository.store)
    await answer(
        repository, lease, actor, operation="first", text="first output", delivery=delivery
    )
    first_claim = await delivery.claim(lease, "sender")
    first_permit = await delivery_permit(delivery, lease, first_claim)
    await delivery.mark_started(first_permit)
    first_echo = await receive(repository, lease, 999, group="42", user="1000", text="first output")
    await receive(
        repository, lease, 2, group="42", trigger=True, text="describe the visible output"
    )
    await answer(
        repository, lease, actor, operation="second", text="second output", delivery=delivery
    )
    await delivery.complete(first_permit, "SENT", {"message_id": "999"})
    assert await repository.reconcile_exact_echo(lease, first_echo["event_id"])
    second_claim = await delivery.claim(lease, "sender")
    second_permit = await delivery_permit(delivery, lease, second_claim)
    await delivery.mark_started(second_permit)
    await delivery.complete(second_permit, "SENT", {"message_id": "1001"})
    second_echo = await receive(
        repository, lease, 1001, group="42", user="1000", text="second output"
    )
    ids = {
        r["event_id"]: r["source_unit_id"]
        for r in await rows(
            repository, "SELECT event_id,source_unit_id FROM public.bot_memory_source_occurrences"
        )
    }
    first_source, second_source = ids[first_echo["event_id"]], ids[second_echo["event_id"]]
    async with repository.store.pool.connection() as conn:
        closure = await (
            await conn.execute(
                "SELECT * FROM public.forest_source_dependent_closure(%s,%s,4096)",
                (
                    repository.store.bot_id,
                    Jsonb([{"source_unit_id": first_source, "part_id": "0"}]),
                ),
            )
        ).fetchall()
    # The target part intersects the whole-input dependency on the first echo;
    # only the derived echo expands to whole-source. No textual matching occurs.
    assert {tuple(item.values()) for item in closure} == {(first_source, "0"), (second_source, "*")}
    contents = InputContentStore(repository.store)
    with monkeypatch.context() as patch:
        patch.setattr(payloads_module, "MAX_SOURCES", 1)
        with pytest.raises(ForestError) as exc:
            await contents.payloads.put_json(
                lease,
                owner_kind="model.snapshot",
                owner_id="must-not-admit",
                value={"text": "second output"},
                sources=(SourceRef(second_source),),
                expected_deletion_epoch=0,
            )
        assert exc.value.code == "SOURCE_LINK_SCOPE_LIMIT"
    assert not await rows(
        repository, "SELECT 1 FROM public.bot_payload_headers WHERE owner_id='must-not-admit'"
    )
    await contents.payloads.forget_sources(
        lease,
        "erase-first-echo",
        (SourceRef(first_source, part_id="0"),),
        expected_deletion_epoch=0,
    )
    while (await contents.payloads.scrub(lease))["remaining"]:
        pass
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_message_inbox i JOIN public.bot_payload_bodies b USING(bot_id,payload_id) WHERE i.event_id=%s",
        (second_echo["event_id"],),
    )


@pytest.mark.parametrize("suppressed_before_upgrade", [False, True])
async def test_released_exact_alias_upgrade_preserves_manifests_and_old_forget_facts(
    upgrade_database, suppressed_before_upgrade
):
    await old_schema(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        lease = await store.acquire_host("old-host", "old-incarnation", ttl_seconds=120)
        async with store.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,host_incarnation,host_owner_epoch,enabled,receive_enabled,state) VALUES ('bot-a','binding-a','onebot-v11','1000','old bot','old-binding',%s,%s,true,true,'ONLINE')",
                (lease.incarnation, lease.owner_epoch),
            )
        repository = FrozenMessageStore(store)
        delivery = FrozenDeliveryStore(store)
        payloads = FrozenPayloadStore(store)
        contents = InputContentStore(store)  # Pure deterministic source identity only in old phase.
        original = await receive(repository, lease, 1, group="42", trigger=True)
        actor = await owner(repository, lease, original["conversation_id"])
        await answer(
            repository,
            lease,
            actor,
            operation="old-answer",
            text="OLD-ECHO-CANARY",
            delivery=delivery,
        )
        claim = await delivery.claim(lease, "old-sender")
        permit = await delivery_permit(delivery, lease, claim)
        await delivery.mark_started(permit)
        echo = await receive(
            repository, lease, 999, group="42", user="1000", text="OLD-ECHO-CANARY"
        )
        await delivery.complete(permit, "SENT", {"message_id": "999"})
        assert await repository.reconcile_exact_echo(lease, echo["event_id"])
        source = (
            await rows(
                repository,
                "SELECT * FROM public.bot_memory_source_occurrences WHERE event_id=%s",
                (echo["event_id"],),
            )
        )[0]
        early = await payloads.put_json(
            lease,
            owner_kind="model.snapshot",
            owner_id="old-early-copy",
            value={"text": "OLD-ECHO-CANARY"},
            sources=(SourceRef(source["source_unit_id"]),),
            expected_deletion_epoch=0,
        )
        old_headers = await rows(
            repository,
            "SELECT payload_id,manifest,input_digest FROM public.bot_payload_headers ORDER BY payload_id",
        )
        original_row = (
            await rows(
                repository,
                "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
                (original["event_id"],),
            )
        )[0]
        target = contents.source_ref(original_row["source_event_id"])
        if suppressed_before_upgrade:
            await payloads.forget_sources(lease, "old-forget", (target,), expected_deletion_epoch=0)
        old_requests = await rows(
            repository, "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
        )
        old_alias = await rows(repository, "SELECT * FROM public.bot_message_echo_aliases")
        await repository.release_actor(actor)
        await store.release_host(lease)
        await store.migrate()
        new_headers = {
            r["payload_id"]: r
            for r in await rows(
                repository,
                "SELECT payload_id,manifest,input_digest FROM public.bot_payload_headers",
            )
        }
        assert all(new_headers[r["payload_id"]] == r for r in old_headers)
        new_requests = {
            r["operation_id"]: r
            for r in await rows(repository, "SELECT * FROM public.bot_payload_forget_requests")
        }
        assert all(new_requests[r["operation_id"]] == r for r in old_requests)
        assert await rows(repository, "SELECT * FROM public.bot_message_echo_aliases") == old_alias
        links = await rows(repository, "SELECT * FROM public.bot_payload_source_links")
        assert len(links) == 1 and links[0]["event_id"] == echo["event_id"]
        repository = MessageStore(store)
        contents = InputContentStore(store)
        lease = await store.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
        clock = (await rows(repository, "SELECT * FROM public.bot_memory_clock"))[0]
        if not suppressed_before_upgrade:
            assert await contents.payloads.read_json(
                lease, early.payload_id, expected_deletion_epoch=clock["deletion_epoch"]
            ) == {"text": "OLD-ECHO-CANARY"}
            await contents.payloads.forget_sources(
                lease, "new-forget", (target,), expected_deletion_epoch=clock["deletion_epoch"]
            )
        while (await contents.payloads.scrub(lease))["remaining"]:
            pass
        assert not await rows(
            repository,
            "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
            (early.payload_id,),
        )
        before = await rows(
            repository, "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
        )
        await store.migrate()  # No-op upgrade preserves the live host and all decisions.
        assert (
            await rows(
                repository, "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
            )
            == before
        )
        assert await rows(repository, "SELECT * FROM public.bot_payload_source_links") == links
    finally:
        await store.close()


@pytest.mark.parametrize("delete_before_first_alias", [False, True])
async def test_full_size_parent_proof_survives_and_late_revocation_propagates(
    messages, delete_before_first_alias
):
    repository, lease, _ = messages
    state, actor, head = await start(messages, group="42")

    class BroadHostDelivery(DeliveryStore):
        async def enqueue_in(self, conn, lease, **kwargs):
            # Synthetic trusted-host provenance exercises actual PostgreSQL
            # capacity, not semantic quality or end-user authorization. The
            # formal Agent output contributes its two real input/reply refs.
            kwargs["sources"] = tuple(SourceRef("prior-host-source:" + str(i)) for i in range(4094))
            return await super().enqueue_in(conn, lease, **kwargs)

    broad = BroadHostDelivery(repository.store)
    _, request, model_permit = await model_prepare(state, actor, head)
    await model_respond(state, actor, model_permit, request, text="wide first output")
    await state.commit_final(actor, head["run_id"], "wide-first-reply", broad)
    first = await delivery_permit(broad, lease, await broad.claim(lease, "sender"))
    await broad.mark_started(first)
    first_echo = await receive(
        repository, lease, 999, group="42", user="1000", text="wide first output"
    )
    await receive(repository, lease, 2, group="42", trigger=True, text="remember that output")
    batch = await repository.select_input(actor, "next-run")
    head = await state.ensure_run(actor, batch["run_id"], specification())
    _, request, model_permit = await model_prepare(state, actor, head)
    await model_respond(state, actor, model_permit, request, text="derived second output")
    delivery = DeliveryStore(repository.store)
    await state.commit_final(actor, head["run_id"], "second-reply", delivery)
    assert not await rows(
        repository, "SELECT 1 FROM public.bot_memory_source_units WHERE NOT lineage_complete"
    )
    await delivery.complete(first, "SENT", {"message_id": "999"})
    second = await delivery_permit(delivery, lease, await delivery.claim(lease, "sender"))
    await delivery.mark_started(second)
    await delivery.complete(second, "SENT", {"message_id": "1001"})
    contents = InputContentStore(repository.store)
    original_manifests = await rows(
        repository, "SELECT payload_id,manifest,input_digest FROM public.bot_payload_headers"
    )
    if not delete_before_first_alias:
        assert await repository.reconcile_exact_echo(lease, first_echo["event_id"])
        async with repository.store.pool.connection() as conn:
            closure = await (
                await conn.execute(
                    "SELECT d.* FROM public.bot_send_attempts a JOIN public.bot_payload_headers h ON h.bot_id=a.bot_id AND h.payload_id=a.wire_payload_id CROSS JOIN LATERAL public.forest_source_dependency_closure(a.bot_id,h.manifest->'sources',5000) d WHERE a.attempt_id=%s",
                    (second.attempt_id,),
                )
            ).fetchall()
            assert len(closure) > 4096
    second_echo = await receive(
        repository, lease, 1001, group="42", user="1000", text="derived second output"
    )
    # Even an already oversized dynamic parent cannot roll back this real proof.
    assert await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_source_links WHERE event_id=%s",
        (second_echo["event_id"],),
    )
    early = await rows(
        repository,
        "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s",
        (second_echo["event_id"],),
    )
    if not delete_before_first_alias:
        with pytest.raises(ForestError) as exc:
            await contents.payloads.read_json(
                lease, early[0]["payload_id"], expected_deletion_epoch=0
            )
        assert exc.value.code == "SOURCE_LINK_SCOPE_LIMIT"
    await contents.payloads.forget_sources(
        lease, "forget-host-source", (SourceRef("prior-host-source:0"),), expected_deletion_epoch=0
    )
    old_decision = (
        await rows(
            repository,
            "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='forget-host-source'",
        )
    )[0]
    if delete_before_first_alias:
        # The second link was confirmed while its parent was still ACTIVE.
        # Discovering the first now revokes that parent but does not recursively
        # touch every descendant in the input transaction.
        assert await repository.reconcile_exact_echo(lease, first_echo["event_id"])
        assert (
            await rows(
                repository,
                "SELECT h.state FROM public.bot_message_inbox i JOIN public.bot_payload_headers h USING(bot_id,payload_id) WHERE i.event_id=%s",
                (second_echo["event_id"],),
            )
        )[0]["state"] == "ACTIVE"
        clock = (await rows(repository, "SELECT deletion_epoch FROM public.bot_memory_clock"))[0]
        with pytest.raises(ForestError):
            await contents.payloads.read_json(
                lease, early[0]["payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
            )
    iterations = 0
    while True:
        result = await contents.payloads.scrub(lease, limit=1)
        iterations += 1
        if not result["remaining"]:
            break
        assert iterations < 200
    assert not await rows(
        repository,
        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
        (early[0]["payload_id"],),
    )
    assert (
        await rows(
            repository,
            "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='forget-host-source'",
        )
    )[0] == old_decision
    after = {
        r["payload_id"]: r
        for r in await rows(
            repository, "SELECT payload_id,manifest,input_digest FROM public.bot_payload_headers"
        )
    }
    assert all(after[r["payload_id"]] == r for r in original_manifests)

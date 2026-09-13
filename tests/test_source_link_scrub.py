"""Late exact output dependencies survive bounded cleanup and host restart."""

import pytest
from test_agent_store import prepare as model_prepare
from test_agent_store import respond as model_respond
from test_agent_store import specification
from test_delivery import permit as delivery_permit
from test_echo_payloads import rows
from test_messages import messages as messages
from test_messages import owner, receive

from forest.errors import ForestError
from forest.storage.agent import AgentStore
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore
from forest.storage.input_payloads import InputContentStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def pending(repository):
    return (
        await rows(
            repository,
            "SELECT count(*) AS n FROM public.bot_payload_source_links l "
            "JOIN public.bot_payload_headers h ON h.bot_id=l.bot_id AND h.payload_id=l.parent_payload_id "
            "WHERE h.state IN ('REVOKED','SCRUBBED') AND NOT EXISTS(SELECT 1 FROM public.bot_payload_suppression s "
            "WHERE s.bot_id=l.bot_id AND s.source_unit_id=l.dependent_source_unit_id AND s.part_id='*')",
        )
    )[0]["n"]


async def forks(messages, count):
    repository, lease, _ = messages
    original = await receive(repository, lease, 1, group="42", trigger=True)
    actor = await owner(repository, lease, original["conversation_id"])
    delivery = DeliveryStore(repository.store)
    state = AgentStore(repository)

    async def answer(batch, operation, text):
        head = await state.ensure_run(actor, batch["run_id"], specification())
        snapshot, request, model_permit = await model_prepare(state, actor, head)
        await model_respond(state, actor, model_permit, request, text=text)
        await state.commit_final(actor, head["run_id"], operation, delivery)
        return snapshot

    batch = await repository.select_input(actor, "root-input")
    await answer(batch, "root", "root echo")
    claim = await delivery.claim(lease, "sender")
    permit = await delivery_permit(delivery, lease, claim)
    await delivery.mark_started(permit)
    early = await receive(repository, lease, 999, group="42", user="1000", text="root echo")
    await repository.select_input(actor, "drain-root-echo")
    await delivery.complete(permit, "SENT", {"message_id": "999"})
    contents = InputContentStore(repository.store)
    copies = []
    for index in range(count):
        trigger = await receive(repository, lease, 10 + index, group="42", trigger=True)
        batch = await repository.select_input(actor, f"branch-{index}")
        # Isolate the real earlier echo as this branch's only old visible input.
        # This is the same legal visibility change as local context compaction;
        # original payload references, bodies, cuts, and send facts stay fixed.
        async with repository.store.pool.connection() as conn:
            retained = await (
                await conn.execute(
                    "SELECT unit_id,event_id FROM public.bot_context_units "
                    "WHERE conversation_id=%s AND event_id=ANY(%s)",
                    (actor.conversation_id, [early["event_id"], trigger["event_id"]]),
                )
            ).fetchall()
            assert {row["event_id"] for row in retained} == {early["event_id"], trigger["event_id"]}
            await conn.execute(
                "UPDATE public.bot_context_units SET visible=false WHERE conversation_id=%s "
                "AND unit_id<>ALL(%s)",
                (actor.conversation_id, [row["unit_id"] for row in retained]),
            )
        snapshot = await answer(batch, f"branch-answer-{index}", f"branch echo {index}")
        assert {u["unit_id"] for u in snapshot["units"]} == {row["unit_id"] for row in retained}
        claim = await delivery.claim(lease, "sender")
        permit = await delivery_permit(delivery, lease, claim)
        await delivery.mark_started(permit)
        await delivery.complete(permit, "SENT", {"message_id": str(1001 + index)})
        echo = await receive(
            repository, lease, 1001 + index, group="42", user="1000", text=f"branch echo {index}"
        )
        assert await rows(
            repository,
            "SELECT 1 FROM public.bot_payload_source_links WHERE event_id=%s",
            (echo["event_id"],),
        )
        body = (
            await rows(
                repository,
                "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s",
                (echo["event_id"],),
            )
        )[0]
        copies.append(
            await contents.payloads.put_json(
                lease,
                owner_kind="model.snapshot",
                owner_id=f"copy-{index}",
                value={"text": f"BRANCH-CANARY-{index}"},
                parents=(body["payload_id"],),
                expected_deletion_epoch=0,
            )
        )
    assert not await rows(
        repository, "SELECT 1 FROM public.bot_memory_source_units WHERE NOT lineage_complete"
    )
    assert await pending(repository) == 0
    original_row = (
        await rows(
            repository,
            "SELECT source_event_id FROM public.bot_message_inbox WHERE event_id=%s",
            (original["event_id"],),
        )
    )[0]
    await contents.payloads.forget_sources(
        lease,
        "original-forget",
        (contents.source_ref(original_row["source_event_id"]),),
        expected_deletion_epoch=0,
    )
    # Branch wire manifests contain the early echo, not the clipped old source.
    assert await pending(repository) == 0
    assert await repository.reconcile_exact_echo(lease, early["event_id"])
    assert await pending(repository) == count
    for copy in copies:
        assert (
            await rows(
                repository,
                "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
                (copy.payload_id,),
            )
        )[0]["state"] == "ACTIVE"
    return repository, lease, copies


async def test_scrub_counts_pending_links_and_new_host_finishes_each_branch(messages):
    repository, lease, copies = await forks(messages, 3)
    payloads = PayloadStore(repository.store)
    original = (
        await rows(
            repository,
            "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='original-forget'",
        )
    )[0]
    links = await rows(
        repository, "SELECT * FROM public.bot_payload_source_links ORDER BY event_id"
    )
    batch = await payloads.scrub(lease, limit=1)
    assert batch["reconciled_links"] == 1 and batch["pending_links"] == 2
    revoked = (
        await rows(
            repository, "SELECT count(*) AS n FROM public.bot_payload_headers WHERE state='REVOKED'"
        )
    )[0]["n"]
    assert batch["remaining"] == revoked + 2
    await repository.store.release_host(lease)
    successor = BotStore(messages[2], repository.store.bot_id)
    await successor.open()
    try:
        await successor.migrate()
        new = await successor.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
        resumed = PayloadStore(successor)
        results = []
        for _ in range(100):
            result = await resumed.scrub(new, limit=1)
            results.append(result)
            assert result["removed"] <= 1 and result["reconciled_links"] <= 1
            if result["remaining"] == 0:
                break
        else:
            pytest.fail("Bounded cleanup did not converge after restart")
        assert sum(item["reconciled_links"] for item in results) == 2
        assert await pending(repository) == 0
        for copy in copies:
            assert not await rows(
                repository,
                "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
                (copy.payload_id,),
            )
        assert links == await rows(
            repository, "SELECT * FROM public.bot_payload_source_links ORDER BY event_id"
        )
        assert (
            original
            == (
                await rows(
                    repository,
                    "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='original-forget'",
                )
            )[0]
        )
        assert (await resumed.scrub(new))["remaining"] == 0
    finally:
        await successor.close()


async def test_failed_final_scrub_fence_rolls_back_compensation_and_physical_delete(
    messages, monkeypatch
):
    repository, lease, _ = await forks(messages, 2)
    tables = {
        "bot_memory_clock": "bot_id",
        "bot_payload_headers": "payload_id",
        "bot_payload_bodies": "payload_id",
        "bot_payload_forget_requests": "operation_id",
        "bot_payload_suppression": "source_unit_id,part_id",
    }
    before = {
        table: await rows(repository, f"SELECT * FROM public.{table} ORDER BY {order}")
        for table, order in tables.items()
    }
    host_guard = repository.store._host
    calls = 0

    async def final_fence(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ForestError("STALE_HOST", "Injected final owner fence")
        return await host_guard(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(repository.store, "_host", final_fence)
        with pytest.raises(ForestError) as exc:
            await PayloadStore(repository.store).scrub(lease, limit=1)
        assert exc.value.code == "STALE_HOST" and calls == 2
    assert await pending(repository) == 2
    for table, order in tables.items():
        assert before[table] == await rows(
            repository, f"SELECT * FROM public.{table} ORDER BY {order}"
        )
    assert (await PayloadStore(repository.store).scrub(lease, limit=1))["reconciled_links"] == 1

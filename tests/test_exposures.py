"""Real PostgreSQL Worker exposure union, invalidation and bounded drain."""

import asyncio
from types import SimpleNamespace

import psycopg
import pytest
import pytest_asyncio
from test_bot_store import candidate
from test_bot_store import database as database
from test_bot_store import store as store
from test_messages import messages as messages

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.exposures import ExposureStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def exposures(store):
    lease = await store.acquire_host("host", "incarnation", ttl_seconds=180)
    await candidate(store, lease)
    return ExposureStore(store), lease


async def query(repo, statement, args=()):
    async with repo.store.pool.connection() as conn:
        return await (await conn.execute(statement, args)).fetchall()


async def make(repo, lease, name, *, source=None, epoch=0):
    return await repo.payloads.put_json(
        lease,
        owner_kind="state.value",
        owner_id=name,
        value={"text": "CANARY-" + name},
        sources=(SourceRef(source),) if source else (),
        expected_deletion_epoch=epoch,
    )


async def expose(
    repo, lease, identifier, refs, *, generation="gen-1", kind="STATE_READ", subject="key:1"
):
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._routes(conn, ["counter"])
        await repo.store._host(conn, lease)
        return await repo.expose_in(conn, generation, identifier, kind, refs, subject)


async def guarded(repo, lease, generation="gen-1", *, phase="admission"):
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._routes(conn, ["counter"])
        await repo.store._host(conn, lease)
        return await repo.guard_generation_in(conn, generation, phase=phase)


async def fails(code, operation):
    with pytest.raises(ForestError) as exc:
        await operation
    assert exc.value.code == code
    return exc.value


async def test_actual_concurrent_deliveries_merge_and_receipts_bind_exact_input(exposures):
    repo, lease = exposures
    initial = await guarded(repo, lease)
    a, b = await make(repo, lease, "a"), await make(repo, lease, "b")
    results = await asyncio.gather(
        expose(repo, lease, "read-a", [a.payload_id]),
        expose(repo, lease, "read-b", [b.payload_id]),
        *(expose(repo, lease, "read-a", [a.payload_id]) for _ in range(5)),
    )
    assert sum(not r["duplicate"] for r in results) == 2
    head = await guarded(repo, lease)
    assert head["revision"] == initial["revision"] + 2
    assert head["payload_count"] == initial["payload_count"] + 2
    assert set(head["payload_ids"]) == set(initial["payload_ids"]) | {a.payload_id, b.payload_id}
    receipt = next(r for r in results if r["exposure_id"] == "read-a")
    replay = await expose(repo, lease, "read-a", [a.payload_id, a.payload_id])
    assert replay["exposure_revision"] == receipt["exposure_revision"]
    assert replay["input_digest"] == receipt["input_digest"] and replay["duplicate"]
    for kwargs in ({"refs": [b.payload_id]}, {"kind": "CALL_INPUT"}, {"subject": "key:2"}):
        await fails(
            "EXPOSURE_CONFLICT",
            expose(repo, lease, "read-a", kwargs.pop("refs", [a.payload_id]), **kwargs),
        )
    assert await guarded(repo, lease) == head
    # Output reads the current union, even if its own Call started at revision 1.
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._routes(conn, ["counter"])
        await repo.store._host(conn, lease)
        parents = await repo.generation_parents_in(conn, "gen-1")
        output = await repo.payloads.put_json_in(
            conn,
            owner_kind="kernel.result",
            owner_id="later-call",
            value={"result": "cached"},
            parents=parents,
            expected_deletion_epoch=0,
        )
    assert set(
        (
            await query(
                repo,
                "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
                (output.payload_id,),
            )
        )[0]["manifest"]["parents"]
    ) == set(initial["payload_ids"]) | {a.payload_id, b.payload_id}


async def test_deleted_prior_exposure_blocks_new_body_and_scan_persists_separately(exposures):
    repo, lease = exposures
    a = await make(repo, lease, "a", source="forgotten")
    await expose(repo, lease, "old-input", [a.payload_id])
    original_head = await guarded(repo, lease)
    original = await query(repo, "SELECT * FROM public.bot_generation_exposures")
    await repo.payloads.forget_sources(
        lease, "forget", (SourceRef("forgotten"),), expected_deletion_epoch=0
    )
    b = await make(repo, lease, "b", source="survivor", epoch=1)
    await fails("GENERATION_EXPOSURE_REVOKED", expose(repo, lease, "late-input", [b.payload_id]))
    assert (await query(repo, "SELECT status,revision FROM public.bot_generation_exposure_heads"))[
        0
    ] == {"status": "LIVE", "revision": original_head["revision"]}
    result = await repo.scan(lease)
    assert [(r["generation_id"], r["status"]) for r in result["generations"]] == [
        ("gen-1", "REVOKED")
    ]
    assert await repo.scan(lease) == result  # Lost acknowledgement repeats the same decision.
    for phase in ("admission", "commit"):
        await fails("GENERATION_EXPOSURE_REVOKED", guarded(repo, lease, phase=phase))
    await fails("GENERATION_EXPOSURE_REVOKED", expose(repo, lease, "old-input", [a.payload_id]))
    assert await query(repo, "SELECT * FROM public.bot_generation_exposures") == original
    await repo.payloads.scrub(lease)
    assert await repo.payloads.read_json(lease, b.payload_id, expected_deletion_epoch=1) == {
        "text": "CANARY-b"
    }


async def test_256_is_live_overflow_rolls_back_and_trusted_invalidation_allows_drain(exposures):
    repo, lease = exposures
    initial = await guarded(repo, lease)
    refs = []
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        for index in range(256 - initial["payload_count"]):
            ref = await repo.payloads.put_json_in(
                conn,
                owner_kind="state.value",
                owner_id=f"state-{index}",
                value=index,
                expected_deletion_epoch=0,
            )
            refs.append(ref.payload_id)
    admitted = await expose(repo, lease, "restore", refs, kind="RESTORE_STATE")
    assert admitted["payload_count"] == 256 and admitted["status"] == "LIVE"
    assert not (await repo.scan(lease))["generations"]
    with pytest.raises(ForestError) as caught:
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._routes(conn, ["counter"])
            await repo.store._host(conn, lease)
            await repo.ensure_generation_in(conn, "gen-1")
            proposed = await repo.payloads.put_json_in(
                conn,
                owner_kind="state.value",
                owner_id="rolled-back-input",
                value="late-body",
                expected_deletion_epoch=0,
            )
            await repo.expose_in(
                conn, "gen-1", "over-cap", "CALL_INPUT", [proposed.payload_id], "call"
            )
    assert caught.value.code == "GENERATION_EXPOSURE_LIMIT"
    assert not await query(
        repo, "SELECT 1 FROM public.bot_payload_headers WHERE payload_id=%s", (proposed.payload_id,)
    )
    await fails(
        "EXPOSURE_INVALIDATION_UNPROVEN",
        repo.invalidate(lease, "gen-1", admitted["revision"], candidate_payload_refs=refs[:1]),
    )
    exhausted = await repo.invalidate(
        lease, "gen-1", admitted["revision"], candidate_payload_refs=[proposed.payload_id]
    )
    assert exhausted["status"] == "EXHAUSTED"
    assert (
        await repo.invalidate(
            lease, "gen-1", admitted["revision"], candidate_payload_refs=[proposed.payload_id]
        )
        == exhausted
    )
    await fails("GENERATION_EXPOSURE_EXHAUSTED", guarded(repo, lease))
    assert (await guarded(repo, lease, phase="commit"))["payload_ids"] == sorted(
        set(refs) | set(initial["payload_ids"])
    )
    assert (await expose(repo, lease, "restore", refs, kind="RESTORE_STATE"))["duplicate"]
    assert (await expose(repo, lease, "existing-read", refs[:1]))["status"] == "EXHAUSTED"
    await fails(
        "GENERATION_EXPOSURE_LIMIT",
        expose(repo, lease, "over-cap", [proposed.payload_id], kind="CALL_INPUT", subject="call"),
    )
    assert not await query(
        repo, "SELECT 1 FROM public.bot_payload_headers WHERE payload_id=%s", (proposed.payload_id,)
    )
    assert (await repo.scan(lease))["generations"][0]["status"] == "EXHAUSTED"


async def test_scan_pages_past_live_heads_and_never_invalidates_other_generation(exposures):
    repo, lease = exposures
    await candidate(repo.store, lease, generation="gen-2")
    a = await make(repo, lease, "a", source="a")
    b = await make(repo, lease, "b", source="b")
    await expose(repo, lease, "one", [a.payload_id])
    await expose(repo, lease, "two", [b.payload_id], generation="gen-2")
    await repo.payloads.forget_sources(
        lease, "forget-b", (SourceRef("b"),), expected_deletion_epoch=0
    )
    page = await repo.scan(lease, limit=1)
    assert page["generations"] == [] and page["next_cursor"] == "gen-1"
    page = await repo.scan(lease, limit=1, after=page["next_cursor"])
    assert [r["generation_id"] for r in page["generations"]] == ["gen-2"]
    assert (await repo.scan(lease, limit=1, after=page["next_cursor"]))["next_cursor"] is None
    assert (await guarded(repo, lease))["status"] == "LIVE"


async def test_cross_bot_forged_payload_and_old_worker_identity_are_rejected(exposures):
    repo, lease = exposures
    await fails("GENERATION_EXPOSURE_REVOKED", expose(repo, lease, "fake", ["payload:absent"]))
    wrong = ExposureStore(SimpleNamespace(bot_id="other-bot"))
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._routes(conn, ["counter"])
        await repo.store._host(conn, lease)
        await fails("GENERATION_NOT_FOUND", wrong.guard_generation_in(conn, "gen-1"))
    await repo.store.release_host(lease)
    fresh = await repo.store.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
    await fails("STALE_HOST", guarded(repo, fresh))
    await fails("STALE_HOST", repo.scan(lease))
    assert (await repo.scan(fresh))["generations"] == []
    await candidate(repo.store, fresh, generation="gen-new")
    current = await guarded(repo, fresh, "gen-new")
    registered = (
        await query(
            repo,
            "SELECT config_payload_id FROM public.bot_generations WHERE generation_id='gen-new'",
        )
    )[0]
    assert current["payload_ids"] == [registered["config_payload_id"]]


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE public.bot_generation_exposure_heads SET worker_incarnation='forged'",
        "UPDATE public.bot_generation_exposure_heads SET revision=revision+1",
        "UPDATE public.bot_generation_exposure_heads SET payload_count=0",
        "DELETE FROM public.bot_generation_exposure_heads",
        "UPDATE public.bot_generation_exposures SET subject_ref='different'",
        "DELETE FROM public.bot_generation_exposures",
        "DELETE FROM public.bot_generation_exposure_members",
        "UPDATE public.bot_generation_exposure_members SET payload_id='fake'",
    ],
)
async def test_sql_cannot_rewrite_delivered_facts_or_generation_identity(exposures, statement):
    repo, lease = exposures
    ref = await make(repo, lease, "a")
    await expose(repo, lease, "one", [ref.payload_id])
    before = await guarded(repo, lease)
    with pytest.raises(psycopg.Error):
        async with repo.store.pool.connection() as conn:
            await conn.execute(statement)
    assert await guarded(repo, lease) == before


async def test_scan_final_host_failure_rolls_back_durable_invalidation(exposures, monkeypatch):
    repo, lease = exposures
    ref = await make(repo, lease, "a", source="a")
    await expose(repo, lease, "one", [ref.payload_id])
    await repo.payloads.forget_sources(
        lease, "forget", (SourceRef("a"),), expected_deletion_epoch=0
    )
    original = repo.store._host
    calls = 0

    async def fail_final(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ForestError("STALE_HOST", "Injected final Host fence")
        return await original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(repo.store, "_host", fail_final)
        await fails("STALE_HOST", repo.scan(lease))
    assert calls == 2
    assert (await query(repo, "SELECT status FROM public.bot_generation_exposure_heads"))[0][
        "status"
    ] == "LIVE"
    assert (await repo.scan(lease))["generations"][0]["status"] == "REVOKED"


async def test_real_exposure_lock_wait_rechecks_host_before_delivering_or_committing(exposures):
    repo, lease = exposures
    before = {
        table: await query(repo, "SELECT * FROM public." + table)
        for table in (
            "bot_generation_exposure_heads",
            "bot_generation_exposures",
            "bot_generation_exposure_members",
        )
    }
    ref = await make(repo, lease, "a")
    lease = await repo.store.renew_host(lease, ttl_seconds=4)
    task = None
    try:
        async with repo.store.pool.connection() as blocker, blocker.transaction():
            await blocker.execute(
                "SELECT 1 FROM public.bot_generation_exposure_heads WHERE generation_id='gen-1' FOR UPDATE"
            )
            task = asyncio.create_task(expose(repo, lease, "blocked", [ref.payload_id]))
            async with asyncio.timeout(2):
                while True:
                    waiting = await query(
                        repo,
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                        "AND wait_event_type='Lock' AND query LIKE %s) AS waiting",
                        ("%bot_generation_exposure_heads%FOR UPDATE%",),
                    )
                    if waiting[0]["waiting"]:
                        break
                    await asyncio.sleep(0.01)
            remaining = await query(
                repo,
                "SELECT GREATEST(0,extract(epoch FROM expires_at-clock_timestamp())) AS seconds "
                "FROM public.bot_host_lease",
            )
            await asyncio.sleep(float(remaining[0]["seconds"]) + 0.03)
        await fails("STALE_HOST", task)
        for table, original in before.items():
            assert await query(repo, "SELECT * FROM public." + table) == original
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_late_exact_receipt_makes_combined_generation_unavailable_then_scan_revokes(messages):
    from test_agent_store import prepare, respond, start
    from test_delivery import permit as delivery_permit
    from test_messages import receive

    from forest.storage.delivery import DeliveryStore

    repository, lease, _ = messages
    state, actor, run = await start(messages, group="42")

    class BroadDelivery(DeliveryStore):
        async def enqueue_in(self, conn, lease, **kwargs):
            # Real receipt/echo proof; only the additional provenance is supplied
            # by a trusted Host to reach the combined-source boundary cheaply.
            kwargs["sources"] = tuple(SourceRef(f"prior-{i:04}") for i in range(2498))
            return await super().enqueue_in(conn, lease, **kwargs)

    delivery = BroadDelivery(repository.store)
    _, request, model_permit = await prepare(state, actor, run)
    await respond(state, actor, model_permit, request, text="late provenance")
    await state.commit_final(actor, run["run_id"], "wide-reply", delivery)
    permit = await delivery_permit(delivery, lease, await delivery.claim(lease, "sender"))
    await delivery.mark_started(permit)
    echo = await receive(repository, lease, 999, group="42", user="1000", text="late provenance")
    repo = ExposureStore(repository.store)
    await candidate(repository.store, lease)
    other = await repo.payloads.put_json(
        lease,
        owner_kind="state.value",
        owner_id="other-2500",
        value={"other": True},
        sources=tuple(SourceRef(f"other-{i:04}") for i in range(2500)),
        expected_deletion_epoch=0,
    )
    echo_payload = (
        await query(
            repo,
            "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s",
            (echo["event_id"],),
        )
    )[0]["payload_id"]
    await expose(repo, lease, "read-early-echo", [echo_payload])
    await expose(repo, lease, "read-other", [other.payload_id])
    before = await guarded(repo, lease)
    facts = await query(repo, "SELECT * FROM public.bot_generation_exposures ORDER BY revision")
    await delivery.complete(permit, "SENT", {"message_id": "999"})
    assert await repository.reconcile_exact_echo(lease, echo["event_id"])
    # Each individual body is still legal; only their now-expanded combined
    # closure exceeds 4096. Neither manifest nor exposure receipt is rewritten.
    for payload_id in (echo_payload, other.payload_id):
        assert await repo.payloads.read_json(lease, payload_id, expected_deletion_epoch=0)
    await fails("GENERATION_EXPOSURE_REVOKED", guarded(repo, lease, phase="commit"))
    assert (await query(repo, "SELECT status FROM public.bot_generation_exposure_heads"))[0][
        "status"
    ] == "LIVE"  # Failure in a business transaction is not a persisted decision.
    invalid = (await repo.scan(lease))["generations"]
    assert len(invalid) == 1 and invalid[0]["status"] == "REVOKED"
    assert invalid[0]["reason_code"] == "SOURCE_SCOPE_LIMIT"
    assert invalid[0]["revision"] == before["revision"]
    assert invalid[0]["payload_ids"] == before["payload_ids"]
    assert (
        await query(repo, "SELECT * FROM public.bot_generation_exposures ORDER BY revision")
        == facts
    )
    assert (await repo.scan(lease))["generations"] == invalid

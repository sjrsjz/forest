"""Bot authority integration tests against disposable real PostgreSQL databases."""

import asyncio
import multiprocessing
import os
import sys
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.storage.bot import BotStore
from forest.storage.postgres import PostgresStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def database():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private test PostgreSQL instance")
    name = "forest_bot_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    try:
        yield uri
    finally:
        assert name.startswith("forest_bot_") and len(name) == 43
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest_asyncio.fixture
async def store(database):
    instance = BotStore(database, "bot-a")
    await instance.open()
    try:
        await instance.migrate()
        yield instance
    finally:
        await instance.close()


async def candidate(store, lease, generation="gen-1", component="counter"):
    await store.register_generation(
        lease,
        component,
        generation,
        "package-v1",
        "config-v1",
        "worker-" + generation,
        config={"initial": 0},
        instance_id="instance-" + generation,
    )
    await store.mark_ready(lease, generation)


async def operation(store, lease, generation="gen-1", *, epoch=0, component="counter", ttl=30):
    return await store.begin_operation(
        lease,
        component,
        "activate-" + generation,
        "plan-" + generation,
        generation,
        epoch,
        ttl_seconds=ttl,
    )


async def activate(store, lease=None):
    lease = lease or await store.acquire_host("host", "host-incarnation")
    await candidate(store, lease)
    op = await operation(store, lease)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    return lease


async def admitted(store, lease, name="call-1", timeout=30):
    return await store.admit(
        lease, "counter", name, "operation-" + name, "input-" + name, timeout_seconds=timeout
    )


async def commit(store, grant, *, key="count", revision=0, value=1, step="step-1"):
    return await store.commit_state(grant, key, revision, value, step, "input-" + step)


async def fails(code, awaitable):
    with pytest.raises(ForestError) as caught:
        await awaitable
    assert caught.value.code == code


async def test_bot_initialization_rejects_other_bot_and_deployment_without_mutation(
    store, database
):
    other = BotStore(database, "bot-b")
    await other.open()
    try:
        await fails("BOT_SCOPE_MISMATCH", other.migrate())
        await fails("BOT_SCOPE_MISMATCH", other.acquire_host("host", "wrong-bot"))
    finally:
        await other.close()
    assert (await store.runtime())["bot_id"] == "bot-a"
    await store.migrate()


async def test_deployment_database_is_never_migrated_as_bot(database):
    deployment = PostgresStore(database)
    await deployment.open()
    await deployment.migrate()
    bot = BotStore(database, "bot-a")
    await bot.open()
    try:
        await fails("DATABASE_SCOPE_MISMATCH", bot.migrate())
        async with deployment.pool.connection() as conn:
            row = await (
                await conn.execute("SELECT to_regclass('public.forest_bot_identity') AS identity")
            ).fetchone()
            assert row["identity"] is None
    finally:
        await bot.close()
        await deployment.close()


async def test_unknown_nonempty_database_is_not_repurposed(database):
    async with await psycopg.AsyncConnection.connect(database, autocommit=True) as conn:
        await conn.execute("CREATE TABLE unrelated_user_data (id integer PRIMARY KEY)")
    bot = BotStore(database, "bot-a")
    await bot.open()
    try:
        await fails("DATABASE_NOT_EMPTY", bot.migrate())
    finally:
        await bot.close()


async def test_host_epoch_survives_restart_expiry_cannot_renew_or_reuse(store, database):
    first = await store.acquire_host("host", "first", ttl_seconds=0.05)
    assert first.owner_epoch == 1
    await fails("HOST_BUSY", store.acquire_host("rival", "rival-incarnation"))
    await asyncio.sleep(0.06)
    await fails("STALE_HOST", store.renew_host(first))
    await fails("INCARNATION_REUSED", store.acquire_host("host", "first"))
    restarted = BotStore(database, "bot-a")
    await restarted.open()
    try:
        await restarted.migrate()
        second = await restarted.acquire_host("host", "second")
        assert second.owner_epoch == 2
        await fails("STALE_HOST", store.release_host(first))
        await restarted.release_host(second)
        third = await store.acquire_host("host", "third")
        assert third.owner_epoch == 3
        await fails("STALE_HOST", restarted.renew_host(second))
    finally:
        await restarted.close()


async def test_ready_has_no_production_authority_and_generation_identity_is_immutable(store):
    lease = await store.acquire_host("host", "incarnation")
    await candidate(store, lease)
    await fails("ADMISSION_CLOSED", admitted(store, lease))
    await fails(
        "GENERATION_CONFLICT",
        store.register_generation(
            lease,
            "counter",
            "gen-1",
            "changed-package",
            "config-v1",
            "worker-gen-1",
        ),
    )
    await fails(
        "GENERATION_CONFLICT",
        store.register_generation(
            lease,
            "counter",
            "gen-1",
            "package-v1",
            "config-v1",
            "worker-gen-1",
            config={"initial": 0},
            instance_id="instance-gen-1",
            event_definitions=[{
                "direction": "PUBLISH",
                "definition_id": "counter.changed",
                "topic": "counter.changed",
                "contract_major": 1,
                "contract_minor": 0,
                "schema_digest": "event-schema",
            }],
        ),
    )
    op = await operation(store, lease)
    await fails("INVALID_OPERATION_PHASE", store.publish(lease, op["operation_id"], op["op_epoch"]))
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute(
                "UPDATE public.bot_generations SET config='{}'::jsonb WHERE bot_id='bot-a'"
            )


async def test_freeze_drains_multiple_steps_and_switch_rejects_old_grant(store):
    lease = await activate(store)
    call = await admitted(store, lease)
    first = await commit(store, call)
    assert first["revision"] == 1
    assert await store.inflight("counter") == 1
    await candidate(store, lease, "gen-2")
    op = await operation(store, lease, "gen-2", epoch=1)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await fails("ADMISSION_CLOSED", admitted(store, lease, "new-call"))
    await fails("DRAIN_PENDING", store.publish(lease, op["operation_id"], op["op_epoch"]))
    second = await commit(store, call, revision=1, value=2, step="step-2")
    assert second["revision"] == 2
    assert await store.inflight("counter") == 1
    await store.finish_call(call, {"count": 2})
    published = await store.publish(lease, op["operation_id"], op["op_epoch"])
    assert published["route_epoch"] == 2
    await fails("STALE_ROUTE", commit(store, call, revision=2, value=3, step="late"))
    fresh = await admitted(store, lease, "fresh")
    assert await store.read_state(fresh, "count") == {"revision": 2, "value": 2}
    await store.dispose_generation(lease, "gen-1", "old-worker-reaped")
    assert (await store.get_route("counter"))["generation_id"] == "gen-2"
    assert (await store.get_route("counter"))["admission"] is True


async def test_call_and_step_idempotency_are_parent_scoped(store):
    lease = await activate(store)
    left = await admitted(store, lease, "left")
    right = await admitted(store, lease, "right")
    receipt = await commit(store, left, step="same-step")
    assert await commit(store, left, step="same-step") == receipt
    await fails("IDEMPOTENCY_CONFLICT", commit(store, left, step="same-step", value=999))
    # Another call cannot borrow the prior receipt by copying its step ID.
    await fails("REVISION_CONFLICT", commit(store, right, step="same-step"))
    separate = await commit(store, right, step="same-step", revision=1, value=2)
    assert separate["call_operation_id"] == right.operation_id
    await fails("OPERATION_PENDING", admitted(store, lease, "left"))
    await store.finish_call(left, {"ok": True})
    await fails("CALL_COMPLETED", admitted(store, lease, "left"))
    result = await store.read_call(left.operation_id)
    assert result["input_digest"] == left.input_digest
    assert result["component_id"] == left.component_id
    assert result["result"] is None and result["result_payload_id"]
    assert (await store.authorized_call(lease, left.operation_id, left.policy_revision))["result"] == {"ok": True}
    await fails(
        "IDEMPOTENCY_CONFLICT",
        store.admit(
            lease,
            "counter",
            "new-physical-call",
            left.operation_id,
            "different-input",
        ),
    )


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("bot_id", "bot-b", "BOT_SCOPE_MISMATCH"),
        ("worker_incarnation", "forged-worker", "INVALID_GRANT"),
        ("host_incarnation", "forged-host", "STALE_HOST"),
        ("operation_id", "someone-elses-operation", "INVALID_GRANT"),
    ],
)
async def test_forged_grant_is_rejected(store, field, value, code):
    lease = await activate(store)
    call = await admitted(store, lease)
    await fails(code, commit(store, replace(call, **{field: value})))
    assert await store.snapshot_state(lease, "counter") == {}


async def test_policy_maintenance_storage_and_cancellation_fence_writes(store):
    lease = await activate(store)
    policy = await admitted(store, lease, "policy")
    await store.revoke_policy(lease, 1)
    await fails("POLICY_REVOKED", commit(store, policy))
    maintenance = await admitted(store, lease, "maintenance")
    paused = await store.set_maintenance(lease, 1, False)
    assert paused["control_admission"] is True
    await fails("STALE_MAINTENANCE", commit(store, maintenance))
    await fails("ADMISSION_CLOSED", admitted(store, lease, "paused"))
    await store.set_maintenance(lease, 2, True)
    cancelled = await admitted(store, lease, "cancelled")
    await store.cancel_call(lease, cancelled.call_id)
    await fails("CALL_CANCELLED", commit(store, cancelled))
    current = await admitted(store, lease, "current")
    async with store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_runtime_state SET storage_generation=2 WHERE bot_id='bot-a'"
        )
    await fails("STALE_STORAGE", commit(store, current))
    async with store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_runtime_state SET status='SEALED' WHERE bot_id='bot-a'"
        )
    await fails("STORAGE_SEALED", commit(store, current))


async def test_commit_wins_cancellation_preserves_result_and_prior_facts(store):
    lease = await activate(store)
    call = await admitted(store, lease)
    await commit(store, call)
    await store.finish_call(call, {"value": 1})
    cancelled = await store.cancel_call(lease, call.call_id)
    assert cancelled["status"] == "COMPLETED"
    assert cancelled["cancel_epoch"] == 0
    assert (await store.snapshot_state(lease, "counter"))["count"]["value"] == 1


async def test_receipt_read_and_new_admission_require_current_explicit_authority(store):
    lease = await activate(store)
    call = await admitted(store, lease)
    await store.finish_call(call, {"answer": "saved"})
    assert (await store.authorized_call(lease, call.operation_id, 1))["result"] == {
        "answer": "saved"
    }
    await store.revoke_policy(lease, 1)
    await fails("POLICY_REVOKED", store.authorized_call(lease, call.operation_id, 1))
    await fails(
        "POLICY_REVOKED",
        store.admit(
            lease,
            "counter",
            "after-revocation",
            "new-operation",
            "input",
            expected_policy_revision=1,
        ),
    )
    await store.release_host(lease)
    recovered = await store.acquire_host("host", "replacement")
    await fails("STALE_HOST", store.authorized_call(lease, call.operation_id, 2))
    # A new trusted host with current policy can recover an old receipt without
    # requiring the old call's route or execution lease to be live.
    assert (await store.authorized_call(recovered, call.operation_id, 2))["result"] == {
        "answer": "saved"
    }


async def test_resume_reopens_paused_publication_but_never_frozen_generation(store):
    lease = await activate(store)
    await store.set_maintenance(lease, 1, False)
    await candidate(store, lease, "gen-2")
    op = await operation(store, lease, "gen-2", epoch=1)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.set_maintenance(lease, 2, True)
    assert (await store.get_route("counter"))["admission"] is False
    await store.set_maintenance(lease, 3, False)
    published = await store.publish(lease, op["operation_id"], op["op_epoch"])
    assert published["admission"] is False
    assert (await store.read_operation(op["operation_id"]))["phase"] == "COMMITTED"
    await store.set_maintenance(lease, 4, True)
    assert (await store.get_route("counter"))["admission"] is True
    assert (await admitted(store, lease)).generation_id == "gen-2"


async def test_expired_call_and_host_takeover_do_not_block_drain(store):
    lease = await activate(store)
    timed = await admitted(store, lease, "timed", timeout=0.02)
    await asyncio.sleep(0.03)
    await fails("CALL_EXPIRED", commit(store, timed))
    stale = await admitted(store, lease, "stale")
    await commit(store, stale)
    await store.release_host(lease)
    recovered = await store.acquire_host("host", "recovered-incarnation")
    await fails("STALE_HOST", commit(store, stale, revision=1, value=2, step="old-host"))
    assert await store.inflight("counter") == 0
    await candidate(store, recovered, "gen-2")
    op = await operation(store, recovered, "gen-2", epoch=1)
    await store.freeze(recovered, op["operation_id"], op["op_epoch"])
    await store.publish(recovered, op["operation_id"], op["op_epoch"])
    assert (await store.read_call(stale.operation_id))["status"] == "FAILED"
    assert (await store.snapshot_state(recovered, "counter"))["count"]["value"] == 1


async def test_lifecycle_resource_lease_and_pause_cannot_be_overridden(store):
    lease = await activate(store)
    await candidate(store, lease, "gen-2")
    await candidate(store, lease, "gen-3")
    first = await operation(store, lease, "gen-2", epoch=1, ttl=0.04)
    await store.freeze(lease, first["operation_id"], first["op_epoch"])
    await fails("RESOURCE_BUSY", operation(store, lease, "gen-3", epoch=1))
    await asyncio.sleep(0.05)
    await fails(
        "STALE_OPERATION", store.renew_operation(lease, first["operation_id"], first["op_epoch"])
    )
    second = await operation(store, lease, "gen-3", epoch=1)
    await store.freeze(lease, second["operation_id"], second["op_epoch"])
    await store.set_maintenance(lease, 1, False)
    await store.abort_operation(lease, second["operation_id"], second["op_epoch"])
    assert (await store.get_route("counter"))["admission"] is False
    await fails("ADMISSION_CLOSED", admitted(store, lease))
    # The previous coordinator cannot resume another operation's queue.
    assert (await store.abort_operation(lease, first["operation_id"], first["op_epoch"]))[
        "phase"
    ] == "ABANDONED"
    assert (await store.get_route("counter"))["admission"] is False


async def test_fail_generation_revokes_before_dispose_and_does_not_touch_successor(store):
    lease = await activate(store)
    call = await admitted(store, lease)
    await store.fail_generation(lease, "gen-1", "worker-exited")
    failed = await store.get_route("counter")
    assert failed["generation_status"] == "FAILED"
    assert failed["admission"] is False
    await fails("STALE_ROUTE", commit(store, call))
    await candidate(store, lease, "gen-2")
    op = await operation(store, lease, "gen-2", epoch=failed["route_epoch"])
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    before = await store.get_route("counter")
    await store.dispose_generation(lease, "gen-1", "process-joined")
    assert await store.get_route("counter") == before


async def test_state_outbox_failure_rolls_back_fact_head_and_receipt(store):
    lease = await activate(store)
    call = await admitted(store, lease)
    async with store.pool.connection() as conn:
        await conn.execute("""
            CREATE FUNCTION public.inject_bot_outbox_failure() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.topic='state.committed' THEN RAISE EXCEPTION 'injected failure'; END IF; RETURN NEW; END; $$;
            CREATE TRIGGER inject_bot_outbox_failure BEFORE INSERT ON public.bot_outbox_events
              FOR EACH ROW EXECUTE FUNCTION public.inject_bot_outbox_failure();
        """)
    with pytest.raises(psycopg.errors.RaiseException, match="injected failure"):
        await commit(store, call)
    assert await store.read_state(call, "count") == {"revision": 0, "value": None}
    async with store.pool.connection() as conn:
        for table in ("bot_state_steps", "bot_state_revisions"):
            row = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS n FROM public.{}").format(sql.Identifier(table))
                )
            ).fetchone()
            assert row["n"] == 0
        await conn.execute("DROP TRIGGER inject_bot_outbox_failure ON public.bot_outbox_events")
    assert (await commit(store, call))["revision"] == 1


async def test_deadline_is_rechecked_after_waiting_for_state_lock(store, database):
    lease = await activate(store)
    initial = await admitted(store, lease, "initial")
    await commit(store, initial)
    call = await admitted(store, lease, "waiting", timeout=0.4)
    blocker = await psycopg.AsyncConnection.connect(database, autocommit=True)
    try:
        async with blocker.transaction():
            await blocker.execute(
                "SELECT * FROM public.bot_component_state WHERE bot_id='bot-a' FOR UPDATE"
            )
            task = asyncio.create_task(commit(store, call, revision=1, value=2))
            for _ in range(100):
                async with store.pool.connection() as conn:
                    row = await (
                        await conn.execute(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                            "AND wait_event_type='Lock' AND query LIKE 'SELECT revision FROM public.bot_component_state%') AS waiting"
                        )
                    ).fetchone()
                if row["waiting"]:
                    break
                await asyncio.sleep(0.005)
            else:
                pytest.fail("commit did not reach the locked state head")
            await asyncio.sleep(0.45)
        await fails("CALL_EXPIRED", task)
        assert (await store.snapshot_state(lease, "counter"))["count"] == {
            "revision": 1,
            "value": 1,
        }
    finally:
        await blocker.close()


def _process_commit(database, grant, barrier, output):
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    async def run():
        instance = BotStore(database, "bot-a")
        await instance.open()
        try:
            await asyncio.to_thread(barrier.wait, 10)
            try:
                result = await commit(instance, grant)
                output.put(("ok", result["revision"]))
            except ForestError as exc:
                output.put(("rejected", exc.code))
        finally:
            await instance.close()

    asyncio.run(run())


async def test_state_cas_competes_across_real_processes(store, database):
    lease = await activate(store)
    grants = [await admitted(store, lease, name) for name in ("left", "right")]
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    output = context.Queue()
    workers = [
        context.Process(target=_process_commit, args=(database, grant, barrier, output))
        for grant in grants
    ]
    try:
        for worker in workers:
            worker.start()
        results = await asyncio.gather(*(asyncio.to_thread(output.get, True, 20) for _ in workers))
        assert sorted(results) == [("ok", 1), ("rejected", "REVISION_CONFLICT")]
        for worker in workers:
            await asyncio.to_thread(worker.join, 10)
            assert worker.exitcode == 0
        assert (await store.snapshot_state(lease, "counter"))["count"] == {
            "revision": 1,
            "value": 1,
        }
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            await asyncio.to_thread(worker.join, 10)
        output.close()
        output.join_thread()


async def test_admission_publication_race_does_not_consume_operation(store, database, monkeypatch):
    lease = await activate(store)
    selected = await store.get_route("counter")
    await candidate(store, lease, "gen-2")
    op = await operation(store, lease, "gen-2", epoch=1)
    entered = asyncio.Event()
    continue_admission = asyncio.Event()
    original_route = store._route

    async def delayed_route(conn, component_id):
        entered.set()
        await continue_admission.wait()
        return await original_route(conn, component_id)

    monkeypatch.setattr(store, "_route", delayed_route)
    pending = asyncio.create_task(
        store.admit(
            lease,
            "counter",
            "racing-call",
            "stable-operation",
            "stable-input",
            expected_generation_id=selected["generation_id"],
            expected_worker_incarnation=selected["worker_incarnation"],
            expected_route_epoch=selected["route_epoch"],
        )
    )
    publisher = BotStore(database, "bot-a")
    await publisher.open()
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await publisher.freeze(lease, op["operation_id"], op["op_epoch"])
        route = await publisher.publish(lease, op["operation_id"], op["op_epoch"])
        continue_admission.set()
        await fails("STALE_ROUTE", pending)
        assert await store.read_call("stable-operation") is None
        async with store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_outbox_events WHERE bot_id='bot-a' "
                    "AND operation_id='stable-operation'"
                )
            ).fetchone()
            assert row["n"] == 0
        fresh = await publisher.get_route("counter")
        accepted = await store.admit(
            lease,
            "counter",
            "racing-call",
            "stable-operation",
            "stable-input",
            expected_generation_id=route["generation_id"],
            expected_worker_incarnation=fresh["worker_incarnation"],
            expected_route_epoch=route["route_epoch"],
        )
        assert accepted.generation_id == "gen-2"
        assert accepted.operation_id == "stable-operation"
        assert (await store.read_call("stable-operation"))["status"] == "ADMITTED"
    finally:
        continue_admission.set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await publisher.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("expected_generation_id", "other-generation"),
        ("expected_worker_incarnation", "other-worker"),
        ("expected_route_epoch", 0),
    ],
)
async def test_each_selected_route_field_is_checked_before_admission(store, field, value):
    lease = await activate(store)
    expected = {
        "expected_generation_id": "gen-1",
        "expected_worker_incarnation": "worker-gen-1",
        "expected_route_epoch": 1,
    }
    await fails(
        "STALE_ROUTE",
        store.admit(
            lease,
            "counter",
            "call",
            "operation",
            "input",
            **{**expected, field: value},
        ),
    )
    assert await store.read_call("operation") is None
    assert (
        await store.admit(lease, "counter", "call", "operation", "input", **expected)
    ).route_epoch == 1

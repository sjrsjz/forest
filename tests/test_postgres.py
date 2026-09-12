"""Real PostgreSQL transaction tests, each confined to a fresh private schema.

Set FOREST_TEST_DSN to an explicitly designated test database. These tests never
truncate an existing table: fixture cleanup drops only its own UUID schema.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing
import os
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import make_conninfo

from forest.errors import ForestError
from forest.storage import PostgresStore


@pytest_asyncio.fixture
async def store():
    test_dsn = os.environ.get("FOREST_TEST_DSN")
    if not test_dsn:
        pytest.skip("FOREST_TEST_DSN is required for real PostgreSQL verification")
    schema = "forest_test_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(test_dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(test_dsn, options=f"-csearch_path={schema} -cstatement_timeout=15000")
    instance = PostgresStore(dsn)
    try:
        await instance.open()
        await instance.migrate()
        yield instance, dsn
    finally:
        await instance.close()
        # The identifier originates solely from the fixture's UUID above.
        assert schema.startswith("forest_test_") and len(schema) == len("forest_test_") + 32
        await admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        await admin.close()


async def initialized(store: PostgresStore) -> str:
    account = await store.setup("setup-1", "setup-digest", "administrator", "test-password-hash")
    return account["id"]


async def plan(
    store: PostgresStore, subject: str, *, revision: int = 0,
    values: dict | None = None, expires_at: datetime | None = None,
) -> dict:
    values = values if values is not None else {"display_name": "Forest", "session_ttl_minutes": 60}
    plan_id = str(uuid4())
    digest = hashlib.sha256(json.dumps([subject, revision, values], sort_keys=True).encode()).hexdigest()
    await store.save_plan(
        plan_id, subject, "deployment", revision, values, "schema-v1", digest,
        expires_at or datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    return {"resource_key": "deployment", "values": values, "expected_revision": revision,
            "idempotency_key": str(uuid4()), "subject_id": subject, "plan_digest": digest,
            "plan_id": plan_id, "schema_digest": "schema-v1"}


@pytest.mark.asyncio
async def test_setup_unique_and_recoverable(store):
    primary, dsn = store
    other = PostgresStore(dsn)
    await other.open()
    try:
        barrier = asyncio.Barrier(2)

        async def attempt(instance, setup_id):
            await barrier.wait()
            return await instance.setup(setup_id, "digest", setup_id, "hash")

        results = await asyncio.gather(attempt(primary, "left"), attempt(other, "right"), return_exceptions=True)
        success = [result for result in results if isinstance(result, dict)]
        rejected = [result for result in results if isinstance(result, ForestError)]
        assert len(success) == len(rejected) == 1
        assert rejected[0].code == "ALREADY_INITIALIZED"
        winner = success[0]
        repeated = await other.setup(winner["username"], "digest", winner["username"], "new-random-salt-hash")
        assert repeated == winner
        status = await primary.setup_status(winner["username"])
        assert status["account_id"] == winner["id"]
        with pytest.raises(ForestError, match="frozen"):
            await primary.setup(winner["username"], "changed", winner["username"], "hash")
        async with primary.pool.connection() as conn:
            count = await (await conn.execute("SELECT count(*) AS n FROM web_accounts")).fetchone()
            assert count["n"] == 1
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_sessions_revocation_expiry_and_disabled_account(store):
    repository, _ = store
    subject = await initialized(repository)
    await repository.create_session("token", "csrf", subject, datetime.now(timezone.utc) + timedelta(hours=1))
    session = await repository.get_session("token")
    assert session["account_id"] == subject
    assert session["csrf_hash"] == "csrf"
    assert "password_hash" not in session
    await repository.revoke_session("token")
    assert await repository.get_session("token") is None
    await repository.revoke_session("token")
    await repository.create_session("expired", "csrf", subject, datetime.now(timezone.utc) - timedelta(seconds=1))
    assert await repository.get_session("expired") is None
    await repository.create_session("live", "csrf", subject, datetime.now(timezone.utc) + timedelta(hours=1))
    async with repository.pool.connection() as conn:
        await conn.execute("UPDATE web_accounts SET disabled = true WHERE id = %s", (subject,))
    assert await repository.get_session("live") is None
    with pytest.raises(ForestError) as caught:
        await repository.create_session("forbidden", "csrf", subject, datetime.now(timezone.utc) + timedelta(hours=1))
    assert caught.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_cas_two_connections_and_atomic_facts(store):
    repository, dsn = store
    subject = await initialized(repository)
    # Distinct admins ensure the resource lock itself arbitrates the race.
    subject_b = str(uuid4())
    async with repository.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO web_accounts(id, username, password_hash, role) VALUES (%s, 'other-admin', 'hash', 'admin')",
            (subject_b,),
        )
    assert await repository.read_config("deployment") == {
        "resource_key": "deployment", "head_revision": 0, "desired": None, "effective": None,
    }
    left = await plan(repository, subject, values={"display_name": "Left"})
    right = await plan(repository, subject_b, values={"display_name": "Right"})
    other = PostgresStore(dsn)
    await other.open()
    try:
        barrier = asyncio.Barrier(2)

        async def apply(instance, payload):
            await barrier.wait()
            return await instance.apply_config(**payload)

        results = await asyncio.gather(apply(repository, left), apply(other, right), return_exceptions=True)
        winners = [result for result in results if isinstance(result, dict)]
        losers = [result for result in results if isinstance(result, ForestError)]
        assert len(winners) == len(losers) == 1
        assert losers[0].code == "REVISION_CONFLICT"
        current = await other.read_config("deployment")
        assert current["head_revision"] == 1
        assert current["desired"] == current["effective"] == winners[0]["desired"]
        async with repository.pool.connection() as conn:
            for table in ("config_revisions", "management_requests", "outbox_events"):
                count = await (await conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table)))).fetchone()
                assert count["n"] == 1
            audit = await (await conn.execute("SELECT count(*) AS n FROM audit_records WHERE action = 'config.apply'")).fetchone()
            assert audit["n"] == 1
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_idempotency_recovers_original_revision_and_rejects_forged_digest(store):
    repository, _ = store
    subject = await initialized(repository)
    payload = await plan(repository, subject)
    original = await repository.apply_config(**payload)
    next_plan = await plan(repository, subject, revision=1, values={"display_name": "Later"})
    await repository.apply_config(**next_plan)
    assert await repository.apply_config(**payload) == original
    assert await repository.apply_config(**dict(payload, schema_digest="schema-after-upgrade")) == original
    assert (await repository.read_config("deployment"))["head_revision"] == 2
    # Even a caller lying about the digest cannot change its frozen actual input.
    changed = dict(payload, values={"display_name": "Forged"})
    with pytest.raises(ForestError) as caught:
        await repository.apply_config(**changed)
    assert caught.value.code == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_plan_binding_expiry_and_original_recovery_after_expiry(store):
    repository, _ = store
    subject = await initialized(repository)
    expired = await plan(repository, subject, expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    with pytest.raises(ForestError) as caught:
        await repository.apply_config(**expired)
    assert caught.value.code == "PLAN_EXPIRED"
    payload = await plan(repository, subject, expires_at=datetime.now(timezone.utc) + timedelta(seconds=1))
    with pytest.raises(ForestError) as caught:
        await repository.apply_config(**dict(payload, schema_digest="new-schema"))
    assert caught.value.code == "PLAN_MISMATCH"
    with pytest.raises(ForestError) as caught:
        await repository.apply_config(**dict(payload, values={"display_name": "unreviewed"}))
    assert caught.value.code == "PLAN_MISMATCH"
    result = await repository.apply_config(**payload)
    await asyncio.sleep(1.05)
    assert await repository.apply_config(**payload) == result


@pytest.mark.asyncio
async def test_final_authority_checked_after_waiting_for_revocation(store):
    repository, dsn = store
    subject = await initialized(repository)
    payload = await plan(repository, subject)
    revoker = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        async with revoker.transaction():
            await revoker.execute("UPDATE web_accounts SET disabled = true WHERE id = %s", (subject,))
            task = asyncio.create_task(repository.apply_config(**payload))
            # Wait until the apply backend is demonstrably blocked by our account lock.
            for _ in range(100):
                async with repository.pool.connection() as conn:
                    state = await (await conn.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() "
                        "AND wait_event_type = 'Lock' AND query LIKE 'SELECT id, disabled, role%') AS blocked"
                    )).fetchone()
                if state["blocked"]:
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("apply never reached the final authority row lock")
        with pytest.raises(ForestError) as caught:
            await task
        assert caught.value.code == "FORBIDDEN"
        assert (await repository.read_config("deployment"))["head_revision"] == 0
    finally:
        await revoker.close()


@pytest.mark.asyncio
async def test_outbox_failure_rolls_back_configuration_and_request(store):
    repository, _ = store
    subject = await initialized(repository)
    payload = await plan(repository, subject)
    async with repository.pool.connection() as conn:
        await conn.execute("""
            CREATE FUNCTION fail_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected outbox failure'; END; $$;
            CREATE TRIGGER fail_test_outbox BEFORE INSERT ON outbox_events
              FOR EACH ROW EXECUTE FUNCTION fail_test_outbox();
        """)
    with pytest.raises(psycopg.errors.RaiseException, match="injected outbox"):
        await repository.apply_config(**payload)
    assert (await repository.read_config("deployment"))["head_revision"] == 0
    async with repository.pool.connection() as conn:
        for table in ("config_revisions", "management_requests", "outbox_events"):
            row = await (await conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table)))).fetchone()
            assert row["n"] == 0
        await conn.execute("DROP TRIGGER fail_test_outbox ON outbox_events")
    assert (await repository.apply_config(**payload))["head_revision"] == 1


@pytest.mark.asyncio
async def test_session_revocation_is_checked_inside_config_commit(store):
    repository, dsn = store
    subject = await initialized(repository)
    await repository.create_session("web-token", "csrf", subject, datetime.now(timezone.utc) + timedelta(hours=1))
    payload = dict(await plan(repository, subject), session_token_hash="web-token")
    revoker = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        async with revoker.transaction():
            await revoker.execute("UPDATE web_sessions SET revoked_at = clock_timestamp() WHERE token_hash = 'web-token'")
            task = asyncio.create_task(repository.apply_config(**payload))
            for _ in range(100):
                async with repository.pool.connection() as conn:
                    state = await (await conn.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() "
                        "AND wait_event_type = 'Lock' AND query LIKE 'SELECT account_id, revoked_at%') AS blocked"
                    )).fetchone()
                if state["blocked"]:
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("apply never reached its session authority lock")
        with pytest.raises(ForestError) as caught:
            await task
        assert caught.value.code == "SESSION_INVALID"
        assert (await repository.read_config("deployment"))["head_revision"] == 0
    finally:
        await revoker.close()


@pytest.mark.asyncio
async def test_migrations_verified_and_facts_immutable(store):
    repository, dsn = store
    other = PostgresStore(dsn)
    await other.open()
    try:
        await asyncio.gather(repository.migrate(), other.migrate())
    finally:
        await other.close()
    subject = await initialized(repository)
    await repository.apply_config(**await plan(repository, subject))
    async with repository.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute("UPDATE config_revisions SET values = '{}'::jsonb")
        await conn.execute("UPDATE forest_schema_migrations SET checksum = 'tampered'")
    with pytest.raises(ForestError) as caught:
        await repository.migrate()
    assert caught.value.code == "MIGRATION_CHANGED"


def _apply_process(dsn, barrier, output, payload):
    """Spawned interpreter owns its own pool and event loop, sharing only PG."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    async def run():
        repository = PostgresStore(dsn)
        await repository.open()
        try:
            await asyncio.to_thread(barrier.wait, 10)
            try:
                result = await repository.apply_config(**payload)
                output.put(("ok", result["head_revision"]))
            except ForestError as exc:
                output.put(("rejected", exc.code))
        finally:
            await repository.close()

    asyncio.run(run())


@pytest.mark.asyncio
async def test_configuration_cas_across_spawned_processes(store):
    repository, dsn = store
    subject = await initialized(repository)
    payloads = [await plan(repository, subject, values={"display_name": name}) for name in ("P1", "P2")]
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    output = context.Queue()
    workers = [context.Process(target=_apply_process, args=(dsn, barrier, output, payload)) for payload in payloads]
    try:
        for worker in workers:
            worker.start()
        results = await asyncio.gather(*(asyncio.to_thread(output.get, True, 20) for _ in workers))
        assert sorted(results) == [("ok", 1), ("rejected", "REVISION_CONFLICT")]
        for worker in workers:
            await asyncio.to_thread(worker.join, 10)
            assert worker.exitcode == 0
        assert (await repository.read_config("deployment"))["head_revision"] == 1
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            await asyncio.to_thread(worker.join, 10)
        output.close()
        output.join_thread()

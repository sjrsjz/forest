"""Integration tests create only UUID-named, disposable databases and roles."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from forest.errors import ForestError
from forest.management.catalog import BotCatalog
from forest.management.provision import BotProvisioner
from forest.security import SecretVault
from forest.storage.bot import BotStore
from forest.storage.postgres import PostgresStore


@dataclass
class Fixture:
    store: PostgresStore
    catalog: BotCatalog
    admin: psycopg.AsyncConnection
    auth: dict
    admin_dsn: str
    database_name: str
    external: list[tuple[str, str]] = field(default_factory=list)

    async def create(self, **kwargs):
        return await self.catalog.create(**self.auth, display_name="Test Bot",
                                         idempotency_key=uuid4().hex, **kwargs)

    async def plan(self, bot_id: str):
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT p.*,c.mode FROM bot_storage_plans p JOIN bot_catalog c USING(bot_id) WHERE bot_id=%s",
                (bot_id,),
            )).fetchone()
        return row, json.loads(self.catalog.vault.decrypt(row["secret_ciphertext"]))

    async def expire(self, job_id: str):
        async with self.store.pool.connection() as conn:
            await conn.execute(
                "UPDATE bot_provision_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=%s",
                (job_id,),
            )

    async def external_database(self) -> str:
        suffix = uuid4().hex
        database, role = "forest_cat_ext_" + suffix, "forest_cat_role_" + suffix
        self.external.append((database, role))
        await self.admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal("catalog-test-only-password")))
        await self.admin.execute(sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE template0").format(
            sql.Identifier(database), sql.Identifier(role)))
        values = conninfo_to_dict(self.admin_dsn)
        return (f"postgresql://{role}:catalog-test-only-password@{values.get('host','127.0.0.1')}:"
                f"{values.get('port','5432')}/{database}")


@pytest_asyncio.fixture
async def fixture(tmp_path):
    test_dsn = os.environ.get("FOREST_TEST_DSN")
    if not test_dsn:
        pytest.skip("FOREST_TEST_DSN is required for private PostgreSQL integration tests")
    database = "forest_cat_test_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(test_dsn, autocommit=True, row_factory=dict_row)
    await admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(database)))
    dsn = make_conninfo(test_dsn, dbname=database)
    store = PostgresStore(dsn)
    managed: list[tuple[str, str]] = []
    external: list[tuple[str, str]] = []
    try:
        await store.open()
        await store.migrate()
        account = await store.setup(uuid4().hex, "test-plan", "admin", "unused-test-hash")
        await store.create_session("test-session", "test-csrf", account["id"],
                                   datetime.now(timezone.utc) + timedelta(hours=1))
        catalog = BotCatalog(store, SecretVault(tmp_path / "vault"), managed_dsn=dsn)
        instance = Fixture(store, catalog, admin,
                           {"subject_id": account["id"], "session_token_hash": "test-session"},
                           test_dsn, database, external)
        yield instance
    finally:
        async with store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT p.database_name,p.role_name,p.bot_id FROM bot_storage_plans p "
                "JOIN bot_catalog c USING(bot_id) WHERE c.mode='managed'"
            )).fetchall()
            for row in rows:
                # Targets were allocated solely by this fixture's disposable
                # deployment. Assert full UUID derivation before any cleanup.
                suffix = row["bot_id"].replace("-", "")
                assert len(suffix) == 32 and int(suffix, 16) >= 0
                assert row["database_name"] == "forest_b_" + suffix
                assert row["role_name"] == "forest_r_" + suffix
                managed.append((row["database_name"], row["role_name"]))
        await store.close()
        for target, role in managed + external:
            if (target, role) in external:
                assert target.startswith("forest_cat_ext_") and len(target) == 47
                assert role == "forest_cat_role_" + target.removeprefix("forest_cat_ext_")
            assert target != conninfo_to_dict(test_dsn)["dbname"]
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(target)))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
        assert database.startswith("forest_cat_test_") and len(database) == 48
        assert database != conninfo_to_dict(test_dsn)["dbname"]
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        await admin.close()


async def test_managed_independent_credentials_identity_and_no_public_secrets(fixture):
    first, second = await fixture.create(), await fixture.create()
    assert first["bot_id"] != second["bot_id"]
    provision = BotProvisioner(fixture.catalog)
    assert (await provision.run_once())["stage"] == "READY"
    assert (await provision.run_once())["stage"] == "READY"
    assert await provision.run_once() is None
    assert len(await fixture.catalog.list_ready()) == 2
    plans = [await fixture.plan(bot["bot_id"]) for bot in (first, second)]
    passwords = [conninfo_to_dict(secret["dsn"])["password"] for _, secret in plans]
    assert passwords[0] != passwords[1]
    for bot, (plan, secret) in zip((first, second), plans, strict=True):
        assert passwords[0] not in plan["secret_ciphertext"]
        bot_dsn = await fixture.catalog.resolve_dsn(bot["bot_id"])
        async with await psycopg.AsyncConnection.connect(bot_dsn, row_factory=dict_row) as conn:
            identity = await (await conn.execute("SELECT bot_id FROM forest_bot_identity")).fetchone()
            assert identity["bot_id"] == bot["bot_id"]
            attrs = await (await conn.execute(
                "SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user"
            )).fetchone()
            assert not any(attrs.values())
        job = await fixture.catalog.read_job(job_id=bot["job_id"], **fixture.auth)
        public = json.dumps([bot, job, await fixture.catalog.list(**fixture.auth)])
        assert secret["verifier"] not in public and secret["dsn"] not in public
        assert all(password not in public for password in passwords)
        public_access = await (await fixture.admin.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_database d,aclexplode(d.datacl) a "
            "WHERE d.datname=%s AND a.grantee=0 AND a.privilege_type='CONNECT') AS access",
            (plan["database_name"],),
        )).fetchone()
        assert not public_access["access"]
    # A Bot's password cannot open a different Bot's database.
    other_dsn = make_conninfo(plans[0][1]["dsn"], dbname=plans[1][0]["database_name"])
    with pytest.raises(psycopg.OperationalError):
        await psycopg.AsyncConnection.connect(other_dsn)


async def test_create_replay_conflict_and_rename_cas(fixture):
    args = {**fixture.auth, "display_name": "First", "idempotency_key": "create-once"}
    first, same = await asyncio.gather(fixture.catalog.create(**args), fixture.catalog.create(**args))
    assert first == same
    with pytest.raises(ForestError, match="different input"):
        await fixture.catalog.create(**{**args, "display_name": "Different"})
    results = await asyncio.gather(*[
        fixture.catalog.rename(**fixture.auth, bot_id=first["bot_id"], display_name=name,
                               expected_revision=1, idempotency_key=name)
        for name in ("A", "B")
    ], return_exceptions=True)
    assert sum(isinstance(result, dict) for result in results) == 1
    assert [result.code for result in results if isinstance(result, ForestError)] == ["REVISION_CONFLICT"]
    assert len(await fixture.catalog.list(**fixture.auth)) == 1


async def test_replay_reads_and_mutations_check_current_session_and_admin(fixture):
    args = {**fixture.auth, "display_name": "First", "idempotency_key": "once"}
    bot = await fixture.catalog.create(**args)
    await fixture.store.revoke_session("test-session")
    for operation in (
        fixture.catalog.create(**args), fixture.catalog.list(**fixture.auth),
        fixture.catalog.get(bot_id=bot["bot_id"], **fixture.auth),
        fixture.catalog.read_job(job_id=bot["job_id"], **fixture.auth),
    ):
        with pytest.raises(ForestError) as exc:
            await operation
        assert exc.value.code == "SESSION_INVALID"
    # Accepted delegation survives browser logout.
    assert (await BotProvisioner(fixture.catalog).run_once())["stage"] == "READY"


async def test_account_revocation_prevents_first_external_effect(fixture):
    bot = await fixture.create()
    async with fixture.store.pool.connection() as conn:
        await conn.execute("UPDATE web_accounts SET disabled=true WHERE id=%s", (fixture.auth["subject_id"],))
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["stage"] == "FAILED" and result["error_code"] == "FORBIDDEN"
    plan, _ = await fixture.plan(bot["bot_id"])
    row = await (await fixture.admin.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (plan["role_name"],))).fetchone()
    assert row is None


@pytest.mark.parametrize("crash_step", ["CREATE_ROLE", "CREATE_DATABASE", "RESTRICT_DATABASE"])
async def test_crash_after_ddl_before_marker_resumes_same_owned_plan(fixture, monkeypatch, crash_step):
    bot = await fixture.create()
    original_plan, original_secret = await fixture.plan(bot["bot_id"])
    provision = BotProvisioner(fixture.catalog)
    original = provision._ddl

    async def crash(conn, job, step, statement):
        await original(conn, job, step, statement)
        if step == crash_step:
            raise asyncio.CancelledError

    monkeypatch.setattr(provision, "_ddl", crash)
    with pytest.raises(asyncio.CancelledError):
        await provision.run_once()
    await fixture.expire(bot["job_id"])
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["stage"] == "READY" and result["attempt"] == 2
    assert await fixture.plan(bot["bot_id"]) == (original_plan, original_secret)


@pytest.mark.parametrize("collision", ["role", "database"])
async def test_unknown_same_name_resource_is_never_adopted(fixture, collision):
    bot = await fixture.create()
    plan, secret = await fixture.plan(bot["bot_id"])
    if collision == "role":
        await fixture.admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'unrelated-test-password'").format(
            sql.Identifier(plan["role_name"])))
    else:
        # Even a genuine owned role must not cause adoption of another owner's DB.
        await fixture.admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(plan["role_name"]), sql.Literal(secret["verifier"])))
        await fixture.admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
            sql.Identifier(plan["database_name"])))
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["stage"] == "FAILED" and result["error_code"] == "STORAGE_OWNERSHIP_CONFLICT"
    current = await fixture.catalog.get(bot_id=bot["bot_id"], **fixture.auth)
    retry = await fixture.catalog.retry(**fixture.auth, bot_id=bot["bot_id"],
                                        expected_revision=current["revision"], idempotency_key="retry")
    assert retry["stage"] == "PENDING"
    assert (await BotProvisioner(fixture.catalog).run_once())["error_code"] == "STORAGE_OWNERSHIP_CONFLICT"
    assert await fixture.plan(bot["bot_id"]) == (plan, secret)


async def test_live_session_lock_blocks_expired_lease_takeover_and_stale_permit(fixture, monkeypatch):
    bot = await fixture.create()
    provision = BotProvisioner(fixture.catalog)
    entered, release = asyncio.Event(), asyncio.Event()
    original = provision._managed

    async def pause(job, plan, secret):
        entered.set()
        await release.wait()
        await original(job, plan, secret)

    monkeypatch.setattr(provision, "_managed", pause)
    task = asyncio.create_task(provision.run_once())
    await asyncio.wait_for(entered.wait(), 5)
    await fixture.expire(bot["job_id"])
    assert await BotProvisioner(fixture.catalog).run_once() is None
    release.set()
    assert (await task)["stage"] == "PROVISIONING"  # Expired lease cannot be revived.
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["stage"] == "READY" and result["owner_epoch"] == 2


async def test_external_database_initialization_and_identity_conflict(fixture):
    dsn = await fixture.external_database()
    first = await fixture.create(mode="external", dsn=dsn)
    assert (await BotProvisioner(fixture.catalog).run_once())["stage"] == "READY"
    second = await fixture.create(mode="external", dsn=dsn)
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["stage"] == "FAILED" and result["error_code"] == "BOT_SCOPE_MISMATCH"
    first_store = BotStore(await fixture.catalog.resolve_dsn(first["bot_id"]), first["bot_id"])
    try:
        await first_store.open()
        await first_store.migrate()
    finally:
        await first_store.close()
    with pytest.raises(ForestError) as exc:
        await fixture.catalog.resolve_dsn(second["bot_id"])
    assert exc.value.code == "BOT_NOT_READY"


async def test_external_nonempty_database_is_not_modified_or_deleted(fixture):
    dsn = await fixture.external_database()
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute("CREATE TABLE user_data(value text)")
        await conn.execute("INSERT INTO user_data VALUES ('keep')")
    await fixture.create(mode="external", dsn=dsn)
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["error_code"] == "DATABASE_NOT_EMPTY"
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        assert await (await conn.execute("SELECT value FROM user_data")).fetchone() == ("keep",)
        assert await (await conn.execute("SELECT to_regclass('forest_bot_identity')")).fetchone() == (None,)


async def test_external_superuser_is_rejected_without_initialization(fixture):
    parts = conninfo_to_dict(fixture.admin_dsn)
    dsn = (f"postgresql://{quote(parts['user'])}:{quote(parts.get('password',''))}@"
           f"{parts.get('host','127.0.0.1')}:{parts.get('port','5432')}/{fixture.database_name}")
    await fixture.create(mode="external", dsn=dsn)
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["error_code"] == "UNSAFE_RUNTIME_ROLE"


async def test_frozen_plan_is_database_immutable(fixture):
    bot = await fixture.create()
    async with fixture.store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute("UPDATE bot_storage_plans SET database_name='different' WHERE bot_id=%s", (bot["bot_id"],))


async def test_authorization_rechecked_after_managed_plan_preparation(fixture, monkeypatch):
    original = fixture.catalog._prepare_plan

    async def prepare(*args):
        result = await original(*args)
        await fixture.store.revoke_session("test-session")
        return result

    monkeypatch.setattr(fixture.catalog, "_prepare_plan", prepare)
    with pytest.raises(ForestError) as exc:
        await fixture.create()
    assert exc.value.code == "SESSION_INVALID"
    async with fixture.store.pool.connection() as conn:
        assert (await (await conn.execute("SELECT count(*) AS count FROM bot_catalog")).fetchone())["count"] == 0


async def test_revocation_between_ddl_steps_and_retry_by_current_admin(fixture, monkeypatch):
    bot = await fixture.create()
    provision = BotProvisioner(fixture.catalog)
    original = provision._ddl

    async def revoke(conn, job, step, statement):
        await original(conn, job, step, statement)
        if step == "CREATE_ROLE":
            async with fixture.store.pool.connection() as authority:
                await authority.execute("UPDATE web_accounts SET disabled=true WHERE id=%s",
                                        (fixture.auth["subject_id"],))

    monkeypatch.setattr(provision, "_ddl", revoke)
    assert (await provision.run_once())["error_code"] == "FORBIDDEN"
    plan, _ = await fixture.plan(bot["bot_id"])
    assert await (await fixture.admin.execute("SELECT oid FROM pg_database WHERE datname=%s",
                                              (plan["database_name"],))).fetchone() is None
    async with fixture.store.pool.connection() as conn:
        await conn.execute("UPDATE web_accounts SET disabled=false WHERE id=%s", (fixture.auth["subject_id"],))
    current = await fixture.catalog.get(**fixture.auth, bot_id=bot["bot_id"])
    retry_args = {**fixture.auth, "bot_id": bot["bot_id"], "expected_revision": current["revision"],
                  "idempotency_key": "retry-revoked"}
    first = await fixture.catalog.retry(**retry_args)
    assert await fixture.catalog.retry(**retry_args) == first
    assert (await BotProvisioner(fixture.catalog).run_once())["stage"] == "READY"


async def test_claim_rollback_releases_session_lock(fixture, monkeypatch):
    bot = await fixture.create()
    provision = BotProvisioner(fixture.catalog)
    original = provision._event

    async def fail(conn, job, step, **details):
        if step == "RECONCILING":
            await conn.execute("SELECT 1/0")
        await original(conn, job, step, **details)

    monkeypatch.setattr(provision, "_event", fail)
    with pytest.raises(psycopg.errors.DivisionByZero):
        await provision.run_once()
    # Rollback does not itself release a PostgreSQL session advisory lock.
    # A new independent manager must nevertheless be able to claim this job.
    result = await BotProvisioner(fixture.catalog).run_once()
    assert result["job_id"] == bot["job_id"] and result["stage"] == "READY"


async def test_older_epoch_cannot_obtain_new_ddl_permit(fixture):
    bot = await fixture.create()
    old, new = BotProvisioner(fixture.catalog), BotProvisioner(fixture.catalog)
    async with fixture.store.pool.connection() as conn:
        old_job = await old._claim(conn)
        from forest.management.provision import _lock_key

        await conn.execute("SELECT pg_advisory_unlock(%s)", (_lock_key(bot["job_id"]),))
    await fixture.expire(bot["job_id"])
    async with fixture.store.pool.connection() as conn:
        new_job = await new._claim(conn)
        try:
            with pytest.raises(ForestError) as exc:
                await old._permit(old_job, "CREATE_ROLE")
            assert exc.value.code == "PROVISION_LEASE_LOST"
            assert new_job["owner_epoch"] == old_job["owner_epoch"] + 1
        finally:
            await conn.execute("SELECT pg_advisory_unlock(%s)", (_lock_key(bot["job_id"]),))


async def test_matching_verifier_with_escalated_role_is_refused(fixture):
    bot = await fixture.create()
    plan, secret = await fixture.plan(bot["bot_id"])
    await fixture.admin.execute(sql.SQL("CREATE ROLE {} LOGIN CREATEDB PASSWORD {}").format(
        sql.Identifier(plan["role_name"]), sql.Literal(secret["verifier"])))
    assert (await BotProvisioner(fixture.catalog).run_once())["error_code"] == "STORAGE_OWNERSHIP_CONFLICT"


@pytest.mark.parametrize("action", ["permit", "heartbeat"])
async def test_lease_expiring_while_waiting_for_unchanged_job_row_cannot_revive(fixture, action):
    from forest.management.provision import _lock_key

    await fixture.create()
    provision = BotProvisioner(fixture.catalog, lease_seconds=3)
    task = None
    async with fixture.store.pool.connection() as owner:
        job = await provision._claim(owner)
        try:
            await owner.execute(
                "UPDATE bot_provision_jobs SET lease_expires_at=clock_timestamp()+interval '4 seconds' "
                "WHERE job_id=%s", (job["job_id"],),
            )
            async with fixture.store.pool.connection() as blocker, blocker.transaction():
                # No UPDATE: an EPQ re-evaluation of an updated tuple would hide
                # the pre-lock timestamp bug this regression is intended to test.
                await blocker.execute("SELECT * FROM bot_provision_jobs WHERE job_id=%s FOR UPDATE",
                                      (job["job_id"],))
                task = asyncio.create_task(provision._permit(job, "TEST_DDL") if action == "permit"
                                           else provision._heartbeat(job))
                async with asyncio.timeout(5):
                    while True:
                        async with fixture.store.pool.connection() as observer:
                            state = await (await observer.execute(
                                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                                "AND wait_event_type='Lock' AND query LIKE %s) "
                                "AS blocked, (SELECT lease_expires_at>clock_timestamp() FROM bot_provision_jobs "
                                "WHERE job_id=%s) AS unexpired",
                                ("SELECT %FROM bot_provision_jobs %FOR UPDATE", job["job_id"]),
                            )).fetchone()
                        if state["blocked"]:
                            assert state["unexpired"], "Guard must reach the lock before expiry"
                            break
                        await asyncio.sleep(0.01)
                async with asyncio.timeout(5):
                    while True:
                        async with fixture.store.pool.connection() as observer:
                            row = await (await observer.execute(
                                "SELECT lease_expires_at<=clock_timestamp() AS expired FROM bot_provision_jobs WHERE job_id=%s",
                                (job["job_id"],),
                            )).fetchone()
                        if row["expired"]:
                            break
                        await asyncio.sleep(0.02)
            if action == "permit":
                with pytest.raises(ForestError) as exc:
                    await asyncio.wait_for(task, 1)
                assert exc.value.code == "PROVISION_LEASE_LOST"
            else:
                await asyncio.wait_for(task, 1)  # Must stop rather than renew an expired lease.
            row = await (await owner.execute(
                "SELECT lease_expires_at<=clock_timestamp() AS expired,step FROM bot_provision_jobs WHERE job_id=%s",
                (job["job_id"],),
            )).fetchone()
            assert row["expired"] and row["step"] == "RECONCILING"
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await owner.execute("SELECT pg_advisory_unlock(%s)", (_lock_key(job["job_id"]),))


@pytest.mark.parametrize("field,value", [("display_name", ""), ("display_name", "x\n"),
                                          ("expected_revision", True), ("idempotency_key", "")])
async def test_invalid_creation_is_rejected_before_storage(field, value):
    catalog = BotCatalog(None, None)
    args = {"subject_id": "unused", "session_token_hash": "unused", "display_name": "Name",
            "idempotency_key": "key", field: value}
    with pytest.raises(ForestError) as exc:
        await catalog.create(**args)
    assert exc.value.code == "VALIDATION_FAILED"

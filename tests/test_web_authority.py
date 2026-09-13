"""Real PostgreSQL acceptance races at the deployment-to-Bot authority boundary."""

import asyncio
import base64
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.errors import RaiseException

from forest.errors import ForestError
from forest.management.authority import WebAuthority
from forest.packages import digest_json
from forest.security import SecretVault
from forest.storage.bot import BotStore, HostLease
from forest.storage.control import BotControl
from forest.storage.postgres import PostgresStore

pytestmark = pytest.mark.postgres
COMPONENT = "test.counter"
VALUES = {"package_digest": "package-v1", "config": {"increment": 1}, "grants": ["state.read"]}


@dataclass
class Environment:
    deployment: PostgresStore
    store: BotStore
    control: BotControl
    authority: WebAuthority
    lease: HostLease
    subject: str
    vault: SecretVault

    async def ticket(self, action, operation, payload, **changes):
        arguments = {
            "subject_id": self.subject,
            "session_token_hash": "session",
            "bot_id": self.store.bot_id,
            "action": action,
            "operation_id": operation,
            "payload": payload,
            "policy_revision": (await self.store.runtime())["policy_revision"],
            "plan_digest": payload.get("plan_digest", "") if action == "config.apply" else "",
        }
        arguments.update(changes)
        return await self.authority.issue(**arguments)

    def sign(self, claims):
        return {
            "claims": claims,
            "signature": base64.b64encode(
                self.authority.key.sign(digest_json(claims).encode("ascii"))
            ).decode("ascii"),
        }

    async def plan(self, expected=0, values=None, operation=None):
        operation = operation or "plan-" + uuid4().hex
        payload = {
            "component_id": COMPONENT,
            "expected_revision": expected,
            "values": deepcopy(VALUES if values is None else values),
        }
        ticket = await self.ticket("config.plan", operation, payload)
        return await self.control.plan(ticket, operation, payload)

    async def apply(self, plan, operation=None):
        operation = operation or "apply-" + uuid4().hex
        payload = {
            "component_id": COMPONENT,
            "expected_revision": plan["expected_revision"],
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
        }
        ticket = await self.ticket("config.apply", operation, payload)
        return await self.control.apply(ticket, operation, payload)

    async def read(self):
        operation = "read-" + uuid4().hex
        ticket = await self.ticket("config.read", operation, {"component_id": COMPONENT})
        return await self.control.read(ticket, operation, COMPONENT)

    async def candidate(self, revision=1, values=None):
        values = VALUES if values is None else values
        operation, generation = "activate-" + uuid4().hex, "generation-" + uuid4().hex
        await self.control.bind_activation(self.lease, COMPONENT, revision, operation)
        await self.store.register_generation(
            self.lease,
            COMPONENT,
            generation,
            values["package_digest"],
            digest_json(values["config"]),
            "worker-" + generation,
            config=values["config"],
            capability_grants=values["grants"],
            configuration_operation_id=operation,
        )
        await self.store.mark_ready(self.lease, generation)
        route = await self.store.get_route(COMPONENT)
        lifecycle = await self.store.begin_operation(
            self.lease, COMPONENT, operation, digest_json(values), generation, route["route_epoch"]
        )
        await self.store.freeze(self.lease, operation, lifecycle["op_epoch"])
        await self.store.quiesce_operation(self.lease, operation, lifecycle["op_epoch"])
        return lifecycle


@pytest_asyncio.fixture
async def environment(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    names = []
    stores = []
    try:
        for kind in ("deploy", "bot"):
            name = "forest_auth_" + kind + "_" + uuid4().hex
            await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            names.append(name)
            parts = urlsplit(dsn)
            uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
            store = PostgresStore(uri) if kind == "deploy" else BotStore(uri, "bot-a")
            stores.append(store)
            await store.open()
            await store.migrate()
        deployment, store = stores
        account = await deployment.setup("setup", "setup-digest", "admin", "opaque-hash")
        await deployment.create_session(
            "session", "csrf", account["id"], datetime.now(UTC) + timedelta(minutes=10)
        )
        vault = SecretVault(tmp_path / "secrets")
        authority = WebAuthority(deployment, vault, "deployment-a")
        await authority.initialize()
        lease = await store.acquire_host("web-host", "incarnation", ttl_seconds=120)
        control = BotControl(store)
        await control.trust(
            lease,
            issuer_id=authority.issuer_id,
            key_revision=authority.key_revision,
            public_key=authority.public_key,
        )
        yield Environment(deployment, store, control, authority, lease, account["id"], vault)
    finally:
        for store in stores:
            await store.close()
        for name in names:
            assert name.startswith("forest_auth_") and 48 <= len(name) <= 51
            await admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
        await admin.close()


async def rejected(code, action):
    with pytest.raises(ForestError) as caught:
        await action
    assert caught.value.code == code


async def wait_for_lock(store, fragment):
    for _ in range(200):
        async with store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                    "AND wait_event_type='Lock' AND query LIKE %s) AS waiting",
                    (fragment,),
                )
            ).fetchone()
        if row["waiting"]:
            return
        await asyncio.sleep(0.005)
    pytest.fail("Expected authority operation did not reach its database row lock")


async def test_issuer_key_and_signed_receipts_survive_restart(environment):
    env = environment
    reopened = WebAuthority(env.deployment, env.vault, env.authority.issuer_id)
    await reopened.initialize()
    assert reopened.public_key == env.authority.public_key
    env.authority = reopened
    assert (await env.read())["desired_revision"] == 0
    plan = await env.plan()
    accepted = await env.apply(plan, "stable-apply")
    assert accepted["desired_revision"] == 1 and accepted["effective_revision"] == 0
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT ticket_basis FROM public.bot_web_operations WHERE operation_id='stable-apply'"
            )
        ).fetchone()
    assert row["ticket_basis"]["account_revision"] == 1
    assert row["ticket_basis"]["session_revision"] == 1
    assert row["ticket_basis"]["subject_id"] == env.subject


@pytest.mark.parametrize("mutation", ["signature", "payload", "bot", "action", "issuer"])
async def test_tampered_or_wrong_scope_ticket_is_rejected(environment, mutation):
    env = environment
    operation, payload = "read", {"component_id": COMPONENT}
    ticket = await env.ticket("config.read", operation, payload)
    if mutation == "signature":
        ticket["claims"]["subject_id"] = "another-admin"
    elif mutation == "payload":
        operation = "another-operation"
    else:
        claims = dict(ticket["claims"])
        claims[{"bot": "bot_id", "action": "action", "issuer": "issuer_id"}[mutation]] = "other"
        ticket = env.sign(claims)
    await rejected("WEB_AUTHORITY_REJECTED", env.control.read(ticket, operation, COMPONENT))


async def test_policy_and_issuer_revocation_are_final_target_guards(environment):
    env = environment
    payload = {"component_id": COMPONENT}
    before = await env.ticket("config.read", "read", payload)
    await env.store.revoke_policy(env.lease, expected_revision=1)
    await rejected("WEB_AUTHORITY_REJECTED", env.control.read(before, "read", COMPONENT))
    fresh = await env.ticket("config.read", "read", payload)
    assert (await env.control.read(fresh, "read", COMPONENT))["desired_revision"] == 0
    async with env.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_web_issuers SET enabled=false WHERE bot_id='bot-a'")
    await rejected("WEB_AUTHORITY_REJECTED", env.control.read(fresh, "read", COMPONENT))


async def test_session_revocation_stops_new_issue_with_bounded_existing_ticket_window(environment):
    env = environment
    payload = {"component_id": COMPONENT}
    before = await env.ticket("config.read", "read", payload)
    await env.deployment.revoke_session("session")
    await rejected("UNAUTHENTICATED", env.ticket("config.read", "new-read", payload))
    # Cross-database revocation is explicitly bounded by a 30-second ticket;
    # target policy revocation above is immediate. Do not assert distributed atomicity.
    assert (await env.control.read(before, "read", COMPONENT))["desired_revision"] == 0
    claims = dict(before["claims"])
    claims["issued_at"] = (datetime.now(UTC) - timedelta(seconds=31)).isoformat()
    claims["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    await rejected("WEB_AUTHORITY_REJECTED", env.control.read(env.sign(claims), "read", COMPONENT))


async def test_issue_checks_session_expiry_after_waiting_for_session_lock(environment):
    env = environment
    async with env.deployment.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "UPDATE web_sessions SET expires_at=clock_timestamp()+interval '0.3 seconds' "
                "WHERE token_hash='session'"
            )
            task = asyncio.create_task(
                env.ticket("config.read", "read", {"component_id": COMPONENT})
            )
            await wait_for_lock(env.deployment, "SELECT * FROM web_sessions%FOR UPDATE")
            await asyncio.sleep(0.35)
        await rejected("UNAUTHENTICATED", task)


@pytest.mark.parametrize("lock_table", ["bot_runtime_state", "bot_config_heads"])
async def test_ticket_expiry_while_waiting_does_not_create_plan_or_receipt(environment, lock_table):
    env = environment
    operation = "expires-while-waiting"
    payload = {"component_id": COMPONENT, "expected_revision": 0, "values": VALUES}
    ticket = await env.ticket("config.plan", operation, payload)
    claims = dict(ticket["claims"])
    claims["expires_at"] = (datetime.now(UTC) + timedelta(seconds=0.3)).isoformat()
    ticket = env.sign(claims)
    async with env.store.pool.connection() as blocker:
        await blocker.execute(
            "INSERT INTO public.bot_config_heads(bot_id,component_id) VALUES ('bot-a',%s)",
            (COMPONENT,),
        )
        await blocker.commit()
        async with blocker.transaction():
            await blocker.execute(
                sql.SQL("SELECT * FROM public.{} FOR UPDATE").format(sql.Identifier(lock_table))
            )
            task = asyncio.create_task(env.control.plan(ticket, operation, payload))
            await wait_for_lock(env.store, "SELECT * FROM public." + lock_table + "%FOR UPDATE")
            await asyncio.sleep(0.35)
        await rejected("WEB_AUTHORITY_REJECTED", task)
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_config_plans) AS plans, "
                "(SELECT count(*) FROM public.bot_web_operations) AS operations"
            )
        ).fetchone()
    assert row == {"plans": 0, "operations": 0}


async def test_fresh_ticket_replays_original_receipt_without_reapplying_old_revision(environment):
    env = environment
    plan1 = await env.plan()
    original = await env.apply(plan1, "stable-apply")
    plan2 = await env.plan(expected=1)
    await env.apply(plan2)
    assert await env.apply(plan1, "stable-apply") == original
    await rejected("IDEMPOTENCY_CONFLICT", env.apply(plan2, "stable-apply"))
    assert (await env.read())["desired_revision"] == 2
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_config_revisions) AS revisions, "
                "(SELECT count(*) FROM public.bot_outbox_events WHERE operation_id='stable-apply') AS events"
            )
        ).fetchone()
    assert row == {"revisions": 2, "events": 1}


async def test_two_independent_target_acceptors_cas_one_desired_revision(environment):
    env = environment
    other = BotStore(env.store.pool.conninfo, env.store.bot_id)
    await other.open()
    try:
        controls = [env.control, BotControl(other)]
        plans = [await env.plan(), await env.plan()]
        barrier = asyncio.Barrier(2)

        async def contender(number):
            operation = "racer-" + str(number)
            plan = plans[number]
            payload = {
                "component_id": COMPONENT,
                "expected_revision": 0,
                "plan_id": plan["plan_id"],
                "plan_digest": plan["plan_digest"],
            }
            ticket = await env.ticket("config.apply", operation, payload)
            await barrier.wait()
            return await controls[number].apply(ticket, operation, payload)

        results = await asyncio.gather(contender(0), contender(1), return_exceptions=True)
        assert sum(isinstance(result, dict) for result in results) == 1
        errors = [result for result in results if isinstance(result, ForestError)]
        assert len(errors) == 1 and errors[0].code == "REVISION_CONFLICT"
        assert (await env.read())["desired_revision"] == 1
    finally:
        await other.close()


async def test_publication_atomically_advances_effective_with_route_and_outbox(environment):
    env = environment
    await env.apply(await env.plan())
    lifecycle = await env.candidate()
    assert (await env.read())["effective_revision"] == 0
    route = await env.store.publish(env.lease, lifecycle["operation_id"], lifecycle["op_epoch"])
    config = await env.read()
    assert config["desired_revision"] == config["effective_revision"] == 1
    assert config["effective_values"] == VALUES
    assert route["generation_id"] == lifecycle["candidate_generation_id"]
    async with env.store.pool.connection() as conn:
        event = await (
            await conn.execute(
                "SELECT payload FROM public.bot_outbox_events WHERE event_id=%s",
                ("config:" + lifecycle["operation_id"],),
            )
        ).fetchone()
    assert event["payload"] == {"component_id": COMPONENT, "revision": 1}


async def test_new_desired_wins_while_old_publish_waits_and_rolls_back_entire_route(environment):
    env = environment
    await env.apply(await env.plan())
    lifecycle = await env.candidate()
    plan2 = await env.plan(expected=1)
    async with env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("SELECT * FROM public.bot_config_heads FOR UPDATE")
            apply_task = asyncio.create_task(env.apply(plan2))
            await wait_for_lock(env.store, "SELECT * FROM public.bot_config_heads%FOR UPDATE")
            publish_task = asyncio.create_task(
                env.store.publish(env.lease, lifecycle["operation_id"], lifecycle["op_epoch"])
            )
            await wait_for_lock(env.store, "SELECT * FROM public.bot_runtime_state%FOR UPDATE")
        assert (await apply_task)["desired_revision"] == 2
        with pytest.raises(RaiseException, match="CONFIGURATION_SUPERSEDED"):
            await publish_task
    route = await env.store.get_route(COMPONENT)
    assert route["generation_id"] is None and route["route_epoch"] == 0
    assert (await env.store.read_operation(lifecycle["operation_id"]))["phase"] == "QUIESCENT"
    config = await env.read()
    assert config["desired_revision"] == 2 and config["effective_revision"] == 0
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT count(*) AS events FROM public.bot_outbox_events WHERE event_id=%s",
                ("config:" + lifecycle["operation_id"],),
            )
        ).fetchone()
    assert row["events"] == 0


@pytest.mark.parametrize("missing", ["package_digest", "config", "grants"])
async def test_incomplete_desired_configuration_cannot_pass_null_publication_guard(
    environment, missing
):
    env = environment
    malformed = deepcopy(VALUES)
    del malformed[missing]
    # A faulty trusted issuer bypasses the gateway schema; the values writer
    # now refuses this incomplete body before any revision or candidate exists.
    await rejected("CONFIGURATION_VALUES_INVALID", env.plan(values=malformed))
    assert (await env.read())["effective_revision"] == 0
    assert (await env.read())["desired_revision"] == 0


async def test_different_grants_or_revoked_configuration_cannot_activate(environment):
    env = environment
    await env.apply(await env.plan())
    expanded = {**VALUES, "grants": ["state.read", "state.write"]}
    # Real activation provenance is checked before Worker preparation too.
    await rejected("CONFIGURATION_SUPERSEDED", env.candidate(values=expanded))
    await env.store.revoke_policy(env.lease, expected_revision=1)
    await rejected("POLICY_REVOKED", env.control.bind_activation(env.lease, COMPONENT, 1, "late"))
    assert (await env.read())["effective_revision"] == 0


async def test_publication_cannot_outlive_operation_while_configuration_trigger_waits(environment):
    env = environment
    await env.apply(await env.plan())
    lifecycle = await env.candidate()
    await env.store.renew_operation(
        env.lease, lifecycle["operation_id"], lifecycle["op_epoch"], ttl_seconds=0.3
    )
    async with env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("SELECT * FROM public.bot_config_heads FOR UPDATE")
            task = asyncio.create_task(
                env.store.publish(env.lease, lifecycle["operation_id"], lifecycle["op_epoch"])
            )
            await wait_for_lock(
                env.store, "SELECT h.component_id FROM public.bot_config_heads h%FOR UPDATE OF h"
            )
            await asyncio.sleep(0.35)
        await rejected("STALE_OPERATION", task)
    assert (await env.store.get_route(COMPONENT))["generation_id"] is None
    assert (await env.read())["effective_revision"] == 0


async def test_even_signed_tickets_require_complete_subject_session_and_revision_claims(
    environment,
):
    env = environment
    ticket = await env.ticket("config.read", "read", {"component_id": COMPONENT})
    for key in ("ticket_id", "subject_id", "account_revision", "session_ref", "session_revision"):
        malformed = dict(ticket["claims"])
        del malformed[key]
        await rejected(
            "WEB_AUTHORITY_REJECTED", env.control.read(env.sign(malformed), "read", COMPONENT)
        )
    for key in ("key_revision", "account_revision", "session_revision", "policy_revision"):
        for value in (True, 0, -1, "1"):
            malformed = {**ticket["claims"], key: value}
            await rejected(
                "WEB_AUTHORITY_REJECTED", env.control.read(env.sign(malformed), "read", COMPONENT)
            )
    for key in ("subject_id", "session_ref", "ticket_id"):
        malformed = {**ticket["claims"], key: ""}
        await rejected(
            "WEB_AUTHORITY_REJECTED", env.control.read(env.sign(malformed), "read", COMPONENT)
        )


async def test_signed_ticket_timestamps_require_explicit_offset_and_bounded_lifetime(environment):
    env = environment
    ticket = await env.ticket("config.read", "read", {"component_id": COMPONENT})
    for key in ("issued_at", "expires_at"):
        malformed = dict(ticket["claims"])
        malformed[key] = datetime.fromisoformat(malformed[key]).replace(tzinfo=None).isoformat()
        await rejected(
            "WEB_AUTHORITY_REJECTED", env.control.read(env.sign(malformed), "read", COMPONENT)
        )
    malformed = dict(ticket["claims"])
    malformed["expires_at"] = (
        datetime.fromisoformat(malformed["issued_at"]) + timedelta(seconds=31)
    ).isoformat()
    await rejected(
        "WEB_AUTHORITY_REJECTED", env.control.read(env.sign(malformed), "read", COMPONENT)
    )


async def test_expired_host_cannot_bind_activation_after_business_head_wait(environment):
    env = environment
    await env.apply(await env.plan())
    await env.store.renew_host(env.lease, ttl_seconds=0.3)
    async with env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("SELECT * FROM public.bot_config_heads FOR UPDATE")
            task = asyncio.create_task(
                env.control.bind_activation(env.lease, COMPONENT, 1, "expired")
            )
            await wait_for_lock(env.store, "SELECT * FROM public.bot_config_heads%FOR UPDATE")
            await asyncio.sleep(0.35)
        await rejected("STALE_HOST", task)
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute("SELECT count(*) AS n FROM public.bot_config_activations")
        ).fetchone()
    assert row["n"] == 0


async def test_expired_host_cannot_establish_trust_after_insert_lock_wait(environment):
    env = environment
    async with env.store.pool.connection() as conn:
        await conn.execute("DELETE FROM public.bot_web_issuers")
    await env.store.renew_host(env.lease, ttl_seconds=0.3)
    async with env.store.pool.connection() as blocker:
        async with blocker.transaction():
            # SHARE permits the initial SELECT FOR UPDATE but blocks INSERT's
            # table lock, exercising the final authority check after that wait.
            await blocker.execute("LOCK TABLE public.bot_web_issuers IN SHARE MODE")
            task = asyncio.create_task(
                env.control.trust(
                    env.lease,
                    issuer_id=env.authority.issuer_id,
                    key_revision=env.authority.key_revision,
                    public_key=env.authority.public_key,
                )
            )
            await wait_for_lock(env.store, "INSERT INTO public.bot_web_issuers%")
            await asyncio.sleep(0.35)
        await rejected("STALE_HOST", task)
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute("SELECT count(*) AS n FROM public.bot_web_issuers")
        ).fetchone()
    assert row["n"] == 0

"""Binding authority, configuration races, and final PostgreSQL fences."""

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from test_web_authority import environment, wait_for_lock  # noqa: F401

from forest.errors import ForestError
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres

VALUES = {
    "enabled": True,
    "api_base_url": "http://127.0.0.1:5700",
    "response_mode": "disabled",
    "response_template": "",
    "trigger_mentions": True,
    "trigger_prefix": "",
    "private_trigger": True,
    "admin_user_ids": [],
}


class BindingEnvironment:
    def __init__(self, env):
        self.env = env
        self.bindings = BindingStore(env.store)
        self.binding_id = str(uuid4())
        self.secret = env.vault.encrypt(
            json.dumps({"access_token": "api-secret", "secret": "ingress-secret"})
        )

    async def ticket(self, action, operation, payload, **changes):
        return await self.env.ticket(
            action,
            operation,
            payload,
            plan_digest=payload.get("plan_digest", "") if action == "binding.apply" else "",
            **changes,
        )

    async def create(self, binding_id=None, self_id="10001", operation=None):
        operation = operation or "create-" + uuid4().hex
        payload = {
            "binding_id": binding_id or self.binding_id,
            "self_id": self_id,
            "display_name": "测试 Bot 接入",
        }
        ticket = await self.ticket("binding.create", operation, payload)
        return await self.bindings.create(ticket, operation, payload)

    def plan_payload(self, expected=0, values=None, secret=None):
        return {
            "binding_id": self.binding_id,
            "expected_revision": expected,
            "values": deepcopy(VALUES if values is None else values),
            "credentials_changed": True,
            "secret_digest": hashlib.sha256((secret or self.secret).encode()).hexdigest(),
        }

    async def plan(self, expected=0, values=None, operation=None):
        operation = operation or "plan-" + uuid4().hex
        payload = self.plan_payload(expected, values)
        ticket = await self.ticket("binding.plan", operation, payload)
        return await self.bindings.plan(ticket, operation, payload, self.secret)

    async def apply(self, plan, operation=None):
        operation = operation or "apply-" + uuid4().hex
        payload = {
            "binding_id": self.binding_id,
            "expected_revision": plan["expected_revision"],
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
        }
        ticket = await self.ticket("binding.apply", operation, payload)
        return await self.bindings.apply(ticket, operation, payload)

    async def read(self):
        operation = "read-" + uuid4().hex
        ticket = await self.ticket("binding.read", operation, {"binding_id": self.binding_id})
        return await self.bindings.read(ticket, operation, self.binding_id)

    async def activate(self, revision=1, *, enabled=True, lease=None, client=None, epoch=None):
        epoch = (await self.read())["binding_epoch"] if epoch is None else epoch
        return await self.bindings.activate(
            lease or self.env.lease,
            self.binding_id,
            revision,
            enabled=enabled,
            client_incarnation=client or "client-" + uuid4().hex,
            expected_binding_epoch=epoch,
        )


@pytest_asyncio.fixture
async def binding(environment):  # noqa: F811
    result = BindingEnvironment(environment)
    await result.create()
    return result


async def rejected(code, action):
    with pytest.raises(ForestError) as caught:
        await action
    assert caught.value.code == code


async def test_identity_is_fixed_and_management_results_never_expose_secrets(binding):
    b = binding
    initial = await b.read()
    assert (initial["revision"], initial["effective_revision"], initial["state"]) == (
        0,
        0,
        "DISABLED",
    )
    plan = await b.plan()
    accepted = await b.apply(plan)
    before = await b.read()
    assert accepted["revision"] == before["revision"] == 1
    assert before["effective_revision"] == 0 and before["desired_values"] == VALUES
    route = await b.activate()
    after = await b.read()
    assert after["effective_revision"] == 1 and after["config"] == VALUES
    assert route["self_id"] == "10001" and after["state"] == "ONLINE"
    serialized = json.dumps([plan, accepted, before, after])
    assert all(secret not in serialized for secret in (b.secret, "api-secret", "ingress-secret"))
    assert await b.bindings.credentials(b.env.lease, b.binding_id) == b.secret
    async with b.env.store.pool.connection() as conn:
        with pytest.raises(psycopg.Error) as caught:
            await conn.execute(
                "UPDATE public.bot_bindings SET self_id='10002' WHERE binding_id=%s",
                (b.binding_id,),
            )
        assert caught.value.sqlstate == "55000"
    assert (await b.read())["self_id"] == "10001"


async def test_plan_ciphertext_is_bound_to_signed_payload(binding):
    b = binding
    payload = b.plan_payload()
    ticket = await b.ticket("binding.plan", "ciphertext-mismatch", payload)
    changed = b.env.vault.encrypt("different credentials")
    with pytest.raises(ForestError):
        await b.bindings.plan(ticket, "ciphertext-mismatch", payload, changed)
    async with b.env.store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_binding_plans")
            ).fetchone()
        )["n"] == 0


async def test_duplicate_binding_or_account_is_a_safe_conflict(binding):
    b = binding
    await rejected("BINDING_IDENTITY_CONFLICT", b.create(self_id="10002"))
    await rejected("BINDING_IDENTITY_CONFLICT", b.create(str(uuid4()), "10001"))
    assert (await b.read())["self_id"] == "10001"


async def test_activation_replay_is_idempotent_and_retired_client_cannot_be_revived(binding):
    b = binding
    await b.apply(await b.plan())
    original = await b.activate(client="client-first", epoch=1)
    assert await b.activate(client="client-first", epoch=1) == original
    await rejected("STALE_BINDING", b.activate(client="competing-client", epoch=1))
    second = await b.activate(client="client-second", epoch=2)
    assert (second["binding_epoch"], second["connection_epoch"]) == (3, 2)
    await rejected("IDEMPOTENCY_CONFLICT", b.activate(client="client-first", epoch=3))
    async with b.env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_binding_clients) AS clients, "
                "(SELECT count(*) FROM public.bot_audit_records WHERE action='binding.activated') AS activations"
            )
        ).fetchone()
    assert row == {"clients": 2, "activations": 2}


async def test_same_client_incarnation_cannot_be_used_by_another_binding(binding):
    b = binding
    await b.apply(await b.plan())
    await b.activate(client="same-client", epoch=1)
    other = BindingEnvironment(b.env)
    await other.create(self_id="10002")
    await other.apply(await other.plan())
    await rejected("IDEMPOTENCY_CONFLICT", other.activate(client="same-client", epoch=1))
    assert (await other.read())["effective_revision"] == 0


async def test_credential_encryption_is_frozen_once_across_concurrent_retries(binding):
    b = binding
    digest = "sha256:" + "a" * 64
    candidates = [b.env.vault.encrypt("same plaintext") for _ in range(6)]
    results = await asyncio.gather(
        *[
            b.bindings.stage_credentials(
                b.env.lease, b.binding_id, b.env.subject, "credential-operation", digest, cipher
            )
            for cipher in candidates
        ]
    )
    assert len(set(results)) == 1 and results[0] in candidates
    assert b.env.vault.decrypt(results[0]) == "same plaintext"
    async with b.env.store.pool.connection() as conn:
        rows = await (await conn.execute("SELECT * FROM public.bot_binding_credentials")).fetchall()
        assert len(rows) == 1 and rows[0]["secret_ciphertext"] == results[0]
        with pytest.raises(psycopg.Error) as caught:
            await conn.execute(
                "UPDATE public.bot_binding_credentials SET secret_ciphertext='other'"
            )
        assert caught.value.sqlstate == "55000"


@pytest.mark.parametrize("changed", ["subject", "binding", "digest"])
async def test_credential_operation_cannot_be_retargeted(binding, changed):
    b = binding
    digest = "b" * 64
    await b.bindings.stage_credentials(
        b.env.lease, b.binding_id, b.env.subject, "stable", digest, b.secret
    )
    target, subject = b.binding_id, b.env.subject
    if changed == "binding":
        target = str(uuid4())
        await b.create(target, "10002")
    elif changed == "subject":
        subject = "another-subject"
    else:
        digest = "c" * 64
    await rejected(
        "IDEMPOTENCY_CONFLICT",
        b.bindings.stage_credentials(b.env.lease, target, subject, "stable", digest, b.secret),
    )


async def test_signed_plan_freezes_caller_values_and_claims_before_business_wait(binding):
    b = binding
    payload = b.plan_payload()
    original = deepcopy(payload["values"])
    ticket = await b.ticket("binding.plan", "mutable-plan", payload)
    async with b.env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("LOCK TABLE public.bot_binding_plans IN ACCESS EXCLUSIVE MODE")
            task = asyncio.create_task(b.bindings.plan(ticket, "mutable-plan", payload, b.secret))
            await wait_for_lock(b.env.store, "INSERT INTO public.bot_binding_plans%")
            payload["values"]["api_base_url"] = "http://evil.test"
            ticket["claims"]["subject_id"] = "forged-subject"
        result = await task
    assert result["values"] == original
    async with b.env.store.pool.connection() as conn:
        plan = await (
            await conn.execute("SELECT values,subject_id FROM public.bot_binding_plans")
        ).fetchone()
        receipt = await (
            await conn.execute(
                "SELECT subject_id,ticket_basis FROM public.bot_web_operations WHERE operation_id='mutable-plan'"
            )
        ).fetchone()
    assert plan == {"values": original, "subject_id": b.env.subject}
    assert receipt["subject_id"] == receipt["ticket_basis"]["subject_id"] == b.env.subject


@pytest.mark.parametrize(
    "invalid", [float("nan"), {"x": float("inf")}, "\ud800", list(range(20000))]
)
async def test_nonfinite_or_unbounded_configuration_cannot_enter_a_plan(binding, invalid):
    b = binding
    payload = b.plan_payload()
    payload["values"]["injected"] = invalid
    with pytest.raises(ForestError) as caught:
        await b.bindings.plan({}, "invalid", payload, b.secret)
    assert caught.value.code == "BINDING_INVALID"


@pytest.mark.parametrize("read_kind", ["credentials", "desired"])
async def test_host_expiry_after_secret_read_wait_rejects_result(binding, read_kind):
    b = binding
    await b.apply(await b.plan())
    async with b.env.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '0.4 seconds'"
        )
    async with b.env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "LOCK TABLE public.bot_binding_revisions IN ACCESS EXCLUSIVE MODE"
            )
            action = (
                b.bindings.credentials(b.env.lease, b.binding_id)
                if read_kind == "credentials"
                else b.bindings.desired(b.env.lease)
            )
            task = asyncio.create_task(action)
            await wait_for_lock(b.env.store, "SELECT %bot_binding_revisions%")
            await asyncio.sleep(0.45)
        await rejected("STALE_HOST", task)


async def test_credential_staging_cannot_outlive_host_while_business_insert_waits(binding):
    b = binding
    async with b.env.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '0.4 seconds'"
        )
    async with b.env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("LOCK TABLE public.bot_binding_credentials IN EXCLUSIVE MODE")
            task = asyncio.create_task(
                b.bindings.stage_credentials(
                    b.env.lease, b.binding_id, b.env.subject, "expires", "a" * 64, b.secret
                )
            )
            await wait_for_lock(b.env.store, "INSERT INTO public.bot_binding_credentials%")
            await asyncio.sleep(0.45)
        await rejected("STALE_HOST", task)
    async with b.env.store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_binding_credentials")
            ).fetchone()
        )["n"] == 0


async def test_two_independent_acceptors_cas_one_binding_revision(binding):
    b = binding
    plans = [await b.plan(), await b.plan()]
    other = BotStore(b.env.store.pool.conninfo, b.env.store.bot_id)
    await other.open()
    try:
        stores = [b.bindings, BindingStore(other)]
        barrier = asyncio.Barrier(2)

        async def contender(index):
            plan = plans[index]
            payload = {
                "binding_id": b.binding_id,
                "expected_revision": 0,
                "plan_id": plan["plan_id"],
                "plan_digest": plan["plan_digest"],
            }
            operation = "competing-apply-" + str(index)
            ticket = await b.ticket("binding.apply", operation, payload)
            await barrier.wait()
            return await stores[index].apply(ticket, operation, payload)

        results = await asyncio.gather(contender(0), contender(1), return_exceptions=True)
        assert sum(isinstance(result, dict) for result in results) == 1
        failures = [result for result in results if isinstance(result, ForestError)]
        assert len(failures) == 1 and failures[0].code == "REVISION_CONFLICT"
        assert (await b.read())["revision"] == 1
    finally:
        await other.close()


async def test_apply_idempotency_keeps_original_receipt_after_later_revision(binding):
    b = binding
    first = await b.plan()
    result = await b.apply(first, "stable-apply")
    second = await b.plan(expected=1, values={**VALUES, "enabled": False})
    await b.apply(second)
    assert await b.apply(first, "stable-apply") == result
    await rejected("IDEMPOTENCY_CONFLICT", b.apply(second, "stable-apply"))
    async with b.env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_binding_revisions) AS revisions, "
                "(SELECT count(*) FROM public.bot_outbox_events WHERE operation_id='stable-apply') AS receipts"
            )
        ).fetchone()
    assert row == {"revisions": 2, "receipts": 1}


async def test_ticket_cannot_be_retargeted_to_another_binding_or_bot(binding):
    b = binding
    other = str(uuid4())
    await b.create(other, "10002")
    payload = {"binding_id": b.binding_id}
    ticket = await b.ticket("binding.read", "scoped-read", payload)
    await rejected("WEB_AUTHORITY_REJECTED", b.bindings.read(ticket, "scoped-read", other))
    claims = dict(ticket["claims"], bot_id="another-bot")
    await rejected(
        "WEB_AUTHORITY_REJECTED", b.bindings.read(b.env.sign(claims), "scoped-read", b.binding_id)
    )
    await rejected(
        "BOT_SCOPE_MISMATCH",
        b.bindings.credentials(replace(b.env.lease, bot_id="another-bot"), b.binding_id),
    )


async def test_stale_host_cannot_activate_or_read_credentials(binding):
    b = binding
    await b.apply(await b.plan())
    first = await b.activate()
    await b.env.store.release_host(b.env.lease)
    replacement = await b.env.store.acquire_host(
        "replacement", "replacement-incarnation", ttl_seconds=120
    )
    await rejected("STALE_HOST", b.bindings.credentials(b.env.lease, b.binding_id))
    await rejected("STALE_HOST", b.activate())
    async with b.env.store.pool.connection() as conn, conn.transaction():
        await rejected("BINDING_OFFLINE", b.bindings._guard(conn, replacement, b.binding_id))
    second = await b.activate(lease=replacement)
    assert second["connection_epoch"] == first["connection_epoch"] + 1
    assert second["host_incarnation"] == replacement.incarnation


async def test_pause_keeps_receive_and_control_but_refuses_business_dispatch(binding):
    b = binding
    await b.apply(await b.plan())
    route = await b.activate()
    await b.env.store.set_maintenance(b.env.lease, 1, False)
    async with b.env.store.pool.connection() as conn, conn.transaction():
        row = await b.bindings._guard(
            conn,
            b.env.lease,
            b.binding_id,
            route["binding_epoch"],
            data=False,
            require_receive=True,
        )
        await b.bindings._final_guard(conn, b.env.lease, row, data=False, require_receive=True)
    async with b.env.store.pool.connection() as conn, conn.transaction():
        await rejected("DATA_ADMISSION_CLOSED", b.bindings._guard(conn, b.env.lease, b.binding_id))
    assert (await b.read())["receive_enabled"] is True
    await b.env.store.set_maintenance(b.env.lease, 2, True)
    async with b.env.store.pool.connection() as conn, conn.transaction():
        assert (await b.bindings._guard(conn, b.env.lease, b.binding_id))["binding_epoch"] == route[
            "binding_epoch"
        ]


async def test_drain_closes_new_chat_dispatch_but_not_ingress(binding):
    b = binding
    await b.apply(await b.plan())
    await b.activate()
    await b.bindings.drain(b.env.lease, b.binding_id, 1)
    async with b.env.store.pool.connection() as conn, conn.transaction():
        assert (
            await b.bindings._guard(
                conn, b.env.lease, b.binding_id, data=False, require_receive=True
            )
        )["state"] == "DRAINING"
    async with b.env.store.pool.connection() as conn, conn.transaction():
        await rejected("DATA_ADMISSION_CLOSED", b.bindings._guard(conn, b.env.lease, b.binding_id))


async def test_disabled_and_superseded_binding_never_publish_wrong_effective_revision(binding):
    b = binding
    await b.apply(await b.plan())
    first = await b.activate()
    await b.apply(await b.plan(1, {**VALUES, "enabled": False}))
    await rejected("CONFIGURATION_SUPERSEDED", b.activate())
    await rejected("CONFIGURATION_CONFLICT", b.activate(2, enabled=True))
    disabled = await b.activate(2, enabled=False)
    assert disabled["effective_revision"] == 2 and disabled["state"] == "DISABLED"
    assert not disabled["enabled"] and not disabled["receive_enabled"]
    assert disabled["binding_epoch"] == first["binding_epoch"] + 1
    async with b.env.store.pool.connection() as conn, conn.transaction():
        await rejected(
            "BINDING_OFFLINE",
            b.bindings._guard(conn, b.env.lease, b.binding_id, data=False, require_receive=True),
        )


async def test_current_policy_revocation_prevents_binding_activation(binding):
    b = binding
    await b.apply(await b.plan())
    await b.env.store.revoke_policy(b.env.lease, 1)
    await rejected("POLICY_REVOKED", b.activate())
    assert (await b.read())["effective_revision"] == 0


@pytest.mark.parametrize("operation_kind", ["plan", "apply"])
async def test_ticket_expiring_after_outbox_lock_rolls_back_every_management_fact(
    binding, operation_kind
):
    b = binding
    operation = "expires-after-business-lock"
    if operation_kind == "plan":
        payload = b.plan_payload()
    else:
        plan = await b.plan()
        payload = {
            "binding_id": b.binding_id,
            "expected_revision": 0,
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
        }
    ticket = await b.ticket("binding." + operation_kind, operation, payload)
    ticket = b.env.sign(
        dict(ticket["claims"], expires_at=(datetime.now(UTC) + timedelta(seconds=0.4)).isoformat())
    )
    async with b.env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("LOCK TABLE public.bot_outbox_events IN ACCESS EXCLUSIVE MODE")
            action = (
                b.bindings.plan(ticket, operation, payload, b.secret)
                if operation_kind == "plan"
                else b.bindings.apply(ticket, operation, payload)
            )
            task = asyncio.create_task(action)
            await wait_for_lock(b.env.store, "INSERT INTO public.bot_outbox_events%")
            await asyncio.sleep(0.45)
        await rejected("WEB_AUTHORITY_REJECTED", task)
    async with b.env.store.pool.connection() as conn:
        result = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_web_operations WHERE operation_id=%s) AS operations, "
                "(SELECT count(*) FROM public.bot_binding_revisions) AS revisions, "
                "(SELECT count(*) FROM public.bot_audit_records WHERE operation_id=%s) AS audits",
                (operation, operation),
            )
        ).fetchone()
    assert result == {"operations": 0, "revisions": 0, "audits": 0}
    assert (await b.read())["revision"] == 0


async def test_activation_cannot_outlive_host_lease_while_final_outbox_waits(binding):
    b = binding
    await b.apply(await b.plan())
    async with b.env.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '0.4 seconds'"
        )
    async with b.env.store.pool.connection() as blocker:
        async with blocker.transaction():
            await blocker.execute("LOCK TABLE public.bot_outbox_events IN ACCESS EXCLUSIVE MODE")
            task = asyncio.create_task(b.activate())
            await wait_for_lock(b.env.store, "INSERT INTO public.bot_outbox_events%")
            await asyncio.sleep(0.45)
        await rejected("STALE_HOST", task)
    row = await b.read()
    assert row["effective_revision"] == 0 and row["connection_epoch"] == 0


async def test_actor_from_binding_a_cannot_write_binding_b_conversation(binding):
    b = binding
    await b.apply(await b.plan())
    await b.activate()
    other = BindingEnvironment(b.env)
    await other.create(self_id="10002")
    await other.apply(await other.plan())
    await other.activate()
    messages = MessageStore(b.env.store, b.bindings)
    base = {
        "platform": "onebot-v11",
        "self_id": "10001",
        "channel_kind": "group",
        "channel_id": "30001",
        "thread_id": "",
        "platform_message_id": "1",
        "sent_at": "2026-09-13T00:00:00Z",
        "author": {"external_user_id": "20001", "display_name": "用户", "is_self": False},
        "segments": [{"type": "text", "text": "消息"}],
    }
    first = await messages.ingest(b.env.lease, b.binding_id, base, decision="NO_TRIGGER")
    second = await messages.ingest(
        b.env.lease, other.binding_id, {**base, "self_id": "10002"}, decision="NO_TRIGGER"
    )
    first_actor = await messages.acquire_actor(b.env.lease, first["conversation_id"], "same-actor")
    await messages.acquire_actor(b.env.lease, second["conversation_id"], "same-actor")
    forged = replace(first_actor, conversation_id=second["conversation_id"])
    await rejected("STALE_ACTOR", messages.select_input(forged, "wrong-binding"))
    async with b.env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT context_applied_seq FROM public.bot_conversations WHERE conversation_id=%s",
                (second["conversation_id"],),
            )
        ).fetchone()
    assert row["context_applied_seq"] == 0

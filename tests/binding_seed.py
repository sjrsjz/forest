"""Create operational test Bindings through actual signed management writes."""

import base64
import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from weakref import WeakKeyDictionary

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from forest.packages import digest_json
from forest.storage.bindings import BindingStore
from forest.storage.control import BotControl

_KEYS = WeakKeyDictionary()
VALUES = {
    "enabled": True, "api_base_url": "http://127.0.0.1:5700", "response_mode": "disabled",
    "response_template": "", "trigger_mentions": True, "trigger_prefix": "",
    "private_trigger": True, "admin_user_ids": [],
}


async def signed_ticket_factory(store, lease=None):
    """Reuse this fixture's real issuer; only a current Host may bootstrap it."""
    key = _KEYS.get(store)
    if key is None:
        if lease is None:
            raise AssertionError("An unregistered test issuer needs the actual Host lease")
        key = Ed25519PrivateKey.generate()
        _KEYS[store] = key
    if lease is not None:
        await BotControl(store).trust(lease, issuer_id="test-bindings", key_revision=1,
            public_key=base64.b64encode(key.public_key().public_bytes_raw()).decode())

    async def ticket(action, operation, payload, **changes):
        now = datetime.now(UTC)
        claims = {
            "ticket_id": str(uuid4()), "issuer_id": "test-bindings", "subject_id": "test-admin",
            "session_ref": "test-session", "bot_id": store.bot_id, "operation_id": operation,
            "action": action, "input_digest": digest_json(payload), "plan_digest": changes.get("plan_digest", ""),
            "issued_at": (now - timedelta(seconds=1)).isoformat(), "expires_at": (now + timedelta(seconds=28)).isoformat(),
            "key_revision": 1, "account_revision": 1, "session_revision": 1,
            "policy_revision": (await store.runtime())["policy_revision"],
        }
        return {"claims": claims, "signature": base64.b64encode(key.sign(digest_json(claims).encode("ascii"))).decode()}

    return ticket


# Older fixture callers retain the same signed protocol while migrating names.
_ticket_factory = signed_ticket_factory


async def create_online_binding(store, lease, binding_id, self_id, *, name="Test", ticket=None, values=None):
    ticket = ticket or await signed_ticket_factory(store, lease)
    bindings = BindingStore(store)
    operation = "seed-create-" + uuid4().hex
    payload = {"binding_id": binding_id, "self_id": self_id, "display_name": name}
    await bindings.create(await ticket("binding.create", operation, payload), operation, payload)
    secret = "opaque-test-binding-credentials"
    operation = "seed-plan-" + uuid4().hex
    payload = {"binding_id": binding_id, "expected_revision": 0, "values": values or VALUES,
        "credentials_changed": True, "secret_digest": hashlib.sha256(secret.encode()).hexdigest()}
    plan = await bindings.plan(await ticket("binding.plan", operation, payload), operation, payload, secret)
    operation = "seed-apply-" + uuid4().hex
    payload = {"binding_id": binding_id, **{k: plan[k] for k in ("expected_revision", "plan_id", "plan_digest")}}
    await bindings.apply(await ticket("binding.apply", operation, payload, plan_digest=payload["plan_digest"]), operation, payload)
    return await bindings.activate(lease, binding_id, 1, enabled=True,
        client_incarnation="seed-client-" + uuid4().hex, expected_binding_epoch=1)


async def reconnect_binding(store, lease, binding_id="binding-a"):
    async with store.pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT revision,binding_epoch FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s",
            (store.bot_id, binding_id),
        )).fetchone()
    return await BindingStore(store).activate(lease, binding_id, row["revision"], enabled=True,
        client_incarnation="reconnected-" + uuid4().hex, expected_binding_epoch=row["binding_epoch"])

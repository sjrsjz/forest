"""Short-lived, purpose-bound Web authority. Tickets never reach a browser or Worker."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from datetime import timedelta
from uuid import uuid4

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import SecretVault
from forest.storage.postgres import PostgresStore

READ_ACTIONS = frozenset({
    "config.read", "config.history", "bot.runtime.read", "job.read",
    "agent.config.read", "agent.read", "memory.config.read", "memory.read",
    "memory.forget.read", "memory.association.read", "event.read", "component_job.read",
    "binding.read", "message.read",
})


def encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


class WebAuthority:
    def __init__(self, store: PostgresStore, vault: SecretVault, issuer_id: str):
        self.store, self.vault, self.issuer_id = store, vault, issuer_id
        self.key: Ed25519PrivateKey | None = None
        self.public_key: str | None = None
        self.key_revision = 1
        self._secret_digest_key: bytes | None = None

    async def initialize(self) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170783)")
            row = await (await conn.execute(
                "SELECT * FROM web_authority_keys WHERE issuer_id=%s", (self.issuer_id,)
            )).fetchone()
            if row is None:
                key = Ed25519PrivateKey.generate()
                await conn.execute(
                    "INSERT INTO web_authority_keys(issuer_id,public_key,encrypted_private_key) "
                    "VALUES (%s,%s,%s)",
                    (self.issuer_id, encode(key.public_key().public_bytes_raw()),
                     self.vault.encrypt(encode(key.private_bytes_raw()))),
                )
                row = await (await conn.execute(
                    "SELECT * FROM web_authority_keys WHERE issuer_id=%s", (self.issuer_id,)
                )).fetchone()
            self.key = Ed25519PrivateKey.from_private_bytes(
                base64.b64decode(self.vault.decrypt(row["encrypted_private_key"]), validate=True)
            )
            self.public_key = encode(self.key.public_key().public_bytes_raw())
            if self.public_key != row["public_key"]:
                raise ForestError("AUTHORITY_KEY_MISMATCH", "Web authority key does not match", 503)
            self.key_revision = row["key_revision"]
            digest_key = await (await conn.execute(
                "SELECT encrypted_key FROM web_secret_digest_keys WHERE issuer_id=%s",
                (self.issuer_id,),
            )).fetchone()
            if digest_key is None:
                encoded = encode(secrets.token_bytes(32))
                await conn.execute(
                    "INSERT INTO web_secret_digest_keys(issuer_id,encrypted_key) VALUES (%s,%s)",
                    (self.issuer_id, self.vault.encrypt(encoded)),
                )
                self._secret_digest_key = base64.b64decode(encoded, validate=True)
            else:
                self._secret_digest_key = base64.b64decode(
                    self.vault.decrypt(digest_key["encrypted_key"]), validate=True
                )

    def secret_digest(self, purpose: str, value: str) -> str:
        """Create a stable, purpose-bound idempotency digest without a password oracle."""
        if self._secret_digest_key is None or not purpose or not isinstance(value, str):
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        material = purpose.encode("utf-8") + b"\x00" + value.encode("utf-8")
        digest = hmac.new(self._secret_digest_key, material, hashlib.sha256).hexdigest()
        return "hmac-sha256:" + digest

    async def issue(self, *, subject_id: str, session_token_hash: str, bot_id: str,
                    action: str, operation_id: str, payload: dict, policy_revision: int,
                    plan_digest: str = "") -> dict:
        if self.key is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        async with self.store.pool.connection() as conn, conn.transaction():
            account = await self.store._active_account(conn, subject_id)
            if account["role"] != "admin" and action not in READ_ACTIONS:
                raise ForestError("FORBIDDEN", "Current account cannot perform this operation", 403)
            session = await (await conn.execute(
                "SELECT * FROM web_sessions WHERE token_hash=%s AND account_id=%s FOR UPDATE",
                (session_token_hash, subject_id),
            )).fetchone()
            now = (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]
            if session is None or session["revoked_at"] or session["expires_at"] <= now:
                raise ForestError("UNAUTHENTICATED", "Web session has expired", 401)
            claims = {
                "ticket_id": str(uuid4()), "issuer_id": self.issuer_id,
                "key_revision": self.key_revision, "subject_id": subject_id,
                "account_revision": account["revision"], "session_ref": session_token_hash,
                "session_revision": session["revision"], "bot_id": bot_id,
                "operation_id": operation_id, "action": action,
                "input_digest": digest_json(payload), "plan_digest": plan_digest,
                "policy_revision": policy_revision, "issued_at": now.isoformat(),
                "expires_at": min(now + timedelta(seconds=30), session["expires_at"]).isoformat(),
            }
            signature = encode(self.key.sign(digest_json(claims).encode("ascii")))
            return {"claims": claims, "signature": signature}

    async def issue_delegation(
        self, *, subject_id: str, bot_id: str, operation_id: str, payload: dict,
        policy_revision: int, delegation_id: str, expected_account_revision: int,
        expected_key_revision: int,
    ) -> dict:
        """Renew one durable Job's dispatch authority without a browser session.

        A live account and the exact accepted account/key revisions are required.
        Logging out does not cancel a schedule. Revocation bounds match Web tickets:
        an already issued proof can remain usable for at most thirty seconds.
        The target repository also verifies the specific delegation and occurrence.
        """
        if self.key is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        if (not isinstance(delegation_id, str) or not 1 <= len(delegation_id) <= 128
                or type(expected_account_revision) is not int or expected_account_revision < 1
                or type(expected_key_revision) is not int or expected_key_revision < 1):
            raise ForestError("JOB_DELEGATION_INVALID", "Invalid durable authority", 403)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._active_account(conn, subject_id, require_admin=True)
            account = await (await conn.execute(
                "SELECT revision FROM web_accounts WHERE id=%s", (subject_id,),
            )).fetchone()
            key = await (await conn.execute(
                "SELECT key_revision,public_key FROM web_authority_keys "
                "WHERE issuer_id=%s FOR SHARE", (self.issuer_id,),
            )).fetchone()
            if (account["revision"] != expected_account_revision or key is None
                    or key["key_revision"] != expected_key_revision
                    or key["key_revision"] != self.key_revision
                    or key["public_key"] != self.public_key):
                raise ForestError("JOB_DELEGATION_REVOKED", "Account or authority changed", 403)
            now = (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]
            claims = {
                "ticket_id": str(uuid4()), "issuer_id": self.issuer_id,
                "key_revision": self.key_revision, "subject_id": subject_id,
                "account_revision": account["revision"], "session_ref": "delegation:" + delegation_id,
                "session_revision": 1, "bot_id": bot_id,
                "operation_id": operation_id, "action": "job.run",
                "input_digest": digest_json(payload), "plan_digest": "",
                "policy_revision": policy_revision, "issued_at": now.isoformat(),
                "expires_at": (now + timedelta(seconds=30)).isoformat(),
            }
            return {"claims": claims, "signature": encode(self.key.sign(digest_json(claims).encode("ascii")))}

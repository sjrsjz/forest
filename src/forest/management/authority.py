"""Short-lived, purpose-bound Web authority. Tickets never reach a browser or Worker."""

from __future__ import annotations

import base64
from datetime import timedelta
from uuid import uuid4

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import SecretVault
from forest.storage.postgres import PostgresStore


def encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


class WebAuthority:
    def __init__(self, store: PostgresStore, vault: SecretVault, issuer_id: str):
        self.store, self.vault, self.issuer_id = store, vault, issuer_id
        self.key: Ed25519PrivateKey | None = None
        self.public_key: str | None = None
        self.key_revision = 1

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

    async def issue(self, *, subject_id: str, session_token_hash: str, bot_id: str,
                    action: str, operation_id: str, payload: dict, policy_revision: int,
                    plan_digest: str = "") -> dict:
        if self.key is None:
            raise ForestError("AUTHORITY_UNAVAILABLE", "Web authority is unavailable", 503)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._active_account(conn, subject_id, require_admin=True)
            account = await (await conn.execute(
                "SELECT revision FROM web_accounts WHERE id=%s", (subject_id,)
            )).fetchone()
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

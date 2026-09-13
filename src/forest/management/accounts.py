"""Deployment account lifecycle with reauthentication, CAS and last-admin safety."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json

_ACCOUNT_KEYS = ("id", "username", "role", "disabled", "revision", "created_at")


def _public(row: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: row[key].isoformat() if isinstance(row[key], datetime) else row[key]
        for key in _ACCOUNT_KEYS
    }
    result["active_sessions"] = int(row.get("active_sessions", 0))
    return result


class AccountManager:
    """Own account mutations in the deployment database.

    All account writes share one advisory transaction lock. This prevents two
    concurrent demotions from both observing another administrator and removing
    the final live administrator. The caller hashes passwords outside database
    transactions; the exact actor password verifier is checked again under lock.
    """

    def __init__(self, store, vault):
        self.store = store
        self.vault = vault

    async def _authorize(
        self,
        conn,
        subject_id: str,
        session_token_hash: str,
        *,
        expected_password_hash: str | None = None,
        expected_revision: int | None = None,
        require_admin: bool = True,
    ) -> dict[str, Any]:
        actor = await self.store._active_account(
            conn, subject_id, require_admin=require_admin
        )
        await self.store._active_session(conn, session_token_hash, subject_id)
        actor = await (await conn.execute(
            "SELECT id,username,password_hash,role,disabled,revision,created_at "
            "FROM web_accounts WHERE id=%s FOR UPDATE",
            (subject_id,),
        )).fetchone()
        if (
            actor is None
            or expected_password_hash is not None
            and actor["password_hash"] != expected_password_hash
            or expected_revision is not None
            and actor["revision"] != expected_revision
        ):
            raise ForestError(
                "REAUTHENTICATION_STALE",
                "Account credentials changed; authenticate again",
                409,
            )
        return actor

    async def credentials(self, subject_id: str) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn:
            return await (await conn.execute(
                "SELECT id,username,password_hash,role,disabled,revision "
                "FROM web_accounts WHERE id=%s",
                (subject_id,),
            )).fetchone()

    async def list(
        self, *, subject_id: str, session_token_hash: str
    ) -> list[dict[str, Any]]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                "SELECT a.id,a.username,a.role,a.disabled,a.revision,a.created_at,"
                "count(s.token_hash) FILTER (WHERE s.revoked_at IS NULL "
                "AND s.expires_at>clock_timestamp()) AS active_sessions "
                "FROM web_accounts a LEFT JOIN web_sessions s ON s.account_id=a.id "
                "GROUP BY a.id ORDER BY a.created_at,a.id"
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return [_public(row) for row in rows]

    async def create(
        self,
        *,
        subject_id: str,
        session_token_hash: str,
        expected_password_hash: str,
        username: str,
        password_hash: str,
        role: str,
        secret_digest: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        username = username.strip()
        semantic = digest_json({
            "action": "account.create",
            "username": username,
            "role": role,
            "password_secret_digest": secret_digest,
        })
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170784)")
            await self._authorize(
                conn,
                subject_id,
                session_token_hash,
                expected_password_hash=expected_password_hash,
            )
            previous = await (await conn.execute(
                "SELECT semantic_digest,result FROM web_account_requests "
                "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["semantic_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Idempotency key was used for different input", 409
                    )
                await self._authorize(
                    conn,
                    subject_id,
                    session_token_hash,
                    expected_password_hash=expected_password_hash,
                )
                return previous["result"]
            account_id, request_id = str(uuid4()), str(uuid4())
            try:
                row = await (await conn.execute(
                    "INSERT INTO web_accounts(id,username,password_hash,role) "
                    "VALUES (%s,%s,%s,%s) RETURNING *",
                    (account_id, username, password_hash, role),
                )).fetchone()
            except UniqueViolation:
                raise ForestError("ACCOUNT_NAME_CONFLICT", "Account name already exists", 409) from None
            result = _public(row)
            await conn.execute(
                "INSERT INTO web_account_requests(request_id,subject_id,target_account_id,"
                "idempotency_key,action,semantic_digest,result) VALUES (%s,%s,%s,%s,'account.create',%s,%s)",
                (request_id, subject_id, account_id, idempotency_key, semantic, Jsonb(result)),
            )
            await self._record(
                conn,
                request_id=request_id,
                subject_id=subject_id,
                target_account_id=account_id,
                action="account.create",
                details={"username": username, "role": role, "revision": 1},
            )
            await self._authorize(
                conn,
                subject_id,
                session_token_hash,
                expected_password_hash=expected_password_hash,
            )
            return result

    async def plan(
        self,
        *,
        subject_id: str,
        session_token_hash: str,
        target_account_id: str,
        expected_revision: int,
        role: str,
        disabled: bool,
        password_hash_ciphertext: str | None,
        password_secret_digest: str | None,
        plan_id: str,
        expires_at: datetime,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170784)")
            actor = await self._authorize(conn, subject_id, session_token_hash)
            target = await (await conn.execute(
                "SELECT id,username,role,disabled,revision,created_at FROM web_accounts "
                "WHERE id=%s FOR UPDATE",
                (target_account_id,),
            )).fetchone()
            if target is None:
                raise ForestError("ACCOUNT_NOT_FOUND", "Account does not exist", 404)
            if target["revision"] != expected_revision:
                raise ForestError(
                    "REVISION_CONFLICT",
                    "Account changed since it was read",
                    409,
                    {"current_revision": target["revision"]},
                )
            if target_account_id == subject_id and disabled != target["disabled"]:
                raise ForestError(
                    "SELF_DISABLE_REQUIRES_ANOTHER_ADMIN",
                    "Another administrator must change this account's enabled state",
                    409,
                )
            password_change = password_hash_ciphertext is not None
            if (password_hash_ciphertext is None) != (password_secret_digest is None):
                raise ForestError("VALIDATION_FAILED", "Password plan is incomplete", 422)
            desired = {"role": role, "disabled": disabled, "password_change": password_change}
            plan_digest = digest_json({
                "plan_id": plan_id,
                "subject_id": subject_id,
                "subject_revision": actor["revision"],
                "target_account_id": target_account_id,
                "base_revision": expected_revision,
                **desired,
                "password_secret_digest": password_secret_digest,
            })
            await conn.execute(
                "INSERT INTO web_account_plans(plan_id,subject_id,subject_revision,target_account_id,"
                "base_revision,target_role,target_disabled,password_hash_ciphertext,password_secret_digest,"
                "plan_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan_id,
                    subject_id,
                    actor["revision"],
                    target_account_id,
                    expected_revision,
                    role,
                    disabled,
                    password_hash_ciphertext,
                    password_secret_digest,
                    plan_digest,
                    expires_at,
                ),
            )
            return {
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "target_account_id": target_account_id,
                "expected_revision": expected_revision,
                "expires_at": expires_at.isoformat(),
                "before": {"role": target["role"], "disabled": target["disabled"]},
                "after": desired,
            }

    async def apply(
        self,
        *,
        subject_id: str,
        session_token_hash: str,
        expected_password_hash: str,
        target_account_id: str,
        expected_revision: int,
        plan_id: str,
        plan_digest: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        semantic = digest_json({
            "action": "account.apply",
            "target_account_id": target_account_id,
            "expected_revision": expected_revision,
            "plan_id": plan_id,
            "plan_digest": plan_digest,
        })
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170784)")
            actor = await self._authorize(
                conn,
                subject_id,
                session_token_hash,
                expected_password_hash=expected_password_hash,
                require_admin=False,
            )
            previous = await (await conn.execute(
                "SELECT semantic_digest,result FROM web_account_requests "
                "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["semantic_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Idempotency key was used for different input", 409
                    )
                return previous["result"]
            if actor["role"] != "admin":
                raise ForestError("FORBIDDEN", "Current account cannot perform this operation", 403)
            plan = await (await conn.execute(
                "SELECT *,expires_at>clock_timestamp() AS unexpired FROM web_account_plans "
                "WHERE plan_id=%s AND subject_id=%s AND target_account_id=%s",
                (plan_id, subject_id, target_account_id),
            )).fetchone()
            if plan is None:
                raise ForestError("PLAN_NOT_FOUND", "Account change plan is unavailable", 404)
            if not plan["unexpired"]:
                raise ForestError("PLAN_EXPIRED", "Preview the account change again", 409)
            if (
                plan["plan_digest"] != plan_digest
                or plan["base_revision"] != expected_revision
            ):
                raise ForestError("PLAN_MISMATCH", "Apply input differs from the frozen plan", 409)
            if plan["subject_revision"] != actor["revision"]:
                raise ForestError(
                    "REAUTHENTICATION_STALE", "Administrator account changed since preview", 409
                )
            target = await (await conn.execute(
                "SELECT * FROM web_accounts WHERE id=%s FOR UPDATE",
                (target_account_id,),
            )).fetchone()
            if target is None:
                raise ForestError("ACCOUNT_NOT_FOUND", "Account does not exist", 404)
            if target["revision"] != expected_revision:
                raise ForestError(
                    "REVISION_CONFLICT",
                    "Account changed since preview",
                    409,
                    {"current_revision": target["revision"]},
                )
            valid = await (await conn.execute(
                "SELECT expires_at>clock_timestamp() AS unexpired FROM web_account_plans WHERE plan_id=%s",
                (plan_id,),
            )).fetchone()
            if valid is None or not valid["unexpired"]:
                raise ForestError("PLAN_EXPIRED", "Account change plan expired before commit", 409)
            removes_admin = (
                target["role"] == "admin"
                and not target["disabled"]
                and (plan["target_role"] != "admin" or plan["target_disabled"])
            )
            if removes_admin:
                admins = await (await conn.execute(
                    "SELECT count(*) AS n FROM web_accounts WHERE role='admin' AND NOT disabled"
                )).fetchone()
                if admins["n"] <= 1:
                    raise ForestError(
                        "LAST_ADMIN_REQUIRED", "At least one enabled administrator must remain", 409
                    )
            changed = (
                target["role"] != plan["target_role"]
                or target["disabled"] != plan["target_disabled"]
                or plan["password_hash_ciphertext"] is not None
            )
            if not changed:
                raise ForestError("NO_CHANGES", "Account plan has no changes", 409)
            revision = expected_revision + 1
            password_hash = (
                self.vault.decrypt(plan["password_hash_ciphertext"])
                if plan["password_hash_ciphertext"] is not None
                else None
            )
            updated = await (await conn.execute(
                "UPDATE web_accounts SET role=%s,disabled=%s,password_hash=COALESCE(%s,password_hash),"
                "revision=%s WHERE id=%s RETURNING *",
                (
                    plan["target_role"],
                    plan["target_disabled"],
                    password_hash,
                    revision,
                    target_account_id,
                ),
            )).fetchone()
            revoked = await (await conn.execute(
                "UPDATE web_sessions SET revoked_at=COALESCE(revoked_at,clock_timestamp()) "
                "WHERE account_id=%s AND revoked_at IS NULL RETURNING token_hash",
                (target_account_id,),
            )).fetchall()
            result = _public(updated)
            result.update({
                "request_id": str(uuid4()),
                "revoked_sessions": len(revoked),
                "reauthentication_required": target_account_id == subject_id,
            })
            await conn.execute(
                "INSERT INTO web_account_requests(request_id,subject_id,target_account_id,"
                "idempotency_key,action,semantic_digest,result) VALUES (%s,%s,%s,%s,'account.apply',%s,%s)",
                (
                    result["request_id"],
                    subject_id,
                    target_account_id,
                    idempotency_key,
                    semantic,
                    Jsonb(result),
                ),
            )
            await self._record(
                conn,
                request_id=result["request_id"],
                subject_id=subject_id,
                target_account_id=target_account_id,
                action="account.apply",
                details={
                    "previous_revision": expected_revision,
                    "revision": revision,
                    "role": updated["role"],
                    "disabled": updated["disabled"],
                    "password_changed": plan["password_hash_ciphertext"] is not None,
                    "revoked_sessions": len(revoked),
                    "plan_id": plan_id,
                    "plan_digest": plan_digest,
                },
            )
            return result

    @staticmethod
    async def _record(
        conn,
        *,
        request_id: str,
        subject_id: str,
        target_account_id: str,
        action: str,
        details: dict[str, Any],
    ) -> None:
        resource = "account:" + target_account_id
        await conn.execute(
            "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid4()), subject_id, action, resource, request_id, Jsonb(details)),
        )
        await conn.execute(
            "INSERT INTO outbox_events(event_id,topic,resource_key,request_id,payload) "
            "VALUES (%s,%s,%s,%s,%s)",
            (
                str(uuid4()),
                action + ".accepted",
                resource,
                request_id,
                Jsonb({"target_account_id": target_account_id, "revision": details["revision"]}),
            ),
        )

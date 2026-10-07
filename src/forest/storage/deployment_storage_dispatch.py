"""Deployment-owned delivery lease for frozen Host storage requests."""

from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import uuid4

from forest.errors import ForestError
from forest.storage.deployment_catalog import verify_deployment_supervisor_in

_REQUEST_ID = re.compile(r"storage-request/[a-f0-9]{64}\Z")
_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


@dataclass(frozen=True)
class StorageDispatchClaim:
    request_id: str
    claim_token: str
    owner_epoch: int
    attempt_no: int


class DeploymentStorageDispatchAuthority:
    """Claim and settle delivery only; immutable request and Host facts live elsewhere."""

    def __init__(self, store) -> None:
        self.store = store

    async def claim(
        self, supervisor, *, lease_seconds: int = 120, conn=None
    ) -> StorageDispatchClaim | None:
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 300:
            raise ForestError("STORAGE_DISPATCH_INPUT_INVALID", "Lease duration is invalid", 422)
        if conn is None:
            async with self.store.transaction() as owned:
                return await self.claim(supervisor, lease_seconds=lease_seconds, conn=owned)
        await verify_deployment_supervisor_in(conn, supervisor, self.store.deployment_id)
        due = await (
            await conn.execute(
                "SELECT d.request_id FROM forest_deployment.storage_request_dispatch d "
                "JOIN forest_deployment.storage_requests r USING(request_id) "
                "WHERE r.deployment_id=%s AND d.state='PENDING' "
                "AND d.next_attempt_at<=clock_timestamp() "
                "AND (d.claim_expires_at IS NULL OR d.claim_expires_at<=clock_timestamp()) "
                "ORDER BY d.next_attempt_at,d.request_id "
                "FOR UPDATE OF d SKIP LOCKED LIMIT 1",
                (self.store.deployment_id,),
            )
        ).fetchone()
        if due is None:
            return None
        token = "storage-claim/" + uuid4().hex
        saved = await (
            await conn.execute(
                "UPDATE forest_deployment.storage_request_dispatch SET "
                "claim_token=%s,claim_owner_epoch=%s,"
                "claim_expires_at=clock_timestamp()+(%s*interval '1 second'),"
                "attempt_count=attempt_count+1,updated_at=clock_timestamp() "
                "WHERE request_id=%s RETURNING attempt_count",
                (token, supervisor.owner_epoch, lease_seconds, due["request_id"]),
            )
        ).fetchone()
        return StorageDispatchClaim(
            due["request_id"],
            token,
            supervisor.owner_epoch,
            saved["attempt_count"],
        )

    async def renew(
        self, supervisor, claim: StorageDispatchClaim, *, lease_seconds: int = 120, conn=None
    ) -> None:
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 300:
            raise ForestError("STORAGE_DISPATCH_INPUT_INVALID", "Lease duration is invalid", 422)
        if conn is None:
            async with self.store.transaction() as owned:
                return await self.renew(supervisor, claim, lease_seconds=lease_seconds, conn=owned)
        await self.held_claim(supervisor, claim, conn=conn)
        await conn.execute(
            "UPDATE forest_deployment.storage_request_dispatch SET "
            "claim_expires_at=clock_timestamp()+(%s*interval '1 second'),"
            "updated_at=clock_timestamp() WHERE request_id=%s",
            (lease_seconds, claim.request_id),
        )

    async def settle(
        self,
        supervisor,
        claim: StorageDispatchClaim,
        *,
        outcome: str,
        retry_seconds: int = 5,
        error_code: str | None = None,
        conn=None,
    ) -> dict:
        if (
            outcome not in {"WAITING", "SETTLED", "BLOCKED"}
            or (
                error_code is not None
                and (not isinstance(error_code, str) or _ERROR.fullmatch(error_code) is None)
            )
            or (outcome == "BLOCKED" and error_code is None)
            or (type(retry_seconds) is not int or not 1 <= retry_seconds <= 300)
        ):
            raise ForestError("STORAGE_DISPATCH_INPUT_INVALID", "Dispatch outcome is invalid", 422)
        if conn is None:
            async with self.store.transaction() as owned:
                return await self.settle(
                    supervisor,
                    claim,
                    outcome=outcome,
                    retry_seconds=retry_seconds,
                    error_code=error_code,
                    conn=owned,
                )
        await self.held_claim(supervisor, claim, conn=conn)
        if outcome == "SETTLED":
            profile = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.storage_requests r "
                    "JOIN forest_deployment.storage_profiles p ON "
                    "p.profile_ref=r.profile_ref AND p.bot_id=r.bot_id "
                    "AND p.storage_generation=r.storage_generation "
                    "AND p.dsn_secret_ref=r.dsn_secret_ref "
                    "WHERE r.deployment_id=%s AND r.request_id=%s",
                    (self.store.deployment_id, claim.request_id),
                )
            ).fetchone()
            if profile is None:
                raise ForestError(
                    "STORAGE_DISPATCH_NOT_SETTLED",
                    "Storage profile has not committed",
                    409,
                )
        state = "PENDING" if outcome == "WAITING" else outcome
        saved = await (
            await conn.execute(
                "UPDATE forest_deployment.storage_request_dispatch SET "
                "state=%s,next_attempt_at=CASE WHEN %s='PENDING' THEN "
                "clock_timestamp()+(%s*interval '1 second') ELSE NULL END,"
                "claim_token=NULL,claim_owner_epoch=NULL,claim_expires_at=NULL,"
                "last_error_code=%s,updated_at=clock_timestamp() "
                "WHERE request_id=%s RETURNING state,attempt_count,last_error_code",
                (
                    state,
                    state,
                    retry_seconds,
                    None if outcome == "SETTLED" else error_code,
                    claim.request_id,
                ),
            )
        ).fetchone()
        return dict(saved)

    async def read(self, request_id: str) -> dict:
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            raise ForestError("STORAGE_REQUEST_INVALID", "Request ID is invalid", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT d.request_id,d.state,d.attempt_count,d.next_attempt_at,"
                    "d.last_error_code,d.updated_at FROM "
                    "forest_deployment.storage_request_dispatch d "
                    "JOIN forest_deployment.storage_requests r USING(request_id) "
                    "WHERE r.deployment_id=%s AND d.request_id=%s",
                    (self.store.deployment_id, request_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("STORAGE_REQUEST_NOT_FOUND", "Storage request not found", 404)
        return dict(row)

    async def page(
        self,
        *,
        limit: int = 50,
        after: str | None = None,
        bot_id: str | None = None,
        state: str | None = None,
    ) -> dict:
        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or (
                after is not None
                and (not isinstance(after, str) or _REQUEST_ID.fullmatch(after) is None)
            )
            or (
                bot_id is not None
                and (not isinstance(bot_id, str) or not 1 <= len(bot_id.encode()) <= 128)
            )
            or (state is not None and state not in {"PENDING", "SETTLED", "BLOCKED"})
        ):
            raise ForestError("STORAGE_DISPATCH_INPUT_INVALID", "Page filter is invalid", 422)
        async with self.store.pool.connection() as conn:
            cursor = None
            if after is not None:
                cursor = await (
                    await conn.execute(
                        "SELECT created_at,request_id FROM forest_deployment.storage_requests "
                        "WHERE deployment_id=%s AND request_id=%s",
                        (self.store.deployment_id, after),
                    )
                ).fetchone()
                if cursor is None:
                    raise ForestError("STORAGE_REQUEST_NOT_FOUND", "Page cursor is invalid", 404)
            rows = await (
                await conn.execute(
                    "SELECT r.request_id,r.host_id,r.bot_id,r.storage_generation,"
                    "r.profile_ref,r.dsn_secret_ref,d.state,"
                    "d.attempt_count,d.last_error_code "
                    "FROM forest_deployment.storage_requests r "
                    "JOIN forest_deployment.storage_request_dispatch d USING(request_id) "
                    "WHERE r.deployment_id=%s AND (%s::text IS NULL OR r.bot_id=%s) "
                    "AND (%s::text IS NULL OR d.state=%s) "
                    "AND (%s::timestamptz IS NULL OR "
                    "(r.created_at,r.request_id)<(%s::timestamptz,%s::text)) "
                    "ORDER BY r.created_at DESC,r.request_id DESC LIMIT %s",
                    (
                        self.store.deployment_id,
                        bot_id,
                        bot_id,
                        state,
                        state,
                        None if cursor is None else cursor["created_at"],
                        None if cursor is None else cursor["created_at"],
                        after,
                        limit + 1,
                    ),
                )
            ).fetchall()
        return {
            "items": [dict(row) for row in rows[:limit]],
            "next_after": rows[limit - 1]["request_id"] if len(rows) > limit else None,
        }

    async def held_claim(self, supervisor, claim: StorageDispatchClaim, *, conn=None) -> None:
        if conn is None:
            async with self.store.transaction() as owned:
                return await self.held_claim(supervisor, claim, conn=owned)
        if (
            not isinstance(claim, StorageDispatchClaim)
            or not isinstance(claim.request_id, str)
            or _REQUEST_ID.fullmatch(claim.request_id) is None
        ):
            raise ForestError("STORAGE_DISPATCH_INPUT_INVALID", "Claim identity is invalid", 422)
        await verify_deployment_supervisor_in(conn, supervisor, self.store.deployment_id)
        held = await (
            await conn.execute(
                "SELECT d.claim_token,d.claim_owner_epoch,d.attempt_count,"
                "d.claim_expires_at,d.state FROM "
                "forest_deployment.storage_request_dispatch d "
                "JOIN forest_deployment.storage_requests r USING(request_id) "
                "WHERE r.deployment_id=%s AND d.request_id=%s FOR UPDATE OF d",
                (self.store.deployment_id, claim.request_id),
            )
        ).fetchone()
        if (
            held is None
            or held["state"] != "PENDING"
            or held["claim_token"] != claim.claim_token
            or held["claim_owner_epoch"] != claim.owner_epoch
            or claim.owner_epoch != supervisor.owner_epoch
            or held["attempt_count"] != claim.attempt_no
            or held["claim_expires_at"] is None
        ):
            raise ForestError("STORAGE_DISPATCH_CLAIM_LOST", "Dispatch claim is stale", 409)
        valid = await (
            await conn.execute(
                "SELECT %s::timestamptz>clock_timestamp() AS valid",
                (held["claim_expires_at"],),
            )
        ).fetchone()
        if not valid["valid"]:
            raise ForestError("STORAGE_DISPATCH_CLAIM_LOST", "Dispatch claim expired", 409)

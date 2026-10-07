"""Kernel authority for the one durable retirement Job of each Bot."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.deployment_catalog import verify_deployment_supervisor_in

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


@dataclass(frozen=True)
class DeploymentRetirementClaim:
    deployment_id: str
    operation_id: str
    bot_id: str
    attempt_no: int
    lease_owner: str
    lease_epoch: int
    lease_expires_at: datetime


class DeploymentRetirementJobAuthority:
    def __init__(self, store) -> None:
        self.store = store

    async def page(self, *, limit: int = 50, after: str | None = None,
                   search: str | None = None, supply_state: str | None = None) -> dict:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("RETIREMENT_QUERY_INVALID", "Page size is invalid", 422)
        for value in (after, search):
            if value is not None and (
                not isinstance(value, str) or len(value.encode("utf-8")) > 128
            ):
                raise ForestError("RETIREMENT_QUERY_INVALID", "Filter is invalid", 422)
        if supply_state not in {None, "READY", "DELETING", "DELETED"}:
            raise ForestError("RETIREMENT_QUERY_INVALID", "State is invalid", 422)
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT b.bot_id,b.display_name,b.supply_state,b.revision,"
                    "i.desired_state,j.operation_id,j.state AS job_state,"
                    "j.revision AS job_revision,j.attempt_no,j.last_error_code,"
                    "EXISTS(SELECT 1 FROM forest_deployment.storage_requests r "
                    "JOIN forest_deployment.storage_request_pins p ON "
                    "p.request_id=r.request_id AND p.profile_ref=r.profile_ref "
                    "AND p.host_id=r.host_id AND p.deployment_id=r.deployment_id "
                    "WHERE r.bot_id=b.bot_id "
                    "AND r.storage_generation=b.storage_generation "
                    "AND r.profile_ref=b.storage_profile_ref "
                    "AND r.deployment_id=%s) AS host_pinned "
                    "FROM forest_deployment.bots b "
                    "JOIN forest_deployment.runtime_intents i USING(bot_id) "
                    "LEFT JOIN forest_deployment.retirement_jobs j USING(bot_id) "
                    "WHERE (%s::text IS NULL OR b.bot_id>%s) "
                    "AND (%s::text IS NULL OR b.supply_state=%s) "
                    "AND (%s::text IS NULL OR b.bot_id ILIKE '%%'||%s||'%%' "
                    "OR b.display_name ILIKE '%%'||%s||'%%') "
                    "ORDER BY b.bot_id LIMIT %s",
                    (self.store.deployment_id, after, after, supply_state, supply_state,
                     search, search, search, limit + 1),
                )
            ).fetchall()
        items = [dict(row) for row in rows[:limit]]
        return {
            "items": items,
            "next_after": items[-1]["bot_id"] if len(rows) > limit else None,
        }

    async def requeue(
        self, *, operation_id: str, bot_id: str,
        expected_job_revision: int, conn=None,
    ) -> dict:
        """Re-admit one blocked Job without changing its frozen resource identity."""
        for value, limit in ((operation_id, 256), (bot_id, 128)):
            if (
                not isinstance(value, str)
                or not 1 <= len(value.encode("utf-8")) <= limit
                or any(ord(char) < 32 or ord(char) == 127 for char in value)
            ):
                raise ForestError("RETIREMENT_INPUT_INVALID", "Identity is invalid", 422)
        if type(expected_job_revision) is not int or expected_job_revision < 1:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Job revision is invalid", 422)
        input_digest = digest_json({
            "bot_id": bot_id, "expected_job_revision": expected_job_revision,
        })
        async with self.store.transaction(conn) as conn:
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.retirement-requeue/" + operation_id,),
            )
            prior = await (
                await conn.execute(
                    "SELECT bot_id,retirement_operation_id,resulting_revision,"
                    "input_digest FROM forest_deployment.retirement_requeue_operations "
                    "WHERE operation_id=%s", (operation_id,),
                )
            ).fetchone()
            if prior is not None:
                if prior["input_digest"] != input_digest or prior["bot_id"] != bot_id:
                    raise ForestError(
                        "RETIREMENT_OPERATION_CONFLICT",
                        "Requeue operation differs", 409,
                    )
                return {
                    "bot_id": bot_id,
                    "retirement_operation_id": prior["retirement_operation_id"],
                    "job_revision": prior["resulting_revision"],
                    "state": "RETRY_WAIT",
                }
            job = await (
                await conn.execute(
                    "SELECT j.operation_id,j.state,j.revision,b.supply_state,"
                    "i.desired_state FROM forest_deployment.retirement_jobs j "
                    "JOIN forest_deployment.bots b USING(bot_id) "
                    "JOIN forest_deployment.runtime_intents i USING(bot_id) "
                    "WHERE j.bot_id=%s FOR UPDATE OF j", (bot_id,),
                )
            ).fetchone()
            if job is None:
                raise ForestError(
                    "RETIREMENT_JOB_NOT_FOUND", "Retirement Job does not exist", 404,
                )
            if job["revision"] != expected_job_revision:
                raise ForestError(
                    "RETIREMENT_JOB_REVISION_CONFLICT", "Retirement Job changed", 409,
                    details={"revision": job["revision"]},
                )
            if job["state"] != "BLOCKED_UNKNOWN":
                raise ForestError(
                    "RETIREMENT_JOB_NOT_BLOCKED", "Only blocked Jobs can be requeued", 409,
                )
            if job["supply_state"] != "DELETING" or job["desired_state"] != "OFFLINE":
                raise ForestError(
                    "RETIREMENT_FENCE_MISMATCH", "Bot retirement fence changed", 409,
                )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.retirement_jobs "
                    "SET state='RETRY_WAIT',retry_not_before=clock_timestamp(),"
                    "revision=revision+1,updated_at=clock_timestamp() "
                    "WHERE operation_id=%s RETURNING revision",
                    (job["operation_id"],),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO forest_deployment.retirement_requeue_operations("
                "operation_id,input_digest,retirement_operation_id,bot_id,"
                "resulting_revision) VALUES (%s,%s,%s,%s,%s)",
                (operation_id, input_digest, job["operation_id"], bot_id,
                 saved["revision"]),
            )
            return {
                "bot_id": bot_id,
                "retirement_operation_id": job["operation_id"],
                "job_revision": saved["revision"],
                "state": "RETRY_WAIT",
            }

    async def claim(self, supervisor, *, worker_id: str, lease_seconds: int, conn=None):
        if not isinstance(worker_id, str) or not 1 <= len(worker_id.encode()) <= 256:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Worker identity is invalid", 422)
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 900:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Retirement lease is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await conn.execute(
                "WITH expired AS (SELECT operation_id FROM forest_deployment.retirement_jobs "
                "WHERE state='RUNNING' AND lease_expires_at<=clock_timestamp() "
                "ORDER BY lease_expires_at,operation_id FOR UPDATE SKIP LOCKED LIMIT 32) "
                "UPDATE forest_deployment.retirement_jobs j SET state='RETRY_WAIT',"
                "lease_owner=NULL,lease_expires_at=NULL,retry_not_before=clock_timestamp(),"
                "last_error_code='RETIREMENT_LEASE_EXPIRED',revision=revision+1,"
                "updated_at=clock_timestamp() FROM expired e "
                "WHERE j.operation_id=e.operation_id"
            )
            row = await (
                await conn.execute(
                    "WITH candidate AS (SELECT operation_id FROM "
                    "forest_deployment.retirement_jobs WHERE state='QUEUED' OR "
                    "(state='RETRY_WAIT' AND retry_not_before<=clock_timestamp()) "
                    "ORDER BY created_at,operation_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                    "UPDATE forest_deployment.retirement_jobs j SET state='RUNNING',"
                    "attempt_no=j.attempt_no+1,lease_owner=%s,"
                    "lease_epoch=j.lease_epoch+1,"
                    "lease_expires_at=clock_timestamp()+make_interval(secs => %s),"
                    "retry_not_before=NULL,last_error_code=NULL,revision=j.revision+1,"
                    "updated_at=clock_timestamp() FROM candidate c "
                    "WHERE j.operation_id=c.operation_id RETURNING j.*",
                    (worker_id, lease_seconds),
                )
            ).fetchone()
            return None if row is None else self._claim(row)

    async def held_claim(
        self, supervisor, *, operation_id: str, claim_token: str,
        attempt_no: int, conn=None,
    ) -> DeploymentRetirementClaim:
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            row = await self._held_in(conn, operation_id, claim_token, attempt_no)
            return self._claim(row)

    async def renew(self, supervisor, claim: DeploymentRetirementClaim, *,
                    lease_seconds: int, conn=None) -> DeploymentRetirementClaim:
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 900:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Retirement lease is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await self._held_in(
                conn, claim.operation_id, claim.lease_owner, claim.attempt_no,
            )
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.retirement_jobs SET "
                    "lease_expires_at=clock_timestamp()+make_interval(secs => %s),"
                    "updated_at=clock_timestamp() WHERE operation_id=%s RETURNING *",
                    (lease_seconds, claim.operation_id),
                )
            ).fetchone()
            return self._claim(row)

    async def retry(self, supervisor, claim: DeploymentRetirementClaim, *,
                    error_code: str, retry_seconds: int = 5, conn=None) -> dict:
        if not isinstance(error_code, str) or _ERROR.fullmatch(error_code) is None:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Error code is invalid", 422)
        if type(retry_seconds) is not int or not 1 <= retry_seconds <= 3600:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Retry delay is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await self._held_in(
                conn, claim.operation_id, claim.lease_owner, claim.attempt_no,
            )
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.retirement_jobs SET state='RETRY_WAIT',"
                    "lease_owner=NULL,lease_expires_at=NULL,"
                    "retry_not_before=clock_timestamp()+make_interval(secs => %s),"
                    "last_error_code=%s,revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE operation_id=%s RETURNING *",
                    (retry_seconds, error_code, claim.operation_id),
                )
            ).fetchone()
            return dict(row)

    async def block_unknown(self, supervisor, claim: DeploymentRetirementClaim, *,
                            error_code: str, conn=None) -> dict:
        if not isinstance(error_code, str) or _ERROR.fullmatch(error_code) is None:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Error code is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await self._held_in(
                conn, claim.operation_id, claim.lease_owner, claim.attempt_no,
            )
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.retirement_jobs "
                    "SET state='BLOCKED_UNKNOWN',lease_owner=NULL,"
                    "lease_expires_at=NULL,last_error_code=%s,revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE operation_id=%s RETURNING *",
                    (error_code, claim.operation_id),
                )
            ).fetchone()
            return dict(row)

    async def complete(
        self, supervisor, claim: DeploymentRetirementClaim, *,
        catalog, request_id: str, resource_id: str, conn=None,
    ) -> dict:
        """Host completion was checked by the caller; commit local facts together."""
        if catalog.store is not self.store:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Catalog differs", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await self._held_in(
                conn, claim.operation_id, claim.lease_owner, claim.attempt_no,
            )
            witness = await catalog.retirement_quiescence_in(conn, claim.bot_id)
            revoked = await (
                await conn.execute(
                    "SELECT request_id,resource_id FROM "
                    "forest_deployment.storage_profile_revocations "
                    "WHERE retirement_operation_id=%s AND bot_id=%s "
                    "AND profile_ref=%s AND storage_generation=%s FOR SHARE",
                    (claim.operation_id, claim.bot_id,
                     witness["storage_profile_ref"], witness["storage_generation"]),
                )
            ).fetchone()
            if (
                witness["retirement_operation_id"] != claim.operation_id
                or revoked is None or revoked["request_id"] != request_id
                or revoked["resource_id"] != resource_id
            ):
                raise ForestError(
                    "RETIREMENT_PROOF_MISMATCH", "Retirement proof differs", 409,
                )
            await conn.execute(
                "UPDATE forest_deployment.bots SET supply_state='DELETED',"
                "revision=revision+1,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND supply_state='DELETING'", (claim.bot_id,),
            )
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.retirement_jobs SET state='SUCCEEDED',"
                    "lease_owner=NULL,lease_expires_at=NULL,"
                    "completed_at=clock_timestamp(),revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE operation_id=%s RETURNING *",
                    (claim.operation_id,),
                )
            ).fetchone()
            return dict(row)

    async def _held_in(self, conn, operation_id, claim_token, attempt_no):
        if not isinstance(operation_id, str) or not isinstance(claim_token, str):
            raise ForestError("RETIREMENT_INPUT_INVALID", "Claim identity is invalid", 422)
        if type(attempt_no) is not int or attempt_no < 1:
            raise ForestError("RETIREMENT_INPUT_INVALID", "Claim attempt is invalid", 422)
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.retirement_jobs WHERE "
                "operation_id=%s AND state='RUNNING' AND lease_owner=%s "
                "AND attempt_no=%s AND lease_expires_at>clock_timestamp() FOR UPDATE",
                (operation_id, claim_token, attempt_no),
            )
        ).fetchone()
        if row is None:
            raise ForestError("STALE_RETIREMENT_CLAIM", "Retirement claim is stale", 409)
        return row

    async def _verify_supervisor_in(self, conn, lease):
        await verify_deployment_supervisor_in(
            conn, lease, self.store.deployment_id,
            invalid_code="RETIREMENT_INPUT_INVALID",
        )

    def _claim(self, row):
        return DeploymentRetirementClaim(
            self.store.deployment_id, row["operation_id"], row["bot_id"],
            row["attempt_no"], row["lease_owner"], row["lease_epoch"],
            row["lease_expires_at"],
        )

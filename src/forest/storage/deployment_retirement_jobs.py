"""Kernel authority for the one durable retirement Job of each Bot."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from forest.errors import ForestError
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

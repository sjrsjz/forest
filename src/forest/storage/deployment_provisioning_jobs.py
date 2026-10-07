"""Durable, fenced provisioning Jobs in the deployment control plane."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from forest.control_action_authorization import authorize_control_action_in
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.deployment_catalog import (
    DeploymentSupervisorLease,
    verify_deployment_supervisor_in,
)

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_RECOVERY_BATCH_LIMIT = 32


def _text(value: object, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ForestError("PROVISIONING_INPUT_INVALID", f"Invalid {field}", 422)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("PROVISIONING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True)
class DeploymentProvisioningClaim:
    deployment_id: str
    job_id: str
    bot_id: str
    display_name: str
    storage_profile_ref: str
    storage_generation: int
    bootstrap_profile_ref: str
    bootstrap_manifest_digest: str
    attempt_no: int
    lease_owner: str
    lease_epoch: int
    lease_expires_at: datetime


class DeploymentProvisioningJobAuthority:
    """Queue provisioning without turning deployment metadata into module configuration."""

    def __init__(self, store) -> None:
        self.store = store

    async def enqueue(
        self,
        *,
        operation_id: str,
        job_id: str,
        bot_id: str,
        display_name: str,
        storage_profile_ref: str,
        storage_generation: int,
        bootstrap_profile_ref: str,
        max_attempts: int = 5,
        authorization=None,
        conn=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (job_id, "job_id", 256),
            (bot_id, "bot_id", 128),
            (display_name, "display_name", 256),
            (storage_profile_ref, "storage_profile_ref", 512),
            (bootstrap_profile_ref, "bootstrap_profile_ref", 512),
        ):
            _text(value, field, limit)
        if (
            type(storage_generation) is not int
            or storage_generation < 1
            or type(max_attempts) is not int
            or not 1 <= max_attempts <= 20
        ):
            raise ForestError("PROVISIONING_INPUT_INVALID", "Provisioning bound is invalid", 422)
        request = {
            "job_id": job_id,
            "bot_id": bot_id,
            "display_name": display_name,
            "storage_profile_ref": storage_profile_ref,
            "storage_generation": storage_generation,
            "bootstrap_profile_ref": bootstrap_profile_ref,
            "max_attempts": max_attempts,
        }
        input_digest = digest_json(request)
        async with self.store.transaction(conn) as conn:
            await authorize_control_action_in(
                conn, authorization, operation_id=operation_id
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.provisioning-operation/{operation_id}",),
            )
            replay = await (
                await conn.execute(
                    "SELECT o.input_digest,j.* FROM forest_deployment.provisioning_operations o "
                    "JOIN forest_deployment.provisioning_jobs j USING(job_id) "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "PROVISIONING_OPERATION_CONFLICT",
                        "Provisioning operation input differs",
                        409,
                    )
                return dict(replay)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.storage-profile-revoke/profile/" + storage_profile_ref,),
            )
            revoked = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.storage_profile_revocations "
                    "WHERE profile_ref=%s", (storage_profile_ref,),
                )
            ).fetchone()
            if revoked is not None:
                raise ForestError(
                    "STORAGE_PROFILE_REVOKED", "Storage profile is retired", 409,
                )
            registered = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.bots WHERE bot_id=%s FOR SHARE",
                    (bot_id,),
                )
            ).fetchone()
            if registered is not None:
                raise ForestError(
                    "PROVISIONING_IDENTITY_EXISTS",
                    "Bot identity already exists", 409,
                )
            profile = await (
                await conn.execute(
                    "SELECT manifest_digest FROM forest_deployment.bootstrap_profiles "
                    "WHERE profile_ref=%s AND bot_id=%s AND storage_profile_ref=%s "
                    "AND storage_generation=%s FOR SHARE",
                    (
                        bootstrap_profile_ref,
                        bot_id,
                        storage_profile_ref,
                        storage_generation,
                    ),
                )
            ).fetchone()
            if profile is None:
                raise ForestError(
                    "BOOTSTRAP_PROFILE_SCOPE_MISMATCH",
                    "Bootstrap profile differs from the provisioning target",
                    409,
                )
            existing = await (
                await conn.execute(
                    "SELECT job_id FROM forest_deployment.provisioning_jobs "
                    "WHERE job_id=%s OR bot_id=%s FOR UPDATE",
                    (job_id, bot_id),
                )
            ).fetchone()
            if existing is not None:
                raise ForestError(
                    "PROVISIONING_IDENTITY_EXISTS",
                    "Provisioning Job or Bot identity cannot be reused",
                    409,
                )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.provisioning_jobs("
                    "job_id,bot_id,display_name,storage_profile_ref,storage_generation,"
                    "bootstrap_profile_ref,bootstrap_manifest_digest,state,max_attempts) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,'QUEUED',%s) RETURNING *",
                    (
                        job_id,
                        bot_id,
                        display_name,
                        storage_profile_ref,
                        storage_generation,
                        bootstrap_profile_ref,
                        profile["manifest_digest"],
                        max_attempts,
                    ),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO forest_deployment.provisioning_operations("
                "operation_id,operation_kind,input_digest,job_id,resulting_revision) "
                "VALUES (%s,'ENQUEUE',%s,%s,%s)",
                (operation_id, input_digest, job_id, saved["revision"]),
            )
            return dict(saved)

    async def claim(
        self,
        supervisor: DeploymentSupervisorLease,
        *,
        worker_id: str,
        lease_seconds: int = 120,
        conn=None,
    ) -> DeploymentProvisioningClaim | None:
        _text(worker_id, "worker_id", 256)
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 900:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Provisioning lease is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await conn.execute(
                "WITH expired AS (SELECT job_id FROM forest_deployment.provisioning_jobs "
                "WHERE state='RUNNING' AND lease_expires_at<=clock_timestamp() "
                "ORDER BY lease_expires_at,job_id FOR UPDATE SKIP LOCKED LIMIT %s) "
                "UPDATE forest_deployment.provisioning_jobs j SET state='BLOCKED_UNKNOWN',"
                "last_error_code='PROVISIONING_CLAIM_EXPIRED',lease_owner=NULL,"
                "lease_expires_at=NULL,retry_not_before=NULL,revision=revision+1,"
                "updated_at=clock_timestamp() FROM expired e WHERE j.job_id=e.job_id",
                (_RECOVERY_BATCH_LIMIT,),
            )
            await conn.execute(
                "WITH exhausted AS (SELECT job_id FROM forest_deployment.provisioning_jobs "
                "WHERE state='RETRY_WAIT' AND attempt_no>=max_attempts "
                "AND retry_not_before<=clock_timestamp() "
                "ORDER BY retry_not_before,job_id FOR UPDATE SKIP LOCKED LIMIT %s) "
                "UPDATE forest_deployment.provisioning_jobs j SET state='FAILED',"
                "last_error_code='PROVISIONING_ATTEMPTS_EXHAUSTED',lease_owner=NULL,"
                "lease_expires_at=NULL,retry_not_before=NULL,completed_at=clock_timestamp(),"
                "revision=revision+1,updated_at=clock_timestamp() FROM exhausted e "
                "WHERE j.job_id=e.job_id",
                (_RECOVERY_BATCH_LIMIT,),
            )
            row = await (
                await conn.execute(
                    "WITH candidate AS (SELECT job_id FROM forest_deployment.provisioning_jobs "
                    "WHERE attempt_no<max_attempts AND (state='QUEUED' OR "
                    "(state='RETRY_WAIT' AND retry_not_before<=clock_timestamp())) "
                    "ORDER BY created_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                    "UPDATE forest_deployment.provisioning_jobs j SET state='RUNNING',"
                    "attempt_no=j.attempt_no+1,lease_owner=%s,lease_epoch=j.lease_epoch+1,"
                    "lease_expires_at=clock_timestamp()+make_interval(secs => %s),"
                    "retry_not_before=NULL,last_error_code=NULL,revision=j.revision+1,"
                    "updated_at=clock_timestamp() FROM candidate c WHERE j.job_id=c.job_id "
                    "RETURNING j.*",
                    (worker_id, lease_seconds),
                )
            ).fetchone()
            return None if row is None else self._claim(row)

    async def renew(
        self,
        supervisor: DeploymentSupervisorLease,
        claim: DeploymentProvisioningClaim,
        *,
        lease_seconds: int = 120,
        conn=None,
    ) -> DeploymentProvisioningClaim:
        self._validate_claim(claim)
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 900:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Provisioning lease is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.provisioning_jobs SET "
                    "lease_expires_at=clock_timestamp()+make_interval(secs => %s),"
                    "updated_at=clock_timestamp() WHERE job_id=%s AND state='RUNNING' "
                    "AND lease_owner=%s AND lease_epoch=%s AND lease_expires_at>clock_timestamp() "
                    "RETURNING *",
                    (lease_seconds, claim.job_id, claim.lease_owner, claim.lease_epoch),
                )
            ).fetchone()
            if row is None:
                raise ForestError("STALE_PROVISIONING_CLAIM", "Provisioning claim is stale", 409)
            return self._claim(row)

    async def complete_ready(
        self,
        supervisor: DeploymentSupervisorLease,
        claim: DeploymentProvisioningClaim,
        *,
        catalog,
        conn=None,
    ) -> dict:
        """Publish the Bot identity and settle its fenced Job in one transaction."""
        self._validate_claim(claim)
        if catalog.store is not self.store:
            raise ForestError(
                "PROVISIONING_INPUT_INVALID", "Catalog belongs to another deployment store", 422
            )
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await self._owned_in(conn, claim)
            await catalog.register_ready(
                operation_id=f"provision-ready/{claim.job_id}",
                bot_id=claim.bot_id,
                display_name=claim.display_name,
                storage_profile_ref=claim.storage_profile_ref,
                bootstrap_profile_ref=claim.bootstrap_profile_ref,
                bootstrap_manifest_digest=claim.bootstrap_manifest_digest,
                storage_generation=claim.storage_generation,
                conn=conn,
            )
            await self._verify_supervisor_in(conn, supervisor)
            row = await self._finish_in(conn, claim, state="SUCCEEDED", error_code=None)
            return dict(row)

    async def fail(
        self,
        supervisor: DeploymentSupervisorLease,
        claim: DeploymentProvisioningClaim,
        *,
        error_code: str,
        retryable: bool = True,
        retry_seconds: int = 5,
        conn=None,
    ) -> dict:
        self._validate_claim(claim)
        if not isinstance(error_code, str) or _ERROR.fullmatch(error_code) is None:
            error_code = "BOT_PROVISIONING_FAILED"
        if type(retryable) is not bool:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Retry policy is invalid", 422)
        if type(retry_seconds) is not int or not 1 <= retry_seconds <= 3600:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Retry delay is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            current = await self._owned_in(conn, claim)
            terminal = not retryable or current["attempt_no"] >= current["max_attempts"]
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.provisioning_jobs SET state=%s,lease_owner=NULL,"
                    "lease_expires_at=NULL,retry_not_before=CASE WHEN %s THEN NULL ELSE "
                    "clock_timestamp()+make_interval(secs => %s) END,last_error_code=%s,"
                    "completed_at=CASE WHEN %s THEN clock_timestamp() END,revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE job_id=%s RETURNING *",
                    (
                        "FAILED" if terminal else "RETRY_WAIT",
                        terminal,
                        retry_seconds,
                        error_code,
                        terminal,
                        claim.job_id,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def block_unknown(
        self, supervisor: DeploymentSupervisorLease,
        claim: DeploymentProvisioningClaim, *, error_code: str,
        conn=None,
    ) -> dict:
        self._validate_claim(claim)
        if not isinstance(error_code, str) or _ERROR.fullmatch(error_code) is None:
            error_code = "BOT_PROVISIONING_RESULT_UNKNOWN"
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            await self._owned_in(conn, claim)
            row = await (
                await conn.execute(
                    "UPDATE forest_deployment.provisioning_jobs SET state='BLOCKED_UNKNOWN',"
                    "lease_owner=NULL,lease_expires_at=NULL,retry_not_before=NULL,"
                    "last_error_code=%s,revision=revision+1,updated_at=clock_timestamp() "
                    "WHERE job_id=%s RETURNING *",
                    (error_code, claim.job_id),
                )
            ).fetchone()
            return dict(row)

    async def reconcile_ready(
        self, *, operation_id: str, job_id: str, expected_revision: int,
        proof: dict, catalog, conn=None,
    ) -> dict:
        """Publish READY only from an immutable receipt in the dedicated Bot DB."""
        _text(operation_id, "operation_id", 256)
        _text(job_id, "job_id", 256)
        if type(expected_revision) is not int or expected_revision < 1:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Job revision is invalid", 422)
        if catalog.store is not self.store or not isinstance(proof, dict):
            raise ForestError("PROVISIONING_INPUT_INVALID", "Receipt authority is invalid", 422)
        fields = (
            "bot_id", "deployment_id", "job_id", "storage_profile_ref",
            "storage_generation", "bootstrap_profile_ref", "bootstrap_manifest_digest",
        )
        if any(key not in proof for key in fields):
            raise ForestError("PROVISIONING_PROOF_INVALID", "Receipt is incomplete", 409)
        input_digest = digest_json({
            "job_id": job_id, "expected_revision": expected_revision,
            "proof": {key: proof[key] for key in fields},
        })
        async with self.store.transaction(conn) as conn:
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.provisioning-operation/{operation_id}",),
            )
            replay = await (
                await conn.execute(
                    "SELECT o.input_digest,j.* FROM forest_deployment.provisioning_operations o "
                    "JOIN forest_deployment.provisioning_jobs j USING(job_id) "
                    "WHERE o.operation_id=%s", (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "PROVISIONING_OPERATION_CONFLICT",
                        "Provisioning operation input differs", 409,
                    )
                return dict(replay)
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.provisioning_jobs "
                    "WHERE job_id=%s FOR UPDATE", (job_id,),
                )
            ).fetchone()
            if row is None:
                raise ForestError("PROVISIONING_JOB_NOT_FOUND", "Provisioning Job not found", 404)
            if row["revision"] != expected_revision:
                raise ForestError(
                    "PROVISIONING_REVISION_CONFLICT", "Provisioning Job changed", 409,
                    details={"revision": row["revision"]},
                )
            if row["state"] != "BLOCKED_UNKNOWN":
                raise ForestError(
                    "PROVISIONING_NOT_RECONCILABLE",
                    "Only blocked provisioning Jobs accept materialization proof", 409,
                )
            if (
                proof["deployment_id"] != self.store.deployment_id
                or any(proof[key] != row[key] for key in fields if key != "deployment_id")
            ):
                raise ForestError(
                    "PROVISIONING_PROOF_MISMATCH",
                    "Bot materialization receipt differs from the frozen Job", 409,
                )
            await catalog.register_ready(
                operation_id=f"provision-ready/{job_id}",
                bot_id=row["bot_id"], display_name=row["display_name"],
                storage_profile_ref=row["storage_profile_ref"],
                storage_generation=row["storage_generation"],
                bootstrap_profile_ref=row["bootstrap_profile_ref"],
                bootstrap_manifest_digest=row["bootstrap_manifest_digest"],
                conn=conn,
            )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.provisioning_jobs SET state='SUCCEEDED',"
                    "last_error_code=NULL,completed_at=clock_timestamp(),"
                    "revision=revision+1,updated_at=clock_timestamp() "
                    "WHERE job_id=%s RETURNING *", (job_id,),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO forest_deployment.provisioning_operations("
                "operation_id,operation_kind,input_digest,job_id,resulting_revision) "
                "VALUES (%s,'RECONCILE',%s,%s,%s)",
                (operation_id, input_digest, job_id, saved["revision"]),
            )
            return dict(saved)

    async def cancel(
        self,
        *,
        operation_id: str,
        job_id: str,
        expected_revision: int,
        authorization=None,
        conn=None,
    ) -> dict:
        _text(operation_id, "operation_id", 256)
        _text(job_id, "job_id", 256)
        if type(expected_revision) is not int or expected_revision < 1:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Job revision is invalid", 422)
        input_digest = digest_json([job_id, expected_revision])
        async with self.store.transaction(conn) as conn:
            await authorize_control_action_in(
                conn, authorization, operation_id=operation_id
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.provisioning-operation/{operation_id}",),
            )
            replay = await (
                await conn.execute(
                    "SELECT o.input_digest,j.* FROM forest_deployment.provisioning_operations o "
                    "JOIN forest_deployment.provisioning_jobs j USING(job_id) "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "PROVISIONING_OPERATION_CONFLICT",
                        "Provisioning operation input differs",
                        409,
                    )
                return dict(replay)
            current = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.provisioning_jobs WHERE job_id=%s FOR UPDATE",
                    (job_id,),
                )
            ).fetchone()
            if current is None:
                raise ForestError("PROVISIONING_JOB_NOT_FOUND", "Provisioning Job not found", 404)
            if current["revision"] != expected_revision:
                raise ForestError(
                    "PROVISIONING_REVISION_CONFLICT",
                    "Provisioning Job changed",
                    409,
                    details={"revision": current["revision"]},
                )
            if current["state"] not in {"QUEUED", "RETRY_WAIT"}:
                raise ForestError(
                    "PROVISIONING_NOT_CANCELLABLE",
                    "Only idle provisioning Jobs can be cancelled",
                    409,
                )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.provisioning_jobs SET state='CANCELLED',"
                    "retry_not_before=NULL,last_error_code=NULL,completed_at=clock_timestamp(),"
                    "revision=revision+1,updated_at=clock_timestamp() WHERE job_id=%s RETURNING *",
                    (job_id,),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO forest_deployment.provisioning_operations("
                "operation_id,operation_kind,input_digest,job_id,resulting_revision) "
                "VALUES (%s,'CANCEL',%s,%s,%s)",
                (operation_id, input_digest, job_id, saved["revision"]),
            )
            return dict(saved)

    async def inspect(self, job_id: str) -> dict:
        _text(job_id, "job_id", 256)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.provisioning_jobs WHERE job_id=%s",
                    (job_id,),
                )
            ).fetchone()
        if row is None:
            raise ForestError("PROVISIONING_JOB_NOT_FOUND", "Provisioning Job not found", 404)
        return dict(row)

    async def held_claim(
        self, supervisor: DeploymentSupervisorLease, *, job_id: str,
        claim_token: str, attempt_no: int, conn=None,
    ) -> DeploymentProvisioningClaim:
        _text(job_id, "job_id", 256)
        _text(claim_token, "claim_token", 256)
        if type(attempt_no) is not int or attempt_no < 1:
            raise ForestError("PROVISIONING_INPUT_INVALID", "Attempt is invalid", 422)
        async with self.store.transaction(conn) as conn:
            await self._verify_supervisor_in(conn, supervisor)
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.provisioning_jobs WHERE job_id=%s "
                    "AND state='RUNNING' AND lease_owner=%s AND attempt_no=%s "
                    "AND lease_expires_at>clock_timestamp() FOR SHARE",
                    (job_id, claim_token, attempt_no),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "STALE_PROVISIONING_CLAIM", "Provisioning claim is stale", 409
                )
            return self._claim(row)

    async def list(self, *, limit: int = 100) -> tuple[dict, ...]:
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ForestError("PROVISIONING_INPUT_INVALID", "List limit is invalid", 422)
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.provisioning_jobs "
                    "ORDER BY created_at,job_id LIMIT %s",
                    (limit,),
                )
            ).fetchall()
            return tuple(dict(row) for row in rows)

    async def page(
        self, *, limit: int = 50, after: str | None = None,
        bot_id: str | None = None, state: str | None = None,
    ) -> dict:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("PROVISIONING_QUERY_INVALID", "Page limit is invalid", 422)
        if after is not None:
            _text(after, "after", 256)
        if bot_id is not None:
            _text(bot_id, "bot_id", 128)
        if state is not None and state not in {
            "QUEUED", "RUNNING", "RETRY_WAIT", "BLOCKED_UNKNOWN",
            "SUCCEEDED", "FAILED", "CANCELLED",
        }:
            raise ForestError("PROVISIONING_QUERY_INVALID", "Job state is invalid", 422)
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT job_id,bot_id,state,revision,attempt_no,max_attempts,"
                    "last_error_code FROM forest_deployment.provisioning_jobs "
                    "WHERE (%s::text IS NULL OR job_id>%s) "
                    "AND (%s::text IS NULL OR bot_id=%s) "
                    "AND (%s::text IS NULL OR state=%s) "
                    "ORDER BY job_id LIMIT %s",
                    (after, after, bot_id, bot_id, state, state, limit + 1),
                )
            ).fetchall()
        items = [dict(row) for row in rows[:limit]]
        return {
            "items": items,
            "next_after": items[-1]["job_id"] if len(rows) > limit else None,
        }

    async def _finish_in(self, conn, claim, *, state: str, error_code: str | None):
        await self._owned_in(conn, claim)
        return await (
            await conn.execute(
                "UPDATE forest_deployment.provisioning_jobs SET state=%s,lease_owner=NULL,"
                "lease_expires_at=NULL,retry_not_before=NULL,last_error_code=%s,"
                "completed_at=clock_timestamp(),revision=revision+1,updated_at=clock_timestamp() "
                "WHERE job_id=%s RETURNING *",
                (state, error_code, claim.job_id),
            )
        ).fetchone()

    async def _owned_in(self, conn, claim: DeploymentProvisioningClaim):
        self._validate_claim(claim)
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.provisioning_jobs WHERE job_id=%s "
                "AND state='RUNNING' AND lease_owner=%s AND lease_epoch=%s "
                "AND lease_expires_at>clock_timestamp() FOR UPDATE",
                (claim.job_id, claim.lease_owner, claim.lease_epoch),
            )
        ).fetchone()
        if row is None:
            raise ForestError("STALE_PROVISIONING_CLAIM", "Provisioning claim is stale", 409)
        return row

    async def _verify_supervisor_in(self, conn, lease: DeploymentSupervisorLease) -> None:
        await verify_deployment_supervisor_in(
            conn, lease, self.store.deployment_id,
            invalid_code="PROVISIONING_INPUT_INVALID",
        )

    def _claim(self, row) -> DeploymentProvisioningClaim:
        return DeploymentProvisioningClaim(
            self.store.deployment_id,
            row["job_id"],
            row["bot_id"],
            row["display_name"],
            row["storage_profile_ref"],
            row["storage_generation"],
            row["bootstrap_profile_ref"],
            row["bootstrap_manifest_digest"],
            row["attempt_no"],
            row["lease_owner"],
            row["lease_epoch"],
            row["lease_expires_at"],
        )

    def _validate_claim(self, claim: DeploymentProvisioningClaim) -> None:
        if (
            type(claim) is not DeploymentProvisioningClaim
            or claim.deployment_id != self.store.deployment_id
        ):
            raise ForestError("PROVISIONING_INPUT_INVALID", "Provisioning claim is invalid", 422)

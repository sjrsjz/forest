"""Durable, generation-fenced job queue owned by ``job.scheduler``."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.payload_authority import PayloadSource

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


def _bounded(value, field: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("JOB_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _canonical(value) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError("JOB_INPUT_INVALID", "Job input is not canonical JSON", 422) from exc


@dataclass(frozen=True)
class JobRoute:
    job_type: str
    job_version: int
    input_schema_digest: str
    max_attempts: int
    timeout_seconds: int
    max_concurrency: int
    target_component_id: str
    target_generation_id: str
    target_service_id: str
    target_service_version: int


@dataclass(frozen=True)
class JobClaim:
    job_id: str
    job_type: str
    job_version: int
    input_payload_id: str
    input_payload_digest: str
    requested_by: str
    revision: int
    attempt_count: int
    claim_token: str
    worker_generation_id: str
    target_component_id: str
    target_generation_id: str
    target_service_id: str
    target_service_version: int
    lease_until: datetime
    timeout_seconds: int


class JobSchedulerRepository:
    """Serialize job admission, lease ownership, cancellation and terminal evidence."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payloads,
        generation_admission_verifier=None,
        generation_member_verifier=None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.job.scheduler")
        self.payloads = payloads
        self.generation_admission_verifier = generation_admission_verifier
        self.generation_member_verifier = generation_member_verifier

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    async def _admit_in(self, conn, generation_id: str, lane: str) -> None:
        if self.generation_admission_verifier is None:
            return
        await self.generation_admission_verifier(
            conn,
            component_id="job.scheduler",
            generation_id=generation_id,
            admission_lane=lane,
        )

    async def _member_in(self, conn, generation_id: str) -> None:
        if self.generation_member_verifier is None:
            return
        await self.generation_member_verifier(
            conn,
            component_id="job.scheduler",
            generation_id=generation_id,
        )

    @staticmethod
    def _validate_route(route: JobRoute) -> None:
        if (
            type(route) is not JobRoute
            or _IDENTIFIER.fullmatch(route.job_type) is None
            or type(route.job_version) is not int
            or route.job_version < 1
            or not isinstance(route.input_schema_digest, str)
            or not route.input_schema_digest.startswith("sha256:")
            or type(route.max_attempts) is not int
            or not 1 <= route.max_attempts <= 32
            or type(route.timeout_seconds) is not int
            or not 1 <= route.timeout_seconds <= 86_400
            or type(route.max_concurrency) is not int
            or not 1 <= route.max_concurrency <= 128
            or _IDENTIFIER.fullmatch(route.target_component_id) is None
            or _IDENTIFIER.fullmatch(route.target_service_id) is None
            or type(route.target_service_version) is not int
            or route.target_service_version < 1
        ):
            raise ForestError("JOB_ROUTE_INVALID", "Job route is invalid", 422)
        _bounded(route.target_generation_id, "target_generation_id", 256)

    @staticmethod
    def _projection(row) -> dict:
        return {
            key: row[key]
            for key in (
                "job_id",
                "operation_id",
                "job_type",
                "job_version",
                "input_schema_digest",
                "input_payload_id",
                "input_payload_digest",
                "requested_by",
                "priority",
                "state",
                "revision",
                "attempt_count",
                "max_attempts",
                "timeout_seconds",
                "worker_generation_id",
                "target_component_id",
                "target_generation_id",
                "target_service_id",
                "target_service_version",
                "lease_until",
                "cancel_requested_at",
                "cancel_requested_by",
                "cancel_reason_code",
                "result_payload_id",
                "result_payload_digest",
                "error_code",
                "available_at",
                "created_at",
                "started_at",
                "completed_at",
                "updated_at",
            )
        }

    async def enqueue(
        self,
        *,
        route: JobRoute,
        operation_id: str,
        requested_by: str,
        input_value: dict,
        source_conversation_ref: str | None = None,
        priority: int = 4,
        scheduler_generation_id: str | None = None,
    ) -> dict:
        self._validate_route(route)
        _bounded(operation_id, "operation_id", 256)
        _bounded(requested_by, "requested_by", 256)
        if not isinstance(input_value, dict) or type(priority) is not int or not 0 <= priority <= 4:
            raise ForestError("JOB_INPUT_INVALID", "Job input or priority is invalid", 422)
        if source_conversation_ref is not None:
            _bounded(source_conversation_ref, "source_conversation_ref", 256)
        input_digest = digest_json(
            {
                "job_type": route.job_type,
                "job_version": route.job_version,
                "input_schema_digest": route.input_schema_digest,
                "requested_by": requested_by,
                "input": input_value,
                "priority": priority,
            }
        )
        job_id = str(uuid5(NAMESPACE_URL, f"forest:{self.store.bot_id}:job:{operation_id}"))
        async with self.store.pool.connection() as conn, conn.transaction():
            if scheduler_generation_id is not None:
                _bounded(scheduler_generation_id, "scheduler_generation_id", 256)
                await self._admit_in(conn, scheduler_generation_id, "CONTROL")
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.job.scheduler/{self.store.bot_id}/{operation_id}",),
            )
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("jobs")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError("JOB_OPERATION_CONFLICT", "Job operation input differs", 409)
                return self._projection(existing)
            deletion_epoch = await self.payloads.epoch_in(conn)
            payload = await self.payloads.put_json_in(
                conn,
                owner_component_id="job.scheduler",
                owner_kind="job.input",
                owner_id=job_id,
                value=input_value,
                sources=(
                    PayloadSource(
                        "job.scheduler",
                        "management.request",
                        operation_id,
                        1,
                        observation_conversation_id=source_conversation_ref,
                        author_subject_id=requested_by,
                    ),
                ),
                expected_deletion_epoch=deletion_epoch,
            )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,job_id,operation_id,input_digest,job_type,"
                        "job_version,input_schema_digest,input_payload_id,input_payload_digest,"
                        "requested_by,priority,max_attempts,timeout_seconds,max_concurrency) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("jobs")),
                    (
                        self.store.bot_id,
                        job_id,
                        operation_id,
                        input_digest,
                        route.job_type,
                        route.job_version,
                        route.input_schema_digest,
                        payload.payload_id,
                        payload.content_digest,
                        requested_by,
                        priority,
                        route.max_attempts,
                        route.timeout_seconds,
                        route.max_concurrency,
                    ),
                )
            ).fetchone()
            return self._projection(saved)

    async def claim(
        self,
        *,
        worker_generation_id: str,
        routes: dict[tuple[str, int], JobRoute],
        lease_seconds: int = 30,
    ) -> JobClaim | None:
        _bounded(worker_generation_id, "worker_generation_id", 256)
        if type(lease_seconds) is not int or not 3 <= lease_seconds <= 300:
            raise ForestError("JOB_INPUT_INVALID", "Job lease is invalid", 422)
        for key, route in routes.items():
            self._validate_route(route)
            if key != (route.job_type, route.job_version):
                raise ForestError("JOB_ROUTE_INVALID", "Job route key differs", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._admit_in(conn, worker_generation_id, "DATA")
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='CANCELLED',revision=revision+1,claim_token=NULL,"
                    "worker_component_id=NULL,worker_generation_id=NULL,target_component_id=NULL,"
                    "target_generation_id=NULL,target_service_id=NULL,target_service_version=NULL,"
                    "lease_until=NULL,completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND state='RUNNING' AND lease_until<=clock_timestamp() "
                    "AND cancel_requested_at IS NOT NULL"
                ).format(self._table("jobs")),
                (self.store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='UNKNOWN',revision=revision+1,claim_token=NULL,"
                    "worker_component_id=NULL,worker_generation_id=NULL,target_component_id=NULL,"
                    "target_generation_id=NULL,target_service_id=NULL,target_service_version=NULL,"
                    "lease_until=NULL,error_code='JOB_LEASE_EXHAUSTED',"
                    "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND state='RUNNING' AND lease_until<=clock_timestamp() "
                    "AND attempt_count>=max_attempts"
                ).format(self._table("jobs")),
                (self.store.bot_id,),
            )
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} j WHERE j.bot_id=%s AND ("
                        "(j.state='QUEUED' AND j.available_at<=clock_timestamp()) OR "
                        "(j.state='RUNNING' AND j.lease_until<=clock_timestamp())) "
                        "AND j.attempt_count<j.max_attempts AND NOT EXISTS ("
                        "SELECT 1 FROM {} active WHERE active.bot_id=j.bot_id "
                        "AND active.job_type=j.job_type AND active.job_version=j.job_version "
                        "AND active.state='RUNNING' AND active.lease_until>clock_timestamp() "
                        "AND active.job_id<>j.job_id GROUP BY active.bot_id "
                        "HAVING count(*)>=j.max_concurrency) "
                        "ORDER BY j.priority,j.available_at,j.created_at,j.job_id "
                        "FOR UPDATE OF j SKIP LOCKED LIMIT 32"
                    ).format(self._table("jobs"), self._table("jobs")),
                    (self.store.bot_id,),
                )
            ).fetchall()
            selected = None
            route = None
            for candidate in rows:
                current = routes.get((candidate["job_type"], candidate["job_version"]))
                if current is None or current.input_schema_digest != candidate["input_schema_digest"]:
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='FAILED',revision=revision+1,"
                            "claim_token=NULL,worker_component_id=NULL,worker_generation_id=NULL,"
                            "target_component_id=NULL,target_generation_id=NULL,"
                            "target_service_id=NULL,target_service_version=NULL,lease_until=NULL,"
                            "error_code='JOB_ROUTE_UNAVAILABLE',completed_at=clock_timestamp(),"
                            "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s"
                        ).format(self._table("jobs")),
                        (self.store.bot_id, candidate["job_id"]),
                    )
                    continue
                selected, route = candidate, current
                break
            if selected is None or route is None:
                return None
            token = str(uuid5(NAMESPACE_URL, f"{selected['job_id']}:{selected['revision'] + 1}"))
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='RUNNING',revision=revision+1,"
                        "attempt_count=attempt_count+1,claim_token=%s,"
                        "worker_component_id='job.scheduler',worker_generation_id=%s,"
                        "target_component_id=%s,target_generation_id=%s,target_service_id=%s,"
                        "target_service_version=%s,lease_until=clock_timestamp()+"
                        "make_interval(secs => %s),started_at=COALESCE(started_at,clock_timestamp()),"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s RETURNING *"
                    ).format(self._table("jobs")),
                    (
                        token,
                        worker_generation_id,
                        route.target_component_id,
                        route.target_generation_id,
                        route.target_service_id,
                        route.target_service_version,
                        lease_seconds,
                        self.store.bot_id,
                        selected["job_id"],
                    ),
                )
            ).fetchone()
            return JobClaim(
                **{
                    field: saved[field]
                    for field in JobClaim.__dataclass_fields__
                }
            )

    async def renew(self, claim: JobClaim, *, lease_seconds: int = 30) -> JobClaim:
        if type(claim) is not JobClaim or type(lease_seconds) is not int or not 3 <= lease_seconds <= 300:
            raise ForestError("JOB_INPUT_INVALID", "Job renewal is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._member_in(conn, claim.worker_generation_id)
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET lease_until=clock_timestamp()+"
                        "make_interval(secs => %s),updated_at=clock_timestamp() WHERE bot_id=%s "
                        "AND job_id=%s AND state='RUNNING' AND claim_token=%s "
                        "AND worker_generation_id=%s AND lease_until>clock_timestamp() RETURNING *"
                    ).format(self._table("jobs")),
                    (
                        lease_seconds,
                        self.store.bot_id,
                        claim.job_id,
                        claim.claim_token,
                        claim.worker_generation_id,
                    ),
                )
            ).fetchone()
            if saved is None:
                raise ForestError("STALE_JOB_CLAIM", "Job claim is no longer owned", 409)
            return JobClaim(
                **{field: saved[field] for field in JobClaim.__dataclass_fields__}
            )

    async def cancellation_requested(self, claim: JobClaim) -> bool:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._member_in(conn, claim.worker_generation_id)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT cancel_requested_at FROM {} WHERE bot_id=%s AND job_id=%s "
                        "AND state='RUNNING' AND claim_token=%s AND worker_generation_id=%s "
                        "AND lease_until>clock_timestamp()"
                    ).format(self._table("jobs")),
                    (
                        self.store.bot_id,
                        claim.job_id,
                        claim.claim_token,
                        claim.worker_generation_id,
                    ),
                )
            ).fetchone()
        if row is None:
            raise ForestError("STALE_JOB_CLAIM", "Job claim is no longer owned", 409)
        return row["cancel_requested_at"] is not None

    async def cancel(
        self,
        *,
        operation_id: str,
        job_id: str,
        expected_revision: int,
        requested_by: str,
        reason_code: str = "ADMIN_REQUESTED",
        scheduler_generation_id: str | None = None,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (job_id, "job_id"),
            (requested_by, "requested_by"),
        ):
            _bounded(value, field, 256)
        if type(expected_revision) is not int or expected_revision <= 0 or _ERROR.fullmatch(reason_code) is None:
            raise ForestError("JOB_INPUT_INVALID", "Job cancellation is invalid", 422)
        input_digest = digest_json(
            {
                "job_id": job_id,
                "expected_revision": expected_revision,
                "requested_by": requested_by,
                "reason_code": reason_code,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            if scheduler_generation_id is not None:
                _bounded(scheduler_generation_id, "scheduler_generation_id", 256)
                await self._admit_in(conn, scheduler_generation_id, "CONTROL")
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.job.scheduler/cancel/{self.store.bot_id}/{job_id}",),
            )
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("cancellation_requests")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("JOB_OPERATION_CONFLICT", "Cancellation input differs", 409)
                return dict(replay)
            job = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND job_id=%s FOR UPDATE").format(
                        self._table("jobs")
                    ),
                    (self.store.bot_id, job_id),
                )
            ).fetchone()
            if job is None:
                raise ForestError("JOB_NOT_FOUND", "Job does not exist", 404)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND job_id=%s").format(
                        self._table("cancellation_requests")
                    ),
                    (self.store.bot_id, job_id),
                )
            ).fetchone()
            if existing is not None:
                raise ForestError("JOB_CANCELLATION_CONFLICT", "Job already has a cancellation", 409)
            if job["revision"] != expected_revision:
                raise ForestError(
                    "JOB_REVISION_CONFLICT",
                    "Job revision changed",
                    409,
                    details={"revision": job["revision"]},
                )
            if job["state"] == "QUEUED":
                job = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='CANCELLED',revision=revision+1,"
                            "cancel_requested_at=clock_timestamp(),cancel_requested_by=%s,"
                            "cancel_reason_code=%s,completed_at=clock_timestamp(),"
                            "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s RETURNING *"
                        ).format(self._table("jobs")),
                        (requested_by, reason_code, self.store.bot_id, job_id),
                    )
                ).fetchone()
            elif job["state"] == "RUNNING":
                job = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET revision=revision+1,cancel_requested_at=clock_timestamp(),"
                            "cancel_requested_by=%s,cancel_reason_code=%s,updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND job_id=%s RETURNING *"
                        ).format(self._table("jobs")),
                        (requested_by, reason_code, self.store.bot_id, job_id),
                    )
                ).fetchone()
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,job_id,expected_revision,"
                        "requested_by,reason_code,resulting_state,resulting_revision) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("cancellation_requests")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        job_id,
                        expected_revision,
                        requested_by,
                        reason_code,
                        job["state"],
                        job["revision"],
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def succeed(self, claim: JobClaim, *, result: dict) -> dict:
        if type(claim) is not JobClaim or not isinstance(result, dict):
            raise ForestError("JOB_RESULT_INVALID", "Job result is invalid", 422)
        body = _canonical(result)
        if len(body) > 1024 * 1024:
            raise ForestError("JOB_RESULT_INVALID", "Job result exceeds 1 MiB", 413)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._member_in(conn, claim.worker_generation_id)
            row = await self._owned_in(conn, claim)
            if row["cancel_requested_at"] is not None:
                saved = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='CANCELLED',revision=revision+1,"
                            "claim_token=NULL,worker_component_id=NULL,worker_generation_id=NULL,"
                            "target_component_id=NULL,target_generation_id=NULL,"
                            "target_service_id=NULL,target_service_version=NULL,lease_until=NULL,"
                            "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND job_id=%s AND claim_token=%s RETURNING *"
                        ).format(self._table("jobs")),
                        (self.store.bot_id, claim.job_id, claim.claim_token),
                    )
                ).fetchone()
                if saved is None:
                    raise ForestError("STALE_JOB_CLAIM", "Job claim is no longer owned", 409)
                return self._projection(saved)
            epoch = await self.payloads.epoch_in(conn)
            payload = await self.payloads.put_json_in(
                conn,
                owner_component_id="job.scheduler",
                owner_kind="job.result",
                owner_id=claim.job_id,
                value=result,
                parents=(row["input_payload_id"],),
                expected_deletion_epoch=epoch,
            )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='SUCCEEDED',revision=revision+1,claim_token=NULL,"
                        "worker_component_id=NULL,worker_generation_id=NULL,target_component_id=NULL,"
                        "target_generation_id=NULL,target_service_id=NULL,target_service_version=NULL,"
                        "lease_until=NULL,result_payload_id=%s,result_payload_digest=%s,"
                        "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND job_id=%s AND claim_token=%s RETURNING *"
                    ).format(self._table("jobs")),
                    (
                        payload.payload_id,
                        payload.content_digest,
                        self.store.bot_id,
                        claim.job_id,
                        claim.claim_token,
                    ),
                )
            ).fetchone()
            if saved is None:
                raise ForestError("STALE_JOB_CLAIM", "Job claim is no longer owned", 409)
            return self._projection(saved)

    async def fail(
        self,
        claim: JobClaim,
        *,
        error_code: str,
        retryable: bool,
        unknown: bool = False,
        retry_delay_seconds: int = 1,
    ) -> dict:
        if (
            type(claim) is not JobClaim
            or _ERROR.fullmatch(error_code) is None
            or type(retryable) is not bool
            or type(unknown) is not bool
            or type(retry_delay_seconds) is not int
            or not 0 <= retry_delay_seconds <= 86_400
        ):
            raise ForestError("JOB_RESULT_INVALID", "Job failure is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._member_in(conn, claim.worker_generation_id)
            row = await self._owned_in(conn, claim)
            cancelled = row["cancel_requested_at"] is not None
            can_retry = retryable and not unknown and not cancelled and row["attempt_count"] < row[
                "max_attempts"
            ]
            state = "QUEUED" if can_retry else "CANCELLED" if cancelled else "UNKNOWN" if unknown else "FAILED"
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,revision=revision+1,claim_token=NULL,"
                        "worker_component_id=NULL,worker_generation_id=NULL,target_component_id=NULL,"
                        "target_generation_id=NULL,target_service_id=NULL,target_service_version=NULL,"
                        "lease_until=NULL,error_code=%s,available_at=CASE WHEN %s THEN "
                        "clock_timestamp()+make_interval(secs => %s) ELSE available_at END,"
                        "completed_at=CASE WHEN %s THEN NULL ELSE clock_timestamp() END,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s "
                        "AND claim_token=%s RETURNING *"
                    ).format(self._table("jobs")),
                    (
                        state,
                        None if can_retry or cancelled else error_code,
                        can_retry,
                        retry_delay_seconds,
                        can_retry,
                        self.store.bot_id,
                        claim.job_id,
                        claim.claim_token,
                    ),
                )
            ).fetchone()
            if saved is None:
                raise ForestError("STALE_JOB_CLAIM", "Job claim is no longer owned", 409)
            return self._projection(saved)

    async def _owned_in(self, conn, claim: JobClaim):
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND job_id=%s AND state='RUNNING' "
                    "AND claim_token=%s AND worker_generation_id=%s "
                    "AND lease_until>clock_timestamp() FOR UPDATE"
                ).format(self._table("jobs")),
                (
                    self.store.bot_id,
                    claim.job_id,
                    claim.claim_token,
                    claim.worker_generation_id,
                ),
            )
        ).fetchone()
        if row is None:
            raise ForestError("STALE_JOB_CLAIM", "Job claim is no longer owned", 409)
        return row

    async def inspect(self, job_id: str) -> dict:
        _bounded(job_id, "job_id", 256)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND job_id=%s").format(
                        self._table("jobs")
                    ),
                    (self.store.bot_id, job_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("JOB_NOT_FOUND", "Job does not exist", 404)
        return self._projection(row)

    async def list_jobs(
        self,
        *,
        limit: int = 10,
        state: str | None = None,
        job_type: str | None = None,
        before_created_at: datetime | None = None,
        before_job_id: str | None = None,
    ) -> tuple[dict, ...]:
        page = await self.query_jobs(
            limit=limit,
            states=() if state is None else (state,),
            job_type=job_type,
            before_created_at=before_created_at,
            before_job_id=before_job_id,
        )
        return page["items"]

    async def query_jobs(
        self,
        *,
        limit: int = 50,
        states: tuple[str, ...] = (),
        job_type: str | None = None,
        requested_by: str | None = None,
        snapshot_created_at: datetime | None = None,
        before_created_at: datetime | None = None,
        before_job_id: str | None = None,
    ) -> dict:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("JOB_INPUT_INVALID", "Job page limit is invalid", 422)
        allowed_states = {
            "QUEUED",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
            "UNKNOWN",
        }
        if (
            not isinstance(states, tuple)
            or len(states) != len(set(states))
            or any(state not in allowed_states for state in states)
        ):
            raise ForestError("JOB_INPUT_INVALID", "Job state filter is invalid", 422)
        if job_type is not None and _IDENTIFIER.fullmatch(job_type) is None:
            raise ForestError("JOB_INPUT_INVALID", "Job type filter is invalid", 422)
        if requested_by is not None:
            _bounded(requested_by, "requested_by", 256)
        if (before_created_at is None) != (before_job_id is None):
            raise ForestError("JOB_INPUT_INVALID", "Job cursor is incomplete", 422)
        for value, field in (
            (snapshot_created_at, "snapshot_created_at"),
            (before_created_at, "before_created_at"),
        ):
            if value is not None and value.tzinfo is None:
                raise ForestError("JOB_INPUT_INVALID", f"{field} has no timezone", 422)
        if snapshot_created_at is not None:
            snapshot_created_at = snapshot_created_at.astimezone(timezone.utc)
        if before_created_at is not None:
            before_created_at = before_created_at.astimezone(timezone.utc)
            _bounded(before_job_id, "before_job_id", 256)
        async with self.store.pool.connection() as conn:
            if snapshot_created_at is None:
                snapshot_created_at = (await (await conn.execute("SELECT clock_timestamp() AS value")).fetchone())[
                    "value"
                ]
            clauses = [sql.SQL("bot_id=%s"), sql.SQL("created_at<=%s")]
            values: list[object] = [self.store.bot_id, snapshot_created_at]
            if states:
                clauses.append(sql.SQL("state=ANY(%s)"))
                values.append(list(states))
            if job_type is not None:
                clauses.append(sql.SQL("job_type=%s"))
                values.append(job_type)
            if requested_by is not None:
                clauses.append(sql.SQL("requested_by=%s"))
                values.append(requested_by)
            if before_created_at is not None:
                clauses.append(sql.SQL("(created_at,job_id)<(%s,%s)"))
                values.extend((before_created_at, before_job_id))
            query = sql.SQL(
                "SELECT * FROM {} WHERE {} ORDER BY created_at DESC,job_id DESC LIMIT %s"
            ).format(self._table("jobs"), sql.SQL(" AND ").join(clauses))
            values.append(limit + 1)
            rows = await (await conn.execute(query, tuple(values))).fetchall()
        visible = rows[:limit]
        has_more = len(rows) > limit
        return {
            "snapshot_created_at": snapshot_created_at,
            "items": tuple(self._projection(row) for row in visible),
            "has_more": has_more,
            "next_before_created_at": visible[-1]["created_at"] if has_more else None,
            "next_before_job_id": visible[-1]["job_id"] if has_more else None,
        }

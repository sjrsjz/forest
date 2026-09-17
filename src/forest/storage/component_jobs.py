"""Durable plugin-owned work, separate from conversation Agent schedules."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.bot import CallGrant, HostLease

_JOB_STATUSES = frozenset({"PENDING", "INFLIGHT", "RETRY_WAIT", "ACKED", "DEAD", "RETIRED"})


def _management_payload(payload, required, optional=()):
    if (
        not isinstance(payload, dict)
        or set(payload) - set(required) - set(optional)
        or not set(required) <= set(payload)
    ):
        raise ForestError("COMPONENT_JOB_MANAGEMENT_INVALID", "Management fields differ", 422)
    for key in ("job_id", "component_id", "handler_id", "plan_id", "plan_digest"):
        if key in payload and (
            not isinstance(payload[key], str) or not 1 <= len(payload[key]) <= 256
        ):
            raise ForestError(
                "COMPONENT_JOB_MANAGEMENT_INVALID",
                "Management identifiers must be bounded text",
                422,
            )
    for key in ("expected_revision", "limit"):
        if key in payload and (type(payload[key]) is not int or payload[key] < 0):
            raise ForestError(
                "COMPONENT_JOB_MANAGEMENT_INVALID", "Revision or limit is invalid", 422
            )
    if "limit" in payload and not 1 <= payload["limit"] <= 200:
        raise ForestError("COMPONENT_JOB_MANAGEMENT_INVALID", "Limit must be 1 to 200", 422)
    if "status" in payload and payload["status"] not in _JOB_STATUSES:
        raise ForestError("COMPONENT_JOB_MANAGEMENT_INVALID", "Unknown job status", 422)


class ComponentJobStore:
    def __init__(self, store, *, control=None):
        self.store = store
        if control is None:
            from forest.storage.control import BotControl

            control = BotControl(store)
        self.control = control

    async def _clock_in(self, conn) -> dict:
        await conn.execute(
            "INSERT INTO public.bot_component_job_clock(bot_id) VALUES (%s) "
            "ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_component_job_clock WHERE bot_id=%s FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchone()

    async def register_generation_in(
        self,
        conn,
        generation_id: str,
        component_id: str,
        definitions: list[dict],
        *,
        existing_generation: bool = False,
    ) -> None:
        normalized = sorted(
            (
                item["handler_id"],
                item["schema_digest"],
                item["max_attempts"],
                float(item["timeout_seconds"]),
                item["max_concurrency"],
            )
            for item in definitions
        )
        existing = await (
            await conn.execute(
                "SELECT handler_id,schema_digest,max_attempts,timeout_seconds,max_concurrency "
                "FROM public.bot_generation_job_definitions WHERE bot_id=%s AND generation_id=%s "
                "ORDER BY handler_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        if existing:
            actual = [
                (
                    row["handler_id"], row["schema_digest"], row["max_attempts"],
                    float(row["timeout_seconds"]), row["max_concurrency"],
                )
                for row in existing
            ]
            if actual != normalized:
                raise ForestError(
                    "GENERATION_CONFLICT", "Generation job declarations cannot be reused", 409
                )
            return
        if existing_generation and normalized:
            raise ForestError(
                "GENERATION_CONFLICT", "Generation job declarations cannot be added later", 409
            )
        for item in definitions:
            await conn.execute(
                "INSERT INTO public.bot_generation_job_definitions("
                "bot_id,generation_id,component_id,handler_id,schema_digest,max_attempts,"
                "timeout_seconds,max_concurrency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, generation_id, component_id, item["handler_id"],
                    item["schema_digest"], item["max_attempts"], item["timeout_seconds"],
                    item["max_concurrency"],
                ),
            )

    async def publish_generation_in(
        self, conn, generation_id: str, component_id: str
    ) -> None:
        await self._clock_in(conn)
        definitions = await (
            await conn.execute(
                "SELECT * FROM public.bot_generation_job_definitions WHERE bot_id=%s "
                "AND generation_id=%s ORDER BY handler_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        existing = await (
            await conn.execute(
                "SELECT * FROM public.bot_component_job_heads WHERE bot_id=%s "
                "AND component_id=%s ORDER BY handler_id FOR UPDATE",
                (self.store.bot_id, component_id),
            )
        ).fetchall()
        wanted = {row["handler_id"] for row in definitions}
        if any(row["active"] and row["handler_id"] not in wanted for row in existing):
            raise ForestError(
                "JOB_HANDLER_REMOVAL_REQUIRES_MANAGEMENT",
                "Durable job handlers require an explicit retirement operation",
                409,
            )
        for definition in definitions:
            current = next(
                (row for row in existing if row["handler_id"] == definition["handler_id"]),
                None,
            )
            if current is not None and current["schema_digest"] != definition["schema_digest"]:
                raise ForestError(
                    "JOB_HANDLER_IDENTITY_CONFLICT",
                    "Stable job handler identity changed its input schema",
                    409,
                )
            if current is not None and not current["active"]:
                raise ForestError(
                    "JOB_HANDLER_REACTIVATION_REQUIRES_MANAGEMENT",
                    "A retired job handler cannot be restored by deployment",
                    409,
                )
            await conn.execute(
                "INSERT INTO public.bot_component_job_heads("
                "bot_id,component_id,handler_id,generation_id,schema_digest,max_attempts,"
                "timeout_seconds,max_concurrency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (bot_id,component_id,handler_id) DO UPDATE SET "
                "generation_id=EXCLUDED.generation_id,max_attempts=EXCLUDED.max_attempts,"
                "timeout_seconds=EXCLUDED.timeout_seconds,"
                "max_concurrency=EXCLUDED.max_concurrency,updated_at=clock_timestamp()",
                (
                    self.store.bot_id, component_id, definition["handler_id"], generation_id,
                    definition["schema_digest"], definition["max_attempts"],
                    definition["timeout_seconds"], definition["max_concurrency"],
                ),
            )

    @staticmethod
    def _receipt(row: dict) -> dict:
        return {
            "job_id": row["job_id"],
            "handler_id": row["handler_id"],
            "status": row["status"],
            "available_at": row["first_available_at"].isoformat(),
        }

    async def schedule(
        self,
        grant: CallGrant,
        *,
        handler_id: str,
        schema_digest: str,
        operation_id: str,
        input_digest: str,
        payload,
        delay_seconds: float,
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            call, host = await self.store._guard(conn, grant, pending=False)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s "
                    "AND component_id=%s AND operation_id=%s",
                    (self.store.bot_id, grant.component_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["handler_id"] != handler_id
                    or existing["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Job operation already has different input", 409
                    )
                await self.store._guard_time(conn, host, grant)
                return self._receipt(existing)
            definition = await (
                await conn.execute(
                    "SELECT d.* FROM public.bot_generation_job_definitions d "
                    "JOIN public.bot_component_job_heads h ON h.bot_id=d.bot_id "
                    "AND h.component_id=d.component_id AND h.handler_id=d.handler_id "
                    "WHERE d.bot_id=%s AND d.generation_id=%s AND d.handler_id=%s "
                    "AND h.active=true AND h.generation_id=d.generation_id",
                    (self.store.bot_id, grant.generation_id, handler_id),
                )
            ).fetchone()
            if definition is None or definition["schema_digest"] != schema_digest:
                raise ForestError(
                    "JOB_DECLARATION_MISMATCH", "Job handler is not declared by this generation", 403
                )
            parents = set(await self.store.content.parents_in(conn, grant.generation_id))
            if call.get("input_payload_id"):
                parents.add(call["input_payload_id"])
            epoch = await self.store.content.epoch_in(conn)
            clock = await self._clock_in(conn)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s "
                    "AND component_id=%s AND operation_id=%s",
                    (self.store.bot_id, grant.component_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["handler_id"] != handler_id
                    or existing["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Job operation already has different input", 409
                    )
                await self.store._guard_time(conn, host, grant)
                return self._receipt(existing)
            job_seq = clock["next_sequence"] + 1
            await conn.execute(
                "UPDATE public.bot_component_job_clock SET next_sequence=%s WHERE bot_id=%s",
                (job_seq, self.store.bot_id),
            )
            job_id = str(uuid4())
            payload_ref = await self.store.content.bodies.put_in(
                conn,
                "component.job",
                "job:" + job_id,
                payload,
                parents=tuple(sorted(parents)),
                epoch=epoch,
            )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_component_jobs("
                    "bot_id,job_id,job_seq,component_id,handler_id,source_generation_id,"
                    "source_call_id,operation_id,input_digest,payload_id,payload_digest,"
                    "max_attempts,first_available_at,available_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "clock_timestamp()+%s*interval '1 second',"
                    "clock_timestamp()+%s*interval '1 second') RETURNING *",
                    (
                        self.store.bot_id, job_id, job_seq, grant.component_id, handler_id,
                        grant.generation_id, grant.call_id, operation_id, input_digest,
                        payload_ref.payload_id, payload_ref.digest, definition["max_attempts"],
                        delay_seconds, delay_seconds,
                    ),
                )
            ).fetchone()
            await self.store._event(
                conn,
                "component.job.scheduled",
                operation_id,
                {"job_id": job_id, "component_id": grant.component_id, "handler_id": handler_id},
            )
            await self.store._guard_time(conn, host, grant)
            return self._receipt(row)

    async def claim(self, lease: HostLease, *, lease_seconds: float = 30) -> dict | None:
        if type(lease_seconds) not in {int, float} or not 3 <= lease_seconds <= 120:
            raise ForestError("INVALID_JOB_LEASE", "Job claim lease must be 3 to 120 seconds", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            if not runtime["data_admission"]:
                return None
            deletion_epoch = await self.store.content.epoch_in(conn)
            await self._clock_in(conn)
            row = await (
                await conn.execute(
                    "SELECT j.*,h.generation_id AS target_generation_id,h.schema_digest,"
                    "sc.causal_root_kind,sc.causal_root_id,"
                    "h.timeout_seconds,h.max_concurrency,r.route_epoch "
                    "FROM public.bot_component_jobs j "
                    "JOIN public.bot_calls sc ON sc.bot_id=j.bot_id AND sc.call_id=j.source_call_id "
                    "JOIN public.bot_component_job_heads h ON h.bot_id=j.bot_id "
                    "AND h.component_id=j.component_id AND h.handler_id=j.handler_id "
                    "JOIN public.bot_component_routes r ON r.bot_id=j.bot_id "
                    "AND r.component_id=j.component_id "
                    "JOIN public.bot_generations g ON g.bot_id=j.bot_id "
                    "AND g.generation_id=h.generation_id "
                    "WHERE j.bot_id=%s AND ((j.status IN ('PENDING','RETRY_WAIT') "
                    "AND j.available_at<=clock_timestamp() AND j.attempt_count<j.max_attempts) "
                    "OR (j.status='INFLIGHT' AND j.claim_until<=clock_timestamp())) "
                    "AND h.active=true AND r.generation_id=h.generation_id "
                    "AND r.admission=true AND g.status='ACTIVE' "
                    "AND g.host_incarnation=%s AND g.host_owner_epoch=%s "
                    "AND (h.max_concurrency>1 OR NOT EXISTS ("
                    "SELECT 1 FROM public.bot_component_jobs prior WHERE prior.bot_id=j.bot_id "
                    "AND prior.component_id=j.component_id AND prior.handler_id=j.handler_id "
                    "AND prior.job_seq<j.job_seq "
                    "AND prior.status NOT IN ('ACKED','DEAD','RETIRED'))) "
                    "AND (SELECT count(*) FROM public.bot_component_jobs running "
                    "WHERE running.bot_id=j.bot_id AND running.component_id=j.component_id "
                    "AND running.handler_id=j.handler_id AND running.job_id<>j.job_id "
                    "AND running.status='INFLIGHT' AND running.claim_until>clock_timestamp()) "
                    "< h.max_concurrency "
                    "ORDER BY j.available_at,j.job_seq,j.job_id LIMIT 1 "
                    "FOR UPDATE OF j,h SKIP LOCKED",
                    (self.store.bot_id, lease.incarnation, lease.owner_epoch),
                )
            ).fetchone()
            if row is None:
                return None
            continuing = row["status"] == "INFLIGHT"
            attempt = row["attempt_count"] if continuing else row["attempt_count"] + 1
            dispatch = row["dispatch_count"] if continuing else row["dispatch_count"] + 1
            operation = row["call_operation_id"] if continuing else "component-job:" + hashlib.sha256(
                f'{row["job_id"]}:{row["replay_epoch"]}:{dispatch}'.encode()
            ).hexdigest()
            token = str(uuid4())
            claimed = await (
                await conn.execute(
                    "UPDATE public.bot_component_jobs SET status='INFLIGHT',attempt_count=%s,"
                    "dispatch_count=%s,claim_token=%s,claim_host_incarnation=%s,"
                    "claim_owner_epoch=%s,claim_until=clock_timestamp()+%s*interval '1 second',"
                    "claim_new_attempt=%s,last_error_code=NULL,call_operation_id=%s "
                    "WHERE bot_id=%s AND job_id=%s RETURNING *",
                    (
                        attempt, dispatch, token, lease.incarnation, lease.owner_epoch,
                        lease_seconds, not continuing, operation, self.store.bot_id, row["job_id"],
                    ),
                )
            ).fetchone()
            try:
                payload = await self.store.content.bodies.read_in(
                    conn, row["payload_id"], epoch=deletion_epoch
                )
                self.store.content.bodies._verify(payload, row["payload_digest"])
            except ForestError as exc:
                if exc.code != "KERNEL_CONTENT_REVOKED":
                    raise
                await conn.execute(
                    "UPDATE public.bot_component_jobs SET status='DEAD',claim_token=NULL,"
                    "claim_host_incarnation=NULL,claim_owner_epoch=NULL,claim_until=NULL,"
                    "claim_new_attempt=NULL,last_error_code='KERNEL_CONTENT_REVOKED' "
                    "WHERE bot_id=%s AND job_id=%s",
                    (self.store.bot_id, row["job_id"]),
                )
                await self.store._event(
                    conn,
                    "component.job.dead",
                    None,
                    {"job_id": row["job_id"], "handler_id": row["handler_id"],
                     "error_code": "KERNEL_CONTENT_REVOKED"},
                )
                return None
            await self.store._host(conn, lease, runtime)
            return {
                **claimed,
                "claim_token": token,
                "causal_root_kind": row["causal_root_kind"],
                "causal_root_id": row["causal_root_id"],
                "target_generation_id": row["target_generation_id"],
                "route_epoch": row["route_epoch"],
                "schema_digest": row["schema_digest"],
                "timeout_seconds": float(row["timeout_seconds"]),
                "payload": payload,
                "job_payload_id": row["payload_id"],
                "call_operation_id": operation,
            }

    async def _claimed_in(self, conn, lease: HostLease, job: dict, *, lock: bool = True) -> dict:
        suffix = " FOR UPDATE" if lock else ""
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s AND job_id=%s" + suffix,
                (self.store.bot_id, job["job_id"]),
            )
        ).fetchone()
        if (
            row is None
            or row["status"] != "INFLIGHT"
            or row["claim_token"] != job["claim_token"]
            or row["claim_host_incarnation"] != lease.incarnation
            or row["claim_owner_epoch"] != lease.owner_epoch
            or row["call_operation_id"] != job["call_operation_id"]
        ):
            raise ForestError("JOB_DELIVERY_FENCED", "Component job lost its claim", 409)
        return row

    async def defer(self, lease: HostLease, job: dict, *, delay_seconds: float = 0.1) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await self._claimed_in(conn, lease, job)
            if not row["claim_new_attempt"]:
                await conn.execute(
                    "UPDATE public.bot_component_jobs SET claim_until=clock_timestamp(),"
                    "last_error_code='DELIVERY_DEFERRED' WHERE bot_id=%s AND job_id=%s",
                    (self.store.bot_id, job["job_id"]),
                )
                await self.store._host(conn, lease)
                return
            call = await (
                await conn.execute(
                    "SELECT status FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, row["call_operation_id"]),
                )
            ).fetchone()
            if call is not None and call["status"] in {"ADMITTED", "COMPLETED"}:
                # Preserve the exact operation after execution may have started.
                # A later claim reconciles the call ledger instead of running a
                # second handler attempt.
                await conn.execute(
                    "UPDATE public.bot_component_jobs SET claim_until=clock_timestamp(),"
                    "claim_new_attempt=false,last_error_code='DELIVERY_DEFERRED' "
                    "WHERE bot_id=%s AND job_id=%s",
                    (self.store.bot_id, job["job_id"]),
                )
                await self.store._host(conn, lease)
                return
            await conn.execute(
                "UPDATE public.bot_component_jobs SET status='RETRY_WAIT',"
                "attempt_count=attempt_count-1,dispatch_count=dispatch_count-%s,"
                "available_at=clock_timestamp()+%s*interval '1 second',claim_token=NULL,"
                "claim_host_incarnation=NULL,claim_owner_epoch=NULL,claim_until=NULL,"
                "claim_new_attempt=NULL WHERE bot_id=%s AND job_id=%s",
                (1 if call is None else 0, delay_seconds, self.store.bot_id, job["job_id"]),
            )
            await self.store._host(conn, lease)

    async def abandon(self, lease: HostLease, job: dict) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s AND job_id=%s FOR UPDATE",
                    (self.store.bot_id, job["job_id"]),
                )
            ).fetchone()
            if row is None or row["status"] in {"ACKED", "DEAD", "RETIRED"}:
                return
            await self._claimed_in(conn, lease, job, lock=False)
            await conn.execute(
                "UPDATE public.bot_component_jobs SET claim_until=clock_timestamp(),"
                "last_error_code='KERNEL_CLOSING' WHERE bot_id=%s AND job_id=%s",
                (self.store.bot_id, job["job_id"]),
            )
            await self.store._host(conn, lease)

    async def authorize_dispatch(self, lease: HostLease, job: dict) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            route = await self.store._route(conn, job["component_id"])
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            row = await (
                await conn.execute(
                    "SELECT j.*,h.generation_id FROM public.bot_component_jobs j "
                    "JOIN public.bot_component_job_heads h ON h.bot_id=j.bot_id "
                    "AND h.component_id=j.component_id AND h.handler_id=j.handler_id "
                    "WHERE j.bot_id=%s AND j.job_id=%s FOR UPDATE OF j",
                    (self.store.bot_id, job["job_id"]),
                )
            ).fetchone()
            await self._claimed_in(conn, lease, job, lock=False)
            if (
                row["claim_until"] <= await self.store._now(conn)
                or row["generation_id"] != job["target_generation_id"]
                or route["generation_id"] != row["generation_id"]
                or route["route_epoch"] != job["route_epoch"]
                or not route["admission"]
                or not runtime["data_admission"]
            ):
                raise ForestError(
                    "JOB_DELIVERY_FENCED", "Component job lost execution authority", 409
                )
            await self.store._host(conn, lease, runtime)

    async def acknowledge(
        self, lease: HostLease, job: dict, *, result_payload_id: str
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s AND job_id=%s FOR UPDATE",
                    (self.store.bot_id, job["job_id"]),
                )
            ).fetchone()
            if row is None or row["status"] == "ACKED":
                return
            row = await self._claimed_in(conn, lease, job, lock=False)
            proof = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s "
                    "AND component_id=%s AND status='COMPLETED' AND result_payload_id=%s",
                    (
                        self.store.bot_id, row["call_operation_id"], row["component_id"],
                        result_payload_id,
                    ),
                )
            ).fetchone()
            if proof is None:
                raise ForestError(
                    "JOB_RESULT_MISMATCH", "Job acknowledgement has no matching call result", 409
                )
            await conn.execute(
                "UPDATE public.bot_component_jobs SET status='ACKED',result_payload_id=%s,"
                "acked_at=clock_timestamp(),claim_token=NULL,claim_host_incarnation=NULL,"
                "claim_owner_epoch=NULL,claim_until=NULL,claim_new_attempt=NULL "
                "WHERE bot_id=%s AND job_id=%s",
                (result_payload_id, self.store.bot_id, job["job_id"]),
            )
            await self.store._host(conn, lease)

    async def fail(
        self, lease: HostLease, job: dict, error_code: str, *, terminal: bool = False
    ) -> None:
        if not isinstance(error_code, str) or not 1 <= len(error_code) <= 128:
            error_code = "JOB_HANDLER_FAILED"
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s AND job_id=%s FOR UPDATE",
                    (self.store.bot_id, job["job_id"]),
                )
            ).fetchone()
            if row is None or row["status"] in {"ACKED", "DEAD", "RETIRED"}:
                return
            row = await self._claimed_in(conn, lease, job, lock=False)
            dead = terminal or row["attempt_count"] >= row["max_attempts"]
            jitter = int(hashlib.sha256(
                f'{row["job_id"]}:{row["attempt_count"]}'.encode()
            ).hexdigest()[:2], 16) / 255
            delay = min(60, 2 ** max(0, row["attempt_count"] - 1)) + jitter
            await conn.execute(
                "UPDATE public.bot_component_jobs SET status=%s,"
                "available_at=clock_timestamp()+%s*interval '1 second',last_error_code=%s,"
                "claim_token=NULL,claim_host_incarnation=NULL,claim_owner_epoch=NULL,"
                "claim_until=NULL,claim_new_attempt=NULL WHERE bot_id=%s AND job_id=%s",
                (
                    "DEAD" if dead else "RETRY_WAIT", delay, error_code,
                    self.store.bot_id, job["job_id"],
                ),
            )
            if dead:
                await self.store._event(
                    conn,
                    "component.job.dead",
                    job["call_operation_id"],
                    {"job_id": job["job_id"], "handler_id": row["handler_id"],
                     "error_code": error_code},
                )
            await self.store._host(conn, lease)

    @staticmethod
    def _timestamp(value):
        return value.isoformat() if value is not None else None

    async def _head_in(self, conn, component_id: str, handler_id: str, *, lock=False):
        suffix = " FOR UPDATE" if lock else ""
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_component_job_heads WHERE bot_id=%s "
                "AND component_id=%s AND handler_id=%s" + suffix,
                (self.store.bot_id, component_id, handler_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "COMPONENT_JOB_HANDLER_NOT_FOUND", "Job handler does not belong to this Bot", 404
            )
        return row

    async def _impact_in(self, conn, component_id: str, handler_id: str) -> dict:
        rows = await (
            await conn.execute(
                "SELECT status,count(*) AS count FROM public.bot_component_jobs "
                "WHERE bot_id=%s AND component_id=%s AND handler_id=%s GROUP BY status",
                (self.store.bot_id, component_id, handler_id),
            )
        ).fetchall()
        counts = {status.lower(): 0 for status in _JOB_STATUSES}
        counts.update({row["status"].lower(): row["count"] for row in rows})
        return {
            "retire_pending": counts["pending"] + counts["retry_wait"],
            "inflight": counts["inflight"],
            "acked": counts["acked"],
            "dead": counts["dead"],
            "retired": counts["retired"],
            "total": sum(counts.values()),
        }

    async def read(self, ticket, operation_id, payload=None) -> dict:
        payload = {} if payload is None else dict(payload)
        _management_payload(payload, (), ("component_id", "handler_id", "status", "limit"))
        limit = payload.get("limit", 100)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "component_job.read", operation_id, payload
            )
            heads = await (
                await conn.execute(
                    "SELECT component_id,handler_id,generation_id,schema_digest,max_attempts,"
                    "timeout_seconds,max_concurrency,active,management_revision,updated_at,retired_at "
                    "FROM public.bot_component_job_heads WHERE bot_id=%s "
                    "AND (%s::text IS NULL OR component_id=%s) "
                    "AND (%s::text IS NULL OR handler_id=%s) "
                    "ORDER BY component_id,handler_id LIMIT %s",
                    (
                        self.store.bot_id, payload.get("component_id"), payload.get("component_id"),
                        payload.get("handler_id"), payload.get("handler_id"), limit,
                    ),
                )
            ).fetchall()
            rows = await (
                await conn.execute(
                    "SELECT j.job_id,j.job_seq,j.component_id,j.handler_id,j.operation_id,j.status,"
                    "j.attempt_count,j.dispatch_count,j.max_attempts,j.replay_epoch,"
                    "j.management_revision,j.last_error_code,j.first_available_at,j.available_at,"
                    "j.acked_at,j.retired_at,j.created_at,h.active AS handler_active,"
                    "CASE WHEN p.state='ACTIVE' AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b "
                    "WHERE b.bot_id=p.bot_id AND b.payload_id=p.payload_id) "
                    "THEN 'ACTIVE' ELSE 'UNAVAILABLE' END AS payload_state "
                    "FROM public.bot_component_jobs j "
                    "JOIN public.bot_component_job_heads h ON h.bot_id=j.bot_id "
                    "AND h.component_id=j.component_id AND h.handler_id=j.handler_id "
                    "JOIN public.bot_payload_headers p ON p.bot_id=j.bot_id AND p.payload_id=j.payload_id "
                    "WHERE j.bot_id=%s AND (%s::text IS NULL OR j.component_id=%s) "
                    "AND (%s::text IS NULL OR j.handler_id=%s) "
                    "AND (%s::text IS NULL OR j.status=%s) "
                    "ORDER BY j.job_seq DESC,j.job_id DESC LIMIT %s",
                    (
                        self.store.bot_id, payload.get("component_id"), payload.get("component_id"),
                        payload.get("handler_id"), payload.get("handler_id"),
                        payload.get("status"), payload.get("status"), limit,
                    ),
                )
            ).fetchall()
            await self.control._time(conn, claims)
            return {
                "handlers": [
                    {
                        **{key: row[key] for key in (
                            "component_id", "handler_id", "generation_id", "schema_digest",
                            "max_attempts", "max_concurrency", "active", "management_revision",
                        )},
                        "timeout_seconds": float(row["timeout_seconds"]),
                        "updated_at": self._timestamp(row["updated_at"]),
                        "retired_at": self._timestamp(row["retired_at"]),
                    }
                    for row in heads
                ],
                "jobs": [
                    {
                        **{key: row[key] for key in (
                            "job_id", "job_seq", "component_id", "handler_id", "operation_id",
                            "status", "attempt_count", "dispatch_count", "max_attempts",
                            "replay_epoch", "management_revision", "last_error_code", "payload_state",
                        )},
                        "first_available_at": self._timestamp(row["first_available_at"]),
                        "available_at": self._timestamp(row["available_at"]),
                        "created_at": self._timestamp(row["created_at"]),
                        "acked_at": self._timestamp(row["acked_at"]),
                        "retired_at": self._timestamp(row["retired_at"]),
                        "replay_allowed": bool(
                            row["status"] == "DEAD"
                            and row["handler_active"]
                            and row["payload_state"] == "ACTIVE"
                        ),
                    }
                    for row in rows
                ],
            }

    async def replay(self, ticket, operation_id, payload) -> dict:
        payload = dict(payload)
        _management_payload(payload, ("job_id", "expected_revision"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "component_job.replay", operation_id, payload
            )
            replayed = await self.control._replay(conn, claims)
            if self.control.is_replay(replayed):
                return replayed
            deletion_epoch = await self.store.content.epoch_in(conn)
            await self._clock_in(conn)
            identity = await (
                await conn.execute(
                    "SELECT component_id,handler_id FROM public.bot_component_jobs "
                    "WHERE bot_id=%s AND job_id=%s",
                    (self.store.bot_id, payload["job_id"]),
                )
            ).fetchone()
            if identity is None:
                raise ForestError(
                    "COMPONENT_JOB_NOT_FOUND", "Job does not belong to this Bot", 404
                )
            head = await self._head_in(
                conn, identity["component_id"], identity["handler_id"], lock=True
            )
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s AND job_id=%s FOR UPDATE",
                    (self.store.bot_id, payload["job_id"]),
                )
            ).fetchone()
            if row["management_revision"] != payload["expected_revision"]:
                raise ForestError(
                    "COMPONENT_JOB_REVISION_CONFLICT", "Job changed; reload it", 409
                )
            if row["status"] != "DEAD":
                raise ForestError("COMPONENT_JOB_NOT_DEAD", "Only DEAD jobs can be replayed", 409)
            if not head["active"]:
                raise ForestError(
                    "COMPONENT_JOB_HANDLER_RETIRED", "A retired handler cannot be replayed", 409
                )
            try:
                await self.store.content.payloads.require_available_in(
                    conn, (row["payload_id"],), expected_deletion_epoch=deletion_epoch
                )
            except ForestError as exc:
                if exc.code not in {
                    "PAYLOAD_ERASED", "PAYLOAD_INCOMPLETE", "PAYLOAD_NOT_FOUND",
                    "SOURCE_SUPPRESSED", "SOURCE_LINK_SCOPE_LIMIT",
                }:
                    raise
                raise ForestError(
                    "COMPONENT_JOB_PAYLOAD_UNAVAILABLE", "The job body is unavailable", 409
                ) from None
            next_epoch = row["replay_epoch"] + 1
            next_revision = row["management_revision"] + 1
            await conn.execute(
                "INSERT INTO public.bot_component_job_replays("
                "bot_id,job_id,replay_epoch,operation_id,subject_id,issuer_id,"
                "previous_attempt_count,previous_dispatch_count,previous_error_code) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, row["job_id"], next_epoch, operation_id,
                    claims["subject_id"], claims["issuer_id"], row["attempt_count"],
                    row["dispatch_count"], row["last_error_code"],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_component_jobs SET status='RETRY_WAIT',attempt_count=0,"
                "dispatch_count=0,replay_epoch=%s,management_revision=%s,"
                "available_at=clock_timestamp(),claim_token=NULL,claim_host_incarnation=NULL,"
                "claim_owner_epoch=NULL,claim_until=NULL,claim_new_attempt=NULL,"
                "call_operation_id=NULL,result_payload_id=NULL,last_error_code=NULL,acked_at=NULL,"
                "retired_at=NULL WHERE bot_id=%s AND job_id=%s",
                (next_epoch, next_revision, self.store.bot_id, row["job_id"]),
            )
            result = {
                "operation_id": operation_id,
                "job_id": row["job_id"],
                "status": "RETRY_WAIT",
                "replay_epoch": next_epoch,
                "management_revision": next_revision,
            }
            await self.control._record(conn, claims, result)
            return result

    async def plan_retirement(self, ticket, operation_id, payload) -> dict:
        payload = dict(payload)
        _management_payload(payload, ("component_id", "handler_id", "expected_revision"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "component_job.retire.plan", operation_id, payload
            )
            replayed = await self.control._replay(conn, claims)
            if self.control.is_replay(replayed):
                return replayed
            await self.store.content.epoch_in(conn)
            await self._clock_in(conn)
            head = await self._head_in(
                conn, payload["component_id"], payload["handler_id"], lock=True
            )
            if head["management_revision"] != payload["expected_revision"]:
                raise ForestError(
                    "COMPONENT_JOB_HANDLER_REVISION_CONFLICT", "Handler changed; reload it", 409
                )
            if not head["active"]:
                raise ForestError(
                    "COMPONENT_JOB_HANDLER_RETIRED", "Handler is already retired", 409
                )
            impact = await self._impact_in(conn, head["component_id"], head["handler_id"])
            impact_digest = digest_json(impact)
            frozen = {
                "component_id": head["component_id"],
                "handler_id": head["handler_id"],
                "expected_revision": head["management_revision"],
                "impact_digest": impact_digest,
                "subject_id": claims["subject_id"],
                "issuer_id": claims["issuer_id"],
            }
            plan_id, plan_digest = str(uuid4()), digest_json(frozen)
            expires = await self.store._now(conn) + timedelta(minutes=10)
            await conn.execute(
                "INSERT INTO public.bot_component_job_retirement_plans("
                "bot_id,plan_id,operation_id,subject_id,issuer_id,component_id,handler_id,"
                "expected_revision,impact,impact_digest,plan_digest,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, plan_id, operation_id, claims["subject_id"],
                    claims["issuer_id"], head["component_id"], head["handler_id"],
                    head["management_revision"], Jsonb(impact), impact_digest, plan_digest, expires,
                ),
            )
            result = {
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "component_id": head["component_id"],
                "handler_id": head["handler_id"],
                "expected_revision": head["management_revision"],
                "impact": impact,
                "expires_at": expires.isoformat(),
            }
            await self.control._record(conn, claims, result)
            return result

    async def apply_retirement(self, ticket, operation_id, payload) -> dict:
        payload = dict(payload)
        _management_payload(payload, ("expected_revision", "plan_id", "plan_digest"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "component_job.retire.apply", operation_id, payload,
                payload["plan_digest"],
            )
            replayed = await self.control._replay(conn, claims)
            if self.control.is_replay(replayed):
                return replayed
            await self.store.content.epoch_in(conn)
            await self._clock_in(conn)
            plan = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_job_retirement_plans WHERE bot_id=%s "
                    "AND plan_id=%s FOR SHARE",
                    (self.store.bot_id, payload["plan_id"]),
                )
            ).fetchone()
            if (
                plan is None
                or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id"))
                or plan["plan_digest"] != payload["plan_digest"]
                or plan["expected_revision"] != payload["expected_revision"]
            ):
                raise ForestError("PLAN_CONFLICT", "Retirement plan does not match", 409)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the retirement again", 409)
            head = await self._head_in(
                conn, plan["component_id"], plan["handler_id"], lock=True
            )
            if head["management_revision"] != plan["expected_revision"]:
                raise ForestError(
                    "COMPONENT_JOB_HANDLER_REVISION_CONFLICT",
                    "Handler changed after preview",
                    409,
                )
            if not head["active"]:
                raise ForestError(
                    "COMPONENT_JOB_HANDLER_RETIRED", "Handler is already retired", 409
                )
            impact = await self._impact_in(conn, head["component_id"], head["handler_id"])
            if digest_json(impact) != plan["impact_digest"]:
                raise ForestError(
                    "COMPONENT_JOB_RETIREMENT_PLAN_STALE",
                    "Job state changed; preview retirement again",
                    409,
                )
            if impact["inflight"]:
                raise ForestError(
                    "COMPONENT_JOB_HANDLER_BUSY", "Wait for in-flight work to settle", 409
                )
            revision = head["management_revision"] + 1
            await conn.execute(
                "UPDATE public.bot_component_job_heads SET active=false,management_revision=%s,"
                "retired_at=clock_timestamp(),retirement_operation_id=%s,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND component_id=%s AND handler_id=%s",
                (
                    revision, operation_id, self.store.bot_id,
                    head["component_id"], head["handler_id"],
                ),
            )
            retired = await (
                await conn.execute(
                    "UPDATE public.bot_component_jobs SET status='RETIRED',"
                    "last_error_code='JOB_HANDLER_RETIRED',retired_at=clock_timestamp(),"
                    "claim_token=NULL,claim_host_incarnation=NULL,claim_owner_epoch=NULL,"
                    "claim_until=NULL,claim_new_attempt=NULL WHERE bot_id=%s AND component_id=%s "
                    "AND handler_id=%s AND status IN ('PENDING','RETRY_WAIT') RETURNING job_id",
                    (self.store.bot_id, head["component_id"], head["handler_id"]),
                )
            ).fetchall()
            await conn.execute(
                "INSERT INTO public.bot_component_job_handler_retirements("
                "bot_id,component_id,handler_id,management_revision,operation_id,plan_id,"
                "subject_id,issuer_id,retired_job_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, head["component_id"], head["handler_id"], revision,
                    operation_id, plan["plan_id"], claims["subject_id"], claims["issuer_id"],
                    len(retired),
                ),
            )
            from forest.storage.control_receipts import ControlReceiptStore

            source = await ControlReceiptStore(self.store).operation_in(conn, plan["operation_id"])
            if (
                source["action"] != "component_job.retire.plan"
                or not isinstance(source.get("result"), dict)
                or source["result"].get("plan_id") != plan["plan_id"]
                or source["result"].get("plan_digest") != plan["plan_digest"]
            ):
                raise ForestError(
                    "CONTROL_PLAN_SOURCE_CONFLICT", "Retirement plan source differs", 409
                )
            result = {
                "operation_id": operation_id,
                "component_id": head["component_id"],
                "handler_id": head["handler_id"],
                "active": False,
                "management_revision": revision,
                "retired_job_count": len(retired),
            }
            await self.control._record(
                conn, claims, result, parents=(source["result_payload_id"],)
            )
            return result

    async def jobs(self) -> list[dict]:
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT job_id,job_seq,component_id,handler_id,operation_id,status,"
                    "attempt_count,dispatch_count,max_attempts,first_available_at,available_at,"
                    "last_error_code,acked_at,created_at FROM public.bot_component_jobs "
                    "WHERE bot_id=%s ORDER BY job_seq",
                    (self.store.bot_id,),
                )
            ).fetchall()
        return [dict(row) for row in rows]

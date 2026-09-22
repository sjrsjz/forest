"""ComponentJob schedule, claim, and settlement commands."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.payload_authority import PayloadSource
from forest.storage.routing_graph_types import (
    ComponentJobClaim,
    ComponentJobRoute,
    _bounded,
    _error_code,
    _identifier,
    _validated_unique,
)


class ComponentJobCommands:
    def __init__(
        self,
        store,
        *,
        payloads,
        generation_member_verifier,
        namespace: str | None = None,
    ) -> None:
        self.store = store
        self.payloads = payloads
        self.generation_member_verifier = generation_member_verifier
        self.namespace = namespace or module_schema_namespace("forest.routing.graph")

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    async def schedule_component_job(
        self,
        *,
        owner_component_id: str,
        owner_generation_id: str,
        handler_id: str,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
        payload: dict,
        delay_seconds: int,
    ) -> dict:
        _identifier(owner_component_id, "owner_component_id")
        _bounded(owner_generation_id, "owner_generation_id", 512)
        _identifier(handler_id, "handler_id")
        _bounded(operation_id, "operation_id", 256)
        _bounded(causal_root_kind, "causal_root_kind", 128)
        _bounded(causal_root_id, "causal_root_id", 256)
        if not isinstance(payload, dict) or type(delay_seconds) is not int or not (
            0 <= delay_seconds <= 86_400
        ):
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid ComponentJob input", 422)
        input_digest = digest_json(
            {
                "owner_component_id": owner_component_id,
                "handler_id": handler_id,
                "causal_root_kind": causal_root_kind,
                "causal_root_id": causal_root_id,
                "payload": payload,
                "delay_seconds": delay_seconds,
            }
        )
        job_id = str(
            uuid5(
                NAMESPACE_URL,
                f"forest:{self.store.bot_id}:component-job:"
                f"{owner_component_id}:{operation_id}",
            )
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            accepted = await self.generation_member_verifier(
                conn,
                component_id=owner_component_id,
                generation_id=owner_generation_id,
            )
            graph_id = accepted.graph_id
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (
                    f"forest.routing.graph/job/{self.store.bot_id}/"
                    f"{owner_component_id}/{operation_id}",
                ),
            )
            existing = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT job_id,input_digest,state FROM {} WHERE bot_id=%s "
                        "AND owner_component_id=%s AND operation_id=%s"
                    ).format(self._table("component_jobs")),
                    (self.store.bot_id, owner_component_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError(
                        "COMPONENT_JOB_OPERATION_CONFLICT",
                        "ComponentJob operation input differs",
                        409,
                    )
                return {
                    "job_id": existing["job_id"],
                    "state": existing["state"],
                    "replayed": True,
                }
            route_row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND graph_id=%s "
                        "AND owner_component_id=%s AND handler_id=%s"
                    ).format(self._table("component_job_routes")),
                    (self.store.bot_id, graph_id, owner_component_id, handler_id),
                )
            ).fetchone()
            if (
                route_row is None
                or route_row["owner_generation_id"] != owner_generation_id
            ):
                raise ForestError(
                    "COMPONENT_JOB_ROUTE_UNAVAILABLE",
                    "ComponentJob handler is absent from the frozen routing directory",
                    409,
                )
            route = ComponentJobRoute(
                **{
                    field: route_row[field]
                    for field in ComponentJobRoute.__dataclass_fields__
                }
            )
            deletion_epoch = await self.payloads.epoch_in(conn)
            saved = await self.payloads.put_json_in(
                conn,
                owner_component_id="routing.graph",
                owner_kind="component.job-input",
                owner_id=job_id,
                value=payload,
                sources=(
                    PayloadSource(
                        owner_component_id,
                        "component.call",
                        operation_id,
                        1,
                    ),
                ),
                expected_deletion_epoch=deletion_epoch,
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,job_id,operation_id,input_digest,"
                    "owner_component_id,scheduled_by_generation_id,graph_id,handler_id,"
                    "input_schema_digest,input_payload_id,input_payload_digest,"
                    "causal_root_kind,causal_root_id,max_attempts,timeout_seconds,"
                    "max_concurrency,handler_service_id,handler_service_version,available_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "clock_timestamp()+make_interval(secs => %s))"
                ).format(self._table("component_jobs")),
                (
                    self.store.bot_id,
                    job_id,
                    operation_id,
                    input_digest,
                    owner_component_id,
                    owner_generation_id,
                    graph_id,
                    handler_id,
                    route.input_schema_digest,
                    saved.payload_id,
                    saved.content_digest,
                    causal_root_kind,
                    causal_root_id,
                    route.max_attempts,
                    route.timeout_seconds,
                    route.max_concurrency,
                    route.handler_service_id,
                    route.handler_service_version,
                    delay_seconds,
                ),
            )
            return {"job_id": job_id, "state": "WAITING", "replayed": False}

    async def claim_component_job(
        self,
        *,
        routing_generation_id: str,
        routes: tuple[ComponentJobRoute, ...],
    ) -> ComponentJobClaim | None:
        _bounded(routing_generation_id, "routing_generation_id", 512)
        routes = _validated_unique(
            routes,
            lambda item: (item.owner_component_id, item.handler_id),
            "ComponentJob routes are ambiguous",
        )
        available = {
            (item.owner_component_id, item.handler_id): item for item in routes
        }
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generation_member_verifier(
                conn,
                component_id="routing.graph",
                generation_id=routing_generation_id,
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} a SET state='UNKNOWN',error_code='JOB_LEASE_EXPIRED',"
                    "completed_at=clock_timestamp() FROM {} j WHERE a.bot_id=j.bot_id "
                    "AND a.attempt_id=j.active_attempt_id AND j.bot_id=%s "
                    "AND j.state='CLAIMED' AND j.lease_until<=clock_timestamp() "
                    "AND a.state='DISPATCHING'"
                ).format(
                    self._table("component_job_attempts"),
                    self._table("component_jobs"),
                ),
                (self.store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='UNKNOWN',revision=revision+1,claim_token=NULL,"
                    "routing_generation_id=NULL,target_generation_id=NULL,"
                    "active_attempt_id=NULL,lease_until=NULL,error_code='JOB_LEASE_EXPIRED',"
                    "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND state='CLAIMED' AND lease_until<=clock_timestamp()"
                ).format(self._table("component_jobs")),
                (self.store.bot_id,),
            )
            candidates = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} j WHERE j.bot_id=%s "
                        "AND j.state IN ('WAITING','RETRY_WAIT') "
                        "AND j.available_at<=clock_timestamp() AND NOT EXISTS ("
                        "SELECT 1 FROM {} active WHERE active.bot_id=j.bot_id "
                        "AND active.owner_component_id=j.owner_component_id "
                        "AND active.handler_id=j.handler_id AND active.state='CLAIMED' "
                        "GROUP BY active.bot_id HAVING count(*)>=j.max_concurrency) "
                        "ORDER BY j.available_at,j.updated_at,j.job_id "
                        "FOR UPDATE OF j SKIP LOCKED LIMIT 32"
                    ).format(
                        self._table("component_jobs"),
                        self._table("component_jobs"),
                    ),
                    (self.store.bot_id,),
                )
            ).fetchall()
            for row in candidates:
                route = available.get((row["owner_component_id"], row["handler_id"]))
                if route is None or any(
                    (
                        route.input_schema_digest != row["input_schema_digest"],
                        route.handler_service_id != row["handler_service_id"],
                        route.handler_service_version
                        != row["handler_service_version"],
                    )
                ):
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='FAILED',revision=revision+1,"
                            "error_code='COMPONENT_JOB_ROUTE_UNAVAILABLE',"
                            "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND job_id=%s"
                        ).format(self._table("component_jobs")),
                        (self.store.bot_id, row["job_id"]),
                    )
                    continue
                attempt_no = row["attempt_count"] + 1
                claim_token = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"forest:{self.store.bot_id}:component-job-claim:"
                        f"{row['job_id']}:{row['revision'] + 1}",
                    )
                )
                attempt_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"forest:{self.store.bot_id}:component-job-attempt:"
                        f"{row['job_id']}:{attempt_no}",
                    )
                )
                lease_seconds = max(15, int(route.timeout_seconds) + 15)
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='CLAIMED',revision=revision+1,"
                        "attempt_count=attempt_count+1,claim_token=%s,"
                        "routing_generation_id=%s,target_generation_id=%s,"
                        "active_attempt_id=%s,lease_until=clock_timestamp()+"
                        "make_interval(secs => %s),error_code=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s"
                    ).format(self._table("component_jobs")),
                    (
                        claim_token,
                        routing_generation_id,
                        route.owner_generation_id,
                        attempt_id,
                        lease_seconds,
                        self.store.bot_id,
                        row["job_id"],
                    ),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,attempt_id,job_id,attempt_no,claim_token,"
                        "routing_generation_id,target_generation_id,operation_id,state) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'DISPATCHING')"
                    ).format(self._table("component_job_attempts")),
                    (
                        self.store.bot_id,
                        attempt_id,
                        row["job_id"],
                        attempt_no,
                        claim_token,
                        routing_generation_id,
                        route.owner_generation_id,
                        attempt_id,
                    ),
                )
                return ComponentJobClaim(
                    job_id=row["job_id"],
                    owner_component_id=row["owner_component_id"],
                    scheduled_by_generation_id=row["scheduled_by_generation_id"],
                    handler_id=row["handler_id"],
                    input_payload_id=row["input_payload_id"],
                    input_payload_digest=row["input_payload_digest"],
                    causal_root_kind=row["causal_root_kind"],
                    causal_root_id=row["causal_root_id"],
                    attempt_no=attempt_no,
                    attempt_id=attempt_id,
                    claim_token=claim_token,
                    routing_generation_id=routing_generation_id,
                    target_generation_id=route.owner_generation_id,
                    handler_service_id=route.handler_service_id,
                    handler_service_version=route.handler_service_version,
                    max_attempts=row["max_attempts"],
                    timeout_seconds=row["timeout_seconds"],
                )
            return None

    async def succeed_component_job(
        self,
        claim: ComponentJobClaim,
        *,
        result: dict,
    ) -> None:
        if type(claim) is not ComponentJobClaim or not isinstance(result, dict):
            raise ForestError("ROUTING_RESULT_INVALID", "Invalid ComponentJob result", 422)
        await self._settle_component_job(
            claim,
            result=result,
            error_code=None,
            retryable=False,
            unknown=False,
            retry_delay_seconds=0,
        )

    async def fail_component_job(
        self,
        claim: ComponentJobClaim,
        *,
        error_code: str,
        retryable: bool,
        unknown: bool = False,
        retry_delay_seconds: int = 1,
    ) -> None:
        if (
            type(claim) is not ComponentJobClaim
            or _error_code(error_code) != error_code
            or type(retryable) is not bool
            or type(unknown) is not bool
            or type(retry_delay_seconds) is not int
            or not 0 <= retry_delay_seconds <= 86_400
        ):
            raise ForestError("ROUTING_RESULT_INVALID", "Invalid ComponentJob result", 422)
        await self._settle_component_job(
            claim,
            result=None,
            error_code=error_code,
            retryable=retryable,
            unknown=unknown,
            retry_delay_seconds=retry_delay_seconds,
        )

    async def _settle_component_job(
        self,
        claim: ComponentJobClaim,
        *,
        result: dict | None,
        error_code: str | None,
        retryable: bool,
        unknown: bool,
        retry_delay_seconds: int,
    ) -> None:
        succeeded = result is not None
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generation_member_verifier(
                conn,
                component_id="routing.graph",
                generation_id=claim.routing_generation_id,
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND job_id=%s "
                        "AND state='CLAIMED' AND claim_token=%s AND routing_generation_id=%s "
                        "AND active_attempt_id=%s AND lease_until>clock_timestamp() FOR UPDATE"
                    ).format(self._table("component_jobs")),
                    (
                        self.store.bot_id,
                        claim.job_id,
                        claim.claim_token,
                        claim.routing_generation_id,
                        claim.attempt_id,
                    ),
                )
            ).fetchone()
            if row is None:
                raise ForestError("STALE_ROUTING_CLAIM", "ComponentJob claim is stale", 409)
            can_retry = (
                error_code is not None
                and retryable
                and not unknown
                and row["attempt_count"] < row["max_attempts"]
            )
            result_payload = None
            if succeeded:
                deletion_epoch = await self.payloads.epoch_in(conn)
                result_payload = await self.payloads.put_json_in(
                    conn,
                    owner_component_id="routing.graph",
                    owner_kind="component.job-result",
                    owner_id=claim.job_id,
                    value=result,
                    parents=(row["input_payload_id"],),
                    expected_deletion_epoch=deletion_epoch,
                )
            attempt_state = "UNKNOWN" if unknown else "FAILED" if error_code else "SUCCEEDED"
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,error_code=%s,completed_at=clock_timestamp() "
                    "WHERE bot_id=%s AND attempt_id=%s AND state='DISPATCHING'"
                ).format(self._table("component_job_attempts")),
                (
                    attempt_state,
                    error_code,
                    self.store.bot_id,
                    claim.attempt_id,
                ),
            )
            state = (
                "SUCCEEDED"
                if succeeded
                else "RETRY_WAIT"
                if can_retry
                else "UNKNOWN"
                if unknown
                else "FAILED"
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,revision=revision+1,claim_token=NULL,"
                    "routing_generation_id=NULL,target_generation_id=NULL,"
                    "active_attempt_id=NULL,lease_until=NULL,result_payload_id=%s,"
                    "result_payload_digest=%s,error_code=%s,available_at=CASE WHEN %s "
                    "THEN clock_timestamp()+make_interval(secs => %s) ELSE available_at END,"
                    "completed_at=CASE WHEN %s THEN NULL ELSE clock_timestamp() END,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND job_id=%s"
                ).format(self._table("component_jobs")),
                (
                    state,
                    result_payload.payload_id if result_payload is not None else None,
                    result_payload.content_digest if result_payload is not None else None,
                    None if can_retry else error_code,
                    can_retry,
                    retry_delay_seconds,
                    can_retry,
                    self.store.bot_id,
                    claim.job_id,
                ),
            )

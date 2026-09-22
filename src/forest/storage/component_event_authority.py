"""Event publication, delivery claim, and settlement commands."""

from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_URL, uuid5

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.payload_authority import PayloadSource
from forest.storage.routing_graph_types import (
    _CONTRACT,
    EventDeliveryClaim,
    EventSubscriptionRoute,
    _bounded,
    _error_code,
    _identifier,
    _validated_unique,
)


class ComponentEventCommands:
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

    async def publish_event(
        self,
        *,
        publisher_component_id: str,
        publisher_generation_id: str,
        topic: str,
        contract: str,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
        payload: Any,
    ) -> dict:
        _identifier(publisher_component_id, "publisher_component_id")
        _bounded(publisher_generation_id, "publisher_generation_id", 512)
        _identifier(topic, "topic")
        if _CONTRACT.fullmatch(contract) is None:
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid event contract", 422)
        _bounded(operation_id, "operation_id", 256)
        _bounded(causal_root_kind, "causal_root_kind", 128)
        _bounded(causal_root_id, "causal_root_id", 256)
        input_digest = digest_json(
            {
                "publisher_component_id": publisher_component_id,
                "topic": topic,
                "contract": contract,
                "causal_root_kind": causal_root_kind,
                "causal_root_id": causal_root_id,
                "payload": payload,
            }
        )
        event_id = str(
            uuid5(
                NAMESPACE_URL,
                f"forest:{self.store.bot_id}:event:{publisher_component_id}:{operation_id}",
            )
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            accepted = await self.generation_member_verifier(
                conn,
                component_id=publisher_component_id,
                generation_id=publisher_generation_id,
            )
            graph_id = accepted.graph_id
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (
                    f"forest.routing.graph/event/{self.store.bot_id}/"
                    f"{publisher_component_id}/{operation_id}",
                ),
            )
            existing = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT event_id,input_digest FROM {} WHERE bot_id=%s "
                        "AND publisher_component_id=%s AND operation_id=%s"
                    ).format(self._table("component_events")),
                    (self.store.bot_id, publisher_component_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError(
                        "EVENT_OPERATION_CONFLICT", "Event operation input differs", 409
                    )
                return {"event_id": existing["event_id"], "replayed": True}
            stored_publication = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT publisher_generation_id,event_schema_digest FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s AND publisher_component_id=%s "
                        "AND topic=%s AND contract=%s"
                    ).format(self._table("event_publication_routes")),
                    (
                        self.store.bot_id,
                        graph_id,
                        publisher_component_id,
                        topic,
                        contract,
                    ),
                )
            ).fetchone()
            if (
                stored_publication is None
                or stored_publication["publisher_generation_id"]
                != publisher_generation_id
            ):
                raise ForestError(
                    "EVENT_ROUTE_UNAVAILABLE",
                    "Event publication is absent from the frozen routing directory",
                    409,
                )
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT subscriber_id,contract_major,topic,contract,"
                        "event_schema_digest,owner_component_id,owner_generation_id,"
                        "handler_service_id,handler_service_version,max_attempts FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s AND topic=%s AND contract=%s "
                        "ORDER BY subscriber_id,contract_major"
                    ).format(self._table("event_subscription_routes")),
                    (self.store.bot_id, graph_id, topic, contract),
                )
            ).fetchall()
            subscriptions = tuple(
                EventSubscriptionRoute(**dict(row)) for row in rows
            )
            deletion_epoch = await self.payloads.epoch_in(conn)
            saved = await self.payloads.put_json_in(
                conn,
                owner_component_id="routing.graph",
                owner_kind="component.event",
                owner_id=event_id,
                value=payload,
                sources=(
                    PayloadSource(
                        publisher_component_id,
                        "component.call",
                        operation_id,
                        1,
                    ),
                ),
                expected_deletion_epoch=deletion_epoch,
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,event_id,operation_id,input_digest,"
                    "publisher_component_id,publisher_generation_id,graph_id,topic,contract,"
                    "event_schema_digest,payload_id,payload_digest,causal_root_kind,"
                    "causal_root_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("component_events")),
                (
                    self.store.bot_id,
                    event_id,
                    operation_id,
                    input_digest,
                    publisher_component_id,
                    publisher_generation_id,
                    graph_id,
                    topic,
                    contract,
                    stored_publication["event_schema_digest"],
                    saved.payload_id,
                    saved.content_digest,
                    causal_root_kind,
                    causal_root_id,
                ),
            )
            for route in subscriptions:
                delivery_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"forest:{self.store.bot_id}:event-delivery:{event_id}:"
                        f"{route.subscriber_id}:{route.contract_major}",
                    )
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,delivery_id,event_id,subscriber_id,"
                        "contract_major,topic,contract,event_schema_digest,owner_component_id,"
                        "handler_service_id,handler_service_version,max_attempts) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("event_deliveries")),
                    (
                        self.store.bot_id,
                        delivery_id,
                        event_id,
                        route.subscriber_id,
                        route.contract_major,
                        route.topic,
                        route.contract,
                        route.event_schema_digest,
                        route.owner_component_id,
                        route.handler_service_id,
                        route.handler_service_version,
                        route.max_attempts,
                    ),
                )
            return {
                "event_id": event_id,
                "payload_digest": saved.content_digest,
                "delivery_count": len(subscriptions),
                "replayed": False,
            }

    async def claim_event_delivery(
        self,
        *,
        routing_generation_id: str,
        routes: tuple[EventSubscriptionRoute, ...],
        lease_seconds: int = 45,
    ) -> EventDeliveryClaim | None:
        _bounded(routing_generation_id, "routing_generation_id", 512)
        if type(lease_seconds) is not int or not 3 <= lease_seconds <= 300:
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid delivery lease", 422)
        routes = _validated_unique(
            routes,
            lambda item: (item.subscriber_id, item.contract_major),
            "Event subscriptions are ambiguous",
        )
        available = {
            (item.subscriber_id, item.contract_major): item for item in routes
        }
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generation_member_verifier(
                conn,
                component_id="routing.graph",
                generation_id=routing_generation_id,
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} a SET state='UNKNOWN',error_code='DELIVERY_LEASE_EXPIRED',"
                    "completed_at=clock_timestamp() FROM {} d WHERE a.bot_id=d.bot_id "
                    "AND a.attempt_id=d.active_attempt_id AND d.bot_id=%s "
                    "AND d.state='CLAIMED' AND d.lease_until<=clock_timestamp() "
                    "AND a.state='DISPATCHING'"
                ).format(
                    self._table("event_delivery_attempts"),
                    self._table("event_deliveries"),
                ),
                (self.store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='UNKNOWN',revision=revision+1,claim_token=NULL,"
                    "routing_generation_id=NULL,target_generation_id=NULL,"
                    "active_attempt_id=NULL,lease_until=NULL,error_code='DELIVERY_LEASE_EXPIRED',"
                    "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND state='CLAIMED' AND lease_until<=clock_timestamp()"
                ).format(self._table("event_deliveries")),
                (self.store.bot_id,),
            )
            candidates = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT d.*,e.publisher_component_id,e.publisher_generation_id,"
                        "e.payload_id,e.payload_digest,e.causal_root_kind,e.causal_root_id "
                        "FROM {} d JOIN {} e ON e.bot_id=d.bot_id AND e.event_id=d.event_id "
                        "WHERE d.bot_id=%s AND d.state IN ('QUEUED','RETRY_WAIT') "
                        "AND d.available_at<=clock_timestamp() AND NOT EXISTS ("
                        "SELECT 1 FROM {} active WHERE active.bot_id=d.bot_id "
                        "AND active.subscriber_id=d.subscriber_id AND active.state='CLAIMED') "
                        "ORDER BY d.available_at,d.updated_at,d.delivery_id "
                        "FOR UPDATE OF d SKIP LOCKED LIMIT 32"
                    ).format(
                        self._table("event_deliveries"),
                        self._table("component_events"),
                        self._table("event_deliveries"),
                    ),
                    (self.store.bot_id,),
                )
            ).fetchall()
            for row in candidates:
                route = available.get((row["subscriber_id"], row["contract_major"]))
                if route is None or any(
                    (
                        route.topic != row["topic"],
                        route.contract != row["contract"],
                        route.event_schema_digest != row["event_schema_digest"],
                        route.owner_component_id != row["owner_component_id"],
                        route.handler_service_id != row["handler_service_id"],
                        route.handler_service_version
                        != row["handler_service_version"],
                    )
                ):
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='DEAD_LETTER',revision=revision+1,"
                            "error_code='EVENT_ROUTE_UNAVAILABLE',"
                            "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND delivery_id=%s"
                        ).format(self._table("event_deliveries")),
                        (self.store.bot_id, row["delivery_id"]),
                    )
                    continue
                attempt_no = row["attempt_count"] + 1
                claim_token = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"forest:{self.store.bot_id}:event-claim:"
                        f"{row['delivery_id']}:{row['revision'] + 1}",
                    )
                )
                attempt_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"forest:{self.store.bot_id}:event-attempt:"
                        f"{row['delivery_id']}:{attempt_no}",
                    )
                )
                saved = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='CLAIMED',revision=revision+1,"
                            "attempt_count=attempt_count+1,claim_token=%s,"
                            "routing_generation_id=%s,target_generation_id=%s,"
                            "active_attempt_id=%s,lease_until=clock_timestamp()+"
                            "make_interval(secs => %s),error_code=NULL,"
                            "updated_at=clock_timestamp() WHERE bot_id=%s AND delivery_id=%s "
                            "RETURNING *"
                        ).format(self._table("event_deliveries")),
                        (
                            claim_token,
                            routing_generation_id,
                            route.owner_generation_id,
                            attempt_id,
                            lease_seconds,
                            self.store.bot_id,
                            row["delivery_id"],
                        ),
                    )
                ).fetchone()
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,attempt_id,delivery_id,attempt_no,"
                        "claim_token,routing_generation_id,target_generation_id,operation_id,state) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'DISPATCHING')"
                    ).format(self._table("event_delivery_attempts")),
                    (
                        self.store.bot_id,
                        attempt_id,
                        row["delivery_id"],
                        attempt_no,
                        claim_token,
                        routing_generation_id,
                        route.owner_generation_id,
                        attempt_id,
                    ),
                )
                return EventDeliveryClaim(
                    delivery_id=row["delivery_id"],
                    event_id=row["event_id"],
                    subscriber_id=row["subscriber_id"],
                    topic=row["topic"],
                    contract=row["contract"],
                    publisher_component_id=row["publisher_component_id"],
                    publisher_generation_id=row["publisher_generation_id"],
                    payload_id=row["payload_id"],
                    payload_digest=row["payload_digest"],
                    causal_root_kind=row["causal_root_kind"],
                    causal_root_id=row["causal_root_id"],
                    attempt_no=attempt_no,
                    attempt_id=attempt_id,
                    claim_token=claim_token,
                    routing_generation_id=routing_generation_id,
                    target_generation_id=route.owner_generation_id,
                    handler_service_id=route.handler_service_id,
                    handler_service_version=route.handler_service_version,
                    max_attempts=saved["max_attempts"],
                )
            return None

    async def succeed_event_delivery(self, claim: EventDeliveryClaim) -> None:
        if type(claim) is not EventDeliveryClaim:
            raise ForestError("ROUTING_RESULT_INVALID", "Invalid delivery claim", 422)
        await self._settle_event_delivery(
            claim,
            error_code=None,
            retryable=False,
            unknown=False,
            retry_delay_seconds=0,
        )

    async def fail_event_delivery(
        self,
        claim: EventDeliveryClaim,
        *,
        error_code: str,
        retryable: bool,
        unknown: bool = False,
        retry_delay_seconds: int = 1,
    ) -> None:
        if (
            type(claim) is not EventDeliveryClaim
            or _error_code(error_code) != error_code
            or type(retryable) is not bool
            or type(unknown) is not bool
            or type(retry_delay_seconds) is not int
            or not 0 <= retry_delay_seconds <= 86_400
        ):
            raise ForestError("ROUTING_RESULT_INVALID", "Invalid delivery result", 422)
        await self._settle_event_delivery(
            claim,
            error_code=error_code,
            retryable=retryable,
            unknown=unknown,
            retry_delay_seconds=retry_delay_seconds,
        )

    async def _settle_event_delivery(
        self,
        claim: EventDeliveryClaim,
        *,
        error_code: str | None,
        retryable: bool,
        unknown: bool,
        retry_delay_seconds: int,
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generation_member_verifier(
                conn,
                component_id="routing.graph",
                generation_id=claim.routing_generation_id,
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND delivery_id=%s "
                        "AND state='CLAIMED' AND claim_token=%s AND routing_generation_id=%s "
                        "AND active_attempt_id=%s AND lease_until>clock_timestamp() FOR UPDATE"
                    ).format(self._table("event_deliveries")),
                    (
                        self.store.bot_id,
                        claim.delivery_id,
                        claim.claim_token,
                        claim.routing_generation_id,
                        claim.attempt_id,
                    ),
                )
            ).fetchone()
            if row is None:
                raise ForestError("STALE_ROUTING_CLAIM", "Delivery claim is stale", 409)
            can_retry = (
                error_code is not None
                and retryable
                and not unknown
                and row["attempt_count"] < row["max_attempts"]
            )
            attempt_state = "UNKNOWN" if unknown else "FAILED" if error_code else "SUCCEEDED"
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,error_code=%s,completed_at=clock_timestamp() "
                    "WHERE bot_id=%s AND attempt_id=%s AND state='DISPATCHING'"
                ).format(self._table("event_delivery_attempts")),
                (
                    attempt_state,
                    error_code,
                    self.store.bot_id,
                    claim.attempt_id,
                ),
            )
            state = (
                "RETRY_WAIT"
                if can_retry
                else "UNKNOWN"
                if unknown
                else "DEAD_LETTER"
                if error_code is not None
                else "SUCCEEDED"
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,revision=revision+1,claim_token=NULL,"
                    "routing_generation_id=NULL,target_generation_id=NULL,"
                    "active_attempt_id=NULL,lease_until=NULL,error_code=%s,"
                    "available_at=CASE WHEN %s THEN clock_timestamp()+"
                    "make_interval(secs => %s) ELSE available_at END,"
                    "completed_at=CASE WHEN %s THEN NULL ELSE clock_timestamp() END,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND delivery_id=%s"
                ).format(self._table("event_deliveries")),
                (
                    state,
                    None if can_retry else error_code,
                    can_retry,
                    retry_delay_seconds,
                    can_retry,
                    self.store.bot_id,
                    claim.delivery_id,
                ),
            )


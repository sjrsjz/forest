"""Durable component events with stable subscription heads and leased delivery."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.bot import CallGrant, HostLease


def _contract(value: str) -> tuple[int, int]:
    try:
        major, minor = value.split(".", 1)
        result = int(major), int(minor)
    except (AttributeError, ValueError):
        raise ForestError("INVALID_EVENT_CONTRACT", "Event contract must be major.minor", 422) from None
    if result[0] < 1 or result[1] < 0 or value != f"{result[0]}.{result[1]}":
        raise ForestError("INVALID_EVENT_CONTRACT", "Event contract must be major.minor", 422)
    return result


_DELIVERY_STATUSES = frozenset(
    {"PENDING", "INFLIGHT", "RETRY_WAIT", "ACKED", "DEAD", "RETIRED"}
)


def _management_payload(payload, required, optional=()):
    if not isinstance(payload, dict) or set(payload) - set(required) - set(optional) \
            or not set(required) <= set(payload):
        raise ForestError("EVENT_MANAGEMENT_INVALID", "Event management fields differ", 422)
    for key in ("subscriber_id", "delivery_id", "topic", "plan_id", "plan_digest"):
        if key in payload and (
            not isinstance(payload[key], str) or not 1 <= len(payload[key]) <= 256
        ):
            raise ForestError(
                "EVENT_MANAGEMENT_INVALID", "Event management identifiers must be bounded text", 422
            )
    for key in ("contract_major", "expected_revision", "limit"):
        if key in payload and (type(payload[key]) is not int or payload[key] < 0):
            raise ForestError(
                "EVENT_MANAGEMENT_INVALID", "Event management revisions and limits are invalid", 422
            )
    if "contract_major" in payload and payload["contract_major"] < 1:
        raise ForestError("EVENT_MANAGEMENT_INVALID", "Contract major must be positive", 422)
    if "limit" in payload and not 1 <= payload["limit"] <= 200:
        raise ForestError("EVENT_MANAGEMENT_INVALID", "Event read limit must be 1 to 200", 422)
    if "status" in payload and payload["status"] not in _DELIVERY_STATUSES:
        raise ForestError("EVENT_MANAGEMENT_INVALID", "Unknown delivery status", 422)


class EventStore:
    def __init__(self, store, *, control=None):
        self.store = store
        if control is None:
            from forest.storage.control import BotControl

            control = BotControl(store)
        self.control = control

    async def _clock_in(self, conn) -> dict:
        await conn.execute(
            "INSERT INTO public.bot_event_clock(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_event_clock WHERE bot_id=%s FOR UPDATE",
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
                item["direction"],
                item["definition_id"],
                item["topic"],
                item["contract_major"],
                item["contract_minor"],
                item["schema_digest"],
                item.get("max_attempts"),
            )
            for item in definitions
        )
        existing = await (
            await conn.execute(
                "SELECT direction,definition_id,topic,contract_major,contract_minor,schema_digest,max_attempts "
                "FROM public.bot_generation_event_definitions WHERE bot_id=%s AND generation_id=%s "
                "ORDER BY direction,definition_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        if existing:
            actual = [
                (
                    row["direction"], row["definition_id"], row["topic"],
                    row["contract_major"], row["contract_minor"], row["schema_digest"],
                    row["max_attempts"],
                )
                for row in existing
            ]
            if actual != normalized:
                raise ForestError(
                    "GENERATION_CONFLICT", "Generation event declarations cannot be reused", 409
                )
            return
        if existing_generation and normalized:
            raise ForestError(
                "GENERATION_CONFLICT", "Generation event declarations cannot be added later", 409
            )
        for item in definitions:
            await conn.execute(
                "INSERT INTO public.bot_generation_event_definitions("
                "bot_id,generation_id,component_id,direction,definition_id,topic,contract_major,"
                "contract_minor,schema_digest,max_attempts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, generation_id, component_id, item["direction"],
                    item["definition_id"], item["topic"], item["contract_major"],
                    item["contract_minor"], item["schema_digest"], item.get("max_attempts"),
                ),
            )

    async def publish_generation_in(self, conn, generation_id: str, component_id: str) -> None:
        await self._clock_in(conn)
        definitions = await (
            await conn.execute(
                "SELECT * FROM public.bot_generation_event_definitions WHERE bot_id=%s "
                "AND generation_id=%s ORDER BY direction,definition_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        for definition in definitions:
            contract = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_contracts WHERE bot_id=%s AND topic=%s "
                    "AND contract_major=%s AND contract_minor=%s FOR UPDATE",
                    (
                        self.store.bot_id, definition["topic"], definition["contract_major"],
                        definition["contract_minor"],
                    ),
                )
            ).fetchone()
            if contract is not None and contract["schema_digest"] != definition["schema_digest"]:
                raise ForestError(
                    "EVENT_CONTRACT_SCHEMA_CONFLICT",
                    "An event contract version is already bound to another schema",
                    409,
                )
            if contract is None:
                await conn.execute(
                    "INSERT INTO public.bot_event_contracts("
                    "bot_id,topic,contract_major,contract_minor,schema_digest,first_generation_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        self.store.bot_id, definition["topic"], definition["contract_major"],
                        definition["contract_minor"], definition["schema_digest"], generation_id,
                    ),
                )
        candidates = [
            definition for definition in definitions if definition["direction"] == "SUBSCRIBE"
        ]
        existing = await (
            await conn.execute(
                "SELECT * FROM public.bot_event_subscription_heads WHERE bot_id=%s "
                "AND component_id=%s ORDER BY subscriber_id,contract_major FOR UPDATE",
                (self.store.bot_id, component_id),
            )
        ).fetchall()
        wanted = {(row["definition_id"], row["contract_major"]) for row in candidates}
        if any(
            row["active"]
            and (row["subscriber_id"], row["contract_major"]) not in wanted
            for row in existing
        ):
            raise ForestError(
                "SUBSCRIPTION_REMOVAL_REQUIRES_RETIREMENT",
                "Durable subscriptions require an explicit retirement operation",
                409,
            )
        for candidate in candidates:
            key = candidate["definition_id"], candidate["contract_major"]
            current = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_subscription_heads WHERE bot_id=%s "
                    "AND subscriber_id=%s AND contract_major=%s FOR UPDATE",
                    (self.store.bot_id, *key),
                )
            ).fetchone()
            if current is not None and any(
                current[field] != candidate[source]
                for field, source in (
                    ("component_id", "component_id"),
                    ("topic", "topic"),
                    ("contract_minor", "contract_minor"),
                    ("schema_digest", "schema_digest"),
                )
            ):
                raise ForestError(
                    "SUBSCRIPTION_IDENTITY_CONFLICT",
                    "Stable subscriber identity changed component, topic, or schema",
                    409,
                )
            if current is not None and not current["active"]:
                raise ForestError(
                    "SUBSCRIPTION_REACTIVATION_REQUIRES_MANAGEMENT",
                    "A retired subscriber identity cannot be restored by deployment",
                    409,
                )
            await conn.execute(
                "INSERT INTO public.bot_event_subscription_heads("
                "bot_id,subscriber_id,contract_major,component_id,generation_id,topic,"
                "contract_minor,schema_digest,max_attempts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (bot_id,subscriber_id,contract_major) DO UPDATE SET "
                "generation_id=EXCLUDED.generation_id,contract_minor=EXCLUDED.contract_minor,"
                "max_attempts=EXCLUDED.max_attempts,updated_at=clock_timestamp()",
                (
                    self.store.bot_id, candidate["definition_id"], candidate["contract_major"],
                    component_id, generation_id, candidate["topic"], candidate["contract_minor"],
                    candidate["schema_digest"], candidate["max_attempts"],
                ),
            )

    async def publish(
        self,
        grant: CallGrant,
        *,
        topic: str,
        contract: str,
        schema_digest: str,
        operation_id: str,
        input_digest: str,
        payload,
    ) -> dict:
        major, minor = _contract(contract)
        async with self.store.pool.connection() as conn, conn.transaction():
            call, host = await self.store._guard(conn, grant, pending=False)
            declaration = await (
                await conn.execute(
                    "SELECT * FROM public.bot_generation_event_definitions WHERE bot_id=%s "
                    "AND generation_id=%s AND direction='PUBLISH' AND definition_id=%s",
                    (self.store.bot_id, grant.generation_id, topic),
                )
            ).fetchone()
            if declaration is None or any(
                declaration[field] != expected
                for field, expected in (
                    ("contract_major", major), ("contract_minor", minor),
                    ("schema_digest", schema_digest),
                )
            ):
                raise ForestError(
                    "EVENT_DECLARATION_MISMATCH", "Event is not declared by this generation", 403
                )
            parents = set(await self.store.content.parents_in(conn, grant.generation_id))
            if call.get("input_payload_id"):
                parents.add(call["input_payload_id"])
            # Lifecycle publication already holds the payload clock before it
            # publishes subscription heads. Event publication follows the same
            # payload-clock -> event-clock order to avoid a cross-component cycle.
            epoch = await self.store.content.epoch_in(conn)
            clock = await self._clock_in(conn)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_events WHERE bot_id=%s "
                    "AND publisher_component_id=%s AND operation_id=%s",
                    (self.store.bot_id, grant.component_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Event operation already has different input", 409
                    )
                receipt = await self._receipt_in(conn, existing)
                await self.store._guard_time(conn, host, grant)
                return receipt
            event_seq = clock["next_sequence"] + 1
            await conn.execute(
                "UPDATE public.bot_event_clock SET next_sequence=%s WHERE bot_id=%s",
                (event_seq, self.store.bot_id),
            )
            event_id = str(uuid4())
            payload_ref = await self.store.content.bodies.put_in(
                conn,
                "component.event",
                "event:" + event_id,
                payload,
                parents=tuple(sorted(parents)),
                epoch=epoch,
            )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_component_events("
                    "bot_id,event_id,event_seq,topic,contract_major,contract_minor,schema_digest,"
                    "publisher_component_id,publisher_generation_id,publisher_call_id,operation_id,"
                    "input_digest,payload_id,payload_digest) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id, event_id, event_seq, topic, major, minor, schema_digest,
                        grant.component_id, grant.generation_id, grant.call_id, operation_id,
                        input_digest, payload_ref.payload_id, payload_ref.digest,
                    ),
                )
            ).fetchone()
            subscriptions = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_subscription_heads WHERE bot_id=%s "
                    "AND active=true AND topic=%s AND contract_major=%s "
                    "AND contract_minor=%s AND schema_digest=%s "
                    "ORDER BY subscriber_id",
                    (self.store.bot_id, topic, major, minor, schema_digest),
                )
            ).fetchall()
            for subscription in subscriptions:
                await conn.execute(
                    "INSERT INTO public.bot_event_deliveries("
                    "bot_id,delivery_id,event_id,event_seq,subscriber_id,contract_major,"
                    "component_id,max_attempts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.store.bot_id, str(uuid4()), event_id, event_seq,
                        subscription["subscriber_id"], major,
                        subscription["component_id"], subscription["max_attempts"],
                    ),
                )
            await self.store._event(
                conn,
                "component.event.published",
                operation_id,
                {"event_id": event_id, "topic": topic, "deliveries": len(subscriptions)},
            )
            await self.store._guard_time(conn, host, grant)
            return await self._receipt_in(conn, row, delivery_count=len(subscriptions))

    async def _receipt_in(self, conn, event: dict, delivery_count: int | None = None) -> dict:
        if delivery_count is None:
            count = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_event_deliveries WHERE bot_id=%s AND event_id=%s",
                    (self.store.bot_id, event["event_id"]),
                )
            ).fetchone()
            delivery_count = count["n"]
        return {
            "event_id": event["event_id"],
            "topic": event["topic"],
            "contract": f'{event["contract_major"]}.{event["contract_minor"]}',
            "delivery_count": delivery_count,
        }

    async def claim(self, lease: HostLease, *, lease_seconds: float = 30) -> dict | None:
        if type(lease_seconds) not in {int, float} or not 3 <= lease_seconds <= 120:
            raise ForestError("INVALID_EVENT_LEASE", "Event claim lease must be 3 to 120 seconds", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            if not runtime["data_admission"]:
                return None
            # Generation publication takes the payload clock before subscription
            # heads. Claim follows the same order now that it locks both the head
            # and delivery, avoiding head -> payload inversion during hot reload.
            deletion_epoch = await self.store.content.epoch_in(conn)
            row = await (
                await conn.execute(
                    "SELECT d.*,e.topic,e.contract_minor,e.schema_digest,e.publisher_component_id,"
                    "e.publisher_generation_id,e.payload_id,e.payload_digest,"
                    "e.created_at AS event_created_at,"
                    "h.generation_id AS target_generation_id,r.route_epoch "
                    "FROM public.bot_event_deliveries d "
                    "JOIN public.bot_component_events e ON e.bot_id=d.bot_id AND e.event_id=d.event_id "
                    "JOIN public.bot_event_subscription_heads h ON h.bot_id=d.bot_id "
                    "AND h.subscriber_id=d.subscriber_id AND h.contract_major=d.contract_major "
                    "JOIN public.bot_component_routes r ON r.bot_id=d.bot_id AND r.component_id=d.component_id "
                    "JOIN public.bot_generations g ON g.bot_id=d.bot_id AND g.generation_id=h.generation_id "
                    "WHERE d.bot_id=%s AND ((d.status IN ('PENDING','RETRY_WAIT') "
                    "AND d.available_at<=clock_timestamp() AND d.attempt_count<d.max_attempts) "
                    "OR (d.status='INFLIGHT' AND d.claim_until<=clock_timestamp())) "
                    "AND h.active=true AND h.topic=e.topic AND h.contract_minor=e.contract_minor "
                    "AND h.schema_digest=e.schema_digest AND h.component_id=d.component_id "
                    "AND r.generation_id=h.generation_id AND r.admission=true AND g.status='ACTIVE' "
                    "AND g.host_incarnation=%s AND g.host_owner_epoch=%s "
                    "AND NOT EXISTS (SELECT 1 FROM public.bot_event_deliveries prior "
                    "WHERE prior.bot_id=d.bot_id AND prior.subscriber_id=d.subscriber_id "
                    "AND prior.contract_major=d.contract_major "
                    "AND prior.status NOT IN ('ACKED','DEAD','RETIRED') "
                    "AND prior.event_seq<d.event_seq) "
                    "ORDER BY d.event_seq,d.delivery_id LIMIT 1 FOR UPDATE OF d,h SKIP LOCKED",
                    (self.store.bot_id, lease.incarnation, lease.owner_epoch),
                )
            ).fetchone()
            if row is None:
                return None
            continuing = row["status"] == "INFLIGHT"
            attempt = row["attempt_count"] if continuing else row["attempt_count"] + 1
            dispatch = row["dispatch_count"] if continuing else row["dispatch_count"] + 1
            operation = row["call_operation_id"] if continuing else "event:" + hashlib.sha256(
                f'{row["delivery_id"]}:{row["replay_epoch"]}:{dispatch}'.encode()
            ).hexdigest()
            token = str(uuid4())
            claimed = await (
                await conn.execute(
                    "UPDATE public.bot_event_deliveries SET status='INFLIGHT',attempt_count=%s,"
                    "dispatch_count=%s,"
                    "claim_token=%s,claim_host_incarnation=%s,claim_owner_epoch=%s,"
                    "claim_until=clock_timestamp()+%s*interval '1 second',claim_new_attempt=%s,"
                    "last_error_code=NULL,"
                    "call_operation_id=%s "
                    "WHERE bot_id=%s AND delivery_id=%s RETURNING *",
                    (
                        attempt, dispatch, token, lease.incarnation, lease.owner_epoch, lease_seconds,
                        not continuing, operation, self.store.bot_id, row["delivery_id"],
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
                    "UPDATE public.bot_event_deliveries SET status='DEAD',claim_token=NULL,"
                    "claim_host_incarnation=NULL,claim_owner_epoch=NULL,claim_until=NULL,"
                    "claim_new_attempt=NULL,"
                    "last_error_code='KERNEL_CONTENT_REVOKED' WHERE bot_id=%s AND delivery_id=%s",
                    (self.store.bot_id, row["delivery_id"]),
                )
                await self.store._event(
                    conn,
                    "component.event.dead",
                    None,
                    {
                        "delivery_id": row["delivery_id"],
                        "event_id": row["event_id"],
                        "subscriber_id": row["subscriber_id"],
                        "error_code": "KERNEL_CONTENT_REVOKED",
                    },
                )
                return None
            await self.store._host(conn, lease, runtime)
            return {
                **claimed,
                "claim_token": token,
                "target_generation_id": row["target_generation_id"],
                "route_epoch": row["route_epoch"],
                "event": {
                    "event_id": row["event_id"],
                    "topic": row["topic"],
                    "contract": f'{row["contract_major"]}.{row["contract_minor"]}',
                    "published_at": row["event_created_at"].isoformat(),
                    "publisher_component_id": row["publisher_component_id"],
                    "publisher_generation_id": row["publisher_generation_id"],
                    "payload": payload,
                },
                "event_payload_id": row["payload_id"],
                "call_operation_id": operation,
            }

    async def defer(self, lease: HostLease, delivery: dict, *, delay_seconds: float = 0.1) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_deliveries WHERE bot_id=%s AND delivery_id=%s FOR UPDATE",
                    (self.store.bot_id, delivery["delivery_id"]),
                )
            ).fetchone()
            if (
                row is None
                or row["status"] != "INFLIGHT"
                or row["claim_token"] != delivery["claim_token"]
                or row["claim_host_incarnation"] != lease.incarnation
                or row["claim_owner_epoch"] != lease.owner_epoch
                or row["call_operation_id"] != delivery["call_operation_id"]
            ):
                raise ForestError("EVENT_DELIVERY_FENCED", "Event deferral lost its claim", 409)
            if not row["claim_new_attempt"]:
                await conn.execute(
                    "UPDATE public.bot_event_deliveries SET claim_until=clock_timestamp(),"
                    "last_error_code='DELIVERY_DEFERRED' WHERE bot_id=%s AND delivery_id=%s",
                    (self.store.bot_id, delivery["delivery_id"]),
                )
                await self.store._host(conn, lease)
                return
            call = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, row["call_operation_id"]),
                )
            ).fetchone()
            attempt = row["attempt_count"] - 1
            dispatch = row["dispatch_count"] - (1 if call is None else 0)
            await conn.execute(
                "UPDATE public.bot_event_deliveries SET status='RETRY_WAIT',attempt_count=%s,"
                "dispatch_count=%s,"
                "available_at=clock_timestamp()+%s*interval '1 second',claim_token=NULL,"
                "claim_host_incarnation=NULL,claim_owner_epoch=NULL,claim_until=NULL,"
                "claim_new_attempt=NULL "
                "WHERE bot_id=%s AND delivery_id=%s",
                (attempt, dispatch, delay_seconds, self.store.bot_id, delivery["delivery_id"]),
            )
            await self.store._host(conn, lease)

    async def abandon(self, lease: HostLease, delivery: dict) -> None:
        """Expire a graceful-shutdown claim without consuming another attempt."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_deliveries WHERE bot_id=%s AND delivery_id=%s FOR UPDATE",
                    (self.store.bot_id, delivery["delivery_id"]),
                )
            ).fetchone()
            if row is None or row["status"] in {"ACKED", "DEAD", "RETIRED"}:
                return
            if (
                row["status"] != "INFLIGHT"
                or row["claim_token"] != delivery["claim_token"]
                or row["claim_host_incarnation"] != lease.incarnation
                or row["claim_owner_epoch"] != lease.owner_epoch
                or row["call_operation_id"] != delivery["call_operation_id"]
            ):
                raise ForestError("EVENT_DELIVERY_FENCED", "Event abandonment lost its claim", 409)
            await conn.execute(
                "UPDATE public.bot_event_deliveries SET claim_until=clock_timestamp(),"
                "last_error_code='KERNEL_CLOSING' WHERE bot_id=%s AND delivery_id=%s",
                (self.store.bot_id, delivery["delivery_id"]),
            )
            await self.store._host(conn, lease)

    async def authorize_dispatch(self, lease: HostLease, delivery: dict) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            route = await self.store._route(conn, delivery["component_id"])
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            row = await (
                await conn.execute(
                    "SELECT d.*,h.generation_id,h.active FROM public.bot_event_deliveries d "
                    "JOIN public.bot_event_subscription_heads h ON h.bot_id=d.bot_id "
                    "AND h.subscriber_id=d.subscriber_id AND h.contract_major=d.contract_major "
                    "WHERE d.bot_id=%s AND d.delivery_id=%s FOR UPDATE OF d",
                    (self.store.bot_id, delivery["delivery_id"]),
                )
            ).fetchone()
            if (
                row is None
                or row["status"] != "INFLIGHT"
                or row["claim_token"] != delivery["claim_token"]
                or row["claim_host_incarnation"] != lease.incarnation
                or row["claim_owner_epoch"] != lease.owner_epoch
                or row["call_operation_id"] != delivery["call_operation_id"]
                or row["claim_until"] <= await self.store._now(conn)
                or not row["active"]
                or row["generation_id"] != delivery["target_generation_id"]
                or route["generation_id"] != row["generation_id"]
                or route["route_epoch"] != delivery["route_epoch"]
                or not route["admission"]
                or not runtime["data_admission"]
            ):
                raise ForestError("EVENT_DELIVERY_FENCED", "Event delivery lost execution authority", 409)
            await self.store._host(conn, lease, runtime)

    async def acknowledge(
        self, lease: HostLease, delivery: dict, *, result_payload_id: str
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_deliveries WHERE bot_id=%s AND delivery_id=%s FOR UPDATE",
                    (self.store.bot_id, delivery["delivery_id"]),
                )
            ).fetchone()
            if row is None or row["status"] == "ACKED":
                return
            if (
                row["status"] != "INFLIGHT"
                or row["claim_token"] != delivery["claim_token"]
                or row["claim_host_incarnation"] != lease.incarnation
                or row["claim_owner_epoch"] != lease.owner_epoch
                or row["call_operation_id"] != delivery["call_operation_id"]
            ):
                raise ForestError("EVENT_DELIVERY_FENCED", "Event acknowledgement lost its claim", 409)
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
                    "EVENT_RESULT_MISMATCH",
                    "Event acknowledgement has no matching completed call result",
                    409,
                )
            await conn.execute(
                "UPDATE public.bot_event_deliveries SET status='ACKED',"
                "result_payload_id=%s,acked_at=clock_timestamp(),claim_token=NULL,"
                "claim_host_incarnation=NULL,claim_owner_epoch=NULL,claim_until=NULL,"
                "claim_new_attempt=NULL "
                "WHERE bot_id=%s AND delivery_id=%s",
                (
                    result_payload_id, self.store.bot_id, delivery["delivery_id"],
                ),
            )
            await self.store._host(conn, lease)

    async def fail(
        self, lease: HostLease, delivery: dict, error_code: str, *, terminal: bool = False
    ) -> None:
        if not isinstance(error_code, str) or not 1 <= len(error_code) <= 128:
            error_code = "EVENT_HANDLER_FAILED"
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_deliveries WHERE bot_id=%s AND delivery_id=%s FOR UPDATE",
                    (self.store.bot_id, delivery["delivery_id"]),
                )
            ).fetchone()
            if row is None or row["status"] in {"ACKED", "DEAD", "RETIRED"}:
                return
            if (
                row["status"] != "INFLIGHT"
                or row["claim_token"] != delivery["claim_token"]
                or row["claim_host_incarnation"] != lease.incarnation
                or row["claim_owner_epoch"] != lease.owner_epoch
                or row["call_operation_id"] != delivery["call_operation_id"]
            ):
                raise ForestError("EVENT_DELIVERY_FENCED", "Event failure lost its claim", 409)
            dead = terminal or row["attempt_count"] >= row["max_attempts"]
            # Stable bounded jitter prevents every subscriber from retrying on the same edge.
            jitter = int(hashlib.sha256(
                f'{row["delivery_id"]}:{row["attempt_count"]}'.encode()
            ).hexdigest()[:2], 16) / 255
            delay = min(60, 2 ** max(0, row["attempt_count"] - 1)) + jitter
            await conn.execute(
                "UPDATE public.bot_event_deliveries SET status=%s,available_at=clock_timestamp()+%s*interval '1 second',"
                "last_error_code=%s,claim_token=NULL,claim_host_incarnation=NULL,"
                "claim_owner_epoch=NULL,claim_until=NULL,claim_new_attempt=NULL "
                "WHERE bot_id=%s AND delivery_id=%s",
                (
                    "DEAD" if dead else "RETRY_WAIT", delay, error_code,
                    self.store.bot_id, delivery["delivery_id"],
                ),
            )
            if dead:
                await self.store._event(
                    conn,
                    "component.event.dead",
                    delivery["call_operation_id"],
                    {
                        "delivery_id": delivery["delivery_id"],
                        "event_id": delivery["event_id"],
                        "subscriber_id": delivery["subscriber_id"],
                        "error_code": error_code,
                    },
                )
            await self.store._host(conn, lease)

    @staticmethod
    def _timestamp(value):
        return value.isoformat() if value is not None else None

    async def _head_in(self, conn, subscriber_id: str, contract_major: int, *, lock=False):
        suffix = " FOR UPDATE" if lock else ""
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_event_subscription_heads WHERE bot_id=%s "
                "AND subscriber_id=%s AND contract_major=%s" + suffix,
                (self.store.bot_id, subscriber_id, contract_major),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "EVENT_SUBSCRIPTION_NOT_FOUND", "Subscription does not belong to this Bot", 404
            )
        return row

    async def _impact_in(self, conn, subscriber_id: str, contract_major: int) -> dict:
        rows = await (
            await conn.execute(
                "SELECT status,count(*) AS count FROM public.bot_event_deliveries "
                "WHERE bot_id=%s AND subscriber_id=%s AND contract_major=%s GROUP BY status",
                (self.store.bot_id, subscriber_id, contract_major),
            )
        ).fetchall()
        counts = {status.lower(): 0 for status in _DELIVERY_STATUSES}
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
        _management_payload(payload, (), ("subscriber_id", "topic", "status", "limit"))
        limit = payload.get("limit", 100)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "event.read", operation_id, payload)
            heads = await (
                await conn.execute(
                    "SELECT subscriber_id,contract_major,component_id,generation_id,topic,"
                    "contract_minor,schema_digest,max_attempts,active,management_revision,"
                    "updated_at,retired_at FROM public.bot_event_subscription_heads "
                    "WHERE bot_id=%s AND (%s::text IS NULL OR subscriber_id=%s) "
                    "AND (%s::text IS NULL OR topic=%s) "
                    "ORDER BY subscriber_id,contract_major LIMIT %s",
                    (
                        self.store.bot_id, payload.get("subscriber_id"),
                        payload.get("subscriber_id"), payload.get("topic"),
                        payload.get("topic"), limit,
                    ),
                )
            ).fetchall()
            deliveries = await (
                await conn.execute(
                    "SELECT d.delivery_id,d.event_id,d.event_seq,d.subscriber_id,"
                    "d.contract_major,d.component_id,d.status,d.attempt_count,d.dispatch_count,"
                    "d.replay_epoch,d.management_revision,d.last_error_code,d.acked_at,"
                    "d.retired_at,d.created_at,e.topic,e.contract_minor,e.publisher_component_id,"
                    "e.created_at AS published_at,h.active AS subscription_active,"
                    "CASE WHEN p.state='ACTIVE' AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b "
                    "WHERE b.bot_id=p.bot_id AND b.payload_id=p.payload_id) "
                    "THEN 'ACTIVE' ELSE 'UNAVAILABLE' END AS payload_state "
                    "FROM public.bot_event_deliveries d "
                    "JOIN public.bot_component_events e ON e.bot_id=d.bot_id AND e.event_id=d.event_id "
                    "JOIN public.bot_event_subscription_heads h ON h.bot_id=d.bot_id "
                    "AND h.subscriber_id=d.subscriber_id AND h.contract_major=d.contract_major "
                    "JOIN public.bot_payload_headers p ON p.bot_id=e.bot_id AND p.payload_id=e.payload_id "
                    "WHERE d.bot_id=%s AND (%s::text IS NULL OR d.subscriber_id=%s) "
                    "AND (%s::text IS NULL OR e.topic=%s) "
                    "AND (%s::text IS NULL OR d.status=%s) "
                    "ORDER BY d.event_seq DESC,d.delivery_id DESC LIMIT %s",
                    (
                        self.store.bot_id, payload.get("subscriber_id"),
                        payload.get("subscriber_id"), payload.get("topic"),
                        payload.get("topic"), payload.get("status"),
                        payload.get("status"), limit,
                    ),
                )
            ).fetchall()
            await self.control._time(conn, claims)
            return {
                "subscriptions": [
                    {
                        **{key: row[key] for key in (
                            "subscriber_id", "contract_major", "component_id", "generation_id",
                            "topic", "contract_minor", "schema_digest", "max_attempts", "active",
                            "management_revision",
                        )},
                        "contract": f'{row["contract_major"]}.{row["contract_minor"]}',
                        "updated_at": self._timestamp(row["updated_at"]),
                        "retired_at": self._timestamp(row["retired_at"]),
                    }
                    for row in heads
                ],
                "deliveries": [
                    {
                        **{key: row[key] for key in (
                            "delivery_id", "event_id", "event_seq", "subscriber_id",
                            "contract_major", "component_id", "topic", "status", "attempt_count",
                            "dispatch_count", "replay_epoch", "management_revision",
                            "last_error_code", "publisher_component_id", "payload_state",
                        )},
                        "contract": f'{row["contract_major"]}.{row["contract_minor"]}',
                        "published_at": self._timestamp(row["published_at"]),
                        "created_at": self._timestamp(row["created_at"]),
                        "acked_at": self._timestamp(row["acked_at"]),
                        "retired_at": self._timestamp(row["retired_at"]),
                        "replay_allowed": bool(
                            row["status"] == "DEAD"
                            and row["subscription_active"]
                            and row["payload_state"] == "ACTIVE"
                        ),
                    }
                    for row in deliveries
                ],
            }

    async def replay(self, ticket, operation_id, payload) -> dict:
        payload = dict(payload)
        _management_payload(payload, ("delivery_id", "expected_revision"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "event.replay", operation_id, payload)
            replayed = await self.control._replay(conn, claims)
            if self.control.is_replay(replayed):
                return replayed
            deletion_epoch = await self.store.content.epoch_in(conn)
            identity = await (
                await conn.execute(
                    "SELECT subscriber_id,contract_major FROM public.bot_event_deliveries "
                    "WHERE bot_id=%s AND delivery_id=%s",
                    (self.store.bot_id, payload["delivery_id"]),
                )
            ).fetchone()
            if identity is None:
                raise ForestError(
                    "EVENT_DELIVERY_NOT_FOUND", "Delivery does not belong to this Bot", 404
                )
            head = await self._head_in(
                conn, identity["subscriber_id"], identity["contract_major"], lock=True
            )
            row = await (
                await conn.execute(
                    "SELECT d.*,e.payload_id FROM public.bot_event_deliveries d "
                    "JOIN public.bot_component_events e ON e.bot_id=d.bot_id AND e.event_id=d.event_id "
                    "WHERE d.bot_id=%s AND d.delivery_id=%s FOR UPDATE OF d",
                    (self.store.bot_id, payload["delivery_id"]),
                )
            ).fetchone()
            if row["management_revision"] != payload["expected_revision"]:
                raise ForestError(
                    "EVENT_DELIVERY_REVISION_CONFLICT", "Delivery changed; reload it", 409
                )
            if row["status"] != "DEAD":
                raise ForestError("EVENT_DELIVERY_NOT_DEAD", "Only DEAD deliveries can be replayed", 409)
            if not head["active"]:
                raise ForestError(
                    "EVENT_SUBSCRIPTION_RETIRED", "A retired subscription cannot be replayed", 409
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
                    "EVENT_PAYLOAD_UNAVAILABLE", "The event body is no longer available", 409
                ) from None
            next_epoch = row["replay_epoch"] + 1
            next_revision = row["management_revision"] + 1
            await conn.execute(
                "INSERT INTO public.bot_event_delivery_replays("
                "bot_id,delivery_id,replay_epoch,operation_id,subject_id,issuer_id,"
                "previous_attempt_count,previous_dispatch_count,previous_error_code) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, row["delivery_id"], next_epoch, operation_id,
                    claims["subject_id"], claims["issuer_id"], row["attempt_count"],
                    row["dispatch_count"], row["last_error_code"],
                ),
            )
            await conn.execute(
                "UPDATE public.bot_event_deliveries SET status='RETRY_WAIT',attempt_count=0,"
                "dispatch_count=0,replay_epoch=%s,management_revision=%s,"
                "available_at=clock_timestamp(),claim_token=NULL,claim_host_incarnation=NULL,"
                "claim_owner_epoch=NULL,claim_until=NULL,claim_new_attempt=NULL,"
                "call_operation_id=NULL,result_payload_id=NULL,last_error_code=NULL,acked_at=NULL,"
                "retired_at=NULL WHERE bot_id=%s AND delivery_id=%s",
                (next_epoch, next_revision, self.store.bot_id, row["delivery_id"]),
            )
            result = {
                "operation_id": operation_id,
                "delivery_id": row["delivery_id"],
                "event_id": row["event_id"],
                "status": "RETRY_WAIT",
                "replay_epoch": next_epoch,
                "management_revision": next_revision,
            }
            await self.control._record(conn, claims, result)
            return result

    async def plan_retirement(self, ticket, operation_id, payload) -> dict:
        payload = dict(payload)
        _management_payload(payload, ("subscriber_id", "contract_major", "expected_revision"))
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "event.retire.plan", operation_id, payload
            )
            replayed = await self.control._replay(conn, claims)
            if self.control.is_replay(replayed):
                return replayed
            await self.store.content.epoch_in(conn)
            await self._clock_in(conn)
            head = await self._head_in(
                conn, payload["subscriber_id"], payload["contract_major"], lock=True
            )
            if head["management_revision"] != payload["expected_revision"]:
                raise ForestError(
                    "EVENT_SUBSCRIPTION_REVISION_CONFLICT", "Subscription changed; reload it", 409
                )
            if not head["active"]:
                raise ForestError("EVENT_SUBSCRIPTION_RETIRED", "Subscription is already retired", 409)
            impact = await self._impact_in(conn, head["subscriber_id"], head["contract_major"])
            impact_digest = digest_json(impact)
            frozen = {
                "subscriber_id": head["subscriber_id"],
                "contract_major": head["contract_major"],
                "expected_revision": head["management_revision"],
                "impact_digest": impact_digest,
                "subject_id": claims["subject_id"],
                "issuer_id": claims["issuer_id"],
            }
            plan_id = str(uuid4())
            plan_digest = digest_json(frozen)
            expires = await self.store._now(conn) + timedelta(minutes=10)
            await conn.execute(
                "INSERT INTO public.bot_event_retirement_plans("
                "bot_id,plan_id,operation_id,subject_id,issuer_id,subscriber_id,contract_major,"
                "expected_revision,impact,impact_digest,plan_digest,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, plan_id, operation_id, claims["subject_id"],
                    claims["issuer_id"], head["subscriber_id"], head["contract_major"],
                    head["management_revision"], Jsonb(impact), impact_digest, plan_digest, expires,
                ),
            )
            result = {
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "subscriber_id": head["subscriber_id"],
                "contract_major": head["contract_major"],
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
                conn, ticket, "event.retire.apply", operation_id, payload,
                payload["plan_digest"],
            )
            replayed = await self.control._replay(conn, claims)
            if self.control.is_replay(replayed):
                return replayed
            await self.store.content.epoch_in(conn)
            await self._clock_in(conn)
            plan = await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_retirement_plans WHERE bot_id=%s "
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
                conn, plan["subscriber_id"], plan["contract_major"], lock=True
            )
            if head["management_revision"] != plan["expected_revision"]:
                raise ForestError(
                    "EVENT_SUBSCRIPTION_REVISION_CONFLICT", "Subscription changed after preview", 409
                )
            if not head["active"]:
                raise ForestError("EVENT_SUBSCRIPTION_RETIRED", "Subscription is already retired", 409)
            impact = await self._impact_in(conn, head["subscriber_id"], head["contract_major"])
            if digest_json(impact) != plan["impact_digest"]:
                raise ForestError(
                    "EVENT_RETIREMENT_PLAN_STALE", "Delivery state changed; preview retirement again", 409
                )
            if impact["inflight"]:
                raise ForestError(
                    "EVENT_SUBSCRIPTION_BUSY", "Wait for in-flight delivery to settle", 409
                )
            revision = head["management_revision"] + 1
            await conn.execute(
                "UPDATE public.bot_event_subscription_heads SET active=false,"
                "management_revision=%s,retired_at=clock_timestamp(),retirement_operation_id=%s,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND subscriber_id=%s "
                "AND contract_major=%s",
                (
                    revision, operation_id, self.store.bot_id, head["subscriber_id"],
                    head["contract_major"],
                ),
            )
            retired = await (
                await conn.execute(
                    "UPDATE public.bot_event_deliveries SET status='RETIRED',"
                    "last_error_code='SUBSCRIPTION_RETIRED',retired_at=clock_timestamp(),"
                    "claim_token=NULL,claim_host_incarnation=NULL,claim_owner_epoch=NULL,"
                    "claim_until=NULL,claim_new_attempt=NULL WHERE bot_id=%s AND subscriber_id=%s "
                    "AND contract_major=%s AND status IN ('PENDING','RETRY_WAIT') "
                    "RETURNING delivery_id",
                    (self.store.bot_id, head["subscriber_id"], head["contract_major"]),
                )
            ).fetchall()
            await conn.execute(
                "INSERT INTO public.bot_event_subscription_retirements("
                "bot_id,subscriber_id,contract_major,management_revision,operation_id,plan_id,"
                "subject_id,issuer_id,retired_delivery_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, head["subscriber_id"], head["contract_major"], revision,
                    operation_id, plan["plan_id"], claims["subject_id"], claims["issuer_id"],
                    len(retired),
                ),
            )
            from forest.storage.control_receipts import ControlReceiptStore

            source = await ControlReceiptStore(self.store).operation_in(conn, plan["operation_id"])
            if (
                source["action"] != "event.retire.plan"
                or not isinstance(source.get("result"), dict)
                or source["result"].get("plan_id") != plan["plan_id"]
                or source["result"].get("plan_digest") != plan["plan_digest"]
            ):
                raise ForestError(
                    "CONTROL_PLAN_SOURCE_CONFLICT", "Retirement plan source differs", 409
                )
            result = {
                "operation_id": operation_id,
                "subscriber_id": head["subscriber_id"],
                "contract_major": head["contract_major"],
                "active": False,
                "management_revision": revision,
                "retired_delivery_count": len(retired),
            }
            await self.control._record(
                conn, claims, result, parents=(source["result_payload_id"],)
            )
            return result

    async def deliveries(self) -> list[dict]:
        async with self.store.pool.connection() as conn:
            await self.store._identity(conn)
            return await (
                await conn.execute(
                    "SELECT * FROM public.bot_event_deliveries WHERE bot_id=%s ORDER BY created_at,delivery_id",
                    (self.store.bot_id,),
                )
            ).fetchall()

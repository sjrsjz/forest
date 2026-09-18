"""Clean ordered Delivery authority with Effect and Adapter fence participants."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace
from forest.storage.effect_authority import EffectPermit

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_LOCK_DOMAIN = b"forest.delivery/1\0"


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("DELIVERY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str, *, optional: bool = False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("DELIVERY_INPUT_INVALID", f"Invalid {field}", 422)
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
        raise ForestError("DELIVERY_INPUT_INVALID", "Delivery input is not canonical", 422) from exc


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value, field: str):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ForestError("DELIVERY_INPUT_INVALID", f"{field} needs a timezone", 422)
    return value.astimezone(timezone.utc)


def _lock_key(bot_id: str, identity: str) -> int:
    digest = hashlib.sha256(
        _LOCK_DOMAIN + bot_id.encode() + b"\0" + identity.encode()
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def _call(port, conn, **values):
    result = port(conn, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class DeliveryPayloadRef:
    payload_id: str
    digest: str
    owner_component_id: str
    owner_kind: str
    owner_id: str

    def validate(self, field: str) -> None:
        _bounded(self.payload_id, f"{field}.payload_id", 256)
        _validated_digest(self.digest, f"{field}.digest")
        _bounded(self.owner_component_id, f"{field}.owner_component_id", 128)
        _bounded(self.owner_kind, f"{field}.owner_kind", 128)
        _bounded(self.owner_id, f"{field}.owner_id", 512)

    def canonical(self) -> dict:
        return {
            "payload_id": self.payload_id,
            "digest": self.digest,
            "owner_component_id": self.owner_component_id,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
        }


@dataclass(frozen=True)
class DeliveryPart:
    part_operation_id: str
    effect_operation_ref: str
    payload: DeliveryPayloadRef

    def validate(self) -> None:
        _bounded(self.part_operation_id, "part_operation_id", 256)
        _bounded(self.effect_operation_ref, "effect_operation_ref", 256)
        self.payload.validate("part.payload")

    def canonical(self) -> dict:
        return {
            "part_operation_id": self.part_operation_id,
            "effect_operation_ref": self.effect_operation_ref,
            "payload": self.payload.canonical(),
        }


@dataclass(frozen=True)
class DeliveryClaim:
    bot_id: str
    intent_id: str
    part_index: int
    part_operation_id: str
    effect_operation_ref: str
    semantic_digest: str
    worker_id: str
    owner_epoch: int
    owner_expires_at: datetime
    binding_ref: str
    lane_kind: str
    ordering_key: str
    lane_sequence: int
    channel_kind: str
    channel_ref: str
    delivery_generation_id: str
    payload: DeliveryPayloadRef


class DeliveryRepository:
    """Serialize each logical output lane without owning Adapter transports."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier=None,
        binding_fence_verifier=None,
        effect_admitter=None,
        effect_permitter=None,
        effect_handoff=None,
        effect_observer=None,
        effect_abandoner=None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.delivery")
        self.payload_verifier = payload_verifier
        self.binding_fence_verifier = binding_fence_verifier
        self.effect_admitter = effect_admitter
        self.effect_permitter = effect_permitter
        self.effect_handoff = effect_handoff
        self.effect_observer = effect_observer
        self.effect_abandoner = effect_abandoner

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _lock(self, conn, identity: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (_lock_key(self.store.bot_id, identity),),
        )

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    async def _verify_payload(self, conn, reference: DeliveryPayloadRef) -> None:
        if self.payload_verifier is None:
            raise ForestError("PAYLOAD_AUTHORITY_MISSING", "Delivery payload port is unavailable", 503)
        await _call(
            self.payload_verifier,
            conn,
            payload_id=reference.payload_id,
            digest=reference.digest,
            owner_component_id=reference.owner_component_id,
            owner_kind=reference.owner_kind,
            owner_id=reference.owner_id,
        )

    async def _verify_binding(self, conn, **values) -> None:
        if self.binding_fence_verifier is None:
            raise ForestError("BINDING_AUTHORITY_MISSING", "Delivery Binding port is unavailable", 503)
        await _call(self.binding_fence_verifier, conn, **values)

    async def create_intent(
        self,
        *,
        operation_id: str,
        intent_id: str,
        binding_ref: str,
        lane_kind: str,
        ordering_key: str,
        channel_kind: str,
        channel_ref: str,
        cause_component_id: str,
        cause_kind: str,
        cause_id: str,
        delivery_generation_id: str,
        parts,
        conversation_ref: str | None = None,
        reply_ref: str | None = None,
        _connection=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (intent_id, "intent_id", 256),
            (binding_ref, "binding_ref", 256),
            (ordering_key, "ordering_key", 256),
            (channel_ref, "channel_ref", 256),
            (cause_component_id, "cause_component_id", 128),
            (cause_kind, "cause_kind", 128),
            (cause_id, "cause_id", 512),
            (delivery_generation_id, "delivery_generation_id", 256),
        ):
            _bounded(value, field, limit)
        _bounded(conversation_ref, "conversation_ref", 256, optional=True)
        _bounded(reply_ref, "reply_ref", 512, optional=True)
        if lane_kind not in {"CHAT", "CONTROL"}:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Delivery lane", 422)
        if channel_kind not in {"GROUP", "PRIVATE"}:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Delivery channel", 422)
        if lane_kind == "CHAT":
            if conversation_ref is None or reply_ref is None or ordering_key != conversation_ref:
                raise ForestError("DELIVERY_INPUT_INVALID", "CHAT intent identity is incomplete", 422)
            if (
                cause_component_id != "conversation.scheduler"
                or cause_kind != "context.reply"
                or cause_id != reply_ref
            ):
                raise ForestError(
                    "DELIVERY_CAUSE_INVALID",
                    "CHAT intent must be caused by the committed Conversation reply",
                    422,
                )
        elif reply_ref is not None:
            raise ForestError("DELIVERY_INPUT_INVALID", "CONTROL intent cannot answer a reply", 422)
        items = tuple(parts)
        if not 1 <= len(items) <= 64 or any(type(item) is not DeliveryPart for item in items):
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Delivery part set", 422)
        for item in items:
            item.validate()
        if len({item.part_operation_id for item in items}) != len(items) or len(
            {item.effect_operation_ref for item in items}
        ) != len(items):
            raise ForestError("DELIVERY_INPUT_INVALID", "Delivery part identity repeats", 422)
        input_digest = _digest(
            {
                "intent_id": intent_id,
                "binding_ref": binding_ref,
                "lane_kind": lane_kind,
                "ordering_key": ordering_key,
                "conversation_ref": conversation_ref,
                "channel_kind": channel_kind,
                "channel_ref": channel_ref,
                "cause_component_id": cause_component_id,
                "cause_kind": cause_kind,
                "cause_id": cause_id,
                "delivery_generation_id": delivery_generation_id,
                "reply_ref": reply_ref,
                "parts": [item.canonical() for item in items],
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "intent:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND create_operation_id=%s").format(
                        self._table("send_intents")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest or replay["intent_id"] != intent_id:
                    raise ForestError("DELIVERY_INTENT_CONFLICT", "SendIntent input differs", 409)
                return dict(replay)
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s").format(
                        self._table("send_intents")
                    ),
                    (self.store.bot_id, intent_id),
                )
            ).fetchone()
            if occupied is not None:
                raise ForestError("DELIVERY_INTENT_CONFLICT", "SendIntent identity differs", 409)
            for reference in sorted((item.payload for item in items), key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,binding_ref,lane_kind,ordering_key) "
                        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING").format(
                    self._table("delivery_lanes")
                ),
                (self.store.bot_id, binding_ref, lane_kind, ordering_key),
            )
            lane = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND binding_ref=%s "
                            "AND lane_kind=%s AND ordering_key=%s FOR UPDATE").format(
                        self._table("delivery_lanes")
                    ),
                    (self.store.bot_id, binding_ref, lane_kind, ordering_key),
                )
            ).fetchone()
            lane_sequence = lane["next_sequence"]
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,intent_id,create_operation_id,input_digest,"
                        "binding_ref,lane_kind,ordering_key,lane_sequence,conversation_ref,"
                        "channel_kind,channel_ref,cause_component_id,cause_kind,cause_id,"
                        "delivery_generation_id,reply_ref,part_count) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("send_intents")),
                    (
                        self.store.bot_id,
                        intent_id,
                        operation_id,
                        input_digest,
                        binding_ref,
                        lane_kind,
                        ordering_key,
                        lane_sequence,
                        conversation_ref,
                        channel_kind,
                        channel_ref,
                        cause_component_id,
                        cause_kind,
                        cause_id,
                        delivery_generation_id,
                        reply_ref,
                        len(items),
                    ),
                )
            ).fetchone()
            for index, item in enumerate(items):
                semantic_digest = _digest(
                    {
                        "intent_id": intent_id,
                        "part_index": index,
                        "part": item.canonical(),
                        "target": [binding_ref, channel_kind, channel_ref],
                    }
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,intent_id,part_index,part_operation_id,"
                        "effect_operation_ref,semantic_digest,payload_id,payload_digest,"
                        "payload_owner_component_id,payload_owner_kind,payload_owner_id) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("send_parts")),
                    (
                        self.store.bot_id,
                        intent_id,
                        index,
                        item.part_operation_id,
                        item.effect_operation_ref,
                        semantic_digest,
                        item.payload.payload_id,
                        item.payload.digest,
                        item.payload.owner_component_id,
                        item.payload.owner_kind,
                        item.payload.owner_id,
                    ),
                )
                if self.effect_admitter is None:
                    raise ForestError(
                        "EFFECT_AUTHORITY_MISSING", "Delivery Effect admission port is unavailable", 503
                    )
                target_id = f"{binding_ref}:{channel_kind}:{channel_ref}"
                _bounded(target_id, "effect.target_id", 512)
                root_operation_id = cause_id
                if len(root_operation_id.encode("utf-8")) > 256:
                    root_operation_id = "cause:" + hashlib.sha256(
                        root_operation_id.encode("utf-8")
                    ).hexdigest()
                await _call(
                    self.effect_admitter,
                    conn,
                    effect_operation_id=item.effect_operation_ref,
                    input_digest=semantic_digest,
                    effect_kind="platform.send",
                    target_kind="delivery-target",
                    target_id=target_id,
                    producer_component_id="delivery",
                    producer_generation_id=delivery_generation_id,
                    root_operation_kind=cause_kind,
                    root_operation_id=root_operation_id,
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET next_sequence=next_sequence+1,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND binding_ref=%s "
                        "AND lane_kind=%s AND ordering_key=%s").format(
                    self._table("delivery_lanes")
                ),
                (self.store.bot_id, binding_ref, lane_kind, ordering_key),
            )
            return dict(row)

    async def create_intent_in(self, conn, **values) -> dict:
        return await self.create_intent(_connection=conn, **values)

    @staticmethod
    def _claim_from(row) -> DeliveryClaim:
        return DeliveryClaim(
            bot_id=row["bot_id"],
            intent_id=row["intent_id"],
            part_index=row["part_index"],
            part_operation_id=row["part_operation_id"],
            effect_operation_ref=row["effect_operation_ref"],
            semantic_digest=row["semantic_digest"],
            worker_id=row["owner_id"],
            owner_epoch=row["owner_epoch"],
            owner_expires_at=row["owner_expires_at"],
            binding_ref=row["binding_ref"],
            lane_kind=row["lane_kind"],
            ordering_key=row["ordering_key"],
            lane_sequence=row["lane_sequence"],
            channel_kind=row["channel_kind"],
            channel_ref=row["channel_ref"],
            delivery_generation_id=row["delivery_generation_id"],
            payload=DeliveryPayloadRef(
                row["payload_id"],
                row["payload_digest"],
                row["payload_owner_component_id"],
                row["payload_owner_kind"],
                row["payload_owner_id"],
            ),
        )

    async def _expanded_part(self, conn, intent_id: str, part_index: int):
        return await (
            await conn.execute(
                sql.SQL("SELECT p.*,i.binding_ref,i.lane_kind,i.ordering_key,i.lane_sequence,"
                        "i.channel_kind,i.channel_ref,i.delivery_generation_id "
                        "FROM {} p JOIN {} i ON i.bot_id=p.bot_id AND i.intent_id=p.intent_id "
                        "WHERE p.bot_id=%s AND p.intent_id=%s AND p.part_index=%s").format(
                    self._table("send_parts"), self._table("send_intents")
                ),
                (self.store.bot_id, intent_id, part_index),
            )
        ).fetchone()

    async def claim(
        self,
        *,
        operation_id: str,
        worker_id: str,
        lanes=("CONTROL", "CHAT"),
        ttl_seconds: float = 30.0,
    ) -> DeliveryClaim | None:
        _bounded(operation_id, "operation_id", 256)
        _bounded(worker_id, "worker_id", 256)
        lane_set = tuple(dict.fromkeys(lanes))
        if not lane_set or len(lane_set) > 2 or any(item not in {"CHAT", "CONTROL"} for item in lane_set):
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid claim lanes", 422)
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)) or not 1 <= ttl_seconds <= 120:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid claim lifetime", 422)
        input_digest = _digest({"worker_id": worker_id, "lanes": lane_set, "ttl": float(ttl_seconds)})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "claim:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("claim_operations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("DELIVERY_CLAIM_CONFLICT", "Claim operation input differs", 409)
                if replay["selected_intent_id"] is None:
                    return None
                selected = await self._expanded_part(
                    conn, replay["selected_intent_id"], replay["selected_part_index"]
                )
                selected = dict(selected)
                selected.update(
                    owner_id=replay["worker_id"],
                    owner_epoch=replay["selected_owner_epoch"],
                    owner_expires_at=replay["owner_expires_at"],
                )
                return self._claim_from(selected)
            candidate = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT p.*,i.binding_ref,i.lane_kind,i.ordering_key,i.lane_sequence,"
                        "i.channel_kind,i.channel_ref,i.delivery_generation_id "
                        "FROM {} p JOIN {} i ON i.bot_id=p.bot_id AND i.intent_id=p.intent_id "
                        "WHERE p.bot_id=%s AND p.state='QUEUED' AND i.state='QUEUED' "
                        "AND p.part_index=i.current_part_index AND i.lane_kind=ANY(%s) "
                        "AND NOT EXISTS (SELECT 1 FROM {} earlier WHERE earlier.bot_id=i.bot_id "
                        "AND earlier.binding_ref=i.binding_ref AND earlier.lane_kind=i.lane_kind "
                        "AND earlier.ordering_key=i.ordering_key AND earlier.lane_sequence<i.lane_sequence "
                        "AND earlier.state IN ('QUEUED','SENDING','UNKNOWN')) "
                        "ORDER BY CASE i.lane_kind WHEN 'CONTROL' THEN 0 ELSE 1 END,"
                        "i.created_at,i.intent_id FOR UPDATE OF p,i SKIP LOCKED LIMIT 1"
                    ).format(
                        self._table("send_parts"),
                        self._table("send_intents"),
                        self._table("send_intents"),
                    ),
                    (self.store.bot_id, list(lane_set)),
                )
            ).fetchone()
            if candidate is None:
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,worker_id) "
                            "VALUES (%s,%s,%s,%s)").format(self._table("claim_operations")),
                    (self.store.bot_id, operation_id, input_digest, worker_id),
                )
                return None
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            expires = now["now"] + timedelta(seconds=float(ttl_seconds))
            owner_epoch = candidate["owner_epoch"] + 1
            await conn.execute(
                sql.SQL("UPDATE {} SET state='CLAIMED',owner_id=%s,owner_epoch=%s,"
                        "owner_expires_at=%s,updated_at=%s WHERE bot_id=%s AND intent_id=%s "
                        "AND part_index=%s").format(self._table("send_parts")),
                (
                    worker_id,
                    owner_epoch,
                    expires,
                    now["now"],
                    self.store.bot_id,
                    candidate["intent_id"],
                    candidate["part_index"],
                ),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='SENDING',updated_at=%s WHERE bot_id=%s "
                        "AND intent_id=%s").format(self._table("send_intents")),
                (now["now"], self.store.bot_id, candidate["intent_id"]),
            )
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,worker_id,"
                        "selected_intent_id,selected_part_index,selected_owner_epoch,"
                        "owner_expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)").format(
                    self._table("claim_operations")
                ),
                (
                    self.store.bot_id,
                    operation_id,
                    input_digest,
                    worker_id,
                    candidate["intent_id"],
                    candidate["part_index"],
                    owner_epoch,
                    expires,
                ),
            )
            candidate = dict(candidate)
            candidate.update(owner_id=worker_id, owner_epoch=owner_epoch, owner_expires_at=expires)
            return self._claim_from(candidate)

    async def _guard_claim(self, conn, claim: DeliveryClaim):
        if type(claim) is not DeliveryClaim or claim.bot_id != self.store.bot_id:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Delivery claim", 422)
        part = await (
            await conn.execute(
                sql.SQL("SELECT p.*,i.binding_ref,i.lane_kind,i.ordering_key,i.lane_sequence,"
                        "i.channel_kind,i.channel_ref,i.delivery_generation_id,"
                        "clock_timestamp() AS database_now "
                        "FROM {} p JOIN {} i ON i.bot_id=p.bot_id AND i.intent_id=p.intent_id "
                        "WHERE p.bot_id=%s AND p.intent_id=%s AND p.part_index=%s "
                        "FOR UPDATE OF p,i").format(
                    self._table("send_parts"), self._table("send_intents")
                ),
                (self.store.bot_id, claim.intent_id, claim.part_index),
            )
        ).fetchone()
        if part is None:
            raise ForestError("DELIVERY_PART_NOT_FOUND", "Delivery part does not exist", 404)
        if (
            part["owner_id"] != claim.worker_id
            or part["owner_epoch"] != claim.owner_epoch
            or part["owner_expires_at"] != claim.owner_expires_at
            or part["database_now"] >= part["owner_expires_at"]
            or part["part_operation_id"] != claim.part_operation_id
            or part["effect_operation_ref"] != claim.effect_operation_ref
            or part["semantic_digest"] != claim.semantic_digest
            or part["binding_ref"] != claim.binding_ref
            or part["lane_kind"] != claim.lane_kind
            or part["ordering_key"] != claim.ordering_key
            or part["lane_sequence"] != claim.lane_sequence
            or part["channel_kind"] != claim.channel_kind
            or part["channel_ref"] != claim.channel_ref
            or part["delivery_generation_id"] != claim.delivery_generation_id
            or part["payload_id"] != claim.payload.payload_id
            or part["payload_digest"] != claim.payload.digest
            or part["payload_owner_component_id"] != claim.payload.owner_component_id
            or part["payload_owner_kind"] != claim.payload.owner_kind
            or part["payload_owner_id"] != claim.payload.owner_id
        ):
            raise ForestError("STALE_DELIVERY_OWNER", "Delivery claim is stale", 409)
        return part

    async def prepare_attempt(
        self,
        claim: DeliveryClaim,
        *,
        operation_id: str,
        attempt_id: str,
        transport_action: str,
        wire: DeliveryPayloadRef,
        binding_epoch: int,
        connection_epoch: int,
        client_incarnation: str,
        adapter_generation_id: str,
        executor_fence_digest: str,
        authorization_ticket_ref: str,
        authorization_plan_digest: str | None,
        deadline_at: datetime,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (attempt_id, "attempt_id", 256),
            (transport_action, "transport_action", 128),
            (client_incarnation, "client_incarnation", 256),
            (adapter_generation_id, "adapter_generation_id", 256),
            (authorization_ticket_ref, "authorization_ticket_ref", 256),
        ):
            _bounded(value, field, limit)
        if type(binding_epoch) is not int or binding_epoch <= 0 or type(connection_epoch) is not int or connection_epoch <= 0:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Binding fence", 422)
        _validated_digest(executor_fence_digest, "executor_fence_digest")
        _validated_digest(authorization_plan_digest, "authorization_plan_digest", optional=True)
        deadline_at = _timestamp(deadline_at, "deadline_at")
        wire.validate("wire")
        if (wire.owner_component_id, wire.owner_kind, wire.owner_id) != (
            "kernel.effect",
            "effect.wire",
            attempt_id,
        ):
            raise ForestError("DELIVERY_WIRE_IDENTITY_CONFLICT", "Wire payload owner differs", 409)
        input_digest = _digest(
            {
                "claim": [claim.intent_id, claim.part_index, claim.owner_epoch],
                "attempt_id": attempt_id,
                "transport_action": transport_action,
                "wire": wire.canonical(),
                "binding": [
                    claim.binding_ref,
                    binding_epoch,
                    connection_epoch,
                    client_incarnation,
                    adapter_generation_id,
                    executor_fence_digest,
                ],
                "authorization": [authorization_ticket_ref, authorization_plan_digest],
                "deadline_at": deadline_at.isoformat(),
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "attempt:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND "
                            "(prepare_operation_id=%s OR attempt_id=%s)").format(
                        self._table("transport_attempts")
                    ),
                    (self.store.bot_id, operation_id, attempt_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest or replay["attempt_id"] != attempt_id:
                    raise ForestError("DELIVERY_ATTEMPT_CONFLICT", "Transport attempt input differs", 409)
                return dict(replay)
            part = await self._guard_claim(conn, claim)
            if part["state"] != "CLAIMED":
                raise ForestError("DELIVERY_ATTEMPT_CONFLICT", "Delivery part is not claimed", 409)
            await self._verify_binding(
                conn,
                binding_ref=claim.binding_ref,
                binding_epoch=binding_epoch,
                connection_epoch=connection_epoch,
                client_incarnation=client_incarnation,
                adapter_generation_id=adapter_generation_id,
                executor_fence_digest=executor_fence_digest,
            )
            if self.effect_permitter is None:
                raise ForestError("EFFECT_AUTHORITY_MISSING", "Delivery Effect permit port is unavailable", 503)
            permit = await _call(
                self.effect_permitter,
                conn,
                effect_operation_id=claim.effect_operation_ref,
                attempt_id=attempt_id,
                permit_operation_id=operation_id,
                wire_payload_id=wire.payload_id,
                wire_payload_digest=wire.digest,
                authorization_ticket_id=authorization_ticket_ref,
                authorization_action="delivery.send",
                authorization_resource_kind="bot",
                authorization_resource_id=self.store.bot_id,
                authorization_plan_digest=authorization_plan_digest,
                executor_component_id="adapter.onebot",
                executor_generation_id=adapter_generation_id,
                fence_kind="adapter.binding",
                fence_id=client_incarnation,
                fence_epoch=connection_epoch,
                deadline_at=deadline_at,
            )
            if (
                permit.effect_operation_id != claim.effect_operation_ref
                or permit.attempt_id != attempt_id
                or permit.wire_payload_id != wire.payload_id
                or permit.wire_payload_digest != wire.digest
            ):
                raise ForestError("EFFECT_RECEIPT_INVALID", "Effect permit identity differs", 503)
            attempt_no = part["attempt_count"] + 1
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,attempt_id,intent_id,part_index,attempt_no,"
                        "prepare_operation_id,input_digest,effect_operation_ref,effect_attempt_ref,"
                        "effect_attempt_no,transport_action,wire_payload_id,wire_payload_digest,binding_epoch,"
                        "connection_epoch,client_incarnation,adapter_generation_id,"
                        "executor_fence_digest,authorization_ticket_ref,authorization_plan_digest,"
                        "deadline_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("transport_attempts")),
                    (
                        self.store.bot_id,
                        attempt_id,
                        claim.intent_id,
                        claim.part_index,
                        attempt_no,
                        operation_id,
                        input_digest,
                        claim.effect_operation_ref,
                        permit.attempt_id,
                        permit.attempt_no,
                        transport_action,
                        wire.payload_id,
                        wire.digest,
                        binding_epoch,
                        connection_epoch,
                        client_incarnation,
                        adapter_generation_id,
                        executor_fence_digest,
                        authorization_ticket_ref,
                        authorization_plan_digest,
                        deadline_at,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET state='PREPARED',current_attempt_id=%s,"
                        "attempt_count=%s,updated_at=clock_timestamp() WHERE bot_id=%s "
                        "AND intent_id=%s AND part_index=%s").format(self._table("send_parts")),
                (
                    attempt_id,
                    attempt_no,
                    self.store.bot_id,
                    claim.intent_id,
                    claim.part_index,
                ),
            )
            return dict(row)

    async def mark_handoff(
        self,
        claim: DeliveryClaim,
        *,
        attempt_id: str,
        handoff_operation_id: str,
    ) -> dict:
        _bounded(attempt_id, "attempt_id", 256)
        _bounded(handoff_operation_id, "handoff_operation_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            part = await self._guard_claim(conn, claim)
            attempt = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                        self._table("transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if attempt is None or attempt["intent_id"] != claim.intent_id or attempt["part_index"] != claim.part_index:
                raise ForestError("DELIVERY_ATTEMPT_NOT_FOUND", "Transport attempt does not exist", 404)
            if attempt["state"] == "DISPATCHING" and attempt["handoff_operation_id"] == handoff_operation_id:
                return dict(attempt)
            if part["state"] != "PREPARED" or attempt["state"] != "PREPARED":
                raise ForestError("DELIVERY_HANDOFF_CONFLICT", "Transport attempt cannot hand off", 409)
            await self._verify_binding(
                conn,
                binding_ref=claim.binding_ref,
                binding_epoch=attempt["binding_epoch"],
                connection_epoch=attempt["connection_epoch"],
                client_incarnation=attempt["client_incarnation"],
                adapter_generation_id=attempt["adapter_generation_id"],
                executor_fence_digest=attempt["executor_fence_digest"],
            )
            if self.effect_handoff is None:
                raise ForestError("EFFECT_AUTHORITY_MISSING", "Delivery Effect handoff port is unavailable", 503)
            effect_permit = EffectPermit(
                self.store.bot_id,
                attempt["effect_operation_ref"],
                attempt["effect_attempt_ref"],
                attempt["effect_attempt_no"],
                attempt["wire_payload_id"],
                attempt["wire_payload_digest"],
                "adapter.onebot",
                attempt["adapter_generation_id"],
                "adapter.binding",
                attempt["client_incarnation"],
                attempt["connection_epoch"],
                attempt["deadline_at"],
            )
            effect = await _call(
                self.effect_handoff,
                conn,
                permit=effect_permit,
                handoff_operation_id=handoff_operation_id,
            )
            if effect["attempt_id"] != attempt_id or effect["state"] != "DISPATCHING":
                raise ForestError("EFFECT_RECEIPT_INVALID", "Effect handoff identity differs", 503)
            now = effect["started_at"]
            row = await (
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='DISPATCHING',handoff_operation_id=%s,"
                            "started_at=%s WHERE bot_id=%s AND attempt_id=%s RETURNING *").format(
                        self._table("transport_attempts")
                    ),
                    (handoff_operation_id, now, self.store.bot_id, attempt_id),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET state='DISPATCHING',updated_at=%s WHERE bot_id=%s "
                        "AND intent_id=%s AND part_index=%s").format(self._table("send_parts")),
                (now, self.store.bot_id, claim.intent_id, claim.part_index),
            )
            return dict(row)

    async def observe(
        self,
        *,
        observation_operation_id: str,
        effect_observation_operation_id: str,
        attempt_id: str,
        observed_status: str,
        evidence_kind: str,
        receipt: DeliveryPayloadRef | None = None,
        error_code: str | None = None,
    ) -> dict:
        for value, field in (
            (observation_operation_id, "observation_operation_id"),
            (effect_observation_operation_id, "effect_observation_operation_id"),
            (attempt_id, "attempt_id"),
        ):
            _bounded(value, field, 256)
        if observed_status not in {
            "SENT",
            "RETRYABLE_NOT_APPLIED",
            "REJECTED_NOT_APPLIED",
            "UNKNOWN",
        }:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Delivery observation", 422)
        if evidence_kind not in {"EXECUTOR", "RECONCILIATION"}:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid Delivery evidence kind", 422)
        _bounded(error_code, "error_code", 128, optional=True)
        if observed_status == "SENT":
            if receipt is None or error_code is not None:
                raise ForestError("DELIVERY_INPUT_INVALID", "SENT needs only a receipt", 422)
        elif error_code is None:
            raise ForestError("DELIVERY_INPUT_INVALID", "Non-success needs an error code", 422)
        if receipt is not None:
            receipt.validate("receipt")
            if (
                receipt.owner_component_id,
                receipt.owner_kind,
                receipt.owner_id,
            ) != ("kernel.effect", "effect.evidence", effect_observation_operation_id):
                raise ForestError("DELIVERY_RECEIPT_IDENTITY_CONFLICT", "Receipt owner differs", 409)
        input_digest = _digest(
            {
                "attempt_id": attempt_id,
                "observed_status": observed_status,
                "evidence_kind": evidence_kind,
                "receipt": receipt.canonical() if receipt else None,
                "error_code": error_code,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "observation:" + observation_operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s "
                            "AND observation_operation_id=%s").format(
                        self._table("delivery_observations")
                    ),
                    (self.store.bot_id, observation_operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("DELIVERY_OBSERVATION_CONFLICT", "Observation input differs", 409)
                return dict(replay)
            if receipt is not None:
                await self._verify_payload(conn, receipt)
            attempt = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                        self._table("transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if attempt is None:
                raise ForestError("DELIVERY_ATTEMPT_NOT_FOUND", "Transport attempt does not exist", 404)
            intent = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s FOR UPDATE").format(
                        self._table("send_intents")
                    ),
                    (self.store.bot_id, attempt["intent_id"]),
                )
            ).fetchone()
            part = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s "
                            "AND part_index=%s FOR UPDATE").format(self._table("send_parts")),
                    (self.store.bot_id, attempt["intent_id"], attempt["part_index"]),
                )
            ).fetchone()
            if (
                attempt["intent_id"] != part["intent_id"]
                or attempt["part_index"] != part["part_index"]
            ):
                raise ForestError(
                    "DELIVERY_ATTEMPT_CONFLICT", "Transport attempt identity differs", 409
                )
            if attempt["state"] not in {
                "DISPATCHING",
                "SENT",
                "NOT_APPLIED",
                "REJECTED",
                "UNKNOWN",
            }:
                raise ForestError(
                    "DELIVERY_OBSERVATION_BOUNDARY_CLOSED",
                    "Transport attempt has not crossed the handoff boundary",
                    409,
                )
            is_current = (
                part["current_attempt_id"] == attempt_id
                and intent["current_part_index"] == part["part_index"]
            )
            if not is_current and attempt["state"] in {"DISPATCHING", "UNKNOWN"}:
                raise ForestError(
                    "DELIVERY_ATTEMPT_CONFLICT",
                    "An unsettled transport attempt is no longer current",
                    409,
                )
            effect_status = {
                "SENT": "SUCCEEDED",
                "RETRYABLE_NOT_APPLIED": "NOT_APPLIED",
                "REJECTED_NOT_APPLIED": "NOT_APPLIED",
                "UNKNOWN": "UNKNOWN",
            }[observed_status]
            if self.effect_observer is None:
                raise ForestError("EFFECT_AUTHORITY_MISSING", "Delivery Effect observation port is unavailable", 503)
            effect_observation = await _call(
                self.effect_observer,
                conn,
                effect_operation_id=attempt["effect_operation_ref"],
                attempt_id=attempt["effect_attempt_ref"],
                observation_operation_id=effect_observation_operation_id,
                status=effect_status,
                evidence_kind="reconciliation" if evidence_kind == "RECONCILIATION" else "executor",
                evidence_payload_id=receipt.payload_id if receipt else None,
                evidence_payload_digest=receipt.digest if receipt else None,
            )
            previous = attempt["state"]
            observed_result = {
                "SENT": "SENT",
                "RETRYABLE_NOT_APPLIED": "NOT_APPLIED",
                "REJECTED_NOT_APPLIED": "REJECTED",
                "UNKNOWN": "UNKNOWN",
            }[observed_status]
            accepted = (
                is_current
                and (
                    previous == "DISPATCHING"
                    or (
                        previous == "UNKNOWN"
                        and evidence_kind == "RECONCILIATION"
                        and observed_status != "UNKNOWN"
                    )
                )
            ) or (previous == observed_result and effect_observation["accepted"])
            resulting = observed_result if accepted else previous
            conflict_code = None if accepted else "DELIVERY_RESULT_ALREADY_SETTLED"
            if accepted and not effect_observation["accepted"]:
                raise ForestError("EFFECT_RECEIPT_INVALID", "Effect rejected an accepted Delivery result", 503)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,observation_operation_id,"
                        "effect_observation_operation_id,attempt_id,input_digest,evidence_kind,"
                        "observed_status,receipt_payload_id,receipt_payload_digest,error_code,"
                        "accepted,previous_state,resulting_state,conflict_code) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("delivery_observations")),
                    (
                        self.store.bot_id,
                        observation_operation_id,
                        effect_observation_operation_id,
                        attempt_id,
                        input_digest,
                        evidence_kind,
                        observed_status,
                        receipt.payload_id if receipt else None,
                        receipt.digest if receipt else None,
                        error_code,
                        accepted,
                        previous,
                        resulting,
                        conflict_code,
                    ),
                )
            ).fetchone()
            if not accepted or previous == resulting:
                return dict(row)
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,receipt_payload_id=%s,receipt_payload_digest=%s,"
                        "error_code=%s,settled_at=COALESCE(settled_at,%s),resolved_at=%s "
                        "WHERE bot_id=%s AND attempt_id=%s").format(
                    self._table("transport_attempts")
                ),
                (
                    resulting,
                    receipt.payload_id if receipt else None,
                    receipt.digest if receipt else None,
                    error_code,
                    now["now"],
                    now["now"] if previous == "UNKNOWN" else None,
                    self.store.bot_id,
                    attempt_id,
                ),
            )
            if observed_status == "SENT":
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='SENT',owner_id=NULL,owner_expires_at=NULL,"
                            "updated_at=%s WHERE bot_id=%s AND intent_id=%s AND part_index=%s").format(
                        self._table("send_parts")
                    ),
                    (now["now"], self.store.bot_id, intent["intent_id"], part["part_index"]),
                )
                if part["part_index"] + 1 == intent["part_count"]:
                    await conn.execute(
                        sql.SQL("UPDATE {} SET state='SENT',completed_at=%s,updated_at=%s "
                                "WHERE bot_id=%s AND intent_id=%s").format(
                            self._table("send_intents")
                        ),
                        (now["now"], now["now"], self.store.bot_id, intent["intent_id"]),
                    )
                else:
                    await conn.execute(
                        sql.SQL("UPDATE {} SET state='QUEUED',current_part_index=%s,updated_at=%s "
                                "WHERE bot_id=%s AND intent_id=%s").format(
                            self._table("send_intents")
                        ),
                        (
                            part["part_index"] + 1,
                            now["now"],
                            self.store.bot_id,
                            intent["intent_id"],
                        ),
                    )
            elif observed_status == "RETRYABLE_NOT_APPLIED":
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,"
                            "current_attempt_id=NULL,updated_at=%s WHERE bot_id=%s "
                            "AND intent_id=%s AND part_index=%s").format(self._table("send_parts")),
                    (now["now"], self.store.bot_id, intent["intent_id"], part["part_index"]),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='QUEUED',error_code=NULL,updated_at=%s "
                            "WHERE bot_id=%s AND intent_id=%s").format(self._table("send_intents")),
                    (now["now"], self.store.bot_id, intent["intent_id"]),
                )
            elif observed_status == "REJECTED_NOT_APPLIED":
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='FAILED',owner_id=NULL,owner_expires_at=NULL,"
                            "updated_at=%s WHERE bot_id=%s AND intent_id=%s AND part_index=%s").format(
                        self._table("send_parts")
                    ),
                    (now["now"], self.store.bot_id, intent["intent_id"], part["part_index"]),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='CANCELLED',updated_at=%s WHERE bot_id=%s "
                            "AND intent_id=%s AND part_index>%s AND state='QUEUED'").format(
                        self._table("send_parts")
                    ),
                    (
                        now["now"],
                        self.store.bot_id,
                        intent["intent_id"],
                        part["part_index"],
                    ),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='FAILED',error_code=%s,completed_at=%s,"
                            "updated_at=%s WHERE bot_id=%s AND intent_id=%s").format(
                        self._table("send_intents")
                    ),
                    (
                        error_code,
                        now["now"],
                        now["now"],
                        self.store.bot_id,
                        intent["intent_id"],
                    ),
                )
            else:
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='UNKNOWN',owner_id=NULL,owner_expires_at=NULL,"
                            "updated_at=%s WHERE bot_id=%s AND intent_id=%s AND part_index=%s").format(
                        self._table("send_parts")
                    ),
                    (now["now"], self.store.bot_id, intent["intent_id"], part["part_index"]),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='UNKNOWN',error_code=%s,updated_at=%s "
                            "WHERE bot_id=%s AND intent_id=%s").format(self._table("send_intents")),
                    (error_code, now["now"], self.store.bot_id, intent["intent_id"]),
                )
            return dict(row)

    async def release_claim(
        self,
        claim: DeliveryClaim,
        *,
        operation_id: str,
        reason_code: str,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(reason_code, "reason_code", 128)
        input_digest = _digest(
            [claim.intent_id, claim.part_index, claim.owner_epoch, reason_code]
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "release:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("claim_releases")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("DELIVERY_RELEASE_CONFLICT", "Release input differs", 409)
                return dict(replay)
            part = await self._guard_claim(conn, claim)
            if part["state"] != "CLAIMED":
                raise ForestError("DELIVERY_RELEASE_CONFLICT", "Only a claim can be released", 409)
            row = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,intent_id,part_index,"
                            "owner_epoch,reason_code) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *").format(
                        self._table("claim_releases")
                    ),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        claim.intent_id,
                        claim.part_index,
                        claim.owner_epoch,
                        reason_code,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s "
                        "AND part_index=%s").format(self._table("send_parts")),
                (self.store.bot_id, claim.intent_id, claim.part_index),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='QUEUED',updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND intent_id=%s").format(self._table("send_intents")),
                (self.store.bot_id, claim.intent_id),
            )
            return dict(row)

    async def recoverable_attempts(self, *, limit: int = 64) -> tuple[dict, ...]:
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ForestError("DELIVERY_INPUT_INVALID", "Invalid recovery limit", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT a.*,p.state AS part_state,p.owner_expires_at,i.state AS intent_state "
                        "FROM {} a JOIN {} p ON p.bot_id=a.bot_id AND p.intent_id=a.intent_id "
                        "AND p.part_index=a.part_index JOIN {} i ON i.bot_id=p.bot_id "
                        "AND i.intent_id=p.intent_id WHERE a.bot_id=%s "
                        "AND p.current_attempt_id=a.attempt_id "
                        "AND a.state IN ('PREPARED','DISPATCHING') "
                        "AND (p.owner_expires_at IS NULL OR p.owner_expires_at<=clock_timestamp()) "
                        "ORDER BY a.prepared_at,a.attempt_id LIMIT %s"
                    ).format(
                        self._table("transport_attempts"),
                        self._table("send_parts"),
                        self._table("send_intents"),
                    ),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
        return tuple(dict(row) for row in rows)

    async def abandon_prepared_attempt(
        self,
        *,
        attempt_id: str,
        operation_id: str,
        reason_code: str,
    ) -> dict:
        for value, field, limit in (
            (attempt_id, "attempt_id", 256),
            (operation_id, "operation_id", 256),
            (reason_code, "reason_code", 128),
        ):
            _bounded(value, field, limit)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "abandon:" + operation_id)
            attempt = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                        self._table("transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if attempt is None:
                raise ForestError("DELIVERY_ATTEMPT_NOT_FOUND", "Transport attempt does not exist", 404)
            if attempt["state"] == "ABANDONED":
                return dict(attempt)
            part = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s AND part_index=%s "
                        "FOR UPDATE"
                    ).format(self._table("send_parts")),
                    (
                        self.store.bot_id,
                        attempt["intent_id"],
                        attempt["part_index"],
                    ),
                )
            ).fetchone()
            if (
                attempt["state"] != "PREPARED"
                or part is None
                or part["state"] != "PREPARED"
                or part["current_attempt_id"] != attempt_id
            ):
                raise ForestError(
                    "DELIVERY_ATTEMPT_NOT_ABANDONABLE",
                    "Only the current prepared attempt can be abandoned",
                    409,
                )
            if self.effect_abandoner is None:
                raise ForestError(
                    "EFFECT_AUTHORITY_MISSING", "Delivery Effect abandonment port is unavailable", 503
                )
            await _call(
                self.effect_abandoner,
                conn,
                effect_operation_id=attempt["effect_operation_ref"],
                attempt_id=attempt["effect_attempt_ref"],
                abandonment_operation_id=operation_id,
                reason_code=reason_code,
                cancel_effect=False,
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='ABANDONED',settled_at=clock_timestamp() "
                        "WHERE bot_id=%s AND attempt_id=%s RETURNING *"
                    ).format(self._table("transport_attempts")),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='CLAIMED',current_attempt_id=NULL,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s "
                    "AND part_index=%s"
                ).format(self._table("send_parts")),
                (
                    self.store.bot_id,
                    attempt["intent_id"],
                    attempt["part_index"],
                ),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='QUEUED',owner_id=NULL,owner_expires_at=NULL,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s "
                    "AND part_index=%s"
                ).format(self._table("send_parts")),
                (
                    self.store.bot_id,
                    attempt["intent_id"],
                    attempt["part_index"],
                ),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='QUEUED',error_code=NULL,updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND intent_id=%s"
                ).format(self._table("send_intents")),
                (self.store.bot_id, attempt["intent_id"]),
            )
            return dict(row)

    async def cancel_intent(
        self,
        *,
        operation_id: str,
        intent_id: str,
        reason_code: str,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(intent_id, "intent_id", 256)
        _bounded(reason_code, "reason_code", 128)
        input_digest = _digest([intent_id, reason_code])
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "cancel:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("intent_cancellations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("DELIVERY_CANCEL_CONFLICT", "Cancellation input differs", 409)
                return dict(replay)
            intent = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s FOR UPDATE").format(
                        self._table("send_intents")
                    ),
                    (self.store.bot_id, intent_id),
                )
            ).fetchone()
            if intent is None:
                raise ForestError("DELIVERY_INTENT_NOT_FOUND", "SendIntent does not exist", 404)
            if intent["state"] in {"SENT", "FAILED", "CANCELLED"}:
                raise ForestError("DELIVERY_ALREADY_TERMINAL", "SendIntent is already terminal", 409)
            part = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s "
                            "AND part_index=%s FOR UPDATE").format(self._table("send_parts")),
                    (self.store.bot_id, intent_id, intent["current_part_index"]),
                )
            ).fetchone()
            if part["state"] in {"DISPATCHING", "UNKNOWN"}:
                resulting = "UNKNOWN"
            else:
                if part["state"] == "PREPARED":
                    if self.effect_abandoner is None:
                        raise ForestError(
                            "EFFECT_AUTHORITY_MISSING",
                            "Delivery Effect abandonment port is unavailable",
                            503,
                        )
                    attempt = await (
                        await conn.execute(
                            sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s "
                                    "FOR UPDATE").format(self._table("transport_attempts")),
                            (self.store.bot_id, part["current_attempt_id"]),
                        )
                    ).fetchone()
                    abandonment_id = "delivery-cancel:" + operation_id
                    if len(abandonment_id.encode("utf-8")) > 256:
                        abandonment_id = "delivery-cancel:" + hashlib.sha256(
                            operation_id.encode("utf-8")
                        ).hexdigest()
                    await _call(
                        self.effect_abandoner,
                        conn,
                        effect_operation_id=attempt["effect_operation_ref"],
                        attempt_id=attempt["effect_attempt_ref"],
                        abandonment_operation_id=abandonment_id,
                        reason_code=reason_code,
                        cancel_effect=True,
                    )
                    await conn.execute(
                        sql.SQL("UPDATE {} SET state='ABANDONED',settled_at=clock_timestamp() "
                                "WHERE bot_id=%s AND attempt_id=%s").format(
                            self._table("transport_attempts")
                        ),
                        (self.store.bot_id, attempt["attempt_id"]),
                    )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='CANCELLED',owner_id=NULL,owner_expires_at=NULL,"
                            "current_attempt_id=NULL,updated_at=clock_timestamp() WHERE bot_id=%s "
                            "AND intent_id=%s AND part_index>=%s "
                            "AND state IN ('QUEUED','CLAIMED','PREPARED')").format(
                        self._table("send_parts")
                    ),
                    (self.store.bot_id, intent_id, intent["current_part_index"]),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='CANCELLED',error_code=%s,"
                            "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND intent_id=%s").format(
                        self._table("send_intents")
                    ),
                    (reason_code, self.store.bot_id, intent_id),
                )
                resulting = "CANCELLED"
            row = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,intent_id,"
                            "reason_code,resulting_state) VALUES (%s,%s,%s,%s,%s,%s) "
                            "RETURNING *").format(self._table("intent_cancellations")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        intent_id,
                        reason_code,
                        resulting,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def load_intent(self, intent_id: str) -> dict:
        _bounded(intent_id, "intent_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND intent_id=%s").format(
                        self._table("send_intents")
                    ),
                    (self.store.bot_id, intent_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("DELIVERY_INTENT_NOT_FOUND", "SendIntent does not exist", 404)
            return dict(row)

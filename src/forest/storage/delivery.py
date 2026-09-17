"""Bot-local effect ledger. External I/O never runs inside these transactions.

The effect root is the cancellation linearization row. A Run owner locks its
conversation/Run first, then calls cancel_root_in; a sender does not acquire a
Run lock after that root. An accepted permit cannot promise network revocation.
"""

from __future__ import annotations

import base64
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.storage.bot import BotStore, HostLease
from forest.storage.delivery_payloads import UNAVAILABLE, DeliveryContentStore, receipt_metadata

TERMINAL = ("SENT", "FAILED", "CANCELLED", "SKIPPED")
ACTIVE = ("PENDING", "CLAIMED", "DISPATCHING", "UNKNOWN")


def _freeze(value: Any, maximum: int = 524288) -> tuple[Any, str]:
    try:
        wire = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        if len(wire) > maximum:
            raise ValueError("too large")
        return json.loads(wire), hashlib.sha256(wire).hexdigest()
    except (ValueError, TypeError, RecursionError) as exc:
        raise ForestError("INVALID_DELIVERY", "Delivery input must be bounded JSON", 422) from exc


def _text(value: Any, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ForestError("INVALID_DELIVERY", f"Invalid {name}", 422)
    return value


@dataclass(frozen=True)
class DeliveryClaim:
    bot_id: str
    intent_id: str
    part_index: int
    part_operation_id: str
    owner_epoch: int
    worker_id: str
    host_incarnation: str
    host_owner_epoch: int
    claim_until: datetime
    binding_epoch: int
    connection_epoch: int
    binding_revision: int
    route: dict
    payload: list[dict]
    adapter_contract: str
    payload_id: str = ""
    deletion_epoch: int = 0


@dataclass(frozen=True)
class DeliveryPermit:
    bot_id: str
    intent_id: str
    part_index: int
    attempt_id: str
    owner_epoch: int
    host_incarnation: str
    host_owner_epoch: int
    deadline_at: datetime
    action: str
    wire_body: bytes
    wire_digest: str
    binding_epoch: int
    connection_epoch: int
    wire_payload_id: str = ""
    deletion_epoch: int = 0


class DeliveryStore:
    def __init__(self, store: BotStore, bindings=None):
        if bindings is None:
            from forest.storage.bindings import BindingStore

            bindings = BindingStore(store)
        self.store = store
        self.bindings = bindings
        self.contents = DeliveryContentStore(store)

    async def _binding_guard(self, conn, lease, route, *, data, expected_epoch=None):
        await self.bindings._guard(conn, lease, route["binding_id"], expected_epoch, data=data)
        binding = await (
            await conn.execute(
                "SELECT * FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s",
                (self.store.bot_id, route["binding_id"]),
            )
        ).fetchone()
        if binding["self_id"] != route["self_id"] or binding["platform"] != route["platform"]:
            raise ForestError(
                "BINDING_ROUTE_MISMATCH", "Reply route changed its account identity", 403
            )
        return binding

    def _route(self, route: dict) -> dict:
        route, _ = _freeze(route, 8192)
        if not isinstance(route, dict) or route.get("bot_id") != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Reply route belongs to another Bot", 403)
        for name in ("binding_id", "platform", "self_id", "channel_id"):
            _text(route.get(name), name)
        if route.get("platform") != "onebot-v11" or route.get("channel_kind") not in (
            "group",
            "private",
        ):
            raise ForestError("INVALID_DELIVERY", "Unsupported reply route", 422)
        if route.get("visibility") != route["channel_kind"]:
            raise ForestError("INVALID_DELIVERY", "Reply route visibility must be explicit", 422)
        if (
            not isinstance(route.get("binding_epoch"), int)
            or isinstance(route["binding_epoch"], bool)
            or route["binding_epoch"] < 1
        ):
            raise ForestError("INVALID_DELIVERY", "Reply route requires a binding epoch", 422)
        if route.get("thread_id", "") != "":
            raise ForestError("INVALID_DELIVERY", "OneBot v11 does not support thread targets", 422)
        return route

    async def enqueue(self, lease: HostLease, **kwargs) -> dict:
        route = self._route(kwargs["route"])
        parts, _ = _freeze(kwargs["parts"])
        kwargs = {
            **kwargs,
            "route": route,
            "parts": parts,
            "parents": tuple(kwargs.get("parents", ())),
            "sources": self.contents.payloads._sources(kwargs.get("sources", ())),
        }
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._binding_guard(
                conn,
                lease,
                route,
                data=kwargs.get("lane", "CHAT") == "CHAT",
                expected_epoch=route["binding_epoch"],
            )
            result = await self.enqueue_in(conn, lease, **kwargs)
            await self._binding_guard(
                conn,
                lease,
                route,
                data=kwargs.get("lane", "CHAT") == "CHAT",
                expected_epoch=route["binding_epoch"],
            )
            return result

    async def enqueue_in(
        self,
        conn,
        lease: HostLease,
        *,
        operation_id: str,
        route: dict,
        parts: list[list[dict]],
        conversation_id: str,
        lane: str = "CHAT",
        response_seq: int | None = None,
        command_id: str | None = None,
        control_seq: int | None = None,
        root_operation_id: str | None = None,
        cause_kind: str = "DELIVERY_OPERATION",
        cause_id: str | None = None,
        expected_policy_revision: int | None = None,
        adapter_contract: str = "onebot-v11:1",
        parents=(),
        sources=(),
    ) -> dict:
        """Join the caller's final transaction, after Binding/runtime/host/Run guards.

        This method only adds lower-order effect locks. It never selects a new
        Binding or obtains a component/Run lock behind an effect lock.
        """
        self.store._scope(lease)
        parents = tuple(sorted(set(parents)))
        sources = self.contents.payloads._sources(sources)
        _, lineage_digest = _freeze([parents, [asdict(source) for source in sources]])
        route = self._route(route)
        _text(operation_id, "operation_id")
        _text(conversation_id, "conversation_id")
        root_operation_id = root_operation_id or operation_id
        _text(root_operation_id, "root_operation_id")
        cause_id = cause_id or operation_id
        _text(cause_id, "cause_id")
        if cause_kind not in {
            "SHELL_COMMAND", "CONVERSATION_NOTICE", "MESSAGE_HANDLER_CHAIN",
            "BUSINESS_COMMAND_REQUEST", "CONVERSATION_RUN", "KERNEL_CALL",
            "DELIVERY_OPERATION",
        }:
            raise ForestError("INVALID_DELIVERY", "Delivery cause kind is invalid", 422)
        if lane not in ("CHAT", "CONTROL") or adapter_contract != "onebot-v11:1":
            raise ForestError(
                "INVALID_DELIVERY", "Unsupported delivery lane or adapter contract", 422
            )
        sequence = response_seq if lane == "CHAT" else control_seq
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            raise ForestError(
                "INVALID_DELIVERY", "Delivery sequence must be a positive integer", 422
            )
        if (lane == "CHAT" and (command_id is not None or control_seq is not None)) or (
            lane == "CONTROL" and response_seq is not None
        ):
            raise ForestError(
                "INVALID_DELIVERY", "CHAT and CONTROL use separate ordering keys", 422
            )
        if lane == "CONTROL":
            _text(command_id, "command_id")
        parts, _ = _freeze(parts)
        if (
            not isinstance(parts, list)
            or not 1 <= len(parts) <= 64
            or any(
                not isinstance(part, list)
                or not 1 <= len(part) <= 256
                or any(not isinstance(segment, dict) for segment in part)
                for part in parts
            )
        ):
            raise ForestError("INVALID_DELIVERY", "Delivery requires bounded nonempty parts", 422)
        for part in parts:
            _freeze(part, 65536)
        stream_id = _freeze([lane, conversation_id if lane == "CHAT" else command_id])[1]
        _, input_digest = _freeze(
            {
                "route": route,
                "parts": parts,
                "conversation_id": conversation_id,
                "lane": lane,
                "sequence": sequence,
                "command_id": command_id,
                "root": root_operation_id,
                "cause": [cause_kind, cause_id],
                "adapter_contract": adapter_contract,
                "policy": expected_policy_revision,
            }
        )
        # Runtime/host are already locked by the caller; reads do not invert its
        # lock order. The last clock check occurs after all lower-order waits.
        runtime = await (
            await conn.execute(
                "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s", (self.store.bot_id,)
            )
        ).fetchone()
        if runtime is None or (
            expected_policy_revision is not None
            and runtime["policy_revision"] != expected_policy_revision
        ):
            raise ForestError("POLICY_CHANGED", "Delivery output authority changed", 403)
        old = await (
            await conn.execute(
                "SELECT * FROM public.bot_send_intents WHERE bot_id=%s AND operation_id=%s",
                (self.store.bot_id, operation_id),
            )
        ).fetchone()
        if old:
            if old["input_digest"] != input_digest or (
                old["lineage_digest"] is not None and old["lineage_digest"] != lineage_digest
            ):
                raise ForestError("IDEMPOTENCY_CONFLICT", "Delivery operation input changed")
            await self._final_host(conn, lease)
            return self._public_intent(old)
        root = await self._root(conn, root_operation_id)
        if root["effects_cancelled"]:
            raise ForestError("CANCELLED", "The effect root was cancelled")
        await conn.execute(
            "INSERT INTO public.bot_delivery_streams(bot_id,stream_id,binding_id,lane) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id, stream_id, route["binding_id"], lane),
        )
        stream = await (
            await conn.execute(
                "SELECT * FROM public.bot_delivery_streams WHERE bot_id=%s AND stream_id=%s FOR UPDATE",
                (self.store.bot_id, stream_id),
            )
        ).fetchone()
        if stream["binding_id"] != route["binding_id"] or stream["last_seq"] + 1 != sequence:
            raise ForestError(
                "DELIVERY_SEQUENCE_CONFLICT",
                "Reply sequence must be contiguous in its fixed stream",
            )
        intent_id = str(uuid4())
        row = await (
            await conn.execute(
                "INSERT INTO public.bot_send_intents(bot_id,intent_id,operation_id,input_digest,root_operation_id,cause_kind,cause_id,binding_id,binding_epoch,policy_revision,storage_generation,stream_id,send_lane,conversation_id,response_seq,command_id,control_seq,sequence,reply_route,adapter_contract,lineage_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    self.store.bot_id,
                    intent_id,
                    operation_id,
                    input_digest,
                    root_operation_id,
                    cause_kind,
                    cause_id,
                    route["binding_id"],
                    route["binding_epoch"],
                    runtime["policy_revision"],
                    runtime["storage_generation"],
                    stream_id,
                    lane,
                    conversation_id,
                    response_seq,
                    command_id,
                    control_seq,
                    sequence,
                    Jsonb(route),
                    adapter_contract,
                    lineage_digest,
                ),
            )
        ).fetchone()
        clock = await self.contents.payloads.lock_in(conn)
        for index, part in enumerate(parts):
            part_operation_id = _freeze([operation_id, index])[1]
            body = await self.contents.payloads.put_json_in(
                conn,
                owner_kind="delivery.payload",
                owner_id=part_operation_id,
                value=part,
                parents=parents,
                sources=sources,
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            if body.state != "ACTIVE":
                raise ForestError("PAYLOAD_ERASED", "Delivery body was removed", 410)
            await conn.execute(
                "INSERT INTO public.bot_send_parts(bot_id,intent_id,part_index,part_operation_id,payload_id,payload_digest) VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    intent_id,
                    index,
                    part_operation_id,
                    body.payload_id,
                    _freeze(part)[1],
                ),
            )
        await conn.execute(
            "UPDATE public.bot_delivery_streams SET last_seq=%s WHERE bot_id=%s AND stream_id=%s",
            (sequence, self.store.bot_id, stream_id),
        )
        await self.store._event(
            conn,
            "delivery.enqueued",
            operation_id,
            {"intent_id": intent_id, "lane": lane, "parts": len(parts), "sequence": sequence},
        )
        await self._final_host(conn, lease)
        return self._public_intent(row)

    async def _final_host(self, conn, lease):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_host_lease WHERE bot_id=%s", (self.store.bot_id,)
            )
        ).fetchone()
        if (
            row is None
            or row["incarnation"] != lease.incarnation
            or row["host_id"] != lease.host_id
            or row["owner_epoch"] != lease.owner_epoch
            or row["expires_at"] <= await self.store._now(conn)
        ):
            raise ForestError("STALE_HOST", "Host ownership expired or was replaced")

    async def _root(self, conn, root_operation_id):
        await conn.execute(
            "INSERT INTO public.bot_delivery_roots(bot_id,root_operation_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id, root_operation_id),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_delivery_roots WHERE bot_id=%s AND root_operation_id=%s FOR UPDATE",
                (self.store.bot_id, root_operation_id),
            )
        ).fetchone()

    @staticmethod
    def _public_intent(row):
        return {
            key: row[key]
            for key in (
                "intent_id",
                "operation_id",
                "send_lane",
                "conversation_id",
                "response_seq",
                "command_id",
                "control_seq",
            )
        }

    async def _intent(self, conn, intent_id):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_send_intents WHERE bot_id=%s AND intent_id=%s",
                (self.store.bot_id, intent_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("NOT_FOUND", "Delivery intent does not exist", 404)
        return row

    async def claim(
        self, lease: HostLease, worker_id: str, *, lanes=("CONTROL", "CHAT"), ttl_seconds=30
    ) -> DeliveryClaim | None:
        self.store._scope(lease)
        self.store._ttl(ttl_seconds)
        _text(worker_id, "worker_id")
        if not lanes or any(lane not in ("CHAT", "CONTROL") for lane in lanes):
            raise ValueError("Invalid delivery lanes")
        # Candidate enumeration takes no row locks. Every candidate is checked
        # again after acquiring its Binding and Bot authority in global order.
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            candidates = await (
                await conn.execute(
                    "SELECT i.intent_id,p.part_index FROM public.bot_send_intents i "
                    "JOIN public.bot_send_parts p USING(bot_id,intent_id) "
                    "JOIN public.bot_delivery_streams s USING(bot_id,stream_id) "
                    "JOIN public.bot_bindings b ON b.bot_id=i.bot_id AND b.binding_id=i.binding_id "
                    "JOIN public.bot_runtime_state rt ON rt.bot_id=i.bot_id "
                    "LEFT JOIN public.bot_delivery_binding_limits l ON l.bot_id=i.bot_id AND l.binding_id=i.binding_id "
                    "WHERE i.bot_id=%s AND i.send_lane=ANY(%s) AND p.state='PENDING' "
                    "AND b.host_incarnation=%s AND b.host_owner_epoch=%s "
                    "AND ((i.send_lane='CONTROL' AND rt.control_admission AND b.state IN ('ONLINE','DRAINING')) "
                    "OR (i.send_lane='CHAT' AND rt.data_admission AND b.enabled AND b.state='ONLINE')) "
                    "AND p.available_at<=clock_timestamp() AND (l.next_send_at IS NULL OR l.next_send_at<=clock_timestamp()) "
                    "AND NOT EXISTS(SELECT 1 FROM public.bot_send_intents older JOIN public.bot_send_parts op USING(bot_id,intent_id) "
                    "WHERE older.bot_id=i.bot_id AND older.stream_id=i.stream_id AND op.state=ANY(%s) "
                    "AND (older.sequence<i.sequence OR (older.sequence=i.sequence AND op.part_index<p.part_index))) "
                    "ORDER BY CASE WHEN i.send_lane='CONTROL' AND coalesce(l.control_streak,0)<4 THEN 0 WHEN i.send_lane='CHAT' THEN 1 ELSE 2 END,s.last_selected_at,i.created_at,p.part_index LIMIT 128",
                    (
                        self.store.bot_id,
                        list(lanes),
                        lease.incarnation,
                        lease.owner_epoch,
                        list(ACTIVE),
                    ),
                )
            ).fetchall()
            await self._final_host(conn, lease)
        for candidate in candidates:
            async with self.store.pool.connection() as conn, conn.transaction():
                intent = await self._intent(conn, candidate["intent_id"])
                try:
                    binding = await self._binding_guard(
                        conn, lease, intent["reply_route"], data=intent["send_lane"] == "CHAT"
                    )
                except ForestError as exc:
                    if exc.code in {"STALE_HOST", "BOT_SCOPE_MISMATCH", "STORAGE_SEALED"}:
                        raise
                    # An unavailable Binding or paused lane cannot starve other
                    # accounts in this Bot. Its durable intent remains visible.
                    continue
                root = await self._root(conn, intent["root_operation_id"])
                await conn.execute(
                    "SELECT stream_id FROM public.bot_delivery_streams WHERE bot_id=%s AND stream_id=%s FOR UPDATE",
                    (self.store.bot_id, intent["stream_id"]),
                )
                part = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s AND part_index=%s FOR UPDATE",
                        (self.store.bot_id, intent["intent_id"], candidate["part_index"]),
                    )
                ).fetchone()
                now = await self.store._now(conn)
                if part["state"] != "PENDING" or part["available_at"] > now:
                    continue
                runtime = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s",
                        (self.store.bot_id,),
                    )
                ).fetchone()
                if (
                    root["effects_cancelled"]
                    or runtime["policy_revision"] != intent["policy_revision"]
                    or runtime["storage_generation"] != intent["storage_generation"]
                ):
                    await conn.execute(
                        "UPDATE public.bot_send_parts SET state='CANCELLED',revision=revision+1,error_code=%s,updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s",
                        (
                            "CANCELLED" if root["effects_cancelled"] else "AUTHORITY_CHANGED",
                            self.store.bot_id,
                            intent["intent_id"],
                            part["part_index"],
                        ),
                    )
                    continue
                prior = await (
                    await conn.execute(
                        "SELECT 1 FROM public.bot_send_intents i JOIN public.bot_send_parts p USING(bot_id,intent_id) WHERE i.bot_id=%s AND i.stream_id=%s AND p.state=ANY(%s) AND (i.sequence<%s OR (i.sequence=%s AND p.part_index<%s)) LIMIT 1",
                        (
                            self.store.bot_id,
                            intent["stream_id"],
                            list(ACTIVE),
                            intent["sequence"],
                            intent["sequence"],
                            part["part_index"],
                        ),
                    )
                ).fetchone()
                if prior:
                    continue
                clock = await self.contents.payloads.lock_in(conn)
                try:
                    body = await self.contents.read_part_in(
                        conn, part, expected_deletion_epoch=clock["deletion_epoch"]
                    )
                except ForestError as exc:
                    if exc.code not in UNAVAILABLE:
                        raise
                    await conn.execute(
                        "UPDATE public.bot_send_parts SET state='CANCELLED',revision=revision+1,error_code='DELIVERY_CONTENT_REMOVED',"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s",
                        (self.store.bot_id, part["intent_id"], part["part_index"]),
                    )
                    await self._final_host(conn, lease)
                    continue
                claimed = await (
                    await conn.execute(
                        "UPDATE public.bot_send_parts SET state='CLAIMED',revision=revision+1,owner_epoch=owner_epoch+1,host_incarnation=%s,host_owner_epoch=%s,worker_id=%s,claim_until=%s,claim_binding_epoch=%s,claim_connection_epoch=%s,claim_binding_revision=%s,updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s RETURNING *",
                        (
                            lease.incarnation,
                            lease.owner_epoch,
                            worker_id,
                            now + timedelta(seconds=ttl_seconds),
                            binding["binding_epoch"],
                            binding["connection_epoch"],
                            binding["revision"],
                            self.store.bot_id,
                            intent["intent_id"],
                            part["part_index"],
                        ),
                    )
                ).fetchone()
                await self.store._event(
                    conn,
                    "delivery.claimed",
                    intent["operation_id"],
                    {
                        "intent_id": intent["intent_id"],
                        "part_index": part["part_index"],
                        "accepted_binding_epoch": intent["binding_epoch"],
                        "binding_epoch": binding["binding_epoch"],
                        "connection_epoch": binding["connection_epoch"],
                        "owner_epoch": claimed["owner_epoch"],
                    },
                )
                await conn.execute(
                    "UPDATE public.bot_delivery_streams SET last_selected_at=clock_timestamp() WHERE bot_id=%s AND stream_id=%s",
                    (self.store.bot_id, intent["stream_id"]),
                )
                await self._final_host(conn, lease)
                return DeliveryClaim(
                    self.store.bot_id,
                    intent["intent_id"],
                    part["part_index"],
                    part["part_operation_id"],
                    claimed["owner_epoch"],
                    worker_id,
                    lease.incarnation,
                    lease.owner_epoch,
                    claimed["claim_until"],
                    binding["binding_epoch"],
                    binding["connection_epoch"],
                    binding["revision"],
                    intent["reply_route"],
                    body,
                    intent["adapter_contract"],
                    part["payload_id"],
                    clock["deletion_epoch"],
                )
        return None

    async def permit(
        self,
        lease: HostLease,
        claim: DeliveryClaim,
        *,
        action: str,
        wire_body: bytes,
        wire_digest: str,
        timeout_seconds: float = 15,
        executor_ref: dict | None = None,
        interval_seconds: float = 1,
    ) -> DeliveryPermit:
        self.store._scope(lease)
        self.store._ttl(timeout_seconds)
        if claim.bot_id != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Delivery claim belongs to another Bot", 403)
        if (
            not isinstance(wire_body, bytes)
            or not 0 < len(wire_body) <= 1048576
            or hashlib.sha256(wire_body).hexdigest() != wire_digest
        ):
            raise ForestError(
                "INVALID_WIRE_PAYLOAD", "Exact prepared bytes and digest must agree", 422
            )
        if (
            action not in ("send_group_msg", "send_private_msg")
            or not 0 <= interval_seconds <= 3600
        ):
            raise ForestError("INVALID_DELIVERY", "Invalid action or account rate", 422)
        from forest.adapters.onebot import PreparedSend

        try:
            PreparedSend(action, wire_body, wire_digest)
        except (ValueError, ForestError) as exc:
            raise ForestError(
                "INVALID_WIRE_PAYLOAD", "Prepared OneBot request is invalid", 422
            ) from exc
        executor_ref, _ = _freeze(executor_ref or {"driver": "onebot-v11:1"}, 8192)
        async with self.store.pool.connection() as conn, conn.transaction():
            intent = await self._intent(conn, claim.intent_id)
            # Adapter routes precede Binding/runtime locks in the same order as
            # kernel publication. A prepared old generation cannot gain a new
            # permit after its route was frozen or replaced.
            if "component_id" in executor_ref:
                adapter = await (
                    await conn.execute(
                        "SELECT r.*,g.worker_incarnation,g.status FROM public.bot_component_routes r JOIN public.bot_generations g USING(bot_id,generation_id) WHERE r.bot_id=%s AND r.component_id=%s FOR UPDATE OF r",
                        (self.store.bot_id, executor_ref["component_id"]),
                    )
                ).fetchone()
                if (
                    adapter is None
                    or not adapter[
                        "control_admission"
                        if executor_ref.get("admission_lane") == "CONTROL"
                        else "admission"
                    ]
                    or adapter["status"] != "ACTIVE"
                    or any(
                        adapter[field] != executor_ref.get(field)
                        for field in ("generation_id", "route_epoch", "worker_incarnation")
                    )
                ):
                    raise ForestError(
                        "STALE_ADAPTER", "Prepared adapter generation lost publication authority"
                    )
            binding = await self._binding_guard(
                conn,
                lease,
                intent["reply_route"],
                data=intent["send_lane"] == "CHAT",
                expected_epoch=claim.binding_epoch,
            )
            if (
                binding["connection_epoch"] != claim.connection_epoch
                or binding["revision"] != claim.binding_revision
            ):
                raise ForestError(
                    "STALE_BINDING", "Prepared output belongs to another Binding connection"
                )
            root = await self._root(conn, intent["root_operation_id"])
            part = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s AND part_index=%s FOR UPDATE",
                    (self.store.bot_id, claim.intent_id, claim.part_index),
                )
            ).fetchone()
            self._check_claim(part, claim)
            runtime = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s", (self.store.bot_id,)
                )
            ).fetchone()
            if root["effects_cancelled"]:
                raise ForestError("CANCELLED", "The effect root was cancelled")
            if (
                runtime["policy_revision"] != intent["policy_revision"]
                or runtime["storage_generation"] != intent["storage_generation"]
            ):
                raise ForestError("POLICY_CHANGED", "Delivery output authority changed", 403)
            if (
                "component_id" in executor_ref
                and executor_ref.get("policy_revision") != runtime["policy_revision"]
            ):
                raise ForestError(
                    "POLICY_CHANGED", "Prepared adapter output authority changed", 403
                )
            route = intent["reply_route"]
            if action != (
                "send_group_msg" if route["channel_kind"] == "group" else "send_private_msg"
            ):
                raise ForestError(
                    "INVALID_WIRE_PAYLOAD", "Prepared action changed the frozen target", 422
                )
            # Core validates target before signing the permit. Conversion may run
            # in a Worker, but it cannot redirect this output to another group.
            try:
                body = json.loads(wire_body)
                target = body["group_id" if route["channel_kind"] == "group" else "user_id"]
                if str(target) != route["channel_id"] or not isinstance(body.get("message"), list):
                    raise ValueError("target mismatch")
            except (ValueError, KeyError, TypeError, UnicodeError) as exc:
                raise ForestError(
                    "INVALID_WIRE_PAYLOAD",
                    "Prepared wire target does not match the frozen route",
                    422,
                ) from exc
            await conn.execute(
                "INSERT INTO public.bot_delivery_binding_limits(bot_id,binding_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, intent["binding_id"]),
            )
            limit = await (
                await conn.execute(
                    "SELECT * FROM public.bot_delivery_binding_limits WHERE bot_id=%s AND binding_id=%s FOR UPDATE",
                    (self.store.bot_id, intent["binding_id"]),
                )
            ).fetchone()
            busy = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_send_intents i JOIN public.bot_send_parts p USING(bot_id,intent_id) WHERE i.bot_id=%s AND i.binding_id=%s AND p.state='DISPATCHING' LIMIT 1",
                    (self.store.bot_id, intent["binding_id"]),
                )
            ).fetchone()
            if (
                intent["send_lane"] == "CONTROL"
                and limit["control_streak"] >= 4
                and runtime["data_admission"]
            ):
                waiting_chat = await (
                    await conn.execute(
                        "SELECT 1 FROM public.bot_send_intents i JOIN public.bot_send_parts p USING(bot_id,intent_id) JOIN public.bot_delivery_roots r USING(bot_id,root_operation_id) WHERE i.bot_id=%s AND i.binding_id=%s AND i.send_lane='CHAT' AND i.policy_revision=%s AND NOT r.effects_cancelled AND p.state IN ('PENDING','CLAIMED') AND p.available_at<=clock_timestamp() "
                        "AND NOT EXISTS(SELECT 1 FROM public.bot_send_intents older JOIN public.bot_send_parts op USING(bot_id,intent_id) WHERE older.bot_id=i.bot_id AND older.stream_id=i.stream_id AND op.state=ANY(%s) AND (older.sequence<i.sequence OR (older.sequence=i.sequence AND op.part_index<p.part_index))) LIMIT 1",
                        (
                            self.store.bot_id,
                            intent["binding_id"],
                            runtime["policy_revision"],
                            list(ACTIVE),
                        ),
                    )
                ).fetchone()
                if waiting_chat:
                    raise ForestError(
                        "DELIVERY_RATE_LIMITED",
                        "A queued CHAT turn owns the next fair send opportunity",
                        429,
                    )
            now = await self.store._now(conn)
            if busy or limit["next_send_at"] > now:
                raise ForestError(
                    "DELIVERY_RATE_LIMITED", "Binding's next send opportunity is pending", 429
                )
            if part["claim_until"] <= now:
                raise ForestError("STALE_DELIVERY_OWNER", "Delivery claim expired")
            settings = await self.bindings.current_config_in(
                conn, self.store.bot_id, intent["binding_id"], expected_binding_epoch=claim.binding_epoch,
            )
            clock = await self.contents.payloads.lock_in(conn)
            if clock["deletion_epoch"] != claim.deletion_epoch:
                raise ForestError(
                    "DELETION_CONFLICT", "Deletion epoch changed during output preparation", 409
                )
            if part["payload_id"] != claim.payload_id:
                raise ForestError("DELIVERY_PAYLOAD_CONFLICT", "Claim body reference changed", 409)
            await self.contents.payloads.require_available_in(
                conn, (part["payload_id"],), expected_deletion_epoch=claim.deletion_epoch
            )
            wire_parents = [part["payload_id"], settings["config_payload_id"]]
            if "component_id" in executor_ref:
                call = await (await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s",
                    (self.store.bot_id, executor_ref.get("call_id")),
                )).fetchone()
                fields = ("component_id", "generation_id", "worker_incarnation", "route_epoch",
                          "policy_revision", "admission_lane", "result_payload_id", "result_digest")
                if (call is None or call["status"] != "COMPLETED"
                        or call["control_service"] != "onebot.prepare"
                        or any(call[key] != executor_ref.get(key) for key in fields)):
                    raise ForestError("FORMAT_RECEIPT_MISSING", "Prepared wire has no matching Kernel receipt", 409)
                call = await self.store.content.hydrate_call_in(conn, call)
                expected_result = {
                    "action": action, "wire_body_b64": base64.b64encode(wire_body).decode(),
                    "wire_digest": wire_digest,
                }
                if call["result"] != expected_result:
                    raise ForestError("INVALID_WIRE_PAYLOAD", "Prepared wire differs from the Kernel receipt", 422)
                # The codec is a pure protocol projection. Recompute the full
                # value from the authorized SendPart: a persistent codec may
                # not substitute content retained from another conversation.
                from forest.adapters.onebot import prepare_send

                semantic = await self.contents.read_part_in(
                    conn, part, expected_deletion_epoch=claim.deletion_epoch,
                )
                projection = prepare_send(route["channel_kind"], route["channel_id"], semantic)
                if (projection.action != action or projection.wire_body != wire_body
                        or projection.wire_digest != wire_digest):
                    raise ForestError("INVALID_WIRE_PAYLOAD", "Codec changed the authorized message projection", 422)
                wire_parents.append(call["result_payload_id"])
            await self._final_host(conn, lease)
            now = await self.store._now(conn)
            deadline = now + timedelta(seconds=timeout_seconds)
            attempt_id = str(uuid4())
            wire = await self.contents.payloads.put_bytes_in(
                conn,
                owner_kind="delivery.request",
                owner_id=attempt_id,
                value=wire_body,
                parents=tuple(wire_parents),
                expected_deletion_epoch=claim.deletion_epoch,
            )
            await conn.execute(
                "INSERT INTO public.bot_send_attempts(bot_id,attempt_id,intent_id,part_index,owner_epoch,host_incarnation,host_owner_epoch,policy_revision,binding_epoch,connection_epoch,binding_revision,cancel_epoch,action,wire_payload_id,wire_digest,executor_ref,permitted_at,deadline_at,deletion_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    attempt_id,
                    claim.intent_id,
                    claim.part_index,
                    claim.owner_epoch,
                    lease.incarnation,
                    lease.owner_epoch,
                    intent["policy_revision"],
                    claim.binding_epoch,
                    claim.connection_epoch,
                    claim.binding_revision,
                    root["cancel_epoch"],
                    action,
                    wire.payload_id,
                    wire_digest,
                    Jsonb(executor_ref),
                    now,
                    deadline,
                    claim.deletion_epoch,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_send_parts SET state='DISPATCHING',attempt_id=%s,attempt_count=attempt_count+1,revision=revision+1,updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s",
                (attempt_id, self.store.bot_id, claim.intent_id, claim.part_index),
            )
            await conn.execute(
                "UPDATE public.bot_delivery_binding_limits SET next_send_at=%s,control_streak=CASE WHEN %s='CONTROL' THEN control_streak+1 ELSE 0 END WHERE bot_id=%s AND binding_id=%s",
                (
                    now + timedelta(seconds=interval_seconds),
                    intent["send_lane"],
                    self.store.bot_id,
                    intent["binding_id"],
                ),
            )
            await self.store._event(
                conn,
                "delivery.permitted",
                intent["operation_id"],
                {
                    "attempt_id": attempt_id,
                    "intent_id": claim.intent_id,
                    "part_index": claim.part_index,
                    "wire_digest": wire_digest,
                },
            )
            await self._final_host(conn, lease)
            if min(part["claim_until"], deadline) <= await self.store._now(conn):
                raise ForestError(
                    "STALE_DELIVERY_OWNER", "Delivery claim or permit expired before commit"
                )
            return DeliveryPermit(
                self.store.bot_id,
                claim.intent_id,
                claim.part_index,
                attempt_id,
                claim.owner_epoch,
                lease.incarnation,
                lease.owner_epoch,
                deadline,
                action,
                wire_body,
                wire_digest,
                claim.binding_epoch,
                claim.connection_epoch,
                wire.payload_id,
                claim.deletion_epoch,
            )

    @staticmethod
    def _check_claim(part, claim):
        if (
            part is None
            or part["state"] != "CLAIMED"
            or any(
                part[field] != getattr(claim, field)
                for field in ("owner_epoch", "host_incarnation", "host_owner_epoch", "worker_id")
            )
            or any(
                part["claim_" + field] != getattr(claim, field)
                for field in ("binding_epoch", "connection_epoch", "binding_revision")
            )
        ):
            raise ForestError("STALE_DELIVERY_OWNER", "Delivery claim expired or was replaced")

    async def release(
        self,
        lease: HostLease,
        claim: DeliveryClaim,
        *,
        error_code="PREPARE_RETRY",
        delay_seconds=1,
        terminal=False,
    ) -> None:
        """Only a not-yet-permitted claim can be returned to the queue."""
        self.store._scope(lease)
        if claim.bot_id != self.store.bot_id or not 0 <= delay_seconds <= 3600:
            raise ForestError("INVALID_DELIVERY", "Invalid delivery release", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            part = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s AND part_index=%s FOR UPDATE",
                    (self.store.bot_id, claim.intent_id, claim.part_index),
                )
            ).fetchone()
            self._check_claim(part, claim)
            await self._final_host(conn, lease)
            await conn.execute(
                "UPDATE public.bot_send_parts SET state=%s,revision=revision+1,error_code=%s,available_at=clock_timestamp()+(%s*interval '1 second'),updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s",
                (
                    "FAILED" if terminal else "PENDING",
                    error_code,
                    delay_seconds,
                    self.store.bot_id,
                    claim.intent_id,
                    claim.part_index,
                ),
            )

            await self._final_host(conn, lease)

    async def mark_started(self, permit: DeliveryPermit) -> None:
        """Consume a permit once before entering the transport, never replay it.

        The timestamp describes local handoff, not evidence that a platform saw
        bytes. Lost acknowledgement here is conservatively UNKNOWN as well.
        """
        if permit.bot_id != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Delivery permit belongs to another Bot", 403)
        async with self.store.pool.connection() as conn, conn.transaction():
            part = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s AND part_index=%s FOR UPDATE",
                    (self.store.bot_id, permit.intent_id, permit.part_index),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_attempts WHERE bot_id=%s AND attempt_id=%s FOR UPDATE",
                    (self.store.bot_id, permit.attempt_id),
                )
            ).fetchone()
            if (
                attempt is None
                or part is None
                or part["attempt_id"] != permit.attempt_id
                or any(
                    attempt[field] != getattr(permit, field)
                    for field in (
                        "intent_id",
                        "part_index",
                        "owner_epoch",
                        "host_incarnation",
                        "host_owner_epoch",
                        "wire_digest",
                    )
                )
            ):
                raise ForestError(
                    "STALE_DELIVERY_OWNER", "Transport handoff does not match its permit"
                )
            if attempt["started_at"] is not None:
                raise ForestError(
                    "DELIVERY_ALREADY_STARTED", "A consumed transport permit cannot be replayed"
                )
            if part["state"] != "DISPATCHING" or attempt["state"] != "DISPATCHING":
                raise ForestError(
                    "STALE_DELIVERY_OWNER", "Transport permit is no longer dispatchable"
                )
            if attempt["deadline_at"] <= await self.store._now(conn):
                raise ForestError(
                    "DELIVERY_PERMIT_EXPIRED", "Transport permit expired before handoff"
                )
            # The permit has not entered transport yet: a removed body can
            # prevent this handoff, even if its original bytes remain in RAM.
            await self.contents.payloads.require_available_in(
                conn,
                (attempt["wire_payload_id"],),
                expected_deletion_epoch=attempt["deletion_epoch"],
            )
            body = await self.contents.read_attempt_in(
                conn, attempt, expected_deletion_epoch=attempt["deletion_epoch"]
            )
            if body != permit.wire_body:
                raise ForestError(
                    "DELIVERY_PAYLOAD_CONFLICT", "Permit bytes differ from the frozen attempt", 409
                )
            if attempt["deadline_at"] <= await self.store._now(conn):
                raise ForestError(
                    "DELIVERY_PERMIT_EXPIRED", "Transport permit expired before handoff"
                )
            await conn.execute(
                "UPDATE public.bot_send_attempts SET started_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s",
                (self.store.bot_id, permit.attempt_id),
            )

    async def complete(
        self, permit: DeliveryPermit, status: str, receipt: dict | None = None
    ) -> dict:
        """Accept evidence for the original attempt even after host replacement.

        This capability records an observation only. It cannot grant a new send,
        change the target, or overwrite a manual decision for the current part.
        """
        if permit.bot_id != self.store.bot_id or status not in ("SENT", "NOT_SENT", "UNKNOWN"):
            raise ForestError("INVALID_DELIVERY", "Invalid delivery observation", 422)
        full_receipt, receipt_digest = _freeze(receipt or {}, 16384)
        receipt = receipt_metadata(full_receipt)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            # Receipts lock part before attempt; permit/recovery/reconciliation
            # use the same order, and no later authority lock is acquired.
            part = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s AND part_index=%s FOR UPDATE",
                    (self.store.bot_id, permit.intent_id, permit.part_index),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_attempts WHERE bot_id=%s AND attempt_id=%s FOR UPDATE",
                    (self.store.bot_id, permit.attempt_id),
                )
            ).fetchone()
            if attempt is None or any(
                attempt[field] != getattr(permit, field)
                for field in (
                    "intent_id",
                    "part_index",
                    "owner_epoch",
                    "host_incarnation",
                    "host_owner_epoch",
                    "wire_digest",
                )
            ):
                raise ForestError(
                    "STALE_DELIVERY_OWNER", "Observation does not match a permitted attempt"
                )
            if attempt["state"] in ("SENT", "NOT_SENT"):
                if status not in (attempt["state"], "UNKNOWN"):
                    raise ForestError(
                        "DELIVERY_RECEIPT_CONFLICT", "A settled attempt has conflicting evidence"
                    )
                return self._public_part(part)
            if attempt["state"] == "UNKNOWN" and status == "UNKNOWN":
                return self._public_part(part)
            clock = await self.contents.payloads.lock_in(conn)
            receipt_payload_id = await self.contents.retain_observation_in(
                conn,
                owner_id="attempt:" + permit.attempt_id + ":" + status + ":" + receipt_digest,
                value=full_receipt,
                parent=attempt["wire_payload_id"],
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            await conn.execute(
                "UPDATE public.bot_send_attempts SET state=%s,receipt=%s,receipt_payload_id=%s,observed_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s",
                (status, Jsonb(receipt), receipt_payload_id, self.store.bot_id, permit.attempt_id),
            )
            if part["attempt_id"] == permit.attempt_id and part["state"] in (
                "DISPATCHING",
                "UNKNOWN",
            ):
                # Definite rejection is terminal, not an implicit retry loop.
                # A caller can submit a distinct corrected business proposal;
                # UNKNOWN needs explicit reconciliation before any replay.
                state = {"SENT": "SENT", "NOT_SENT": "FAILED", "UNKNOWN": "UNKNOWN"}[status]
                part = await (
                    await conn.execute(
                        "UPDATE public.bot_send_parts SET state=%s,receipt=%s,receipt_payload_id=%s,error_code=%s,revision=revision+1,updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s RETURNING *",
                        (
                            state,
                            Jsonb(receipt),
                            receipt_payload_id,
                            receipt.get("error_code"),
                            self.store.bot_id,
                            permit.intent_id,
                            permit.part_index,
                        ),
                    )
                ).fetchone()
            await self.store._event(
                conn,
                "delivery.observed",
                permit.attempt_id,
                {"attempt_id": permit.attempt_id, "status": status, "part_state": part["state"]},
            )
            if (
                status == "SENT"
                and isinstance(receipt.get("message_id"), str)
                and receipt["message_id"]
            ):
                intent = await self._intent(conn, permit.intent_id)
                await self.store._event(
                    conn,
                    "delivery.echo_resolve",
                    permit.attempt_id,
                    {
                        "intent_id": permit.intent_id,
                        "part_index": permit.part_index,
                        "attempt_id": permit.attempt_id,
                        "binding_id": intent["binding_id"],
                        "conversation_id": intent["conversation_id"],
                        "send_lane": intent["send_lane"],
                        "platform_message_id": receipt["message_id"],
                        "channel_kind": intent["reply_route"]["channel_kind"],
                        "channel_id": intent["reply_route"]["channel_id"],
                    },
                )
            return self._public_part(part)

    @staticmethod
    def _public_part(part):
        return {
            key: part[key]
            for key in (
                "intent_id",
                "part_index",
                "state",
                "revision",
                "attempt_id",
                "attempt_count",
                "error_code",
                "receipt",
            )
        }

    async def cancel_root_in(self, conn, lease, root_operation_id: str) -> dict:
        """Called by the already-guarded Run cancellation transaction."""
        self.store._scope(lease)
        root = await self._root(conn, root_operation_id)
        if not root["effects_cancelled"]:
            await conn.execute(
                "UPDATE public.bot_delivery_roots SET effects_cancelled=true,cancel_epoch=cancel_epoch+1 WHERE bot_id=%s AND root_operation_id=%s",
                (self.store.bot_id, root_operation_id),
            )
        changed = await (
            await conn.execute(
                "UPDATE public.bot_send_parts p SET state='CANCELLED',revision=revision+1,error_code='CANCELLED',updated_at=clock_timestamp() FROM public.bot_send_intents i WHERE i.bot_id=p.bot_id AND i.intent_id=p.intent_id AND i.bot_id=%s AND i.root_operation_id=%s AND p.state IN ('PENDING','CLAIMED') RETURNING p.intent_id,p.part_index",
                (self.store.bot_id, root_operation_id),
            )
        ).fetchall()
        inflight = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_send_parts p JOIN public.bot_send_intents i USING(bot_id,intent_id) WHERE i.bot_id=%s AND i.root_operation_id=%s AND p.state IN ('DISPATCHING','UNKNOWN')",
                (self.store.bot_id, root_operation_id),
            )
        ).fetchone()
        await self._final_host(conn, lease)
        await self.store._event(
            conn,
            "delivery.cancelled",
            root_operation_id,
            {"cancelled_parts": len(changed), "inflight_or_unknown": inflight["n"]},
        )
        await self._final_host(conn, lease)
        return {"cancelled_parts": len(changed), "inflight_or_unknown": inflight["n"]}

    async def cancel_root(self, lease, root_operation_id: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            return await self.cancel_root_in(conn, lease, root_operation_id)

    async def recover(self, lease: HostLease) -> dict:
        """Recover only verified expired claims/attempts or replaced host owners."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            rows = await (
                await conn.execute(
                    "SELECT p.* FROM public.bot_send_parts p LEFT JOIN public.bot_send_attempts a ON a.bot_id=p.bot_id AND a.attempt_id=p.attempt_id WHERE p.bot_id=%s AND ((p.state='CLAIMED' AND (p.claim_until<=clock_timestamp() OR p.host_incarnation<>%s OR p.host_owner_epoch<>%s)) OR (p.state='DISPATCHING' AND (a.deadline_at<=clock_timestamp() OR p.host_incarnation<>%s OR p.host_owner_epoch<>%s))) ORDER BY p.intent_id,p.part_index FOR UPDATE OF p",
                    (
                        self.store.bot_id,
                        lease.incarnation,
                        lease.owner_epoch,
                        lease.incarnation,
                        lease.owner_epoch,
                    ),
                )
            ).fetchall()
            result = {"requeued": 0, "unknown": 0}
            for part in rows:
                state = "PENDING" if part["state"] == "CLAIMED" else "UNKNOWN"
                await conn.execute(
                    "UPDATE public.bot_send_parts SET state=%s,revision=revision+1,error_code=%s,updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s",
                    (
                        state,
                        "CLAIM_EXPIRED" if state == "PENDING" else "RECEIPT_LOST",
                        self.store.bot_id,
                        part["intent_id"],
                        part["part_index"],
                    ),
                )
                if state == "UNKNOWN":
                    await conn.execute(
                        "UPDATE public.bot_send_attempts SET state='UNKNOWN',observed_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s AND state='DISPATCHING'",
                        (self.store.bot_id, part["attempt_id"]),
                    )
                result["requeued" if state == "PENDING" else "unknown"] += 1
            if rows:
                await self.store._event(conn, "delivery.recovered", None, result)
            await self._final_host(conn, lease)
            return result

    @asynccontextmanager
    async def _transaction(self, existing=None):
        if existing is not None:
            yield existing
        else:
            async with self.store.pool.connection() as conn, conn.transaction():
                yield conn

    async def reconcile_in(
        self, conn, lease: HostLease, intent_id: str, part_index: int, **kwargs
    ) -> dict:
        """Join the target Web/Shell ticket guard and operation receipt transaction."""
        return await self.reconcile(lease, intent_id, part_index, _conn=conn, **kwargs)

    async def reconcile(
        self,
        lease: HostLease,
        intent_id: str,
        part_index: int,
        *,
        operation_id: str,
        decision: str,
        expected_revision: int,
        reason: str,
        input_parent_ids: tuple[str, ...] = (),
        _conn=None,
    ) -> dict:
        """Trusted management path, after its current principal authorization.

        NOT_SENT is an explicit operator assertion permitting the *same semantic
        part* to get a fresh attempt. SKIP releases order but preserves UNKNOWN
        in the old attempt; it never asserts the platform did not receive it.
        """
        if decision not in ("SENT", "NOT_SENT", "SKIP"):
            raise ForestError("INVALID_DELIVERY", "Invalid reconciliation decision", 422)
        _text(operation_id, "operation_id")
        _text(reason, "reason", 2000)
        input_parent_ids = tuple(input_parent_ids)
        _, digest = _freeze([intent_id, part_index, decision, expected_revision, reason])
        async with self._transaction(_conn) as conn:
            intent = await self._intent(conn, intent_id)
            # Reconciliation remains possible while business is paused. The
            # Binding cannot be redirected or enabled through this operation.
            await self.store._host(conn, lease)
            root = await self._root(conn, intent["root_operation_id"])
            old = await (
                await conn.execute(
                    "SELECT * FROM public.bot_delivery_resolutions WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if old:
                if old["input_digest"] != digest:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Reconciliation operation input changed"
                    )
                await self._final_host(conn, lease)
                return old["result_metadata"]
            part = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s AND part_index=%s FOR UPDATE",
                    (self.store.bot_id, intent_id, part_index),
                )
            ).fetchone()
            if part is None or part["revision"] != expected_revision:
                raise ForestError("REVISION_CONFLICT", "Delivery changed before reconciliation")
            if part["state"] != "UNKNOWN":
                raise ForestError(
                    "DELIVERY_NOT_UNKNOWN", "Only unknown results require manual reconciliation"
                )
            if decision == "NOT_SENT" and root["effects_cancelled"]:
                raise ForestError("CANCELLED", "Cancelled effects cannot be replayed")
            clock = await self.contents.payloads.lock_in(conn)
            if decision == "NOT_SENT":
                await self.contents.payloads.require_available_in(
                    conn, (part["payload_id"],), expected_deletion_epoch=clock["deletion_epoch"]
                )
            state = {"SENT": "SENT", "NOT_SENT": "PENDING", "SKIP": "SKIPPED"}[decision]
            part = await (
                await conn.execute(
                    "UPDATE public.bot_send_parts SET state=%s,revision=revision+1,error_code=%s,available_at=clock_timestamp(),updated_at=clock_timestamp() WHERE bot_id=%s AND intent_id=%s AND part_index=%s RETURNING *",
                    (state, "MANUAL_" + decision, self.store.bot_id, intent_id, part_index),
                )
            ).fetchone()
            result = self._public_part(part)
            resolution_payload_id = await self.contents.retain_observation_in(
                conn,
                owner_id="resolution:" + _freeze(operation_id)[1],
                value={"reason": reason, "result": result},
                parent=part["payload_id"],
                parents=input_parent_ids,
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            await conn.execute(
                "INSERT INTO public.bot_delivery_resolutions(bot_id,operation_id,intent_id,part_index,input_digest,decision,payload_id,result_metadata) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    operation_id,
                    intent_id,
                    part_index,
                    digest,
                    decision,
                    resolution_payload_id,
                    Jsonb(result),
                ),
            )
            await self.store._event(
                conn,
                "delivery.reconciled",
                operation_id,
                {"intent_id": intent_id, "part_index": part_index, "decision": decision},
            )
            await self._final_host(conn, lease)
            return result

    async def list(
        self, lease: HostLease, *, limit: int = 100, states: list[str] | None = None
    ) -> list[dict]:
        if not isinstance(limit, int) or not 1 <= limit <= 200:
            raise ValueError("Delivery page size must be 1 to 200")
        states = states or list(ACTIVE + TERMINAL)
        if any(state not in ACTIVE + TERMINAL for state in states):
            raise ValueError("Unknown delivery state")
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            rows = await (
                await conn.execute(
                    "SELECT i.*,p.part_index,p.state,p.revision,p.attempt_id,p.attempt_count,p.error_code,p.receipt FROM public.bot_send_intents i JOIN public.bot_send_parts p USING(bot_id,intent_id) WHERE i.bot_id=%s AND p.state=ANY(%s) ORDER BY i.created_at DESC,p.part_index LIMIT %s",
                    (self.store.bot_id, states, limit),
                )
            ).fetchall()
            await self._final_host(conn, lease)
            return [
                {
                    **self._public_intent(row),
                    **self._public_part(row),
                    "binding_id": row["binding_id"],
                    "route": row["reply_route"],
                }
                for row in rows
            ]

    async def find_echo(
        self,
        lease: HostLease,
        binding_id: str,
        platform_message_id: str,
        channel_kind: str,
        channel_id: str,
    ) -> dict | None:
        """Find a confirmed self-output by its fixed account and actual target.

        The ingress gateway separately verifies the event's trusted self author.
        An absent mapping is not proof an unknown send did not reach a platform.
        """
        _text(platform_message_id, "platform_message_id")
        _text(channel_id, "channel_id")
        if channel_kind not in ("group", "private"):
            raise ForestError("INVALID_DELIVERY", "Invalid echo channel", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.bindings._guard(conn, lease, binding_id, data=False, require_receive=True)
            row = await (
                await conn.execute(
                    "SELECT i.*,a.part_index,a.attempt_id FROM public.bot_send_attempts a JOIN public.bot_send_intents i USING(bot_id,intent_id) WHERE a.bot_id=%s AND i.binding_id=%s AND a.state='SENT' AND a.receipt->>'message_id'=%s AND i.reply_route->>'channel_kind'=%s AND i.reply_route->>'channel_id'=%s ORDER BY a.observed_at LIMIT 1",
                    (self.store.bot_id, binding_id, platform_message_id, channel_kind, channel_id),
                )
            ).fetchone()
            await self._final_host(conn, lease)
            if row is None:
                return None
            return {
                **self._public_intent(row),
                "binding_id": binding_id,
                "part_index": row["part_index"],
                "attempt_id": row["attempt_id"],
                "platform_message_id": platform_message_id,
                "origin": "control_output" if row["send_lane"] == "CONTROL" else "chat_output",
            }

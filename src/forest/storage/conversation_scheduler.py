"""Clean repository for conversation ordering, trigger cuts and Context references."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace
from forest.storage.payload_authority import PayloadSource

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_BOUNDARIES = frozenset({"IDLE", "MODEL_SAFE_POINT", "SESSION_END"})
_DECISIONS = frozenset({"NO_TRIGGER", "TRIGGER", "CONTROL_COMMAND", "ROUTED", "FAILED"})
_LOCK_DOMAIN = b"forest.conversation.scheduler/1\0"


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("CONVERSATION_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str):
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("CONVERSATION_INPUT_INVALID", f"Invalid {field}", 422)
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
        raise ForestError(
            "CONVERSATION_INPUT_INVALID", "Conversation identity is not canonical", 422
        ) from exc


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value, field: str, *, optional: bool = False):
    if value is None and optional:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ForestError("CONVERSATION_INPUT_INVALID", f"{field} needs a timezone", 422)
    return value.astimezone(timezone.utc)


def _operation_key(bot_id: str, operation_id: str) -> int:
    value = hashlib.sha256(
        _LOCK_DOMAIN + bot_id.encode() + b"\0" + operation_id.encode()
    ).digest()
    return int.from_bytes(value[:8], "big", signed=True)


@dataclass(frozen=True)
class SchedulerPayloadRef:
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


@dataclass(frozen=True)
class ConversationActor:
    conversation_id: str
    owner_id: str
    epoch: int
    expires_at: datetime


@dataclass(frozen=True)
class ContextViewPolicy:
    """Deterministic local-only policy for the persistent canonical Context view."""

    high_watermark_units: int = 512
    retain_units: int = 256
    policy_revision: str = "context-view-unit-window-v1"

    def validate(self) -> None:
        if (
            type(self.high_watermark_units) is not int
            or type(self.retain_units) is not int
            or not 2 <= self.high_watermark_units <= 1_000_000
            or not 1 <= self.retain_units < self.high_watermark_units
        ):
            raise ForestError(
                "CONTEXT_VIEW_POLICY_INVALID", "Invalid Context view unit bounds", 422
            )
        _bounded(self.policy_revision, "context_view_policy.policy_revision", 256)


@dataclass(frozen=True)
class SchedulerContextFact:
    unit_id: str
    kind: str
    source_component_id: str
    source_kind: str
    source_id: str
    payload: SchedulerPayloadRef
    author: SchedulerPayloadRef | None = None
    author_subject_ref: str | None = None

    def validate(self) -> None:
        _bounded(self.unit_id, "fact.unit_id", 256)
        if self.kind not in {"AGENT_STEP", "REPLY", "CONTROL", "EFFECT"}:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid Context fact kind", 422)
        _bounded(self.source_component_id, "fact.source_component_id", 128)
        _bounded(self.source_kind, "fact.source_kind", 128)
        _bounded(self.source_id, "fact.source_id", 512)
        self.payload.validate("fact.payload")
        if (self.author is None) != (self.author_subject_ref is None):
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Context fact author identity is incomplete", 422
            )
        if self.author is not None:
            self.author.validate("fact.author")
            _bounded(self.author_subject_ref, "fact.author_subject_ref", 256)

    def canonical(self) -> dict:
        return {
            "unit_id": self.unit_id,
            "kind": self.kind,
            "source_component_id": self.source_component_id,
            "source_kind": self.source_kind,
            "source_id": self.source_id,
            "payload": vars(self.payload),
            "author": vars(self.author) if self.author is not None else None,
            "author_subject_ref": self.author_subject_ref,
        }


class ConversationSchedulerRepository:
    """Own one serial lane per conversation without owning Agent execution state."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier: Callable[..., Awaitable[object]] | None = None,
        payloads=None,
        context_view_policy: ContextViewPolicy | None = None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.conversation.scheduler")
        self.payload_verifier = payload_verifier
        self.payloads = payloads or getattr(payload_verifier, "__self__", None)
        self.context_view_policy = context_view_policy or ContextViewPolicy()
        self.context_view_policy.validate()

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _lock_operation(self, conn, operation_id: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (_operation_key(self.store.bot_id, operation_id),),
        )

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    async def _verify_payload(self, conn, reference: SchedulerPayloadRef) -> None:
        if self.payload_verifier is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "Conversation payload verifier is unavailable", 503
            )
        await self.payload_verifier(
            conn,
            payload_id=reference.payload_id,
            digest=reference.digest,
            owner_component_id=reference.owner_component_id,
            owner_kind=reference.owner_kind,
            owner_id=reference.owner_id,
        )

    async def _lane(self, conn, conversation_id: str, *, update: bool = False):
        row = await (
            await conn.execute(
                sql.SQL("SELECT *,clock_timestamp() AS database_now FROM {} "
                        "WHERE bot_id=%s AND conversation_id=%s {}").format(
                    self._table("conversation_lanes"),
                    sql.SQL("FOR UPDATE" if update else ""),
                ),
                (self.store.bot_id, conversation_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("CONVERSATION_NOT_FOUND", "Conversation lane does not exist", 404)
        return row

    @staticmethod
    def _lane_identity(
        *, source_component_id, binding_ref, platform, channel_kind, channel_ref, thread_ref
    ) -> tuple:
        return source_component_id, binding_ref, platform, channel_kind, channel_ref, thread_ref

    async def admit_message(
        self,
        *,
        operation_id: str,
        conversation_id: str,
        source_component_id: str,
        binding_ref: str,
        platform: str,
        channel_kind: str,
        channel_ref: str,
        thread_ref: str | None,
        event_id: str,
        source_event_ref: str,
        event_kind: str,
        platform_message_ref: str | None,
        author_subject_ref: str,
        content: SchedulerPayloadRef,
        author: SchedulerPayloadRef,
        received_at: datetime,
        classification_deadline: datetime,
        sent_at: datetime | None = None,
        context_disposition: str = "MATERIALIZE",
        _connection=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (conversation_id, "conversation_id", 256),
            (source_component_id, "source_component_id", 128),
            (binding_ref, "binding_ref", 256),
            (platform, "platform", 64),
            (channel_kind, "channel_kind", 16),
            (channel_ref, "channel_ref", 256),
            (event_id, "event_id", 256),
            (source_event_ref, "source_event_ref", 512),
            (event_kind, "event_kind", 128),
            (author_subject_ref, "author_subject_ref", 256),
        ):
            _bounded(value, field, limit)
        _bounded(thread_ref, "thread_ref", 256, optional=True)
        _bounded(platform_message_ref, "platform_message_ref", 512, optional=True)
        if channel_kind not in {"GROUP", "PRIVATE", "SYSTEM"}:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid channel_kind", 422)
        content.validate("content")
        author.validate("author")
        received_at = _timestamp(received_at, "received_at")
        sent_at = _timestamp(sent_at, "sent_at", optional=True)
        classification_deadline = _timestamp(
            classification_deadline, "classification_deadline"
        )
        if not received_at < classification_deadline <= received_at + timedelta(minutes=15):
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Classification deadline is outside its bound", 422
            )
        if context_disposition not in {"MATERIALIZE", "PLACEHOLDER"}:
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Invalid context_disposition", 422
            )
        lane_identity = self._lane_identity(
            source_component_id=source_component_id,
            binding_ref=binding_ref,
            platform=platform,
            channel_kind=channel_kind,
            channel_ref=channel_ref,
            thread_ref=thread_ref,
        )
        lane_digest = _digest(lane_identity)
        input_digest = _digest(
            {
                "conversation_id": conversation_id,
                "lane": lane_identity,
                "event_id": event_id,
                "source_event_ref": source_event_ref,
                "event_kind": event_kind,
                "platform_message_ref": platform_message_ref,
                "author_subject_ref": author_subject_ref,
                "content": vars(content),
                "author": vars(author),
                "sent_at": sent_at.isoformat() if sent_at else None,
                "context_disposition": context_disposition,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock_operation(conn, operation_id)
            operation = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND admission_operation_id=%s").format(
                        self._table("inbox_messages")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if operation is not None:
                if operation["input_digest"] != input_digest:
                    raise ForestError(
                        "MESSAGE_OPERATION_CONFLICT", "Admission operation input differs", 409
                    )
                return {**dict(operation), "duplicate": True}
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "AND event_id=%s").format(self._table("inbox_messages")),
                    (self.store.bot_id, conversation_id, event_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError("MESSAGE_ID_CONFLICT", "Event identity differs", 409)
                return {**dict(existing), "duplicate": True}
            # Payload authority precedes the business lane for every new admission.
            for reference in sorted((content, author), key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,conversation_id,conversation_key_digest,"
                    "source_component_id,binding_ref,platform,channel_kind,channel_ref,thread_ref) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                ).format(self._table("conversation_lanes")),
                (
                    self.store.bot_id,
                    conversation_id,
                    lane_digest,
                    source_component_id,
                    binding_ref,
                    platform,
                    channel_kind,
                    channel_ref,
                    thread_ref,
                ),
            )
            lane = await self._lane(conn, conversation_id, update=True)
            actual_lane = self._lane_identity(
                source_component_id=lane["source_component_id"],
                binding_ref=lane["binding_ref"],
                platform=lane["platform"],
                channel_kind=lane["channel_kind"],
                channel_ref=lane["channel_ref"],
                thread_ref=lane["thread_ref"],
            )
            if lane["conversation_key_digest"] != lane_digest or actual_lane != lane_identity:
                raise ForestError(
                    "CONVERSATION_IDENTITY_CONFLICT", "Conversation identity differs", 409
                )
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "AND event_id=%s").format(self._table("inbox_messages")),
                    (self.store.bot_id, conversation_id, event_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError("MESSAGE_ID_CONFLICT", "Event identity differs", 409)
                return {**dict(existing), "duplicate": True}
            ingress_seq = lane["next_ingress_seq"]
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,conversation_id,event_id,ingress_seq,"
                        "admission_operation_id,input_digest,source_component_id,source_event_ref,"
                        "event_kind,platform_message_ref,author_subject_ref,content_payload_id,"
                        "content_payload_digest,content_owner_component_id,content_owner_kind,"
                        "content_owner_id,author_payload_id,author_payload_digest,"
                        "author_owner_component_id,author_owner_kind,author_owner_id,"
                        "context_disposition,sent_at,received_at,classification_deadline) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("inbox_messages")),
                    (
                        self.store.bot_id,
                        conversation_id,
                        event_id,
                        ingress_seq,
                        operation_id,
                        input_digest,
                        source_component_id,
                        source_event_ref,
                        event_kind,
                        platform_message_ref,
                        author_subject_ref,
                        content.payload_id,
                        content.digest,
                        content.owner_component_id,
                        content.owner_kind,
                        content.owner_id,
                        author.payload_id,
                        author.digest,
                        author.owner_component_id,
                        author.owner_kind,
                        author.owner_id,
                        context_disposition,
                        sent_at,
                        received_at,
                        classification_deadline,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET next_ingress_seq=next_ingress_seq+1,"
                        "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (self.store.bot_id, conversation_id),
            )
            return {**dict(row), "duplicate": False}

    async def admit_message_in(self, conn, **values) -> dict:
        """Participate in a caller-owned Bot transaction without changing ownership."""

        values.pop("admission_lane", None)
        return await self.admit_message(_connection=conn, **values)

    async def complete_classification(
        self,
        *,
        operation_id: str,
        conversation_id: str,
        event_id: str,
        decision: str,
        classifier_component_id: str,
        classifier_generation_id: str,
        rule_revision: str | None = None,
        reason_code: str | None = None,
        route_ref: str | None = None,
        trigger_id: str | None = None,
        principal_ref: str | None = None,
        _connection=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (conversation_id, "conversation_id", 256),
            (event_id, "event_id", 256),
            (classifier_component_id, "classifier_component_id", 128),
            (classifier_generation_id, "classifier_generation_id", 256),
        ):
            _bounded(value, field, limit)
        if decision not in _DECISIONS:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid trigger decision", 422)
        _bounded(rule_revision, "rule_revision", 256, optional=True)
        _bounded(reason_code, "reason_code", 128, optional=True)
        _bounded(route_ref, "route_ref", 512, optional=True)
        if decision == "TRIGGER":
            _bounded(trigger_id, "trigger_id", 256)
            _bounded(principal_ref, "principal_ref", 256)
        elif trigger_id is not None or principal_ref is not None:
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Only TRIGGER may create a trigger record", 422
            )
        input_digest = _digest(
            {
                "conversation_id": conversation_id,
                "event_id": event_id,
                "decision": decision,
                "classifier_component_id": classifier_component_id,
                "classifier_generation_id": classifier_generation_id,
                "rule_revision": rule_revision,
                "reason_code": reason_code,
                "route_ref": route_ref,
                "trigger_id": trigger_id,
                "principal_ref": principal_ref,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock_operation(conn, operation_id)
            lane = await self._lane(conn, conversation_id, update=True)
            other = await (
                await conn.execute(
                    sql.SQL("SELECT conversation_id,event_id,classification_input_digest FROM {} "
                            "WHERE bot_id=%s AND classification_operation_id=%s").format(
                        self._table("inbox_messages")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if other is not None and (
                other["conversation_id"] != conversation_id
                or other["event_id"] != event_id
                or other["classification_input_digest"] != input_digest
            ):
                raise ForestError(
                    "CLASSIFICATION_OPERATION_CONFLICT", "Classification operation input differs", 409
                )
            message = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "AND event_id=%s FOR UPDATE").format(self._table("inbox_messages")),
                    (self.store.bot_id, conversation_id, event_id),
                )
            ).fetchone()
            if message is None:
                raise ForestError("MESSAGE_NOT_FOUND", "Inbox message does not exist", 404)
            if message["classification_state"] == "FINAL":
                if message["classification_input_digest"] != input_digest:
                    raise ForestError(
                        "CLASSIFICATION_CONFLICT", "Message classification differs", 409
                    )
                return {
                    "event_id": event_id,
                    "ingress_seq": message["ingress_seq"],
                    "decision": message["trigger_decision"],
                    "classified_through_seq": lane["classified_through_seq"],
                    "duplicate": True,
                }
            if (
                lane["database_now"] >= message["classification_deadline"]
                and classifier_component_id != "conversation.scheduler.deadline"
            ):
                raise ForestError(
                    "CLASSIFICATION_DEADLINE_EXCEEDED",
                    "Classification deadline has passed",
                    409,
                )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET classification_state='FINAL',trigger_decision=%s,"
                    "classification_operation_id=%s,classification_input_digest=%s,"
                    "classifier_component_id=%s,classifier_generation_id=%s,rule_revision=%s,"
                    "reason_code=%s,route_ref=%s,classified_at=clock_timestamp() "
                    "WHERE bot_id=%s AND conversation_id=%s AND event_id=%s"
                ).format(self._table("inbox_messages")),
                (
                    decision,
                    operation_id,
                    input_digest,
                    classifier_component_id,
                    classifier_generation_id,
                    rule_revision,
                    reason_code,
                    route_ref,
                    self.store.bot_id,
                    conversation_id,
                    event_id,
                ),
            )
            if decision == "TRIGGER":
                trigger_digest = _digest(
                    {
                        "conversation_id": conversation_id,
                        "event_id": event_id,
                        "ingress_seq": message["ingress_seq"],
                        "principal_ref": principal_ref,
                    }
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,conversation_id,trigger_id,event_id,ingress_seq,"
                        "principal_ref,input_digest) VALUES (%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT DO NOTHING"
                    ).format(self._table("conversation_triggers")),
                    (
                        self.store.bot_id,
                        conversation_id,
                        trigger_id,
                        event_id,
                        message["ingress_seq"],
                        principal_ref,
                        trigger_digest,
                    ),
                )
                stored_trigger = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND trigger_id=%s").format(
                            self._table("conversation_triggers")
                        ),
                        (self.store.bot_id, trigger_id),
                    )
                ).fetchone()
                if (
                    stored_trigger is None
                    or stored_trigger["conversation_id"] != conversation_id
                    or stored_trigger["event_id"] != event_id
                    or stored_trigger["input_digest"] != trigger_digest
                ):
                    raise ForestError("TRIGGER_ID_CONFLICT", "Trigger identity differs", 409)
            rows = await (
                await conn.execute(
                    sql.SQL("SELECT ingress_seq,classification_state FROM {} WHERE bot_id=%s "
                            "AND conversation_id=%s AND ingress_seq>%s ORDER BY ingress_seq").format(
                        self._table("inbox_messages")
                    ),
                    (self.store.bot_id, conversation_id, lane["classified_through_seq"]),
                )
            ).fetchall()
            classified = lane["classified_through_seq"]
            expected = classified + 1
            for row in rows:
                if row["ingress_seq"] != expected:
                    raise ForestError(
                        "CONVERSATION_SEQUENCE_CORRUPT", "Inbox sequence has a gap", 500
                    )
                if row["classification_state"] != "FINAL":
                    break
                classified = row["ingress_seq"]
                expected += 1
            await conn.execute(
                sql.SQL("UPDATE {} SET classified_through_seq=%s,"
                        "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (classified, self.store.bot_id, conversation_id),
            )
            return {
                "event_id": event_id,
                "ingress_seq": message["ingress_seq"],
                "decision": decision,
                "classified_through_seq": classified,
                "duplicate": False,
            }

    async def complete_classification_in(self, conn, **values) -> dict:
        """Finalize classification inside a caller-owned UnitOfWork."""

        values.pop("admission_lane", None)
        return await self.complete_classification(_connection=conn, **values)

    async def expire_classifications(self, *, limit: int = 128) -> int:
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid expiration limit", 422)
        async with self.store.pool.connection() as conn:
            due = await (
                await conn.execute(
                    sql.SQL("SELECT conversation_id,event_id FROM {} WHERE bot_id=%s "
                            "AND classification_state='PENDING' "
                            "AND classification_deadline<=clock_timestamp() "
                            "ORDER BY classification_deadline,conversation_id,ingress_seq "
                            "LIMIT %s").format(self._table("inbox_messages")),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
        expired = 0
        for item in due:
            operation_id = "classification-expiry:" + hashlib.sha256(
                _canonical(
                    [self.store.bot_id, item["conversation_id"], item["event_id"]]
                )
            ).hexdigest()
            try:
                result = await self.complete_classification(
                    operation_id=operation_id,
                    conversation_id=item["conversation_id"],
                    event_id=item["event_id"],
                    decision="FAILED",
                    classifier_component_id="conversation.scheduler.deadline",
                    classifier_generation_id="builtin/1",
                    rule_revision="classification-deadline/1",
                    reason_code="classification_deadline_exceeded",
                )
            except ForestError as exc:
                if exc.code == "CLASSIFICATION_CONFLICT":
                    continue
                raise
            if not result["duplicate"]:
                expired += 1
        return expired

    async def acquire_actor(
        self,
        conversation_id: str,
        owner_id: str,
        *,
        lease_seconds: int = 30,
        _connection=None,
    ) -> ConversationActor:
        _bounded(conversation_id, "conversation_id", 256)
        _bounded(owner_id, "owner_id", 256)
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid actor lease", 422)
        async with self._transaction(_connection) as conn:
            lane = await self._lane(conn, conversation_id, update=True)
            alive = (
                lane["actor_owner_id"] is not None
                and lane["actor_expires_at"] > lane["database_now"]
            )
            if lane["resume_not_before"] is not None and (
                lane["resume_not_before"] > lane["database_now"]
            ):
                raise ForestError(
                    "CONVERSATION_DEFERRED",
                    "Conversation is waiting for an external durable decision",
                    409,
                )
            if alive and lane["actor_owner_id"] != owner_id:
                raise ForestError("CONVERSATION_ACTOR_BUSY", "Conversation actor is busy", 409)
            epoch = lane["actor_epoch"] if alive else lane["actor_epoch"] + 1
            expires = lane["database_now"] + timedelta(seconds=lease_seconds)
            await conn.execute(
                sql.SQL("UPDATE {} SET actor_owner_id=%s,actor_epoch=%s,actor_expires_at=%s,"
                        "resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (owner_id, epoch, expires, self.store.bot_id, conversation_id),
            )
            return ConversationActor(conversation_id, owner_id, epoch, expires)

    async def acquire_actor_in(self, conn, conversation_id: str, owner_id: str, **values):
        return await self.acquire_actor(
            conversation_id,
            owner_id,
            _connection=conn,
            **values,
        )

    async def _guard_actor(self, conn, actor: ConversationActor):
        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        lane = await self._lane(conn, actor.conversation_id, update=True)
        if (
            lane["actor_owner_id"] != actor.owner_id
            or lane["actor_epoch"] != actor.epoch
            or lane["actor_expires_at"] is None
            or lane["actor_expires_at"] <= lane["database_now"]
        ):
            raise ForestError("STALE_CONVERSATION_ACTOR", "Conversation actor is stale", 409)
        return lane

    async def renew_actor(
        self, actor: ConversationActor, *, lease_seconds: int = 30, _connection=None
    ) -> ConversationActor:
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid actor lease", 422)
        async with self._transaction(_connection) as conn:
            lane = await self._guard_actor(conn, actor)
            expires = lane["database_now"] + timedelta(seconds=lease_seconds)
            await conn.execute(
                sql.SQL("UPDATE {} SET actor_expires_at=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (expires, self.store.bot_id, actor.conversation_id),
            )
            return ConversationActor(actor.conversation_id, actor.owner_id, actor.epoch, expires)

    async def renew_actor_in(self, conn, actor: ConversationActor, **values):
        return await self.renew_actor(actor, _connection=conn, **values)

    async def release_actor(self, actor: ConversationActor, *, _connection=None) -> None:
        async with self._transaction(_connection) as conn:
            await self._guard_actor(conn, actor)
            await conn.execute(
                sql.SQL("UPDATE {} SET actor_owner_id=NULL,actor_expires_at=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (self.store.bot_id, actor.conversation_id),
            )

    async def release_actor_in(self, conn, actor: ConversationActor) -> None:
        await self.release_actor(actor, _connection=conn)

    async def defer_actor(
        self,
        actor: ConversationActor,
        *,
        delay_seconds: float,
        reason_code: str,
        reason_ref: str | None = None,
        _connection=None,
    ) -> dict:
        if (
            not isinstance(delay_seconds, (int, float))
            or isinstance(delay_seconds, bool)
            or not 0.1 <= delay_seconds <= 3600
        ):
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid defer delay", 422)
        _bounded(reason_code, "reason_code", 128)
        _bounded(reason_ref, "reason_ref", 256, optional=True)
        async with self._transaction(_connection) as conn:
            lane = await self._guard_actor(conn, actor)
            if lane["active_session_ref"] is None:
                raise ForestError(
                    "CONVERSATION_SESSION_REQUIRED",
                    "Only an active Agent Session can be deferred",
                    409,
                )
            deferred = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET resume_not_before=clock_timestamp()+(%s * interval '1 second'),"
                        "resume_reason_code=%s,resume_reason_ref=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND conversation_id=%s RETURNING *"
                    ).format(self._table("conversation_lanes")),
                    (
                        float(delay_seconds),
                        reason_code,
                        reason_ref,
                        self.store.bot_id,
                        actor.conversation_id,
                    ),
                )
            ).fetchone()
            return dict(deferred)

    async def defer_actor_in(self, conn, actor: ConversationActor, **values) -> dict:
        return await self.defer_actor(actor, _connection=conn, **values)

    async def resume_deferred(
        self,
        conversation_id: str,
        *,
        reason_code: str,
        reason_ref: str,
        _connection=None,
    ) -> dict:
        _bounded(conversation_id, "conversation_id", 256)
        _bounded(reason_code, "reason_code", 128)
        _bounded(reason_ref, "reason_ref", 256)
        async with self._transaction(_connection) as conn:
            lane = await self._lane(conn, conversation_id, update=True)
            matched = (
                lane["resume_reason_code"] == reason_code
                and lane["resume_reason_ref"] == reason_ref
            )
            if not matched:
                return {**dict(lane), "resumed": False}
            resumed = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET resume_not_before=NULL,resume_reason_code=NULL,"
                        "resume_reason_ref=NULL,mailbox_revision=mailbox_revision+1,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s "
                        "RETURNING *"
                    ).format(self._table("conversation_lanes")),
                    (self.store.bot_id, conversation_id),
                )
            ).fetchone()
            return {**dict(resumed), "resumed": True}

    async def resume_deferred_in(self, conn, conversation_id: str, **values) -> dict:
        return await self.resume_deferred(conversation_id, _connection=conn, **values)

    async def wake_cancel_request(
        self,
        conversation_id: str,
        *,
        run_ref: str,
        cancellation_operation_id: str,
        _connection=None,
    ) -> dict:
        """Wake one exact active Run after its durable cancellation intent commits."""

        _bounded(conversation_id, "conversation_id", 256)
        _bounded(run_ref, "run_ref", 256)
        _bounded(cancellation_operation_id, "cancellation_operation_id", 256)
        async with self._transaction(_connection) as conn:
            lane = await self._lane(conn, conversation_id, update=True)
            if lane["active_run_ref"] != run_ref:
                return {**dict(lane), "resumed": False}
            resumed = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET resume_not_before=NULL,resume_reason_code=NULL,"
                        "resume_reason_ref=NULL,mailbox_revision=mailbox_revision+1,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s "
                        "AND active_run_ref=%s RETURNING *"
                    ).format(self._table("conversation_lanes")),
                    (self.store.bot_id, conversation_id, run_ref),
                )
            ).fetchone()
            return {
                **dict(resumed),
                "resumed": True,
                "cancellation_operation_id": cancellation_operation_id,
            }

    async def claimable_conversations(self, *, limit: int = 64, _connection=None) -> tuple[str, ...]:
        """Return bounded wake candidates; actor acquisition remains the ownership point."""

        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid claim candidate limit", 422)
        async with self._transaction(_connection) as conn:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT l.conversation_id FROM {} l WHERE l.bot_id=%s "
                        "AND (l.resume_not_before IS NULL "
                        "OR l.resume_not_before<=clock_timestamp()) AND ("
                        "l.active_session_ref IS NOT NULL OR "
                        "l.context_applied_seq < l.classified_through_seq OR EXISTS ("
                        "SELECT 1 FROM {} t WHERE t.bot_id=l.bot_id "
                        "AND t.conversation_id=l.conversation_id AND t.status='PENDING' "
                        "AND t.ingress_seq<=l.classified_through_seq)) "
                        "ORDER BY l.updated_at,l.conversation_id LIMIT %s"
                    ).format(
                        self._table("conversation_lanes"),
                        self._table("conversation_triggers"),
                    ),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            return tuple(row["conversation_id"] for row in rows)

    async def claimable_conversations_in(self, conn, *, limit: int = 64):
        return await self.claimable_conversations(limit=limit, _connection=conn)

    async def execution_state(self, actor: ConversationActor, *, _connection=None) -> dict:
        """Load the durable Session/Run fence held by one acquired group actor.

        An active fence is itself recoverable work, even when every currently
        classified message has already entered Context.  The caller uses this
        projection to resume the same Agent chain after process loss instead
        of attempting an ``IDLE`` selection with fresh identities.
        """

        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        async with self._transaction(_connection) as conn:
            lane = await self._guard_actor(conn, actor)
            claimed = []
            if lane["active_session_ref"] is not None:
                claimed = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT trigger_id,principal_ref,ingress_seq FROM {} "
                            "WHERE bot_id=%s AND conversation_id=%s AND status='CLAIMED' "
                            "AND claimed_session_ref=%s AND claimed_run_ref=%s "
                            "ORDER BY ingress_seq,trigger_id"
                        ).format(self._table("conversation_triggers")),
                        (
                            self.store.bot_id,
                            actor.conversation_id,
                            lane["active_session_ref"],
                            lane["active_run_ref"],
                        ),
                    )
                ).fetchall()
                batch = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT operation_id FROM {} WHERE bot_id=%s "
                            "AND conversation_id=%s AND run_ref=%s "
                            "ORDER BY created_at DESC,input_batch_id DESC LIMIT 1"
                        ).format(self._table("input_batches")),
                        (
                            self.store.bot_id,
                            actor.conversation_id,
                            lane["active_run_ref"],
                        ),
                    )
                ).fetchone()
                active_input = None
                if batch is not None:
                    receipt = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s"
                            ).format(self._table("selection_operations")),
                            (self.store.bot_id, batch["operation_id"]),
                        )
                    ).fetchone()
                    if receipt is None:
                        raise ForestError(
                            "CONVERSATION_SEQUENCE_CORRUPT",
                            "Active Run has no input selection receipt",
                            500,
                        )
                    active_input = await self._selection_result(conn, receipt)
            else:
                active_input = None
            return {
                "conversation_id": actor.conversation_id,
                "active": lane["active_session_ref"] is not None,
                "session_ref": lane["active_session_ref"],
                "run_ref": lane["active_run_ref"],
                "context_revision": lane["context_revision"],
                "context_view_revision": lane["context_view_revision"],
                "context_applied_seq": lane["context_applied_seq"],
                "classified_through_seq": lane["classified_through_seq"],
                "mailbox_revision": lane["mailbox_revision"],
                "claimed_triggers": [dict(row) for row in claimed],
                "active_input": active_input,
            }

    async def execution_state_in(self, conn, actor: ConversationActor) -> dict:
        return await self.execution_state(actor, _connection=conn)

    async def _selection_result(self, conn, receipt) -> dict:
        result = {
            "status": receipt["result_status"],
            "operation_id": receipt["operation_id"],
            "conversation_id": receipt["conversation_id"],
            "classified_through_seq": receipt["observed_classified_through_seq"],
            "selected_input_end_seq": receipt["selected_input_end_seq"],
            "mailbox_revision": receipt["observed_mailbox_revision"],
        }
        if receipt["input_batch_id"] is None:
            return result
        batch = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND input_batch_id=%s").format(
                    self._table("input_batches")
                ),
                (self.store.bot_id, receipt["input_batch_id"]),
            )
        ).fetchone()
        events = await (
            await conn.execute(
                sql.SQL("SELECT event_id,ingress_seq FROM {} WHERE bot_id=%s "
                        "AND input_batch_id=%s ORDER BY event_index").format(
                    self._table("input_batch_events")
                ),
                (self.store.bot_id, receipt["input_batch_id"]),
            )
        ).fetchall()
        triggers = await (
            await conn.execute(
                sql.SQL("SELECT m.trigger_id,t.principal_ref,t.ingress_seq FROM {} m JOIN {} t "
                        "ON t.bot_id=m.bot_id AND t.trigger_id=m.trigger_id "
                        "WHERE m.bot_id=%s AND m.input_batch_id=%s ORDER BY m.trigger_index").format(
                    self._table("input_batch_triggers"),
                    self._table("conversation_triggers"),
                ),
                (self.store.bot_id, receipt["input_batch_id"]),
            )
        ).fetchall()
        result = {
            **result,
            "status": "BATCH",
            **dict(batch),
            "event_ids": [row["event_id"] for row in events],
            "trigger_ids": [row["trigger_id"] for row in triggers],
            "triggers": [dict(row) for row in triggers],
        }
        compaction_operation_id = "context-compact:" + hashlib.sha256(
            _canonical([self.store.bot_id, receipt["operation_id"]])
        ).hexdigest()
        compaction = await (
            await conn.execute(
                sql.SQL(
                    "SELECT compaction_id FROM {} WHERE bot_id=%s AND operation_id=%s"
                ).format(self._table("context_compactions")),
                (self.store.bot_id, compaction_operation_id),
            )
        ).fetchone()
        if compaction is not None:
            archived_events = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT u.event_id FROM {} cu JOIN {} u ON u.bot_id=cu.bot_id "
                        "AND u.conversation_id=cu.conversation_id AND u.unit_id=cu.unit_id "
                        "WHERE cu.bot_id=%s AND cu.compaction_id=%s AND u.event_id IS NOT NULL "
                        "ORDER BY cu.archived_index"
                    ).format(
                        self._table("context_compaction_units"),
                        self._table("context_units"),
                    ),
                    (self.store.bot_id, compaction["compaction_id"]),
                )
            ).fetchall()
            result["memory_archive"] = {
                "compaction_ref": compaction["compaction_id"],
                "event_refs": tuple(row["event_id"] for row in archived_events),
            }
        return result

    async def _record_selection(
        self,
        conn,
        *,
        operation_id,
        input_digest,
        boundary,
        status,
        batch_id,
        lane,
        actor,
        selected_input_end_seq,
    ) -> dict:
        receipt = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,conversation_id,input_digest,boundary,"
                    "result_status,input_batch_id,observed_mailbox_revision,"
                    "observed_classified_through_seq,selected_input_end_seq,actor_owner_id,"
                    "actor_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                ).format(self._table("selection_operations")),
                (
                    self.store.bot_id,
                    operation_id,
                    actor.conversation_id,
                    input_digest,
                    boundary,
                    status,
                    batch_id,
                    lane["mailbox_revision"],
                    lane["classified_through_seq"],
                    selected_input_end_seq,
                    actor.owner_id,
                    actor.epoch,
                ),
            )
        ).fetchone()
        return await self._selection_result(conn, receipt)

    async def select_input(
        self,
        actor: ConversationActor,
        operation_id: str,
        *,
        boundary: str,
        session_ref: str | None = None,
        run_ref: str | None = None,
        next_run_ref: str | None = None,
        context_view_policy: ContextViewPolicy | None = None,
        _connection=None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        if boundary not in _BOUNDARIES:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid input boundary", 422)
        if (session_ref is None) != (run_ref is None):
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Session and Run references are incomplete", 422
            )
        _bounded(session_ref, "session_ref", 256, optional=True)
        _bounded(run_ref, "run_ref", 256, optional=True)
        _bounded(next_run_ref, "next_run_ref", 256, optional=True)
        if next_run_ref is not None and boundary != "SESSION_END":
            raise ForestError(
                "CONVERSATION_INPUT_INVALID",
                "A successor Run is only valid at Session end",
                422,
            )
        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        policy = context_view_policy or self.context_view_policy
        if type(policy) is not ContextViewPolicy:
            raise ForestError(
                "CONTEXT_VIEW_POLICY_INVALID", "Invalid Context view policy", 422
            )
        policy.validate()
        input_digest = _digest(
            {
                "conversation_id": actor.conversation_id,
                "actor_owner_id": actor.owner_id,
                "actor_epoch": actor.epoch,
                "boundary": boundary,
                "session_ref": session_ref,
                "run_ref": run_ref,
                "next_run_ref": next_run_ref,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock_operation(conn, operation_id)
            lane = await self._guard_actor(conn, actor)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("selection_operations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "SELECTION_OPERATION_CONFLICT", "Selection operation input differs", 409
                    )
                return await self._selection_result(conn, replay)
            if boundary == "IDLE":
                if lane["active_session_ref"] is not None:
                    raise ForestError(
                        "CONVERSATION_SESSION_ACTIVE", "Conversation already has an active Session", 409
                    )
            elif (
                session_ref is None
                or lane["active_session_ref"] != session_ref
                or lane["active_run_ref"] != run_ref
            ):
                raise ForestError(
                    "CONVERSATION_SESSION_CONFLICT", "Session or Run fence differs", 409
                )
            pending = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "AND status='PENDING' AND ingress_seq<=%s "
                            "ORDER BY ingress_seq,trigger_id FOR UPDATE").format(
                        self._table("conversation_triggers")
                    ),
                    (
                        self.store.bot_id,
                        actor.conversation_id,
                        lane["classified_through_seq"],
                    ),
                )
            ).fetchall()
            if pending:
                if session_ref is None:
                    raise ForestError(
                        "CONVERSATION_SESSION_REQUIRED",
                        "A trigger cut requires caller-owned Session and Run references",
                        409,
                    )
                selected_run_ref = run_ref
                if boundary == "SESSION_END":
                    if next_run_ref is None:
                        raise ForestError(
                            "CONVERSATION_SUCCESSOR_RUN_REQUIRED",
                            "A pending trigger after a final reply needs a successor Run",
                            409,
                        )
                    selected_run_ref = next_run_ref
                trigger_cut = max(row["ingress_seq"] for row in pending)
                if trigger_cut > lane["context_applied_seq"]:
                    kind = "TRIGGER_INPUT"
                    start_seq = lane["context_applied_seq"] + 1
                    end_seq = trigger_cut
                else:
                    kind = "RECOVERY_CLAIM"
                    start_seq = end_seq = None
                return await self._commit_batch(
                    conn,
                    lane=lane,
                    actor=actor,
                    operation_id=operation_id,
                    input_digest=input_digest,
                    boundary=boundary,
                    kind=kind,
                    start_seq=start_seq,
                    end_seq=end_seq,
                    trigger_cut_seq=trigger_cut,
                    session_ref=session_ref,
                    run_ref=selected_run_ref,
                    triggers=pending,
                    context_view_policy=policy,
                )
            has_gap = lane["classified_through_seq"] < lane["next_ingress_seq"] - 1
            if boundary == "MODEL_SAFE_POINT":
                status = "WAIT_INPUT" if has_gap else "CONTINUE"
                return await self._record_selection(
                    conn,
                    operation_id=operation_id,
                    input_digest=input_digest,
                    boundary=boundary,
                    status=status,
                    batch_id=None,
                    lane=lane,
                    actor=actor,
                    selected_input_end_seq=lane["context_applied_seq"],
                )
            if lane["classified_through_seq"] > lane["context_applied_seq"]:
                return await self._commit_batch(
                    conn,
                    lane=lane,
                    actor=actor,
                    operation_id=operation_id,
                    input_digest=input_digest,
                    boundary=boundary,
                    kind="SILENT_DRAIN",
                    start_seq=lane["context_applied_seq"] + 1,
                    end_seq=lane["classified_through_seq"],
                    trigger_cut_seq=None,
                    session_ref=None,
                    run_ref=None,
                    triggers=(),
                    clear_session=boundary == "SESSION_END",
                    context_view_policy=policy,
                )
            status = "WAIT_INPUT" if has_gap else "IDLE"
            if boundary == "SESSION_END" and status == "IDLE":
                await conn.execute(
                    sql.SQL("UPDATE {} SET active_session_ref=NULL,active_run_ref=NULL,"
                            "resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,"
                            "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND conversation_id=%s").format(
                        self._table("conversation_lanes")
                    ),
                    (self.store.bot_id, actor.conversation_id),
                )
                lane = dict(lane)
                lane["mailbox_revision"] += 1
            return await self._record_selection(
                conn,
                operation_id=operation_id,
                input_digest=input_digest,
                boundary=boundary,
                status=status,
                batch_id=None,
                lane=lane,
                actor=actor,
                selected_input_end_seq=lane["context_applied_seq"],
            )

    async def select_input_in(self, conn, actor: ConversationActor, operation_id: str, **values):
        return await self.select_input(
            actor,
            operation_id,
            _connection=conn,
            **values,
        )

    async def _compact_context_view(
        self,
        conn,
        *,
        lane,
        actor: ConversationActor,
        selection_operation_id: str,
        policy: ContextViewPolicy,
        pin_active_session: bool,
    ) -> dict:
        """Archive an old immutable prefix without generating semantic summaries.

        Original Context rows and payloads remain authoritative.  The compacted
        view stores an exact membership relation plus a small machine-readable
        archive reference, so snapshot cost is bounded while provenance remains
        queryable.
        """

        policy.validate()
        lane = dict(lane)
        active_count = lane["context_view_unit_count"]
        if active_count <= policy.high_watermark_units:
            return lane
        if self.payloads is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING",
                "Context compaction payload authority is unavailable",
                503,
            )
        archive_target = active_count - policy.retain_units
        rows = await (
            await conn.execute(
                sql.SQL(
                    "WITH active AS ("
                    "SELECT u.*,row_number() OVER (ORDER BY u.context_revision DESC,u.unit_index DESC) "
                    "AS recent_rank FROM {} u WHERE u.bot_id=%s AND u.conversation_id=%s "
                    "AND NOT EXISTS (SELECT 1 FROM {} cu WHERE cu.bot_id=u.bot_id "
                    "AND cu.conversation_id=u.conversation_id AND cu.unit_id=u.unit_id)) "
                    "SELECT a.unit_id,a.context_revision,a.unit_index,a.payload_digest,"
                    "a.author_payload_digest,a.event_id,a.input_batch_id FROM active a "
                    "WHERE a.recent_rank>%s "
                    "AND NOT EXISTS (SELECT 1 FROM {} t WHERE t.bot_id=a.bot_id "
                    "AND t.conversation_id=a.conversation_id AND t.event_id=a.event_id "
                    "AND t.status IN ('PENDING','CLAIMED')) "
                    "AND (NOT %s OR NOT ("
                    "EXISTS (SELECT 1 FROM {} b WHERE b.bot_id=a.bot_id "
                    "AND b.input_batch_id=a.input_batch_id AND b.session_ref=%s) OR "
                    "EXISTS (SELECT 1 FROM {} op WHERE op.bot_id=a.bot_id "
                    "AND op.conversation_id=a.conversation_id "
                    "AND op.context_revision=a.context_revision AND op.session_ref=%s))) "
                    "ORDER BY a.context_revision,a.unit_index LIMIT %s"
                ).format(
                    self._table("context_units"),
                    self._table("context_compaction_units"),
                    self._table("conversation_triggers"),
                    self._table("input_batches"),
                    self._table("context_append_operations"),
                ),
                (
                    self.store.bot_id,
                    actor.conversation_id,
                    policy.retain_units,
                    pin_active_session,
                    lane["active_session_ref"],
                    lane["active_session_ref"],
                    archive_target,
                ),
            )
        ).fetchall()
        if not rows:
            return lane

        source_set = [
            [
                row["unit_id"],
                row["context_revision"],
                row["unit_index"],
                row["payload_digest"],
                row["author_payload_digest"],
                row["event_id"],
                row["input_batch_id"],
            ]
            for row in rows
        ]
        source_set_digest = _digest(source_set)
        published_view_revision = lane["context_view_revision"] + 1
        compaction_operation_id = "context-compact:" + hashlib.sha256(
            _canonical([self.store.bot_id, selection_operation_id])
        ).hexdigest()
        compaction_id = "compaction:" + hashlib.sha256(
            _canonical(
                [
                    self.store.bot_id,
                    actor.conversation_id,
                    published_view_revision,
                    source_set_digest,
                    policy.policy_revision,
                ]
            )
        ).hexdigest()
        resulting_count = active_count - len(rows)
        cumulative_count = lane["context_archived_unit_count"] + len(rows)
        first_revision = min(row["context_revision"] for row in rows)
        last_revision = max(row["context_revision"] for row in rows)
        archive_value = {
            "protocol": "forest.context-archive-reference/1",
            "conversation_id": actor.conversation_id,
            "compaction_id": compaction_id,
            "published_view_revision": published_view_revision,
            "policy_revision": policy.policy_revision,
            "archived_unit_count": len(rows),
            "cumulative_archived_unit_count": cumulative_count,
            "source_set_digest": source_set_digest,
            "coverage": {
                "first_context_revision": first_revision,
                "last_context_revision": last_revision,
            },
            "authority": "conversation.scheduler/context_compaction_units",
        }
        deletion_epoch = await self.payloads.epoch_in(conn)
        archive = await self.payloads.put_json_in(
            conn,
            owner_component_id="conversation.scheduler",
            owner_kind="context.archive",
            owner_id=compaction_id,
            value=archive_value,
            sources=(
                PayloadSource(
                    source_component_id="conversation.scheduler",
                    source_kind="context.archive",
                    source_id=compaction_id,
                    source_revision=published_view_revision,
                    observation_conversation_id=actor.conversation_id,
                ),
            ),
            expected_deletion_epoch=deletion_epoch,
        )
        input_digest = _digest(
            {
                "conversation_id": actor.conversation_id,
                "base_view_revision": lane["context_view_revision"],
                "published_view_revision": published_view_revision,
                "source_set_digest": source_set_digest,
                "policy_revision": policy.policy_revision,
                "high_watermark_units": policy.high_watermark_units,
                "retain_units": policy.retain_units,
            }
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,conversation_id,compaction_id,operation_id,input_digest,"
                "base_view_revision,published_view_revision,source_set_digest,policy_revision,"
                "archive_payload_id,archive_payload_digest,archived_unit_count,"
                "resulting_view_unit_count,cumulative_archived_unit_count,"
                "first_context_revision,last_context_revision) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            ).format(self._table("context_compactions")),
            (
                self.store.bot_id,
                actor.conversation_id,
                compaction_id,
                compaction_operation_id,
                input_digest,
                lane["context_view_revision"],
                published_view_revision,
                source_set_digest,
                policy.policy_revision,
                archive.payload_id,
                archive.content_digest,
                len(rows),
                resulting_count,
                cumulative_count,
                first_revision,
                last_revision,
            ),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,conversation_id,compaction_id,archived_index,unit_id,"
                "context_revision,unit_index,payload_digest,author_payload_digest) "
                "SELECT %s,%s,%s,x.ordinality-1,x.unit_id,x.context_revision,x.unit_index,"
                "x.payload_digest,x.author_payload_digest FROM "
                "unnest(%s::text[],%s::bigint[],%s::integer[],%s::text[],%s::text[]) "
                "WITH ORDINALITY AS x(unit_id,context_revision,unit_index,payload_digest,"
                "author_payload_digest,ordinality)"
            ).format(self._table("context_compaction_units")),
            (
                self.store.bot_id,
                actor.conversation_id,
                compaction_id,
                [row["unit_id"] for row in rows],
                [row["context_revision"] for row in rows],
                [row["unit_index"] for row in rows],
                [row["payload_digest"] for row in rows],
                [row["author_payload_digest"] for row in rows],
            ),
        )
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET context_view_revision=%s,context_view_unit_count=%s,"
                "context_archived_unit_count=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND conversation_id=%s"
            ).format(self._table("conversation_lanes")),
            (
                published_view_revision,
                resulting_count,
                cumulative_count,
                self.store.bot_id,
                actor.conversation_id,
            ),
        )
        lane["context_view_revision"] = published_view_revision
        lane["context_view_unit_count"] = resulting_count
        lane["context_archived_unit_count"] = cumulative_count
        lane["_memory_archive"] = {
            "compaction_ref": compaction_id,
            "event_refs": tuple(row["event_id"] for row in rows if row["event_id"] is not None),
        }
        return lane

    async def _commit_batch(
        self,
        conn,
        *,
        lane,
        actor,
        operation_id,
        input_digest,
        boundary,
        kind,
        start_seq,
        end_seq,
        trigger_cut_seq,
        session_ref,
        run_ref,
        triggers,
        clear_session=False,
        context_view_policy: ContextViewPolicy | None = None,
    ) -> dict:
        events = []
        if start_seq is not None:
            events = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "AND ingress_seq BETWEEN %s AND %s ORDER BY ingress_seq FOR SHARE").format(
                        self._table("inbox_messages")
                    ),
                    (self.store.bot_id, actor.conversation_id, start_seq, end_seq),
                )
            ).fetchall()
            expected = list(range(start_seq, end_seq + 1))
            if [row["ingress_seq"] for row in events] != expected or any(
                row["classification_state"] != "FINAL" for row in events
            ):
                raise ForestError(
                    "CONVERSATION_SEQUENCE_CORRUPT", "Selected input is not a final prefix", 500
                )
        context_revision = lane["context_revision"] + (1 if events else 0)
        if context_revision <= 0:
            raise ForestError(
                "CONVERSATION_SEQUENCE_CORRUPT", "Recovery claim has no committed Context", 500
            )
        lane = await self._compact_context_view(
            conn,
            lane=lane,
            actor=actor,
            selection_operation_id=operation_id,
            policy=context_view_policy or self.context_view_policy,
            pin_active_session=boundary == "MODEL_SAFE_POINT",
        )
        memory_archive = lane.pop("_memory_archive", None)
        selected_end = end_seq if end_seq is not None else lane["context_applied_seq"]
        batch_id = "batch:" + hashlib.sha256(
            _canonical([self.store.bot_id, operation_id])
        ).hexdigest()
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,conversation_id,input_batch_id,operation_id,input_digest,"
                "boundary,kind,start_seq,end_seq,trigger_cut_seq,selected_input_end_seq,"
                "session_ref,run_ref,actor_owner_id,actor_epoch,context_revision,"
                "context_view_revision) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            ).format(self._table("input_batches")),
            (
                self.store.bot_id,
                actor.conversation_id,
                batch_id,
                operation_id,
                input_digest,
                boundary,
                kind,
                start_seq,
                end_seq,
                trigger_cut_seq,
                selected_end,
                session_ref,
                run_ref,
                actor.owner_id,
                actor.epoch,
                context_revision,
                lane["context_view_revision"],
            ),
        )
        for index, event in enumerate(events):
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,input_batch_id,event_index,conversation_id,"
                        "event_id,ingress_seq) VALUES (%s,%s,%s,%s,%s,%s)").format(
                    self._table("input_batch_events")
                ),
                (
                    self.store.bot_id,
                    batch_id,
                    index,
                    actor.conversation_id,
                    event["event_id"],
                    event["ingress_seq"],
                ),
            )
            unit_id = "input:" + hashlib.sha256(
                _canonical([self.store.bot_id, actor.conversation_id, event["event_id"]])
            ).hexdigest()
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,conversation_id,unit_id,context_revision,unit_index,"
                    "kind,source_component_id,source_kind,source_id,input_batch_id,event_id,"
                    "ingress_seq,payload_id,payload_digest,payload_owner_component_id,"
                    "payload_owner_kind,payload_owner_id,author_payload_id,author_payload_digest,"
                    "author_owner_component_id,author_owner_kind,author_owner_id,"
                    "author_subject_ref,trigger_decision) VALUES "
                    "(%s,%s,%s,%s,%s,'INPUT',%s,'conversation.input',%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("context_units")),
                (
                    self.store.bot_id,
                    actor.conversation_id,
                    unit_id,
                    context_revision,
                    index,
                    event["source_component_id"],
                    event["event_id"],
                    batch_id,
                    event["event_id"],
                    event["ingress_seq"],
                    event["content_payload_id"],
                    event["content_payload_digest"],
                    event["content_owner_component_id"],
                    event["content_owner_kind"],
                    event["content_owner_id"],
                    event["author_payload_id"],
                    event["author_payload_digest"],
                    event["author_owner_component_id"],
                    event["author_owner_kind"],
                    event["author_owner_id"],
                    event["author_subject_ref"],
                    event["trigger_decision"],
                ),
            )
        for index, trigger in enumerate(triggers):
            await conn.execute(
                sql.SQL("UPDATE {} SET status='CLAIMED',claimed_session_ref=%s,"
                        "claimed_run_ref=%s,input_batch_id=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND trigger_id=%s AND status='PENDING'").format(
                    self._table("conversation_triggers")
                ),
                (session_ref, run_ref, batch_id, self.store.bot_id, trigger["trigger_id"]),
            )
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,input_batch_id,trigger_index,trigger_id) "
                        "VALUES (%s,%s,%s,%s)").format(self._table("input_batch_triggers")),
                (self.store.bot_id, batch_id, index, trigger["trigger_id"]),
            )
        next_session = None if clear_session else (
            session_ref if session_ref is not None else lane["active_session_ref"]
        )
        next_run = None if clear_session else (
            run_ref if run_ref is not None else lane["active_run_ref"]
        )
        await conn.execute(
            sql.SQL("UPDATE {} SET context_applied_seq=%s,context_revision=%s,"
                    "context_view_unit_count=context_view_unit_count+%s,"
                    "active_session_ref=%s,active_run_ref=%s,mailbox_revision=mailbox_revision+1,"
                    "resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                self._table("conversation_lanes")
            ),
            (
                selected_end,
                context_revision,
                len(events),
                next_session,
                next_run,
                self.store.bot_id,
                actor.conversation_id,
            ),
        )
        lane = dict(lane)
        lane["context_applied_seq"] = selected_end
        lane["context_revision"] = context_revision
        lane["context_view_unit_count"] += len(events)
        lane["mailbox_revision"] += 1
        selection = await self._record_selection(
            conn,
            operation_id=operation_id,
            input_digest=input_digest,
            boundary=boundary,
            status="BATCH",
            batch_id=batch_id,
            lane=lane,
            actor=actor,
            selected_input_end_seq=selected_end,
        )
        if memory_archive is not None:
            selection = {**selection, "memory_archive": memory_archive}
        return selection

    async def append_context(
        self,
        actor: ConversationActor,
        *,
        operation_id: str,
        facts,
        session_ref: str | None = None,
        run_ref: str | None = None,
        answer_trigger_ids=(),
        reply_ref: str | None = None,
        _connection=None,
    ) -> dict:
        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        _bounded(operation_id, "operation_id", 256)
        if (session_ref is None) != (run_ref is None):
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Session and Run references are incomplete", 422
            )
        _bounded(session_ref, "session_ref", 256, optional=True)
        _bounded(run_ref, "run_ref", 256, optional=True)
        _bounded(reply_ref, "reply_ref", 512, optional=True)
        units = tuple(facts)
        if not 1 <= len(units) <= 4096 or any(
            type(item) is not SchedulerContextFact for item in units
        ):
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid Context fact set", 422)
        for fact in units:
            fact.validate()
        if len({fact.unit_id for fact in units}) != len(units):
            raise ForestError("CONVERSATION_INPUT_INVALID", "Context unit identity repeats", 422)
        trigger_ids = tuple(sorted(set(answer_trigger_ids)))
        if len(trigger_ids) > 4096:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Too many answered triggers", 422)
        for trigger_id in trigger_ids:
            _bounded(trigger_id, "trigger_id", 256)
        replies = [fact for fact in units if fact.kind == "REPLY"]
        if trigger_ids:
            if (
                session_ref is None
                or reply_ref is None
                or len(replies) != 1
                or replies[0].source_id != reply_ref
            ):
                raise ForestError(
                    "CONVERSATION_INPUT_INVALID",
                    "Final reply needs one matching REPLY fact and claimed triggers",
                    422,
                )
            append_kind = "FINAL_REPLY"
        else:
            if replies or reply_ref is not None:
                raise ForestError(
                    "CONVERSATION_INPUT_INVALID", "REPLY fact needs answered triggers", 422
                )
            append_kind = "FACTS"
        if any(fact.kind == "AGENT_STEP" for fact in units) and session_ref is None:
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Agent Context facts need Session and Run", 422
            )
        input_digest = _digest(
            {
                "conversation_id": actor.conversation_id,
                "actor_owner_id": actor.owner_id,
                "actor_epoch": actor.epoch,
                "session_ref": session_ref,
                "run_ref": run_ref,
                "facts": [fact.canonical() for fact in units],
                "answer_trigger_ids": trigger_ids,
                "reply_ref": reply_ref,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock_operation(conn, operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("context_append_operations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                await self._guard_actor(conn, actor)
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "CONTEXT_APPEND_CONFLICT", "Context append operation input differs", 409
                    )
                return {
                    "operation_id": operation_id,
                    "context_revision": replay["context_revision"],
                    "unit_ids": [fact.unit_id for fact in units],
                    "answered_trigger_ids": list(trigger_ids),
                    "reply_ref": reply_ref,
                }
            references = [fact.payload for fact in units]
            references.extend(fact.author for fact in units if fact.author is not None)
            for reference in sorted(references, key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            lane = await self._guard_actor(conn, actor)
            if session_ref is not None and (
                lane["active_session_ref"] != session_ref or lane["active_run_ref"] != run_ref
            ):
                raise ForestError(
                    "CONVERSATION_SESSION_CONFLICT", "Session or Run fence differs", 409
                )
            claimed = []
            if trigger_ids:
                claimed = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND trigger_id=ANY(%s) "
                                "ORDER BY trigger_id FOR UPDATE").format(
                            self._table("conversation_triggers")
                        ),
                        (self.store.bot_id, list(trigger_ids)),
                    )
                ).fetchall()
                if {row["trigger_id"] for row in claimed} != set(trigger_ids):
                    raise ForestError("TRIGGER_NOT_FOUND", "A trigger does not exist", 404)
                if any(
                    row["conversation_id"] != actor.conversation_id
                    or row["status"] != "CLAIMED"
                    or row["claimed_session_ref"] != session_ref
                    or row["claimed_run_ref"] != run_ref
                    for row in claimed
                ):
                    raise ForestError(
                        "TRIGGER_STATE_CONFLICT", "A trigger is not owned by this Run", 409
                    )
            context_revision = lane["context_revision"] + 1
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,conversation_id,input_digest,append_kind,"
                    "session_ref,run_ref,context_revision,unit_count,answered_trigger_count,"
                    "reply_ref,actor_owner_id,actor_epoch) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("context_append_operations")),
                (
                    self.store.bot_id,
                    operation_id,
                    actor.conversation_id,
                    input_digest,
                    append_kind,
                    session_ref,
                    run_ref,
                    context_revision,
                    len(units),
                    len(trigger_ids),
                    reply_ref,
                    actor.owner_id,
                    actor.epoch,
                ),
            )
            for index, fact in enumerate(units):
                author = fact.author
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,conversation_id,unit_id,context_revision,unit_index,"
                        "kind,source_component_id,source_kind,source_id,payload_id,payload_digest,"
                        "payload_owner_component_id,payload_owner_kind,payload_owner_id,"
                        "author_payload_id,author_payload_digest,author_owner_component_id,"
                        "author_owner_kind,author_owner_id,author_subject_ref) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("context_units")),
                    (
                        self.store.bot_id,
                        actor.conversation_id,
                        fact.unit_id,
                        context_revision,
                        index,
                        fact.kind,
                        fact.source_component_id,
                        fact.source_kind,
                        fact.source_id,
                        fact.payload.payload_id,
                        fact.payload.digest,
                        fact.payload.owner_component_id,
                        fact.payload.owner_kind,
                        fact.payload.owner_id,
                        author.payload_id if author else None,
                        author.digest if author else None,
                        author.owner_component_id if author else None,
                        author.owner_kind if author else None,
                        author.owner_id if author else None,
                        fact.author_subject_ref,
                    ),
                )
            for trigger in claimed:
                await conn.execute(
                    sql.SQL("UPDATE {} SET status='ANSWERED',answered_reply_ref=%s,"
                            "updated_at=clock_timestamp() WHERE bot_id=%s AND trigger_id=%s").format(
                        self._table("conversation_triggers")
                    ),
                    (reply_ref, self.store.bot_id, trigger["trigger_id"]),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET context_revision=%s,"
                        "context_view_unit_count=context_view_unit_count+%s,"
                        "mailbox_revision=mailbox_revision+1,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (context_revision, len(units), self.store.bot_id, actor.conversation_id),
            )
            return {
                "operation_id": operation_id,
                "context_revision": context_revision,
                "unit_ids": [fact.unit_id for fact in units],
                "answered_trigger_ids": list(trigger_ids),
                "reply_ref": reply_ref,
            }

    async def append_context_in(self, conn, actor: ConversationActor, **values) -> dict:
        """Append Context inside a caller-owned UnitOfWork transaction."""

        return await self.append_context(actor, _connection=conn, **values)

    async def requeue_run(
        self,
        actor: ConversationActor,
        *,
        operation_id: str,
        session_ref: str,
        run_ref: str,
        reason_code: str,
        _connection=None,
    ) -> tuple[str, ...]:
        _bounded(operation_id, "operation_id", 256)
        _bounded(session_ref, "session_ref", 256)
        _bounded(run_ref, "run_ref", 256)
        _bounded(reason_code, "reason_code", 128)
        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        input_digest = _digest(
            {
                "conversation_id": actor.conversation_id,
                "actor_owner_id": actor.owner_id,
                "actor_epoch": actor.epoch,
                "session_ref": session_ref,
                "run_ref": run_ref,
                "reason_code": reason_code,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock_operation(conn, operation_id)
            lane = await self._guard_actor(conn, actor)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("trigger_requeues")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "TRIGGER_REQUEUE_CONFLICT", "Trigger requeue operation input differs", 409
                    )
                rows = await (
                    await conn.execute(
                        sql.SQL("SELECT trigger_id FROM {} WHERE bot_id=%s AND operation_id=%s "
                                "ORDER BY item_index").format(
                            self._table("trigger_requeue_items")
                        ),
                        (self.store.bot_id, operation_id),
                    )
                ).fetchall()
                return tuple(row["trigger_id"] for row in rows)
            if (
                lane["active_session_ref"] != session_ref
                or lane["active_run_ref"] != run_ref
            ):
                raise ForestError(
                    "CONVERSATION_SESSION_CONFLICT", "Abandoned Session or Run fence differs", 409
                )
            triggers = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "AND status='CLAIMED' AND claimed_session_ref=%s AND claimed_run_ref=%s "
                            "ORDER BY ingress_seq,trigger_id FOR UPDATE").format(
                        self._table("conversation_triggers")
                    ),
                    (self.store.bot_id, actor.conversation_id, session_ref, run_ref),
                )
            ).fetchall()
            if not triggers:
                raise ForestError(
                    "TRIGGER_REQUEUE_EMPTY", "The abandoned Run owns no pending trigger", 409
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,conversation_id,input_digest,"
                    "abandoned_session_ref,abandoned_run_ref,reason_code,actor_owner_id,"
                    "actor_epoch,trigger_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("trigger_requeues")),
                (
                    self.store.bot_id,
                    operation_id,
                    actor.conversation_id,
                    input_digest,
                    session_ref,
                    run_ref,
                    reason_code,
                    actor.owner_id,
                    actor.epoch,
                    len(triggers),
                ),
            )
            for index, trigger in enumerate(triggers):
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,item_index,trigger_id) "
                            "VALUES (%s,%s,%s,%s)").format(
                        self._table("trigger_requeue_items")
                    ),
                    (self.store.bot_id, operation_id, index, trigger["trigger_id"]),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET status='PENDING',claimed_session_ref=NULL,"
                            "claimed_run_ref=NULL,input_batch_id=NULL,updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND trigger_id=%s").format(
                        self._table("conversation_triggers")
                    ),
                    (self.store.bot_id, trigger["trigger_id"]),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET active_session_ref=NULL,active_run_ref=NULL,"
                        "resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,"
                        "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (self.store.bot_id, actor.conversation_id),
            )
            return tuple(trigger["trigger_id"] for trigger in triggers)

    async def requeue_run_in(self, conn, actor: ConversationActor, **values):
        return await self.requeue_run(actor, _connection=conn, **values)

    async def terminal_run(
        self,
        actor: ConversationActor,
        *,
        operation_id: str,
        session_ref: str,
        run_ref: str,
        resulting_status: str,
        reason_code: str,
        _connection=None,
    ) -> tuple[str, ...]:
        """Terminally settle one Run's claimed triggers and release its lane."""

        _bounded(operation_id, "operation_id", 256)
        _bounded(session_ref, "session_ref", 256)
        _bounded(run_ref, "run_ref", 256)
        _bounded(reason_code, "reason_code", 128)
        if resulting_status not in {"FAILED", "CANCELLED"}:
            raise ForestError(
                "CONVERSATION_INPUT_INVALID", "Invalid terminal trigger status", 422
            )
        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        input_digest = _digest(
            {
                "conversation_id": actor.conversation_id,
                "actor_owner_id": actor.owner_id,
                "actor_epoch": actor.epoch,
                "session_ref": session_ref,
                "run_ref": run_ref,
                "resulting_status": resulting_status,
                "reason_code": reason_code,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock_operation(conn, operation_id)
            lane = await self._guard_actor(conn, actor)
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s"
                    ).format(self._table("trigger_terminal_operations")),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "TRIGGER_TERMINAL_CONFLICT",
                        "Trigger terminal operation input differs",
                        409,
                    )
                rows = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT trigger_id FROM {} WHERE bot_id=%s AND operation_id=%s "
                            "ORDER BY item_index"
                        ).format(self._table("trigger_terminal_items")),
                        (self.store.bot_id, operation_id),
                    )
                ).fetchall()
                return tuple(row["trigger_id"] for row in rows)
            if (
                lane["active_session_ref"] != session_ref
                or lane["active_run_ref"] != run_ref
            ):
                raise ForestError(
                    "CONVERSATION_SESSION_CONFLICT", "Terminal Session or Run fence differs", 409
                )
            triggers = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                        "AND status='CLAIMED' AND claimed_session_ref=%s AND claimed_run_ref=%s "
                        "ORDER BY ingress_seq,trigger_id FOR UPDATE"
                    ).format(self._table("conversation_triggers")),
                    (self.store.bot_id, actor.conversation_id, session_ref, run_ref),
                )
            ).fetchall()
            if not triggers:
                raise ForestError(
                    "TRIGGER_TERMINAL_EMPTY", "The terminal Run owns no claimed trigger", 409
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,conversation_id,input_digest,"
                    "terminal_session_ref,terminal_run_ref,resulting_status,reason_code,"
                    "actor_owner_id,actor_epoch,trigger_count) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("trigger_terminal_operations")),
                (
                    self.store.bot_id,
                    operation_id,
                    actor.conversation_id,
                    input_digest,
                    session_ref,
                    run_ref,
                    resulting_status,
                    reason_code,
                    actor.owner_id,
                    actor.epoch,
                    len(triggers),
                ),
            )
            for index, trigger in enumerate(triggers):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,item_index,trigger_id) "
                        "VALUES (%s,%s,%s,%s)"
                    ).format(self._table("trigger_terminal_items")),
                    (self.store.bot_id, operation_id, index, trigger["trigger_id"]),
                )
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET status=%s,terminal_reason_code=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND trigger_id=%s "
                        "AND status='CLAIMED'"
                    ).format(self._table("conversation_triggers")),
                    (
                        resulting_status,
                        reason_code,
                        self.store.bot_id,
                        trigger["trigger_id"],
                    ),
                )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET active_session_ref=NULL,active_run_ref=NULL,"
                    "resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,"
                    "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND conversation_id=%s"
                ).format(self._table("conversation_lanes")),
                (self.store.bot_id, actor.conversation_id),
            )
            return tuple(trigger["trigger_id"] for trigger in triggers)

    async def fail_run(self, actor: ConversationActor, **values):
        return await self.terminal_run(
            actor, resulting_status="FAILED", **values
        )

    async def fail_run_in(self, conn, actor: ConversationActor, **values):
        return await self.fail_run(actor, _connection=conn, **values)

    async def cancel_run(self, actor: ConversationActor, **values):
        return await self.terminal_run(
            actor, resulting_status="CANCELLED", **values
        )

    async def cancel_run_in(self, conn, actor: ConversationActor, **values):
        return await self.cancel_run(actor, _connection=conn, **values)

    async def verify_input_batch_occurrence_in(
        self,
        conn,
        *,
        input_batch_ref: str,
        conversation_ref: str,
        event_ref: str,
        context_revision: int,
    ) -> dict:
        """Verify that one committed InputBatch contains the exact occurrence."""

        _bounded(input_batch_ref, "input_batch_ref", 256)
        _bounded(conversation_ref, "conversation_ref", 256)
        _bounded(event_ref, "event_ref", 256)
        if type(context_revision) is not int or context_revision <= 0:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid Context revision", 422)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT b.input_batch_id,b.conversation_id,b.context_revision,e.event_id,"
                    "e.ingress_seq FROM {} b JOIN {} e ON e.bot_id=b.bot_id "
                    "AND e.input_batch_id=b.input_batch_id WHERE b.bot_id=%s "
                    "AND b.input_batch_id=%s AND b.conversation_id=%s "
                    "AND b.context_revision=%s AND e.event_id=%s"
                ).format(
                    self._table("input_batches"),
                    self._table("input_batch_events"),
                ),
                (
                    self.store.bot_id,
                    input_batch_ref,
                    conversation_ref,
                    context_revision,
                    event_ref,
                ),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "CONVERSATION_INPUT_BATCH_UNAVAILABLE",
                "InputBatch does not contain this occurrence",
                409,
            )
        return dict(row)

    async def verify_context_archive_occurrence_in(
        self,
        conn,
        *,
        conversation_ref: str,
        compaction_ref: str,
        event_ref: str,
    ) -> dict:
        """Prove that an input occurrence belongs to one published archive cut."""

        _bounded(conversation_ref, "conversation_ref", 256)
        _bounded(compaction_ref, "compaction_ref", 256)
        _bounded(event_ref, "event_ref", 256)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT cu.compaction_id,u.input_batch_id AS input_batch_ref,"
                    "u.context_revision,u.event_id FROM {} cu JOIN {} u ON u.bot_id=cu.bot_id "
                    "AND u.conversation_id=cu.conversation_id AND u.unit_id=cu.unit_id "
                    "WHERE cu.bot_id=%s AND cu.conversation_id=%s AND cu.compaction_id=%s "
                    "AND u.kind='INPUT' AND u.event_id=%s"
                ).format(
                    self._table("context_compaction_units"),
                    self._table("context_units"),
                ),
                (self.store.bot_id, conversation_ref, compaction_ref, event_ref),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "CONVERSATION_ARCHIVE_UNAVAILABLE",
                "Context archive does not contain this occurrence",
                409,
            )
        return dict(row)

    async def answered_trigger_events_in(self, conn, *, trigger_ids) -> tuple[str, ...]:
        """Resolve the immutable source events behind an answered trigger set."""

        ids = tuple(sorted(set(trigger_ids)))
        if not ids:
            return ()
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT trigger_id,event_id FROM {} WHERE bot_id=%s AND trigger_id=ANY(%s) "
                    "ORDER BY trigger_id"
                ).format(self._table("conversation_triggers")),
                (self.store.bot_id, list(ids)),
            )
        ).fetchall()
        if [row["trigger_id"] for row in rows] != list(ids):
            raise ForestError("TRIGGER_NOT_FOUND", "An answered trigger does not exist", 404)
        return tuple(row["event_id"] for row in rows)

    async def context_snapshot(self, actor: ConversationActor, *, _connection=None) -> dict:
        async with self._transaction(_connection) as conn:
            lane = await self._guard_actor(conn, actor)
            compaction = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                        "ORDER BY published_view_revision DESC LIMIT 1"
                    ).format(self._table("context_compactions")),
                    (self.store.bot_id, actor.conversation_id),
                )
            ).fetchone()
            units = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT u.* FROM {} u WHERE u.bot_id=%s AND u.conversation_id=%s "
                        "AND NOT EXISTS (SELECT 1 FROM {} cu WHERE cu.bot_id=u.bot_id "
                        "AND cu.conversation_id=u.conversation_id AND cu.unit_id=u.unit_id) "
                        "ORDER BY u.context_revision,u.unit_index"
                    ).format(
                        self._table("context_units"),
                        self._table("context_compaction_units"),
                    ),
                    (self.store.bot_id, actor.conversation_id),
                )
            ).fetchall()
            projected_units = [dict(row) for row in units]
            archive = None
            if compaction is not None:
                archive = {
                    "compaction_id": compaction["compaction_id"],
                    "published_view_revision": compaction["published_view_revision"],
                    "source_set_digest": compaction["source_set_digest"],
                    "policy_revision": compaction["policy_revision"],
                    "archived_unit_count": compaction["archived_unit_count"],
                    "cumulative_archived_unit_count": compaction[
                        "cumulative_archived_unit_count"
                    ],
                    "first_context_revision": compaction["first_context_revision"],
                    "last_context_revision": compaction["last_context_revision"],
                }
                projected_units.insert(
                    0,
                    {
                        "bot_id": self.store.bot_id,
                        "conversation_id": actor.conversation_id,
                        "unit_id": "archive:" + compaction["compaction_id"].removeprefix(
                            "compaction:"
                        ),
                        "context_revision": compaction["first_context_revision"],
                        "unit_index": 0,
                        "kind": "CONTROL",
                        "source_component_id": "conversation.scheduler",
                        "source_kind": "context.archive",
                        "source_id": compaction["compaction_id"],
                        "input_batch_id": None,
                        "event_id": None,
                        "ingress_seq": None,
                        "payload_id": compaction["archive_payload_id"],
                        "payload_digest": compaction["archive_payload_digest"],
                        "payload_owner_component_id": "conversation.scheduler",
                        "payload_owner_kind": "context.archive",
                        "payload_owner_id": compaction["compaction_id"],
                        "author_payload_id": None,
                        "author_payload_digest": None,
                        "author_owner_component_id": None,
                        "author_owner_kind": None,
                        "author_owner_id": None,
                        "author_subject_ref": None,
                        "trigger_decision": None,
                    },
                )
            return {
                "conversation_id": actor.conversation_id,
                "context_applied_seq": lane["context_applied_seq"],
                "classified_through_seq": lane["classified_through_seq"],
                "context_revision": lane["context_revision"],
                "context_view_revision": lane["context_view_revision"],
                "source_component_id": lane["source_component_id"],
                "binding_ref": lane["binding_ref"],
                "platform": lane["platform"],
                "channel_kind": lane["channel_kind"],
                "channel_ref": lane["channel_ref"],
                "thread_ref": lane["thread_ref"],
                "context_view_unit_count": lane["context_view_unit_count"],
                "context_archived_unit_count": lane["context_archived_unit_count"],
                "archive": archive,
                "units": projected_units,
            }

    async def context_snapshot_in(self, conn, actor: ConversationActor) -> dict:
        return await self.context_snapshot(actor, _connection=conn)

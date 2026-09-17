"""Clean repository for conversation ordering, trigger cuts and Context references."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

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
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.conversation.scheduler")
        self.payload_verifier = payload_verifier

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _lock_operation(self, conn, operation_id: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (_operation_key(self.store.bot_id, operation_id),),
        )

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
        *, source_component_id, binding_ref, platform, channel_ref, thread_ref
    ) -> tuple:
        return source_component_id, binding_ref, platform, channel_ref, thread_ref

    async def admit_message(
        self,
        *,
        operation_id: str,
        conversation_id: str,
        source_component_id: str,
        binding_ref: str,
        platform: str,
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
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (conversation_id, "conversation_id", 256),
            (source_component_id, "source_component_id", 128),
            (binding_ref, "binding_ref", 256),
            (platform, "platform", 64),
            (channel_ref, "channel_ref", 256),
            (event_id, "event_id", 256),
            (source_event_ref, "source_event_ref", 512),
            (event_kind, "event_kind", 128),
            (author_subject_ref, "author_subject_ref", 256),
        ):
            _bounded(value, field, limit)
        _bounded(thread_ref, "thread_ref", 256, optional=True)
        _bounded(platform_message_ref, "platform_message_ref", 512, optional=True)
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
                "received_at": received_at.isoformat(),
                "classification_deadline": classification_deadline.isoformat(),
                "context_disposition": context_disposition,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
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
                    "source_component_id,binding_ref,platform,channel_ref,thread_ref) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                ).format(self._table("conversation_lanes")),
                (
                    self.store.bot_id,
                    conversation_id,
                    lane_digest,
                    source_component_id,
                    binding_ref,
                    platform,
                    channel_ref,
                    thread_ref,
                ),
            )
            lane = await self._lane(conn, conversation_id, update=True)
            actual_lane = self._lane_identity(
                source_component_id=lane["source_component_id"],
                binding_ref=lane["binding_ref"],
                platform=lane["platform"],
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
        async with self.store.pool.connection() as conn, conn.transaction():
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
        self, conversation_id: str, owner_id: str, *, lease_seconds: int = 30
    ) -> ConversationActor:
        _bounded(conversation_id, "conversation_id", 256)
        _bounded(owner_id, "owner_id", 256)
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid actor lease", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            lane = await self._lane(conn, conversation_id, update=True)
            alive = (
                lane["actor_owner_id"] is not None
                and lane["actor_expires_at"] > lane["database_now"]
            )
            if alive and lane["actor_owner_id"] != owner_id:
                raise ForestError("CONVERSATION_ACTOR_BUSY", "Conversation actor is busy", 409)
            epoch = lane["actor_epoch"] if alive else lane["actor_epoch"] + 1
            expires = lane["database_now"] + timedelta(seconds=lease_seconds)
            await conn.execute(
                sql.SQL("UPDATE {} SET actor_owner_id=%s,actor_epoch=%s,actor_expires_at=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (owner_id, epoch, expires, self.store.bot_id, conversation_id),
            )
            return ConversationActor(conversation_id, owner_id, epoch, expires)

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
        self, actor: ConversationActor, *, lease_seconds: int = 30
    ) -> ConversationActor:
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("CONVERSATION_INPUT_INVALID", "Invalid actor lease", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
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

    async def release_actor(self, actor: ConversationActor) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard_actor(conn, actor)
            await conn.execute(
                sql.SQL("UPDATE {} SET actor_owner_id=NULL,actor_expires_at=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (self.store.bot_id, actor.conversation_id),
            )

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
        return {
            **result,
            "status": "BATCH",
            **dict(batch),
            "event_ids": [row["event_id"] for row in events],
            "trigger_ids": [row["trigger_id"] for row in triggers],
            "triggers": [dict(row) for row in triggers],
        }

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
        if type(actor) is not ConversationActor:
            raise ForestError("STALE_CONVERSATION_ACTOR", "Invalid conversation actor", 409)
        input_digest = _digest(
            {
                "conversation_id": actor.conversation_id,
                "actor_owner_id": actor.owner_id,
                "actor_epoch": actor.epoch,
                "boundary": boundary,
                "session_ref": session_ref,
                "run_ref": run_ref,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
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
                    run_ref=run_ref,
                    triggers=pending,
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
                )
            status = "WAIT_INPUT" if has_gap else "IDLE"
            if boundary == "SESSION_END" and status == "IDLE":
                await conn.execute(
                    sql.SQL("UPDATE {} SET active_session_ref=NULL,active_run_ref=NULL,"
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
        selected_end = end_seq if end_seq is not None else lane["context_applied_seq"]
        batch_id = "batch:" + hashlib.sha256(
            _canonical([self.store.bot_id, operation_id])
        ).hexdigest()
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,conversation_id,input_batch_id,operation_id,input_digest,"
                "boundary,kind,start_seq,end_seq,trigger_cut_seq,selected_input_end_seq,"
                "session_ref,run_ref,actor_owner_id,actor_epoch,context_revision) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
                    "active_session_ref=%s,active_run_ref=%s,mailbox_revision=mailbox_revision+1,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                self._table("conversation_lanes")
            ),
            (
                selected_end,
                context_revision,
                next_session,
                next_run,
                self.store.bot_id,
                actor.conversation_id,
            ),
        )
        lane = dict(lane)
        lane["context_applied_seq"] = selected_end
        lane["context_revision"] = context_revision
        lane["mailbox_revision"] += 1
        return await self._record_selection(
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
        async with self.store.pool.connection() as conn, conn.transaction():
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
                sql.SQL("UPDATE {} SET context_revision=%s,mailbox_revision=mailbox_revision+1,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (context_revision, self.store.bot_id, actor.conversation_id),
            )
            return {
                "operation_id": operation_id,
                "context_revision": context_revision,
                "unit_ids": [fact.unit_id for fact in units],
                "answered_trigger_ids": list(trigger_ids),
                "reply_ref": reply_ref,
            }

    async def requeue_run(
        self,
        actor: ConversationActor,
        *,
        operation_id: str,
        session_ref: str,
        run_ref: str,
        reason_code: str,
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
        async with self.store.pool.connection() as conn, conn.transaction():
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
                        "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND conversation_id=%s").format(
                    self._table("conversation_lanes")
                ),
                (self.store.bot_id, actor.conversation_id),
            )
            return tuple(trigger["trigger_id"] for trigger in triggers)

    async def context_snapshot(self, actor: ConversationActor) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            lane = await self._guard_actor(conn, actor)
            units = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND conversation_id=%s "
                            "ORDER BY context_revision,unit_index").format(
                        self._table("context_units")
                    ),
                    (self.store.bot_id, actor.conversation_id),
                )
            ).fetchall()
            return {
                "conversation_id": actor.conversation_id,
                "context_applied_seq": lane["context_applied_seq"],
                "classified_through_seq": lane["classified_through_seq"],
                "context_revision": lane["context_revision"],
                "context_view_revision": lane["context_view_revision"],
                "units": [dict(row) for row in units],
            }

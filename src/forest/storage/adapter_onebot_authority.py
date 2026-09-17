"""Clean OneBot Binding, connection-generation and platform-event authority."""

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

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_SELF_ID_RE = re.compile(r"[1-9][0-9]{0,18}\Z")
_LOCK_DOMAIN = b"forest.adapter.onebot/1\0"


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str):
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", f"Invalid {field}", 422)
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
            "ONEBOT_AUTHORITY_INPUT_INVALID", "OneBot input is not canonical", 422
        ) from exc


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value, field: str, *, optional: bool = False):
    if value is None and optional:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", f"{field} needs a timezone", 422)
    return value.astimezone(timezone.utc)


def _lock_key(bot_id: str, identity: str) -> int:
    digest = hashlib.sha256(
        _LOCK_DOMAIN + bot_id.encode() + b"\0" + identity.encode()
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


@dataclass(frozen=True)
class OneBotPayloadRef:
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
class OneBotPlatformEvent:
    event_id: str
    platform_message_ref: str
    event_kind: str
    channel_kind: str
    channel_ref: str
    conversation_ref: str
    author_subject_ref: str
    raw: OneBotPayloadRef
    normalized: OneBotPayloadRef
    author: OneBotPayloadRef
    received_at: datetime
    classification_deadline: datetime
    sent_at: datetime | None = None

    def validate(self) -> None:
        for value, field, limit in (
            (self.event_id, "event_id", 256),
            (self.platform_message_ref, "platform_message_ref", 512),
            (self.event_kind, "event_kind", 128),
            (self.channel_ref, "channel_ref", 256),
            (self.conversation_ref, "conversation_ref", 256),
            (self.author_subject_ref, "author_subject_ref", 256),
        ):
            _bounded(value, field, limit)
        if self.channel_kind not in {"GROUP", "PRIVATE"}:
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid OneBot channel kind", 422
            )
        for reference, field, kind in (
            (self.raw, "raw", "onebot.raw-event"),
            (self.normalized, "normalized", "onebot.normalized-event"),
            (self.author, "author", "onebot.author-snapshot"),
        ):
            reference.validate(field)
            if (
                reference.owner_component_id != "adapter.onebot"
                or reference.owner_kind != kind
                or reference.owner_id != self.event_id
            ):
                raise ForestError(
                    "ONEBOT_PAYLOAD_IDENTITY_CONFLICT",
                    f"{field} payload owner differs from the platform event",
                    409,
                )
        received = _timestamp(self.received_at, "received_at")
        deadline = _timestamp(self.classification_deadline, "classification_deadline")
        sent = _timestamp(self.sent_at, "sent_at", optional=True)
        if not received < deadline <= received + timedelta(minutes=15):
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "Classification deadline is outside its bound", 422
            )
        if sent is not None and sent > received + timedelta(minutes=5):
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "Platform sent_at is implausibly late", 422
            )

    def semantic(self) -> dict:
        return {
            "event_id": self.event_id,
            "platform_message_ref": self.platform_message_ref,
            "event_kind": self.event_kind,
            "channel_kind": self.channel_kind,
            "channel_ref": self.channel_ref,
            "conversation_ref": self.conversation_ref,
            "author_subject_ref": self.author_subject_ref,
            "raw": self.raw.canonical(),
            "normalized": self.normalized.canonical(),
            "author": self.author.canonical(),
            "sent_at": _timestamp(self.sent_at, "sent_at", optional=True).isoformat()
            if self.sent_at is not None
            else None,
        }


class OneBotAdapterRepository:
    """Own only Binding, connection and authenticated platform-event facts."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier: Callable[..., Awaitable[object]] | None = None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.adapter.onebot")
        self.payload_verifier = payload_verifier

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

    async def _verify_payload(self, conn, reference: OneBotPayloadRef) -> None:
        if self.payload_verifier is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "OneBot payload verifier is unavailable", 503
            )
        await self.payload_verifier(
            conn,
            payload_id=reference.payload_id,
            digest=reference.digest,
            owner_component_id=reference.owner_component_id,
            owner_kind=reference.owner_kind,
            owner_id=reference.owner_id,
        )

    async def _head(self, conn, binding_id: str, *, lock: str = "") -> dict:
        suffix = {"": sql.SQL(""), "share": sql.SQL("FOR SHARE"), "update": sql.SQL("FOR UPDATE")}
        if lock not in suffix:
            raise AssertionError("invalid internal lock mode")
        row = await (
            await conn.execute(
                sql.SQL("SELECT *,clock_timestamp() AS database_now FROM {} "
                        "WHERE bot_id=%s AND binding_id=%s {}").format(
                    self._table("binding_heads"), suffix[lock]
                ),
                (self.store.bot_id, binding_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("BINDING_NOT_FOUND", "OneBot Binding does not exist", 404)
        return dict(row)

    async def register_binding(
        self,
        *,
        operation_id: str,
        binding_id: str,
        self_id: str,
        display: OneBotPayloadRef,
    ) -> dict:
        for value, field in ((operation_id, "operation_id"), (binding_id, "binding_id")):
            _bounded(value, field, 256)
        if not isinstance(self_id, str) or _SELF_ID_RE.fullmatch(self_id) is None:
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid OneBot account identity", 422
            )
        if int(self_id) >= 2**63:
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "OneBot account identity is too large", 422
            )
        display.validate("display")
        if (
            display.owner_component_id,
            display.owner_kind,
            display.owner_id,
        ) != ("adapter.onebot", "binding.display", binding_id):
            raise ForestError(
                "ONEBOT_PAYLOAD_IDENTITY_CONFLICT", "Binding display payload owner differs", 409
            )
        input_digest = _digest(
            {"binding_id": binding_id, "self_id": self_id, "display": display.canonical()}
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "register:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND registry_operation_id=%s").format(
                        self._table("binding_heads")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["registry_input_digest"] != input_digest:
                    raise ForestError(
                        "BINDING_OPERATION_CONFLICT", "Binding registration input differs", 409
                    )
                return dict(replay)
            await self._verify_payload(conn, display)
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND "
                            "(binding_id=%s OR (platform='onebot-v11' AND self_id=%s)) "
                            "FOR UPDATE").format(self._table("binding_heads")),
                    (self.store.bot_id, binding_id, self_id),
                )
            ).fetchone()
            if occupied is not None:
                raise ForestError(
                    "BINDING_IDENTITY_CONFLICT",
                    "Binding ID or OneBot account is already registered",
                    409,
                )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,binding_id,self_id,registry_operation_id,"
                        "registry_input_digest,display_payload_id,display_payload_digest,"
                        "display_owner_component_id,display_owner_kind,display_owner_id) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("binding_heads")),
                    (
                        self.store.bot_id,
                        binding_id,
                        self_id,
                        operation_id,
                        input_digest,
                        display.payload_id,
                        display.digest,
                        display.owner_component_id,
                        display.owner_kind,
                        display.owner_id,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def plan_configuration(
        self,
        *,
        plan_id: str,
        binding_id: str,
        expected_revision: int,
        enabled: bool,
        receive_enabled: bool,
        transport: str,
        config: OneBotPayloadRef,
        secret_ref: str,
        secret_digest: str,
        actor_principal_ref: str,
        expires_at: datetime,
    ) -> dict:
        for value, field, limit in (
            (plan_id, "plan_id", 256),
            (binding_id, "binding_id", 256),
            (secret_ref, "secret_ref", 512),
            (actor_principal_ref, "actor_principal_ref", 256),
        ):
            _bounded(value, field, limit)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid expected revision", 422)
        if type(enabled) is not bool or type(receive_enabled) is not bool:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid Binding switches", 422)
        if receive_enabled and not enabled:
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "Disabled Binding cannot receive", 422
            )
        if transport not in {"HTTP_POST", "FORWARD_WEBSOCKET"}:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid transport", 422)
        _validated_digest(secret_digest, "secret_digest")
        expires_at = _timestamp(expires_at, "expires_at")
        config.validate("config")
        if (
            config.owner_component_id,
            config.owner_kind,
            config.owner_id,
        ) != ("adapter.onebot", "binding.config", plan_id):
            raise ForestError(
                "ONEBOT_PAYLOAD_IDENTITY_CONFLICT", "Binding config payload owner differs", 409
            )
        input_digest = _digest(
            {
                "binding_id": binding_id,
                "expected_revision": expected_revision,
                "enabled": enabled,
                "receive_enabled": receive_enabled,
                "transport": transport,
                "config": config.canonical(),
                "secret_ref": secret_ref,
                "secret_digest": secret_digest,
                "actor_principal_ref": actor_principal_ref,
                "expires_at": expires_at.isoformat(),
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "config-plan:" + plan_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND plan_id=%s").format(
                        self._table("binding_config_plans")
                    ),
                    (self.store.bot_id, plan_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "BINDING_PLAN_CONFLICT", "Binding config plan input differs", 409
                    )
                return dict(replay)
            await self._verify_payload(conn, config)
            head = await self._head(conn, binding_id, lock="share")
            if head["desired_revision"] != expected_revision:
                raise ForestError("BINDING_REVISION_CONFLICT", "Binding revision changed", 409)
            if not head["database_now"] < expires_at <= head["database_now"] + timedelta(
                minutes=15
            ):
                raise ForestError(
                    "ONEBOT_AUTHORITY_INPUT_INVALID", "Binding config plan expiry is invalid", 422
                )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,plan_id,input_digest,binding_id,expected_revision,"
                        "enabled,receive_enabled,transport,config_payload_id,"
                        "config_payload_digest,config_owner_component_id,config_owner_kind,"
                        "config_owner_id,secret_ref,secret_digest,actor_principal_ref,expires_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "RETURNING *"
                    ).format(self._table("binding_config_plans")),
                    (
                        self.store.bot_id,
                        plan_id,
                        input_digest,
                        binding_id,
                        expected_revision,
                        enabled,
                        receive_enabled,
                        transport,
                        config.payload_id,
                        config.digest,
                        config.owner_component_id,
                        config.owner_kind,
                        config.owner_id,
                        secret_ref,
                        secret_digest,
                        actor_principal_ref,
                        expires_at,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def apply_configuration(self, *, operation_id: str, plan_id: str) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(plan_id, "plan_id", 256)
        input_digest = _digest({"plan_id": plan_id})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "config-apply:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("binding_revisions")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest or replay["plan_id"] != plan_id:
                    raise ForestError(
                        "BINDING_OPERATION_CONFLICT", "Binding config operation input differs", 409
                    )
                return dict(replay)
            plan = await (
                await conn.execute(
                    sql.SQL("SELECT *,clock_timestamp() AS database_now FROM {} "
                            "WHERE bot_id=%s AND plan_id=%s").format(
                        self._table("binding_config_plans")
                    ),
                    (self.store.bot_id, plan_id),
                )
            ).fetchone()
            if plan is None:
                raise ForestError("BINDING_PLAN_NOT_FOUND", "Binding config plan does not exist", 404)
            head = await self._head(conn, plan["binding_id"], lock="update")
            if plan["database_now"] >= plan["expires_at"]:
                raise ForestError("BINDING_PLAN_EXPIRED", "Binding config plan expired", 409)
            if head["desired_revision"] != plan["expected_revision"]:
                raise ForestError("BINDING_REVISION_CONFLICT", "Binding revision changed", 409)
            revision = plan["expected_revision"] + 1
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,binding_id,revision,previous_revision,plan_id,"
                        "operation_id,input_digest,enabled,receive_enabled,transport,"
                        "config_payload_id,config_payload_digest,config_owner_component_id,"
                        "config_owner_kind,config_owner_id,secret_ref,secret_digest,"
                        "actor_principal_ref) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("binding_revisions")),
                    (
                        self.store.bot_id,
                        plan["binding_id"],
                        revision,
                        plan["expected_revision"],
                        plan_id,
                        operation_id,
                        input_digest,
                        plan["enabled"],
                        plan["receive_enabled"],
                        plan["transport"],
                        plan["config_payload_id"],
                        plan["config_payload_digest"],
                        plan["config_owner_component_id"],
                        plan["config_owner_kind"],
                        plan["config_owner_id"],
                        plan["secret_ref"],
                        plan["secret_digest"],
                        plan["actor_principal_ref"],
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET desired_revision=%s,desired_enabled=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND binding_id=%s").format(
                    self._table("binding_heads")
                ),
                (revision, plan["enabled"], self.store.bot_id, plan["binding_id"]),
            )
            return dict(row)

    async def prepare_client(
        self,
        *,
        client_incarnation: str,
        binding_id: str,
        revision: int,
        expected_binding_epoch: int,
        adapter_generation_id: str,
        executor_fence_digest: str,
    ) -> dict:
        for value, field in (
            (client_incarnation, "client_incarnation"),
            (binding_id, "binding_id"),
            (adapter_generation_id, "adapter_generation_id"),
        ):
            _bounded(value, field, 256)
        if type(revision) is not int or revision <= 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid config revision", 422)
        if type(expected_binding_epoch) is not int or expected_binding_epoch < 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid Binding epoch", 422)
        _validated_digest(executor_fence_digest, "executor_fence_digest")
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "client:" + client_incarnation)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND client_incarnation=%s").format(
                        self._table("binding_clients")
                    ),
                    (self.store.bot_id, client_incarnation),
                )
            ).fetchone()
            if existing is not None:
                expected = (
                    binding_id,
                    revision,
                    expected_binding_epoch,
                    adapter_generation_id,
                    executor_fence_digest,
                )
                actual = tuple(
                    existing[key]
                    for key in (
                        "binding_id",
                        "revision",
                        "expected_binding_epoch",
                        "adapter_generation_id",
                        "executor_fence_digest",
                    )
                )
                if actual != expected:
                    raise ForestError(
                        "BINDING_CLIENT_CONFLICT", "Client incarnation identity differs", 409
                    )
                return dict(existing)
            head = await self._head(conn, binding_id, lock="share")
            if head["binding_epoch"] != expected_binding_epoch:
                raise ForestError("STALE_BINDING", "Binding epoch changed", 409)
            revision_row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND binding_id=%s "
                            "AND revision=%s").format(self._table("binding_revisions")),
                    (self.store.bot_id, binding_id, revision),
                )
            ).fetchone()
            if (
                revision_row is None
                or head["desired_revision"] != revision
                or not revision_row["enabled"]
            ):
                raise ForestError(
                    "BINDING_CONFIG_SUPERSEDED", "Binding config cannot prepare a client", 409
                )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,client_incarnation,binding_id,revision,"
                        "expected_binding_epoch,adapter_generation_id,executor_fence_digest,"
                        "transport) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("binding_clients")),
                    (
                        self.store.bot_id,
                        client_incarnation,
                        binding_id,
                        revision,
                        expected_binding_epoch,
                        adapter_generation_id,
                        executor_fence_digest,
                        revision_row["transport"],
                    ),
                )
            ).fetchone()
            return dict(row)

    async def publish_client(
        self,
        *,
        operation_id: str,
        binding_id: str,
        client_incarnation: str,
        revision: int,
        expected_binding_epoch: int,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (binding_id, "binding_id"),
            (client_incarnation, "client_incarnation"),
        ):
            _bounded(value, field, 256)
        if type(revision) is not int or revision <= 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid config revision", 422)
        if type(expected_binding_epoch) is not int or expected_binding_epoch < 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid Binding epoch", 422)
        input_digest = _digest(
            {
                "binding_id": binding_id,
                "client_incarnation": client_incarnation,
                "revision": revision,
                "expected_binding_epoch": expected_binding_epoch,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "publication:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("binding_publications")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "BINDING_OPERATION_CONFLICT", "Binding publication input differs", 409
                    )
                return dict(replay)
            head = await self._head(conn, binding_id, lock="update")
            client = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND client_incarnation=%s "
                            "FOR UPDATE").format(self._table("binding_clients")),
                    (self.store.bot_id, client_incarnation),
                )
            ).fetchone()
            revision_row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND binding_id=%s "
                            "AND revision=%s").format(self._table("binding_revisions")),
                    (self.store.bot_id, binding_id, revision),
                )
            ).fetchone()
            if client is None or client["binding_id"] != binding_id:
                raise ForestError("BINDING_CLIENT_NOT_FOUND", "Client does not exist", 404)
            if (
                head["binding_epoch"] != expected_binding_epoch
                or client["expected_binding_epoch"] != expected_binding_epoch
                or client["revision"] != revision
                or head["desired_revision"] != revision
                or revision_row is None
                or not revision_row["enabled"]
            ):
                raise ForestError("STALE_BINDING", "Binding publication fence changed", 409)
            if client["state"] != "BUFFERING":
                raise ForestError("BINDING_CLIENT_CONFLICT", "Client is not buffering", 409)
            if head["current_client_incarnation"] is not None:
                raise ForestError("BINDING_DRAIN_REQUIRED", "Current client must retire first", 409)
            resulting_binding_epoch = expected_binding_epoch + 1
            resulting_connection_epoch = head["connection_epoch"] + 1
            now = head["database_now"]
            await conn.execute(
                sql.SQL("UPDATE {} SET state='ACTIVE',published_binding_epoch=%s,"
                        "connection_epoch=%s,activated_at=%s WHERE bot_id=%s "
                        "AND client_incarnation=%s").format(self._table("binding_clients")),
                (
                    resulting_binding_epoch,
                    resulting_connection_epoch,
                    now,
                    self.store.bot_id,
                    client_incarnation,
                ),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET effective_revision=%s,binding_epoch=%s,"
                        "connection_epoch=%s,state='ONLINE',effective_enabled=true,"
                        "receive_enabled=%s,current_client_incarnation=%s,error_code=NULL,"
                        "updated_at=%s WHERE bot_id=%s AND binding_id=%s").format(
                    self._table("binding_heads")
                ),
                (
                    revision,
                    resulting_binding_epoch,
                    resulting_connection_epoch,
                    revision_row["receive_enabled"],
                    client_incarnation,
                    now,
                    self.store.bot_id,
                    binding_id,
                ),
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,binding_id,revision,"
                        "publication_kind,client_incarnation,previous_binding_epoch,"
                        "resulting_binding_epoch,resulting_connection_epoch) VALUES "
                        "(%s,%s,%s,%s,%s,'CLIENT',%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("binding_publications")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        binding_id,
                        revision,
                        client_incarnation,
                        expected_binding_epoch,
                        resulting_binding_epoch,
                        resulting_connection_epoch,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def publish_disabled(
        self,
        *,
        operation_id: str,
        binding_id: str,
        revision: int,
        expected_binding_epoch: int,
    ) -> dict:
        for value, field in ((operation_id, "operation_id"), (binding_id, "binding_id")):
            _bounded(value, field, 256)
        if type(revision) is not int or revision <= 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid config revision", 422)
        if type(expected_binding_epoch) is not int or expected_binding_epoch < 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid Binding epoch", 422)
        input_digest = _digest(
            {
                "binding_id": binding_id,
                "revision": revision,
                "expected_binding_epoch": expected_binding_epoch,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "publication:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("binding_publications")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "BINDING_OPERATION_CONFLICT", "Binding publication input differs", 409
                    )
                return dict(replay)
            head = await self._head(conn, binding_id, lock="update")
            revision_row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND binding_id=%s "
                            "AND revision=%s").format(self._table("binding_revisions")),
                    (self.store.bot_id, binding_id, revision),
                )
            ).fetchone()
            if (
                head["binding_epoch"] != expected_binding_epoch
                or head["desired_revision"] != revision
                or revision_row is None
                or revision_row["enabled"]
            ):
                raise ForestError("STALE_BINDING", "Disabled publication fence changed", 409)
            if head["current_client_incarnation"] is not None:
                raise ForestError("BINDING_DRAIN_REQUIRED", "Current client must retire first", 409)
            resulting_epoch = expected_binding_epoch + 1
            await conn.execute(
                sql.SQL("UPDATE {} SET effective_revision=%s,binding_epoch=%s,state='DISABLED',"
                        "effective_enabled=false,receive_enabled=false,error_code=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND binding_id=%s").format(
                    self._table("binding_heads")
                ),
                (revision, resulting_epoch, self.store.bot_id, binding_id),
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,binding_id,revision,"
                        "publication_kind,previous_binding_epoch,resulting_binding_epoch,"
                        "resulting_connection_epoch) VALUES "
                        "(%s,%s,%s,%s,%s,'DISABLED',%s,%s,%s) RETURNING *"
                    ).format(self._table("binding_publications")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        binding_id,
                        revision,
                        expected_binding_epoch,
                        resulting_epoch,
                        head["connection_epoch"],
                    ),
                )
            ).fetchone()
            return dict(row)

    async def observe_client(
        self,
        *,
        operation_id: str,
        client_incarnation: str,
        observed_state: str,
        error_code: str | None = None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(client_incarnation, "client_incarnation", 256)
        _bounded(error_code, "error_code", 128, optional=True)
        if observed_state not in {"RETIRING", "CLOSED", "FAILED"}:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid client state", 422)
        if (observed_state == "FAILED") != (error_code is not None):
            raise ForestError(
                "ONEBOT_AUTHORITY_INPUT_INVALID", "Client failure evidence is incomplete", 422
            )
        input_digest = _digest(
            {
                "client_incarnation": client_incarnation,
                "observed_state": observed_state,
                "error_code": error_code,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "client-observation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("binding_client_observations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "BINDING_OPERATION_CONFLICT", "Client observation input differs", 409
                    )
                return dict(replay)
            client = await (
                await conn.execute(
                    sql.SQL("SELECT *,clock_timestamp() AS database_now FROM {} WHERE bot_id=%s "
                            "AND client_incarnation=%s FOR UPDATE").format(
                        self._table("binding_clients")
                    ),
                    (self.store.bot_id, client_incarnation),
                )
            ).fetchone()
            if client is None:
                raise ForestError("BINDING_CLIENT_NOT_FOUND", "Client does not exist", 404)
            head = await self._head(conn, client["binding_id"], lock="update")
            previous = client["state"]
            allowed = {
                "BUFFERING": {"CLOSED", "FAILED"},
                "ACTIVE": {"RETIRING", "CLOSED", "FAILED"},
                "RETIRING": {"CLOSED", "FAILED"},
            }
            accepted = observed_state in allowed.get(previous, set())
            conflict_code = None if accepted else "CLIENT_ALREADY_SETTLED"
            resulting = observed_state if accepted else previous
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,client_incarnation,"
                        "observed_state,error_code,accepted,previous_state,resulting_state,"
                        "conflict_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("binding_client_observations")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        client_incarnation,
                        observed_state,
                        error_code,
                        accepted,
                        previous,
                        resulting,
                        conflict_code,
                    ),
                )
            ).fetchone()
            if not accepted:
                return dict(row)
            is_current = head["current_client_incarnation"] == client_incarnation
            if previous in {"ACTIVE", "RETIRING"} and not is_current:
                raise ForestError(
                    "BINDING_CLIENT_CONFLICT", "Published client is not the Binding head", 409
                )
            now = client["database_now"]
            if observed_state == "RETIRING":
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='RETIRING',retired_at=%s WHERE bot_id=%s "
                            "AND client_incarnation=%s").format(self._table("binding_clients")),
                    (now, self.store.bot_id, client_incarnation),
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='DRAINING',receive_enabled=false,"
                            "updated_at=%s WHERE bot_id=%s AND binding_id=%s").format(
                        self._table("binding_heads")
                    ),
                    (now, self.store.bot_id, client["binding_id"]),
                )
            else:
                await conn.execute(
                    sql.SQL("UPDATE {} SET state=%s,error_code=%s,closed_at=%s "
                            "WHERE bot_id=%s AND client_incarnation=%s").format(
                        self._table("binding_clients")
                    ),
                    (
                        observed_state,
                        error_code,
                        now,
                        self.store.bot_id,
                        client_incarnation,
                    ),
                )
                if is_current:
                    head_state = "OFFLINE" if head["effective_enabled"] else "DISABLED"
                    await conn.execute(
                        sql.SQL("UPDATE {} SET state=%s,receive_enabled=false,"
                                "current_client_incarnation=NULL,error_code=%s,updated_at=%s "
                                "WHERE bot_id=%s AND binding_id=%s").format(
                            self._table("binding_heads")
                        ),
                        (
                            head_state,
                            error_code,
                            now,
                            self.store.bot_id,
                            client["binding_id"],
                        ),
                    )
            return dict(row)

    async def record_platform_event(
        self,
        *,
        operation_id: str,
        binding_id: str,
        expected_binding_epoch: int,
        client_incarnation: str,
        event: OneBotPlatformEvent,
        _connection=None,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (binding_id, "binding_id"),
            (client_incarnation, "client_incarnation"),
        ):
            _bounded(value, field, 256)
        if type(expected_binding_epoch) is not int or expected_binding_epoch <= 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid Binding epoch", 422)
        if type(event) is not OneBotPlatformEvent:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid platform event", 422)
        event.validate()
        received = _timestamp(event.received_at, "received_at")
        sent = _timestamp(event.sent_at, "sent_at", optional=True)
        deadline = _timestamp(event.classification_deadline, "classification_deadline")
        semantic_digest = _digest(event.semantic())
        input_digest = _digest(
            {
                "binding_id": binding_id,
                "expected_binding_epoch": expected_binding_epoch,
                "client_incarnation": client_incarnation,
                "event": event.semantic(),
                "received_at": received.isoformat(),
                "classification_deadline": deadline.isoformat(),
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "event-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT o.* FROM {} o "
                            "WHERE o.bot_id=%s AND o.operation_id=%s").format(
                        self._table("event_admission_operations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "ONEBOT_EVENT_OPERATION_CONFLICT", "Event admission input differs", 409
                    )
                return dict(replay)
            await self._lock(
                conn,
                "event-dedupe:" + binding_id + ":" + event.platform_message_ref,
            )
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND binding_id=%s "
                            "AND platform_message_ref=%s").format(self._table("platform_events")),
                    (self.store.bot_id, binding_id, event.platform_message_ref),
                )
            ).fetchone()
            if existing is not None:
                if existing["semantic_digest"] != semantic_digest:
                    raise ForestError(
                        "ONEBOT_EVENT_IDENTITY_CONFLICT", "Platform message identity differs", 409
                    )
                row = await (
                    await conn.execute(
                        sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,event_id,duplicate) "
                                "VALUES (%s,%s,%s,%s,true) RETURNING *").format(
                            self._table("event_admission_operations")
                        ),
                        (self.store.bot_id, operation_id, input_digest, existing["event_id"]),
                    )
                ).fetchone()
                return dict(row)
            # Shared head lock lets unrelated events run concurrently while fencing retirement.
            head = await self._head(conn, binding_id, lock="share")
            if (
                head["binding_epoch"] != expected_binding_epoch
                or head["current_client_incarnation"] != client_incarnation
                or head["state"] != "ONLINE"
                or not head["receive_enabled"]
            ):
                raise ForestError("STALE_BINDING", "Binding is not accepting this event", 409)
            client = await (
                await conn.execute(
                    sql.SQL("SELECT state,published_binding_epoch FROM {} WHERE bot_id=%s "
                            "AND client_incarnation=%s").format(self._table("binding_clients")),
                    (self.store.bot_id, client_incarnation),
                )
            ).fetchone()
            if (
                client is None
                or client["state"] != "ACTIVE"
                or client["published_binding_epoch"] != expected_binding_epoch
            ):
                raise ForestError("STALE_BINDING", "OneBot client fence changed", 409)
            for reference in sorted(
                (event.raw, event.normalized, event.author), key=lambda item: item.payload_id
            ):
                await self._verify_payload(conn, reference)
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,event_id,semantic_digest,binding_id,binding_epoch,"
                    "client_incarnation,platform_message_ref,event_kind,channel_kind,channel_ref,"
                    "conversation_ref,author_subject_ref,raw_payload_id,raw_payload_digest,"
                    "raw_owner_component_id,raw_owner_kind,raw_owner_id,normalized_payload_id,"
                    "normalized_payload_digest,normalized_owner_component_id,"
                    "normalized_owner_kind,normalized_owner_id,author_payload_id,"
                    "author_payload_digest,author_owner_component_id,author_owner_kind,"
                    "author_owner_id,sent_at,received_at) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("platform_events")),
                (
                    self.store.bot_id,
                    event.event_id,
                    semantic_digest,
                    binding_id,
                    expected_binding_epoch,
                    client_incarnation,
                    event.platform_message_ref,
                    event.event_kind,
                    event.channel_kind,
                    event.channel_ref,
                    event.conversation_ref,
                    event.author_subject_ref,
                    event.raw.payload_id,
                    event.raw.digest,
                    event.raw.owner_component_id,
                    event.raw.owner_kind,
                    event.raw.owner_id,
                    event.normalized.payload_id,
                    event.normalized.digest,
                    event.normalized.owner_component_id,
                    event.normalized.owner_kind,
                    event.normalized.owner_id,
                    event.author.payload_id,
                    event.author.digest,
                    event.author.owner_component_id,
                    event.author.owner_kind,
                    event.author.owner_id,
                    sent,
                    received,
                ),
            )
            row = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,event_id,duplicate) "
                            "VALUES (%s,%s,%s,%s,false) RETURNING *").format(
                        self._table("event_admission_operations")
                    ),
                    (self.store.bot_id, operation_id, input_digest, event.event_id),
                )
            ).fetchone()
            return dict(row)

    async def record_platform_event_in(self, conn, **values) -> dict:
        """Record one authenticated event inside a caller-owned UnitOfWork."""

        return await self.record_platform_event(_connection=conn, **values)

    async def load_binding(self, binding_id: str) -> dict:
        _bounded(binding_id, "binding_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._head(conn, binding_id)

    async def verify_transport_fence_in(
        self,
        conn,
        *,
        binding_ref: str,
        binding_epoch: int,
        connection_epoch: int,
        client_incarnation: str,
        adapter_generation_id: str,
        executor_fence_digest: str,
    ) -> dict:
        for value, field in (
            (binding_ref, "binding_ref"),
            (client_incarnation, "client_incarnation"),
            (adapter_generation_id, "adapter_generation_id"),
        ):
            _bounded(value, field, 256)
        if type(binding_epoch) is not int or binding_epoch <= 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid Binding epoch", 422)
        if type(connection_epoch) is not int or connection_epoch <= 0:
            raise ForestError("ONEBOT_AUTHORITY_INPUT_INVALID", "Invalid connection epoch", 422)
        _validated_digest(executor_fence_digest, "executor_fence_digest")
        head = await self._head(conn, binding_ref, lock="share")
        client = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND client_incarnation=%s").format(
                    self._table("binding_clients")
                ),
                (self.store.bot_id, client_incarnation),
            )
        ).fetchone()
        if (
            head["state"] != "ONLINE"
            or not head["effective_enabled"]
            or head["binding_epoch"] != binding_epoch
            or head["connection_epoch"] != connection_epoch
            or head["current_client_incarnation"] != client_incarnation
            or client is None
            or client["state"] != "ACTIVE"
            or client["binding_id"] != binding_ref
            or client["published_binding_epoch"] != binding_epoch
            or client["connection_epoch"] != connection_epoch
            or client["adapter_generation_id"] != adapter_generation_id
            or client["executor_fence_digest"] != executor_fence_digest
        ):
            raise ForestError("STALE_BINDING", "OneBot transport fence changed", 409)
        return {"binding_id": binding_ref, "client_incarnation": client_incarnation}

    async def list_bindings(self) -> list[dict]:
        async with self.store.pool.connection() as conn, conn.transaction():
            rows = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s ORDER BY binding_id").format(
                        self._table("binding_heads")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchall()
            return [dict(row) for row in rows]

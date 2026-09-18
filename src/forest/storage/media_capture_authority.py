"""Private durable authority for asynchronous inbound media capture."""

from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.storage.memory_graph_authority import (
    MemoryPart,
    MemoryPayloadRef,
    MemorySourceIdentity,
)


def _bounded(value, field: str, limit: int = 256) -> None:
    if not isinstance(value, str) or not 1 <= len(value.encode()) <= limit:
        raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", f"Invalid {field}", 422)


def _canonical(value: object) -> bytes:
    def default(item):
        if isinstance(item, datetime):
            return item.isoformat()
        if hasattr(item, "canonical"):
            return item.canonical()
        raise TypeError(type(item).__name__)

    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=default,
        ).encode()
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Media capture is invalid", 422) from exc


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True)
class MediaImageCapture:
    part_id: str
    part_index: int
    media_ref: str
    locator: dict

    def validate(self) -> None:
        _bounded(self.part_id, "part_id")
        _bounded(self.media_ref, "media_ref")
        if type(self.part_index) is not int or not 0 <= self.part_index <= 4095:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid image part index", 422)
        if (
            not isinstance(self.locator, dict)
            or set(self.locator) != {"kind", "uri", "platform_file"}
            or self.locator.get("kind") != "onebot-source"
            or not any(self.locator.get(name) for name in ("uri", "platform_file"))
        ):
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid media locator", 422)
        for name in ("uri", "platform_file"):
            value = self.locator.get(name)
            if value is not None and (
                not isinstance(value, str) or len(value.encode()) > 8192
            ):
                raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid media locator", 422)

    def canonical(self) -> dict:
        return {
            "part_id": self.part_id,
            "part_index": self.part_index,
            "media_ref": self.media_ref,
            "locator": self.locator,
        }


@dataclass(frozen=True)
class IngressMediaCapture:
    capture_id: str
    identity: MemorySourceIdentity
    source_unit_id: str
    source_revision: int
    unit_kind: str
    author: MemoryPayloadRef
    text_parts: tuple[MemoryPart, ...]
    image_parts: tuple[MediaImageCapture, ...]
    occurred_at: datetime

    def validate(self) -> None:
        _bounded(self.capture_id, "capture_id")
        _bounded(self.source_unit_id, "source_unit_id")
        _bounded(self.unit_kind, "unit_kind", 128)
        self.identity.validate()
        self.author.validate("author")
        if type(self.source_revision) is not int or self.source_revision < 1:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid source revision", 422)
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid occurrence time", 422)
        parts = (*self.text_parts, *self.image_parts)
        if not 1 <= len(parts) <= 4096 or not self.image_parts:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid capture parts", 422)
        for part in self.text_parts:
            part.validate()
            if part.part_kind != "TEXT":
                raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Text part is not text", 422)
        for part in self.image_parts:
            part.validate()
        identities = [(part.part_id, part.part_index) for part in parts]
        if len({item[0] for item in identities}) != len(parts) or len(
            {item[1] for item in identities}
        ) != len(parts):
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Capture parts repeat", 422)

    def canonical(self) -> dict:
        return {
            "capture_id": self.capture_id,
            "identity": self.identity.canonical(),
            "source_unit_id": self.source_unit_id,
            "source_revision": self.source_revision,
            "unit_kind": self.unit_kind,
            "author": self.author.canonical(),
            "text_parts": [part.canonical() for part in self.text_parts],
            "image_parts": [part.canonical() for part in self.image_parts],
            "occurred_at": self.occurred_at,
        }


class MediaCaptureRepository:
    def __init__(self, store, *, namespace: str, payload_verifier) -> None:
        self.store = store
        self.namespace = namespace
        self.payload_verifier = payload_verifier

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    async def register_in(
        self,
        conn,
        *,
        operation_id: str,
        capture: IngressMediaCapture,
        author_subject_ref: str,
        binding_ref: str,
        conversation_ref: str,
        event_ref: str,
        ingress_seq: int,
        observed_at: datetime,
    ) -> dict:
        _bounded(operation_id, "operation_id")
        for value, field in (
            (author_subject_ref, "author_subject_ref"),
            (binding_ref, "binding_ref"),
            (conversation_ref, "conversation_ref"),
            (event_ref, "event_ref"),
        ):
            _bounded(value, field)
        if type(capture) is not IngressMediaCapture:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid media capture", 422)
        capture.validate()
        if type(ingress_seq) is not int or ingress_seq < 1:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid ingress sequence", 422)
        if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid observation time", 422)
        identity = {
            "operation_id": operation_id,
            "capture": capture.canonical(),
            "author_subject_ref": author_subject_ref,
            "binding_ref": binding_ref,
            "conversation_ref": conversation_ref,
            "event_ref": event_ref,
            "ingress_seq": ingress_seq,
        }
        input_digest = _digest(identity)
        existing = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND (capture_id=%s OR operation_id=%s) "
                    "FOR UPDATE"
                ).format(self._table("media_captures")),
                (self.store.bot_id, capture.capture_id, operation_id),
            )
        ).fetchone()
        if existing is not None:
            if (
                existing["capture_id"] != capture.capture_id
                or existing["operation_id"] != operation_id
                or existing["input_digest"] != input_digest
            ):
                raise ForestError("MEDIA_CAPTURE_CONFLICT", "Media capture identity differs", 409)
            return dict(existing)
        await self.payload_verifier(
            conn,
            payload_id=capture.author.payload_id,
            digest=capture.author.digest,
            owner_component_id=capture.author.owner_component_id,
            owner_kind=capture.author.owner_kind,
            owner_id=capture.author.owner_id,
        )
        for part in capture.text_parts:
            await self.payload_verifier(
                conn,
                payload_id=part.payload.payload_id,
                digest=part.payload.digest,
                owner_component_id=part.payload.owner_component_id,
                owner_kind=part.payload.owner_kind,
                owner_id=part.payload.owner_id,
            )
        source = capture.identity
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,capture_id,operation_id,input_digest,source_unit_id,"
                    "source_revision,source_id,source_kind,platform,realm_id,external_group_id,"
                    "trunk_id,unit_kind,author_subject_ref,author_payload_id,author_payload_digest,"
                    "author_owner_component_id,author_owner_kind,author_owner_id,binding_ref,"
                    "conversation_ref,event_ref,ingress_seq,observed_at,occurred_at) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s) RETURNING *"
                ).format(self._table("media_captures")),
                (
                    self.store.bot_id,
                    capture.capture_id,
                    operation_id,
                    input_digest,
                    capture.source_unit_id,
                    capture.source_revision,
                    source.source_id,
                    source.source_kind,
                    source.platform,
                    source.realm_id,
                    source.external_group_id,
                    source.trunk_id,
                    capture.unit_kind,
                    author_subject_ref,
                    capture.author.payload_id,
                    capture.author.digest,
                    capture.author.owner_component_id,
                    capture.author.owner_kind,
                    capture.author.owner_id,
                    binding_ref,
                    conversation_ref,
                    event_ref,
                    ingress_seq,
                    observed_at,
                    capture.occurred_at,
                ),
            )
        ).fetchone()
        for part in capture.text_parts:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,capture_id,part_id,part_index,part_kind,state,"
                    "payload_id,payload_digest,payload_owner_component_id,payload_owner_kind,"
                    "payload_owner_id,media_type,byte_length) VALUES "
                    "(%s,%s,%s,%s,'TEXT','READY',%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("media_capture_parts")),
                (
                    self.store.bot_id,
                    capture.capture_id,
                    part.part_id,
                    part.part_index,
                    part.payload.payload_id,
                    part.payload.digest,
                    part.payload.owner_component_id,
                    part.payload.owner_kind,
                    part.payload.owner_id,
                    part.media_type,
                    part.byte_length,
                ),
            )
        for part in capture.image_parts:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,capture_id,part_id,part_index,part_kind,state,"
                    "media_ref,locator) VALUES (%s,%s,%s,%s,'IMAGE','PENDING',%s,%s)"
                ).format(self._table("media_capture_parts")),
                (
                    self.store.bot_id,
                    capture.capture_id,
                    part.part_id,
                    part.part_index,
                    part.media_ref,
                    Jsonb(part.locator),
                ),
            )
        return dict(row)

    async def claim(self, *, owner_id: str, lease_seconds: int = 60) -> dict | None:
        _bounded(owner_id, "owner_id")
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid capture lease", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='PENDING',lease_owner=NULL,lease_deadline=NULL,"
                    "next_attempt_at=clock_timestamp(),updated_at=clock_timestamp(),"
                    "error_code='MEDIA_CAPTURE_LEASE_EXPIRED' WHERE bot_id=%s AND state='FETCHING' "
                    "AND lease_deadline<=clock_timestamp()"
                ).format(self._table("media_captures")),
                (self.store.bot_id,),
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND state='PENDING' "
                        "AND next_attempt_at<=clock_timestamp() ORDER BY next_attempt_at,capture_id "
                        "FOR UPDATE SKIP LOCKED LIMIT 1"
                    ).format(self._table("media_captures")),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if row is None:
                return None
            row = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='FETCHING',attempt_count=attempt_count+1,"
                        "lease_owner=%s,lease_deadline=clock_timestamp()+(%s * interval '1 second'),"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND capture_id=%s RETURNING *"
                    ).format(self._table("media_captures")),
                    (owner_id, lease_seconds, self.store.bot_id, row["capture_id"]),
                )
            ).fetchone()
            parts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND capture_id=%s ORDER BY part_index"
                    ).format(self._table("media_capture_parts")),
                    (self.store.bot_id, row["capture_id"]),
                )
            ).fetchall()
            return {**dict(row), "parts": [dict(part) for part in parts]}

    async def publish_image(
        self,
        *,
        capture_id: str,
        part_id: str,
        owner_id: str,
        payload: MemoryPayloadRef,
        media_type: str,
        width: int,
        height: int,
        byte_length: int,
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            capture = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND capture_id=%s FOR UPDATE"
                    ).format(self._table("media_captures")),
                    (self.store.bot_id, capture_id),
                )
            ).fetchone()
            part = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND capture_id=%s AND part_id=%s FOR UPDATE"
                    ).format(self._table("media_capture_parts")),
                    (self.store.bot_id, capture_id, part_id),
                )
            ).fetchone()
            if capture is None or part is None:
                raise ForestError("MEDIA_CAPTURE_NOT_FOUND", "Media capture is unavailable", 404)
            if capture["state"] != "FETCHING" or capture["lease_owner"] != owner_id:
                raise ForestError("MEDIA_CAPTURE_LEASE_LOST", "Media capture lease changed", 409)
            if part["state"] == "READY":
                if (
                    part["payload_id"] != payload.payload_id
                    or part["payload_digest"] != payload.digest
                    or part["media_type"] != media_type
                    or part["width"] != width
                    or part["height"] != height
                    or part["byte_length"] != byte_length
                ):
                    raise ForestError("MEDIA_ASSET_CONFLICT", "Media asset differs", 409)
                return dict(part)
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,media_ref,media_revision,capture_id,part_id,payload_id,"
                    "payload_digest,media_type,width,height,byte_length) VALUES "
                    "(%s,%s,1,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                ).format(self._table("media_assets")),
                (
                    self.store.bot_id,
                    part["media_ref"],
                    capture_id,
                    part_id,
                    payload.payload_id,
                    payload.digest,
                    media_type,
                    width,
                    height,
                    byte_length,
                ),
            )
            asset = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND media_ref=%s AND media_revision=1"
                    ).format(self._table("media_assets")),
                    (self.store.bot_id, part["media_ref"]),
                )
            ).fetchone()
            if asset is None or any(
                asset[field] != expected
                for field, expected in (
                    ("capture_id", capture_id),
                    ("part_id", part_id),
                    ("payload_id", payload.payload_id),
                    ("payload_digest", payload.digest),
                    ("media_type", media_type),
                    ("width", width),
                    ("height", height),
                    ("byte_length", byte_length),
                )
            ):
                raise ForestError("MEDIA_ASSET_CONFLICT", "Media asset differs", 409)
            return dict(
                await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='READY',payload_id=%s,payload_digest=%s,"
                            "payload_owner_component_id=%s,payload_owner_kind=%s,payload_owner_id=%s,"
                            "media_type=%s,width=%s,height=%s,byte_length=%s,error_code=NULL "
                            "WHERE bot_id=%s AND capture_id=%s AND part_id=%s RETURNING *"
                        ).format(self._table("media_capture_parts")),
                        (
                            payload.payload_id,
                            payload.digest,
                            payload.owner_component_id,
                            payload.owner_kind,
                            payload.owner_id,
                            media_type,
                            width,
                            height,
                            byte_length,
                            self.store.bot_id,
                            capture_id,
                            part_id,
                        ),
                    )
                ).fetchone()
            )

    async def fail_part(
        self, *, capture_id: str, part_id: str, owner_id: str, error_code: str, final: bool
    ) -> None:
        _bounded(error_code, "error_code", 128)
        async with self.store.pool.connection() as conn, conn.transaction():
            result = await conn.execute(
                sql.SQL(
                    "UPDATE {} p SET state=%s,error_code=%s FROM {} c WHERE p.bot_id=c.bot_id "
                    "AND p.capture_id=c.capture_id AND p.bot_id=%s AND p.capture_id=%s "
                    "AND p.part_id=%s AND p.state='PENDING' AND c.state='FETCHING' "
                    "AND c.lease_owner=%s"
                ).format(self._table("media_capture_parts"), self._table("media_captures")),
                (
                    "FAILED" if final else "PENDING",
                    error_code,
                    self.store.bot_id,
                    capture_id,
                    part_id,
                    owner_id,
                ),
            )
            if result.rowcount != 1:
                raise ForestError("MEDIA_CAPTURE_LEASE_LOST", "Media capture lease changed", 409)

    async def retry(self, *, capture_id: str, owner_id: str, error_code: str, delay: int) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            result = await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='PENDING',lease_owner=NULL,lease_deadline=NULL,error_code=%s,"
                    "next_attempt_at=clock_timestamp()+(%s * interval '1 second'),"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND capture_id=%s "
                    "AND state='FETCHING' AND lease_owner=%s"
                ).format(self._table("media_captures")),
                (error_code, delay, self.store.bot_id, capture_id, owner_id),
            )
            if result.rowcount != 1:
                raise ForestError("MEDIA_CAPTURE_LEASE_LOST", "Media capture lease changed", 409)

    async def settle(
        self, *, capture_id: str, owner_id: str, state: str, error_code: str | None = None
    ) -> dict:
        if state not in {"READY", "PARTIAL", "FAILED", "CANCELLED"}:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Invalid terminal state", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,lease_owner=NULL,lease_deadline=NULL,error_code=%s,"
                        "settled_at=clock_timestamp(),updated_at=clock_timestamp() WHERE bot_id=%s "
                        "AND capture_id=%s AND state='FETCHING' AND lease_owner=%s RETURNING *"
                    ).format(self._table("media_captures")),
                    (state, error_code, self.store.bot_id, capture_id, owner_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("MEDIA_CAPTURE_LEASE_LOST", "Media capture lease changed", 409)
            return dict(row)

    async def asset_rows(self, media_refs) -> tuple[dict, ...]:
        refs = tuple(dict.fromkeys(media_refs))
        if not refs:
            return ()
        if len(refs) > 256:
            raise ForestError("MEDIA_CAPTURE_INPUT_INVALID", "Too many media references", 422)
        for ref in refs:
            _bounded(ref, "media_ref")
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND media_ref=ANY(%s) AND media_revision=1"
                    ).format(self._table("media_assets")),
                    (self.store.bot_id, list(refs)),
                )
            ).fetchall()
        return tuple(dict(row) for row in rows)

    async def load(self, capture_id: str) -> dict:
        _bounded(capture_id, "capture_id")
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND capture_id=%s").format(
                        self._table("media_captures")
                    ),
                    (self.store.bot_id, capture_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("MEDIA_CAPTURE_NOT_FOUND", "Media capture is unavailable", 404)
            parts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND capture_id=%s ORDER BY part_index"
                    ).format(self._table("media_capture_parts")),
                    (self.store.bot_id, capture_id),
                )
            ).fetchall()
        return {**dict(row), "parts": [dict(part) for part in parts]}

    @staticmethod
    def memory_values(claim: dict) -> tuple[MemorySourceIdentity, MemoryPayloadRef, tuple[MemoryPart, ...]]:
        identity = MemorySourceIdentity(
            claim["source_id"],
            claim["source_kind"],
            platform=claim["platform"],
            realm_id=claim["realm_id"],
            external_group_id=claim["external_group_id"],
            trunk_id=claim["trunk_id"],
        )
        author = MemoryPayloadRef(
            claim["author_payload_id"],
            claim["author_payload_digest"],
            claim["author_owner_component_id"],
            claim["author_owner_kind"],
            claim["author_owner_id"],
        )
        parts = tuple(
            MemoryPart(
                part["part_id"],
                part["part_index"],
                part["part_kind"],
                part["media_type"],
                MemoryPayloadRef(
                    part["payload_id"],
                    part["payload_digest"],
                    part["payload_owner_component_id"],
                    part["payload_owner_kind"],
                    part["payload_owner_id"],
                ),
                width=part["width"],
                height=part["height"],
                byte_length=part["byte_length"],
            )
            for part in claim["parts"]
            if part["state"] == "READY"
        )
        return identity, author, parts

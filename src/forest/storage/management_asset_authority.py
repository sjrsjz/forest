"""Generation-bound, bounded management uploads owned by kernel.payload."""

from __future__ import annotations

import hashlib
import io
import warnings

from PIL import Image, UnidentifiedImageError
from psycopg import sql

from forest.errors import ForestError
from forest.storage.payload_authority import _bounded, _canonical, _digest

CHUNK_BYTES = 256 * 1024
MAX_ASSET_BYTES = 8 * 1024 * 1024
MAX_ACTIVE_PER_ACTOR = 4
MAX_ACTIVE_PER_BOT = 32
MAX_RESERVED_PER_BOT = 128 * 1024 * 1024
_FORMATS = {
    "image/png": "PNG",
    "image/jpeg": "JPEG",
    "image/webp": "WEBP",
}


def management_asset_owner_id(prefix: str, target_ref: str, part_id: str) -> str:
    _bounded(prefix, "owner_prefix", 32)
    _bounded(target_ref, "target_ref", 256)
    _bounded(part_id, "part_id", 256)
    return prefix + ":" + hashlib.sha256(
        target_ref.encode("utf-8") + b"\0" + part_id.encode("utf-8")
    ).hexdigest()


def inspect_management_asset(content: bytes, media_type: str) -> tuple[int | None, int | None]:
    """Derive image dimensions from accepted bytes; never trust browser metadata."""

    if media_type == "text/plain":
        try:
            content.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ForestError("MANAGEMENT_ASSET_INVALID", "Text asset is not UTF-8", 422) from exc
        return None, None
    expected_format = _FORMATS.get(media_type)
    if expected_format is None:
        raise ForestError("MANAGEMENT_ASSET_INVALID", "Asset media type is unsupported", 422)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(content)) as image:
                width, height = image.size
                if (
                    image.format != expected_format
                    or not 1 <= width <= 100000
                    or not 1 <= height <= 100000
                    or width * height > 20_000_000
                ):
                    raise ForestError(
                        "MANAGEMENT_ASSET_INVALID", "Image format or dimensions are invalid", 422
                    )
                image.load()
                return width, height
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombWarning,
            Image.DecompressionBombError) as exc:
        raise ForestError("MANAGEMENT_ASSET_INVALID", "Image bytes are invalid", 422) from exc


class ManagementAssetAuthority:
    """Store upload sessions and chunks beside Payload, then publish one exact ref."""

    def __init__(self, payloads) -> None:
        self.payloads = payloads
        self.store = payloads.store

    def _table(self, name: str):
        return self.payloads._table(name)

    @staticmethod
    def _identity(values: dict) -> str:
        return _digest(_canonical(values))

    async def _upload_in(
        self, conn, upload_id: str, *, component_id: str, asset_id: str,
        actor_subject_ref: str, generation_id: str, lock: bool = False,
    ):
        row = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND upload_id=%s {}").format(
                self._table("management_asset_uploads"),
                sql.SQL("FOR UPDATE" if lock else ""),
            ), (self.store.bot_id, upload_id),
        )).fetchone()
        if row is None:
            raise ForestError("MANAGEMENT_ASSET_NOT_FOUND", "Upload does not exist", 404)
        if (
            row["component_id"] != component_id
            or row["asset_id"] != asset_id
            or row["actor_subject_ref"] != actor_subject_ref
            or row["generation_id"] != generation_id
        ):
            raise ForestError("MANAGEMENT_ASSET_IDENTITY_CONFLICT", "Upload belongs to another authority", 409)
        return row

    async def begin_in(
        self, conn, *, upload_id: str, component_id: str, asset_id: str,
        actor_subject_ref: str, generation_id: str, target_ref: str, part_id: str,
        owner_kind: str, owner_id: str, media_type: str, byte_length: int,
        content_digest: str, chunk_count: int, allowed_media_types: tuple[str, ...],
        max_bytes: int,
    ) -> dict:
        for name, value, limit in (
            ("upload_id", upload_id, 128), ("component_id", component_id, 128),
            ("asset_id", asset_id, 128), ("actor_subject_ref", actor_subject_ref, 128),
            ("generation_id", generation_id, 256), ("target_ref", target_ref, 256),
            ("part_id", part_id, 256), ("owner_kind", owner_kind, 128),
            ("owner_id", owner_id, 512),
        ):
            _bounded(value, name, limit)
        if (
            media_type not in allowed_media_types
            or media_type not in ("text/plain", *_FORMATS)
            or type(max_bytes) is not int
            or not 1 <= max_bytes <= MAX_ASSET_BYTES
            or type(byte_length) is not int
            or not 1 <= byte_length <= min(MAX_ASSET_BYTES, max_bytes)
            or (media_type == "text/plain" and byte_length > 65536)
            or type(chunk_count) is not int
            or chunk_count != (byte_length + CHUNK_BYTES - 1) // CHUNK_BYTES
            or not isinstance(content_digest, str)
            or len(content_digest) != 71
            or not content_digest.startswith("sha256:")
            or any(char not in "0123456789abcdef" for char in content_digest[7:])
        ):
            raise ForestError("MANAGEMENT_ASSET_INVALID", "Upload declaration is invalid", 422)
        identity = {
            "upload_id": upload_id, "component_id": component_id,
            "asset_id": asset_id, "actor_subject_ref": actor_subject_ref,
            "generation_id": generation_id, "target_ref": target_ref,
            "part_id": part_id, "owner_kind": owner_kind, "owner_id": owner_id,
            "media_type": media_type, "byte_length": byte_length,
            "content_digest": content_digest, "chunk_count": chunk_count,
        }
        input_digest = self._identity(identity)
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("forest.kernel.payload/management-assets/" + self.store.bot_id,),
        )
        existing = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND upload_id=%s").format(
                self._table("management_asset_uploads")
            ), (self.store.bot_id, upload_id),
        )).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("MANAGEMENT_ASSET_IDENTITY_CONFLICT", "Upload ID has another input", 409)
            if (existing["payload_expires_at"] or existing["upload_expires_at"]) <= await self._now_in(conn):
                raise ForestError("MANAGEMENT_ASSET_EXPIRED", "Upload has expired", 410)
            return self._status(existing)
        counts = await (await conn.execute(
            sql.SQL(
                "SELECT count(*) FILTER (WHERE state='OPEN') AS total, "
                "count(*) FILTER (WHERE state='OPEN' AND actor_subject_ref=%s) "
                "AS actor_count, COALESCE(sum(byte_length),0) AS reserved_bytes FROM {} "
                "WHERE bot_id=%s AND ((state='OPEN' AND upload_expires_at>clock_timestamp()) "
                "OR (state='FINALIZED' AND payload_expires_at>clock_timestamp()))"
            ).format(self._table("management_asset_uploads")),
            (actor_subject_ref, self.store.bot_id),
        )).fetchone()
        if (
            counts["total"] >= MAX_ACTIVE_PER_BOT
            or counts["actor_count"] >= MAX_ACTIVE_PER_ACTOR
            or counts["reserved_bytes"] + byte_length > MAX_RESERVED_PER_BOT
        ):
            raise ForestError("MANAGEMENT_ASSET_RATE_LIMITED", "Too many active uploads", 429)
        window = await (await conn.execute(
            sql.SQL(
                "INSERT INTO {} AS w (bot_id,actor_subject_ref,window_start,"
                "admission_count,bytes_reserved) VALUES "
                "(%s,%s,date_trunc('minute',clock_timestamp()),1,%s) "
                "ON CONFLICT (bot_id,actor_subject_ref,window_start) DO UPDATE "
                "SET admission_count=w.admission_count+1,"
                "bytes_reserved=w.bytes_reserved+EXCLUDED.bytes_reserved "
                "WHERE w.admission_count<8 AND "
                "w.bytes_reserved+EXCLUDED.bytes_reserved<=33554432 "
                "RETURNING admission_count"
            ).format(self._table("management_asset_admission_windows")),
            (self.store.bot_id, actor_subject_ref, byte_length),
        )).fetchone()
        if window is None:
            raise ForestError("MANAGEMENT_ASSET_RATE_LIMITED", "Upload admission rate exceeded", 429)
        inserted = await (await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,upload_id,input_digest,component_id,asset_id,"
                "actor_subject_ref,generation_id,target_ref,part_id,owner_kind,owner_id,"
                "media_type,byte_length,content_digest,chunk_count,upload_expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                "clock_timestamp()+interval '5 minutes') ON CONFLICT DO NOTHING "
                "RETURNING *"
            ).format(self._table("management_asset_uploads")),
            (self.store.bot_id, upload_id, input_digest, component_id, asset_id,
             actor_subject_ref, generation_id, target_ref, part_id, owner_kind,
             owner_id, media_type, byte_length, content_digest, chunk_count),
        )).fetchone()
        if inserted is None:
            raise ForestError("MANAGEMENT_ASSET_OWNER_CONFLICT", "Asset owner already has an upload", 409)
        return self._status(inserted)

    @staticmethod
    def _status(row) -> dict:
        return {
            "upload_id": row["upload_id"], "state": row["state"],
            "upload_expires_at": row["upload_expires_at"].isoformat(),
            "payload_expires_at": (
                row["payload_expires_at"].isoformat()
                if row["payload_expires_at"] is not None else None
            ),
        }

    async def put_chunk_in(
        self, conn, *, upload_id: str, component_id: str, asset_id: str,
        actor_subject_ref: str, generation_id: str, chunk_index: int,
        content: bytes, content_digest: str,
    ) -> dict:
        upload = await self._upload_in(
            conn, upload_id, component_id=component_id, asset_id=asset_id,
            actor_subject_ref=actor_subject_ref, generation_id=generation_id, lock=True,
        )
        if upload["state"] != "OPEN" or upload["upload_expires_at"] <= await self._now_in(conn):
            raise ForestError("MANAGEMENT_ASSET_EXPIRED", "Upload is no longer writable", 410)
        if (
            type(chunk_index) is not int
            or not 0 <= chunk_index < upload["chunk_count"]
            or type(content) is not bytes
        ):
            raise ForestError("MANAGEMENT_ASSET_CHUNK_INVALID", "Upload chunk differs from its declaration", 422)
        expected_length = min(CHUNK_BYTES, upload["byte_length"] - chunk_index * CHUNK_BYTES)
        if len(content) != expected_length or _digest(content) != content_digest:
            raise ForestError("MANAGEMENT_ASSET_CHUNK_INVALID", "Upload chunk differs from its declaration", 422)
        inserted = await (await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,upload_id,chunk_index,byte_length,"
                "content_digest,content) VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING chunk_index"
            ).format(self._table("management_asset_chunks")),
            (self.store.bot_id, upload_id, chunk_index, len(content), content_digest, content),
        )).fetchone()
        if inserted is None:
            existing = await (await conn.execute(
                sql.SQL("SELECT byte_length,content_digest FROM {} "
                        "WHERE bot_id=%s AND upload_id=%s AND chunk_index=%s").format(
                    self._table("management_asset_chunks")
                ), (self.store.bot_id, upload_id, chunk_index),
            )).fetchone()
            if existing is None or existing["byte_length"] != len(content) or existing["content_digest"] != content_digest:
                raise ForestError("MANAGEMENT_ASSET_CHUNK_CONFLICT", "Chunk index has other bytes", 409)
        return {"upload_id": upload_id, "chunk_index": chunk_index, "digest": content_digest}

    async def finalize_in(
        self, conn, *, upload_id: str, component_id: str, asset_id: str,
        actor_subject_ref: str, generation_id: str,
    ) -> dict:
        upload = await self._upload_in(
            conn, upload_id, component_id=component_id, asset_id=asset_id,
            actor_subject_ref=actor_subject_ref, generation_id=generation_id, lock=True,
        )
        now = await self._now_in(conn)
        if upload["state"] == "FINALIZED":
            if upload["payload_expires_at"] <= now:
                raise ForestError("MANAGEMENT_ASSET_EXPIRED", "Asset has expired", 410)
            ref = await self.payloads.verify_in(
                conn, payload_id=upload["payload_id"], digest=upload["content_digest"],
                owner_component_id=component_id, owner_kind=upload["owner_kind"],
                owner_id=upload["owner_id"],
            )
            return self._finalized(upload, ref)
        if upload["upload_expires_at"] <= now:
            raise ForestError("MANAGEMENT_ASSET_EXPIRED", "Upload has expired", 410)
        chunks = await (await conn.execute(
            sql.SQL("SELECT chunk_index,content FROM {} WHERE bot_id=%s AND upload_id=%s "
                    "ORDER BY chunk_index").format(self._table("management_asset_chunks")),
            (self.store.bot_id, upload_id),
        )).fetchall()
        if len(chunks) != upload["chunk_count"] or any(
            row["chunk_index"] != index for index, row in enumerate(chunks)
        ):
            raise ForestError("MANAGEMENT_ASSET_INCOMPLETE", "Upload chunks are incomplete", 409)
        content = b"".join(bytes(row["content"]) for row in chunks)
        if len(content) != upload["byte_length"] or _digest(content) != upload["content_digest"]:
            raise ForestError("MANAGEMENT_ASSET_DIGEST_CONFLICT", "Asset bytes differ from the declaration", 409)
        width, height = inspect_management_asset(content, upload["media_type"])
        ref = await self.payloads.put_bytes_in(
            conn, owner_component_id=component_id, owner_kind=upload["owner_kind"],
            owner_id=upload["owner_id"], media_type=upload["media_type"],
            content=content, expected_deletion_epoch=await self.payloads.epoch_in(conn),
        )
        finalized = await (await conn.execute(
            sql.SQL(
                "UPDATE {} SET state='FINALIZED',payload_id=%s,width=%s,height=%s,"
                "finalized_at=clock_timestamp(),"
                "payload_expires_at=clock_timestamp()+interval '30 minutes' "
                "WHERE bot_id=%s AND upload_id=%s RETURNING *"
            ).format(self._table("management_asset_uploads")),
            (ref.payload_id, width, height, self.store.bot_id, upload_id),
        )).fetchone()
        await conn.execute(
            sql.SQL("DELETE FROM {} WHERE bot_id=%s AND upload_id=%s").format(
                self._table("management_asset_chunks")
            ), (self.store.bot_id, upload_id),
        )
        return self._finalized(finalized, ref)

    @staticmethod
    def _finalized(row, ref) -> dict:
        return {
            "upload_id": row["upload_id"], "state": "FINALIZED",
            "target_ref": row["target_ref"], "part_id": row["part_id"],
            "media_type": ref.media_type, "byte_length": ref.byte_length,
            "width": row["width"], "height": row["height"],
            "payload_expires_at": row["payload_expires_at"].isoformat(),
            "payload": {
                "payload_id": ref.payload_id, "digest": ref.content_digest,
                "owner_component_id": row["component_id"],
                "owner_kind": row["owner_kind"], "owner_id": row["owner_id"],
            },
        }

    async def verify_asset_in(
        self, conn, *, payload_id: str, digest: str, component_id: str,
        asset_id: str, actor_subject_ref: str, generation_id: str,
        target_ref: str, part_id: str, min_remaining_seconds: int = 0,
    ) -> dict:
        if type(min_remaining_seconds) is not int or not 0 <= min_remaining_seconds <= 1800:
            raise ForestError("MANAGEMENT_ASSET_INVALID", "Invalid asset lifetime bound", 422)
        row = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND payload_id=%s "
                    "AND component_id=%s AND asset_id=%s AND actor_subject_ref=%s "
                    "AND generation_id=%s AND target_ref=%s AND part_id=%s "
                    "AND state='FINALIZED' AND "
                    "payload_expires_at>clock_timestamp()+(%s * interval '1 second')").format(
                self._table("management_asset_uploads")
            ), (self.store.bot_id, payload_id, component_id, asset_id,
                actor_subject_ref, generation_id, target_ref, part_id,
                min_remaining_seconds),
        )).fetchone()
        if row is None:
            raise ForestError("MANAGEMENT_ASSET_IDENTITY_CONFLICT", "Asset is not bound to this action", 409)
        ref = await self.payloads.verify_in(
            conn, payload_id=payload_id, digest=digest,
            owner_component_id=component_id, owner_kind=row["owner_kind"],
            owner_id=row["owner_id"],
        )
        return self._finalized(row, ref)

    async def reap_expired(self, *, limit: int = 16) -> dict:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ForestError("MANAGEMENT_ASSET_INVALID", "Invalid cleanup limit", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            rows = await (await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND "
                    "((state='OPEN' AND upload_expires_at<=clock_timestamp()) OR "
                    "(state='FINALIZED' AND payload_expires_at<=clock_timestamp())) "
                    "ORDER BY COALESCE(payload_expires_at,upload_expires_at),upload_id "
                    "FOR UPDATE SKIP LOCKED LIMIT %s"
                ).format(self._table("management_asset_uploads")),
                (self.store.bot_id, limit),
            )).fetchall()
            for row in rows:
                if row["state"] == "FINALIZED":
                    operation_id = "management-asset-expire:" + hashlib.sha256(
                        (self.store.bot_id + "\0" + row["upload_id"]).encode()
                    ).hexdigest()
                    await self.payloads.forget_in(
                        conn, operation_id=operation_id, payload_ids=(row["payload_id"],)
                    )
                await conn.execute(
                    sql.SQL("DELETE FROM {} WHERE bot_id=%s AND upload_id=%s").format(
                        self._table("management_asset_chunks")
                    ), (self.store.bot_id, row["upload_id"]),
                )
                await conn.execute(
                    sql.SQL("DELETE FROM {} WHERE bot_id=%s AND upload_id=%s").format(
                        self._table("management_asset_uploads")
                    ), (self.store.bot_id, row["upload_id"]),
                )
            await conn.execute(
                sql.SQL(
                    "DELETE FROM {} WHERE (bot_id,actor_subject_ref,window_start) IN "
                    "(SELECT bot_id,actor_subject_ref,window_start FROM {} "
                    "WHERE bot_id=%s AND window_start<clock_timestamp()-interval '1 day' "
                    "ORDER BY window_start LIMIT 256)"
                ).format(self._table("management_asset_admission_windows"),
                         self._table("management_asset_admission_windows")),
                (self.store.bot_id,),
            )
            return {"reaped": len(rows)}

    @staticmethod
    async def _now_in(conn):
        return (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]

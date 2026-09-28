"""Clean namespace repository for the required ``kernel.payload`` module."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

MAX_BYTES = 32 * 1024 * 1024
MAX_SOURCES = 4096
MAX_PARENTS = 256
MAX_FORGET_SELECTION = 100_000


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("PAYLOAD_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError("PAYLOAD_INPUT_INVALID", "Value is not canonical JSON", 422) from exc


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class PayloadSource:
    source_component_id: str
    source_kind: str
    source_id: str
    source_revision: int
    part_id: str = "*"
    source_trunk_id: str | None = None
    observation_conversation_id: str | None = None
    observation_binding_id: str | None = None
    author_subject_id: str | None = None
    occurred_at: datetime | None = None
    propagation_class: str = "bot"

    def identity(self):
        return (
            self.source_component_id,
            self.source_kind,
            self.source_id,
            self.source_revision,
            self.part_id,
        )

    def canonical(self) -> dict:
        result = asdict(self)
        result["occurred_at"] = (
            self.occurred_at.astimezone(timezone.utc).isoformat()
            if self.occurred_at is not None
            else None
        )
        return result

    def validate(self) -> None:
        _bounded(self.source_component_id, "source_component_id", 128)
        _bounded(self.source_kind, "source_kind", 64)
        _bounded(self.source_id, "source_id", 256)
        if type(self.source_revision) is not int or self.source_revision <= 0:
            raise ForestError("PAYLOAD_INPUT_INVALID", "Invalid source_revision", 422)
        _bounded(self.part_id, "part_id", 256)
        for field in (
            "source_trunk_id",
            "observation_conversation_id",
            "observation_binding_id",
            "author_subject_id",
        ):
            _bounded(getattr(self, field), field, 256, optional=True)
        if self.occurred_at is not None and (
            not isinstance(self.occurred_at, datetime)
            or self.occurred_at.tzinfo is None
        ):
            raise ForestError("PAYLOAD_INPUT_INVALID", "occurred_at must have a timezone", 422)
        _bounded(self.propagation_class, "propagation_class", 64)


@dataclass(frozen=True)
class SourceTarget:
    source_component_id: str
    source_kind: str
    source_id: str
    source_revision: int
    part_id: str = "*"

    def validate(self) -> None:
        PayloadSource(**asdict(self)).validate()


@dataclass(frozen=True)
class CleanPayloadRef:
    payload_id: str
    content_digest: str
    media_type: str
    byte_length: int
    source_manifest_id: str
    source_manifest_digest: str
    state: str
    accepted_deletion_epoch: int


class KernelPayloadRepository:
    """Own payload bodies and lineage without acquiring caller business semantics."""

    def __init__(self, store, *, namespace: str | None = None) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.kernel.payload")

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _clock_in(self, conn, *, expected=None, update=False):
        row = await (
            await conn.execute(
                sql.SQL("SELECT deletion_epoch FROM {} WHERE bot_id=%s {}").format(
                    self._table("deletion_clock"),
                    sql.SQL("FOR UPDATE" if update else "FOR SHARE"),
                ),
                (self.store.bot_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("PAYLOAD_AUTHORITY_MISSING", "Payload clock is unavailable", 503)
        if expected is not None and (
            type(expected) is not int or expected < 0 or row["deletion_epoch"] != expected
        ):
            raise ForestError("DELETION_CONFLICT", "Payload deletion epoch changed", 409)
        return row["deletion_epoch"]

    async def epoch(self) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._clock_in(conn)

    async def epoch_in(self, conn, *, expected=None, update=False) -> int:
        """Expose the deletion clock to registered UnitOfWork participants."""

        return await self._clock_in(conn, expected=expected, update=update)

    @staticmethod
    def _source_set(sources) -> tuple[PayloadSource, ...]:
        items = tuple(sources)
        if len(items) > MAX_SOURCES or any(type(item) is not PayloadSource for item in items):
            raise ForestError("PAYLOAD_INPUT_INVALID", "Invalid source manifest", 422)
        merged: dict[tuple, PayloadSource] = {}
        for item in items:
            item.validate()
            previous = merged.setdefault(item.identity(), item)
            if previous != item:
                raise ForestError(
                    "SOURCE_IDENTITY_CONFLICT",
                    "One source identity has conflicting provenance metadata",
                    409,
                )
        return tuple(merged[key] for key in sorted(merged))

    async def _sources_in(self, conn, payload_id: str) -> tuple[PayloadSource, ...]:
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT i.* FROM {} h JOIN {} i "
                    "ON i.bot_id=h.bot_id AND i.manifest_id=h.source_manifest_id "
                    "WHERE h.bot_id=%s AND h.payload_id=%s ORDER BY i.item_index"
                ).format(
                    self._table("payload_headers"),
                    self._table("source_manifest_items"),
                ),
                (self.store.bot_id, payload_id),
            )
        ).fetchall()
        return tuple(
            PayloadSource(
                **{
                    field: row[field]
                    for field in PayloadSource.__dataclass_fields__
                }
            )
            for row in rows
        )

    async def put_bytes_in(
        self,
        conn,
        *,
        owner_component_id: str,
        owner_kind: str,
        owner_id: str,
        media_type: str,
        content: bytes,
        sources=(),
        parents=(),
        expected_deletion_epoch: int,
    ) -> CleanPayloadRef:
        _bounded(owner_component_id, "owner_component_id", 128)
        _bounded(owner_kind, "owner_kind", 128)
        _bounded(owner_id, "owner_id", 512)
        _bounded(media_type, "media_type", 128)
        if type(content) is not bytes or len(content) > MAX_BYTES:
            raise ForestError("PAYLOAD_TOO_LARGE", "Payload exceeds the 32 MiB limit", 413)
        direct_sources = self._source_set(sources)
        parent_ids = tuple(sorted(set(parents)))
        if len(parent_ids) > MAX_PARENTS:
            raise ForestError("PAYLOAD_DEPENDENCY_LIMIT", "Too many payload parents", 422)
        for parent_id in parent_ids:
            _bounded(parent_id, "parent_payload_id", 256)

        # Admissions share the deletion fence, so unrelated groups can write in
        # parallel. Forget advances the same row under FOR UPDATE and therefore
        # linearizes either before every admission or after all of them.
        current_epoch = await self._clock_in(conn)
        merged = {source.identity(): source for source in direct_sources}
        for parent_id in parent_ids:
            for source in await self._sources_in(conn, parent_id):
                previous = merged.setdefault(source.identity(), source)
                if previous != source:
                    raise ForestError(
                        "SOURCE_IDENTITY_CONFLICT",
                        "Parent provenance conflicts with the direct manifest",
                        409,
                    )
                if len(merged) > MAX_SOURCES:
                    raise ForestError(
                        "SOURCE_MANIFEST_LIMIT", "Source manifest exceeds 4096 items", 422
                    )
        source_set = tuple(merged[key] for key in sorted(merged))
        source_values = [source.canonical() for source in source_set]
        manifest_digest = _digest(_canonical(source_values))
        manifest_id = "manifest:" + manifest_digest.removeprefix("sha256:")
        content_digest = _digest(content)
        input_digest = _digest(
            _canonical(
                [
                    owner_component_id,
                    owner_kind,
                    owner_id,
                    media_type,
                    content_digest,
                    source_values,
                    parent_ids,
                ]
            )
        )
        payload_id = "payload:" + hashlib.sha256(
            _canonical([self.store.bot_id, owner_component_id, owner_kind, owner_id])
        ).hexdigest()
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND payload_id=%s").format(
                    self._table("payload_headers")
                ),
                (self.store.bot_id, payload_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError(
                    "PAYLOAD_ID_CONFLICT", "Payload owner already has different content", 409
                )
            return self._ref(existing)
        if (
            type(expected_deletion_epoch) is not int
            or expected_deletion_epoch < 0
            or current_epoch != expected_deletion_epoch
        ):
            raise ForestError("DELETION_CONFLICT", "Payload deletion epoch changed", 409)
        for source in source_set:
            blocked = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT 1 FROM {} t JOIN {} request "
                        "ON request.bot_id=t.bot_id AND request.operation_id=t.operation_id "
                        "WHERE t.bot_id=%s AND request.deletion_epoch<=%s "
                        "AND t.source_component_id=%s AND t.source_kind=%s "
                        "AND t.source_id=%s AND t.source_revision=%s "
                        "AND (t.part_id='*' OR %s='*' OR t.part_id=%s) LIMIT 1"
                    ).format(
                        self._table("forget_source_targets"),
                        self._table("forget_requests"),
                    ),
                    (
                        self.store.bot_id,
                        current_epoch,
                        source.source_component_id,
                        source.source_kind,
                        source.source_id,
                        source.source_revision,
                        source.part_id,
                        source.part_id,
                    ),
                )
            ).fetchone()
            if blocked is not None:
                raise ForestError("SOURCE_SUPPRESSED", "Payload source was removed", 409)

        for parent_id in parent_ids:
            parent = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT h.*,b.payload_id AS body_id FROM {} h LEFT JOIN {} b "
                        "ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id "
                        "WHERE h.bot_id=%s AND h.payload_id=%s"
                    ).format(
                        self._table("payload_headers"), self._table("payload_bodies")
                    ),
                    (self.store.bot_id, parent_id),
                )
            ).fetchone()
            if parent is None or parent["state"] != "ACTIVE" or parent["body_id"] is None:
                raise ForestError("PAYLOAD_PARENT_UNAVAILABLE", "Payload parent is unavailable", 409)
        inserted_manifest = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,manifest_id,manifest_digest,item_count) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING manifest_id"
                ).format(self._table("source_manifests")),
                (self.store.bot_id, manifest_id, manifest_digest, len(source_set)),
            )
        ).fetchone()
        if inserted_manifest is not None:
            for index, source in enumerate(source_set):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,manifest_id,item_index,source_component_id,"
                        "source_kind,source_id,source_revision,part_id,source_trunk_id,"
                        "observation_conversation_id,observation_binding_id,author_subject_id,"
                        "occurred_at,propagation_class) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("source_manifest_items")),
                    (
                        self.store.bot_id,
                        manifest_id,
                        index,
                        source.source_component_id,
                        source.source_kind,
                        source.source_id,
                        source.source_revision,
                        source.part_id,
                        source.source_trunk_id,
                        source.observation_conversation_id,
                        source.observation_binding_id,
                        source.author_subject_id,
                        source.occurred_at,
                        source.propagation_class,
                    ),
                )
        inserted_header = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,payload_id,owner_component_id,owner_kind,owner_id,"
                    "media_type,byte_length,content_digest,input_digest,source_manifest_id,"
                    "source_manifest_digest,parent_count,accepted_deletion_epoch) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT DO NOTHING RETURNING *"
                ).format(self._table("payload_headers")),
                (
                    self.store.bot_id,
                    payload_id,
                    owner_component_id,
                    owner_kind,
                    owner_id,
                    media_type,
                    len(content),
                    content_digest,
                    input_digest,
                    manifest_id,
                    manifest_digest,
                    len(parent_ids),
                    current_epoch,
                ),
            )
        ).fetchone()
        if inserted_header is None:
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND payload_id=%s"
                    ).format(self._table("payload_headers")),
                    (self.store.bot_id, payload_id),
                )
            ).fetchone()
            if replay is None or replay["input_digest"] != input_digest:
                raise ForestError(
                    "PAYLOAD_ID_CONFLICT", "Payload owner already has different content", 409
                )
            return self._ref(replay)
        for index, parent_id in enumerate(parent_ids):
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,payload_id,parent_index,parent_payload_id) "
                    "VALUES (%s,%s,%s,%s)"
                ).format(self._table("payload_derivations")),
                (self.store.bot_id, payload_id, index, parent_id),
            )
        await conn.execute(
            sql.SQL("INSERT INTO {}(bot_id,payload_id,content) VALUES (%s,%s,%s)").format(
                self._table("payload_bodies")
            ),
            (self.store.bot_id, payload_id, content),
        )
        return CleanPayloadRef(
            payload_id,
            content_digest,
            media_type,
            len(content),
            manifest_id,
            manifest_digest,
            "ACTIVE",
            current_epoch,
        )

    async def put_bytes(self, **values) -> CleanPayloadRef:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.put_bytes_in(conn, **values)

    async def put_json_in(self, conn, *, value, **values) -> CleanPayloadRef:
        return await self.put_bytes_in(
            conn,
            content=_canonical(value),
            media_type="application/json",
            **values,
        )

    async def verify_in(
        self,
        conn,
        *,
        payload_id,
        digest,
        owner_component_id,
        owner_kind,
        owner_id,
    ) -> CleanPayloadRef:
        await self._clock_in(conn)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT h.*,b.payload_id AS body_id FROM {} h LEFT JOIN {} b "
                    "ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id "
                    "WHERE h.bot_id=%s AND h.payload_id=%s"
                ).format(self._table("payload_headers"), self._table("payload_bodies")),
                (self.store.bot_id, payload_id),
            )
        ).fetchone()
        expected = (owner_component_id, owner_kind, owner_id, digest)
        actual = None if row is None else (
            row["owner_component_id"],
            row["owner_kind"],
            row["owner_id"],
            row["content_digest"],
        )
        if row is None or actual != expected:
            raise ForestError("PAYLOAD_IDENTITY_CONFLICT", "Payload identity differs", 409)
        if row["state"] != "ACTIVE" or row["body_id"] is None:
            raise ForestError("PAYLOAD_REVOKED", "Payload body is unavailable", 410)
        return self._ref(row)

    async def read_bytes_in(
        self, conn, payload_id: str, *, expected_deletion_epoch: int | None = None
    ) -> bytes:
        await self._clock_in(conn, expected=expected_deletion_epoch)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT h.state,b.content FROM {} h LEFT JOIN {} b "
                    "ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id "
                    "WHERE h.bot_id=%s AND h.payload_id=%s"
                ).format(self._table("payload_headers"), self._table("payload_bodies")),
                (self.store.bot_id, payload_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("PAYLOAD_NOT_FOUND", "Payload does not exist", 404)
        if row["state"] != "ACTIVE" or row["content"] is None:
            raise ForestError("PAYLOAD_REVOKED", "Payload body is unavailable", 410)
        return bytes(row["content"])

    async def read_bytes(self, payload_id: str) -> bytes:
        """Read one active payload under a single deletion-epoch snapshot."""

        async with self.store.pool.connection() as conn, conn.transaction():
            epoch = await self._clock_in(conn)
            return await self.read_bytes_in(
                conn, payload_id, expected_deletion_epoch=epoch
            )

    async def read_verified_batch(self, references) -> tuple[bytes, ...]:
        """Read exact owner-bound payloads under one deletion-epoch snapshot."""

        items = tuple(references)
        if len(items) > 8192:
            raise ForestError("PAYLOAD_INPUT_INVALID", "Payload batch is too large", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            epoch = await self._clock_in(conn)
            result = []
            for reference in items:
                try:
                    values = {
                        "payload_id": reference.payload_id,
                        "digest": reference.digest,
                        "owner_component_id": reference.owner_component_id,
                        "owner_kind": reference.owner_kind,
                        "owner_id": reference.owner_id,
                    }
                except AttributeError as exc:
                    raise ForestError(
                        "PAYLOAD_INPUT_INVALID",
                        "Payload batch contains an invalid reference",
                        422,
                    ) from exc
                await self.verify_in(conn, **values)
                result.append(
                    await self.read_bytes_in(
                        conn,
                        reference.payload_id,
                        expected_deletion_epoch=epoch,
                    )
                )
            return tuple(result)

    async def read_verified_chunk(
        self,
        reference,
        *,
        offset: int,
        max_bytes: int,
    ) -> tuple[bytes, int]:
        """Read a bounded range only after verifying the complete payload identity."""

        if (
            type(offset) is not int
            or offset < 0
            or type(max_bytes) is not int
            or not 1 <= max_bytes <= 96 * 1024
        ):
            raise ForestError(
                "PAYLOAD_RANGE_INVALID",
                "Payload ranges require a non-negative offset and at most 96 KiB",
                422,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._clock_in(conn)
            verified = await self.verify_in(
                conn,
                payload_id=reference.payload_id,
                digest=reference.digest,
                owner_component_id=reference.owner_component_id,
                owner_kind=reference.owner_kind,
                owner_id=reference.owner_id,
            )
            if offset > verified.byte_length:
                raise ForestError(
                    "PAYLOAD_RANGE_INVALID", "Payload offset exceeds its byte length", 416
                )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT substring(content FROM %s FOR %s) AS content FROM {} "
                        "WHERE bot_id=%s AND payload_id=%s"
                    ).format(self._table("payload_bodies")),
                    (offset + 1, max_bytes, self.store.bot_id, reference.payload_id),
                )
            ).fetchone()
            if row is None or row["content"] is None:
                raise ForestError("PAYLOAD_REVOKED", "Payload body is unavailable", 410)
            return bytes(row["content"]), verified.byte_length

    async def forget_in(
        self,
        conn,
        *,
        operation_id: str,
        source_targets=(),
        payload_ids=(),
        expected_deletion_epoch: int | None = None,
        expected_selection_digest: str | None = None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        if expected_deletion_epoch is not None and (
            type(expected_deletion_epoch) is not int or expected_deletion_epoch < 0
        ):
            raise ForestError("PAYLOAD_INPUT_INVALID", "Invalid deletion epoch", 422)
        if expected_selection_digest is not None and (
            not isinstance(expected_selection_digest, str)
            or len(expected_selection_digest) != 71
            or not expected_selection_digest.startswith("sha256:")
            or any(char not in "0123456789abcdef" for char in expected_selection_digest[7:])
        ):
            raise ForestError("PAYLOAD_INPUT_INVALID", "Invalid selection digest", 422)
        targets, requested_payloads = self._forget_selection_input(source_targets, payload_ids)
        input_digest = _digest(
            _canonical(
                [
                    expected_deletion_epoch,
                    expected_selection_digest,
                    [asdict(target) for target in targets],
                    requested_payloads,
                ]
            )
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("forest.kernel.payload/forget/" + self.store.bot_id + "/" + operation_id,),
        )
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                    self._table("forget_requests")
                ),
                (self.store.bot_id, operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError(
                    "FORGET_OPERATION_CONFLICT", "Forget operation input differs", 409
                )
            return {
                "operation_id": operation_id,
                "deletion_epoch": existing["deletion_epoch"],
                "selected_count": existing["selected_count"],
                "selection_digest": existing["selection_digest"],
            }
        current_epoch = await self._clock_in(
            conn, expected=expected_deletion_epoch, update=True
        )
        selected_ids = await self._select_forget_payload_ids_in(
            conn, targets=targets, requested_payloads=requested_payloads
        )
        selection_digest = _digest(_canonical(selected_ids))
        if (
            expected_selection_digest is not None
            and selection_digest != expected_selection_digest
        ):
            raise ForestError(
                "FORGET_SELECTION_CONFLICT", "Payload forget selection changed", 409
            )
        new_epoch = current_epoch + 1
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,operation_id,input_digest,selection_digest,"
                "selected_count,deletion_epoch) VALUES (%s,%s,%s,%s,%s,%s)"
            ).format(self._table("forget_requests")),
            (
                self.store.bot_id,
                operation_id,
                input_digest,
                selection_digest,
                len(selected_ids),
                new_epoch,
            ),
        )
        for target in targets:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,source_component_id,source_kind,"
                    "source_id,source_revision,part_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("forget_source_targets")),
                (
                    self.store.bot_id,
                    operation_id,
                    target.source_component_id,
                    target.source_kind,
                    target.source_id,
                    target.source_revision,
                    target.part_id,
                ),
            )
        for payload_id in selected_ids:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,payload_id) VALUES (%s,%s,%s)"
                ).format(self._table("forget_payload_targets")),
                (self.store.bot_id, operation_id, payload_id),
            )
        await conn.execute(
            sql.SQL("UPDATE {} SET deletion_epoch=%s WHERE bot_id=%s").format(
                self._table("deletion_clock")
            ),
            (new_epoch, self.store.bot_id),
        )
        if selected_ids:
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='REVOKED',revoke_operation_id=%s,"
                    "revoked_at=clock_timestamp() WHERE bot_id=%s "
                    "AND payload_id=ANY(%s) AND state='ACTIVE'"
                ).format(self._table("payload_headers")),
                (operation_id, self.store.bot_id, list(selected_ids)),
            )
        return {
            "operation_id": operation_id,
            "deletion_epoch": new_epoch,
            "selected_count": len(selected_ids),
            "selection_digest": selection_digest,
        }

    @staticmethod
    def _forget_selection_input(source_targets, payload_ids):
        targets = tuple(source_targets)
        if len(targets) > MAX_SOURCES or any(type(item) is not SourceTarget for item in targets):
            raise ForestError("PAYLOAD_INPUT_INVALID", "Invalid forget source targets", 422)
        for target in targets:
            target.validate()
        targets = tuple(sorted(set(targets), key=lambda item: tuple(asdict(item).values())))
        requested_payloads = tuple(sorted(set(payload_ids)))
        if len(requested_payloads) > MAX_FORGET_SELECTION:
            raise ForestError("FORGET_SCOPE_LIMIT", "Forget selection exceeds 100000", 422)
        for payload_id in requested_payloads:
            _bounded(payload_id, "payload_id", 256)
        if not targets and not requested_payloads:
            raise ForestError("PAYLOAD_INPUT_INVALID", "Forget request has no target", 422)
        return targets, requested_payloads

    async def _select_forget_payload_ids_in(self, conn, *, targets, requested_payloads):
        if requested_payloads:
            existing_payloads = {
                row["payload_id"]
                for row in await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT payload_id FROM {} WHERE bot_id=%s AND payload_id=ANY(%s)"
                        ).format(self._table("payload_headers")),
                        (self.store.bot_id, list(requested_payloads)),
                    )
                ).fetchall()
            }
            if existing_payloads != set(requested_payloads):
                raise ForestError("PAYLOAD_NOT_FOUND", "Forget target does not exist", 404)
        selected = set(requested_payloads)
        for target in targets:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT DISTINCT h.payload_id FROM {} h JOIN {} i "
                        "ON i.bot_id=h.bot_id AND i.manifest_id=h.source_manifest_id "
                        "WHERE h.bot_id=%s AND h.state='ACTIVE' "
                        "AND i.source_component_id=%s AND i.source_kind=%s "
                        "AND i.source_id=%s AND i.source_revision=%s "
                        "AND (%s='*' OR i.part_id='*' OR i.part_id=%s)"
                    ).format(
                        self._table("payload_headers"),
                        self._table("source_manifest_items"),
                    ),
                    (
                        self.store.bot_id,
                        target.source_component_id,
                        target.source_kind,
                        target.source_id,
                        target.source_revision,
                        target.part_id,
                        target.part_id,
                    ),
                )
            ).fetchall()
            selected.update(row["payload_id"] for row in rows)
        frontier = set(selected)
        while frontier:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT d.payload_id FROM {} d JOIN {} h "
                        "ON h.bot_id=d.bot_id AND h.payload_id=d.payload_id "
                        "WHERE d.bot_id=%s AND d.parent_payload_id=ANY(%s) AND h.state='ACTIVE'"
                    ).format(
                        self._table("payload_derivations"),
                        self._table("payload_headers"),
                    ),
                    (self.store.bot_id, sorted(frontier)),
                )
            ).fetchall()
            discovered = {row["payload_id"] for row in rows} - selected
            selected.update(discovered)
            if len(selected) > MAX_FORGET_SELECTION:
                raise ForestError("FORGET_SCOPE_LIMIT", "Forget closure exceeds 100000", 422)
            frontier = discovered
        return tuple(sorted(selected))

    async def preview_forget_in(self, conn, *, source_targets=(), payload_ids=()) -> dict:
        """Return an exact closure while fencing admissions and other forgets."""

        targets, requested_payloads = self._forget_selection_input(
            source_targets, payload_ids
        )
        current_epoch = await self._clock_in(conn, update=True)
        selected_ids = await self._select_forget_payload_ids_in(
            conn, targets=targets, requested_payloads=requested_payloads
        )
        return {
            "deletion_epoch": current_epoch,
            "selected_count": len(selected_ids),
            "selection_digest": _digest(_canonical(selected_ids)),
            "selected_payload_ids": list(selected_ids),
        }

    async def forget_selected_payload_ids_in(self, conn, operation_id: str) -> list[str]:
        """Return one committed selection to a same-transaction UoW participant."""

        _bounded(operation_id, "operation_id", 256)
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT payload_id FROM {} WHERE bot_id=%s AND operation_id=%s "
                    "ORDER BY payload_id"
                ).format(self._table("forget_payload_targets")),
                (self.store.bot_id, operation_id),
            )
        ).fetchall()
        return [row["payload_id"] for row in rows]

    async def forget(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.forget_in(conn, **values)

    async def scrub(self, *, limit: int = 100) -> dict:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ForestError("SCRUB_LIMIT_INVALID", "Scrub limit is outside 1..1000", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT payload_id FROM {} WHERE bot_id=%s AND state='REVOKED' "
                        "ORDER BY payload_id FOR UPDATE SKIP LOCKED LIMIT %s"
                    ).format(self._table("payload_headers")),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            payload_ids = [row["payload_id"] for row in rows]
            if payload_ids:
                await conn.execute(
                    sql.SQL("DELETE FROM {} WHERE bot_id=%s AND payload_id=ANY(%s)").format(
                        self._table("payload_bodies")
                    ),
                    (self.store.bot_id, payload_ids),
                )
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='SCRUBBED',scrubbed_at=clock_timestamp() "
                        "WHERE bot_id=%s AND payload_id=ANY(%s)"
                    ).format(self._table("payload_headers")),
                    (self.store.bot_id, payload_ids),
                )
            return {"scrubbed": len(payload_ids)}

    @staticmethod
    def _ref(row) -> CleanPayloadRef:
        return CleanPayloadRef(
            row["payload_id"],
            row["content_digest"],
            row["media_type"],
            row["byte_length"],
            row["source_manifest_id"],
            row["source_manifest_digest"],
            row["state"],
            row["accepted_deletion_epoch"],
        )

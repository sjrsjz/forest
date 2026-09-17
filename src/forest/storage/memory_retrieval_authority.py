"""Clean multimodal embedding and retrieval authority for ``memory.graph``.

The repository owns facet eligibility, embedding attempt convergence and frozen
evidence bundles.  It never captions an image and never treats an embedding
driver as an authority for source identity or visibility.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
import struct
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace
from forest.storage.memory_graph_authority import MemoryPayloadRef

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_ERROR_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_CHUNKER_REVISION = "utf8-contiguous-4096-v1"
_MAX_TEXT_FACET_BYTES = 4096


def _bounded(value, field: str, limit: int):
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("MEMORY_INPUT_INVALID", f"Invalid {field}", 422)
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
        raise ForestError("MEMORY_INPUT_INVALID", "Memory input is not canonical", 422) from exc


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _content_digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


async def _call(port, *args, **values):
    result = port(*args, **values)
    return await result if inspect.isawaitable(result) else result


def _text_chunks(text: str):
    start = count = 0
    for index, char in enumerate(text):
        size = len(char.encode("utf-8"))
        if count + size > _MAX_TEXT_FACET_BYTES:
            yield start, index, text[start:index]
            start, count = index, 0
        count += size
    if start < len(text):
        yield start, len(text), text[start:]


def _float32_vector(value, *, dimensions: int, normalization: str) -> tuple[float, ...]:
    if (
        not isinstance(value, (list, tuple))
        or len(value) != dimensions
        or any(type(item) not in {int, float} or not math.isfinite(item) for item in value)
    ):
        raise ForestError("MEMORY_VECTOR_INVALID", "Vector does not match its space", 422)
    vector = tuple(struct.unpack("<f", struct.pack("<f", float(item)))[0] for item in value)
    norm = math.sqrt(sum(item * item for item in vector))
    if norm == 0 or not math.isfinite(norm):
        raise ForestError("MEMORY_VECTOR_INVALID", "Embedding vector is empty", 422)
    if normalization == "L2" and not 0.999 <= norm <= 1.001:
        raise ForestError("MEMORY_VECTOR_INVALID", "Vector is not L2 normalized", 422)
    return vector


@dataclass(frozen=True)
class RetrievalQueryPart:
    source_unit_id: str
    source_revision: int
    occurrence_id: str
    part_id: str

    def validate(self) -> None:
        _bounded(self.source_unit_id, "query_part.source_unit_id", 256)
        if type(self.source_revision) is not int or self.source_revision < 1:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid query source revision", 422)
        _bounded(self.occurrence_id, "query_part.occurrence_id", 256)
        _bounded(self.part_id, "query_part.part_id", 256)

    def canonical(self) -> dict:
        return vars(self)


@dataclass(frozen=True)
class EmbeddingPermit:
    bot_id: str
    facet_id: str
    attempt_id: str
    attempt_no: int
    executor_component_id: str
    executor_generation_id: str
    route_revision: str
    accepted_deletion_epoch: int
    deadline_at: datetime


class MemoryRetrievalRepository:
    """Own multimodal facets and immutable retrieval decisions."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier=None,
        payload_reader=None,
        deletion_epoch_guard=None,
        fence_verifier=None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.memory.graph")
        self.payload_verifier = payload_verifier
        self.payload_reader = payload_reader
        self.deletion_epoch_guard = deletion_epoch_guard
        self.fence_verifier = fence_verifier

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _lock_identities(self, conn, *identities: str) -> None:
        for identity in sorted(set(identities)):
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.memory.graph/retrieval/{self.store.bot_id}/{identity}",),
            )

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    async def _epoch(self, conn, *, expected=None) -> int:
        if self.deletion_epoch_guard is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "Memory deletion epoch port is unavailable", 503
            )
        return await _call(self.deletion_epoch_guard, conn, expected=expected)

    async def _verify_payload(self, conn, reference: MemoryPayloadRef):
        if self.payload_verifier is None:
            raise ForestError("PAYLOAD_AUTHORITY_MISSING", "Memory payload port is unavailable", 503)
        return await _call(
            self.payload_verifier,
            conn,
            payload_id=reference.payload_id,
            digest=reference.digest,
            owner_component_id=reference.owner_component_id,
            owner_kind=reference.owner_kind,
            owner_id=reference.owner_id,
        )

    async def _read_payload(self, conn, payload_id: str, deletion_epoch: int) -> bytes:
        if self.payload_reader is None:
            raise ForestError("PAYLOAD_AUTHORITY_MISSING", "Memory payload reader is unavailable", 503)
        content = await _call(
            self.payload_reader,
            conn,
            payload_id,
            expected_deletion_epoch=deletion_epoch,
        )
        if not isinstance(content, bytes):
            raise ForestError("PAYLOAD_AUTHORITY_INVALID", "Payload reader returned invalid data", 503)
        return content

    async def _verify_fence(self, conn, permit: EmbeddingPermit) -> None:
        if self.fence_verifier is None:
            return
        await _call(
            self.fence_verifier,
            conn,
            component_id=permit.executor_component_id,
            generation_id=permit.executor_generation_id,
            route_revision=permit.route_revision,
        )

    async def _domain_in(self, conn) -> dict:
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(
                    self._table("memory_domains")
                ),
                (self.store.bot_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("MEMORY_DOMAIN_MISSING", "Memory domain is not initialized", 409)
        return dict(row)

    async def _source_parts_in(self, conn, source_unit_id: str, *, lock=False):
        lock_sql = sql.SQL(" FOR SHARE") if lock else sql.SQL("")
        unit = await (
            await conn.execute(
                sql.SQL(
                    "SELECT u.*,s.source_kind,s.external_group_id,s.platform,s.realm_id "
                    "FROM {} u JOIN {} s ON s.bot_id=u.bot_id AND s.source_id=u.source_id "
                    "WHERE u.bot_id=%s AND u.source_unit_id=%s"
                ).format(self._table("source_units"), self._table("memory_sources"))
                + lock_sql,
                (self.store.bot_id, source_unit_id),
            )
        ).fetchone()
        if unit is None or unit["eligibility_state"] != "ELIGIBLE":
            raise ForestError("MEMORY_SOURCE_UNAVAILABLE", "Memory source is not eligible", 409)
        parts = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=%s "
                    "ORDER BY part_index,part_id"
                ).format(self._table("source_parts")),
                (self.store.bot_id, source_unit_id),
            )
        ).fetchall()
        return dict(unit), [dict(row) for row in parts]

    @staticmethod
    def _part_ref(row) -> MemoryPayloadRef:
        return MemoryPayloadRef(
            row["payload_id"],
            row["payload_digest"],
            row["payload_owner_component_id"],
            row["payload_owner_kind"],
            row["payload_owner_id"],
        )

    async def _facet_values_in(
        self,
        conn,
        *,
        purpose: str,
        query_id: str | None,
        unit: dict,
        parts: list[dict],
        deletion_epoch: int,
        space_id: str,
    ) -> list[dict]:
        values = []
        for part in parts:
            reference = self._part_ref(part)
            await self._verify_payload(conn, reference)
            content = await self._read_payload(conn, reference.payload_id, deletion_epoch)
            if len(content) != part["byte_length"] or _content_digest(content) != reference.digest:
                raise ForestError(
                    "MEMORY_PAYLOAD_CONFLICT", "Memory part differs from its source fact", 409
                )
            selectors = []
            if part["part_kind"] == "TEXT":
                try:
                    text = content.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ForestError(
                        "MEMORY_TEXT_INVALID", "Memory text part is not UTF-8", 422
                    ) from exc
                selectors = [
                    (start, end, chunk.encode("utf-8"))
                    for start, end, chunk in _text_chunks(text)
                ]
            elif part["part_kind"] == "IMAGE":
                selectors = [(None, None, content)]
            for start, end, facet_content in selectors:
                identity = {
                    "purpose": purpose,
                    "query_id": query_id,
                    "source_unit_id": unit["source_unit_id"],
                    "source_revision": unit["source_revision"],
                    "part_id": part["part_id"],
                    "modality": part["part_kind"],
                    "start_char": start,
                    "end_char": end,
                    "payload_digest": part["payload_digest"],
                    "facet_content_digest": _content_digest(facet_content),
                    "space_id": space_id,
                    "chunker_revision": _CHUNKER_REVISION,
                }
                values.append(
                    {
                        **identity,
                        "facet_id": "facet:" + _digest(identity)[7:],
                        "input_payload_id": part["payload_id"],
                        "input_media_type": part["media_type"],
                        "input_byte_length": len(facet_content),
                    }
                )
        if not values:
            raise ForestError(
                "MEMORY_EMBEDDING_INPUT_EMPTY", "Memory source has no encodable content", 422
            )
        return values

    async def _insert_facets_in(self, conn, values: list[dict], deletion_epoch: int) -> None:
        for item in values:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,facet_id,purpose,query_id,source_unit_id,"
                    "source_revision,part_id,modality,selector_start_char,selector_end_char,"
                    "input_payload_id,input_payload_digest,facet_content_digest,input_media_type,"
                    "input_byte_length,space_id,accepted_deletion_epoch) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT DO NOTHING"
                ).format(self._table("embedding_facets")),
                (
                    self.store.bot_id,
                    item["facet_id"],
                    item["purpose"],
                    item["query_id"],
                    item["source_unit_id"],
                    item["source_revision"],
                    item["part_id"],
                    item["modality"],
                    item["start_char"],
                    item["end_char"],
                    item["input_payload_id"],
                    item["payload_digest"],
                    item["facet_content_digest"],
                    item["input_media_type"],
                    item["input_byte_length"],
                    item["space_id"],
                    deletion_epoch,
                ),
            )
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND facet_id=%s").format(
                        self._table("embedding_facets")
                    ),
                    (self.store.bot_id, item["facet_id"]),
                )
            ).fetchone()
            expected = {
                "purpose": item["purpose"],
                "query_id": item["query_id"],
                "source_unit_id": item["source_unit_id"],
                "source_revision": item["source_revision"],
                "part_id": item["part_id"],
                "modality": item["modality"],
                "selector_start_char": item["start_char"],
                "selector_end_char": item["end_char"],
                "input_payload_id": item["input_payload_id"],
                "input_payload_digest": item["payload_digest"],
                "facet_content_digest": item["facet_content_digest"],
                "input_media_type": item["input_media_type"],
                "input_byte_length": item["input_byte_length"],
                "space_id": item["space_id"],
            }
            if row is None or any(row[key] != value for key, value in expected.items()):
                raise ForestError("MEMORY_FACET_CONFLICT", "Embedding facet identity differs", 409)

    async def prepare_source_facets(
        self,
        *,
        operation_id: str,
        source_unit_id: str,
        expected_deletion_epoch: int,
        _connection=None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(source_unit_id, "source_unit_id", 256)
        async with self._transaction(_connection) as conn:
            domain = await self._domain_in(conn)
            deletion_epoch = await self._epoch(conn, expected=expected_deletion_epoch)
            await self._lock_identities(conn, "facet-operation:" + operation_id)
            unit, parts = await self._source_parts_in(conn, source_unit_id, lock=True)
            identity = {
                "source_unit_id": source_unit_id,
                "source_revision": unit["source_revision"],
                "space_id": domain["space_id"],
                "chunker_revision": _CHUNKER_REVISION,
                "deletion_epoch": deletion_epoch,
            }
            input_digest = _digest(identity)
            existing = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s"
                    ).format(self._table("facet_preparation_operations")),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["input_digest"] != input_digest
                    or existing["target_kind"] != "SOURCE_UNIT"
                    or existing["target_id"] != source_unit_id
                ):
                    raise ForestError(
                        "MEMORY_OPERATION_CONFLICT", "Facet preparation input differs", 409
                    )
                rows = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT * FROM {} WHERE bot_id=%s AND purpose='DOCUMENT' "
                            "AND source_unit_id=%s ORDER BY facet_id"
                        ).format(self._table("embedding_facets")),
                        (self.store.bot_id, source_unit_id),
                    )
                ).fetchall()
                return {"duplicate": True, "facets": [dict(row) for row in rows]}
            values = await self._facet_values_in(
                conn,
                purpose="DOCUMENT",
                query_id=None,
                unit=unit,
                parts=parts,
                deletion_epoch=deletion_epoch,
                space_id=domain["space_id"],
            )
            await self._insert_facets_in(conn, values, deletion_epoch)
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,input_digest,target_kind,target_id,"
                    "facet_count) VALUES (%s,%s,%s,'SOURCE_UNIT',%s,%s)"
                ).format(self._table("facet_preparation_operations")),
                (self.store.bot_id, operation_id, input_digest, source_unit_id, len(values)),
            )
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND facet_id=ANY(%s) ORDER BY facet_id"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, [item["facet_id"] for item in values]),
                )
            ).fetchall()
            return {"duplicate": False, "facets": [dict(row) for row in rows]}

    async def _query_part_in(
        self,
        conn,
        part: RetrievalQueryPart,
        *,
        input_batch_ref: str,
        context_revision: int,
    ) -> tuple[dict, dict]:
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT u.*,p.*,o.source_unit_id AS occurrence_source_unit_id,"
                    "o.input_batch_ref,o.context_revision,o.eligibility_state AS occurrence_state "
                    "FROM {} u JOIN {} p ON p.bot_id=u.bot_id AND p.source_unit_id=u.source_unit_id "
                    "JOIN {} o ON o.bot_id=u.bot_id AND o.occurrence_id=%s "
                    "WHERE u.bot_id=%s AND u.source_unit_id=%s AND p.part_id=%s FOR SHARE OF u"
                ).format(
                    self._table("source_units"),
                    self._table("source_parts"),
                    self._table("source_occurrences"),
                ),
                (
                    part.occurrence_id,
                    self.store.bot_id,
                    part.source_unit_id,
                    part.part_id,
                ),
            )
        ).fetchone()
        if row is None:
            raise ForestError("MEMORY_QUERY_SOURCE_MISSING", "Query source part is unavailable", 404)
        if (
            row["source_revision"] != part.source_revision
            or row["eligibility_state"] != "ELIGIBLE"
            or row["occurrence_source_unit_id"] != part.source_unit_id
            or row["occurrence_state"] != "ELIGIBLE"
            or row["input_batch_ref"] != input_batch_ref
            or row["context_revision"] != context_revision
        ):
            raise ForestError(
                "MEMORY_QUERY_SOURCE_UNAVAILABLE",
                "Query part is outside the frozen InputBatch",
                409,
            )
        unit = {key: row[key] for key in (
            "source_unit_id",
            "source_revision",
            "source_id",
            "trunk_id",
            "author_subject_ref",
            "occurred_at",
        )}
        source_part = {key: row[key] for key in (
            "part_id",
            "part_index",
            "part_kind",
            "media_type",
            "payload_id",
            "payload_digest",
            "payload_owner_component_id",
            "payload_owner_kind",
            "payload_owner_id",
            "width",
            "height",
            "byte_length",
        )}
        return unit, source_part

    async def register_query(
        self,
        *,
        operation_id: str,
        query_id: str,
        current_source_id: str,
        input_batch_ref: str,
        context_revision: int,
        parts,
        expected_deletion_epoch: int,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (query_id, "query_id"),
            (current_source_id, "current_source_id"),
            (input_batch_ref, "input_batch_ref"),
        ):
            _bounded(value, field, 256)
        if type(context_revision) is not int or context_revision < 1:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid context revision", 422)
        parts = tuple(parts)
        if (
            not 1 <= len(parts) <= 256
            or any(type(part) is not RetrievalQueryPart for part in parts)
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid query parts", 422)
        for part in parts:
            part.validate()
        if len({(part.source_unit_id, part.part_id) for part in parts}) != len(parts):
            raise ForestError("MEMORY_INPUT_INVALID", "Query parts contain duplicates", 422)
        identity = {
            "query_id": query_id,
            "current_source_id": current_source_id,
            "input_batch_ref": input_batch_ref,
            "context_revision": context_revision,
            "parts": [part.canonical() for part in parts],
            "expected_deletion_epoch": expected_deletion_epoch,
            "chunker_revision": _CHUNKER_REVISION,
        }
        input_digest = _digest(identity)
        async with self.store.pool.connection() as conn, conn.transaction():
            domain = await self._domain_in(conn)
            deletion_epoch = await self._epoch(conn, expected=expected_deletion_epoch)
            await self._lock_identities(
                conn,
                "query:" + query_id,
                "query-operation:" + operation_id,
            )
            source = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND source_id=%s AND status='ACTIVE'"
                    ).format(self._table("memory_sources")),
                    (self.store.bot_id, current_source_id),
                )
            ).fetchone()
            if source is None:
                raise ForestError("MEMORY_SOURCE_UNAVAILABLE", "Current source is unavailable", 409)
            existing = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND (query_id=%s OR operation_id=%s)"
                    ).format(self._table("memory_queries")),
                    (self.store.bot_id, query_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["query_id"] != query_id
                    or existing["operation_id"] != operation_id
                    or existing["input_digest"] != input_digest
                ):
                    raise ForestError("MEMORY_QUERY_CONFLICT", "Memory query identity differs", 409)
                return await self._query_view_in(conn, query_id, duplicate=True)
            resolved = []
            modalities = set()
            for index, part in enumerate(parts):
                unit, source_part = await self._query_part_in(
                    conn,
                    part,
                    input_batch_ref=input_batch_ref,
                    context_revision=context_revision,
                )
                modalities.add(source_part["part_kind"])
                resolved.append((index, part, unit, source_part))
            query_mode = (
                "MIXED" if modalities == {"TEXT", "IMAGE"} else next(iter(modalities))
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,query_id,operation_id,input_digest,current_source_id,"
                    "input_batch_ref,context_revision,query_mode,fusion_policy_revision,"
                    "accepted_deletion_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("memory_queries")),
                (
                    self.store.bot_id,
                    query_id,
                    operation_id,
                    input_digest,
                    current_source_id,
                    input_batch_ref,
                    context_revision,
                    query_mode,
                    domain["fusion_policy_revision"],
                    deletion_epoch,
                ),
            )
            facet_values = []
            for index, part, unit, source_part in resolved:
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,query_id,query_part_index,source_unit_id,"
                        "source_revision,occurrence_id,part_id,modality) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("memory_query_parts")),
                    (
                        self.store.bot_id,
                        query_id,
                        index,
                        part.source_unit_id,
                        part.source_revision,
                        part.occurrence_id,
                        part.part_id,
                        source_part["part_kind"],
                    ),
                )
                facet_values.extend(
                    await self._facet_values_in(
                        conn,
                        purpose="QUERY",
                        query_id=query_id,
                        unit=unit,
                        parts=[source_part],
                        deletion_epoch=deletion_epoch,
                        space_id=domain["space_id"],
                    )
                )
            await self._insert_facets_in(conn, facet_values, deletion_epoch)
            return await self._query_view_in(conn, query_id, duplicate=False)

    async def _query_view_in(self, conn, query_id: str, *, duplicate=False) -> dict:
        query = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND query_id=%s").format(
                    self._table("memory_queries")
                ),
                (self.store.bot_id, query_id),
            )
        ).fetchone()
        if query is None:
            raise ForestError("MEMORY_QUERY_NOT_FOUND", "Memory query does not exist", 404)
        facets = await (
            await conn.execute(
                sql.SQL(
                    "SELECT facet_id,source_unit_id,part_id,modality,selector_start_char,"
                    "selector_end_char,state FROM {} WHERE bot_id=%s AND query_id=%s "
                    "ORDER BY source_unit_id,part_id,selector_start_char NULLS FIRST,facet_id"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, query_id),
            )
        ).fetchall()
        return {**dict(query), "duplicate": duplicate, "facets": [dict(row) for row in facets]}

    async def materialize_facet(
        self,
        facet_id: str,
        *,
        expected_deletion_epoch: int,
        _connection=None,
    ) -> dict:
        _bounded(facet_id, "facet_id", 256)
        async with self._transaction(_connection) as conn:
            deletion_epoch = await self._epoch(conn, expected=expected_deletion_epoch)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT f.*,u.eligibility_state,u.author_subject_ref,u.source_id,u.trunk_id,"
                        "p.width,p.height,"
                        "s.source_kind,s.platform,s.realm_id,s.external_group_id "
                        "FROM {} f JOIN {} u ON u.bot_id=f.bot_id "
                        "AND u.source_unit_id=f.source_unit_id JOIN {} p ON p.bot_id=f.bot_id "
                        "AND p.source_unit_id=f.source_unit_id AND p.part_id=f.part_id "
                        "JOIN {} s ON s.bot_id=u.bot_id "
                        "AND s.source_id=u.source_id WHERE f.bot_id=%s AND f.facet_id=%s"
                    ).format(
                        self._table("embedding_facets"),
                        self._table("source_units"),
                        self._table("source_parts"),
                        self._table("memory_sources"),
                    ),
                    (self.store.bot_id, facet_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("MEMORY_FACET_NOT_FOUND", "Embedding facet does not exist", 404)
            if row["eligibility_state"] != "ELIGIBLE":
                raise ForestError("MEMORY_SOURCE_UNAVAILABLE", "Embedding source is unavailable", 409)
            content = await self._read_payload(conn, row["input_payload_id"], deletion_epoch)
            if _content_digest(content) != row["input_payload_digest"]:
                raise ForestError("MEMORY_PAYLOAD_CONFLICT", "Embedding input payload changed", 409)
            if row["modality"] == "TEXT":
                try:
                    text = content.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ForestError("MEMORY_TEXT_INVALID", "Memory text is not UTF-8", 422) from exc
                selected = text[row["selector_start_char"] : row["selector_end_char"]]
                encoded = selected.encode("utf-8")
                if _content_digest(encoded) != row["facet_content_digest"]:
                    raise ForestError("MEMORY_FACET_CONFLICT", "Text facet selector changed", 409)
                materialized = {"text": selected}
            else:
                if _content_digest(content) != row["facet_content_digest"]:
                    raise ForestError("MEMORY_FACET_CONFLICT", "Image facet content changed", 409)
                materialized = {"image": content, "media_type": row["input_media_type"]}
            return {
                "facet_id": facet_id,
                "purpose": row["purpose"],
                "modality": row["modality"],
                "space_id": row["space_id"],
                "source_revision": row["source_revision"],
                "part_id": row["part_id"],
                "input_payload_id": row["input_payload_id"],
                "input_payload_digest": row["input_payload_digest"],
                "width": row["width"],
                "height": row["height"],
                "source": {
                    key: row[key]
                    for key in (
                        "source_unit_id",
                        "source_id",
                        "trunk_id",
                        "source_kind",
                        "platform",
                        "realm_id",
                        "external_group_id",
                        "author_subject_ref",
                    )
                },
                **materialized,
            }

    @staticmethod
    def _permit(row) -> EmbeddingPermit:
        return EmbeddingPermit(
            row["bot_id"],
            row["facet_id"],
            row["attempt_id"],
            row["attempt_no"],
            row["executor_component_id"],
            row["executor_generation_id"],
            row["route_revision"],
            row["accepted_deletion_epoch"],
            row["deadline_at"],
        )

    @staticmethod
    def _permit_matches(row, permit: EmbeddingPermit) -> bool:
        return all(
            row[field] == getattr(permit, field)
            for field in (
                "bot_id",
                "facet_id",
                "attempt_id",
                "attempt_no",
                "executor_component_id",
                "executor_generation_id",
                "route_revision",
                "accepted_deletion_epoch",
                "deadline_at",
            )
        )

    async def prepare_attempt(
        self,
        *,
        operation_id: str,
        attempt_id: str,
        facet_id: str,
        executor_component_id: str,
        executor_generation_id: str,
        route_revision: str,
        deadline_at: datetime,
    ) -> EmbeddingPermit:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (attempt_id, "attempt_id", 256),
            (facet_id, "facet_id", 256),
            (executor_component_id, "executor_component_id", 128),
            (executor_generation_id, "executor_generation_id", 256),
            (route_revision, "route_revision", 256),
        ):
            _bounded(value, field, limit)
        if not isinstance(deadline_at, datetime) or deadline_at.tzinfo is None:
            raise ForestError("MEMORY_INPUT_INVALID", "Embedding deadline needs a timezone", 422)
        deadline_at = deadline_at.astimezone(timezone.utc)
        now = datetime.now(timezone.utc)
        if not now < deadline_at <= now + timedelta(minutes=5):
            raise ForestError("MEMORY_INPUT_INVALID", "Embedding deadline is outside its bound", 422)
        input_digest = _digest(
            {
                "attempt_id": attempt_id,
                "facet_id": facet_id,
                "executor_component_id": executor_component_id,
                "executor_generation_id": executor_generation_id,
                "route_revision": route_revision,
                "deadline_at": deadline_at.isoformat(),
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock_identities(
                conn,
                "embedding-attempt:" + attempt_id,
                "embedding-operation:" + operation_id,
            )
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND "
                        "(attempt_id=%s OR operation_id=%s)"
                    ).format(self._table("embedding_attempts")),
                    (self.store.bot_id, attempt_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if (
                    replay["attempt_id"] != attempt_id
                    or replay["operation_id"] != operation_id
                    or replay["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "MEMORY_EMBEDDING_ATTEMPT_CONFLICT",
                        "Embedding attempt identity differs",
                        409,
                    )
                return self._permit(replay)
            facet = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND facet_id=%s FOR UPDATE"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, facet_id),
                )
            ).fetchone()
            if facet is None:
                raise ForestError("MEMORY_FACET_NOT_FOUND", "Embedding facet does not exist", 404)
            if facet["state"] != "PENDING":
                raise ForestError(
                    "MEMORY_FACET_NOT_DISPATCHABLE", "Embedding facet cannot start an attempt", 409
                )
            await self._epoch(conn, expected=facet["accepted_deletion_epoch"])
            unit, _ = await self._source_parts_in(conn, facet["source_unit_id"], lock=True)
            if unit["source_revision"] != facet["source_revision"]:
                raise ForestError("MEMORY_SOURCE_UNAVAILABLE", "Embedding source changed", 409)
            provisional = EmbeddingPermit(
                self.store.bot_id,
                facet_id,
                attempt_id,
                facet["attempt_count"] + 1,
                executor_component_id,
                executor_generation_id,
                route_revision,
                facet["accepted_deletion_epoch"],
                deadline_at,
            )
            await self._verify_fence(conn, provisional)
            attempt_no = facet["attempt_count"] + 1
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,attempt_id,operation_id,input_digest,facet_id,"
                        "attempt_no,executor_component_id,executor_generation_id,route_revision,"
                        "accepted_deletion_epoch,deadline_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("embedding_attempts")),
                    (
                        self.store.bot_id,
                        attempt_id,
                        operation_id,
                        input_digest,
                        facet_id,
                        attempt_no,
                        executor_component_id,
                        executor_generation_id,
                        route_revision,
                        facet["accepted_deletion_epoch"],
                        deadline_at,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='PREPARED',attempt_count=%s,current_attempt_id=%s,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND facet_id=%s"
                ).format(self._table("embedding_facets")),
                (attempt_no, attempt_id, self.store.bot_id, facet_id),
            )
            return self._permit(row)

    async def mark_handoff(
        self,
        permit: EmbeddingPermit,
        *,
        handoff_operation_id: str,
    ) -> dict:
        if type(permit) is not EmbeddingPermit or permit.bot_id != self.store.bot_id:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding permit", 422)
        _bounded(handoff_operation_id, "handoff_operation_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            attempt = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE"
                    ).format(self._table("embedding_attempts")),
                    (self.store.bot_id, permit.attempt_id),
                )
            ).fetchone()
            if attempt is None or not self._permit_matches(attempt, permit):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding permit differs", 409)
            if attempt["state"] == "DISPATCHED":
                if attempt["handoff_operation_id"] != handoff_operation_id:
                    raise ForestError(
                        "MEMORY_EMBEDDING_HANDOFF_CONFLICT", "Embedding handoff differs", 409
                    )
                return dict(attempt)
            if attempt["state"] != "PREPARED":
                raise ForestError(
                    "MEMORY_EMBEDDING_NOT_DISPATCHABLE", "Embedding attempt cannot be handed off", 409
                )
            if datetime.now(timezone.utc) >= permit.deadline_at:
                raise ForestError("MEMORY_EMBEDDING_PERMIT_EXPIRED", "Embedding permit expired", 409)
            await self._verify_fence(conn, permit)
            facet = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND facet_id=%s FOR UPDATE"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, permit.facet_id),
                )
            ).fetchone()
            if (
                facet is None
                or facet["state"] != "PREPARED"
                or facet["current_attempt_id"] != permit.attempt_id
            ):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding facet moved", 409)
            await self._epoch(conn, expected=facet["accepted_deletion_epoch"])
            await self._source_parts_in(conn, facet["source_unit_id"], lock=True)
            attempt = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='DISPATCHED',handoff_operation_id=%s,"
                        "dispatched_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s "
                        "RETURNING *"
                    ).format(self._table("embedding_attempts")),
                    (handoff_operation_id, self.store.bot_id, permit.attempt_id),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='DISPATCHED',updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND facet_id=%s"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, permit.facet_id),
            )
            return dict(attempt)

    async def settle_attempt(
        self,
        permit: EmbeddingPermit,
        *,
        settlement_operation_id: str,
        outcome: str,
        expected_deletion_epoch: int,
        result: MemoryPayloadRef | None = None,
        vector=None,
        error_code: str | None = None,
    ) -> dict:
        if type(permit) is not EmbeddingPermit or permit.bot_id != self.store.bot_id:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding permit", 422)
        _bounded(settlement_operation_id, "settlement_operation_id", 256)
        if outcome not in {
            "SUCCEEDED",
            "NOT_SENT",
            "UNKNOWN",
            "RETRYABLE_FAILURE",
            "FAILED",
        }:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding outcome", 422)
        if error_code is not None and (
            not isinstance(error_code, str) or _ERROR_RE.fullmatch(error_code) is None
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding error code", 422)
        if outcome == "SUCCEEDED":
            if type(result) is not MemoryPayloadRef or vector is None or error_code is not None:
                raise ForestError("MEMORY_INPUT_INVALID", "Embedding success is incomplete", 422)
            result.validate("embedding.result")
            if (
                result.owner_component_id != permit.executor_component_id
                or result.owner_kind != "embedding.result"
                or result.owner_id != permit.attempt_id
            ):
                raise ForestError(
                    "MEMORY_EMBEDDING_RESULT_CONFLICT",
                    "Embedding result does not belong to this attempt",
                    409,
                )
        elif result is not None or vector is not None or error_code is None:
            raise ForestError("MEMORY_INPUT_INVALID", "Embedding failure receipt is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            attempt = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE"
                    ).format(self._table("embedding_attempts")),
                    (self.store.bot_id, permit.attempt_id),
                )
            ).fetchone()
            if attempt is None or not self._permit_matches(attempt, permit):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding permit differs", 409)
            if attempt["settlement_operation_id"] is not None:
                if (
                    attempt["settlement_operation_id"] != settlement_operation_id
                    or attempt["state"] != outcome
                ):
                    raise ForestError(
                        "MEMORY_EMBEDDING_SETTLEMENT_CONFLICT",
                        "Embedding settlement differs",
                        409,
                    )
                return dict(attempt)
            expected_states = {"PREPARED", "DISPATCHED"} if outcome == "NOT_SENT" else {"DISPATCHED"}
            if attempt["state"] not in expected_states:
                raise ForestError(
                    "MEMORY_EMBEDDING_NOT_SETTLEABLE", "Embedding attempt state differs", 409
                )
            await self._verify_fence(conn, permit)
            facet = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND facet_id=%s FOR UPDATE"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, permit.facet_id),
                )
            ).fetchone()
            if facet is None or facet["current_attempt_id"] != permit.attempt_id:
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding facet moved", 409)
            domain = await self._domain_in(conn)
            vector_digest = None
            stored_vector = None
            if outcome == "SUCCEEDED":
                await self._epoch(conn, expected=expected_deletion_epoch)
                if facet["accepted_deletion_epoch"] != expected_deletion_epoch:
                    raise ForestError("DELETION_CONFLICT", "Embedding source epoch changed", 409)
                unit, _ = await self._source_parts_in(conn, facet["source_unit_id"], lock=True)
                if unit["source_revision"] != facet["source_revision"]:
                    raise ForestError("MEMORY_SOURCE_UNAVAILABLE", "Embedding source changed", 409)
                await self._verify_payload(conn, result)
                stored_vector = _float32_vector(
                    vector,
                    dimensions=domain["dimensions"],
                    normalization=domain["normalization"],
                )
                vector_digest = _digest(list(stored_vector))
            attempt = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,settlement_operation_id=%s,result_payload_id=%s,"
                        "result_payload_digest=%s,vector_digest=%s,error_code=%s,"
                        "settled_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s RETURNING *"
                    ).format(self._table("embedding_attempts")),
                    (
                        outcome,
                        settlement_operation_id,
                        result.payload_id if result else None,
                        result.digest if result else None,
                        vector_digest,
                        error_code,
                        self.store.bot_id,
                        permit.attempt_id,
                    ),
                )
            ).fetchone()
            if outcome == "SUCCEEDED":
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='READY',vector=%s,vector_digest=%s,"
                        "result_payload_id=%s,result_payload_digest=%s,"
                        "published_at=clock_timestamp(),updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND facet_id=%s"
                    ).format(self._table("embedding_facets")),
                    (
                        list(stored_vector),
                        vector_digest,
                        result.payload_id,
                        result.digest,
                        self.store.bot_id,
                        permit.facet_id,
                    ),
                )
            else:
                facet_state = {
                    "NOT_SENT": "PENDING",
                    "RETRYABLE_FAILURE": "PENDING",
                    "UNKNOWN": "UNKNOWN",
                    "FAILED": "FAILED",
                }[outcome]
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,current_attempt_id=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND facet_id=%s"
                    ).format(self._table("embedding_facets")),
                    (facet_state, self.store.bot_id, permit.facet_id),
                )
            return dict(attempt)

    async def freeze_bundle(
        self,
        *,
        operation_id: str,
        bundle_id: str,
        query_id: str,
        expected_deletion_epoch: int,
        result_limit: int = 8,
        candidate_limit_per_facet: int = 64,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (bundle_id, "bundle_id"),
            (query_id, "query_id"),
        ):
            _bounded(value, field, 256)
        if type(result_limit) is not int or not 1 <= result_limit <= 256:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid retrieval result limit", 422)
        if (
            type(candidate_limit_per_facet) is not int
            or not 1 <= candidate_limit_per_facet <= 256
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid candidate limit", 422)
        input_digest = _digest(
            {
                "bundle_id": bundle_id,
                "query_id": query_id,
                "result_limit": result_limit,
                "candidate_limit_per_facet": candidate_limit_per_facet,
                "expected_deletion_epoch": expected_deletion_epoch,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock_identities(
                conn,
                "bundle:" + bundle_id,
                "bundle-operation:" + operation_id,
            )
            query = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND query_id=%s FOR UPDATE"
                    ).format(self._table("memory_queries")),
                    (self.store.bot_id, query_id),
                )
            ).fetchone()
            if query is None:
                raise ForestError("MEMORY_QUERY_NOT_FOUND", "Memory query does not exist", 404)
            if query["state"] == "FROZEN":
                bundle = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT * FROM {} WHERE bot_id=%s AND query_id=%s"
                        ).format(self._table("retrieval_bundles")),
                        (self.store.bot_id, query_id),
                    )
                ).fetchone()
                if (
                    bundle is None
                    or bundle["bundle_id"] != bundle_id
                    or bundle["operation_id"] != operation_id
                    or bundle["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "MEMORY_QUERY_ALREADY_FROZEN", "Memory query has another evidence bundle", 409
                    )
                return await self._read_bundle_in(conn, bundle_id, expected_deletion_epoch)
            deletion_epoch = await self._epoch(conn, expected=expected_deletion_epoch)
            if query["accepted_deletion_epoch"] != deletion_epoch:
                raise ForestError("DELETION_CONFLICT", "Memory query epoch changed", 409)
            domain = await self._domain_in(conn)
            if query["fusion_policy_revision"] != domain["fusion_policy_revision"]:
                raise ForestError("MEMORY_SPACE_IMMUTABLE", "Query fusion space changed", 409)
            query_facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND query_id=%s ORDER BY facet_id"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, query_id),
                )
            ).fetchall()
            if not query_facets or any(row["state"] != "READY" for row in query_facets):
                raise ForestError(
                    "MEMORY_QUERY_NOT_READY", "Every text and image query facet must be ready", 409
                )
            query_source_ids = sorted({row["source_unit_id"] for row in query_facets})
            index_snapshot = await (
                await conn.execute("SELECT clock_timestamp() AS snapshot")
            ).fetchone()
            snapshot_at = index_snapshot["snapshot"]
            ranked_by_query = []
            for query_facet in query_facets:
                rows = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT f.facet_id,f.source_unit_id,f.source_revision,f.modality,"
                            "{}.memory_vector_dot(f.vector,%s::real[]) AS score "
                            "FROM {} f JOIN {} u ON u.bot_id=f.bot_id "
                            "AND u.source_unit_id=f.source_unit_id "
                            "WHERE f.bot_id=%s AND f.purpose='DOCUMENT' AND f.state='READY' "
                            "AND f.space_id=%s AND f.published_at<=%s "
                            "AND u.eligibility_state='ELIGIBLE' "
                            "AND NOT (f.source_unit_id=ANY(%s)) "
                            "ORDER BY score DESC,f.facet_id LIMIT %s"
                        ).format(
                            sql.Identifier(self.namespace),
                            self._table("embedding_facets"),
                            self._table("source_units"),
                        ),
                        (
                            list(query_facet["vector"]),
                            self.store.bot_id,
                            domain["space_id"],
                            snapshot_at,
                            query_source_ids,
                            candidate_limit_per_facet,
                        ),
                    )
                ).fetchall()
                best_by_source = {}
                for rank, row in enumerate(rows, 1):
                    if row["source_unit_id"] not in best_by_source:
                        best_by_source[row["source_unit_id"]] = (rank, dict(row))
                ranked_by_query.append((dict(query_facet), best_by_source))
            scores = {}
            matches = {}
            for query_facet, candidates in ranked_by_query:
                for source_unit_id, (rank, candidate) in candidates.items():
                    scores[source_unit_id] = scores.get(source_unit_id, 0.0) + 1.0 / (60 + rank)
                    matches.setdefault(source_unit_id, []).append(
                        {
                            "query_facet_id": query_facet["facet_id"],
                            "document_facet_id": candidate["facet_id"],
                            "query_modality": query_facet["modality"],
                            "document_modality": candidate["modality"],
                            "candidate_rank": rank,
                            "score": candidate["score"],
                            "source_revision": candidate["source_revision"],
                        }
                    )
            selected_ids = sorted(scores, key=lambda item: (-scores[item], item))[:result_limit]
            selected = {}
            if selected_ids:
                rows = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=ANY(%s) "
                            "FOR SHARE"
                        ).format(self._table("source_units")),
                        (self.store.bot_id, selected_ids),
                    )
                ).fetchall()
                selected = {row["source_unit_id"]: dict(row) for row in rows}
            if set(selected_ids) != set(selected) or any(
                selected[source_id]["eligibility_state"] != "ELIGIBLE"
                or any(
                    match["source_revision"] != selected[source_id]["source_revision"]
                    for match in matches[source_id]
                )
                for source_id in selected_ids
            ):
                raise ForestError(
                    "MEMORY_EVIDENCE_REVOKED", "A retrieval candidate became unavailable", 409
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,bundle_id,query_id,operation_id,input_digest,"
                    "index_snapshot_at,accepted_deletion_epoch,result_limit,"
                    "candidate_limit_per_facet,result_count) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("retrieval_bundles")),
                (
                    self.store.bot_id,
                    bundle_id,
                    query_id,
                    operation_id,
                    input_digest,
                    snapshot_at,
                    deletion_epoch,
                    result_limit,
                    candidate_limit_per_facet,
                    len(selected_ids),
                ),
            )
            for item_index, source_unit_id in enumerate(selected_ids):
                rank_micros = round(scores[source_unit_id] * 1_000_000_000_000)
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,bundle_id,item_index,source_unit_id,"
                        "source_revision,rank_micros) VALUES (%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("retrieval_items")),
                    (
                        self.store.bot_id,
                        bundle_id,
                        item_index,
                        source_unit_id,
                        selected[source_unit_id]["source_revision"],
                        rank_micros,
                    ),
                )
                ordered_matches = sorted(
                    matches[source_unit_id],
                    key=lambda item: (
                        item["query_facet_id"],
                        item["candidate_rank"],
                        item["document_facet_id"],
                    ),
                )
                for match_index, match in enumerate(ordered_matches):
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,bundle_id,item_index,match_index,"
                            "query_facet_id,document_facet_id,query_modality,document_modality,"
                            "candidate_rank,score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        ).format(self._table("retrieval_matches")),
                        (
                            self.store.bot_id,
                            bundle_id,
                            item_index,
                            match_index,
                            match["query_facet_id"],
                            match["document_facet_id"],
                            match["query_modality"],
                            match["document_modality"],
                            match["candidate_rank"],
                            match["score"],
                        ),
                    )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='FROZEN',bundle_id=%s,frozen_at=clock_timestamp() "
                    "WHERE bot_id=%s AND query_id=%s"
                ).format(self._table("memory_queries")),
                (bundle_id, self.store.bot_id, query_id),
            )
            return await self._read_bundle_in(conn, bundle_id, expected_deletion_epoch)

    async def read_bundle(
        self,
        bundle_id: str,
        *,
        expected_deletion_epoch: int,
    ) -> dict:
        _bounded(bundle_id, "bundle_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._read_bundle_in(conn, bundle_id, expected_deletion_epoch)

    async def _read_bundle_in(self, conn, bundle_id: str, expected_deletion_epoch: int) -> dict:
        deletion_epoch = await self._epoch(conn, expected=expected_deletion_epoch)
        bundle = await (
            await conn.execute(
                sql.SQL(
                    "SELECT b.*,q.current_source_id,q.query_mode,q.input_batch_ref,"
                    "q.context_revision FROM {} b JOIN {} q ON q.bot_id=b.bot_id "
                    "AND q.query_id=b.query_id WHERE b.bot_id=%s AND b.bundle_id=%s"
                ).format(self._table("retrieval_bundles"), self._table("memory_queries")),
                (self.store.bot_id, bundle_id),
            )
        ).fetchone()
        if bundle is None:
            raise ForestError("MEMORY_BUNDLE_NOT_FOUND", "Evidence bundle does not exist", 404)
        if bundle["accepted_deletion_epoch"] != deletion_epoch:
            raise ForestError("MEMORY_EVIDENCE_REVOKED", "Evidence bundle deletion epoch changed", 409)
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT i.*,u.source_id,u.trunk_id,u.unit_kind,u.modality,u.author_subject_ref,"
                    "u.author_payload_id,u.author_payload_digest,u.author_owner_component_id,"
                    "u.author_owner_kind,u.author_owner_id,u.occurred_at,u.eligibility_state,"
                    "s.source_kind,s.platform,s.realm_id,s.external_group_id "
                    "FROM {} i JOIN {} u ON u.bot_id=i.bot_id AND u.source_unit_id=i.source_unit_id "
                    "JOIN {} s ON s.bot_id=u.bot_id AND s.source_id=u.source_id "
                    "WHERE i.bot_id=%s AND i.bundle_id=%s ORDER BY i.item_index"
                ).format(
                    self._table("retrieval_items"),
                    self._table("source_units"),
                    self._table("memory_sources"),
                ),
                (self.store.bot_id, bundle_id),
            )
        ).fetchall()
        if any(row["eligibility_state"] != "ELIGIBLE" for row in rows):
            raise ForestError("MEMORY_EVIDENCE_REVOKED", "Evidence source is unavailable", 409)
        query_facets = await (
            await conn.execute(
                sql.SQL(
                    "SELECT modality,state FROM {} WHERE bot_id=%s AND query_id=%s "
                    "ORDER BY facet_id"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, bundle["query_id"]),
            )
        ).fetchall()
        evidence = []
        for row in rows:
            author = MemoryPayloadRef(
                row["author_payload_id"],
                row["author_payload_digest"],
                row["author_owner_component_id"],
                row["author_owner_kind"],
                row["author_owner_id"],
            )
            await self._verify_payload(conn, author)
            parts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=%s "
                        "ORDER BY part_index,part_id"
                    ).format(self._table("source_parts")),
                    (self.store.bot_id, row["source_unit_id"]),
                )
            ).fetchall()
            part_values = []
            for part in parts:
                reference = self._part_ref(part)
                await self._verify_payload(conn, reference)
                part_values.append(
                    {
                        "part_id": part["part_id"],
                        "part_index": part["part_index"],
                        "part_kind": part["part_kind"],
                        "media_type": part["media_type"],
                        "payload_id": part["payload_id"],
                        "payload_digest": part["payload_digest"],
                        "width": part["width"],
                        "height": part["height"],
                        "byte_length": part["byte_length"],
                    }
                )
            match_rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT query_facet_id,document_facet_id,query_modality,"
                        "document_modality,candidate_rank,score FROM {} "
                        "WHERE bot_id=%s AND bundle_id=%s AND item_index=%s ORDER BY match_index"
                    ).format(self._table("retrieval_matches")),
                    (self.store.bot_id, bundle_id, row["item_index"]),
                )
            ).fetchall()
            evidence.append(
                {
                    "evidence_id": f"{bundle_id}/{row['item_index']}",
                    "source_unit_id": row["source_unit_id"],
                    "source_revision": row["source_revision"],
                    "source_id": row["source_id"],
                    "source_kind": row["source_kind"],
                    "trunk_id": row["trunk_id"],
                    "platform": row["platform"],
                    "realm_id": row["realm_id"],
                    "external_group_id": row["external_group_id"],
                    "is_current_group": row["source_id"] == bundle["current_source_id"],
                    "author_subject_ref": row["author_subject_ref"],
                    "occurred_at": row["occurred_at"],
                    "unit_kind": row["unit_kind"],
                    "modality": row["modality"],
                    "rank_micros": row["rank_micros"],
                    "parts": part_values,
                    "matches": [dict(match) for match in match_rows],
                }
            )
        return {
            "bundle_id": bundle_id,
            "query_id": bundle["query_id"],
            "query_mode": bundle["query_mode"],
            "current_source_id": bundle["current_source_id"],
            "input_batch_ref": bundle["input_batch_ref"],
            "context_revision": bundle["context_revision"],
            "index_snapshot_at": bundle["index_snapshot_at"],
            "accepted_deletion_epoch": bundle["accepted_deletion_epoch"],
            "evidence": evidence,
            "completeness": {
                "query_modalities": sorted({facet["modality"] for facet in query_facets}),
                "query_facets_ready": sum(facet["state"] == "READY" for facet in query_facets),
                "query_facet_count": len(query_facets),
                "result_count": len(evidence),
                "candidate_limit_per_facet": bundle["candidate_limit_per_facet"],
                "exact_projection": True,
                "ann_index": False,
            },
        }

"""Clean multimodal embedding and retrieval authority for ``memory.graph``.

The repository owns facet eligibility, embedding attempt convergence and frozen
evidence bundles.  It never captions an image and never treats an embedding
driver as an authority for source identity or visibility.
"""

from __future__ import annotations

import hashlib
import heapq
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
_CHUNKER_REVISION = "utf8-contiguous-4096-v2"
_MAX_TEXT_FACET_BYTES = 4096
_MAX_IMAGE_FACET_BYTES = 4 * 1024 * 1024
_MAX_BATCH_IMAGE_FACET_BYTES = 8 * 1024 * 1024


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


@dataclass(frozen=True)
class EmbeddingClaim:
    bot_id: str
    claim_id: str
    operation_id: str
    purpose: str
    accepted_deletion_epoch: int
    deadline_at: datetime
    permits: tuple[EmbeddingPermit, ...]
    prepare_operation_ids: tuple[str, ...]


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

    async def _source_parts_in(
        self,
        conn,
        source_unit_id: str,
        *,
        lock=False,
        allow_registered: bool = False,
    ):
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
        accepted_states = {"ELIGIBLE", "REGISTERED"} if allow_registered else {"ELIGIBLE"}
        if unit is None or unit["eligibility_state"] not in accepted_states:
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
                    (
                        start,
                        end,
                        chunk.encode("utf-8"),
                        "PENDING" if chunk.strip() else "SUPPRESSED",
                    )
                    for start, end, chunk in _text_chunks(text)
                ]
            elif part["part_kind"] == "IMAGE":
                selectors = [
                    (
                        None,
                        None,
                        content,
                        "PENDING"
                        if len(content) <= _MAX_IMAGE_FACET_BYTES
                        else "SUPPRESSED",
                    )
                ]
            for start, end, facet_content, initial_state in selectors:
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
                        "initial_state": initial_state,
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
                    "input_byte_length,space_id,accepted_deletion_epoch,state) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
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
                    item["initial_state"],
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
            or row["eligibility_state"] not in {"REGISTERED", "ELIGIBLE"}
            or row["occurrence_source_unit_id"] != part.source_unit_id
            or row["occurrence_state"] not in {"ADMITTED", "ELIGIBLE"}
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

    async def resolve_query_input(
        self,
        *,
        conversation_ref: str,
        input_batch_ref: str,
        context_revision: int,
    ) -> dict | None:
        for value, field in (
            (conversation_ref, "conversation_ref"),
            (input_batch_ref, "input_batch_ref"),
        ):
            _bounded(value, field, 256)
        if type(context_revision) is not int or context_revision < 1:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid context revision", 422)
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT o.occurrence_id,o.source_unit_id,u.source_revision,u.source_id,"
                        "s.source_kind,p.part_id FROM {} o JOIN {} u ON u.bot_id=o.bot_id "
                        "AND u.source_unit_id=o.source_unit_id JOIN {} s ON s.bot_id=u.bot_id "
                        "AND s.source_id=u.source_id JOIN {} p ON p.bot_id=u.bot_id "
                        "AND p.source_unit_id=u.source_unit_id WHERE o.bot_id=%s "
                        "AND o.conversation_ref=%s AND o.input_batch_ref=%s "
                        "AND o.context_revision=%s AND o.eligibility_state IN ('ADMITTED','ELIGIBLE') "
                        "AND u.eligibility_state IN ('REGISTERED','ELIGIBLE') "
                        "ORDER BY o.observed_at,o.occurrence_id,p.part_index,p.part_id"
                    ).format(
                        self._table("source_occurrences"),
                        self._table("source_units"),
                        self._table("memory_sources"),
                        self._table("source_parts"),
                    ),
                    (
                        self.store.bot_id,
                        conversation_ref,
                        input_batch_ref,
                        context_revision,
                    ),
                )
            ).fetchall()
        if not rows:
            return None
        source_ids = {row["source_id"] for row in rows}
        source_kinds = {row["source_kind"] for row in rows}
        if len(source_ids) != 1 or source_kinds != {"GROUP"}:
            return None
        parts = {}
        for row in rows:
            parts.setdefault(
                (row["source_unit_id"], row["part_id"]),
                RetrievalQueryPart(
                    row["source_unit_id"],
                    row["source_revision"],
                    row["occurrence_id"],
                    row["part_id"],
                ),
            )
        return {
            "current_source_id": next(iter(source_ids)),
            "parts": tuple(parts.values()),
        }

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
                    "SELECT facet_id,purpose,source_unit_id,source_revision,part_id,modality,"
                    "selector_start_char,selector_end_char,space_id,state FROM {} "
                    "WHERE bot_id=%s AND query_id=%s "
                    "ORDER BY source_unit_id,part_id,selector_start_char NULLS FIRST,facet_id"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, query_id),
            )
        ).fetchall()
        return {**dict(query), "duplicate": duplicate, "facets": [dict(row) for row in facets]}

    async def query_view(self, query_id: str) -> dict:
        _bounded(query_id, "query_id", 256)
        async with self.store.pool.connection() as conn:
            return await self._query_view_in(conn, query_id)

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
            if row["eligibility_state"] != "ELIGIBLE" and not (
                row["purpose"] == "QUERY" and row["eligibility_state"] == "REGISTERED"
            ):
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

    async def _claim_from_row(self, conn, row) -> EmbeddingClaim:
        items = await (
            await conn.execute(
                sql.SQL(
                    "SELECT a.* FROM {} i JOIN {} a ON a.bot_id=i.bot_id "
                    "AND a.attempt_id=i.attempt_id AND a.facet_id=i.facet_id "
                    "WHERE i.bot_id=%s AND i.claim_id=%s ORDER BY i.item_index"
                ).format(
                    self._table("embedding_claim_items"),
                    self._table("embedding_attempts"),
                ),
                (self.store.bot_id, row["claim_id"]),
            )
        ).fetchall()
        if len(items) != row["item_count"]:
            raise ForestError(
                "MEMORY_EMBEDDING_CLAIM_CORRUPT",
                "Embedding claim items do not match their durable claim",
                500,
            )
        return EmbeddingClaim(
            self.store.bot_id,
            row["claim_id"],
            row["operation_id"],
            row["purpose"],
            row["accepted_deletion_epoch"],
            row["deadline_at"],
            tuple(self._permit(item) for item in items),
            tuple(item["operation_id"] for item in items),
        )

    async def claim_pending_batch(
        self,
        *,
        operation_id: str,
        claim_id: str,
        purpose: str,
        executor_component_id: str,
        executor_generation_id: str,
        route_revision: str,
        deadline_at: datetime,
        item_limit: int = 32,
    ) -> EmbeddingClaim:
        """Freeze one restart-safe Provider batch from currently pending facets."""
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (claim_id, "claim_id", 256),
            (executor_component_id, "executor_component_id", 128),
            (executor_generation_id, "executor_generation_id", 256),
            (route_revision, "route_revision", 256),
        ):
            _bounded(value, field, limit)
        if purpose not in {"DOCUMENT", "QUERY"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding purpose", 422)
        if type(item_limit) is not int or not 1 <= item_limit <= 32:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding claim limit", 422)
        if not isinstance(deadline_at, datetime) or deadline_at.tzinfo is None:
            raise ForestError("MEMORY_INPUT_INVALID", "Embedding deadline needs a timezone", 422)
        deadline_at = deadline_at.astimezone(timezone.utc)
        input_digest = _digest(
            {
                "claim_id": claim_id,
                "purpose": purpose,
                "executor_component_id": executor_component_id,
                "executor_generation_id": executor_generation_id,
                "route_revision": route_revision,
                "deadline_at": deadline_at.isoformat(),
                "item_limit": item_limit,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock_identities(
                conn,
                "embedding-claim:" + claim_id,
                "embedding-claim-operation:" + operation_id,
            )
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND "
                        "(claim_id=%s OR operation_id=%s)"
                    ).format(self._table("embedding_claims")),
                    (self.store.bot_id, claim_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if (
                    replay["claim_id"] != claim_id
                    or replay["operation_id"] != operation_id
                    or replay["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "MEMORY_EMBEDDING_CLAIM_CONFLICT",
                        "Embedding claim identity differs",
                        409,
                    )
                return await self._claim_from_row(conn, replay)
            now = datetime.now(timezone.utc)
            if not now < deadline_at <= now + timedelta(minutes=5):
                raise ForestError(
                    "MEMORY_INPUT_INVALID", "Embedding deadline is outside its bound", 422
                )
            current_deletion_epoch = await self._epoch(conn)
            facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT f.* FROM {} f JOIN {} u ON u.bot_id=f.bot_id "
                        "AND u.source_unit_id=f.source_unit_id "
                        "WHERE f.bot_id=%s AND f.state='PENDING' AND f.purpose=%s "
                        "AND (u.eligibility_state='ELIGIBLE' OR "
                        "(%s='QUERY' AND u.eligibility_state='REGISTERED')) "
                        "AND u.source_revision=f.source_revision "
                        "ORDER BY f.created_at,f.facet_id LIMIT %s "
                        "FOR UPDATE OF f SKIP LOCKED"
                    ).format(
                        self._table("embedding_facets"),
                        self._table("source_units"),
                    ),
                    (self.store.bot_id, purpose, purpose, item_limit),
                )
            ).fetchall()
            selected_facets = []
            selected_image_bytes = 0
            for facet in facets:
                if facet["modality"] == "IMAGE":
                    next_image_bytes = selected_image_bytes + facet["input_byte_length"]
                    if next_image_bytes > _MAX_BATCH_IMAGE_FACET_BYTES:
                        continue
                    selected_image_bytes = next_image_bytes
                selected_facets.append(facet)
            facets = selected_facets
            claim = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,claim_id,operation_id,input_digest,purpose,"
                        "executor_component_id,executor_generation_id,route_revision,"
                        "accepted_deletion_epoch,deadline_at,item_limit,item_count) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("embedding_claims")),
                    (
                        self.store.bot_id,
                        claim_id,
                        operation_id,
                        input_digest,
                        purpose,
                        executor_component_id,
                        executor_generation_id,
                        route_revision,
                        current_deletion_epoch,
                        deadline_at,
                        item_limit,
                        len(facets),
                    ),
                )
            ).fetchone()
            for index, facet in enumerate(facets):
                unit, _ = await self._source_parts_in(
                    conn,
                    facet["source_unit_id"],
                    lock=True,
                    allow_registered=facet["purpose"] == "QUERY",
                )
                if unit["source_revision"] != facet["source_revision"]:
                    raise ForestError(
                        "MEMORY_SOURCE_UNAVAILABLE", "Embedding source changed", 409
                    )
                attempt_no = facet["attempt_count"] + 1
                identity = _canonical(
                    [self.store.bot_id, claim_id, facet["facet_id"], attempt_no]
                )
                identity_digest = hashlib.sha256(identity).hexdigest()
                attempt_id = "embedding-attempt:" + identity_digest
                prepare_operation_id = "embedding-prepare:" + identity_digest
                permit = EmbeddingPermit(
                    self.store.bot_id,
                    facet["facet_id"],
                    attempt_id,
                    attempt_no,
                    executor_component_id,
                    executor_generation_id,
                    route_revision,
                    current_deletion_epoch,
                    deadline_at,
                )
                await self._verify_fence(conn, permit)
                attempt_input_digest = _digest(
                    {
                        "attempt_id": attempt_id,
                        "facet_id": facet["facet_id"],
                        "executor_component_id": executor_component_id,
                        "executor_generation_id": executor_generation_id,
                        "route_revision": route_revision,
                        "deadline_at": deadline_at.isoformat(),
                    }
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,attempt_id,operation_id,input_digest,facet_id,"
                        "attempt_no,executor_component_id,executor_generation_id,route_revision,"
                        "accepted_deletion_epoch,deadline_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("embedding_attempts")),
                    (
                        self.store.bot_id,
                        attempt_id,
                        prepare_operation_id,
                        attempt_input_digest,
                        facet["facet_id"],
                        attempt_no,
                        executor_component_id,
                        executor_generation_id,
                        route_revision,
                        current_deletion_epoch,
                        deadline_at,
                    ),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,claim_id,item_index,facet_id,attempt_id) "
                        "VALUES (%s,%s,%s,%s,%s)"
                    ).format(self._table("embedding_claim_items")),
                    (
                        self.store.bot_id,
                        claim_id,
                        index,
                        facet["facet_id"],
                        attempt_id,
                    ),
                )
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='PREPARED',attempt_count=%s,"
                        "current_attempt_id=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND facet_id=%s"
                    ).format(self._table("embedding_facets")),
                    (
                        attempt_no,
                        attempt_id,
                        self.store.bot_id,
                        facet["facet_id"],
                    ),
                )
            return await self._claim_from_row(conn, claim)

    async def has_pending_facets(self, *, purpose: str = "DOCUMENT") -> bool:
        if purpose not in {"DOCUMENT", "QUERY"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding purpose", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT 1 FROM {} f JOIN {} u ON u.bot_id=f.bot_id "
                        "AND u.source_unit_id=f.source_unit_id WHERE f.bot_id=%s "
                        "AND f.state='PENDING' AND f.purpose=%s "
                        "AND (u.eligibility_state='ELIGIBLE' OR "
                        "(%s='QUERY' AND u.eligibility_state='REGISTERED')) "
                        "AND u.source_revision=f.source_revision LIMIT 1"
                    ).format(
                        self._table("embedding_facets"),
                        self._table("source_units"),
                    ),
                    (self.store.bot_id, purpose, purpose),
                )
            ).fetchone()
        return row is not None

    async def recover_expired_attempts(self, *, limit: int = 100) -> dict:
        """Converge expired attempts without guessing whether a handed-off request ran."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid recovery limit", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            attempts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s "
                        "AND state IN ('PREPARED','DISPATCHED') "
                        "AND deadline_at<=clock_timestamp() "
                        "ORDER BY deadline_at,attempt_id LIMIT %s FOR UPDATE SKIP LOCKED"
                    ).format(self._table("embedding_attempts")),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            not_sent = unknown = 0
            for attempt in attempts:
                if attempt["state"] == "PREPARED":
                    outcome, facet_state = "NOT_SENT", "PENDING"
                    error_code = "EMBEDDING_ATTEMPT_EXPIRED_BEFORE_HANDOFF"
                    not_sent += 1
                else:
                    outcome, facet_state = "UNKNOWN", "UNKNOWN"
                    error_code = "EMBEDDING_ATTEMPT_EXPIRED_AFTER_HANDOFF"
                    unknown += 1
                settlement_operation_id = "embedding-recovery:" + hashlib.sha256(
                    attempt["attempt_id"].encode("utf-8")
                ).hexdigest()
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,settlement_operation_id=%s,error_code=%s,"
                        "settled_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s"
                    ).format(self._table("embedding_attempts")),
                    (
                        outcome,
                        settlement_operation_id,
                        error_code,
                        self.store.bot_id,
                        attempt["attempt_id"],
                    ),
                )
                updated = await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,current_attempt_id=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND facet_id=%s "
                        "AND state=%s AND current_attempt_id=%s"
                    ).format(self._table("embedding_facets")),
                    (
                        facet_state,
                        self.store.bot_id,
                        attempt["facet_id"],
                        attempt["state"],
                        attempt["attempt_id"],
                    ),
                )
                if updated.rowcount != 1:
                    raise ForestError(
                        "MEMORY_EMBEDDING_STATE_CORRUPT",
                        "Embedding facet and attempt states diverged",
                        500,
                    )
            return {
                "recovered": len(attempts),
                "not_sent": not_sent,
                "unknown": unknown,
            }

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
            current_deletion_epoch = await self._epoch(conn)
            unit, _ = await self._source_parts_in(
                conn,
                facet["source_unit_id"],
                lock=True,
                allow_registered=facet["purpose"] == "QUERY",
            )
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
                current_deletion_epoch,
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
                        current_deletion_epoch,
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
            await self._epoch(conn, expected=permit.accepted_deletion_epoch)
            await self._source_parts_in(
                conn,
                facet["source_unit_id"],
                lock=True,
                allow_registered=facet["purpose"] == "QUERY",
            )
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

    async def mark_batch_handoff(
        self,
        permits,
        *,
        batch_operation_id: str,
        _connection=None,
    ) -> tuple[dict, ...]:
        """Commit every facet handoff or none before one Provider request."""
        _bounded(batch_operation_id, "batch_operation_id", 256)
        permits = tuple(permits)
        if (
            not 1 <= len(permits) <= 32
            or any(type(permit) is not EmbeddingPermit for permit in permits)
            or any(permit.bot_id != self.store.bot_id for permit in permits)
            or len({permit.attempt_id for permit in permits}) != len(permits)
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding permit batch", 422)
        handoff_ids = {
            permit.attempt_id: "batch-handoff:"
            + hashlib.sha256(
                f"{batch_operation_id}:{permit.attempt_id}".encode("utf-8")
            ).hexdigest()
            for permit in permits
        }
        ordered = tuple(sorted(permits, key=lambda permit: permit.attempt_id))
        async with self._transaction(_connection) as conn:
            attempts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND attempt_id=ANY(%s) "
                        "ORDER BY attempt_id FOR UPDATE"
                    ).format(self._table("embedding_attempts")),
                    (self.store.bot_id, [permit.attempt_id for permit in ordered]),
                )
            ).fetchall()
            if len(attempts) != len(ordered):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding attempt is missing", 409)
            by_attempt = {row["attempt_id"]: row for row in attempts}
            replay = True
            for permit in ordered:
                attempt = by_attempt[permit.attempt_id]
                if not self._permit_matches(attempt, permit):
                    raise ForestError(
                        "MEMORY_STALE_EMBEDDING_PERMIT", "Embedding permit differs", 409
                    )
                expected_handoff = handoff_ids[permit.attempt_id]
                if attempt["state"] == "DISPATCHED":
                    if attempt["handoff_operation_id"] != expected_handoff:
                        raise ForestError(
                            "MEMORY_EMBEDDING_HANDOFF_CONFLICT",
                            "Embedding batch handoff differs",
                            409,
                        )
                    continue
                replay = False
                if attempt["state"] != "PREPARED":
                    raise ForestError(
                        "MEMORY_EMBEDDING_NOT_DISPATCHABLE",
                        "Embedding batch contains a terminal attempt",
                        409,
                    )
                if datetime.now(timezone.utc) >= permit.deadline_at:
                    raise ForestError(
                        "MEMORY_EMBEDDING_PERMIT_EXPIRED", "Embedding permit expired", 409
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
                if (
                    facet is None
                    or facet["state"] != "PREPARED"
                    or facet["current_attempt_id"] != permit.attempt_id
                ):
                    raise ForestError(
                        "MEMORY_STALE_EMBEDDING_PERMIT", "Embedding facet moved", 409
                    )
                await self._epoch(conn, expected=permit.accepted_deletion_epoch)
                await self._source_parts_in(
                    conn,
                    facet["source_unit_id"],
                    lock=True,
                    allow_registered=facet["purpose"] == "QUERY",
                )
            if replay:
                return tuple(dict(by_attempt[permit.attempt_id]) for permit in permits)
            if any(by_attempt[permit.attempt_id]["state"] == "DISPATCHED" for permit in ordered):
                raise ForestError(
                    "MEMORY_EMBEDDING_HANDOFF_CONFLICT",
                    "Embedding batch was only partially handed off",
                    409,
                )
            result = []
            for permit in ordered:
                attempt = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='DISPATCHED',handoff_operation_id=%s,"
                            "dispatched_at=clock_timestamp() WHERE bot_id=%s AND attempt_id=%s "
                            "RETURNING *"
                        ).format(self._table("embedding_attempts")),
                        (
                            handoff_ids[permit.attempt_id],
                            self.store.bot_id,
                            permit.attempt_id,
                        ),
                    )
                ).fetchone()
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='DISPATCHED',updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND facet_id=%s"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, permit.facet_id),
                )
                result.append(dict(attempt))
            by_result = {row["attempt_id"]: row for row in result}
            return tuple(dict(by_result[permit.attempt_id]) for permit in permits)

    async def settle_batch(
        self,
        permits,
        *,
        batch_attempt_id: str,
        settlement_operation_id: str,
        outcome: str,
        expected_deletion_epoch: int,
        result: MemoryPayloadRef | None = None,
        vectors=None,
        error_code: str | None = None,
        _connection=None,
    ) -> tuple[dict, ...]:
        """Publish one Provider result for every claimed facet or none of them."""
        _bounded(batch_attempt_id, "batch_attempt_id", 256)
        _bounded(settlement_operation_id, "settlement_operation_id", 256)
        permits = tuple(permits)
        if (
            not 1 <= len(permits) <= 32
            or any(type(permit) is not EmbeddingPermit for permit in permits)
            or any(permit.bot_id != self.store.bot_id for permit in permits)
            or len({permit.attempt_id for permit in permits}) != len(permits)
            or len({permit.facet_id for permit in permits}) != len(permits)
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding permit batch", 422)
        if outcome not in {"SUCCEEDED", "NOT_SENT", "UNKNOWN", "FAILED"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding batch outcome", 422)
        if error_code is not None and (
            not isinstance(error_code, str) or _ERROR_RE.fullmatch(error_code) is None
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding error code", 422)
        vector_values = dict(vectors or {})
        if outcome == "SUCCEEDED":
            if (
                type(result) is not MemoryPayloadRef
                or error_code is not None
                or set(vector_values) != {permit.facet_id for permit in permits}
            ):
                raise ForestError("MEMORY_INPUT_INVALID", "Embedding success batch is incomplete", 422)
            result.validate("embedding.batch_response")
            executor_ids = {permit.executor_component_id for permit in permits}
            if (
                len(executor_ids) != 1
                or result.owner_component_id not in executor_ids
                or result.owner_kind != "embedding.batch-response"
                or result.owner_id != batch_attempt_id
            ):
                raise ForestError(
                    "MEMORY_EMBEDDING_RESULT_CONFLICT",
                    "Embedding batch response has another owner",
                    409,
                )
        elif result is not None or vector_values or error_code is None:
            raise ForestError("MEMORY_INPUT_INVALID", "Embedding failure batch is invalid", 422)
        ordered = tuple(sorted(permits, key=lambda permit: permit.attempt_id))
        async with self._transaction(_connection) as conn:
            attempts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND attempt_id=ANY(%s) "
                        "ORDER BY attempt_id FOR UPDATE"
                    ).format(self._table("embedding_attempts")),
                    (self.store.bot_id, [permit.attempt_id for permit in ordered]),
                )
            ).fetchall()
            if len(attempts) != len(ordered):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding attempt is missing", 409)
            by_attempt = {row["attempt_id"]: row for row in attempts}
            if any(
                not self._permit_matches(by_attempt[permit.attempt_id], permit)
                for permit in ordered
            ):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding permit differs", 409)
            facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND facet_id=ANY(%s) "
                        "ORDER BY facet_id FOR UPDATE"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, [permit.facet_id for permit in ordered]),
                )
            ).fetchall()
            if len(facets) != len(ordered):
                raise ForestError("MEMORY_STALE_EMBEDDING_PERMIT", "Embedding facet is missing", 409)
            by_facet = {row["facet_id"]: row for row in facets}
            terminal = [
                by_attempt[permit.attempt_id]["settlement_operation_id"] is not None
                for permit in ordered
            ]
            if any(terminal) and not all(terminal):
                raise ForestError(
                    "MEMORY_EMBEDDING_SETTLEMENT_CONFLICT",
                    "Embedding batch was only partially settled",
                    409,
                )
            domain = await self._domain_in(conn)
            vector_digests = {}
            if outcome == "SUCCEEDED":
                for permit in ordered:
                    stored = _float32_vector(
                        vector_values[permit.facet_id],
                        dimensions=domain["dimensions"],
                        normalization=domain["normalization"],
                    )
                    vector_digests[permit.attempt_id] = _digest(list(stored))
            if all(terminal):
                for permit in ordered:
                    attempt = by_attempt[permit.attempt_id]
                    if (
                        attempt["state"] != outcome
                        or attempt["settlement_operation_id"] != settlement_operation_id
                        or attempt["result_payload_id"]
                        != (result.payload_id if result is not None else None)
                        or attempt["result_payload_digest"]
                        != (result.digest if result is not None else None)
                        or attempt["vector_digest"] != vector_digests.get(permit.attempt_id)
                        or attempt["error_code"] != error_code
                    ):
                        raise ForestError(
                            "MEMORY_EMBEDDING_SETTLEMENT_CONFLICT",
                            "Embedding batch settlement differs",
                            409,
                        )
                return tuple(dict(by_attempt[permit.attempt_id]) for permit in permits)
            if outcome == "SUCCEEDED":
                await self._epoch(conn, expected=expected_deletion_epoch)
                await self._verify_payload(conn, result)
                units = {}
                for source_unit_id in sorted(
                    {by_facet[permit.facet_id]["source_unit_id"] for permit in ordered}
                ):
                    source_facets = [
                        by_facet[permit.facet_id]
                        for permit in ordered
                        if by_facet[permit.facet_id]["source_unit_id"] == source_unit_id
                    ]
                    units[source_unit_id], _ = await self._source_parts_in(
                        conn,
                        source_unit_id,
                        lock=True,
                        allow_registered=all(
                            facet["purpose"] == "QUERY" for facet in source_facets
                        ),
                    )
                for permit in ordered:
                    facet = by_facet[permit.facet_id]
                    unit = units[facet["source_unit_id"]]
                    if unit["source_revision"] != facet["source_revision"]:
                        raise ForestError(
                            "MEMORY_SOURCE_UNAVAILABLE", "Embedding source changed", 409
                        )
            expected_attempt_states = (
                {"PREPARED", "DISPATCHED"} if outcome == "NOT_SENT" else {"DISPATCHED"}
            )
            expected_facet_states = expected_attempt_states
            for permit in ordered:
                attempt = by_attempt[permit.attempt_id]
                facet = by_facet[permit.facet_id]
                if (
                    attempt["state"] not in expected_attempt_states
                    or facet["state"] not in expected_facet_states
                    or facet["current_attempt_id"] != permit.attempt_id
                ):
                    raise ForestError(
                        "MEMORY_EMBEDDING_NOT_SETTLEABLE",
                        "Embedding batch state differs",
                        409,
                    )
                await self._verify_fence(conn, permit)
            result_rows = []
            for permit in ordered:
                attempt = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state=%s,settlement_operation_id=%s,"
                            "result_payload_id=%s,result_payload_digest=%s,vector_digest=%s,"
                            "error_code=%s,settled_at=clock_timestamp() "
                            "WHERE bot_id=%s AND attempt_id=%s RETURNING *"
                        ).format(self._table("embedding_attempts")),
                        (
                            outcome,
                            settlement_operation_id,
                            result.payload_id if result is not None else None,
                            result.digest if result is not None else None,
                            vector_digests.get(permit.attempt_id),
                            error_code,
                            self.store.bot_id,
                            permit.attempt_id,
                        ),
                    )
                ).fetchone()
                if outcome == "SUCCEEDED":
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='EMBEDDED',index_ref_id=NULL,vector_digest=%s,"
                            "result_payload_id=%s,result_payload_digest=%s,"
                            "published_at=NULL,updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND facet_id=%s"
                        ).format(self._table("embedding_facets")),
                        (
                            vector_digests[permit.attempt_id],
                            result.payload_id,
                            result.digest,
                            self.store.bot_id,
                            permit.facet_id,
                        ),
                    )
                else:
                    facet_state = {
                        "NOT_SENT": "PENDING",
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
                result_rows.append(dict(attempt))
            by_result = {row["attempt_id"]: row for row in result_rows}
            return tuple(dict(by_result[permit.attempt_id]) for permit in permits)

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
                unit, _ = await self._source_parts_in(
                    conn,
                    facet["source_unit_id"],
                    lock=True,
                    allow_registered=facet["purpose"] == "QUERY",
                )
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
                        "UPDATE {} SET state='EMBEDDED',index_ref_id=NULL,vector_digest=%s,"
                        "result_payload_id=%s,result_payload_digest=%s,"
                        "published_at=NULL,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND facet_id=%s"
                    ).format(self._table("embedding_facets")),
                    (
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

    async def next_embedded_facet(self, *, purpose: str) -> dict | None:
        """Return one generated vector waiting for publication to the derived index."""
        if purpose not in {"DOCUMENT", "QUERY"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding purpose", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND purpose=%s AND state='EMBEDDED' "
                        "ORDER BY updated_at,facet_id LIMIT 1"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, purpose),
                )
            ).fetchone()
            if row is None:
                return None
            deletion_epoch = await self._epoch(conn)
            body = await self._read_payload(conn, row["result_payload_id"], deletion_epoch)
            try:
                payload = json.loads(body)
                vectors = payload["vectors"]
                match = next(item for item in vectors if item.get("facet_id") == row["facet_id"])
                vector = _float32_vector(
                    match["values"],
                    dimensions=(await self._domain_in(conn))["dimensions"],
                    normalization="L2",
                )
            except (KeyError, TypeError, ValueError, StopIteration, json.JSONDecodeError) as exc:
                raise ForestError(
                    "MEMORY_EMBEDDING_RESULT_INVALID",
                    "Persisted embedding result cannot rebuild its index row",
                    500,
                ) from exc
            if _digest(list(vector)) != row["vector_digest"]:
                raise ForestError(
                    "MEMORY_EMBEDDING_RESULT_INVALID",
                    "Persisted embedding vector digest differs",
                    409,
                )
            return {**dict(row), "vector": vector, "deletion_epoch": deletion_epoch}

    async def next_index_deletion(self) -> dict | None:
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND state='PENDING' "
                        "ORDER BY created_at,index_ref_id LIMIT 1"
                    ).format(self._table("vector_index_deletions")),
                    (self.store.bot_id,),
                )
            ).fetchone()
            return None if row is None else dict(row)

    async def settle_index_deletion(self, index_ref_id: str) -> dict:
        _bounded(index_ref_id, "index_ref_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND index_ref_id=%s FOR UPDATE").format(
                        self._table("vector_index_deletions")
                    ),
                    (self.store.bot_id, index_ref_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("MEMORY_INDEX_DELETION_NOT_FOUND", "Index deletion is unavailable", 404)
            if row["state"] == "PENDING":
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state='DELETED',deleted_at=clock_timestamp() "
                            "WHERE bot_id=%s AND index_ref_id=%s RETURNING *"
                        ).format(self._table("vector_index_deletions")),
                        (self.store.bot_id, index_ref_id),
                    )
                ).fetchone()
            return dict(row)

    async def publish_index_ref(
        self,
        *,
        facet_id: str,
        index_ref_id: str,
        expected_vector_digest: str,
        expected_deletion_epoch: int,
    ) -> dict:
        """Commit only the stable ref after an idempotent external index upsert."""
        _bounded(facet_id, "facet_id", 256)
        _bounded(index_ref_id, "index_ref_id", 256)
        if _SHA256_RE.fullmatch(expected_vector_digest) is None:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid vector digest", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._epoch(conn, expected=expected_deletion_epoch)
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND facet_id=%s FOR UPDATE").format(
                        self._table("embedding_facets")
                    ),
                    (self.store.bot_id, facet_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("MEMORY_FACET_NOT_FOUND", "Embedding facet is unavailable", 404)
            if row["state"] == "READY":
                if row["index_ref_id"] != index_ref_id or row["vector_digest"] != expected_vector_digest:
                    raise ForestError("MEMORY_INDEX_PUBLICATION_CONFLICT", "Index publication differs", 409)
                return dict(row)
            if row["state"] != "EMBEDDED" or row["vector_digest"] != expected_vector_digest:
                raise ForestError("MEMORY_INDEX_PUBLICATION_STALE", "Embedding facet changed", 409)
            updated = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='READY',index_ref_id=%s,published_at=clock_timestamp(),"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND facet_id=%s RETURNING *"
                    ).format(self._table("embedding_facets")),
                    (index_ref_id, self.store.bot_id, facet_id),
                )
            ).fetchone()
            return dict(updated)

    async def prepare_vector_search(self, query_id: str) -> dict:
        """Freeze a PostgreSQL visibility watermark before remote ANN search."""
        _bounded(query_id, "query_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            query = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND query_id=%s FOR SHARE").format(
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
                        "SELECT facet_id,index_ref_id,modality,space_id FROM {} "
                        "WHERE bot_id=%s AND query_id=%s ORDER BY facet_id"
                    ).format(self._table("embedding_facets")),
                    (self.store.bot_id, query_id),
                )
            ).fetchall()
            if not facets or any(row["index_ref_id"] is None for row in facets):
                raise ForestError("MEMORY_QUERY_NOT_READY", "Query refs are not published", 409)
            snapshot = await (await conn.execute("SELECT clock_timestamp() AS value")).fetchone()
            return {
                "query_id": query_id,
                "snapshot_at": snapshot["value"],
                "facets": [dict(row) for row in facets],
            }

    async def _graph_neighbors_in(
        self,
        conn,
        *,
        source_unit_id: str,
        sampling_seed: str,
        sequence_weight: float,
        association_weight: float,
        max_association_edges_per_node: int,
    ) -> tuple[dict, ...]:
        """Load one node's authorized neighbours with bounded association fan-out."""

        sequence_rows = await (
            await conn.execute(
                sql.SQL(
                    "WITH origin AS (SELECT trunk_id,source_order_seq FROM {} "
                    "WHERE bot_id=%s AND source_unit_id=%s AND eligibility_state='ELIGIBLE') "
                    "SELECT u.source_unit_id,u.source_order_seq FROM origin o CROSS JOIN LATERAL ("
                    "(SELECT n.source_unit_id,n.source_order_seq FROM {} n WHERE n.bot_id=%s "
                    "AND n.trunk_id=o.trunk_id AND n.eligibility_state='ELIGIBLE' "
                    "AND n.source_order_seq<o.source_order_seq ORDER BY n.source_order_seq DESC LIMIT 1) "
                    "UNION ALL "
                    "(SELECT n.source_unit_id,n.source_order_seq FROM {} n WHERE n.bot_id=%s "
                    "AND n.trunk_id=o.trunk_id AND n.eligibility_state='ELIGIBLE' "
                    "AND n.source_order_seq>o.source_order_seq ORDER BY n.source_order_seq LIMIT 1)"
                    ") u ORDER BY u.source_order_seq"
                ).format(
                    self._table("source_units"),
                    self._table("source_units"),
                    self._table("source_units"),
                ),
                (
                    self.store.bot_id,
                    source_unit_id,
                    self.store.bot_id,
                    self.store.bot_id,
                ),
            )
        ).fetchall()
        result = []
        for row in sequence_rows:
            target = row["source_unit_id"]
            pair = sorted((source_unit_id, target))
            result.append(
                {
                    "edge_id": "sequence:"
                    + hashlib.sha256(_canonical([self.store.bot_id, *pair])).hexdigest(),
                    "relation_kind": "SEQUENCE",
                    "source_unit_id": source_unit_id,
                    "target_unit_id": target,
                    "weight": sequence_weight,
                    "contribution_id": None,
                }
            )

        association_rows = await (
            await conn.execute(
                sql.SQL(
                    "WITH candidates AS (SELECT c.relation_id,c.contribution_id,c.weight_micros,"
                    "CASE WHEN c.left_id=%s THEN c.right_id ELSE c.left_id END AS target_unit_id,"
                    "row_number() OVER (PARTITION BY CASE WHEN c.left_id=%s THEN c.right_id "
                    "ELSE c.left_id END ORDER BY c.weight_micros DESC,c.contribution_id) AS target_rank "
                    "FROM {} c WHERE c.bot_id=%s AND c.active AND "
                    "c.relation_kind='ASSOCIATES_WITH' AND c.left_kind='SOURCE_UNIT' "
                    "AND c.right_kind='SOURCE_UNIT' AND (c.left_id=%s OR c.right_id=%s) "
                    "AND NOT EXISTS (SELECT 1 FROM {} e JOIN {} eu ON eu.bot_id=e.bot_id "
                    "AND eu.source_unit_id=e.source_unit_id WHERE e.bot_id=c.bot_id "
                    "AND e.contribution_id=c.contribution_id AND eu.eligibility_state<>'ELIGIBLE')) "
                    "SELECT c.relation_id,c.contribution_id,c.target_unit_id,c.weight_micros "
                    "FROM candidates c JOIN {} target ON target.bot_id=%s "
                    "AND target.source_unit_id=c.target_unit_id AND target.eligibility_state='ELIGIBLE' "
                    "WHERE c.target_rank=1 ORDER BY md5(%s || ':' || c.relation_id || "
                    "':' || c.contribution_id || ':' || c.target_unit_id) LIMIT %s"
                ).format(
                    self._table("relation_contributions"),
                    self._table("relation_evidence"),
                    self._table("source_units"),
                    self._table("source_units"),
                ),
                (
                    source_unit_id,
                    source_unit_id,
                    self.store.bot_id,
                    source_unit_id,
                    source_unit_id,
                    self.store.bot_id,
                    sampling_seed,
                    max_association_edges_per_node,
                ),
            )
        ).fetchall()
        result.extend(
            {
                "edge_id": row["relation_id"],
                "relation_kind": "ASSOCIATED",
                "source_unit_id": source_unit_id,
                "target_unit_id": row["target_unit_id"],
                "weight": association_weight,
                "contribution_id": row["contribution_id"],
                "contribution_weight_micros": row["weight_micros"],
            }
            for row in association_rows
        )
        return tuple(result)

    async def _expand_graph_in(
        self,
        conn,
        *,
        seeds: tuple[str, ...],
        sampling_seed: str,
        min_graph_relevance: float,
        max_graph_nodes: int,
        max_association_edges_per_node: int,
        sequence_weight: float,
        association_weight: float,
    ) -> tuple[tuple[dict, ...], tuple[dict, ...]]:
        """Run QiYu-compatible maximum-product expansion over the frozen graph view."""

        relevance = {node_id: 1.0 for node_id in dict.fromkeys(seeds)}
        predecessor: dict[str, tuple[str, str]] = {}
        queue = [(-1.0, index, node_id) for index, node_id in enumerate(relevance)]
        heapq.heapify(queue)
        finalized = set()
        encountered_edges: dict[tuple[str, str, str], dict] = {}
        order = len(queue)
        while queue and len(finalized) < max_graph_nodes:
            negative_score, _, source_id = heapq.heappop(queue)
            source_score = -negative_score
            if source_id in finalized or source_score < relevance.get(source_id, 0.0):
                continue
            finalized.add(source_id)
            neighbors = await self._graph_neighbors_in(
                conn,
                source_unit_id=source_id,
                sampling_seed=sampling_seed,
                sequence_weight=sequence_weight,
                association_weight=association_weight,
                max_association_edges_per_node=max_association_edges_per_node,
            )
            for edge in neighbors:
                encountered_edges[(edge["edge_id"], source_id, edge["target_unit_id"])] = edge
                propagated = source_score * edge["weight"]
                target = edge["target_unit_id"]
                if propagated < min_graph_relevance or propagated <= relevance.get(target, 0.0):
                    continue
                relevance[target] = propagated
                predecessor[target] = (source_id, edge["edge_id"])
                order += 1
                heapq.heappush(queue, (-propagated, order, target))
        activations = tuple(
            {
                "source_unit_id": node_id,
                "activation": relevance[node_id],
                "path_parent_source_unit_id": predecessor.get(node_id, (None, None))[0],
                "path_edge_id": predecessor.get(node_id, (None, None))[1],
            }
            for node_id in sorted(finalized, key=lambda item: (-relevance[item], item))
        )
        included = {item["source_unit_id"] for item in activations}
        edges = tuple(
            edge
            for edge in encountered_edges.values()
            if edge["source_unit_id"] in included and edge["target_unit_id"] in included
        )
        return activations, edges

    async def freeze_bundle(
        self,
        *,
        operation_id: str,
        bundle_id: str,
        query_id: str,
        expected_deletion_epoch: int,
        result_limit: int | None = None,
        seed_limit: int = 4,
        candidate_limit_per_facet: int = 64,
        min_similarity: float = 0.35,
        min_graph_relevance: float = 0.05,
        max_graph_nodes: int = 32,
        max_association_edges_per_node: int = 32,
        sequence_weight: float = 0.55,
        association_weight: float = 0.90,
        index_snapshot_at=None,
        candidate_sets=None,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (bundle_id, "bundle_id"),
            (query_id, "query_id"),
        ):
            _bounded(value, field, 256)
        if result_limit is not None:
            if type(result_limit) is not int or not 1 <= result_limit <= 256:
                raise ForestError("MEMORY_INPUT_INVALID", "Invalid retrieval result limit", 422)
            max_graph_nodes = result_limit
            seed_limit = min(seed_limit, result_limit)
        if type(seed_limit) is not int or not 1 <= seed_limit <= 256:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid retrieval seed limit", 422)
        if type(max_graph_nodes) is not int or not seed_limit <= max_graph_nodes <= 256:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid graph node limit", 422)
        if (
            type(candidate_limit_per_facet) is not int
            or not 1 <= candidate_limit_per_facet <= 256
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid candidate limit", 422)
        if type(min_similarity) not in {int, float} or not -1.0 <= min_similarity <= 1.0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid seed similarity threshold", 422)
        if (
            type(min_graph_relevance) not in {int, float}
            or not 0.0 < min_graph_relevance <= 1.0
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid graph relevance threshold", 422)
        if (
            type(max_association_edges_per_node) is not int
            or not 1 <= max_association_edges_per_node <= 4096
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid association fan-out limit", 422)
        if any(
            type(weight) not in {int, float} or not 0.0 < weight <= 1.0
            for weight in (sequence_weight, association_weight)
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid graph edge weight", 422)
        sampling_seed = query_id
        input_digest = _digest(
            {
                "bundle_id": bundle_id,
                "query_id": query_id,
                "seed_limit": seed_limit,
                "candidate_limit_per_facet": candidate_limit_per_facet,
                "min_similarity": min_similarity,
                "min_graph_relevance": min_graph_relevance,
                "max_graph_nodes": max_graph_nodes,
                "max_association_edges_per_node": max_association_edges_per_node,
                "sequence_weight": sequence_weight,
                "association_weight": association_weight,
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
            if index_snapshot_at is None or not isinstance(candidate_sets, dict):
                raise ForestError(
                    "MEMORY_VECTOR_INDEX_REQUIRED",
                    "Retrieval candidates must come from the vector index service",
                    503,
                )
            snapshot_at = index_snapshot_at
            ranked_by_query = []
            for query_facet in query_facets:
                raw_candidates = candidate_sets.get(query_facet["facet_id"])
                if not isinstance(raw_candidates, (tuple, list)):
                    raise ForestError("MEMORY_VECTOR_INDEX_RESPONSE_INVALID", "Candidate set is missing", 502)
                ordered_refs = []
                external_scores = {}
                for item in raw_candidates[:candidate_limit_per_facet]:
                    if not isinstance(item, dict) or not isinstance(item.get("ref_id"), str):
                        raise ForestError("MEMORY_VECTOR_INDEX_RESPONSE_INVALID", "Candidate is invalid", 502)
                    score = item.get("score")
                    if type(score) not in {int, float} or not math.isfinite(score):
                        raise ForestError("MEMORY_VECTOR_INDEX_RESPONSE_INVALID", "Candidate score is invalid", 502)
                    if float(score) < min_similarity:
                        continue
                    if item["ref_id"] not in external_scores:
                        ordered_refs.append(item["ref_id"])
                        external_scores[item["ref_id"]] = float(score)
                rows_by_ref = {}
                if ordered_refs:
                    valid_rows = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT f.facet_id,f.index_ref_id,f.source_unit_id,f.source_revision,"
                                "f.modality FROM {} f JOIN {} u ON u.bot_id=f.bot_id "
                                "AND u.source_unit_id=f.source_unit_id WHERE f.bot_id=%s "
                                "AND f.index_ref_id=ANY(%s) AND f.purpose='DOCUMENT' "
                                "AND f.state='READY' AND f.space_id=%s AND f.published_at<=%s "
                                "AND u.eligibility_state='ELIGIBLE' "
                                "AND NOT (f.source_unit_id=ANY(%s))"
                            ).format(self._table("embedding_facets"), self._table("source_units")),
                            (
                                self.store.bot_id,
                                ordered_refs,
                                domain["space_id"],
                                snapshot_at,
                                query_source_ids,
                            ),
                        )
                    ).fetchall()
                    rows_by_ref = {row["index_ref_id"]: dict(row) for row in valid_rows}
                rows = []
                for ref_id in ordered_refs:
                    row = rows_by_ref.get(ref_id)
                    if row is not None:
                        row["score"] = external_scores[ref_id]
                        rows.append(row)
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
            seed_ids = sorted(scores, key=lambda item: (-scores[item], item))[:seed_limit]
            activations, graph_edges = await self._expand_graph_in(
                conn,
                seeds=tuple(seed_ids),
                sampling_seed=sampling_seed,
                min_graph_relevance=float(min_graph_relevance),
                max_graph_nodes=max_graph_nodes,
                max_association_edges_per_node=max_association_edges_per_node,
                sequence_weight=float(sequence_weight),
                association_weight=float(association_weight),
            )
            activated_ids = [item["source_unit_id"] for item in activations]
            selected_ids = list(
                dict.fromkeys((*seed_ids, *(item for item in activated_ids if item not in seed_ids)))
            )
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
                    for match in matches.get(source_id, ())
                )
                for source_id in selected_ids
            ):
                raise ForestError(
                    "MEMORY_EVIDENCE_REVOKED", "A retrieval candidate became unavailable", 409
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,bundle_id,query_id,operation_id,input_digest,"
                    "index_snapshot_at,accepted_deletion_epoch,result_limit,seed_limit,"
                    "candidate_limit_per_facet,min_similarity,min_graph_relevance,"
                    "sequence_weight,association_weight,max_association_edges_per_node,"
                    "sampling_seed,seed_count,result_count) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("retrieval_bundles")),
                (
                    self.store.bot_id,
                    bundle_id,
                    query_id,
                    operation_id,
                    input_digest,
                    snapshot_at,
                    deletion_epoch,
                    max_graph_nodes,
                    seed_limit,
                    candidate_limit_per_facet,
                    min_similarity,
                    min_graph_relevance,
                    sequence_weight,
                    association_weight,
                    max_association_edges_per_node,
                    sampling_seed,
                    len(seed_ids),
                    len(selected_ids),
                ),
            )
            activation_by_id = {item["source_unit_id"]: item for item in activations}
            for item_index, source_unit_id in enumerate(selected_ids):
                rank_micros = round(scores.get(source_unit_id, 0.0) * 1_000_000_000_000)
                activation = activation_by_id[source_unit_id]
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,bundle_id,item_index,source_unit_id,"
                        "source_revision,rank_micros,activation_micros,is_seed,"
                        "path_parent_source_unit_id,path_edge_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("retrieval_items")),
                    (
                        self.store.bot_id,
                        bundle_id,
                        item_index,
                        source_unit_id,
                        selected[source_unit_id]["source_revision"],
                        rank_micros,
                        round(activation["activation"] * 1_000_000),
                        source_unit_id in seed_ids,
                        activation["path_parent_source_unit_id"],
                        activation["path_edge_id"],
                    ),
                )
                ordered_matches = sorted(
                    matches.get(source_unit_id, ()),
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
            logical_edges = {}
            for edge in graph_edges:
                left, right = sorted((edge["source_unit_id"], edge["target_unit_id"]))
                key = (edge["edge_id"], left, right)
                logical_edges[key] = {
                    **edge,
                    "source_unit_id": left,
                    "target_unit_id": right,
                }
            for relation_index, edge in enumerate(
                sorted(
                    logical_edges.values(),
                    key=lambda item: (
                        item["relation_kind"],
                        item["edge_id"],
                        item["source_unit_id"],
                        item["target_unit_id"],
                    ),
                )
            ):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,bundle_id,relation_index,edge_id,relation_kind,"
                        "source_unit_id,target_unit_id,weight_micros,contribution_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("retrieval_relations")),
                    (
                        self.store.bot_id,
                        bundle_id,
                        relation_index,
                        edge["edge_id"],
                        edge["relation_kind"],
                        edge["source_unit_id"],
                        edge["target_unit_id"],
                        round(edge["weight"] * 1_000_000),
                        edge["contribution_id"],
                    ),
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,index_ref_id,space_id,source_unit_id,deletion_epoch) "
                    "SELECT bot_id,index_ref_id,space_id,source_unit_id,%s FROM {} "
                    "WHERE bot_id=%s AND query_id=%s AND purpose='QUERY' "
                    "AND state='READY' AND index_ref_id IS NOT NULL ON CONFLICT DO NOTHING"
                ).format(
                    self._table("vector_index_deletions"),
                    self._table("embedding_facets"),
                ),
                (deletion_epoch, self.store.bot_id, query_id),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='SUPPRESSED',current_attempt_id=NULL,index_ref_id=NULL,"
                    "vector_digest=NULL,result_payload_id=NULL,result_payload_digest=NULL,"
                    "published_at=NULL,updated_at=clock_timestamp() WHERE bot_id=%s "
                    "AND query_id=%s AND purpose='QUERY' AND state='READY'"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, query_id),
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

    async def read_exact_sources(
        self,
        source_unit_ids,
        *,
        expected_deletion_epoch: int,
    ) -> dict:
        """Read archived source units by their stable IDs without ANN approximation."""

        ids = tuple(source_unit_ids)
        if not 1 <= len(ids) <= 8 or len(set(ids)) != len(ids):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid exact Memory source set", 422)
        for source_unit_id in ids:
            _bounded(source_unit_id, "source_unit_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            deletion_epoch = await self._epoch(conn, expected=expected_deletion_epoch)
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT u.*,s.source_kind,s.platform,s.realm_id,s.external_group_id "
                        "FROM {} u JOIN {} s ON s.bot_id=u.bot_id AND s.source_id=u.source_id "
                        "WHERE u.bot_id=%s AND u.source_unit_id=ANY(%s) FOR SHARE OF u"
                    ).format(self._table("source_units"), self._table("memory_sources")),
                    (self.store.bot_id, list(ids)),
                )
            ).fetchall()
            by_id = {row["source_unit_id"]: row for row in rows}
            if set(by_id) != set(ids):
                raise ForestError("MEMORY_SOURCE_NOT_FOUND", "Exact Memory source is missing", 404)
            if any(by_id[source_id]["eligibility_state"] != "ELIGIBLE" for source_id in ids):
                raise ForestError(
                    "MEMORY_EVIDENCE_UNAVAILABLE",
                    "Exact Memory source is not archived or was removed",
                    409,
                )
            evidence = []
            for source_id in ids:
                row = by_id[source_id]
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
                        (self.store.bot_id, source_id),
                    )
                ).fetchall()
                part_values = []
                for part in parts:
                    await self._verify_payload(conn, self._part_ref(part))
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
                evidence.append(
                    {
                        "source_unit_id": source_id,
                        "source_revision": row["source_revision"],
                        "source_id": row["source_id"],
                        "source_kind": row["source_kind"],
                        "trunk_id": row["trunk_id"],
                        "platform": row["platform"],
                        "realm_id": row["realm_id"],
                        "external_group_id": row["external_group_id"],
                        "author_subject_ref": row["author_subject_ref"],
                        "occurred_at": row["occurred_at"],
                        "unit_kind": row["unit_kind"],
                        "modality": row["modality"],
                        "parts": part_values,
                    }
                )
            return {
                "protocol": "forest.memory-exact-region/1",
                "accepted_deletion_epoch": deletion_epoch,
                "evidence": evidence,
            }

    async def confirm_bundle_projection(
        self,
        *,
        bundle_id: str,
        source_unit_ids,
        expected_deletion_epoch: int,
    ) -> dict:
        """Freeze the exact whole-unit region delivered to the model projector."""

        _bounded(bundle_id, "bundle_id", 256)
        ids = tuple(source_unit_ids)
        if len(ids) > 256 or len(set(ids)) != len(ids):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid projected evidence set", 422)
        for source_unit_id in ids:
            _bounded(source_unit_id, "source_unit_id", 256)
        projection_digest = _digest(list(ids))
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._epoch(conn, expected=expected_deletion_epoch)
            bundle = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND bundle_id=%s FOR UPDATE").format(
                        self._table("retrieval_bundles")
                    ),
                    (self.store.bot_id, bundle_id),
                )
            ).fetchone()
            if bundle is None:
                raise ForestError("MEMORY_BUNDLE_NOT_FOUND", "Evidence bundle does not exist", 404)
            projection = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND bundle_id=%s"
                    ).format(self._table("retrieval_projections")),
                    (self.store.bot_id, bundle_id),
                )
            ).fetchone()
            if projection is not None:
                if (
                    projection["projection_digest"] != projection_digest
                    or projection["projected_count"] != len(ids)
                ):
                    raise ForestError(
                        "MEMORY_BUNDLE_PROJECTION_CONFLICT",
                        "Evidence projection differs from its frozen region",
                        409,
                    )
                return {"bundle_id": bundle_id, "source_unit_ids": list(ids), "duplicate": True}
            available = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT source_unit_id FROM {} WHERE bot_id=%s AND bundle_id=%s "
                        "ORDER BY item_index"
                    ).format(self._table("retrieval_items")),
                    (self.store.bot_id, bundle_id),
                )
            ).fetchall()
            ordered = [row["source_unit_id"] for row in available]
            if ids != tuple(ordered[: len(ids)]):
                raise ForestError(
                    "MEMORY_BUNDLE_PROJECTION_CONFLICT",
                    "Projected evidence is not a whole-unit prefix",
                    409,
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,bundle_id,projection_digest,projected_count) "
                    "VALUES (%s,%s,%s,%s)"
                ).format(self._table("retrieval_projections")),
                (self.store.bot_id, bundle_id, projection_digest, len(ids)),
            )
            return {"bundle_id": bundle_id, "source_unit_ids": list(ids), "duplicate": False}

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
                    "graph_relevance": row["activation_micros"] / 1_000_000,
                    "is_direct_seed": row["is_seed"],
                    "path_parent_source_unit_id": row["path_parent_source_unit_id"],
                    "path_edge_id": row["path_edge_id"],
                    "parts": part_values,
                    "matches": [dict(match) for match in match_rows],
                }
            )
        relation_rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT edge_id,relation_kind,source_unit_id,target_unit_id,"
                    "weight_micros,contribution_id FROM {} WHERE bot_id=%s AND bundle_id=%s "
                    "ORDER BY relation_index"
                ).format(self._table("retrieval_relations")),
                (self.store.bot_id, bundle_id),
            )
        ).fetchall()
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
            "direct_seed_ids": [
                item["source_unit_id"] for item in evidence if item["is_direct_seed"]
            ],
            "relations": [
                {
                    **dict(row),
                    "weight": row["weight_micros"] / 1_000_000,
                }
                for row in relation_rows
            ],
            "completeness": {
                "query_modalities": sorted({facet["modality"] for facet in query_facets}),
                "query_facets_ready": sum(facet["state"] == "READY" for facet in query_facets),
                "query_facet_count": len(query_facets),
                "result_count": len(evidence),
                "seed_count": bundle["seed_count"],
                "seed_limit": bundle["seed_limit"],
                "candidate_limit_per_facet": bundle["candidate_limit_per_facet"],
                "min_similarity": bundle["min_similarity"],
                "min_graph_relevance": bundle["min_graph_relevance"],
                "sequence_weight": bundle["sequence_weight"],
                "association_weight": bundle["association_weight"],
                "max_graph_nodes": bundle["result_limit"],
                "max_association_edges_per_node": bundle[
                    "max_association_edges_per_node"
                ],
                "association_sampling": "query_seeded_sha256",
                "exact_projection": True,
                "ann_index": True,
            },
        }

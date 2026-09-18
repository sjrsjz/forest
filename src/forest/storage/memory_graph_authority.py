"""Clean multi-trunk, multimodal Memory fact authority."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_LOCK_DOMAIN = b"forest.memory.graph/1\0"
_SOURCE_KINDS = frozenset({"GROUP", "DIRECT", "DOCUMENT", "SYSTEM", "UNKNOWN"})
_ENDPOINT_KINDS = frozenset({"SOURCE_UNIT", "ASSERTION", "ENTITY", "ARTIFACT"})
_SYMMETRIC_RELATIONS = frozenset({"ASSOCIATES_WITH", "CONTRADICTS"})


def _bounded(value, field: str, limit: int, *, optional: bool = False, empty: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or (not empty and len(value.encode("utf-8")) < 1)
        or len(value.encode("utf-8")) > limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("MEMORY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str):
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("MEMORY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _timestamp(value, field: str):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ForestError("MEMORY_INPUT_INVALID", f"{field} needs a timezone", 422)
    return value.astimezone(timezone.utc)


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


def _lock_key(bot_id: str, identity: str) -> int:
    digest = hashlib.sha256(
        _LOCK_DOMAIN + bot_id.encode() + b"\0" + identity.encode()
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def _call(port, *args, **values):
    result = port(*args, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class MemoryPayloadRef:
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
        return vars(self)


@dataclass(frozen=True)
class MemorySourceIdentity:
    source_id: str
    source_kind: str
    identity_incarnation: int = 1
    platform: str | None = None
    realm_id: str | None = None
    external_group_id: str | None = None
    trunk_id: str | None = None

    def validate(self) -> None:
        _bounded(self.source_id, "source_id", 256)
        if self.source_kind not in _SOURCE_KINDS:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid source kind", 422)
        if type(self.identity_incarnation) is not int or self.identity_incarnation <= 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid identity incarnation", 422)
        _bounded(self.platform, "platform", 64, optional=True)
        _bounded(self.realm_id, "realm_id", 256, optional=True)
        _bounded(self.external_group_id, "external_group_id", 256, optional=True)
        _bounded(self.trunk_id, "trunk_id", 256, optional=True)
        group_fields = (self.platform, self.realm_id, self.external_group_id, self.trunk_id)
        if self.source_kind == "GROUP" and not all(value is not None for value in group_fields):
            raise ForestError(
                "MEMORY_INPUT_INVALID", "Group source identity is incomplete", 422
            )
        if self.source_kind != "GROUP" and (
            self.external_group_id is not None or self.trunk_id is not None
        ):
            raise ForestError(
                "MEMORY_INPUT_INVALID", "Non-group source has group identity fields", 422
            )

    def canonical(self) -> dict:
        return vars(self)

    @property
    def identity_digest(self) -> str:
        return _digest(self.canonical())


@dataclass(frozen=True)
class MemoryPart:
    part_id: str
    part_index: int
    part_kind: str
    media_type: str
    payload: MemoryPayloadRef
    width: int | None = None
    height: int | None = None
    byte_length: int | None = None

    def validate(self) -> None:
        _bounded(self.part_id, "part_id", 256)
        if type(self.part_index) is not int or not 0 <= self.part_index <= 4095:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid Memory part index", 422)
        if self.part_kind not in {"TEXT", "IMAGE"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid Memory part kind", 422)
        _bounded(self.media_type, "media_type", 128)
        if self.part_kind == "IMAGE" and not self.media_type.lower().startswith("image/"):
            raise ForestError("MEMORY_INPUT_INVALID", "Image part media type is invalid", 422)
        self.payload.validate("part.payload")
        if self.part_kind == "TEXT":
            if self.width is not None or self.height is not None:
                raise ForestError("MEMORY_INPUT_INVALID", "Text part has image dimensions", 422)
        elif (
            type(self.width) is not int
            or type(self.height) is not int
            or not 1 <= self.width <= 100000
            or not 1 <= self.height <= 100000
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Image dimensions are invalid", 422)
        if type(self.byte_length) is not int or not 0 <= self.byte_length <= 32 * 1024 * 1024:
            raise ForestError("MEMORY_INPUT_INVALID", "Memory part byte length is invalid", 422)

    def canonical(self) -> dict:
        return {
            "part_id": self.part_id,
            "part_index": self.part_index,
            "part_kind": self.part_kind,
            "media_type": self.media_type,
            "payload": self.payload.canonical(),
            "width": self.width,
            "height": self.height,
            "byte_length": self.byte_length,
        }


@dataclass(frozen=True)
class MemoryOccurrence:
    occurrence_id: str
    binding_ref: str
    conversation_ref: str
    event_ref: str
    ingress_seq: int | None
    observed_at: datetime

    def validate(self) -> None:
        for value, field, limit in (
            (self.occurrence_id, "occurrence_id", 256),
            (self.binding_ref, "occurrence.binding_ref", 256),
            (self.conversation_ref, "occurrence.conversation_ref", 256),
            (self.event_ref, "occurrence.event_ref", 256),
        ):
            _bounded(value, field, limit)
        if self.ingress_seq is not None and (
            type(self.ingress_seq) is not int or self.ingress_seq <= 0
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid occurrence ingress sequence", 422)
        _timestamp(self.observed_at, "occurrence.observed_at")

    def canonical(self) -> dict:
        return {
            **vars(self),
            "observed_at": self.observed_at.astimezone(timezone.utc).isoformat(),
        }


@dataclass(frozen=True, order=True)
class MemoryEvidenceRef:
    source_unit_id: str
    source_revision: int
    part_id: str

    def validate(self) -> None:
        _bounded(self.source_unit_id, "evidence.source_unit_id", 256)
        if type(self.source_revision) is not int or self.source_revision <= 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid evidence source revision", 422)
        _bounded(self.part_id, "evidence.part_id", 256)


class MemoryGraphRepository:
    """Own Memory source, trunk and explicit relation facts without payload bodies."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier=None,
        deletion_epoch_guard=None,
        endpoint_verifier=None,
        input_batch_verifier=None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.memory.graph")
        self.payload_verifier = payload_verifier
        self.deletion_epoch_guard = deletion_epoch_guard
        self.endpoint_verifier = endpoint_verifier
        self.input_batch_verifier = input_batch_verifier

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

    async def _epoch(self, conn, *, expected=None, update=False) -> int:
        if self.deletion_epoch_guard is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "Memory deletion epoch port is unavailable", 503
            )
        return await _call(
            self.deletion_epoch_guard,
            conn,
            expected=expected,
            update=update,
        )

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

    async def _verify_endpoint(self, conn, endpoint_kind: str, endpoint_id: str) -> dict:
        if endpoint_kind == "SOURCE_UNIT":
            row = await (
                await conn.execute(
                    sql.SQL("SELECT source_unit_id,source_revision,eligibility_state FROM {} "
                            "WHERE bot_id=%s AND source_unit_id=%s FOR SHARE").format(
                        self._table("source_units")
                    ),
                    (self.store.bot_id, endpoint_id),
                )
            ).fetchone()
            if row is None or row["eligibility_state"] != "ELIGIBLE":
                raise ForestError(
                    "MEMORY_ENDPOINT_UNAVAILABLE", "Relation endpoint is unavailable", 409
                )
            return dict(row)
        if self.endpoint_verifier is None:
            raise ForestError(
                "MEMORY_ENDPOINT_KIND_UNAVAILABLE",
                "This graph node kind has no registered authority",
                409,
            )
        result = await _call(
            self.endpoint_verifier,
            conn,
            endpoint_kind=endpoint_kind,
            endpoint_id=endpoint_id,
        )
        if not result:
            raise ForestError(
                "MEMORY_ENDPOINT_UNAVAILABLE", "Relation endpoint is unavailable", 409
            )
        return dict(result)

    async def initialize_domain(
        self,
        *,
        operation_id: str,
        space_id: str,
        embedding_model_kind: str,
        model_revision_digest: str,
        dimensions: int,
        normalization: str,
        fusion_policy_revision: str,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (space_id, "space_id", 256),
            (embedding_model_kind, "embedding_model_kind", 256),
            (fusion_policy_revision, "fusion_policy_revision", 256),
        ):
            _bounded(value, field, limit)
        _validated_digest(model_revision_digest, "model_revision_digest")
        if type(dimensions) is not int or not 1 <= dimensions <= 4096:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding dimensions", 422)
        if normalization not in {"L2", "NONE"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid vector normalization", 422)
        input_digest = _digest(
            {
                "space_id": space_id,
                "embedding_model_kind": embedding_model_kind,
                "model_revision_digest": model_revision_digest,
                "dimensions": dimensions,
                "normalization": normalization,
                "fusion_policy_revision": fusion_policy_revision,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "domain")
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(
                        self._table("memory_domains")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["initialization_operation_id"] == operation_id
                    and existing["input_digest"] == input_digest
                ):
                    return dict(existing)
                raise ForestError(
                    "MEMORY_SPACE_IMMUTABLE",
                    "Embedding model kind, revision and dimensions are bound to the Bot",
                    409,
                )
            return dict(
                await (
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,initialization_operation_id,input_digest,space_id,"
                            "embedding_model_kind,model_revision_digest,dimensions,normalization,"
                            "fusion_policy_revision) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                        ).format(self._table("memory_domains")),
                        (
                            self.store.bot_id,
                            operation_id,
                            input_digest,
                            space_id,
                            embedding_model_kind,
                            model_revision_digest,
                            dimensions,
                            normalization,
                            fusion_policy_revision,
                        ),
                    )
                ).fetchone()
            )

    async def register_source(
        self,
        *,
        operation_id: str,
        identity: MemorySourceIdentity,
        _connection=None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        if type(identity) is not MemorySourceIdentity:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid Memory source identity", 422)
        identity.validate()
        input_digest = _digest(identity.canonical())
        async with self._transaction(_connection) as conn:
            domain = await (
                await conn.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE bot_id=%s").format(
                        self._table("memory_domains")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if domain is None:
                raise ForestError("MEMORY_DOMAIN_MISSING", "Memory domain is not initialized", 409)
            await self._lock(conn, "source-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT o.*,s.* FROM {} o JOIN {} s ON s.bot_id=o.bot_id "
                            "AND s.source_id=o.source_id WHERE o.bot_id=%s AND o.operation_id=%s").format(
                        self._table("source_registration_operations"),
                        self._table("memory_sources"),
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("MEMORY_SOURCE_CONFLICT", "Source operation differs", 409)
                return dict(replay)
            lock_identities = {
                "source-id:" + identity.source_id,
                "source-identity:" + identity.identity_digest,
            }
            if identity.source_kind == "GROUP":
                lock_identities.add(
                    "group-identity:"
                    + _digest(
                        [
                            identity.platform,
                            identity.realm_id,
                            identity.external_group_id,
                            identity.identity_incarnation,
                        ]
                    )
                )
            for lock_identity in sorted(lock_identities):
                await self._lock(conn, lock_identity)
            occupied = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND "
                        "(source_id=%s OR identity_digest=%s OR "
                        "(%s='GROUP' AND source_kind='GROUP' AND platform=%s AND realm_id=%s "
                        "AND external_group_id=%s AND identity_incarnation=%s))"
                    ).format(self._table("memory_sources")),
                    (
                        self.store.bot_id,
                        identity.source_id,
                        identity.identity_digest,
                        identity.source_kind,
                        identity.platform,
                        identity.realm_id,
                        identity.external_group_id,
                        identity.identity_incarnation,
                    ),
                )
            ).fetchall()
            if occupied:
                if len(occupied) != 1 or occupied[0]["input_digest"] != input_digest:
                    raise ForestError("MEMORY_SOURCE_CONFLICT", "Source identity differs", 409)
                row = occupied[0]
                duplicate_source = True
            else:
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,source_id,registration_operation_id,input_digest,"
                            "source_kind,platform,realm_id,external_group_id,identity_incarnation,"
                            "identity_digest,trunk_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                            "RETURNING *"
                        ).format(self._table("memory_sources")),
                        (
                            self.store.bot_id,
                            identity.source_id,
                            operation_id,
                            input_digest,
                            identity.source_kind,
                            identity.platform,
                            identity.realm_id,
                            identity.external_group_id,
                            identity.identity_incarnation,
                            identity.identity_digest,
                            identity.trunk_id,
                        ),
                    )
                ).fetchone()
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,source_id) VALUES (%s,%s)").format(
                        self._table("source_clocks")
                    ),
                    (self.store.bot_id, identity.source_id),
                )
                if identity.source_kind == "GROUP":
                    await conn.execute(
                        sql.SQL("INSERT INTO {}(bot_id,trunk_id,source_id) VALUES (%s,%s,%s)").format(
                            self._table("memory_trunks")
                        ),
                        (self.store.bot_id, identity.trunk_id, identity.source_id),
                    )
                duplicate_source = False
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,source_id,duplicate_source) "
                        "VALUES (%s,%s,%s,%s,%s)").format(
                    self._table("source_registration_operations")
                ),
                (
                    self.store.bot_id,
                    operation_id,
                    input_digest,
                    identity.source_id,
                    duplicate_source,
                ),
            )
            return {**dict(row), "duplicate_source": duplicate_source}

    async def register_source_in(self, conn, **values) -> dict:
        return await self.register_source(_connection=conn, **values)

    @staticmethod
    def _modality(parts: tuple[MemoryPart, ...], unit_kind: str) -> str:
        kinds = {part.part_kind for part in parts}
        if unit_kind == "CONTROL_DATA":
            return "CONTROL"
        if kinds == {"TEXT"}:
            return "TEXT"
        if kinds == {"IMAGE"}:
            return "IMAGE"
        return "MIXED"

    async def capture_source_unit(
        self,
        *,
        operation_id: str,
        source_unit_id: str,
        source_id: str,
        source_revision: int,
        unit_kind: str,
        author_subject_ref: str,
        author: MemoryPayloadRef,
        parts,
        occurrence: MemoryOccurrence,
        occurred_at: datetime,
        expected_deletion_epoch: int,
        _connection=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (source_unit_id, "source_unit_id", 256),
            (source_id, "source_id", 256),
            (unit_kind, "unit_kind", 128),
            (author_subject_ref, "author_subject_ref", 256),
        ):
            _bounded(value, field, limit)
        if type(source_revision) is not int or source_revision <= 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid source revision", 422)
        if type(expected_deletion_epoch) is not int or expected_deletion_epoch < 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid deletion epoch", 422)
        author.validate("author")
        if type(occurrence) is not MemoryOccurrence:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid source occurrence", 422)
        occurrence.validate()
        occurred_at = _timestamp(occurred_at, "occurred_at")
        items = tuple(parts)
        if not 1 <= len(items) <= 4096 or any(type(item) is not MemoryPart for item in items):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid Memory part set", 422)
        for item in items:
            item.validate()
        if sorted(item.part_index for item in items) != list(range(len(items))):
            raise ForestError("MEMORY_INPUT_INVALID", "Memory part order has a gap", 422)
        if len({item.part_id for item in items}) != len(items):
            raise ForestError("MEMORY_INPUT_INVALID", "Memory part identity repeats", 422)
        ordered = tuple(sorted(items, key=lambda item: item.part_index))
        manifest_digest = _digest([item.canonical() for item in ordered])
        modality = self._modality(ordered, unit_kind)
        unit_identity = {
            "source_unit_id": source_unit_id,
            "source_id": source_id,
            "source_revision": source_revision,
            "unit_kind": unit_kind,
            "author_subject_ref": author_subject_ref,
            "author": author.canonical(),
            "parts": [item.canonical() for item in ordered],
            "occurred_at": occurred_at.isoformat(),
        }
        unit_digest = _digest(unit_identity)
        input_digest = _digest(
            {
                "unit": unit_identity,
                "occurrence": occurrence.canonical(),
                "expected_deletion_epoch": expected_deletion_epoch,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "capture-operation:" + operation_id)
            await self._lock(
                conn,
                "event-eligibility:"
                + _digest([occurrence.conversation_ref, occurrence.event_ref]),
            )
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT c.*,u.source_order_seq,u.trunk_id,u.modality,u.eligibility_state "
                            "FROM {} c JOIN {} u ON u.bot_id=c.bot_id "
                            "AND u.source_unit_id=c.source_unit_id WHERE c.bot_id=%s "
                            "AND c.operation_id=%s").format(
                        self._table("capture_operations"), self._table("source_units")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("MEMORY_CAPTURE_CONFLICT", "Capture operation differs", 409)
                await self._resolve_pending_input_in(
                    conn,
                    source_unit_id=source_unit_id,
                    occurrence_id=occurrence.occurrence_id,
                    conversation_ref=occurrence.conversation_ref,
                    event_ref=occurrence.event_ref,
                )
                refreshed = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT source_order_seq,trunk_id,modality,eligibility_state "
                            "FROM {} WHERE bot_id=%s AND source_unit_id=%s"
                        ).format(self._table("source_units")),
                        (self.store.bot_id, source_unit_id),
                    )
                ).fetchone()
                return {**dict(replay), **dict(refreshed)}
            await self._epoch(conn, expected=expected_deletion_epoch)
            admission_locks = {
                "source-unit:" + source_unit_id,
                "occurrence:" + occurrence.occurrence_id,
                "platform-observation:"
                + _digest([occurrence.binding_ref, occurrence.event_ref]),
            }
            for lock_identity in sorted(admission_locks):
                await self._lock(conn, lock_identity)
            source = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_id=%s FOR SHARE").format(
                        self._table("memory_sources")
                    ),
                    (self.store.bot_id, source_id),
                )
            ).fetchone()
            if source is None or source["status"] != "ACTIVE":
                raise ForestError("MEMORY_SOURCE_NOT_FOUND", "Memory source is unavailable", 404)
            blocked = await (
                await conn.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE bot_id=%s AND source_unit_id=%s").format(
                        self._table("source_suppressions")
                    ),
                    (self.store.bot_id, source_unit_id),
                )
            ).fetchone()
            if blocked is not None:
                raise ForestError("MEMORY_SOURCE_SUPPRESSED", "Memory source unit was removed", 409)
            await self._verify_payload(conn, author)
            for item in ordered:
                verified = await self._verify_payload(conn, item.payload)
                if verified.media_type != item.media_type or verified.byte_length != item.byte_length:
                    raise ForestError(
                        "MEMORY_PAYLOAD_METADATA_CONFLICT",
                        "Memory part metadata differs from the payload authority",
                        409,
                    )
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=%s FOR UPDATE").format(
                        self._table("source_units")
                    ),
                    (self.store.bot_id, source_unit_id),
                )
            ).fetchone()
            duplicate_source = occupied is not None
            if occupied is not None and occupied["input_digest"] != unit_digest:
                raise ForestError("MEMORY_SOURCE_UNIT_CONFLICT", "Source unit identity differs", 409)
            if occupied is None:
                clock = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_id=%s FOR UPDATE").format(
                            self._table("source_clocks")
                        ),
                        (self.store.bot_id, source_id),
                    )
                ).fetchone()
                order_seq = clock["next_source_order_seq"]
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,source_unit_id,capture_operation_id,input_digest,"
                            "source_id,trunk_id,source_order_seq,source_revision,unit_kind,modality,"
                            "part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
                            "author_payload_digest,author_owner_component_id,author_owner_kind,"
                            "author_owner_id,accepted_deletion_epoch,occurred_at) VALUES "
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(
                        self._table("source_units")
                    ),
                    (
                        self.store.bot_id,
                        source_unit_id,
                        operation_id,
                        unit_digest,
                        source_id,
                        source["trunk_id"],
                        order_seq,
                        source_revision,
                        unit_kind,
                        modality,
                        len(ordered),
                        manifest_digest,
                        author_subject_ref,
                        author.payload_id,
                        author.digest,
                        author.owner_component_id,
                        author.owner_kind,
                        author.owner_id,
                        expected_deletion_epoch,
                        occurred_at,
                    ),
                )
                for item in ordered:
                    await conn.execute(
                        sql.SQL("INSERT INTO {}(bot_id,source_unit_id,part_id,part_index,part_kind,"
                                "media_type,payload_id,payload_digest,payload_owner_component_id,"
                                "payload_owner_kind,payload_owner_id,width,height,byte_length) VALUES "
                                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(
                            self._table("source_parts")
                        ),
                        (
                            self.store.bot_id,
                            source_unit_id,
                            item.part_id,
                            item.part_index,
                            item.part_kind,
                            item.media_type,
                            item.payload.payload_id,
                            item.payload.digest,
                            item.payload.owner_component_id,
                            item.payload.owner_kind,
                            item.payload.owner_id,
                            item.width,
                            item.height,
                            item.byte_length,
                        ),
                    )
                await conn.execute(
                    sql.SQL("UPDATE {} SET next_source_order_seq=next_source_order_seq+1,"
                            "updated_at=clock_timestamp() WHERE bot_id=%s AND source_id=%s").format(
                        self._table("source_clocks")
                    ),
                    (self.store.bot_id, source_id),
                )
            occurrence_row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND occurrence_id=%s").format(
                        self._table("source_occurrences")
                    ),
                    (self.store.bot_id, occurrence.occurrence_id),
                )
            ).fetchone()
            expected_occurrence = (
                source_unit_id,
                occurrence.binding_ref,
                occurrence.conversation_ref,
                occurrence.event_ref,
                occurrence.ingress_seq,
                occurrence.observed_at,
            )
            if occurrence_row is None:
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,occurrence_id,source_unit_id,binding_ref,"
                            "conversation_ref,event_ref,ingress_seq,observed_at) VALUES "
                            "(%s,%s,%s,%s,%s,%s,%s,%s)").format(
                        self._table("source_occurrences")
                    ),
                    (self.store.bot_id, occurrence.occurrence_id, *expected_occurrence),
                )
            elif tuple(
                occurrence_row[key]
                for key in (
                    "source_unit_id",
                    "binding_ref",
                    "conversation_ref",
                    "event_ref",
                    "ingress_seq",
                    "observed_at",
                )
            ) != expected_occurrence:
                raise ForestError("MEMORY_OCCURRENCE_CONFLICT", "Occurrence identity differs", 409)
            capture = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,source_unit_id,"
                            "occurrence_id,duplicate_source) VALUES (%s,%s,%s,%s,%s,%s) RETURNING *").format(
                        self._table("capture_operations")
                    ),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        source_unit_id,
                        occurrence.occurrence_id,
                        duplicate_source,
                    ),
                )
            ).fetchone()
            await self._resolve_pending_input_in(
                conn,
                source_unit_id=source_unit_id,
                occurrence_id=occurrence.occurrence_id,
                conversation_ref=occurrence.conversation_ref,
                event_ref=occurrence.event_ref,
            )
            unit = await (
                await conn.execute(
                    sql.SQL("SELECT source_order_seq,trunk_id,modality,eligibility_state "
                            "FROM {} WHERE bot_id=%s AND source_unit_id=%s").format(
                        self._table("source_units")
                    ),
                    (self.store.bot_id, source_unit_id),
                )
            ).fetchone()
            return {**dict(capture), **dict(unit)}

    async def capture_source_unit_in(self, conn, **values) -> dict:
        return await self.capture_source_unit(_connection=conn, **values)

    async def qualify_occurrence(
        self,
        *,
        operation_id: str,
        source_unit_id: str,
        occurrence_id: str,
        input_batch_ref: str,
        context_revision: int,
        expected_deletion_epoch: int,
        _connection=None,
        _input_batch_verifier=None,
        _skip_input_batch_verification: bool = False,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (source_unit_id, "source_unit_id", 256),
            (occurrence_id, "occurrence_id", 256),
            (input_batch_ref, "input_batch_ref", 256),
        ):
            _bounded(value, field, limit)
        if type(context_revision) is not int or context_revision <= 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid Context revision", 422)
        input_digest = _digest(
            [source_unit_id, occurrence_id, input_batch_ref, context_revision, expected_deletion_epoch]
        )
        if type(expected_deletion_epoch) is not int or expected_deletion_epoch < 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid deletion epoch", 422)
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "eligibility-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("eligibility_operations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("MEMORY_ELIGIBILITY_CONFLICT", "Eligibility input differs", 409)
                return dict(replay)
            await self._epoch(conn, expected=expected_deletion_epoch)
            unit = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=%s FOR UPDATE").format(
                        self._table("source_units")
                    ),
                    (self.store.bot_id, source_unit_id),
                )
            ).fetchone()
            occurrence = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND occurrence_id=%s FOR UPDATE").format(
                        self._table("source_occurrences")
                    ),
                    (self.store.bot_id, occurrence_id),
                )
            ).fetchone()
            if unit is None or occurrence is None or occurrence["source_unit_id"] != source_unit_id:
                raise ForestError("MEMORY_SOURCE_NOT_FOUND", "Memory occurrence is unavailable", 404)
            if unit["eligibility_state"] == "SUPPRESSED" or occurrence["eligibility_state"] == "SUPPRESSED":
                raise ForestError("MEMORY_SOURCE_SUPPRESSED", "Memory source unit was removed", 409)
            verifier = _input_batch_verifier or self.input_batch_verifier
            if verifier is None and not _skip_input_batch_verification:
                raise ForestError(
                    "MEMORY_INPUT_AUTHORITY_MISSING",
                    "Conversation InputBatch verification is unavailable",
                    503,
                )
            if not _skip_input_batch_verification:
                await _call(
                    verifier,
                    conn,
                    input_batch_ref=input_batch_ref,
                    conversation_ref=occurrence["conversation_ref"],
                    event_ref=occurrence["event_ref"],
                    context_revision=context_revision,
                )
            if occurrence["eligibility_state"] == "ELIGIBLE":
                if (
                    occurrence["input_batch_ref"] != input_batch_ref
                    or occurrence["context_revision"] != context_revision
                ):
                    raise ForestError(
                        "MEMORY_ELIGIBILITY_CONFLICT", "Occurrence qualification differs", 409
                    )
            else:
                await conn.execute(
                    sql.SQL("UPDATE {} SET eligibility_state='ELIGIBLE',input_batch_ref=%s,"
                            "context_revision=%s,qualified_at=clock_timestamp() WHERE bot_id=%s "
                            "AND occurrence_id=%s").format(self._table("source_occurrences")),
                    (input_batch_ref, context_revision, self.store.bot_id, occurrence_id),
                )
            revision = unit["eligibility_revision"]
            if unit["eligibility_state"] == "REGISTERED":
                revision += 1
                await conn.execute(
                    sql.SQL("UPDATE {} SET eligibility_state='ELIGIBLE',eligibility_revision=%s,"
                            "eligible_at=clock_timestamp(),updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND source_unit_id=%s").format(
                        self._table("source_units")
                    ),
                    (revision, self.store.bot_id, source_unit_id),
                )
            return dict(
                await (
                    await conn.execute(
                        sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,source_unit_id,"
                                "occurrence_id,input_batch_ref,context_revision,"
                                "resulting_eligibility_revision) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                                "RETURNING *").format(self._table("eligibility_operations")),
                        (
                            self.store.bot_id,
                            operation_id,
                            input_digest,
                            source_unit_id,
                            occurrence_id,
                            input_batch_ref,
                            context_revision,
                            revision,
                        ),
                    )
                ).fetchone()
            )

    async def qualify_occurrence_in(self, conn, **values) -> dict:
        return await self.qualify_occurrence(_connection=conn, **values)

    async def qualify_input_batch_in(
        self,
        conn,
        *,
        operation_id: str,
        input_batch_ref: str,
        conversation_ref: str,
        event_refs,
        context_revision: int,
        expected_deletion_epoch: int,
        input_batch_verifier=None,
    ) -> tuple[dict, ...]:
        """Qualify the exact committed Conversation batch as one UoW participant."""

        _bounded(operation_id, "operation_id", 256)
        _bounded(input_batch_ref, "input_batch_ref", 256)
        _bounded(conversation_ref, "conversation_ref", 256)
        events = tuple(event_refs)
        if len(events) > 100000 or len(set(events)) != len(events):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid InputBatch event set", 422)
        for event_ref in events:
            _bounded(event_ref, "event_ref", 256)
        if not events:
            return ()
        verifier = input_batch_verifier or self.input_batch_verifier
        for event_ref in sorted(events):
            await self._lock(
                conn,
                "event-eligibility:" + _digest([conversation_ref, event_ref]),
            )
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT occurrence_id,source_unit_id,event_ref FROM {} WHERE bot_id=%s "
                    "AND conversation_ref=%s AND event_ref=ANY(%s) ORDER BY event_ref"
                ).format(self._table("source_occurrences")),
                (self.store.bot_id, conversation_ref, list(events)),
            )
        ).fetchall()
        by_event = {row["event_ref"]: row for row in rows}
        results = []
        for event_ref in events:
            row = by_event.get(event_ref)
            if row is None:
                if verifier is None:
                    raise ForestError(
                        "MEMORY_INPUT_AUTHORITY_MISSING",
                        "Conversation InputBatch verification is unavailable",
                        503,
                    )
                await _call(
                    verifier,
                    conn,
                    input_batch_ref=input_batch_ref,
                    conversation_ref=conversation_ref,
                    event_ref=event_ref,
                    context_revision=context_revision,
                )
                pending_digest = _digest(
                    [
                        operation_id,
                        input_batch_ref,
                        conversation_ref,
                        event_ref,
                        context_revision,
                        expected_deletion_epoch,
                    ]
                )
                pending = await (
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,conversation_ref,event_ref,operation_id,"
                            "input_digest,input_batch_ref,context_revision,accepted_deletion_epoch) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT "
                            "(bot_id,conversation_ref,event_ref) DO NOTHING RETURNING *"
                        ).format(self._table("pending_input_events")),
                        (
                            self.store.bot_id,
                            conversation_ref,
                            event_ref,
                            operation_id,
                            pending_digest,
                            input_batch_ref,
                            context_revision,
                            expected_deletion_epoch,
                        ),
                    )
                ).fetchone()
                if pending is None:
                    pending = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT * FROM {} WHERE bot_id=%s AND conversation_ref=%s "
                                "AND event_ref=%s"
                            ).format(self._table("pending_input_events")),
                            (self.store.bot_id, conversation_ref, event_ref),
                        )
                    ).fetchone()
                if pending["input_digest"] != pending_digest:
                    raise ForestError(
                        "MEMORY_ELIGIBILITY_CONFLICT",
                        "Pending Memory eligibility differs",
                        409,
                    )
                results.append(dict(pending))
                continue
            item_operation_id = "eligibility:" + hashlib.sha256(
                _canonical([self.store.bot_id, operation_id, row["occurrence_id"]])
            ).hexdigest()
            results.append(
                await self.qualify_occurrence_in(
                    conn,
                    operation_id=item_operation_id,
                    source_unit_id=row["source_unit_id"],
                    occurrence_id=row["occurrence_id"],
                    input_batch_ref=input_batch_ref,
                    context_revision=context_revision,
                    expected_deletion_epoch=expected_deletion_epoch,
                    _input_batch_verifier=verifier,
                )
            )
        return tuple(results)

    async def _resolve_pending_input_in(
        self,
        conn,
        *,
        source_unit_id: str,
        occurrence_id: str,
        conversation_ref: str,
        event_ref: str,
    ) -> dict | None:
        pending = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND conversation_ref=%s "
                    "AND event_ref=%s FOR UPDATE"
                ).format(self._table("pending_input_events")),
                (self.store.bot_id, conversation_ref, event_ref),
            )
        ).fetchone()
        if pending is None or pending["state"] == "RESOLVED":
            return None if pending is None else dict(pending)
        item_operation_id = "eligibility:" + hashlib.sha256(
            _canonical([self.store.bot_id, pending["operation_id"], occurrence_id])
        ).hexdigest()
        await self.qualify_occurrence_in(
            conn,
            operation_id=item_operation_id,
            source_unit_id=source_unit_id,
            occurrence_id=occurrence_id,
            input_batch_ref=pending["input_batch_ref"],
            context_revision=pending["context_revision"],
            expected_deletion_epoch=pending["accepted_deletion_epoch"],
            _skip_input_batch_verification=True,
        )
        return dict(
            await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='RESOLVED',source_unit_id=%s,occurrence_id=%s,"
                        "resolved_at=clock_timestamp() WHERE bot_id=%s AND conversation_ref=%s "
                        "AND event_ref=%s RETURNING *"
                    ).format(self._table("pending_input_events")),
                    (
                        source_unit_id,
                        occurrence_id,
                        self.store.bot_id,
                        conversation_ref,
                        event_ref,
                    ),
                )
            ).fetchone()
        )

    async def suppress_sources_in(
        self,
        conn,
        *,
        operation_id: str,
        source_unit_ids,
        deletion_epoch: int,
        reason_code: str,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(reason_code, "reason_code", 128)
        ids = tuple(sorted(set(source_unit_ids)))
        if not 1 <= len(ids) <= 100000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid suppression source set", 422)
        for source_unit_id in ids:
            _bounded(source_unit_id, "source_unit_id", 256)
        if type(deletion_epoch) is not int or deletion_epoch <= 0:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid suppression epoch", 422)
        input_digest = _digest([ids, deletion_epoch, reason_code])
        await self._lock(conn, "suppression-operation:" + operation_id)
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s ORDER BY source_unit_id").format(
                    self._table("source_suppressions")
                ),
                (self.store.bot_id, operation_id),
            )
        ).fetchall()
        if existing:
            if len(existing) != len(ids) or any(
                row["input_digest"] != input_digest or row["source_unit_id"] != ids[index]
                for index, row in enumerate(existing)
            ):
                raise ForestError("MEMORY_SUPPRESSION_CONFLICT", "Suppression input differs", 409)
            return {"source_unit_ids": list(ids), "deletion_epoch": deletion_epoch, "duplicate": True}
        await self._epoch(conn, expected=deletion_epoch)
        rows = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=ANY(%s) "
                        "ORDER BY source_unit_id FOR UPDATE").format(self._table("source_units")),
                (self.store.bot_id, list(ids)),
            )
        ).fetchall()
        if [row["source_unit_id"] for row in rows] != list(ids):
            raise ForestError("MEMORY_SOURCE_NOT_FOUND", "Suppression source is unavailable", 404)
        for row in rows:
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=%s").format(
                        self._table("source_suppressions")
                    ),
                    (self.store.bot_id, row["source_unit_id"]),
                )
            ).fetchone()
            if occupied is not None:
                raise ForestError("MEMORY_SOURCE_SUPPRESSED", "Memory source is already removed", 409)
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,source_unit_id,"
                        "deletion_epoch,reason_code) VALUES (%s,%s,%s,%s,%s,%s)").format(
                    self._table("source_suppressions")
                ),
                (
                    self.store.bot_id,
                    operation_id,
                    input_digest,
                    row["source_unit_id"],
                    deletion_epoch,
                    reason_code,
                ),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET eligibility_state='SUPPRESSED',"
                        "eligibility_revision=eligibility_revision+1,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND source_unit_id=%s").format(self._table("source_units")),
                (self.store.bot_id, row["source_unit_id"]),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET eligibility_state='SUPPRESSED',input_batch_ref=NULL,"
                        "context_revision=NULL WHERE bot_id=%s AND source_unit_id=%s").format(
                    self._table("source_occurrences")
                ),
                (self.store.bot_id, row["source_unit_id"]),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} a SET state=CASE WHEN a.state='PREPARED' THEN 'NOT_SENT' "
                    "ELSE 'UNKNOWN' END,settlement_operation_id='suppression:' || "
                    "md5(%s || ':' || a.attempt_id),error_code='SOURCE_SUPPRESSED',"
                    "settled_at=clock_timestamp() FROM {} f WHERE a.bot_id=f.bot_id "
                    "AND a.facet_id=f.facet_id AND f.bot_id=%s AND f.source_unit_id=%s "
                    "AND a.state IN ('PREPARED','DISPATCHED')"
                ).format(
                    self._table("embedding_attempts"),
                    self._table("embedding_facets"),
                ),
                (operation_id, self.store.bot_id, row["source_unit_id"]),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='SUPPRESSED',current_attempt_id=NULL,vector=NULL,"
                    "vector_digest=NULL,result_payload_id=NULL,result_payload_digest=NULL,"
                    "published_at=NULL,updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND source_unit_id=%s AND state<>'SUPPRESSED'"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, row["source_unit_id"]),
            )
        return {"source_unit_ids": list(ids), "deletion_epoch": deletion_epoch, "duplicate": False}

    async def payload_ids_for_sources_in(self, conn, source_unit_ids) -> tuple[str, ...]:
        ids = tuple(sorted(set(source_unit_ids)))
        if not 1 <= len(ids) <= 100000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid Memory source selection", 422)
        for source_unit_id in ids:
            _bounded(source_unit_id, "source_unit_id", 256)
        units = await (
            await conn.execute(
                sql.SQL("SELECT source_unit_id,author_payload_id FROM {} WHERE bot_id=%s "
                        "AND source_unit_id=ANY(%s) ORDER BY source_unit_id FOR SHARE").format(
                    self._table("source_units")
                ),
                (self.store.bot_id, list(ids)),
            )
        ).fetchall()
        if [row["source_unit_id"] for row in units] != list(ids):
            raise ForestError("MEMORY_SOURCE_NOT_FOUND", "Memory source is unavailable", 404)
        parts = await (
            await conn.execute(
                sql.SQL("SELECT payload_id FROM {} WHERE bot_id=%s AND source_unit_id=ANY(%s)").format(
                    self._table("source_parts")
                ),
                (self.store.bot_id, list(ids)),
            )
        ).fetchall()
        embedding_results = await (
            await conn.execute(
                sql.SQL(
                    "SELECT result_payload_id FROM {} WHERE bot_id=%s "
                    "AND source_unit_id=ANY(%s) AND result_payload_id IS NOT NULL"
                ).format(self._table("embedding_facets")),
                (self.store.bot_id, list(ids)),
            )
        ).fetchall()
        return tuple(
            sorted(
                {row["author_payload_id"] for row in units}
                | {row["payload_id"] for row in parts}
                | {row["result_payload_id"] for row in embedding_results}
            )
        )

    async def add_relation(
        self,
        *,
        operation_id: str,
        contribution_id: str,
        relation_kind: str,
        left_kind: str,
        left_id: str,
        right_kind: str,
        right_id: str,
        evidence,
        expected_deletion_epoch: int,
        relation_key: str = "",
        origin_kind: str = "MANUAL",
        weight_micros: int = 1_000_000,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (contribution_id, "contribution_id", 256),
            (left_id, "left_id", 256),
            (right_id, "right_id", 256),
        ):
            _bounded(value, field, limit)
        _bounded(relation_key, "relation_key", 256, empty=True)
        if relation_kind not in {"ASSOCIATES_WITH", "EVIDENCE", "CONTRADICTS", "SUBJECT_OF"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation kind", 422)
        if left_kind not in _ENDPOINT_KINDS or right_kind not in _ENDPOINT_KINDS:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation endpoint", 422)
        if (left_kind, left_id) == (right_kind, right_id):
            raise ForestError("MEMORY_INPUT_INVALID", "Relation endpoints repeat", 422)
        if origin_kind not in {"MANUAL", "VECTOR", "RULE", "MODEL"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation origin", 422)
        if type(weight_micros) is not int or not 0 <= weight_micros <= 10**12:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation weight", 422)
        refs = tuple(sorted(set(evidence)))
        if not 1 <= len(refs) <= 4096 or any(
            type(item) is not MemoryEvidenceRef for item in refs
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation evidence", 422)
        for item in refs:
            item.validate()
        if relation_kind in _SYMMETRIC_RELATIONS and (left_kind, left_id) > (right_kind, right_id):
            left_kind, right_kind = right_kind, left_kind
            left_id, right_id = right_id, left_id
        relation_id = "relation:" + hashlib.sha256(
            _canonical(
                [
                    self.store.bot_id,
                    relation_kind,
                    left_kind,
                    left_id,
                    right_kind,
                    right_id,
                    relation_key,
                ]
            )
        ).hexdigest()
        input_digest = _digest(
            {
                "contribution_id": contribution_id,
                "relation_id": relation_id,
                "relation_kind": relation_kind,
                "left": [left_kind, left_id],
                "right": [right_kind, right_id],
                "relation_key": relation_key,
                "origin_kind": origin_kind,
                "weight_micros": weight_micros,
                "evidence": [vars(item) for item in refs],
                "expected_deletion_epoch": expected_deletion_epoch,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "relation-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("relation_contributions")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("MEMORY_RELATION_CONFLICT", "Relation operation differs", 409)
                return dict(replay)
            await self._epoch(conn, expected=expected_deletion_epoch)
            await self._lock(conn, "relation-contribution:" + contribution_id)
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND contribution_id=%s").format(
                        self._table("relation_contributions")
                    ),
                    (self.store.bot_id, contribution_id),
                )
            ).fetchone()
            if occupied is not None:
                raise ForestError(
                    "MEMORY_RELATION_CONFLICT", "Relation contribution identity differs", 409
                )
            await self._verify_endpoint(conn, left_kind, left_id)
            await self._verify_endpoint(conn, right_kind, right_id)
            source_ids = sorted({item.source_unit_id for item in refs})
            sources = await (
                await conn.execute(
                    sql.SQL("SELECT source_unit_id,source_revision,eligibility_state FROM {} "
                            "WHERE bot_id=%s AND source_unit_id=ANY(%s) ORDER BY source_unit_id "
                            "FOR SHARE").format(self._table("source_units")),
                    (self.store.bot_id, source_ids),
                )
            ).fetchall()
            if [row["source_unit_id"] for row in sources] != source_ids or any(
                row["eligibility_state"] != "ELIGIBLE" for row in sources
            ):
                raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Relation evidence is unavailable", 409)
            by_id = {row["source_unit_id"]: row for row in sources}
            if any(by_id[item.source_unit_id]["source_revision"] != item.source_revision for item in refs):
                raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Evidence revision differs", 409)
            parts = await (
                await conn.execute(
                    sql.SQL("SELECT source_unit_id,part_id FROM {} WHERE bot_id=%s "
                            "AND source_unit_id=ANY(%s)").format(self._table("source_parts")),
                    (self.store.bot_id, source_ids),
                )
            ).fetchall()
            available_parts = {(row["source_unit_id"], row["part_id"]) for row in parts}
            if any((item.source_unit_id, item.part_id) not in available_parts for item in refs):
                raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Evidence part differs", 409)
            row = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,contribution_id,operation_id,input_digest,"
                            "relation_id,relation_kind,left_kind,left_id,right_kind,right_id,"
                            "relation_key,origin_kind,weight_micros,admitted_deletion_epoch) VALUES "
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *").format(
                        self._table("relation_contributions")
                    ),
                    (
                        self.store.bot_id,
                        contribution_id,
                        operation_id,
                        input_digest,
                        relation_id,
                        relation_kind,
                        left_kind,
                        left_id,
                        right_kind,
                        right_id,
                        relation_key,
                        origin_kind,
                        weight_micros,
                        expected_deletion_epoch,
                    ),
                )
            ).fetchone()
            for index, item in enumerate(refs):
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,contribution_id,evidence_index,source_unit_id,"
                            "source_revision,part_id) VALUES (%s,%s,%s,%s,%s,%s)").format(
                        self._table("relation_evidence")
                    ),
                    (
                        self.store.bot_id,
                        contribution_id,
                        index,
                        item.source_unit_id,
                        item.source_revision,
                        item.part_id,
                    ),
                )
            return dict(row)

    async def retract_relation(
        self,
        *,
        operation_id: str,
        contribution_id: str,
        reason: MemoryPayloadRef,
        expected_deletion_epoch: int,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(contribution_id, "contribution_id", 256)
        reason.validate("reason")
        input_digest = _digest(
            [contribution_id, reason.canonical(), expected_deletion_epoch]
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "relation-retraction:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("relation_retractions")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("MEMORY_RELATION_CONFLICT", "Retraction input differs", 409)
                return dict(replay)
            await self._epoch(conn, expected=expected_deletion_epoch)
            contribution = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND contribution_id=%s FOR UPDATE").format(
                        self._table("relation_contributions")
                    ),
                    (self.store.bot_id, contribution_id),
                )
            ).fetchone()
            if contribution is None:
                raise ForestError("MEMORY_RELATION_NOT_FOUND", "Relation contribution is unavailable", 404)
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND contribution_id=%s").format(
                        self._table("relation_retractions")
                    ),
                    (self.store.bot_id, contribution_id),
                )
            ).fetchone()
            if occupied is not None:
                raise ForestError("MEMORY_RELATION_CONFLICT", "Relation is already retracted", 409)
            await self._verify_payload(conn, reason)
            row = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,contribution_id,"
                            "reason_payload_id,reason_payload_digest,reason_owner_component_id,"
                            "reason_owner_kind,reason_owner_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                            "RETURNING *").format(self._table("relation_retractions")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        contribution_id,
                        reason.payload_id,
                        reason.digest,
                        reason.owner_component_id,
                        reason.owner_kind,
                        reason.owner_id,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET active=false WHERE bot_id=%s AND contribution_id=%s").format(
                    self._table("relation_contributions")
                ),
                (self.store.bot_id, contribution_id),
            )
            return dict(row)

    async def list_trunks(self) -> list[dict]:
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT t.trunk_id,t.source_id,s.platform,s.realm_id,s.external_group_id,"
                        "s.identity_incarnation,count(u.source_unit_id) FILTER "
                        "(WHERE u.eligibility_state='ELIGIBLE') AS eligible_sources,"
                        "count(u.source_unit_id) AS registered_sources,"
                        "min(u.source_order_seq) AS first_source_order_seq,"
                        "max(u.source_order_seq) AS latest_source_order_seq "
                        "FROM {} t JOIN {} s ON s.bot_id=t.bot_id AND s.source_id=t.source_id "
                        "LEFT JOIN {} u ON u.bot_id=t.bot_id AND u.trunk_id=t.trunk_id "
                        "WHERE t.bot_id=%s GROUP BY t.trunk_id,t.source_id,s.platform,s.realm_id,"
                        "s.external_group_id,s.identity_incarnation ORDER BY t.trunk_id"
                    ).format(
                        self._table("memory_trunks"),
                        self._table("memory_sources"),
                        self._table("source_units"),
                    ),
                    (self.store.bot_id,),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def query_sources(
        self,
        *,
        limit: int = 100,
        trunk_id: str | None = None,
        author_subject_ref: str | None = None,
        modality: str | None = None,
        unit_kind: str | None = None,
        occurred_from: datetime | None = None,
        occurred_to: datetime | None = None,
        snapshot_accepted_at: datetime | None = None,
        after_occurred_at: datetime | None = None,
        after_source_unit_id: str | None = None,
    ) -> dict:
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid source page limit", 422)
        _bounded(trunk_id, "trunk_id", 256, optional=True)
        _bounded(author_subject_ref, "author_subject_ref", 256, optional=True)
        _bounded(unit_kind, "unit_kind", 128, optional=True)
        if modality is not None and modality not in {"TEXT", "IMAGE", "MIXED", "CONTROL"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid modality filter", 422)
        times = {
            name: _timestamp(value, name) if value is not None else None
            for name, value in (
                ("occurred_from", occurred_from),
                ("occurred_to", occurred_to),
                ("snapshot_accepted_at", snapshot_accepted_at),
                ("after_occurred_at", after_occurred_at),
            )
        }
        _bounded(after_source_unit_id, "after_source_unit_id", 256, optional=True)
        if (times["after_occurred_at"] is None) != (after_source_unit_id is None):
            raise ForestError("MEMORY_INPUT_INVALID", "Incomplete Memory page cursor", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            if times["snapshot_accepted_at"] is None:
                snapshot = await (
                    await conn.execute("SELECT clock_timestamp() AS snapshot")
                ).fetchone()
                times["snapshot_accepted_at"] = snapshot["snapshot"]
            conditions = [
                sql.SQL("u.bot_id=%s"),
                sql.SQL("u.eligibility_state='ELIGIBLE'"),
                sql.SQL("u.accepted_at<=%s"),
                sql.SQL("u.eligible_at<=%s"),
            ]
            params = [
                self.store.bot_id,
                times["snapshot_accepted_at"],
                times["snapshot_accepted_at"],
            ]
            for clause, value in (
                ("u.trunk_id=%s", trunk_id),
                ("u.author_subject_ref=%s", author_subject_ref),
                ("u.modality=%s", modality),
                ("u.unit_kind=%s", unit_kind),
                ("u.occurred_at>=%s", times["occurred_from"]),
                ("u.occurred_at<%s", times["occurred_to"]),
            ):
                if value is not None:
                    conditions.append(sql.SQL(clause))
                    params.append(value)
            if times["after_occurred_at"] is not None:
                conditions.append(sql.SQL("(u.occurred_at,u.source_unit_id)<(%s,%s)"))
                params.extend([times["after_occurred_at"], after_source_unit_id])
            params.append(limit + 1)
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT u.*,s.source_kind,s.platform,s.realm_id,s.external_group_id,"
                        "s.identity_incarnation FROM {} u JOIN {} s ON s.bot_id=u.bot_id "
                        "AND s.source_id=u.source_id WHERE {} ORDER BY u.occurred_at DESC,"
                        "u.source_unit_id DESC LIMIT %s"
                    ).format(
                        self._table("source_units"),
                        self._table("memory_sources"),
                        sql.SQL(" AND ").join(conditions),
                    ),
                    params,
                )
            ).fetchall()
            page = rows[:limit]
            unit_ids = [row["source_unit_id"] for row in page]
            part_rows = []
            if unit_ids:
                part_rows = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND source_unit_id=ANY(%s) "
                                "ORDER BY source_unit_id,part_index").format(
                            self._table("source_parts")
                        ),
                        (self.store.bot_id, unit_ids),
                    )
                ).fetchall()
            parts_by_unit = {identity: [] for identity in unit_ids}
            for part in part_rows:
                parts_by_unit[part["source_unit_id"]].append(dict(part))
            items = [
                {**dict(row), "parts": parts_by_unit[row["source_unit_id"]]}
                for row in page
            ]
            next_cursor = None
            if len(rows) > limit and page:
                last = page[-1]
                next_cursor = {
                    "occurred_at": last["occurred_at"],
                    "source_unit_id": last["source_unit_id"],
                }
            return {
                "items": items,
                "has_more": len(rows) > limit,
                "next_cursor": next_cursor,
                "snapshot_accepted_at": times["snapshot_accepted_at"],
            }

    async def query_relations(
        self,
        *,
        endpoint_kind: str,
        endpoint_id: str,
        active: bool | None = True,
        eligible_only: bool = True,
        limit: int = 100,
        after_relation_id: str | None = None,
        after_contribution_id: str | None = None,
        snapshot_created_at: datetime | None = None,
    ) -> dict:
        if endpoint_kind not in _ENDPOINT_KINDS:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation endpoint kind", 422)
        _bounded(endpoint_id, "endpoint_id", 256)
        _bounded(after_relation_id, "after_relation_id", 256, optional=True)
        _bounded(after_contribution_id, "after_contribution_id", 256, optional=True)
        if active is not None and type(active) is not bool:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation active filter", 422)
        if type(eligible_only) is not bool:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid evidence eligibility filter", 422)
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid relation page limit", 422)
        if (after_relation_id is None) != (after_contribution_id is None):
            raise ForestError("MEMORY_INPUT_INVALID", "Incomplete relation page cursor", 422)
        snapshot_created_at = (
            _timestamp(snapshot_created_at, "snapshot_created_at")
            if snapshot_created_at is not None
            else None
        )
        conditions = [
            sql.SQL("c.bot_id=%s"),
            sql.SQL(
                "((c.left_kind=%s AND c.left_id=%s) OR "
                "(c.right_kind=%s AND c.right_id=%s))"
            ),
        ]
        params = [
            self.store.bot_id,
            endpoint_kind,
            endpoint_id,
            endpoint_kind,
            endpoint_id,
        ]
        if active is not None:
            conditions.append(sql.SQL("c.active=%s"))
            params.append(active)
        if after_relation_id is not None:
            conditions.append(sql.SQL("(c.relation_id,c.contribution_id)>(%s,%s)"))
            params.extend([after_relation_id, after_contribution_id])
        async with self.store.pool.connection() as conn, conn.transaction():
            if snapshot_created_at is None:
                snapshot = await (
                    await conn.execute("SELECT clock_timestamp() AS snapshot")
                ).fetchone()
                snapshot_created_at = snapshot["snapshot"]
            conditions.append(sql.SQL("c.created_at<=%s"))
            params.append(snapshot_created_at)
            if eligible_only:
                conditions.append(
                    sql.SQL(
                        "NOT EXISTS (SELECT 1 FROM {} e0 JOIN {} u0 "
                        "ON u0.bot_id=e0.bot_id AND u0.source_unit_id=e0.source_unit_id "
                        "WHERE e0.bot_id=c.bot_id AND e0.contribution_id=c.contribution_id "
                        "AND u0.eligibility_state<>'ELIGIBLE')"
                    ).format(
                        self._table("relation_evidence"),
                        self._table("source_units"),
                    )
                )
                if endpoint_kind == "SOURCE_UNIT":
                    conditions.append(
                        sql.SQL(
                            "EXISTS (SELECT 1 FROM {} ue WHERE ue.bot_id=c.bot_id "
                            "AND ue.source_unit_id=%s AND ue.eligibility_state='ELIGIBLE')"
                        ).format(self._table("source_units"))
                    )
                    params.append(endpoint_id)
            params.append(limit + 1)
            rows = await (
                await conn.execute(
                    sql.SQL("SELECT c.* FROM {} c WHERE {} ORDER BY c.relation_id,c.contribution_id "
                            "LIMIT %s").format(
                        self._table("relation_contributions"),
                        sql.SQL(" AND ").join(conditions),
                    ),
                    params,
                )
            ).fetchall()
            page = rows[:limit]
            contribution_ids = [row["contribution_id"] for row in page]
            evidence_rows = []
            if contribution_ids:
                evidence_rows = await (
                    await conn.execute(
                        sql.SQL("SELECT e.*,u.trunk_id,u.author_subject_ref,u.eligibility_state,"
                                "s.source_kind,"
                                "s.platform,s.realm_id,s.external_group_id FROM {} e JOIN {} u "
                                "ON u.bot_id=e.bot_id AND u.source_unit_id=e.source_unit_id "
                                "JOIN {} s ON s.bot_id=u.bot_id AND s.source_id=u.source_id "
                                "WHERE e.bot_id=%s AND e.contribution_id=ANY(%s) "
                                "ORDER BY e.contribution_id,e.evidence_index").format(
                            self._table("relation_evidence"),
                            self._table("source_units"),
                            self._table("memory_sources"),
                        ),
                        (self.store.bot_id, contribution_ids),
                    )
                ).fetchall()
            by_contribution = {identity: [] for identity in contribution_ids}
            for evidence in evidence_rows:
                by_contribution[evidence["contribution_id"]].append(dict(evidence))
            next_cursor = None
            if len(rows) > limit and page:
                next_cursor = {
                    "relation_id": page[-1]["relation_id"],
                    "contribution_id": page[-1]["contribution_id"],
                }
            return {
                "items": [
                    {**dict(row), "evidence": by_contribution[row["contribution_id"]]}
                    for row in page
                ],
                "has_more": len(rows) > limit,
                "next_cursor": next_cursor,
                "snapshot_created_at": snapshot_created_at,
            }


class MemoryForgetUnitOfWork:
    """Atomically revoke payload closure and suppress Memory source units."""

    def __init__(self, store, *, memory: MemoryGraphRepository, payload_forgetter) -> None:
        self.store = store
        self.memory = memory
        self.payload_forgetter = payload_forgetter

    async def apply(
        self,
        *,
        operation_id: str,
        source_unit_ids,
        expected_deletion_epoch: int,
        reason_code: str,
    ) -> dict:
        if self.payload_forgetter is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "Payload forget authority is unavailable", 503
            )
        source_unit_ids = tuple(sorted(set(source_unit_ids)))
        async with self.store.pool.connection() as conn, conn.transaction():
            payload_ids = await self.memory.payload_ids_for_sources_in(conn, source_unit_ids)
            payload_result = await _call(
                self.payload_forgetter,
                conn,
                operation_id=operation_id,
                payload_ids=payload_ids,
                expected_deletion_epoch=expected_deletion_epoch,
            )
            memory_result = await self.memory.suppress_sources_in(
                conn,
                operation_id=operation_id,
                source_unit_ids=source_unit_ids,
                deletion_epoch=payload_result["deletion_epoch"],
                reason_code=reason_code,
            )
            return {
                **memory_result,
                "payload_selected_count": payload_result["selected_count"],
                "payload_selection_digest": payload_result["selection_digest"],
            }

"""Immutable PostgreSQL adjacency shards built solely by the pure reducer.

Every method is host-only. Returned metadata is a candidate, never permission
to expose another conversation's records; callers must authorize its sources.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_graph import (
    Assertion,
    GraphProjection,
    LogicalKey,
    NodeRef,
    RelationContribution,
    Retraction,
    SourcePart,
    SourceUnit,
    ValueRef,
    reduce_graph,
    relation_id,
)
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore

WINDOW_SIZE = 256
MAX_WINDOWS = 8
MAX_UNITS = 512
MAX_OPERATIONS = 1024
MAX_METADATA_BYTES = 4 * 1024 * 1024
MAX_SOURCE_BYTES = 2 * 1024 * 1024


def _error(code, message, status=409):
    raise ForestError(code, message, status)


def _id(value, *, empty=False):
    if (
        not isinstance(value, str)
        or (not value and not empty)
        or len(value.encode()) > 256
        or any(ord(c) < 32 for c in value)
    ):
        _error("GRAPH_INVALID", "Graph identity must be bounded text", 422)


def _revision(value):
    if type(value) is not int or not 0 <= value < 2**63:
        _error("GRAPH_INVALID", "Graph revision must be a nonnegative integer", 422)


def _encode(value):
    result = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")
    if len(result) > MAX_METADATA_BYTES:
        _error("GRAPH_COVERAGE_LIMIT", "Graph shard metadata exceeds its bounded capacity", 413)
    return result


def _digest(value):
    return "sha256:" + hashlib.sha256(value).hexdigest()


@dataclass(frozen=True, order=True)
class GraphWindow:
    source_id: str
    start_seq: int
    end_seq: int

    def __post_init__(self):
        _id(self.source_id)
        _revision(self.start_seq)
        _revision(self.end_seq)
        if not 1 <= self.start_seq <= self.end_seq < self.start_seq + WINDOW_SIZE:
            _error("GRAPH_COVERAGE_LIMIT", "Graph window must cover one to 256 positions", 422)


@dataclass(frozen=True)
class GraphScope:
    windows: tuple[GraphWindow, ...]
    partition: str = "trunk"

    def __post_init__(self):
        _id(self.partition)
        windows = tuple(sorted(self.windows))
        if (
            not 1 <= len(windows) <= MAX_WINDOWS
            or any(type(w) is not GraphWindow for w in windows)
            or sum(w.end_seq - w.start_seq + 2 for w in windows) > MAX_UNITS
        ):
            _error("GRAPH_COVERAGE_LIMIT", "Scope exceeds bounded windows including halos", 422)
        # One contiguous window per source: reducing disjoint windows together
        # would incorrectly invent an edge across their unexamined gap.
        if len({w.source_id for w in windows}) != len(windows):
            _error("GRAPH_INVALID", "A scope has one contiguous window per source", 422)
        object.__setattr__(self, "windows", windows)

    @property
    def scope_id(self):
        return "graph-scope:" + digest_json(asdict(self))


@dataclass(frozen=True)
class GraphSnapshot:
    metadata_json: str

    @property
    def metadata(self):
        return json.loads(self.metadata_json)


@dataclass(frozen=True)
class GraphCandidate:
    snapshot: GraphSnapshot
    canonical_projection: bytes

    @property
    def projection_digest(self):
        return _digest(self.canonical_projection)


def _scope(value):
    return GraphScope(
        tuple(GraphWindow(**w) for w in value["windows"]), value.get("partition", "trunk")
    )


def _command(kind, value):
    value = dict(value)
    if kind == "assertion":
        return Assertion(
            **{
                **value,
                "logical_key": LogicalKey(**value["logical_key"]),
                "value": ValueRef(**value["value"]),
                "source_refs": tuple(SourceRef(**r) for r in value["source_refs"]),
            }
        )
    if kind == "relation":
        return RelationContribution(
            **{
                **value,
                "left": NodeRef(**value["left"]),
                "right": NodeRef(**value["right"]),
                "source_refs": tuple(SourceRef(**r) for r in value["source_refs"]),
            }
        )
    if kind == "retraction":
        return Retraction(**value)
    _error("GRAPH_INVALID", "Unknown graph operation", 422)


def build(snapshot: GraphSnapshot) -> GraphCandidate:
    """CPU-only phase; never hold a database transaction while reducing."""
    value = snapshot.metadata
    scope = _scope(value["scope"])
    if value["scope_id"] != scope.scope_id:
        _error("GRAPH_INVALID", "Graph coverage identity does not match its window", 422)
    units = tuple(
        SourceUnit(
            **{
                **unit,
                "parts": tuple(SourcePart(**p) for p in unit["parts"]),
            }
        )
        for unit in value["sources"]
    )
    groups = {"assertion": [], "relation": [], "retraction": []}
    for row in value["operations"]:
        groups[row["kind"]].append(_command(row["kind"], row["metadata"]))
    graph: GraphProjection = reduce_graph(
        value["bot_id"],
        sources=units,
        assertions=groups["assertion"],
        relations=groups["relation"],
        retractions=groups["retraction"],
    )
    # The reducer sees only this window plus its explicitly declared predecessor.
    # This metadata cap also bounds all adjacency writes in the publish transaction.
    projection = graph.canonical_bytes()
    if len(projection) > MAX_METADATA_BYTES:
        _error("GRAPH_COVERAGE_LIMIT", "Graph projection exceeds shard capacity", 413)
    return GraphCandidate(snapshot, projection)


class GraphStore:
    def __init__(self, messages):
        self.messages, self.store = messages, messages.store
        self.sources = MemorySourceStore(messages)
        self.payloads = PayloadStore(self.store)

    build = staticmethod(build)

    async def _guard(self, conn, lease):
        # All original source mutations acquire runtime before memory clock.
        # This also fences Job cancellation/echo classification lacking clock bumps.
        runtime = await self.store._runtime(conn)
        await self.store._host(conn, lease)
        clock = await self.payloads.lock_in(conn)
        return {
            **clock,
            "storage_generation": runtime["storage_generation"],
            "maintenance_epoch": runtime["maintenance_epoch"],
            "data_admission": runtime["data_admission"],
        }

    async def _scope_in(self, conn, scope):
        if type(scope) is not GraphScope:
            _error("GRAPH_INVALID", "A typed graph scope is required", 422)
        await conn.execute(
            "INSERT INTO public.bot_memory_graph_scopes(bot_id,scope_id,windows) "
            "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id, scope.scope_id, Jsonb(asdict(scope))),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_graph_scopes WHERE bot_id=%s AND scope_id=%s FOR UPDATE",
                (self.store.bot_id, scope.scope_id),
            )
        ).fetchone()

    async def _qualified_in(self, conn, identities, *, include_payload=True):
        rows = []
        ids = sorted(set(identities))
        if len(ids) > MAX_UNITS:
            _error("GRAPH_COVERAGE_LIMIT", "Source authorization set exceeds shard capacity", 413)
        sizes = await (
            await conn.execute(
                "SELECT COALESCE(sum(h.byte_length),0) AS bytes FROM public.bot_memory_source_units u "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) WHERE u.bot_id=%s AND u.source_unit_id=ANY(%s)",
                (self.store.bot_id, ids),
            )
        ).fetchone()
        if sizes["bytes"] > MAX_SOURCE_BYTES:
            _error("GRAPH_COVERAGE_LIMIT", "Source bodies exceed the bounded metadata scan", 413)
        for offset in range(0, len(ids), 128):
            result = await self.sources.authorize_sources_in(
                conn,
                source_unit_ids=ids[offset : offset + 128],
                limit=128,
                include_payload=include_payload,
            )
            rows.extend(result["sources"])
        return rows

    async def _selected_in(self, conn, scope):
        selected, halos = set(), {}
        for window in scope.windows:
            rows = await (
                await conn.execute(
                    "SELECT source_unit_id FROM public.bot_memory_source_units "
                    "WHERE bot_id=%s AND source_id=%s AND source_order_seq BETWEEN %s AND %s "
                    "AND public.forest_graph_source_eligible(bot_id,source_unit_id) "
                    "ORDER BY source_order_seq LIMIT %s",
                    (
                        self.store.bot_id,
                        window.source_id,
                        window.start_seq,
                        window.end_seq,
                        WINDOW_SIZE + 1,
                    ),
                )
            ).fetchall()
            selected.update(r["source_unit_id"] for r in rows)
            halo = None
            if rows and window.start_seq > 1:
                halo = await (
                    await conn.execute(
                        "SELECT source_unit_id FROM public.bot_memory_source_units "
                        "WHERE bot_id=%s AND source_id=%s AND source_order_seq<%s "
                        "AND public.forest_graph_source_eligible(bot_id,source_unit_id) "
                        "ORDER BY source_order_seq DESC LIMIT 1",
                        (self.store.bot_id, window.source_id, window.start_seq),
                    )
                ).fetchone()
            halos[window.source_id] = halo["source_unit_id"] if halo else None
            if halo:
                selected.add(halo["source_unit_id"])
        if len(selected) > MAX_UNITS:
            _error("GRAPH_COVERAGE_LIMIT", "Too many source units including halos", 413)
        rows = await self._qualified_in(conn, selected)
        if {r["source_unit_id"] for r in rows} != selected:
            _error("GRAPH_SOURCE_CHANGED", "Source qualification changed during graph selection")
        return rows, halos

    async def _source_metadata_in(self, conn, rows):
        result, size = [], 0
        media_rows = await (
            await conn.execute(
                "SELECT h.source_unit_id,h.part_id,h.blob_digest,h.payload_id FROM public.bot_memory_media_heads h "
                "JOIN public.bot_payload_headers p USING(bot_id,payload_id) WHERE h.bot_id=%s "
                "AND h.source_unit_id=ANY(%s) AND h.status='CAPTURED' AND p.state='ACTIVE' "
                "AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=p.bot_id AND b.payload_id=p.payload_id) "
                "ORDER BY h.source_unit_id,h.part_id LIMIT 1025",
                (self.store.bot_id, [r["source_unit_id"] for r in rows]),
            )
        ).fetchall()
        if len(media_rows) > 1024:
            _error("GRAPH_COVERAGE_LIMIT", "Image part metadata requires smaller shards", 413)
        clock = await self.payloads.lock_in(conn)
        media = {}
        for row in media_rows:
            try:
                await self.payloads.require_available_in(
                    conn, (row["payload_id"],), expected_deletion_epoch=clock["deletion_epoch"]
                )
            except ForestError as exc:
                if exc.code not in {
                    "PAYLOAD_ERASED", "SOURCE_SUPPRESSED", "PAYLOAD_INCOMPLETE",
                    "SOURCE_LINK_SCOPE_LIMIT",
                }:
                    raise
            else:
                media[(row["source_unit_id"], row["part_id"])] = row["blob_digest"]
        for row in sorted(rows, key=lambda r: r["source_unit_id"]):
            size += len(_encode(row["parts"]))
            if size > MAX_SOURCE_BYTES:
                _error("GRAPH_COVERAGE_LIMIT", "Source metadata scan exceeds its byte budget", 413)
            # Canonical image identity is not evidence that its original bytes
            # were captured. Available image parts are added by managed media.
            parts = tuple(
                SourcePart(
                    part_id=str(p.get("part_id", i)),
                    part_index=i,
                    kind=p["type"] if p["type"] in {"text", "image", "json", "control"} else "json",
                    content_digest=media.get(
                        (row["source_unit_id"], str(p.get("part_id", i))), _digest(_encode(p))
                    ),
                )
                for i, p in enumerate(row["parts"])
            )
            available = tuple(
                p.part_id
                for p in parts
                if p.kind != "image" or (row["source_unit_id"], p.part_id) in media
            )
            occurrences = await (
                await conn.execute(
                    "SELECT occurrence_id FROM public.bot_memory_source_occurrences "
                    "WHERE bot_id=%s AND source_unit_id=%s ORDER BY occurrence_id LIMIT 1025",
                    (self.store.bot_id, row["source_unit_id"]),
                )
            ).fetchall()
            if len(occurrences) > 1024:
                _error(
                    "GRAPH_COVERAGE_LIMIT",
                    "Source observation metadata requires a smaller shard",
                    413,
                )
            unit = SourceUnit(
                bot_id=self.store.bot_id,
                source_unit_id=row["source_unit_id"],
                source_id=row["source_id"],
                source_kind=row["source_kind"],
                source_order_seq=row["source_order_seq"],
                source_revision=row["source_revision"],
                content_digest=row["content_digest"],
                payload_ref=row["payload_id"],
                unit_kind=row["kind"],
                author_id=row["principal_ref"],
                parts=parts,
                available_parts=available,
                group_source_id=row["source_id"] if row["source_kind"] == "GROUP" else "",
                platform=row["platform"],
                realm_id=row["realm_id"],
                external_group_id=row["channel_id"] if row["source_kind"] == "GROUP" else "",
                occurrence_refs=tuple(o["occurrence_id"] for o in occurrences),
            )
            result.append(asdict(unit))
        return result

    async def _operations_in(self, conn, scope_id):
        rows = await (
            await conn.execute(
                "SELECT kind,metadata FROM public.bot_memory_graph_operations "
                "WHERE bot_id=%s AND scope_id=%s ORDER BY operation_id LIMIT %s",
                (self.store.bot_id, scope_id, MAX_OPERATIONS + 1),
            )
        ).fetchall()
        targets = set()
        for row in rows:
            metadata = row["metadata"]
            if row["kind"] == "assertion":
                targets.add(metadata["assertion_id"])
            elif row["kind"] == "relation":
                command = _command("relation", metadata)
                targets.add(command.contribution_id)
                targets.add(
                    relation_id(
                        command.bot_id,
                        command.kind,
                        command.left,
                        command.right,
                        command.relation_key,
                    )
                )
        if targets:
            external = await (
                await conn.execute(
                    "SELECT kind,metadata FROM public.bot_memory_graph_operations WHERE bot_id=%s AND scope_id<>%s "
                    "AND kind='retraction' AND metadata->>'target_id'=ANY(%s) ORDER BY operation_id LIMIT %s",
                    (self.store.bot_id, scope_id, sorted(targets), MAX_OPERATIONS + 1),
                )
            ).fetchall()
            rows.extend(external)
        if len(rows) > MAX_OPERATIONS:
            _error("GRAPH_COVERAGE_LIMIT", "Graph operation shard requires partitioning", 413)
        rows.sort(key=lambda row: row["metadata"]["operation_id"])
        _encode(rows)
        return rows

    async def append(self, lease, scope, command, *, expected_deletion_epoch):
        if type(command) not in {Assertion, RelationContribution, Retraction}:
            _error("GRAPH_INVALID", "Only typed graph operations are accepted", 422)
        if command.bot_id != self.store.bot_id:
            _error("GRAPH_BOT_MISMATCH", "Graph operation belongs to another Bot", 403)
        kind = {Assertion: "assertion", RelationContribution: "relation", Retraction: "retraction"}[
            type(command)
        ]
        metadata = asdict(command)
        # Reconstructing validates the complete nested typed DTO.
        _command(kind, metadata)
        digest = _digest(_encode([scope.scope_id, kind, metadata]))
        identity = (
            "assertion:" + command.assertion_id
            if kind == "assertion"
            else "contribution:" + command.contribution_id
            if kind == "relation"
            else "retraction:" + command.operation_id
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._guard(conn, lease)
            head = await self._scope_in(conn, scope)
            previous = await (
                await conn.execute(
                    "SELECT operation_id,input_digest FROM public.bot_memory_graph_operations "
                    "WHERE bot_id=%s AND (operation_id=%s OR fact_id=%s)",
                    (self.store.bot_id, command.operation_id, identity),
                )
            ).fetchall()
            if previous:
                if len(previous) != 1 or previous[0]["input_digest"] != digest:
                    _error(
                        "IDEMPOTENCY_CONFLICT", "Graph operation identity has different metadata"
                    )
                await self.store._host(conn, lease)
                return {"operation_id": command.operation_id, "duplicate": True}
            if clock["deletion_epoch"] != expected_deletion_epoch:
                _error("DELETION_CONFLICT", "Memory deletion epoch changed")
            if head["operation_revision"] >= MAX_OPERATIONS:
                _error("GRAPH_COVERAGE_LIMIT", "Graph operation shard is full", 413)
            if kind != "retraction":
                ids = {r.source_unit_id for r in command.source_refs}
                qualified = await self._qualified_in(conn, ids)
                if {r["source_unit_id"] for r in qualified} != ids:
                    _error(
                        "GRAPH_SOURCE_UNAVAILABLE",
                        "Graph fact requires currently qualified sources",
                    )
                for row in qualified:
                    part_ids = {str(p.get("part_id", i)) for i, p in enumerate(row["parts"])}
                    if any(
                        ref.source_unit_id == row["source_unit_id"]
                        and (
                            ref.source_revision != row["source_revision"]
                            or (ref.part_id != "*" and ref.part_id not in part_ids)
                        )
                        for ref in command.source_refs
                    ):
                        _error(
                            "GRAPH_SOURCE_REFERENCE",
                            "Evidence refers to an unknown source revision or part",
                            422,
                        )
                    if not any(
                        w.source_id == row["source_id"]
                        and w.start_seq <= row["source_order_seq"] <= w.end_seq
                        for w in scope.windows
                    ):
                        _error(
                            "GRAPH_SOURCE_SCOPE",
                            "Fact evidence is outside its declared window",
                            422,
                        )
                if kind == "assertion":
                    header = await self.payloads._header_in(conn, command.value.value_ref)
                    refs = {SourceRef(**r) for r in header["manifest"]["sources"]}
                    if (
                        header["owner_kind"] != "memory.assertion"
                        or header["digest"] != command.value.value_digest
                        or not refs
                        or refs != set(command.source_refs)
                    ):
                        _error(
                            "GRAPH_VALUE_MANIFEST",
                            "Assertion value must bind its complete source manifest",
                            422,
                        )
                    await self.payloads.require_available_in(
                        conn,
                        (header["payload_id"],),
                        expected_deletion_epoch=expected_deletion_epoch,
                    )
            await conn.execute(
                "INSERT INTO public.bot_memory_graph_operations(bot_id,scope_id,operation_id,fact_id,kind,metadata,input_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    scope.scope_id,
                    command.operation_id,
                    identity,
                    kind,
                    Jsonb(metadata),
                    digest,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_memory_graph_scopes SET operation_revision=operation_revision+1 "
                "WHERE bot_id=%s AND scope_id=%s",
                (self.store.bot_id, scope.scope_id),
            )
            await conn.execute(
                "UPDATE public.bot_memory_clock SET fact_revision=fact_revision+1 WHERE bot_id=%s",
                (self.store.bot_id,),
            )
            await self.store._host(conn, lease)
            return {"operation_id": command.operation_id, "duplicate": False}

    async def capture(self, lease, scope):
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._guard(conn, lease)
            head = await self._scope_in(conn, scope)
            rows, halos = await self._selected_in(conn, scope)
            value = {
                "schema": "forest-graph-snapshot-v1",
                "bot_id": self.store.bot_id,
                "scope_id": scope.scope_id,
                "scope": asdict(scope),
                "halos": halos,
                "expected_revision": head["revision"],
                "operation_revision": head["operation_revision"],
                "fact_revision": clock["fact_revision"],
                "deletion_epoch": clock["deletion_epoch"],
                "storage_generation": clock["storage_generation"],
                "maintenance_epoch": clock["maintenance_epoch"],
                "sources": await self._source_metadata_in(conn, rows),
                "operations": await self._operations_in(conn, scope.scope_id),
            }
            await self.store._host(conn, lease)
            return GraphSnapshot(_encode(value).decode("ascii"))

    async def refresh(self, lease, scope):
        snapshot = await self.capture(lease, scope)
        candidate = build(snapshot)
        operation = "graph-refresh:" + digest_json(snapshot.metadata)
        return await self.publish(lease, candidate, operation, _skip_unchanged=True)

    async def publish(self, lease, candidate, operation_id, *, _skip_unchanged=False):
        _id(operation_id)
        if type(candidate) is not GraphCandidate or build(candidate.snapshot) != candidate:
            _error(
                "GRAPH_CANDIDATE_INVALID", "Projection differs from deterministic reduction", 422
            )
        value = candidate.snapshot.metadata
        if value["bot_id"] != self.store.bot_id:
            _error("GRAPH_BOT_MISMATCH", "Graph candidate belongs to another Bot", 403)
        scope = _scope(value["scope"])
        digest = _digest(_encode([value, candidate.projection_digest]))
        projection = json.loads(candidate.canonical_projection)
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._guard(conn, lease)
            previous = await (
                await conn.execute(
                    "SELECT scope_id,revision,graph_revision,projection_digest,input_digest "
                    "FROM public.bot_memory_graph_publications WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if previous:
                if previous.pop("input_digest") != digest:
                    _error("IDEMPOTENCY_CONFLICT", "Graph publish identity has different content")
                await self.store._host(conn, lease)
                return {**previous, "duplicate": True}
            head = await self._scope_in(conn, scope)
            if (
                head["revision"] != value["expected_revision"]
                or head["operation_revision"] != value["operation_revision"]
            ):
                _error("GRAPH_HEAD_CONFLICT", "A newer graph scope head or fact set won")
            if not clock["data_admission"]:
                _error("GRAPH_DATA_CLOSED", "Graph publication requires open data admission")
            if any(
                clock[key] != value[key]
                for key in (
                    "fact_revision",
                    "deletion_epoch",
                    "storage_generation",
                    "maintenance_epoch",
                )
            ):
                _error("GRAPH_SOURCE_CHANGED", "Memory facts or deletion epoch changed")
            rows, halos = await self._selected_in(conn, scope)
            current = await self._source_metadata_in(conn, rows)
            if _encode(current) != _encode(value["sources"]) or halos != value["halos"]:
                _error(
                    "GRAPH_SOURCE_CHANGED", "Original source qualification changed after capture"
                )
            # Independently freeze exact facts, not just a caller-controlled clock.
            if await self._operations_in(conn, scope.scope_id) != value["operations"]:
                _error("GRAPH_HEAD_CONFLICT", "Graph fact set changed after capture")
            if _skip_unchanged and head["revision"]:
                old = await (
                    await conn.execute(
                        "SELECT revision,graph_revision,projection_digest,snapshot FROM public.bot_memory_graph_publications "
                        "WHERE bot_id=%s AND scope_id=%s AND revision=%s",
                        (self.store.bot_id, scope.scope_id, head["revision"]),
                    )
                ).fetchone()
                if old["projection_digest"] == candidate.projection_digest and all(
                    old["snapshot"][key] == value[key]
                    for key in ("sources", "halos", "operation_revision", "operations")
                ):
                    await self.store._host(conn, lease)
                    return {
                        "scope_id": scope.scope_id,
                        "revision": old["revision"],
                        "graph_revision": old["graph_revision"],
                        "projection_digest": old["projection_digest"],
                        "duplicate": False,
                        "unchanged": True,
                    }
            await conn.execute(
                "INSERT INTO public.bot_memory_graph_clock(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id,),
            )
            global_head = await (
                await conn.execute(
                    "UPDATE public.bot_memory_graph_clock SET graph_revision=graph_revision+1 "
                    "WHERE bot_id=%s RETURNING graph_revision",
                    (self.store.bot_id,),
                )
            ).fetchone()
            revision, global_revision = head["revision"] + 1, global_head["graph_revision"]
            await conn.execute(
                "INSERT INTO public.bot_memory_graph_publications(bot_id,scope_id,revision,graph_revision,operation_id,"
                "input_digest,snapshot,projection_digest,canonical_projection) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    scope.scope_id,
                    revision,
                    global_revision,
                    operation_id,
                    digest,
                    Jsonb(value),
                    candidate.projection_digest,
                    candidate.canonical_projection,
                ),
            )
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    "INSERT INTO public.bot_memory_graph_nodes(bot_id,scope_id,revision,node_id,metadata) VALUES (%s,%s,%s,%s,%s)",
                    [
                        (self.store.bot_id, scope.scope_id, revision, n["node_id"], Jsonb(n))
                        for n in projection["nodes"]
                    ],
                )
                await cursor.executemany(
                    "INSERT INTO public.bot_memory_graph_edges(bot_id,scope_id,revision,edge_id,from_node_id,to_node_id,metadata) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    [
                        (
                            self.store.bot_id,
                            scope.scope_id,
                            revision,
                            e["edge_id"],
                            e["from_node_id"],
                            e["to_node_id"],
                            Jsonb(e),
                        )
                        for e in projection["edges"]
                    ],
                )
            await conn.execute(
                "UPDATE public.bot_memory_graph_scopes SET revision=%s WHERE bot_id=%s AND scope_id=%s",
                (revision, self.store.bot_id, scope.scope_id),
            )
            await self.store._host(conn, lease)
            return {
                "scope_id": scope.scope_id,
                "revision": revision,
                "graph_revision": global_revision,
                "projection_digest": candidate.projection_digest,
                "duplicate": False,
            }

    async def read(
        self, lease, scope_id, *, graph_revision=None, after="", limit=64, node_ids=None
    ):
        """Pinned adjacency candidates; caller must apply final conversation cuts."""
        _id(scope_id)
        _id(after, empty=True)
        if type(limit) is not int or not 1 <= limit <= 256:
            _error("GRAPH_INVALID", "Graph page must contain one to 256 records", 422)
        ids = None if node_ids is None else sorted(set(node_ids))
        if ids is not None:
            if len(ids) > 64:
                _error("GRAPH_INVALID", "Too many graph neighbor identities", 422)
            for identity in ids:
                _id(identity)
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._guard(conn, lease)
            global_row = await (
                await conn.execute(
                    "SELECT graph_revision FROM public.bot_memory_graph_clock WHERE bot_id=%s",
                    (self.store.bot_id,),
                )
            ).fetchone()
            current_revision = global_row["graph_revision"] if global_row else 0
            selected_revision = current_revision if graph_revision is None else graph_revision
            _revision(selected_revision)
            if selected_revision > current_revision:
                _error("GRAPH_REVISION_MISSING", "Graph revision has not been published", 404)
            publication = await (
                await conn.execute(
                    "SELECT p.*,s.operation_revision FROM public.bot_memory_graph_publications p "
                    "JOIN public.bot_memory_graph_scopes s USING(bot_id,scope_id) "
                    "WHERE p.bot_id=%s AND p.scope_id=%s AND p.graph_revision<=%s ORDER BY p.graph_revision DESC LIMIT 1",
                    (self.store.bot_id, scope_id, selected_revision),
                )
            ).fetchone()
            if publication is None:
                _error("GRAPH_REVISION_MISSING", "Scope is not covered by this graph revision", 404)
            frozen = publication["snapshot"]
            if (
                publication["operation_revision"] != frozen["operation_revision"]
                or await self._operations_in(conn, scope_id) != frozen["operations"]
            ):
                _error("GRAPH_STALE", "Graph facts changed since this publication")
            identities = [s["source_unit_id"] for s in frozen["sources"]]
            rows = await self._qualified_in(conn, identities)
            metadata = await self._source_metadata_in(conn, rows)
            if _encode(metadata) != _encode(frozen["sources"]):
                _error("GRAPH_SOURCE_CHANGED", "A published graph source is no longer available")
            edges = await (
                await conn.execute(
                    "SELECT edge_id,metadata FROM public.bot_memory_graph_edges WHERE bot_id=%s AND scope_id=%s AND revision=%s "
                    "AND edge_id>%s AND (%s::text[] IS NULL OR from_node_id=ANY(%s) OR to_node_id=ANY(%s)) "
                    "ORDER BY edge_id LIMIT %s",
                    (
                        self.store.bot_id,
                        scope_id,
                        publication["revision"],
                        after,
                        ids,
                        ids,
                        ids,
                        limit + 1,
                    ),
                )
            ).fetchall()
            more = len(edges) > limit
            edges = edges[:limit]
            endpoints = {
                identity
                for row in edges
                for identity in (row["metadata"]["from_node_id"], row["metadata"]["to_node_id"])
            }
            nodes = await (
                await conn.execute(
                    "SELECT metadata FROM public.bot_memory_graph_nodes WHERE bot_id=%s AND scope_id=%s AND revision=%s "
                    "AND node_id=ANY(%s) ORDER BY node_id",
                    (self.store.bot_id, scope_id, publication["revision"], sorted(endpoints)),
                )
            ).fetchall()
            await self.store._host(conn, lease)
            return {
                "scope_id": scope_id,
                "graph_revision": selected_revision,
                "scope_revision": publication["revision"],
                "published_at_graph_revision": publication["graph_revision"],
                "deletion_epoch": clock["deletion_epoch"],
                "fact_revision": frozen["fact_revision"],
                "coverage": frozen["scope"],
                "halos": frozen["halos"],
                "nodes": [n["metadata"] for n in nodes],
                "edges": [e["metadata"] for e in edges],
                "required_source_refs": [
                    asdict(SourceRef(s["source_unit_id"], s["source_revision"]))
                    for s in frozen["sources"]
                ],
                "next_after": edges[-1]["edge_id"] if more else None,
                "requires_conversation_authorization": True,
                "requires_partition_union_for_global_facts": True,
            }

    async def coverage(self, lease, *, graph_revision=None, after_scope_id="", limit=64):
        _id(after_scope_id, empty=True)
        if type(limit) is not int or not 1 <= limit <= 256:
            _error("GRAPH_INVALID", "Graph coverage page must be bounded", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, lease)
            current = await (
                await conn.execute(
                    "SELECT graph_revision FROM public.bot_memory_graph_clock WHERE bot_id=%s",
                    (self.store.bot_id,),
                )
            ).fetchone()
            selected = (
                (current["graph_revision"] if current else 0)
                if graph_revision is None
                else graph_revision
            )
            _revision(selected)
            if selected > (current["graph_revision"] if current else 0):
                _error("GRAPH_REVISION_MISSING", "Graph revision has not been published", 404)
            rows = await (
                await conn.execute(
                    "SELECT DISTINCT ON (scope_id) scope_id,revision,graph_revision,snapshot->'scope' AS coverage "
                    "FROM public.bot_memory_graph_publications WHERE bot_id=%s AND graph_revision<=%s AND scope_id>%s "
                    "ORDER BY scope_id,graph_revision DESC LIMIT %s",
                    (self.store.bot_id, selected, after_scope_id, limit + 1),
                )
            ).fetchall()
            await self.store._host(conn, lease)
            return {
                "graph_revision": selected,
                "scopes": rows[:limit],
                "next_after": rows[limit - 1]["scope_id"] if len(rows) > limit else None,
            }

    async def discover(self, lease, *, after_source_id="", after_start_seq=0, limit=16):
        """Finite keyset page of physical windows, including empty/unbuilt ones."""
        _id(after_source_id, empty=True)
        _revision(after_start_seq)
        if type(limit) is not int or not 1 <= limit <= 64:
            _error("GRAPH_INVALID", "Discovery page must be bounded", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, lease)
            rows = await (
                await conn.execute(
                    "SELECT source_id,1+((source_order_seq-1)/256)*256 AS start_seq "
                    "FROM public.bot_memory_source_units WHERE bot_id=%s AND "
                    "(source_id>%s OR (source_id=%s AND 1+((source_order_seq-1)/256)*256>%s)) "
                    "GROUP BY source_id,start_seq ORDER BY source_id,start_seq LIMIT %s",
                    (
                        self.store.bot_id,
                        after_source_id,
                        after_source_id,
                        after_start_seq,
                        limit + 1,
                    ),
                )
            ).fetchall()
            await self.store._host(conn, lease)
            scopes = [
                GraphScope((GraphWindow(r["source_id"], r["start_seq"], r["start_seq"] + 255),))
                for r in rows[:limit]
            ]
            return {
                "scopes": scopes,
                "next_after": {
                    "after_source_id": rows[limit - 1]["source_id"],
                    "after_start_seq": rows[limit - 1]["start_seq"],
                }
                if len(rows) > limit
                else None,
            }

"""Deterministic cross-trunk associations derived from existing embeddings."""

from __future__ import annotations

from dataclasses import asdict

from forest.memory_graph import NodeRef, RelationContribution, Retraction
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.memory_graph import GraphScope, GraphStore, GraphWindow

ALGORITHM_REVISION = "semantic-cross-trunk-top4-v1"
RELATION_KEY_PREFIX = "semantic-vector-association-v1:"
MIN_SCORE_PPM = 880_000
MAX_NEIGHBORS = 4
MAX_CANDIDATES = 4096


def _digest_id(prefix, value):
    return prefix + digest_json(value).split(":", 1)[1]


def _association_id(bot_id, space_id, source_ids):
    return _digest_id(
        "memory-semantic-association:",
        [ALGORITHM_REVISION, bot_id, space_id, *sorted(source_ids)],
    )


def _scope(rows):
    return GraphScope(
        tuple(
            GraphWindow(row["source_id"], row["source_order_seq"], row["source_order_seq"])
            for row in rows
        ),
        "semantic-associations-v1",
    )


def _saved_scope(value):
    return GraphScope(
        tuple(GraphWindow(**window) for window in value["windows"]), value["partition"]
    )


def _score_ppm(value):
    return max(0, min(1_000_000, int(float(value) * 1_000_000 + 0.5)))


class MemorySemanticAssociationStore:
    """Maintains directed Top-K votes and projects their set union as graph edges."""

    def __init__(self, messages):
        self.messages, self.store = messages, messages.store
        self.graph = GraphStore(messages)

    async def enqueue_sources(self, lease, space_id, source_unit_ids):
        identities = sorted(set(source_unit_ids))
        if not identities:
            return 0
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.graph._guard(conn, lease)
            rows = await (await conn.execute(
                "SELECT source_unit_id FROM public.bot_memory_source_units "
                "WHERE bot_id=%s AND source_unit_id=ANY(%s) ORDER BY source_unit_id",
                (self.store.bot_id, identities),
            )).fetchall()
            for row in rows:
                await conn.execute(
                    "INSERT INTO public.bot_memory_semantic_tasks(bot_id,space_id,source_unit_id) "
                    "VALUES (%s,%s,%s) ON CONFLICT(bot_id,space_id,source_unit_id) "
                    "DO UPDATE SET requested_revision="
                    "public.bot_memory_semantic_tasks.requested_revision+1,"
                    "updated_at=clock_timestamp()",
                    (self.store.bot_id, space_id, row["source_unit_id"]),
                )
            await self.store._host(conn, lease)
            return len(rows)

    async def repair_stale(self, lease, *, limit=16):
        """Queue origins whose currently projected endpoint/vector became unavailable."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.graph._guard(conn, lease)
            rows = await (await conn.execute(
                "SELECT DISTINCT v.space_id,v.origin_source_unit_id "
                "FROM public.bot_memory_semantic_votes v WHERE v.bot_id=%s AND v.active AND ("
                "NOT public.forest_graph_source_eligible(v.bot_id,v.origin_source_unit_id) OR "
                "NOT public.forest_graph_source_eligible(v.bot_id,v.neighbor_source_unit_id) OR "
                "NOT EXISTS(SELECT 1 FROM public.bot_memory_vectors x "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE x.bot_id=v.bot_id AND x.space_id=v.space_id "
                "AND x.source_unit_id=v.origin_source_unit_id AND h.state='ACTIVE') OR "
                "NOT EXISTS(SELECT 1 FROM public.bot_memory_vectors x "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE x.bot_id=v.bot_id AND x.space_id=v.space_id "
                "AND x.source_unit_id=v.neighbor_source_unit_id AND h.state='ACTIVE')) "
                "ORDER BY v.space_id,v.origin_source_unit_id LIMIT %s",
                (self.store.bot_id, limit),
            )).fetchall()
            for row in rows:
                await conn.execute(
                    "INSERT INTO public.bot_memory_semantic_tasks(bot_id,space_id,source_unit_id) "
                    "VALUES (%s,%s,%s) ON CONFLICT(bot_id,space_id,source_unit_id) "
                    "DO UPDATE SET requested_revision="
                    "public.bot_memory_semantic_tasks.requested_revision+1,"
                    "updated_at=clock_timestamp()",
                    (self.store.bot_id, row["space_id"], row["origin_source_unit_id"]),
                )
            await self.store._host(conn, lease)
            return len(rows)

    async def _candidates_in(self, conn, task):
        rows = await (await conn.execute(
            "WITH target AS ("
            " SELECT v.facet_id,v.vector FROM public.bot_memory_vectors v "
            " JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
            " WHERE v.bot_id=%s AND v.space_id=%s AND v.source_unit_id=%s "
            " AND h.state='ACTIVE'"
            "), scored AS ("
            " SELECT cu.source_unit_id,cu.source_id,cu.source_order_seq,cu.source_revision,"
            " tv.facet_id AS origin_facet_id,cv.facet_id AS neighbor_facet_id,sim.score "
            " FROM target tv CROSS JOIN public.bot_memory_vectors cv "
            " JOIN public.bot_payload_headers ch ON ch.bot_id=cv.bot_id AND ch.payload_id=cv.payload_id "
            " JOIN public.bot_memory_source_units cu ON cu.bot_id=cv.bot_id "
            " AND cu.source_unit_id=cv.source_unit_id "
            " JOIN public.bot_memory_source_units tu ON tu.bot_id=cv.bot_id AND tu.source_unit_id=%s "
            " CROSS JOIN LATERAL (SELECT sum(a.value::double precision*b.value::double precision) AS score "
            " FROM unnest(tv.vector) WITH ORDINALITY a(value,n) "
            " JOIN unnest(cv.vector) WITH ORDINALITY b(value,n) USING(n)) sim "
            " WHERE cv.bot_id=%s AND cv.space_id=%s AND cv.source_unit_id<>%s "
            " AND cu.source_id<>tu.source_id AND ch.state='ACTIVE' "
            " AND cardinality(cv.vector)=cardinality(tv.vector) "
            " AND public.forest_graph_source_eligible(cv.bot_id,cv.source_unit_id)"
            "), best AS ("
            " SELECT *,row_number() OVER(PARTITION BY source_unit_id "
            " ORDER BY score DESC,origin_facet_id,neighbor_facet_id) AS choice FROM scored"
            ") SELECT * FROM best WHERE choice=1 ORDER BY score DESC,source_unit_id LIMIT %s",
            (
                self.store.bot_id,
                task["space_id"],
                task["source_unit_id"],
                task["source_unit_id"],
                self.store.bot_id,
                task["space_id"],
                task["source_unit_id"],
                MAX_CANDIDATES + 1,
            ),
        )).fetchall()
        return [
            {**row, "score_ppm": _score_ppm(row["score"])}
            for row in rows
            if _score_ppm(row["score"]) >= MIN_SCORE_PPM
        ]

    async def _append_change_in(self, conn, lease, task, row, *, active, endpoint=None):
        revision = row["revision"] + 1
        operation_id = _digest_id(
            "semantic-association-op:",
            [
                ALGORITHM_REVISION,
                self.store.bot_id,
                task["space_id"],
                task["source_unit_id"],
                row["neighbor_source_unit_id"],
                revision,
                active,
            ],
        )
        contribution_id = None
        scope = _saved_scope(row["windows"])
        if active:
            contribution_id = _digest_id(
                "semantic-association-contribution:", [operation_id, "contribution"]
            )
            endpoint_rows = sorted(
                (endpoint["origin"], endpoint["neighbor"]), key=lambda item: item["source_unit_id"]
            )
            refs = tuple(
                SourceRef(item["source_unit_id"], item["source_revision"])
                for item in endpoint_rows
            )
            command = RelationContribution(
                self.store.bot_id,
                operation_id,
                contribution_id,
                "ASSOCIATES_WITH",
                NodeRef(self.store.bot_id, "SOURCE_UNIT", endpoint_rows[0]["source_unit_id"]),
                NodeRef(self.store.bot_id, "SOURCE_UNIT", endpoint_rows[1]["source_unit_id"]),
                refs,
                RELATION_KEY_PREFIX + task["space_id"],
            )
        else:
            command = Retraction(
                self.store.bot_id,
                operation_id,
                "contribution",
                row["current_contribution_id"],
            )
        await self.graph.append_in(
            conn,
            lease,
            scope,
            command,
            expected_deletion_epoch=task["deletion_epoch"],
        )
        await conn.execute(
            "UPDATE public.bot_memory_semantic_votes SET revision=%s,active=%s,"
            "current_contribution_id=%s,updated_at=clock_timestamp() WHERE bot_id=%s "
            "AND space_id=%s AND origin_source_unit_id=%s AND neighbor_source_unit_id=%s",
            (
                revision,
                active,
                contribution_id,
                self.store.bot_id,
                task["space_id"],
                task["source_unit_id"],
                row["neighbor_source_unit_id"],
            ),
        )
        await conn.execute(
            "INSERT INTO public.bot_memory_semantic_vote_changes("
            "bot_id,space_id,origin_source_unit_id,neighbor_source_unit_id,revision,active,"
            "graph_operation_id,contribution_id,score_ppm,origin_facet_id,neighbor_facet_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id,
                task["space_id"],
                task["source_unit_id"],
                row["neighbor_source_unit_id"],
                revision,
                active,
                operation_id,
                contribution_id,
                row["score_ppm"],
                row["origin_facet_id"],
                row["neighbor_facet_id"],
            ),
        )

    async def _activate_in(self, conn, lease, task, origin, candidate, current):
        neighbor = {
            key: candidate[key]
            for key in ("source_unit_id", "source_id", "source_order_seq", "source_revision")
        }
        scope = _scope((origin, neighbor))
        association_id = _association_id(
            self.store.bot_id,
            task["space_id"],
            (origin["source_unit_id"], neighbor["source_unit_id"]),
        )
        if current is None:
            await self.graph._scope_in(conn, scope)
            await conn.execute(
                "INSERT INTO public.bot_memory_semantic_votes("
                "bot_id,space_id,origin_source_unit_id,neighbor_source_unit_id,association_id,"
                "scope_id,score_ppm,origin_facet_id,neighbor_facet_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    task["space_id"],
                    origin["source_unit_id"],
                    neighbor["source_unit_id"],
                    association_id,
                    scope.scope_id,
                    candidate["score_ppm"],
                    candidate["origin_facet_id"],
                    candidate["neighbor_facet_id"],
                ),
            )
            current = {
                "revision": 0,
                "active": False,
                "current_contribution_id": None,
                "neighbor_source_unit_id": neighbor["source_unit_id"],
                "score_ppm": candidate["score_ppm"],
                "origin_facet_id": candidate["origin_facet_id"],
                "neighbor_facet_id": candidate["neighbor_facet_id"],
                "windows": asdict(scope),
            }
        else:
            if (
                current["score_ppm"],
                current["origin_facet_id"],
                current["neighbor_facet_id"],
            ) != (
                candidate["score_ppm"],
                candidate["origin_facet_id"],
                candidate["neighbor_facet_id"],
            ):
                await conn.execute(
                    "UPDATE public.bot_memory_semantic_votes SET score_ppm=%s,origin_facet_id=%s,"
                    "neighbor_facet_id=%s,updated_at=clock_timestamp() WHERE bot_id=%s "
                    "AND space_id=%s AND origin_source_unit_id=%s AND neighbor_source_unit_id=%s",
                    (
                        candidate["score_ppm"],
                        candidate["origin_facet_id"],
                        candidate["neighbor_facet_id"],
                        self.store.bot_id,
                        task["space_id"],
                        origin["source_unit_id"],
                        neighbor["source_unit_id"],
                    ),
                )
                current = {**current, **{
                    key: candidate[key]
                    for key in ("score_ppm", "origin_facet_id", "neighbor_facet_id")
                }}
        if not current["active"]:
            await self._append_change_in(
                conn,
                lease,
                task,
                current,
                active=True,
                endpoint={"origin": origin, "neighbor": neighbor},
            )

    async def process_one(self, lease):
        """Process one durable source watermark and return reverse origins to wake."""
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self.graph._guard(conn, lease)
            task = await (await conn.execute(
                "SELECT * FROM public.bot_memory_semantic_tasks WHERE bot_id=%s "
                "AND processed_revision<requested_revision ORDER BY updated_at,space_id,source_unit_id "
                "LIMIT 1 FOR UPDATE SKIP LOCKED",
                (self.store.bot_id,),
            )).fetchone()
            if task is None:
                await self.store._host(conn, lease)
                return None
            task = {**task, "deletion_epoch": clock["deletion_epoch"]}
            origin = await (await conn.execute(
                "SELECT * FROM public.bot_memory_source_units WHERE bot_id=%s AND source_unit_id=%s",
                (self.store.bot_id, task["source_unit_id"]),
            )).fetchone()
            eligible = bool(origin) and bool((await (await conn.execute(
                "SELECT public.forest_graph_source_eligible(%s,%s) AS eligible",
                (self.store.bot_id, task["source_unit_id"]),
            )).fetchone())["eligible"])
            candidates = await self._candidates_in(conn, task) if eligible else []
            current_rows = await (await conn.execute(
                "SELECT v.*,s.windows FROM public.bot_memory_semantic_votes v "
                "JOIN public.bot_memory_graph_scopes s USING(bot_id,scope_id) "
                "WHERE v.bot_id=%s AND v.space_id=%s AND v.origin_source_unit_id=%s "
                "ORDER BY v.neighbor_source_unit_id FOR UPDATE OF v",
                (self.store.bot_id, task["space_id"], task["source_unit_id"]),
            )).fetchall()
            current = {row["neighbor_source_unit_id"]: row for row in current_rows}
            if len(candidates) > MAX_CANDIDATES:
                for row in current.values():
                    if row["active"]:
                        await self._append_change_in(conn, lease, task, row, active=False)
                await conn.execute(
                    "UPDATE public.bot_memory_semantic_tasks SET processed_revision=%s,"
                    "outcome='CAPACITY_EXCEEDED',association_count=0,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s "
                    "AND space_id=%s AND source_unit_id=%s",
                    (
                        task["requested_revision"],
                        self.store.bot_id,
                        task["space_id"],
                        task["source_unit_id"],
                    ),
                )
                await self.store._host(conn, lease)
                return {"processed": True, "affected": [], "outcome": "CAPACITY_EXCEEDED"}
            candidates.sort(key=lambda row: (-row["score_ppm"], row["source_unit_id"]))
            desired = {row["source_unit_id"]: row for row in candidates[:MAX_NEIGHBORS]}
            for identity, row in current.items():
                if row["active"] and identity not in desired:
                    await self._append_change_in(conn, lease, task, row, active=False)
            if origin:
                for identity, candidate in desired.items():
                    await self._activate_in(
                        conn, lease, task, origin, candidate, current.get(identity)
                    )

            candidate_ids = [row["source_unit_id"] for row in candidates]
            reverse_rows = await (await conn.execute(
                "SELECT * FROM public.bot_memory_semantic_votes WHERE bot_id=%s AND space_id=%s "
                "AND origin_source_unit_id=ANY(%s) AND active ORDER BY origin_source_unit_id,"
                "score_ppm DESC,neighbor_source_unit_id",
                (self.store.bot_id, task["space_id"], candidate_ids or [""]),
            )).fetchall()
            by_origin = {}
            for row in reverse_rows:
                by_origin.setdefault(row["origin_source_unit_id"], []).append(row)
            affected = set()
            candidate_map = {row["source_unit_id"]: row for row in candidates}
            for identity, candidate in candidate_map.items():
                votes = by_origin.get(identity, [])
                reverse = next(
                    (
                        row
                        for row in votes
                        if row["neighbor_source_unit_id"] == task["source_unit_id"]
                    ),
                    None,
                )
                expected = (
                    candidate["score_ppm"],
                    candidate["neighbor_facet_id"],
                    candidate["origin_facet_id"],
                )
                if reverse:
                    actual = (
                        reverse["score_ppm"],
                        reverse["origin_facet_id"],
                        reverse["neighbor_facet_id"],
                    )
                    if actual != expected:
                        affected.add(identity)
                elif len(votes) < MAX_NEIGHBORS or (
                    -candidate["score_ppm"], task["source_unit_id"]
                ) < (-votes[-1]["score_ppm"], votes[-1]["neighbor_source_unit_id"]):
                    affected.add(identity)
            incoming = await (await conn.execute(
                "SELECT origin_source_unit_id FROM public.bot_memory_semantic_votes "
                "WHERE bot_id=%s AND space_id=%s AND neighbor_source_unit_id=%s AND active",
                (self.store.bot_id, task["space_id"], task["source_unit_id"]),
            )).fetchall()
            for row in incoming:
                if row["origin_source_unit_id"] not in candidate_map:
                    affected.add(row["origin_source_unit_id"])

            outcome = "CURRENT" if eligible else "SOURCE_UNAVAILABLE"
            await conn.execute(
                "UPDATE public.bot_memory_semantic_tasks SET processed_revision=%s,outcome=%s,"
                "association_count=%s,updated_at=clock_timestamp() WHERE bot_id=%s "
                "AND space_id=%s AND source_unit_id=%s",
                (
                    task["requested_revision"],
                    outcome,
                    len(desired),
                    self.store.bot_id,
                    task["space_id"],
                    task["source_unit_id"],
                ),
            )
            await self.store._host(conn, lease)
            return {
                "processed": True,
                "space_id": task["space_id"],
                "source_unit_id": task["source_unit_id"],
                "affected": sorted(affected - {task["source_unit_id"]}),
                "outcome": outcome,
                "association_count": len(desired),
            }

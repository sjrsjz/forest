"""Reviewed manual source associations over the durable memory graph."""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_graph import NodeRef, RelationContribution, Retraction
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.control import BotControl
from forest.storage.memory_administration import MemoryAdministration, _clone, _identifier
from forest.storage.memory_graph import GraphScope, GraphStore, GraphWindow
from forest.storage.memory_sources import MemorySourceStore

RELATION_KEY = "manual-source-association-v1"


def _association_id(source_ids):
    return "memory-association:" + digest_json(
        ["forest-manual-source-association-v1", *sorted(source_ids)]
    ).split(":", 1)[1]


def _scope(rows):
    return GraphScope(
        tuple(
            GraphWindow(row["source_id"], row["source_order_seq"], row["source_order_seq"])
            for row in rows
        ),
        "associations-v1",
    )


def _saved_scope(value):
    return GraphScope(
        tuple(GraphWindow(**window) for window in value["windows"]),
        value["partition"],
    )


class MemoryAssociationAdministration:
    def __init__(self, messages, *, control=None):
        self.messages, self.store = messages, messages.store
        self.control = control or BotControl(self.store)
        self.sources = MemorySourceStore(messages)
        self.graph = GraphStore(messages)

    async def read(self, ticket, operation_id, payload=None):
        payload = _clone({} if payload is None else payload)
        if set(payload) - {
            "after", "limit", "source_unit_id", "group_source_id", "active",
            "projection_state",
        }:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Unknown association selector", 422)
        after = payload.get("after", "")
        if after:
            _identifier(after, maximum=256)
        limit = payload.get("limit", 50)
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Association page limit is 1 to 50", 422)
        source_unit_id = payload.get("source_unit_id")
        if source_unit_id is not None:
            _identifier(source_unit_id)
        group_source_id = payload.get("group_source_id")
        if group_source_id is not None:
            _identifier(group_source_id)
        active = payload.get("active")
        if active is not None and type(active) is not bool:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Association state is invalid", 422)
        projection = payload.get("projection_state")
        if projection not in {None, "CURRENT", "PENDING"}:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Projection state is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.association.read", operation_id, payload
            )
            scoped_group = await MemoryAdministration(
                self.messages, control=self.control
            )._group_scope_in(conn, claims, group_source_id)
            rows = await (await conn.execute(
                "SELECT h.*,s.operation_revision,latest.published_operation_revision,"
                "latest.graph_revision AS published_graph_revision,"
                "ls.target_group_source_id AS left_group_source_id,"
                "rs.target_group_source_id AS right_group_source_id "
                "FROM public.bot_memory_association_heads h "
                "JOIN public.bot_memory_graph_scopes s USING(bot_id,scope_id) "
                "JOIN public.bot_memory_source_units lu ON lu.bot_id=h.bot_id "
                "AND lu.source_unit_id=h.left_source_unit_id "
                "JOIN public.bot_memory_sources ls ON ls.bot_id=lu.bot_id AND ls.source_id=lu.source_id "
                "JOIN public.bot_memory_source_units ru ON ru.bot_id=h.bot_id "
                "AND ru.source_unit_id=h.right_source_unit_id "
                "JOIN public.bot_memory_sources rs ON rs.bot_id=ru.bot_id AND rs.source_id=ru.source_id "
                "LEFT JOIN LATERAL (SELECT p.graph_revision,"
                "(p.snapshot->>'operation_revision')::bigint AS published_operation_revision "
                "FROM public.bot_memory_graph_publications p WHERE p.bot_id=h.bot_id "
                "AND p.scope_id=h.scope_id ORDER BY p.revision DESC LIMIT 1) latest ON true "
                "WHERE h.bot_id=%s AND h.association_id>%s "
                "AND (%s::text IS NULL OR h.left_source_unit_id=%s OR h.right_source_unit_id=%s) "
                "AND (%s::text IS NULL OR ls.target_group_source_id=%s OR rs.target_group_source_id=%s) "
                "AND (%s::boolean IS NULL OR h.active=%s) "
                "AND (%s::text IS NULL OR (%s='CURRENT' AND latest.published_operation_revision=s.operation_revision) "
                "OR (%s='PENDING' AND latest.published_operation_revision IS DISTINCT FROM s.operation_revision)) "
                "ORDER BY h.association_id LIMIT %s",
                (
                    self.store.bot_id, after,
                    source_unit_id, source_unit_id, source_unit_id,
                    scoped_group, scoped_group, scoped_group,
                    active, active,
                    projection, projection, projection,
                    limit,
                ),
            )).fetchall()
            ids = sorted({
                identity for row in rows
                for identity in (row["left_source_unit_id"], row["right_source_unit_id"])
            })
            selected = {}
            view = None
            if ids:
                view = await self.sources.authorize_sources_in(
                    conn, source_unit_ids=ids, limit=len(ids), include_payload=True
                )
                selected = {row["source_unit_id"]: row for row in view["sources"]}
            clock = await self.graph.payloads.lock_in(conn)
            graph_clock = await (await conn.execute(
                "SELECT graph_revision FROM public.bot_memory_graph_clock WHERE bot_id=%s",
                (self.store.bot_id,),
            )).fetchone()
            result = []
            for row in rows:
                left = selected.get(row["left_source_unit_id"])
                right = selected.get(row["right_source_unit_id"])
                if left is None or right is None:
                    continue
                result.append({
                    "association_id": row["association_id"],
                    "revision": row["revision"],
                    "active": row["active"],
                    "relation_key": RELATION_KEY,
                    "projection_state": (
                        "CURRENT"
                        if row["published_operation_revision"] == row["operation_revision"]
                        else "PENDING"
                    ),
                    "published_graph_revision": row["published_graph_revision"],
                    "relation": {
                        "kind": "ASSOCIATES_WITH",
                        "origin": "REVIEWED_MANUAL",
                        "scope_id": row["scope_id"],
                        "contribution_id": row["current_contribution_id"],
                        "operation_revision": row["operation_revision"],
                        "published_operation_revision": row["published_operation_revision"],
                        "cross_trunk": (
                            row["left_group_source_id"] != row["right_group_source_id"]
                        ),
                        "left_group_source_id": row["left_group_source_id"],
                        "right_group_source_id": row["right_group_source_id"],
                    },
                    "left": MemoryAdministration._public_source(left),
                    "right": MemoryAdministration._public_source(right),
                })
            await self.control._time(conn, claims)
            return {
                "deletion_epoch": view["deletion_epoch"] if view else clock["deletion_epoch"],
                "fact_revision": view["fact_revision"] if view else clock["fact_revision"],
                "graph_revision": graph_clock["graph_revision"] if graph_clock else 0,
                "associations": result,
                "next_after": rows[-1]["association_id"] if len(rows) == limit else None,
            }

    async def graph_scopes(self, ticket, operation_id, payload, *, lease):
        payload = _clone(payload)
        if set(payload) - {"after", "limit", "graph_revision"}:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Unknown graph scope selector", 422)
        after = payload.get("after", "")
        if after:
            _identifier(after)
        limit = payload.get("limit", 25)
        revision = payload.get("graph_revision")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Graph scope limit is 1 to 100", 422)
        if revision is not None and (type(revision) is not int or revision < 0):
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Graph revision is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.association.read", operation_id, payload
            )
            if claims.get("source_conversation_id") is not None:
                raise ForestError(
                    "SHELL_SCOPE_DENIED", "Group commands cannot traverse the global memory graph", 403
                )
            await self.control._time(conn, claims)
        result = await self.graph.coverage(
            lease, graph_revision=revision, after_scope_id=after, limit=limit
        )
        result["scopes"] = [dict(row) for row in result["scopes"]]
        return result

    async def graph_scope(self, ticket, operation_id, payload, *, lease):
        payload = _clone(payload)
        if set(payload) - {"scope_id", "after", "limit", "graph_revision", "node_id"}:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Unknown graph edge selector", 422)
        scope_id = _identifier(payload.get("scope_id"), maximum=256)
        after = payload.get("after", "")
        if after:
            _identifier(after)
        limit = payload.get("limit", 100)
        revision = payload.get("graph_revision")
        node_id = payload.get("node_id")
        if node_id is not None:
            _identifier(node_id)
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Graph edge limit is 1 to 256", 422)
        if revision is not None and (type(revision) is not int or revision < 0):
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Graph revision is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.association.read", operation_id, payload
            )
            if claims.get("source_conversation_id") is not None:
                raise ForestError(
                    "SHELL_SCOPE_DENIED", "Group commands cannot traverse the global memory graph", 403
                )
            await self.control._time(conn, claims)
        return await self.graph.read(
            lease,
            scope_id,
            graph_revision=revision,
            after=after,
            limit=limit,
            node_ids=[node_id] if node_id else None,
        )

    async def _plan_target_in(self, conn, payload):
        desired = payload.get("active")
        revision = payload.get("expected_revision")
        epoch = payload.get("expected_deletion_epoch")
        if type(desired) is not bool or type(revision) is not int or revision < 0:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Association state/revision is invalid", 422)
        if type(epoch) is not int or epoch < 0:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Deletion epoch is invalid", 422)
        if desired:
            if set(payload) != {
                "active", "expected_revision", "expected_deletion_epoch", "source_unit_ids"
            }:
                raise ForestError("MEMORY_ASSOCIATION_INVALID", "Associate plan fields differ", 422)
            ids = payload["source_unit_ids"]
            if (
                not isinstance(ids, list)
                or len(ids) != 2
                or len(set(ids)) != 2
            ):
                raise ForestError("MEMORY_ASSOCIATION_INVALID", "Select exactly two sources", 422)
            for identity in ids:
                _identifier(identity)
            ids = sorted(ids)
            association_id = _association_id(ids)
        else:
            if set(payload) != {
                "active", "expected_revision", "expected_deletion_epoch", "association_id"
            }:
                raise ForestError("MEMORY_ASSOCIATION_INVALID", "Retraction plan fields differ", 422)
            association_id = _identifier(payload["association_id"])
            head = await (await conn.execute(
                "SELECT * FROM public.bot_memory_association_heads "
                "WHERE bot_id=%s AND association_id=%s",
                (self.store.bot_id, association_id),
            )).fetchone()
            if head is None:
                raise ForestError("MEMORY_ASSOCIATION_NOT_FOUND", "Association does not exist", 404)
            ids = sorted([head["left_source_unit_id"], head["right_source_unit_id"]])
        head = await (await conn.execute(
            "SELECT * FROM public.bot_memory_association_heads "
            "WHERE bot_id=%s AND association_id=%s",
            (self.store.bot_id, association_id),
        )).fetchone()
        current_revision = head["revision"] if head else 0
        current_active = head["active"] if head else False
        if revision != current_revision:
            raise ForestError("ASSOCIATION_REVISION_CONFLICT", "Association revision changed", 409)
        if desired == current_active:
            raise ForestError("ASSOCIATION_NO_CHANGE", "Association already has this state", 409)
        view = await self.sources.authorize_sources_in(
            conn, source_unit_ids=ids, limit=2, include_payload=True
        )
        selected = {row["source_unit_id"]: row for row in view["sources"]}
        if set(selected) != set(ids):
            raise ForestError(
                "MEMORY_ASSOCIATION_SOURCE_UNAVAILABLE",
                "Association endpoint is unavailable",
                404,
            )
        if view["deletion_epoch"] != epoch:
            raise ForestError("DELETION_CONFLICT", "Memory deletion epoch changed", 409)
        scope = _scope([selected[identity] for identity in ids])
        if head is not None and head["scope_id"] != scope.scope_id:
            raise ForestError("MEMORY_ASSOCIATION_CONFLICT", "Association scope changed", 409)
        refs = [
            asdict(SourceRef(identity, selected[identity]["source_revision"]))
            for identity in ids
        ]
        return association_id, ids, selected, scope, refs

    async def plan(self, ticket, operation_id, payload):
        payload = _clone(payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.association.plan", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            association_id, ids, selected, scope, refs = await self._plan_target_in(conn, payload)
            plan_id = str(uuid4())
            frozen = {
                "bot_id": self.store.bot_id,
                "subject_id": claims["subject_id"],
                "issuer_id": claims["issuer_id"],
                "association_id": association_id,
                "expected_revision": payload["expected_revision"],
                "expected_deletion_epoch": payload["expected_deletion_epoch"],
                "desired_active": payload["active"],
                "source_refs": refs,
                "scope": asdict(scope),
            }
            plan_digest = digest_json(frozen)
            expires = await self.store._now(conn) + timedelta(minutes=10)
            await conn.execute(
                "INSERT INTO public.bot_memory_association_plans("
                "bot_id,plan_id,operation_id,subject_id,issuer_id,association_id,expected_revision,"
                "expected_deletion_epoch,desired_active,source_refs,scope,plan_digest,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, plan_id, operation_id, claims["subject_id"],
                    claims["issuer_id"], association_id, payload["expected_revision"],
                    payload["expected_deletion_epoch"], payload["active"], Jsonb(refs),
                    Jsonb(asdict(scope)), plan_digest, expires,
                ),
            )
            result = {
                "plan_id": plan_id,
                "plan_digest": plan_digest,
                "association_id": association_id,
                "expected_revision": payload["expected_revision"],
                "expected_deletion_epoch": payload["expected_deletion_epoch"],
                "active": payload["active"],
                "expires_at": expires.isoformat(),
                "left": MemoryAdministration._public_source(selected[ids[0]]),
                "right": MemoryAdministration._public_source(selected[ids[1]]),
            }
            await self.control._record(
                conn, claims, result,
                parents=tuple(selected[identity]["payload_id"] for identity in ids),
            )
            return result

    async def apply(self, ticket, operation_id, payload, *, lease):
        payload = _clone(payload)
        if set(payload) != {"expected_deletion_epoch", "plan_id", "plan_digest"}:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Association apply fields differ", 422)
        epoch = payload["expected_deletion_epoch"]
        if type(epoch) is not int or epoch < 0:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Deletion epoch is invalid", 422)
        _identifier(payload["plan_id"], maximum=128)
        _identifier(payload["plan_digest"], maximum=128)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "memory.association.apply", operation_id, payload,
                payload["plan_digest"],
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            plan = await (await conn.execute(
                "SELECT * FROM public.bot_memory_association_plans "
                "WHERE bot_id=%s AND plan_id=%s FOR SHARE",
                (self.store.bot_id, payload["plan_id"]),
            )).fetchone()
            if plan is None or any(plan[key] != claims[key] for key in ("subject_id", "issuer_id")):
                raise ForestError("PLAN_NOT_FOUND", "Association plan is unavailable", 404)
            if (
                plan["plan_digest"] != payload["plan_digest"]
                or plan["expected_deletion_epoch"] != epoch
            ):
                raise ForestError("PLAN_CONFLICT", "Association plan does not match", 409)
            if plan["expires_at"] <= await self.store._now(conn):
                raise ForestError("PLAN_EXPIRED", "Preview the association again", 409)
            refs = tuple(SourceRef(**value) for value in plan["source_refs"])
            ids = [ref.source_unit_id for ref in refs]
            view = await self.sources.authorize_sources_in(
                conn, source_unit_ids=ids, limit=2, include_payload=False
            )
            selected = {row["source_unit_id"]: row for row in view["sources"]}
            if set(selected) != set(ids) or any(
                selected[ref.source_unit_id]["source_revision"] != ref.source_revision
                for ref in refs
            ):
                raise ForestError(
                    "MEMORY_ASSOCIATION_SOURCE_CHANGED",
                    "Association endpoint changed or became unavailable",
                    409,
                )
            scope = _saved_scope(plan["scope"])
            if _scope([selected[identity] for identity in sorted(ids)]) != scope:
                raise ForestError("MEMORY_ASSOCIATION_SOURCE_CHANGED", "Association scope changed", 409)
            # PostgreSQL cannot row-lock a head that does not exist yet. Serialize
            # the deterministic association identity so two reviewed create plans
            # converge through the same revision check instead of a raw unique-key
            # failure (and use the same lock for later state transitions).
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (self.store.bot_id + "\x1f" + plan["association_id"],),
            )
            head = await (await conn.execute(
                "SELECT * FROM public.bot_memory_association_heads "
                "WHERE bot_id=%s AND association_id=%s FOR UPDATE",
                (self.store.bot_id, plan["association_id"]),
            )).fetchone()
            current_revision = head["revision"] if head else 0
            current_active = head["active"] if head else False
            if current_revision != plan["expected_revision"]:
                raise ForestError("ASSOCIATION_REVISION_CONFLICT", "Association revision changed", 409)
            if current_active == plan["desired_active"]:
                raise ForestError("ASSOCIATION_NO_CHANGE", "Association already has this state", 409)
            revision = current_revision + 1
            graph_operation = "association-op:" + digest_json(
                [self.store.bot_id, operation_id]
            ).split(":", 1)[1]
            contribution = None
            if plan["desired_active"]:
                contribution = plan["association_id"] + ":revision:" + str(revision)
                command = RelationContribution(
                    self.store.bot_id,
                    graph_operation,
                    contribution,
                    "ASSOCIATES_WITH",
                    NodeRef(self.store.bot_id, "SOURCE_UNIT", ids[0]),
                    NodeRef(self.store.bot_id, "SOURCE_UNIT", ids[1]),
                    refs,
                    RELATION_KEY,
                )
            else:
                command = Retraction(
                    self.store.bot_id,
                    graph_operation,
                    "contribution",
                    head["current_contribution_id"],
                )
            await self.graph.append_in(
                conn, lease, scope, command, expected_deletion_epoch=epoch
            )
            if head is None:
                await conn.execute(
                    "INSERT INTO public.bot_memory_association_heads("
                    "bot_id,association_id,left_source_unit_id,right_source_unit_id,scope_id,revision,"
                    "active,current_contribution_id) VALUES (%s,%s,%s,%s,%s,%s,true,%s)",
                    (
                        self.store.bot_id, plan["association_id"], ids[0], ids[1],
                        scope.scope_id, revision, contribution,
                    ),
                )
            else:
                updated = await (await conn.execute(
                    "UPDATE public.bot_memory_association_heads SET revision=%s,active=%s,"
                    "current_contribution_id=%s WHERE bot_id=%s AND association_id=%s "
                    "AND revision=%s RETURNING association_id",
                    (
                        revision, plan["desired_active"], contribution, self.store.bot_id,
                        plan["association_id"], current_revision,
                    ),
                )).fetchone()
                if updated is None:
                    raise ForestError(
                        "ASSOCIATION_REVISION_CONFLICT", "Association revision changed", 409
                    )
            await conn.execute(
                "INSERT INTO public.bot_memory_association_changes("
                "bot_id,association_id,revision,operation_id,active,graph_operation_id,contribution_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, plan["association_id"], revision, operation_id,
                    plan["desired_active"], graph_operation, contribution,
                ),
            )
            result = {
                "operation_id": operation_id,
                "association_id": plan["association_id"],
                "revision": revision,
                "active": plan["desired_active"],
                "deletion_epoch": epoch,
                "projection_state": "PENDING",
            }
            await self.control._record(conn, claims, result)
            return result

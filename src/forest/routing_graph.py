"""Authoritative, explainable routing graph compiled from durable registry heads."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import contract_matches, digest_json
from forest.storage.control import BotControl


def _iso(value):
    return value.isoformat() if value is not None else None


class RouteGraphStore:
    protocol = "forest.route-graph/1"

    def __init__(self, store, *, control=None):
        self.store = store
        self.control = control or BotControl(store)

    async def _rows(self, conn, sql: str):
        return await (await conn.execute(sql, (self.store.bot_id,))).fetchall()

    async def _route_rows_in(self, conn):
        return await self._rows(
            conn,
            "SELECT r.component_id,r.generation_id,r.route_epoch,r.admission,r.control_admission,"
            "g.status AS generation_status,g.package_digest,g.worker_incarnation "
            "FROM public.bot_component_routes r LEFT JOIN public.bot_generations g "
            "ON g.bot_id=r.bot_id AND g.generation_id=r.generation_id "
            "WHERE r.bot_id=%s ORDER BY r.component_id",
        )

    async def _snapshot_in(self, conn, *, routes=None, clocks=None) -> dict:
        routes = routes if routes is not None else await self._route_rows_in(conn)
        services = await self._rows(
            conn,
            "SELECT service_name,contract_major,contract_minor,component_id,generation_id,"
            "route_epoch,registry_revision,input_schema_digest,output_schema_digest "
            "FROM public.bot_service_heads WHERE bot_id=%s AND active=true "
            "ORDER BY service_name,contract_major",
        )
        requirements = await self._rows(
            conn,
            "SELECT d.component_id,d.generation_id,d.service_name,d.contract_range,d.required,"
            "d.startup_dependency FROM public.bot_generation_service_requirements d "
            "JOIN public.bot_component_routes r ON r.bot_id=d.bot_id "
            "AND r.component_id=d.component_id AND r.generation_id=d.generation_id "
            "WHERE d.bot_id=%s ORDER BY d.component_id,d.service_name",
        )
        commands = await self._rows(
            conn,
            "SELECT command_id,name,component_id,generation_id,route_epoch,registry_revision,"
            "permission,argument_mode,priority,schema_digest,spec_digest "
            "FROM public.bot_command_heads WHERE bot_id=%s AND active=true "
            "ORDER BY name,command_id",
        )
        handlers = await self._rows(
            conn,
            "SELECT handler_id,handler,component_id,generation_id,route_epoch,registry_revision,"
            "priority,spec_digest FROM public.bot_message_handler_heads "
            "WHERE bot_id=%s AND active=true ORDER BY priority DESC,component_id,handler_id",
        )
        publications = await self._rows(
            conn,
            "SELECT d.definition_id,d.topic,d.contract_major,d.contract_minor,d.schema_digest,"
            "d.component_id,d.generation_id,r.route_epoch "
            "FROM public.bot_generation_event_definitions d "
            "JOIN public.bot_component_routes r ON r.bot_id=d.bot_id "
            "AND r.component_id=d.component_id AND r.generation_id=d.generation_id "
            "WHERE d.bot_id=%s AND d.direction='PUBLISH' "
            "ORDER BY d.topic,d.contract_major,d.contract_minor,d.component_id",
        )
        subscriptions = await self._rows(
            conn,
            "SELECT h.subscriber_id,h.topic,h.contract_major,h.contract_minor,h.schema_digest,"
            "h.component_id,h.generation_id,h.max_attempts,r.route_epoch "
            "FROM public.bot_event_subscription_heads h "
            "JOIN public.bot_component_routes r ON r.bot_id=h.bot_id "
            "AND r.component_id=h.component_id AND r.generation_id=h.generation_id "
            "WHERE h.bot_id=%s AND h.active=true "
            "ORDER BY h.topic,h.contract_major,h.contract_minor,h.subscriber_id",
        )
        jobs = await self._rows(
            conn,
            "SELECT h.component_id,h.handler_id,h.generation_id,h.schema_digest,h.max_attempts,"
            "h.timeout_seconds,h.max_concurrency,r.route_epoch "
            "FROM public.bot_component_job_heads h JOIN public.bot_component_routes r "
            "ON r.bot_id=h.bot_id AND r.component_id=h.component_id "
            "AND r.generation_id=h.generation_id WHERE h.bot_id=%s AND h.active=true "
            "ORDER BY h.component_id,h.handler_id",
        )
        runtime_modules = await self._rows(
            conn,
            "SELECT m.component_id,m.generation_id,m.graph_id,m.descriptor_digest,"
            "g.descriptor_set_digest,g.host_id,g.host_incarnation,g.host_owner_epoch,"
            "l.state AS lifecycle_state FROM public.bot_runtime_graph_heads h "
            "JOIN public.bot_runtime_graphs g ON g.bot_id=h.bot_id AND g.graph_id=h.graph_id "
            "JOIN public.bot_runtime_module_generations m ON m.bot_id=g.bot_id "
            "AND m.graph_id=g.graph_id LEFT JOIN LATERAL (SELECT f.state FROM "
            "public.bot_runtime_module_lifecycle_facts f WHERE f.bot_id=m.bot_id "
            "AND f.generation_id=m.generation_id ORDER BY f.sequence DESC LIMIT 1) l ON true "
            "WHERE h.bot_id=%s AND h.graph_id IS NOT NULL ORDER BY m.component_id",
        )
        runtime_dependencies = await self._rows(
            conn,
            "SELECT d.source_component_id,d.source_generation_id,d.target_component_id,"
            "d.target_generation_id,d.graph_id FROM public.bot_runtime_module_dependencies d "
            "JOIN public.bot_runtime_graph_heads h ON h.bot_id=d.bot_id AND h.graph_id=d.graph_id "
            "WHERE d.bot_id=%s ORDER BY d.source_component_id,d.target_component_id",
        )
        if clocks is None:
            clocks = {}
            for key, table in (
                ("service", "bot_service_clock"),
                ("command", "bot_command_clocks"),
                ("message_handler", "bot_message_handler_clocks"),
                ("runtime", "bot_runtime_graph_clock"),
            ):
                row = await (
                    await conn.execute(
                        f"SELECT revision FROM public.{table} WHERE bot_id=%s",
                        (self.store.bot_id,),
                    )
                ).fetchone()
                clocks[key] = row["revision"] if row else 0

        nodes = []
        for row in routes:
            nodes.append({
                "node_id": "component:" + row["component_id"],
                "kind": "component",
                **row,
            })
        for row in runtime_modules:
            nodes.append({
                "node_id": "runtime:" + row["component_id"],
                "kind": "runtime_module",
                **row,
            })
        for row in services:
            nodes.append({
                "node_id": f'service:{row["service_name"]}:v{row["contract_major"]}',
                "kind": "service",
                "contract": f'{row["contract_major"]}.{row["contract_minor"]}',
                **row,
            })
        for row in commands:
            nodes.append({"node_id": "command:" + row["command_id"], "kind": "command", **row})
        for row in handlers:
            nodes.append({
                "node_id": f'message:{row["component_id"]}:{row["handler_id"]}',
                "kind": "message_handler",
                **row,
            })
        for row in publications:
            nodes.append({
                "node_id": f'event-publisher:{row["component_id"]}:{row["definition_id"]}',
                "kind": "event_publisher",
                "contract": f'{row["contract_major"]}.{row["contract_minor"]}',
                **row,
            })
        for row in subscriptions:
            nodes.append({
                "node_id": f'event-subscriber:{row["subscriber_id"]}:v{row["contract_major"]}',
                "kind": "event_subscriber",
                "contract": f'{row["contract_major"]}.{row["contract_minor"]}',
                **row,
            })
        for row in jobs:
            row = {**row, "timeout_seconds": float(row["timeout_seconds"])}
            nodes.append({
                "node_id": f'job:{row["component_id"]}:{row["handler_id"]}',
                "kind": "job_handler",
                **row,
            })

        edges = []
        for node in nodes:
            if node["kind"] not in {"component", "runtime_module"}:
                edges.append({
                    "edge_id": f'owned-by:{node["node_id"]}',
                    "kind": "owned_by",
                    "source": node["node_id"],
                    "target": "component:" + node["component_id"],
                    "status": "RESOLVED",
                })
        service_by_name = {}
        for row in services:
            service_by_name.setdefault(row["service_name"], []).append(row)
        for requirement in requirements:
            matches = [
                row for row in service_by_name.get(requirement["service_name"], [])
                if contract_matches(
                    requirement["contract_range"],
                    f'{row["contract_major"]}.{row["contract_minor"]}',
                )
            ]
            target = None
            status = "MISSING"
            if len(matches) == 1:
                target = f'service:{matches[0]["service_name"]}:v{matches[0]["contract_major"]}'
                status = "RESOLVED"
            elif matches:
                status = "AMBIGUOUS"
            elif service_by_name.get(requirement["service_name"]):
                status = "INCOMPATIBLE"
            edges.append({
                "edge_id": f'requires:{requirement["component_id"]}:{requirement["service_name"]}',
                "kind": "requires_service",
                "source": "component:" + requirement["component_id"],
                "target": target,
                "status": status,
                **requirement,
            })
        for dependency in runtime_dependencies:
            edges.append({
                "edge_id": (
                    "runtime-requires:"
                    + dependency["source_component_id"]
                    + ":"
                    + dependency["target_component_id"]
                ),
                "kind": "requires_component",
                "source": "runtime:" + dependency["source_component_id"],
                "target": "runtime:" + dependency["target_component_id"],
                "status": "RESOLVED",
                **dependency,
            })
        for publication in publications:
            source = f'event-publisher:{publication["component_id"]}:{publication["definition_id"]}'
            for subscription in subscriptions:
                if all(publication[key] == subscription[key] for key in (
                    "topic", "contract_major", "contract_minor", "schema_digest"
                )):
                    edges.append({
                        "edge_id": f'event:{source}:{subscription["subscriber_id"]}',
                        "kind": "event_delivery",
                        "source": source,
                        "target": f'event-subscriber:{subscription["subscriber_id"]}:v{subscription["contract_major"]}',
                        "status": "RESOLVED",
                    })
        nodes.sort(key=lambda row: row["node_id"])
        edges.sort(key=lambda row: row["edge_id"])
        graph = {
            "protocol": self.protocol,
            "bot_id": self.store.bot_id,
            "revisions": clocks,
            "nodes": nodes,
            "edges": edges,
        }
        return {**graph, "graph_digest": digest_json(graph)}

    async def snapshot(self) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            await self.store._identity(conn)
            return await self._snapshot_in(conn)

    async def freeze_in(self, conn) -> dict:
        """Persist the exact graph visible to an admitting transaction."""
        # Message admission already holds the Bot runtime fence. Registry
        # clocks close policy/registry writers that do not require that fence;
        # use lifecycle publication order to avoid a cross-family deadlock.
        clocks = {}
        for key, table in (
            ("command", "bot_command_clocks"),
            ("message_handler", "bot_message_handler_clocks"),
            ("service", "bot_service_clock"),
            ("runtime", "bot_runtime_graph_clock"),
        ):
            row = await (
                await conn.execute(
                    f"SELECT revision FROM public.{table} WHERE bot_id=%s FOR SHARE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            if row is None:
                await conn.execute(
                    f"INSERT INTO public.{table}(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (self.store.bot_id,),
                )
                row = await (
                    await conn.execute(
                        f"SELECT revision FROM public.{table} WHERE bot_id=%s FOR SHARE",
                        (self.store.bot_id,),
                    )
                ).fetchone()
            clocks[key] = row["revision"]
        routes = await self._route_rows_in(conn)
        route_digest = digest_json(routes)
        cached = await (
            await conn.execute(
                "SELECT snapshot FROM public.bot_route_graph_snapshots WHERE bot_id=%s "
                "AND route_digest=%s AND service_revision=%s AND command_revision=%s "
                "AND message_handler_revision=%s AND runtime_revision=%s",
                (
                    self.store.bot_id,
                    route_digest,
                    clocks["service"],
                    clocks["command"],
                    clocks["message_handler"],
                    clocks["runtime"],
                ),
            )
        ).fetchone()
        if cached is not None:
            return cached["snapshot"]
        snapshot = await self._snapshot_in(conn, routes=routes, clocks=clocks)
        from psycopg.types.json import Jsonb

        inserted = await (
            await conn.execute(
                "INSERT INTO public.bot_route_graph_snapshots("
                "bot_id,graph_digest,protocol,route_digest,service_revision,command_revision,"
                "message_handler_revision,runtime_revision,snapshot) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING graph_digest",
                (
                    self.store.bot_id,
                    snapshot["graph_digest"],
                    snapshot["protocol"],
                    route_digest,
                    clocks["service"],
                    clocks["command"],
                    clocks["message_handler"],
                    clocks["runtime"],
                    Jsonb(snapshot),
                ),
            )
        ).fetchone()
        if inserted is None:
            stored = await (
                await conn.execute(
                    "SELECT protocol,snapshot FROM public.bot_route_graph_snapshots "
                    "WHERE bot_id=%s AND graph_digest=%s",
                    (self.store.bot_id, snapshot["graph_digest"]),
                )
            ).fetchone()
            if stored is None or stored["protocol"] != snapshot["protocol"] or stored["snapshot"] != snapshot:
                raise ForestError(
                    "ROUTE_GRAPH_DIGEST_CONFLICT",
                    "A route graph digest identifies different snapshot bytes",
                    409,
                )
        return snapshot

    async def read(self, ticket, operation_id: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            claims = await self.control._guard(conn, ticket, "module.read", operation_id, {})
            result = await self._snapshot_in(conn)
            await self.control._time(conn, claims)
            return result

    async def read_version(self, ticket, operation_id: str, graph_digest: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "module.read", operation_id, {"graph_digest": graph_digest}
            )
            row = await (
                await conn.execute(
                    "SELECT snapshot,captured_at FROM public.bot_route_graph_snapshots "
                    "WHERE bot_id=%s AND graph_digest=%s",
                    (self.store.bot_id, graph_digest),
                )
            ).fetchone()
            if row is None:
                raise ForestError("ROUTE_GRAPH_SNAPSHOT_NOT_FOUND", "Route graph snapshot not found", 404)
            await self.control._time(conn, claims)
            return {**row["snapshot"], "captured_at": _iso(row["captured_at"])}


class RouteGraphRuntime:
    def __init__(self, runtime):
        from forest.dispatch_trace import DispatchTraceStore
        from forest.storage.component_jobs import ComponentJobStore
        from forest.storage.events import EventStore

        self.graph = RouteGraphStore(
            runtime.bot.store, control=getattr(runtime.bot, "control", None)
        )
        self.traces = DispatchTraceStore(
            runtime.bot.store, control=getattr(runtime.bot, "control", None)
        )
        self.events = EventStore(
            runtime.bot.store, control=getattr(runtime.bot, "control", None)
        )
        self.component_jobs = ComponentJobStore(
            runtime.bot.store, control=getattr(runtime.bot, "control", None)
        )

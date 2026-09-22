"""Atomic publication of one immutable RuntimeGraph routing directory."""

from __future__ import annotations

from dataclasses import asdict

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.routing_graph_types import (
    _bounded,
    _digest,
    _validated_unique,
)


class RoutingDirectoryCommands:
    def __init__(self, store, *, namespace: str | None = None) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.routing.graph")

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    async def publish_directory_in(
        self,
        conn,
        *,
        graph_id: str,
        graph_digest: str,
        routing_generation_id: str,
        publications=(),
        subscriptions=(),
        component_jobs=(),
    ) -> str:
        _bounded(graph_id, "graph_id", 512)
        _digest(graph_digest, "graph_digest")
        _bounded(routing_generation_id, "routing_generation_id", 512)
        publications = _validated_unique(
            publications,
            lambda item: (
                item.publisher_component_id,
                item.topic,
                item.contract,
            ),
            "Event publication routes are ambiguous",
        )
        subscriptions = _validated_unique(
            subscriptions,
            lambda item: (item.subscriber_id, item.contract_major),
            "Event subscription routes are ambiguous",
        )
        component_jobs = _validated_unique(
            component_jobs,
            lambda item: (item.owner_component_id, item.handler_id),
            "ComponentJob routes are ambiguous",
        )
        event_schemas: dict[tuple[str, str], str] = {}
        for route in publications:
            key = (route.topic, route.contract)
            existing = event_schemas.setdefault(key, route.event_schema_digest)
            if existing != route.event_schema_digest:
                raise ForestError(
                    "ROUTING_DIRECTORY_CONFLICT",
                    "Publishers disagree on one exact event contract schema",
                    409,
                )
        for route in subscriptions:
            published = event_schemas.get((route.topic, route.contract))
            if published is not None and published != route.event_schema_digest:
                raise ForestError(
                    "ROUTING_DIRECTORY_CONFLICT",
                    "Subscriber schema differs from the exact event contract",
                    409,
                )
        directory = {
            "protocol": "forest.routing-directory/1",
            "graph_id": graph_id,
            "graph_digest": graph_digest,
            "routing_generation_id": routing_generation_id,
            "publications": [asdict(item) for item in publications],
            "subscriptions": [asdict(item) for item in subscriptions],
            "component_jobs": [asdict(item) for item in component_jobs],
        }
        directory_digest = digest_json(directory)
        existing = await (
            await conn.execute(
                sql.SQL(
                    "SELECT directory_digest FROM {} WHERE bot_id=%s AND graph_id=%s"
                ).format(self._table("route_publications")),
                (self.store.bot_id, graph_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["directory_digest"] != directory_digest:
                raise ForestError(
                    "ROUTING_DIRECTORY_CONFLICT",
                    "RuntimeGraph routing directory differs",
                    409,
                )
            return directory_digest
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,graph_id,routing_generation_id,graph_digest,"
                "directory_digest) VALUES (%s,%s,%s,%s,%s)"
            ).format(self._table("route_publications")),
            (
                self.store.bot_id,
                graph_id,
                routing_generation_id,
                graph_digest,
                directory_digest,
            ),
        )
        for route in publications:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,publisher_component_id,"
                    "publisher_generation_id,topic,contract,event_schema_digest) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("event_publication_routes")),
                (
                    self.store.bot_id,
                    graph_id,
                    route.publisher_component_id,
                    route.publisher_generation_id,
                    route.topic,
                    route.contract,
                    route.event_schema_digest,
                ),
            )
        for route in subscriptions:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,subscriber_id,contract_major,topic,"
                    "contract,event_schema_digest,owner_component_id,owner_generation_id,"
                    "handler_service_id,handler_service_version,max_attempts) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("event_subscription_routes")),
                (
                    self.store.bot_id,
                    graph_id,
                    route.subscriber_id,
                    route.contract_major,
                    route.topic,
                    route.contract,
                    route.event_schema_digest,
                    route.owner_component_id,
                    route.owner_generation_id,
                    route.handler_service_id,
                    route.handler_service_version,
                    route.max_attempts,
                ),
            )
        for route in component_jobs:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,owner_component_id,owner_generation_id,"
                    "handler_id,input_schema_digest,handler_service_id,"
                    "handler_service_version,max_attempts,timeout_seconds,max_concurrency) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("component_job_routes")),
                (
                    self.store.bot_id,
                    graph_id,
                    route.owner_component_id,
                    route.owner_generation_id,
                    route.handler_id,
                    route.input_schema_digest,
                    route.handler_service_id,
                    route.handler_service_version,
                    route.max_attempts,
                    route.timeout_seconds,
                    route.max_concurrency,
                ),
            )
        return directory_digest


"""Persistent route directory and Event commands owned by ``routing.graph``."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.payload_authority import PayloadSource

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_CONTRACT = re.compile(r"[1-9][0-9]*\.[0-9]+\Z")
_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")


def _bounded(value: object, field: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("ROUTING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ForestError("ROUTING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ForestError("ROUTING_INPUT_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True)
class EventPublicationRoute:
    publisher_component_id: str
    publisher_generation_id: str
    topic: str
    contract: str
    event_schema_digest: str

    def validate(self) -> None:
        _identifier(self.publisher_component_id, "publisher_component_id")
        _bounded(self.publisher_generation_id, "publisher_generation_id", 512)
        _identifier(self.topic, "topic")
        if _CONTRACT.fullmatch(self.contract) is None:
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid event contract", 422)
        _digest(self.event_schema_digest, "event_schema_digest")


@dataclass(frozen=True)
class EventSubscriptionRoute:
    subscriber_id: str
    contract_major: int
    topic: str
    contract: str
    event_schema_digest: str
    owner_component_id: str
    owner_generation_id: str
    handler_service_id: str
    handler_service_version: int
    max_attempts: int

    def validate(self) -> None:
        _identifier(self.subscriber_id, "subscriber_id")
        _identifier(self.topic, "topic")
        _identifier(self.owner_component_id, "owner_component_id")
        _identifier(self.handler_service_id, "handler_service_id")
        _bounded(self.owner_generation_id, "owner_generation_id", 512)
        _digest(self.event_schema_digest, "event_schema_digest")
        if (
            type(self.contract_major) is not int
            or self.contract_major <= 0
            or _CONTRACT.fullmatch(self.contract) is None
            or int(self.contract.split(".", 1)[0]) != self.contract_major
            or type(self.handler_service_version) is not int
            or self.handler_service_version <= 0
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
        ):
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid subscription route", 422)


@dataclass(frozen=True)
class ComponentJobRoute:
    owner_component_id: str
    owner_generation_id: str
    handler_id: str
    input_schema_digest: str
    handler_service_id: str
    handler_service_version: int
    max_attempts: int
    timeout_seconds: float
    max_concurrency: int

    def validate(self) -> None:
        for value, field in (
            (self.owner_component_id, "owner_component_id"),
            (self.handler_id, "handler_id"),
            (self.handler_service_id, "handler_service_id"),
        ):
            _identifier(value, field)
        _bounded(self.owner_generation_id, "owner_generation_id", 512)
        _digest(self.input_schema_digest, "input_schema_digest")
        if (
            type(self.handler_service_version) is not int
            or self.handler_service_version <= 0
            or type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 32
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0 < self.timeout_seconds <= 120
            or type(self.max_concurrency) is not int
            or not 1 <= self.max_concurrency <= 16
        ):
            raise ForestError("ROUTING_INPUT_INVALID", "Invalid ComponentJob route", 422)


def _validated_unique(items, key, message: str):
    values = tuple(items)
    identities = [key(item) for item in values]
    if len(identities) != len(set(identities)):
        raise ForestError("ROUTING_DIRECTORY_CONFLICT", message, 409)
    for item in values:
        item.validate()
    return tuple(sorted(values, key=key))


class RoutingGraphCommands:
    """Mutate only routing-owned facts under exact graph and Generation fences."""

    def __init__(
        self,
        store,
        *,
        payloads,
        generation_member_verifier,
        namespace: str | None = None,
    ) -> None:
        self.store = store
        self.payloads = payloads
        self.generation_member_verifier = generation_member_verifier
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

    async def publish_event(
        self,
        *,
        graph_id: str,
        publisher_component_id: str,
        publisher_generation_id: str,
        topic: str,
        contract: str,
        event_schema_digest: str,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
        payload: Any,
        subscriptions: tuple[EventSubscriptionRoute, ...],
    ) -> dict:
        publication = EventPublicationRoute(
            publisher_component_id,
            publisher_generation_id,
            topic,
            contract,
            event_schema_digest,
        )
        publication.validate()
        _bounded(graph_id, "graph_id", 512)
        _bounded(operation_id, "operation_id", 256)
        _bounded(causal_root_kind, "causal_root_kind", 128)
        _bounded(causal_root_id, "causal_root_id", 256)
        subscriptions = _validated_unique(
            subscriptions,
            lambda item: (item.subscriber_id, item.contract_major),
            "Event subscriptions are ambiguous",
        )
        input_digest = digest_json(
            {
                "graph_id": graph_id,
                "publication": asdict(publication),
                "causal_root_kind": causal_root_kind,
                "causal_root_id": causal_root_id,
                "payload": payload,
                "subscriptions": [asdict(item) for item in subscriptions],
            }
        )
        event_id = str(
            uuid5(
                NAMESPACE_URL,
                f"forest:{self.store.bot_id}:event:{publisher_component_id}:{operation_id}",
            )
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            accepted = await self.generation_member_verifier(
                conn,
                component_id=publisher_component_id,
                generation_id=publisher_generation_id,
            )
            if accepted.graph_id != graph_id:
                raise ForestError("ROUTING_GRAPH_STALE", "Event graph is no longer a member", 409)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (
                    f"forest.routing.graph/event/{self.store.bot_id}/"
                    f"{publisher_component_id}/{operation_id}",
                ),
            )
            existing = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT event_id,input_digest FROM {} WHERE bot_id=%s "
                        "AND publisher_component_id=%s AND operation_id=%s"
                    ).format(self._table("component_events")),
                    (self.store.bot_id, publisher_component_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest:
                    raise ForestError(
                        "EVENT_OPERATION_CONFLICT", "Event operation input differs", 409
                    )
                return {"event_id": existing["event_id"], "replayed": True}
            stored_publication = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT publisher_generation_id,event_schema_digest FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s AND publisher_component_id=%s "
                        "AND topic=%s AND contract=%s"
                    ).format(self._table("event_publication_routes")),
                    (
                        self.store.bot_id,
                        graph_id,
                        publisher_component_id,
                        topic,
                        contract,
                    ),
                )
            ).fetchone()
            if stored_publication is None or dict(stored_publication) != {
                "publisher_generation_id": publisher_generation_id,
                "event_schema_digest": event_schema_digest,
            }:
                raise ForestError(
                    "EVENT_ROUTE_UNAVAILABLE",
                    "Event publication is absent from the frozen routing directory",
                    409,
                )
            stored_subscriptions = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT subscriber_id,contract_major,topic,contract,"
                        "event_schema_digest,owner_component_id,owner_generation_id,"
                        "handler_service_id,handler_service_version,max_attempts FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s AND topic=%s AND contract=%s "
                        "ORDER BY subscriber_id,contract_major"
                    ).format(self._table("event_subscription_routes")),
                    (self.store.bot_id, graph_id, topic, contract),
                )
            ).fetchall()
            if [dict(row) for row in stored_subscriptions] != [
                asdict(item) for item in subscriptions
            ]:
                raise ForestError(
                    "EVENT_ROUTE_STALE",
                    "Event subscription set differs from the frozen routing directory",
                    409,
                )
            deletion_epoch = await self.payloads.epoch_in(conn)
            saved = await self.payloads.put_json_in(
                conn,
                owner_component_id="routing.graph",
                owner_kind="component.event",
                owner_id=event_id,
                value=payload,
                sources=(
                    PayloadSource(
                        publisher_component_id,
                        "component.call",
                        operation_id,
                        1,
                    ),
                ),
                expected_deletion_epoch=deletion_epoch,
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,event_id,operation_id,input_digest,"
                    "publisher_component_id,publisher_generation_id,graph_id,topic,contract,"
                    "event_schema_digest,payload_id,payload_digest,causal_root_kind,"
                    "causal_root_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("component_events")),
                (
                    self.store.bot_id,
                    event_id,
                    operation_id,
                    input_digest,
                    publisher_component_id,
                    publisher_generation_id,
                    graph_id,
                    topic,
                    contract,
                    event_schema_digest,
                    saved.payload_id,
                    saved.content_digest,
                    causal_root_kind,
                    causal_root_id,
                ),
            )
            for route in subscriptions:
                delivery_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"forest:{self.store.bot_id}:event-delivery:{event_id}:"
                        f"{route.subscriber_id}:{route.contract_major}",
                    )
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,delivery_id,event_id,subscriber_id,"
                        "contract_major,topic,contract,event_schema_digest,owner_component_id,"
                        "handler_service_id,handler_service_version,max_attempts) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("event_deliveries")),
                    (
                        self.store.bot_id,
                        delivery_id,
                        event_id,
                        route.subscriber_id,
                        route.contract_major,
                        route.topic,
                        route.contract,
                        route.event_schema_digest,
                        route.owner_component_id,
                        route.handler_service_id,
                        route.handler_service_version,
                        route.max_attempts,
                    ),
                )
            return {
                "event_id": event_id,
                "payload_digest": saved.content_digest,
                "delivery_count": len(subscriptions),
                "replayed": False,
            }

"""Host binding for durable control-scope Graph changes and Bot handoff facts."""

from __future__ import annotations

import re
from uuid import UUID

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.storage.control_graph_changes import ControlGraphChangeAuthority

_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}")


def _binding_triples(value) -> tuple[tuple[str, str, str], ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError("Driver binding plan must be nonempty")
    triples = []
    for item in value:
        if (
            not isinstance(item, (list, tuple)) or len(item) != 3
            or any(not isinstance(part, str) or not part or len(part.encode("utf-8")) > 256
                   for part in item)
        ):
            raise ValueError("Driver binding plan contains an invalid triple")
        triples.append(tuple(item))
    if tuple(triples) != tuple(sorted(set(triples))):
        raise ValueError("Driver binding plan must be sorted and unique")
    return tuple(triples)


class HostGraphChangeAuthority(ControlGraphChangeAuthority):
    scope = "host"

    async def stage_consumers(
        self, lease, *, host_activation_id: str, consumers: tuple[dict, ...],
    ) -> tuple[dict, ...]:
        """Freeze the Bot handoff roster before the Host graph can publish."""

        try:
            UUID(host_activation_id)
            if not consumers or tuple(sorted(
                row["bot_id"] for row in consumers
            )) != tuple(row["bot_id"] for row in consumers):
                raise ValueError
            if len({row["bot_id"] for row in consumers}) != len(consumers):
                raise ValueError
            for row in consumers:
                UUID(row["bot_activation_id"])
                source_bindings = _binding_triples(row["source_bindings"])
                target_bindings = _binding_triples(row["target_bindings"])
                if (
                    tuple((service, kind) for service, kind, _source in source_bindings)
                    != tuple((service, kind) for service, kind, _source in target_bindings)
                    or any(old[2] == target[2] for old, target in zip(
                        source_bindings, target_bindings, strict=True
                    ))
                ):
                    raise ValueError("Driver binding plan does not change exact sources")
                if (
                    not isinstance(row["bot_id"], str)
                    or not 1 <= len(row["bot_id"].encode("utf-8")) <= 128
                    or not isinstance(row["source_bot_graph_id"], str)
                    or not 1 <= len(row["source_bot_graph_id"].encode("utf-8")) <= 256
                    or type(row["source_bot_graph_revision"]) is not int
                    or row["source_bot_graph_revision"] < 1
                ):
                    raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise ForestError(
                "HOST_DRIVER_CONSUMER_INPUT_INVALID",
                "Joint transition consumer roster is invalid", 422,
            ) from exc
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generations._lease_in(conn, lease)
            graph = await (await conn.execute(
                "SELECT state FROM forest_host.runtime_graphs "
                "WHERE host_id=%s AND activation_id=%s FOR SHARE",
                (self.scope_id, host_activation_id),
            )).fetchone()
            if graph is None or graph["state"] != "PREPARED":
                raise ForestError(
                    "HOST_DRIVER_TRANSITION_NOT_PREPARED",
                    "Host graph must be prepared before freezing consumers", 409,
                )
            for row in consumers:
                await conn.execute(
                    "INSERT INTO forest_host.driver_transition_consumers("
                    "host_id,host_activation_id,bot_id,bot_activation_id,"
                    "source_bot_graph_id,source_bot_graph_revision,"
                    "source_bindings,target_bindings) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        self.scope_id, host_activation_id, row["bot_id"],
                        row["bot_activation_id"], row["source_bot_graph_id"],
                        row["source_bot_graph_revision"],
                        Jsonb(row["source_bindings"]), Jsonb(row["target_bindings"]),
                    ),
                )
            saved = await (await conn.execute(
                "SELECT * FROM forest_host.driver_transition_consumers "
                "WHERE host_id=%s AND host_activation_id=%s ORDER BY bot_id",
                (self.scope_id, host_activation_id),
            )).fetchall()
            if len(saved) != len(consumers) or any(
                (
                    saved[index]["bot_id"],
                    saved[index]["bot_activation_id"],
                    saved[index]["source_bot_graph_id"],
                    saved[index]["source_bot_graph_revision"],
                    _binding_triples(saved[index]["source_bindings"]),
                    _binding_triples(saved[index]["target_bindings"]),
                ) != (
                    row["bot_id"], row["bot_activation_id"],
                    row["source_bot_graph_id"], row["source_bot_graph_revision"],
                    _binding_triples(row["source_bindings"]),
                    _binding_triples(row["target_bindings"]),
                )
                for index, row in enumerate(consumers)
            ):
                raise ForestError(
                    "HOST_DRIVER_CONSUMER_ROSTER_CONFLICT",
                    "Prepared Bot handoff roster differs from the frozen ledger", 409,
                )
            return tuple(dict(row) for row in saved)

    async def finish_consumer(
        self, lease, *, host_activation_id: str, bot_id: str,
        state: str, graph: dict | None = None, error_code: str | None = None,
    ) -> dict:
        """Settle one prepared Bot handoff exactly once with graph evidence."""

        if (
            state not in {"SUCCEEDED", "FAILED", "PARTIAL"}
            or (graph is None) != (state == "FAILED")
            or (error_code is None) != (state == "SUCCEEDED")
            or (
                error_code is not None
                and _ERROR_CODE.fullmatch(error_code) is None
            )
            or (
                graph is not None
                and (
                    not isinstance(graph, dict)
                    or not isinstance(graph.get("graph_id"), str)
                    or type(graph.get("graph_revision")) is not int
                    or graph["graph_revision"] < 1
                    or graph.get("state") not in {"ACTIVE", "RETIRED"}
                )
            )
        ):
            raise ForestError(
                "HOST_DRIVER_CONSUMER_RESULT_INVALID",
                "Bot handoff result requires a consistent graph proof", 422,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generations._lease_in(conn, lease)
            row = await (await conn.execute(
                "SELECT * FROM forest_host.driver_transition_consumers "
                "WHERE host_id=%s AND host_activation_id=%s AND bot_id=%s FOR UPDATE",
                (self.scope_id, host_activation_id, bot_id),
            )).fetchone()
            if row is None:
                raise ForestError(
                    "HOST_DRIVER_CONSUMER_NOT_FOUND",
                    "Bot handoff was not in the prepared roster", 404,
                )
            if graph is not None and graph.get("activation_id") != (
                row["bot_activation_id"]
            ):
                raise ForestError(
                    "HOST_DRIVER_CONSUMER_RESULT_INVALID",
                    "Bot graph proof names another activation", 409,
                )
            result_graph_id = graph["graph_id"] if graph is not None else None
            result_revision = graph["graph_revision"] if graph is not None else None
            if row["state"] != "PENDING":
                if (
                    row["state"] != state
                    or row["result_bot_graph_id"] != result_graph_id
                    or row["result_bot_graph_revision"] != result_revision
                    or row["error_code"] != error_code
                ):
                    raise ForestError(
                        "HOST_DRIVER_CONSUMER_RESULT_CONFLICT",
                        "Bot handoff already has another terminal result", 409,
                    )
                return dict(row)
            saved = await (await conn.execute(
                "UPDATE forest_host.driver_transition_consumers "
                "SET state=%s,result_bot_graph_id=%s,"
                "result_bot_graph_revision=%s,error_code=%s,"
                "updated_at=clock_timestamp() "
                "WHERE host_id=%s AND host_activation_id=%s AND bot_id=%s RETURNING *",
                (
                    state, result_graph_id, result_revision, error_code,
                    self.scope_id, host_activation_id, bot_id,
                ),
            )).fetchone()
            return dict(saved)

    async def consumer_results(self, host_activation_id: str) -> tuple[dict, ...]:
        async with self.store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT * FROM forest_host.driver_transition_consumers "
                "WHERE host_id=%s AND host_activation_id=%s ORDER BY bot_id",
                (self.scope_id, host_activation_id),
            )).fetchall()
            return tuple(dict(row) for row in rows)

    async def get(self, operation_id: str) -> dict:
        request = await super().get(operation_id)
        return {
            **request,
            "bot_results": list(await self.consumer_results(request["activation_id"])),
        }

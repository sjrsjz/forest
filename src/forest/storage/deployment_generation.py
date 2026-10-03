"""Durable deployment RuntimeGraph and Generation authority."""

from __future__ import annotations

import re

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import (
    PreparedGenerationBinding,
    RuntimeGenerationGraph,
    validate_prepared_bindings,
)
from forest.storage.deployment_catalog import DeploymentSupervisorLease

_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


async def _storage_binding_in(conn, deployment_id: str, binding: dict) -> None:
    storage = binding["storage"]
    if storage is None:
        return
    row = await (
        await conn.execute(
            "SELECT component_id,schema_version,namespace,baseline_sha256,package_digest,"
            "repository_contract,execution_profile,state FROM "
            "forest_deployment.schema_bundles WHERE deployment_id=%s AND schema_id=%s",
            (deployment_id, storage["schema_id"]),
        )
    ).fetchone()
    if row is None or any(
        str(row[key]) != value for key, value in {
            "component_id": binding["component_id"],
            "schema_version": storage["schema_version"],
            "namespace": storage["namespace"],
            "baseline_sha256": storage["baseline_sha256"],
            "package_digest": storage["package_digest"],
            "repository_contract": storage["repository_contract"],
            "execution_profile": storage["execution_profile"],
            "state": "READY",
        }.items()
    ) or (
        storage["execution_profile"] == "official" and binding["package"]["trust_level"] != "official"
    ):
        raise ForestError(
            "RUNTIME_STORAGE_BINDING_MISMATCH",
            "Generation storage differs from deployment's immutable ledger", 409,
        )


class DeploymentGenerationAuthority:
    """Fence module generations by the deployment's durable Supervisor lease."""

    def __init__(self, store) -> None:
        self.store = store

    async def verify_active_generation(
        self, *, component_id: str, generation_id: str, admission_lane: str
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.verify_active_generation_in(
                conn, component_id=component_id, generation_id=generation_id,
                admission_lane=admission_lane,
            )

    async def verify_generation_member(
        self, *, component_id: str, generation_id: str
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.verify_generation_member_in(
                conn, component_id=component_id, generation_id=generation_id,
            )

    async def _lease_in(self, conn, lease: DeploymentSupervisorLease) -> None:
        if type(lease) is not DeploymentSupervisorLease or lease.deployment_id != self.store.deployment_id:
            raise ForestError("STALE_DEPLOYMENT_SUPERVISOR", "Supervisor lease is invalid", 409)
        row = await (
            await conn.execute(
                "SELECT 1 FROM forest_deployment.supervisor_lease WHERE singleton "
                "AND host_id=%s AND incarnation=%s AND owner_epoch=%s "
                "AND expires_at>clock_timestamp() FOR SHARE",
                (lease.host_id, lease.incarnation, lease.owner_epoch),
            )
        ).fetchone()
        if row is None:
            raise ForestError("STALE_DEPLOYMENT_SUPERVISOR", "Supervisor lease is stale", 409)

    async def _clock_in(self, conn, *, update: bool = False) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.runtime_graph_clock WHERE deployment_id=%s "
                + ("FOR UPDATE" if update else "FOR SHARE"),
                (self.store.deployment_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("DEPLOYMENT_RUNTIME_NOT_INITIALIZED", "Graph clock is absent", 503)
        return dict(row)

    async def _graph_in(self, conn, graph: RuntimeGenerationGraph, *, update=False) -> dict:
        if not isinstance(graph, RuntimeGenerationGraph):
            raise ForestError("RUNTIME_GRAPH_IDENTITY_CONFLICT", "Graph identity is invalid", 409)
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.runtime_graphs WHERE deployment_id=%s "
                "AND graph_id=%s " + ("FOR UPDATE" if update else "FOR SHARE"),
                (self.store.deployment_id, graph.graph_id),
            )
        ).fetchone()
        if row is None or any((
            row["graph_revision"] != graph.graph_revision,
            row["graph_digest"] != graph.graph_digest,
            row["activation_id"] != graph.activation_id,
        )):
            raise ForestError("RUNTIME_GRAPH_IDENTITY_CONFLICT", "Graph identity differs", 409)
        members = await (
            await conn.execute(
                "SELECT component_id,generation_id FROM forest_deployment.runtime_graph_members "
                "WHERE deployment_id=%s AND graph_id=%s ORDER BY component_id",
                (self.store.deployment_id, graph.graph_id),
            )
        ).fetchall()
        if {row["component_id"]: row["generation_id"] for row in members} != graph.generations:
            raise ForestError("RUNTIME_GRAPH_IDENTITY_CONFLICT", "Graph members differ", 409)
        return dict(row)

    async def prepare_graph(
        self,
        lease: DeploymentSupervisorLease,
        bindings: tuple[PreparedGenerationBinding, ...] | list[PreparedGenerationBinding],
        activation_id: str,
        *,
        retained_generations: dict[str, str] | None = None,
        source_graph: RuntimeGenerationGraph | None = None,
    ) -> RuntimeGenerationGraph:
        retained = dict(retained_generations or {})
        if (bool(retained) != (source_graph is not None)) or (
            source_graph is not None and not isinstance(source_graph, RuntimeGenerationGraph)
        ):
            raise ForestError(
                "RUNTIME_RETAINED_GENERATION_INVALID", "Retained members require an exact source graph", 422
            )
        ordered = validate_prepared_bindings(
            bindings, activation_id, available_components=set(retained)
        )
        if set(retained) & {item.component_id for item in ordered}:
            raise ForestError(
                "RUNTIME_RETAINED_GENERATION_INVALID", "A member cannot be retained and rebuilt", 422
            )
        graph_digest = digest_json({
            "protocol": "forest.runtime-graph/1",
            "members": [item.as_fact() for item in ordered],
            **({"retained_from": source_graph.graph_id, "retained": sorted(retained.items())}
               if retained else {}),
        })
        graph_id = f"runtime-graph:{activation_id}"
        generations = {
            item.component_id: f"generation:{activation_id}:{digest_json(item.as_fact())[-24:]}"
            for item in ordered
        }
        generations.update(retained)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lease_in(conn, lease)
            clock = await self._clock_in(conn, update=True)
            existing = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_graphs "
                    "WHERE deployment_id=%s AND activation_id=%s",
                    (self.store.deployment_id, activation_id),
                )
            ).fetchone()
            if existing is not None:
                graph = RuntimeGenerationGraph(
                    graph_id, existing["graph_revision"], graph_digest, activation_id, generations
                )
                if existing["owner_epoch"] != lease.owner_epoch:
                    raise ForestError("RUNTIME_GRAPH_STALE", "Graph belongs to another Supervisor", 409)
                await self._graph_in(conn, graph)
                return graph
            if source_graph is not None:
                await self._graph_in(conn, source_graph)
                if clock["active_graph_id"] != source_graph.graph_id or (
                    clock["active_revision"] != source_graph.graph_revision
                ):
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_STALE", "Source Graph is no longer active", 409
                    )
                source_members = await (
                    await conn.execute(
                        "SELECT m.component_id,m.generation_id,g.state,g.owner_epoch,g.binding "
                        "FROM forest_deployment.runtime_graph_members m JOIN "
                        "forest_deployment.runtime_generations g ON "
                        "g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                        "WHERE m.deployment_id=%s AND m.graph_id=%s",
                        (self.store.deployment_id, source_graph.graph_id),
                    )
                ).fetchall()
                available = {item["component_id"]: item for item in source_members}
                if any(
                    component_id not in available
                    or available[component_id]["generation_id"] != generation_id
                    or available[component_id]["state"] != "ACTIVE"
                    or available[component_id]["owner_epoch"] != lease.owner_epoch
                    for component_id, generation_id in retained.items()
                ):
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_STALE", "Retained member is not current", 409
                    )
                replaced_or_removed = set(available) - set(retained)
                if any(
                    (set(available[component_id]["binding"]["requires"])
                     | set(available[component_id]["binding"]["retention_requires"]))
                    & replaced_or_removed
                    for component_id in retained
                ):
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_DEPENDENCY",
                        "A retained Generation depends on a replaced member", 409,
                    )
            revision = clock["next_revision"] + 1
            for item in ordered:
                await _storage_binding_in(conn, self.store.deployment_id, item.as_fact())
            previous_routes = {}
            if clock["active_graph_id"] is not None:
                previous_rows = await (
                    await conn.execute(
                        "SELECT component_id,generation_id,route_epoch FROM "
                        "forest_deployment.runtime_graph_members WHERE deployment_id=%s "
                        "AND graph_id=%s",
                        (self.store.deployment_id, clock["active_graph_id"]),
                    )
                ).fetchall()
                previous_routes = {row["component_id"]: row for row in previous_rows}
            await conn.execute(
                "INSERT INTO forest_deployment.runtime_graphs("
                "deployment_id,graph_id,graph_revision,graph_digest,activation_id,"
                "source_graph_id,source_revision,owner_epoch,state) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'PREPARED')",
                (
                    self.store.deployment_id, graph_id, revision, graph_digest, activation_id,
                    clock["active_graph_id"], clock["active_revision"], lease.owner_epoch,
                ),
            )
            for item in ordered:
                fact = item.as_fact()
                await conn.execute(
                    "INSERT INTO forest_deployment.runtime_generations("
                    "deployment_id,generation_id,component_id,binding_digest,binding,owner_epoch,state) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'PREPARED')",
                    (
                        self.store.deployment_id, generations[item.component_id], item.component_id,
                        digest_json(fact), Jsonb(fact), lease.owner_epoch,
                    ),
                )
            for component_id, generation_id in sorted(generations.items()):
                binding = next((item for item in ordered if item.component_id == component_id), None)
                if binding is None:
                    source = await (
                        await conn.execute(
                        "SELECT data_admission,control_admission FROM "
                            "forest_deployment.runtime_graph_members WHERE deployment_id=%s "
                            "AND graph_id=%s AND component_id=%s",
                            (self.store.deployment_id, source_graph.graph_id, component_id),
                        )
                    ).fetchone()
                    data_admission = source["data_admission"]
                    control_admission = source["control_admission"]
                else:
                    data_admission = binding.data_admission
                    control_admission = binding.control_admission
                previous = previous_routes.get(component_id)
                route_epoch = (
                    1 if previous is None else previous["route_epoch"]
                    + (previous["generation_id"] != generation_id)
                )
                await conn.execute(
                    "INSERT INTO forest_deployment.runtime_graph_members("
                    "deployment_id,graph_id,component_id,generation_id,"
                    "data_admission,control_admission,route_epoch) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.store.deployment_id, graph_id, component_id, generation_id,
                        data_admission, control_admission, route_epoch,
                    ),
                )
            await conn.execute(
                "UPDATE forest_deployment.runtime_graph_clock SET next_revision=%s "
                "WHERE deployment_id=%s", (revision, self.store.deployment_id),
            )
            return RuntimeGenerationGraph(
                graph_id, revision, graph_digest, activation_id, generations
            )

    async def publish_graph(
        self,
        lease: DeploymentSupervisorLease,
        graph: RuntimeGenerationGraph,
        *,
        configuration_activators: dict[str, object] | None = None,
        publication_participants: dict[str, object] | None = None,
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lease_in(conn, lease)
            clock = await self._clock_in(conn, update=True)
            saved = await self._graph_in(conn, graph, update=True)
            if saved["owner_epoch"] != lease.owner_epoch:
                raise ForestError("RUNTIME_GRAPH_STALE", "Graph belongs to another Supervisor", 409)
            if clock["active_graph_id"] == graph.graph_id and saved["state"] == "ACTIVE":
                return graph.graph_revision
            if saved["state"] != "PREPARED" or (
                saved["source_graph_id"] != clock["active_graph_id"]
                or saved["source_revision"] != clock["active_revision"]
            ):
                raise ForestError("RUNTIME_GRAPH_STALE", "Graph publication base changed", 409)
            members = await (
                await conn.execute(
                    "SELECT m.*,g.state,g.binding,g.owner_epoch FROM "
                    "forest_deployment.runtime_graph_members m JOIN "
                    "forest_deployment.runtime_generations g ON "
                    "g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                    "WHERE m.deployment_id=%s AND m.graph_id=%s ORDER BY m.component_id FOR UPDATE OF g",
                    (self.store.deployment_id, graph.graph_id),
                )
            ).fetchall()
            old = await (
                await conn.execute(
                    "SELECT component_id,generation_id FROM forest_deployment.runtime_graph_members "
                    "WHERE deployment_id=%s AND graph_id=%s",
                    (self.store.deployment_id, clock["active_graph_id"]),
                )
            ).fetchall() if clock["active_graph_id"] is not None else ()
            retained = {
                item["component_id"] for item in members
                if any(previous["component_id"] == item["component_id"]
                       and previous["generation_id"] == item["generation_id"] for previous in old)
            }
            if any(
                item["owner_epoch"] != lease.owner_epoch
                or item["state"] != ("ACTIVE" if item["component_id"] in retained else "PREPARED")
                for item in members
            ):
                raise ForestError("RUNTIME_GENERATION_NOT_READY", "Graph members are not ready", 409)
            new_components = {item["component_id"] for item in members} - retained
            for item in members:
                if item["component_id"] in new_components:
                    await _storage_binding_in(
                        conn, self.store.deployment_id, item["binding"]
                    )
            if configuration_activators is None or set(configuration_activators) != new_components:
                raise ForestError(
                    "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID",
                    "Every new Generation requires its configuration activator", 503,
                )
            for item in members:
                component_id = item["component_id"]
                if component_id in retained:
                    continue
                activate = getattr(configuration_activators[component_id], "activate_in", None)
                if activate is None:
                    raise ForestError(
                        "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID",
                        "Configuration participant has no transaction port", 503,
                    )
                binding = item["binding"]["configuration"]
                await activate(
                    conn,
                    operation_id=f"runtime-publish:{graph.graph_id}:{component_id}",
                    revision=binding["revision"], generation_id=item["generation_id"],
                    expected_payload_id=binding["payload_id"],
                    expected_payload_digest=binding["payload_digest"],
                )
            participants = publication_participants or {}
            if set(participants) - new_components:
                raise ForestError(
                    "RUNTIME_PUBLICATION_PARTICIPANTS_INVALID", "Participant is not a new member", 503
                )
            for component_id in sorted(participants):
                publish = getattr(participants[component_id], "publish_generation_in", None)
                if publish is None:
                    raise ForestError(
                        "RUNTIME_PUBLICATION_PARTICIPANTS_INVALID", "Participant has no transaction port", 503
                    )
                await publish(
                    conn, generation_id=graph.generations[component_id], graph_id=graph.graph_id
                )
            if clock["active_graph_id"] is not None:
                await conn.execute(
                    "UPDATE forest_deployment.runtime_graphs SET state='RETIRED',"
                    "retired_at=clock_timestamp() WHERE deployment_id=%s AND graph_id=%s",
                    (self.store.deployment_id, clock["active_graph_id"]),
                )
                for previous in old:
                    if previous["component_id"] not in retained:
                        await conn.execute(
                            "UPDATE forest_deployment.runtime_generations SET state='QUIESCING' "
                            "WHERE deployment_id=%s AND generation_id=%s AND state='ACTIVE'",
                            (self.store.deployment_id, previous["generation_id"]),
                        )
            await conn.execute(
                "UPDATE forest_deployment.runtime_generations SET state='ACTIVE' "
                "WHERE deployment_id=%s AND generation_id=ANY(%s) AND state='PREPARED'",
                (self.store.deployment_id,
                 [graph.generations[item] for item in sorted(new_components)]),
            )
            await conn.execute(
                "UPDATE forest_deployment.runtime_graphs SET state='ACTIVE',"
                "published_at=clock_timestamp() WHERE deployment_id=%s AND graph_id=%s",
                (self.store.deployment_id, graph.graph_id),
            )
            await conn.execute(
                "UPDATE forest_deployment.runtime_graph_clock SET active_graph_id=%s,"
                "active_revision=%s WHERE deployment_id=%s",
                (graph.graph_id, graph.graph_revision, self.store.deployment_id),
            )
            return graph.graph_revision

    async def current_graph(self) -> RuntimeGenerationGraph | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._clock_in(conn)
            if clock["active_graph_id"] is None:
                return None
            row = await (
                await conn.execute(
                    "SELECT graph_digest,activation_id FROM forest_deployment.runtime_graphs "
                    "WHERE deployment_id=%s AND graph_id=%s AND state='ACTIVE'",
                    (self.store.deployment_id, clock["active_graph_id"]),
                )
            ).fetchone()
            if row is None:
                raise ForestError("RUNTIME_GRAPH_IDENTITY_CONFLICT", "Active graph is missing", 409)
            members = await (
                await conn.execute(
                    "SELECT component_id,generation_id FROM forest_deployment.runtime_graph_members "
                    "WHERE deployment_id=%s AND graph_id=%s",
                    (self.store.deployment_id, clock["active_graph_id"]),
                )
            ).fetchall()
            return RuntimeGenerationGraph(
                clock["active_graph_id"], clock["active_revision"], row["graph_digest"],
                row["activation_id"],
                {item["component_id"]: item["generation_id"] for item in members},
            )

    async def active_package_selection(self) -> tuple[RuntimeGenerationGraph | None, dict[str, dict]]:
        """Read each active artifact and its frozen configuration binding."""

        graph = await self.current_graph()
        if graph is None:
            return None, {}
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._clock_in(conn)
            if clock["active_graph_id"] != graph.graph_id or (
                clock["active_revision"] != graph.graph_revision
            ):
                raise ForestError("RUNTIME_GRAPH_NOT_CURRENT", "RuntimeGraph changed", 409)
            rows = await (
                await conn.execute(
                    "SELECT m.component_id,m.generation_id,g.binding,g.binding_digest,g.state "
                    "FROM forest_deployment.runtime_graph_members m JOIN "
                    "forest_deployment.runtime_generations g ON "
                    "g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                    "WHERE m.deployment_id=%s AND m.graph_id=%s",
                    (self.store.deployment_id, graph.graph_id),
                )
            ).fetchall()
            if {row["component_id"]: row["generation_id"] for row in rows} != graph.generations:
                raise ForestError(
                    "RUNTIME_GRAPH_IDENTITY_CONFLICT", "Active Graph members changed", 409
                )
            selection = {}
            for row in rows:
                binding = row["binding"]
                package = binding.get("package") if isinstance(binding, dict) else None
                digest = package.get("package_digest") if isinstance(package, dict) else None
                configuration = binding.get("configuration") if isinstance(binding, dict) else None
                if (
                    row["state"] != "ACTIVE"
                    or row["binding_digest"] != digest_json(binding)
                    or not isinstance(digest, str)
                    or not isinstance(configuration, dict)
                    or type(configuration.get("revision")) is not int
                    or configuration["revision"] < 1
                    or not isinstance(configuration.get("payload_id"), str)
                    or not isinstance(configuration.get("payload_digest"), str)
                ):
                    raise ForestError(
                        "RUNTIME_GENERATION_BINDING_INVALID",
                        "Active deployment package binding is invalid", 409,
                    )
                selection[row["component_id"]] = {
                    "package_digest": digest,
                    "configuration": configuration,
                }
            return graph, selection

    async def _current_members_in(
        self, conn, graph: RuntimeGenerationGraph, *, update: bool = False
    ) -> list[dict]:
        clock = await self._clock_in(conn, update=update)
        await self._graph_in(conn, graph, update=update)
        if clock["active_graph_id"] != graph.graph_id:
            raise ForestError("RUNTIME_GRAPH_NOT_CURRENT", "RuntimeGraph is not current", 409)
        rows = await (
            await conn.execute(
                "SELECT m.*,g.binding,g.state,g.owner_epoch FROM "
                "forest_deployment.runtime_graph_members m JOIN "
                "forest_deployment.runtime_generations g ON "
                "g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                "WHERE m.deployment_id=%s AND m.graph_id=%s ORDER BY m.component_id "
                + ("FOR UPDATE OF m,g" if update else ""),
                (self.store.deployment_id, graph.graph_id),
            )
        ).fetchall()
        return [dict(row) for row in rows]

    async def freeze_graph_admission(
        self,
        lease: DeploymentSupervisorLease,
        graph: RuntimeGenerationGraph,
        *,
        components: tuple[str, ...] | None = None,
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lease_in(conn, lease)
            rows = await self._current_members_in(conn, graph, update=True)
            if any(row["owner_epoch"] != lease.owner_epoch for row in rows):
                raise ForestError("RUNTIME_GRAPH_STALE", "Graph belongs to another Supervisor", 409)
            selected = set(components) if components is not None else {
                row["component_id"] for row in rows
            }
            if len(selected) != (len(components) if components is not None else len(rows)) or (
                selected - {row["component_id"] for row in rows}
            ):
                raise ForestError("RUNTIME_REPLACEMENT_COMPONENT_INVALID", "Unknown graph member", 422)
            for row in rows:
                if row["component_id"] in selected:
                    continue
                binding = row["binding"]
                dependencies = set(binding["requires"]) | set(binding["retention_requires"])
                if dependencies & selected:
                    raise ForestError(
                        "RUNTIME_REPLACEMENT_DEPENDENCY_OPEN",
                        "Replacement must include dependent members", 409,
                    )
            changed = 0
            for row in rows:
                if row["component_id"] not in selected:
                    continue
                if row["data_admission"] or row["control_admission"]:
                    await conn.execute(
                        "UPDATE forest_deployment.runtime_graph_members SET "
                        "data_admission=false,control_admission=false,route_epoch=route_epoch+1 "
                        "WHERE deployment_id=%s AND graph_id=%s AND component_id=%s",
                        (self.store.deployment_id, graph.graph_id, row["component_id"]),
                    )
                    changed += 1
            return changed

    async def resume_graph_admission(
        self,
        lease: DeploymentSupervisorLease,
        graph: RuntimeGenerationGraph,
        *,
        components: tuple[str, ...] | None = None,
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lease_in(conn, lease)
            rows = await self._current_members_in(conn, graph, update=True)
            if any(row["owner_epoch"] != lease.owner_epoch for row in rows):
                raise ForestError("RUNTIME_GRAPH_STALE", "Graph belongs to another Supervisor", 409)
            selected = set(components) if components is not None else {
                row["component_id"] for row in rows
            }
            if len(selected) != (len(components) if components is not None else len(rows)) or (
                selected - {row["component_id"] for row in rows}
            ):
                raise ForestError("RUNTIME_REPLACEMENT_COMPONENT_INVALID", "Unknown graph member", 422)
            changed = 0
            for row in rows:
                if row["component_id"] not in selected:
                    continue
                if row["state"] != "ACTIVE" or row["owner_epoch"] != lease.owner_epoch:
                    raise ForestError("RUNTIME_GRAPH_RESUME_UNSAFE", "Generation cannot resume", 409)
                desired_data = row["binding"]["data_admission"]
                desired_control = row["binding"]["control_admission"]
                if (row["data_admission"], row["control_admission"]) != (
                    desired_data, desired_control
                ):
                    await conn.execute(
                        "UPDATE forest_deployment.runtime_graph_members SET "
                        "data_admission=%s,control_admission=%s,route_epoch=route_epoch+1 "
                        "WHERE deployment_id=%s AND graph_id=%s AND component_id=%s",
                        (
                            desired_data, desired_control, self.store.deployment_id,
                            graph.graph_id, row["component_id"],
                        ),
                    )
                    changed += 1
            return changed

    async def retire_graph(
        self, lease: DeploymentSupervisorLease, graph: RuntimeGenerationGraph
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lease_in(conn, lease)
            clock = await self._clock_in(conn, update=True)
            saved = await self._graph_in(conn, graph, update=True)
            if saved["owner_epoch"] != lease.owner_epoch:
                raise ForestError("RUNTIME_GRAPH_STALE", "Graph belongs to another Supervisor", 409)
            if saved["state"] == "RETIRED" and clock["active_graph_id"] != graph.graph_id:
                return 0
            if clock["active_graph_id"] != graph.graph_id or saved["state"] != "ACTIVE":
                raise ForestError("RUNTIME_GRAPH_NOT_CURRENT", "RuntimeGraph is not current", 409)
            await conn.execute(
                "UPDATE forest_deployment.runtime_graph_members SET "
                "data_admission=false,control_admission=false,route_epoch=route_epoch+1 "
                "WHERE deployment_id=%s AND graph_id=%s "
                "AND (data_admission OR control_admission)",
                (self.store.deployment_id, graph.graph_id),
            )
            await conn.execute(
                "UPDATE forest_deployment.runtime_generations SET state='QUIESCING' "
                "WHERE deployment_id=%s AND generation_id=ANY(%s) AND state='ACTIVE'",
                (self.store.deployment_id, list(graph.generations.values())),
            )
            await conn.execute(
                "UPDATE forest_deployment.runtime_graphs SET state='RETIRED',"
                "retired_at=clock_timestamp() WHERE deployment_id=%s AND graph_id=%s",
                (self.store.deployment_id, graph.graph_id),
            )
            await conn.execute(
                "UPDATE forest_deployment.runtime_graph_clock SET active_graph_id=NULL "
                "WHERE deployment_id=%s", (self.store.deployment_id,),
            )
            return graph.graph_revision

    async def record_states(
        self,
        lease: DeploymentSupervisorLease,
        graph: RuntimeGenerationGraph,
        states: dict[str, tuple[str, str | None]],
    ) -> None:
        allowed = {"PREPARED", "ACTIVE", "QUIESCING", "STOPPED", "FAILED"}
        if not isinstance(states, dict) or set(states) - set(graph.generations) or any(
            not isinstance(value, tuple) or len(value) != 2 or value[0] not in allowed
            or (
                value[0] == "FAILED"
                and (not isinstance(value[1], str) or _ERROR_CODE.fullmatch(value[1]) is None)
            )
            or (value[0] != "FAILED" and value[1] is not None)
            for value in states.values()
        ):
            raise ForestError("RUNTIME_GENERATION_STATE_INVALID", "Generation state is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lease_in(conn, lease)
            await self._graph_in(conn, graph, update=True)
            clock = await self._clock_in(conn, update=True)
            for component_id, (state, failure_code) in states.items():
                generation_id = graph.generations[component_id]
                current = await (
                    await conn.execute(
                        "SELECT state,failure_code,owner_epoch FROM forest_deployment.runtime_generations "
                        "WHERE deployment_id=%s AND generation_id=%s FOR UPDATE",
                        (self.store.deployment_id, generation_id),
                    )
                ).fetchone()
                if current is None or current["owner_epoch"] != lease.owner_epoch:
                    raise ForestError("RUNTIME_GENERATION_NOT_MEMBER", "Generation is stale", 409)
                if current["state"] == state:
                    if current["failure_code"] != failure_code:
                        raise ForestError(
                            "RUNTIME_GENERATION_STATE_INVALID", "Generation failure fact differs", 409
                        )
                    continue
                if state in {"QUIESCING", "STOPPED", "FAILED"} and (
                    clock["active_graph_id"] is not None
                    and clock["active_graph_id"] != graph.graph_id
                ):
                    retained = await (
                        await conn.execute(
                            "SELECT 1 FROM forest_deployment.runtime_graph_members "
                            "WHERE deployment_id=%s AND graph_id=%s AND generation_id=%s",
                            (
                                self.store.deployment_id, clock["active_graph_id"], generation_id,
                            ),
                        )
                    ).fetchone()
                    if retained is not None:
                        raise ForestError(
                            "RUNTIME_GENERATION_STILL_ACTIVE",
                            "Retained Generation belongs to the current graph", 409,
                        )
                transitions = {
                    "PREPARED": {"STOPPED", "FAILED"},
                    "ACTIVE": {"QUIESCING", "FAILED"},
                    "QUIESCING": {"STOPPED", "FAILED"},
                    "FAILED": {"STOPPED"},
                    "STOPPED": set(),
                }
                if state not in transitions[current["state"]]:
                    raise ForestError(
                        "RUNTIME_GENERATION_STATE_INVALID", "Generation state transition is invalid", 409
                    )
                await conn.execute(
                    "UPDATE forest_deployment.runtime_generations SET state=%s,failure_code=%s "
                    "WHERE deployment_id=%s AND generation_id=%s",
                    (state, failure_code, self.store.deployment_id, generation_id),
                )
            if any(state == "FAILED" for state, _code in states.values()):
                await conn.execute(
                    "UPDATE forest_deployment.runtime_graphs SET state='FAILED' "
                    "WHERE deployment_id=%s AND graph_id=%s AND state='PREPARED'",
                    (self.store.deployment_id, graph.graph_id),
                )

    async def verify_active_generation_in(
        self, conn, *, component_id: str, generation_id: str, admission_lane: str
    ) -> dict:
        if admission_lane not in {"DATA", "CONTROL"}:
            raise ForestError("RUNTIME_ADMISSION_LANE_INVALID", "Admission lane is invalid", 422)
        row = await (
            await conn.execute(
                "SELECT m.*,g.state,g.owner_epoch FROM forest_deployment.runtime_graph_clock h "
                "JOIN forest_deployment.runtime_graph_members m ON "
                "m.deployment_id=h.deployment_id AND m.graph_id=h.active_graph_id "
                "JOIN forest_deployment.runtime_generations g ON "
                "g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                "WHERE h.deployment_id=%s AND m.component_id=%s AND m.generation_id=%s "
                "FOR SHARE OF h,m,g",
                (self.store.deployment_id, component_id, generation_id),
            )
        ).fetchone()
        lease = await (
            await conn.execute(
                "SELECT owner_epoch FROM forest_deployment.supervisor_lease "
                "WHERE singleton AND expires_at>clock_timestamp() FOR SHARE"
            )
        ).fetchone()
        if row is None or lease is None or row["owner_epoch"] != lease["owner_epoch"] or (
            row["state"] != "ACTIVE"
            or not row["data_admission" if admission_lane == "DATA" else "control_admission"]
        ):
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Generation is not admitted", 409)
        return dict(row)

    async def verify_generation_member_in(
        self, conn, *, component_id: str, generation_id: str
    ) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.runtime_generations WHERE deployment_id=%s "
                "AND component_id=%s AND generation_id=%s",
                (self.store.deployment_id, component_id, generation_id),
            )
        ).fetchone()
        lease = await (
            await conn.execute(
                "SELECT owner_epoch FROM forest_deployment.supervisor_lease "
                "WHERE singleton AND expires_at>clock_timestamp() FOR SHARE"
            )
        ).fetchone()
        if row is None or lease is None or row["owner_epoch"] != lease["owner_epoch"] or (
            row["state"] not in {"ACTIVE", "QUIESCING"}
        ):
            raise ForestError("RUNTIME_GENERATION_NOT_MEMBER", "Generation is not a member", 409)
        return dict(row)

    async def verify_generation_member_snapshot_in(
        self, conn, *, component_id: str, generation_id: str
    ) -> dict:
        row = await (
            await conn.execute(
                "SELECT g.* FROM forest_deployment.runtime_generations g JOIN "
                "forest_deployment.supervisor_lease l ON l.singleton "
                "WHERE g.deployment_id=%s AND g.component_id=%s AND g.generation_id=%s "
                "AND g.owner_epoch=l.owner_epoch AND l.expires_at>clock_timestamp() "
                "AND g.state IN ('ACTIVE','QUIESCING')",
                (self.store.deployment_id, component_id, generation_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("RUNTIME_GENERATION_NOT_MEMBER", "Generation is not a member", 409)
        return dict(row)

    async def verify_prepared_generation_in(
        self, conn, *, component_id: str, generation_id: str
    ) -> dict:
        row = await (
            await conn.execute(
                "SELECT g.* FROM forest_deployment.runtime_generations g JOIN "
                "forest_deployment.runtime_graph_members m ON "
                "m.deployment_id=g.deployment_id AND m.generation_id=g.generation_id "
                "JOIN forest_deployment.runtime_graphs r ON "
                "r.deployment_id=m.deployment_id AND r.graph_id=m.graph_id "
                "WHERE g.deployment_id=%s AND g.component_id=%s AND g.generation_id=%s "
                "AND g.state='PREPARED' AND r.state='PREPARED'",
                (self.store.deployment_id, component_id, generation_id),
            )
        ).fetchone()
        lease = await (
            await conn.execute(
                "SELECT owner_epoch FROM forest_deployment.supervisor_lease "
                "WHERE singleton AND expires_at>clock_timestamp() FOR SHARE"
            )
        ).fetchone()
        if row is None or lease is None or row["owner_epoch"] != lease["owner_epoch"]:
            raise ForestError("RUNTIME_GENERATION_NOT_PREPARED", "Generation is not prepared", 409)
        return dict(row)

    async def verify_generation_cleanup_in(
        self, conn, *, component_id: str, generation_id: str
    ) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.runtime_generations WHERE deployment_id=%s "
                "AND component_id=%s AND generation_id=%s "
                "AND state IN ('QUIESCING','STOPPED','FAILED')",
                (self.store.deployment_id, component_id, generation_id),
            )
        ).fetchone()
        lease = await (
            await conn.execute(
                "SELECT owner_epoch FROM forest_deployment.supervisor_lease "
                "WHERE singleton AND expires_at>clock_timestamp() FOR SHARE"
            )
        ).fetchone()
        if row is None or lease is None or row["owner_epoch"] != lease["owner_epoch"]:
            raise ForestError("RUNTIME_GENERATION_CLEANUP_DENIED", "Generation is not retiring", 409)
        return dict(row)

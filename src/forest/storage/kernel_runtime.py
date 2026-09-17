"""Clean ``kernel.runtime`` authority for host leases and immutable runtime graphs."""

from __future__ import annotations

import re
from datetime import timedelta
from uuid import UUID

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import PreparedGenerationBinding, RuntimeGenerationGraph
from forest.storage.bot import HostLease

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RAW_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class KernelRuntimeRepository:
    """The only writer for the clean runtime Generation and RuntimeGraph facts."""

    def __init__(self, store, namespace: str) -> None:
        if not namespace:
            raise ValueError("kernel runtime namespace is required")
        self.store = store
        self.namespace = namespace

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    async def claim_host(
        self,
        host_id: str,
        incarnation: str,
        *,
        ttl: timedelta = timedelta(seconds=30),
    ) -> HostLease:
        seconds = ttl.total_seconds()
        if (
            not isinstance(host_id, str)
            or not host_id
            or len(host_id) > 256
            or not isinstance(incarnation, str)
            or not incarnation
            or len(incarnation) > 256
            or not 5 <= seconds <= 300
        ):
            raise ForestError("HOST_LEASE_INVALID", "Host lease identity or TTL is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT h.*,clock_timestamp() AS current_time FROM {} h "
                        "WHERE bot_id=%s FOR UPDATE"
                    ).format(self._table("host_lease")),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if row is None:
                raise ForestError("KERNEL_RUNTIME_NOT_INITIALIZED", "Host lease row is absent", 503)
            if row["incarnation"] == incarnation:
                if row["host_id"] != host_id or row["expires_at"] <= row["current_time"]:
                    raise ForestError(
                        "STALE_HOST", "A host incarnation cannot be changed or resurrected", 409
                    )
                saved = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET expires_at=clock_timestamp()+make_interval(secs => %s) "
                            "WHERE bot_id=%s RETURNING *"
                        ).format(self._table("host_lease")),
                        (seconds, self.store.bot_id),
                    )
                ).fetchone()
                return self._lease(saved)
            if row["incarnation"] is not None and row["expires_at"] > row["current_time"]:
                raise ForestError("HOST_LEASE_HELD", "Another host incarnation owns the Bot", 409)
            prior = await (
                await conn.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE bot_id=%s AND incarnation=%s").format(
                        self._table("host_incarnations")
                    ),
                    (self.store.bot_id, incarnation),
                )
            ).fetchone()
            if prior is not None:
                raise ForestError("STALE_HOST", "A retired host incarnation cannot be reused", 409)
            owner_epoch = row["owner_epoch"] + 1
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,incarnation,host_id,owner_epoch) VALUES (%s,%s,%s,%s)"
                ).format(self._table("host_incarnations")),
                (self.store.bot_id, incarnation, host_id, owner_epoch),
            )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET host_id=%s,incarnation=%s,owner_epoch=%s,"
                        "expires_at=clock_timestamp()+make_interval(secs => %s) "
                        "WHERE bot_id=%s RETURNING *"
                    ).format(self._table("host_lease")),
                    (host_id, incarnation, owner_epoch, seconds, self.store.bot_id),
                )
            ).fetchone()
            return self._lease(saved)

    async def prepare_graph(
        self,
        lease: HostLease,
        bindings: tuple[PreparedGenerationBinding, ...] | list[PreparedGenerationBinding],
        activation_id: str,
    ) -> RuntimeGenerationGraph:
        ordered = self._validate_bindings(bindings, activation_id)
        graph_digest = digest_json(
            {
                "protocol": "forest.runtime-graph/1",
                "members": [item.as_fact() for item in ordered],
            }
        )
        generations = {
            item.component_id: (
                f"generation:{activation_id}:{digest_json(item.as_fact())[-24:]}"
            )
            for item in ordered
        }
        graph_id = f"runtime-graph:{activation_id}"
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            clock = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_clock")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if clock is None:
                raise ForestError(
                    "KERNEL_RUNTIME_NOT_INITIALIZED", "Runtime graph clock is absent", 503
                )
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND activation_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, activation_id),
                )
            ).fetchone()
            if existing is not None:
                await self._guard_graph_replay_in(
                    conn, lease, existing, graph_id, graph_digest, generations
                )
                return RuntimeGenerationGraph(
                    graph_id,
                    existing["graph_revision"],
                    graph_digest,
                    activation_id,
                    generations,
                )
            revision = clock["next_revision"]
            await conn.execute(
                sql.SQL("UPDATE {} SET next_revision=next_revision+1 WHERE bot_id=%s").format(
                    self._table("runtime_graph_clock")
                ),
                (self.store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,graph_revision,graph_digest,activation_id,state) "
                    "VALUES (%s,%s,%s,%s,%s,'PREPARED')"
                ).format(self._table("runtime_graphs")),
                (self.store.bot_id, graph_id, revision, graph_digest, activation_id),
            )
            for item in ordered:
                await self._storage_in(conn, item)
                generation_id = generations[item.component_id]
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,generation_id,component_id,package_digest,"
                        "config_revision,config_payload_id,config_payload_digest,instance_id,"
                        "worker_incarnation,host_incarnation,host_owner_epoch,state_schema,"
                        "state_contract_digest,capability_grants_digest,status) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'READY')"
                    ).format(self._table("generations")),
                    (
                        self.store.bot_id,
                        generation_id,
                        item.component_id,
                        item.package_digest,
                        item.config_revision,
                        item.config_payload_id,
                        item.config_payload_digest,
                        item.instance_id,
                        item.worker_incarnation,
                        lease.incarnation,
                        lease.owner_epoch,
                        item.state_schema,
                        item.state_contract_digest,
                        item.capability_grants_digest,
                    ),
                )
                storage = item.storage
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,graph_id,component_id,generation_id,package_id,"
                        "package_version,package_digest,driver,trust_level,descriptor_digest,"
                        "route_contract_digest,management_contract_digest,management_ui_digest,"
                        "data_admission,control_admission,storage_schema_id,storage_schema_version,"
                        "storage_namespace,storage_baseline_sha256,storage_repository_contract,"
                        "storage_execution_profile) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("runtime_graph_modules")),
                    (
                        self.store.bot_id,
                        graph_id,
                        item.component_id,
                        generation_id,
                        item.package_id,
                        item.package_version,
                        item.package_digest,
                        item.driver,
                        item.trust_level,
                        item.descriptor_digest,
                        item.route_contract_digest,
                        item.management_contract_digest,
                        item.management_ui_digest,
                        item.data_admission,
                        item.control_admission,
                        storage.schema_id if storage else None,
                        storage.schema_version if storage else None,
                        storage.namespace if storage else None,
                        storage.baseline_sha256 if storage else None,
                        storage.repository_contract if storage else None,
                        storage.execution_profile if storage else None,
                    ),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,graph_id,component_id,sequence,state) "
                        "VALUES (%s,%s,%s,1,'PREPARED')"
                    ).format(self._table("runtime_graph_lifecycle_facts")),
                    (self.store.bot_id, graph_id, item.component_id),
                )
            for item in ordered:
                for required in item.requires:
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,graph_id,component_id,required_component_id) "
                            "VALUES (%s,%s,%s,%s)"
                        ).format(self._table("runtime_graph_dependencies")),
                        (self.store.bot_id, graph_id, item.component_id, required),
                    )
        return RuntimeGenerationGraph(
            graph_id, revision, graph_digest, activation_id, generations
        )

    async def publish_graph(
        self,
        lease: HostLease,
        graph: RuntimeGenerationGraph,
        *,
        configuration_activators: dict[str, object] | None = None,
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            saved = await self._graph_in(conn, graph, lock=True)
            head = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_head")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if head is not None and head["graph_id"] == graph.graph_id:
                if saved["state"] != "ACTIVE":
                    raise ForestError(
                        "RUNTIME_GRAPH_IDENTITY_CONFLICT",
                        "The published head is not an active graph",
                        409,
                    )
                return head["graph_revision"]
            if head is not None and head["graph_revision"] >= graph.graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_STALE", "A newer runtime publication already exists", 409
                )
            members = await self._members_in(conn, graph, lock=True)
            if any(
                item["host_incarnation"] != lease.incarnation
                or item["host_owner_epoch"] != lease.owner_epoch
                or item["status"] != "READY"
                for item in members
            ):
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_READY",
                    "Every graph Generation must be READY under the current Host fence",
                    409,
                )
            if configuration_activators is not None:
                components = {item["component_id"] for item in members}
                if set(configuration_activators) != components:
                    raise ForestError(
                        "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID",
                        "Every graph member must provide exactly one configuration activator",
                        503,
                    )
                for item in members:
                    activator = configuration_activators[item["component_id"]]
                    activate = getattr(activator, "activate_in", None)
                    if activate is None:
                        raise ForestError(
                            "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID",
                            "Configuration participant does not expose activate_in",
                            503,
                        )
                    await activate(
                        conn,
                        operation_id=(
                            f"runtime-publish:{graph.graph_id}:{item['component_id']}"
                        ),
                        revision=item["config_revision"],
                        generation_id=item["generation_id"],
                        expected_payload_id=item["config_payload_id"],
                        expected_payload_digest=item["config_payload_digest"],
                    )
            old_components: set[str] = set()
            if head is not None and head["graph_id"] is not None:
                old_members = await self._members_for_id_in(conn, head["graph_id"], lock=True)
                old_components = {item["component_id"] for item in old_members}
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='RETIRED' WHERE bot_id=%s AND graph_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, head["graph_id"]),
                )
                for item in old_members:
                    if item["status"] == "ACTIVE":
                        await conn.execute(
                            sql.SQL(
                                "UPDATE {} SET status='QUIESCING' "
                                "WHERE bot_id=%s AND generation_id=%s"
                            ).format(self._table("generations")),
                            (self.store.bot_id, item["generation_id"]),
                        )
                        await self._append_lifecycle_in(
                            conn, head["graph_id"], item["component_id"], "QUIESCING"
                        )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='ACTIVE' WHERE bot_id=%s AND graph_id=%s").format(
                    self._table("runtime_graphs")
                ),
                (self.store.bot_id, graph.graph_id),
            )
            new_components = set()
            for item in members:
                new_components.add(item["component_id"])
                await conn.execute(
                    sql.SQL("UPDATE {} SET status='ACTIVE' WHERE bot_id=%s AND generation_id=%s").format(
                        self._table("generations")
                    ),
                    (self.store.bot_id, item["generation_id"]),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,component_id,generation_id,route_epoch,"
                        "data_admission,control_admission) VALUES (%s,%s,%s,1,%s,%s) "
                        "ON CONFLICT (bot_id,component_id) DO UPDATE SET "
                        "generation_id=EXCLUDED.generation_id,route_epoch=routes.route_epoch+1,"
                        "data_admission=EXCLUDED.data_admission,"
                        "control_admission=EXCLUDED.control_admission"
                    ).format(self._table("routes")),
                    (
                        self.store.bot_id,
                        item["component_id"],
                        item["generation_id"],
                        item["data_admission"],
                        item["control_admission"],
                    ),
                )
                await self._append_lifecycle_in(
                    conn, graph.graph_id, item["component_id"], "ACTIVE"
                )
            for component_id in old_components - new_components:
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET generation_id=NULL,route_epoch=route_epoch+1,"
                        "data_admission=false,control_admission=false "
                        "WHERE bot_id=%s AND component_id=%s"
                    ).format(self._table("routes")),
                    (self.store.bot_id, component_id),
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,graph_revision) VALUES (%s,%s,%s) "
                    "ON CONFLICT (bot_id) DO UPDATE SET graph_id=EXCLUDED.graph_id,"
                    "graph_revision=EXCLUDED.graph_revision,published_at=clock_timestamp()"
                ).format(self._table("runtime_graph_head")),
                (self.store.bot_id, graph.graph_id, graph.graph_revision),
            )
            return graph.graph_revision

    async def retire_graph(
        self, lease: HostLease, graph: RuntimeGenerationGraph
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            await self._graph_in(conn, graph, lock=True)
            head = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_head")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if head is None or head["graph_id"] != graph.graph_id:
                return head["graph_revision"] if head is not None else 0
            clock = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_clock")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            revision = clock["next_revision"]
            await conn.execute(
                sql.SQL("UPDATE {} SET next_revision=next_revision+1 WHERE bot_id=%s").format(
                    self._table("runtime_graph_clock")
                ),
                (self.store.bot_id,),
            )
            members = await self._members_in(conn, graph, lock=True)
            for item in members:
                if item["status"] == "ACTIVE":
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET status='QUIESCING' "
                            "WHERE bot_id=%s AND generation_id=%s"
                        ).format(self._table("generations")),
                        (self.store.bot_id, item["generation_id"]),
                    )
                    await self._append_lifecycle_in(
                        conn, graph.graph_id, item["component_id"], "QUIESCING"
                    )
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET generation_id=NULL,route_epoch=route_epoch+1,"
                        "data_admission=false,control_admission=false "
                        "WHERE bot_id=%s AND component_id=%s AND generation_id=%s"
                    ).format(self._table("routes")),
                    (self.store.bot_id, item["component_id"], item["generation_id"]),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='RETIRED' WHERE bot_id=%s AND graph_id=%s").format(
                    self._table("runtime_graphs")
                ),
                (self.store.bot_id, graph.graph_id),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET graph_id=NULL,graph_revision=%s,published_at=clock_timestamp() "
                    "WHERE bot_id=%s"
                ).format(self._table("runtime_graph_head")),
                (revision, self.store.bot_id),
            )
            return revision

    async def record_states(
        self,
        lease: HostLease,
        graph: RuntimeGenerationGraph,
        states: dict[str, tuple[str, str | None]],
    ) -> None:
        allowed = {"PREPARED", "ACTIVE", "QUIESCING", "QUIESCENT", "STOPPED", "FAILED"}
        if set(states) - set(graph.generations) or any(
            state not in allowed or (state != "FAILED" and error_code is not None)
            for state, error_code in states.values()
        ):
            raise ForestError(
                "RUNTIME_LIFECYCLE_INVALID", "Runtime lifecycle update is invalid", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            await self._graph_in(conn, graph, lock=True)
            members = {
                item["component_id"]: item
                for item in await self._members_in(conn, graph, lock=True)
            }
            for component_id, (state, error_code) in sorted(states.items()):
                member = members[component_id]
                latest = await self._latest_lifecycle_in(conn, graph.graph_id, component_id)
                path = self._lifecycle_path(latest["state"], state)
                for step in path:
                    await self._append_lifecycle_in(
                        conn,
                        graph.graph_id,
                        component_id,
                        step,
                        error_code=error_code if step == "FAILED" else None,
                    )
                generation_status = {
                    "PREPARED": "READY",
                    "ACTIVE": "ACTIVE",
                    "QUIESCING": "QUIESCING",
                    "QUIESCENT": "QUIESCENT",
                    "STOPPED": "DISPOSED",
                    "FAILED": "FAILED",
                }[state]
                await conn.execute(
                    sql.SQL("UPDATE {} SET status=%s WHERE bot_id=%s AND generation_id=%s").format(
                        self._table("generations")
                    ),
                    (generation_status, self.store.bot_id, member["generation_id"]),
                )
            final_states = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT DISTINCT ON (component_id) component_id,state FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s ORDER BY component_id,sequence DESC"
                    ).format(self._table("runtime_graph_lifecycle_facts")),
                    (self.store.bot_id, graph.graph_id),
                )
            ).fetchall()
            if final_states and all(item["state"] == "STOPPED" for item in final_states):
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='STOPPED' WHERE bot_id=%s AND graph_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, graph.graph_id),
                )
            elif any(item["state"] == "FAILED" for item in final_states):
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='FAILED' WHERE bot_id=%s AND graph_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, graph.graph_id),
                )

    async def _identity_in(self, conn) -> None:
        row = await (
            await conn.execute(
                "SELECT bot_id FROM forest_kernel.bot_identity WHERE singleton FOR SHARE"
            )
        ).fetchone()
        if row is None or row["bot_id"] != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Runtime authority belongs to another Bot", 403)

    async def _storage_in(self, conn, binding: PreparedGenerationBinding) -> None:
        storage = binding.storage
        if storage is None:
            return
        row = await (
            await conn.execute(
                "SELECT * FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND schema_id=%s FOR SHARE",
                (self.store.bot_id, storage.schema_id),
            )
        ).fetchone()
        expected = {
            "component_id": binding.component_id,
            "schema_version": storage.schema_version,
            "namespace": storage.namespace,
            "baseline_sha256": storage.baseline_sha256,
            "package_digest": binding.package_digest,
            "repository_contract": storage.repository_contract,
            "execution_profile": storage.execution_profile,
            "state": "READY",
        }
        if row is None or any(str(row[key]) != value for key, value in expected.items()):
            raise ForestError(
                "RUNTIME_STORAGE_BINDING_INVALID",
                f"Generation storage is not the installed READY bundle for {binding.component_id}",
                409,
            )

    async def _host_in(self, conn, lease: HostLease) -> None:
        if lease.bot_id != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Host lease belongs to another Bot", 403)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT *,clock_timestamp() AS current_time FROM {} "
                    "WHERE bot_id=%s FOR SHARE"
                ).format(self._table("host_lease")),
                (self.store.bot_id,),
            )
        ).fetchone()
        if (
            row is None
            or row["host_id"] != lease.host_id
            or row["incarnation"] != lease.incarnation
            or row["owner_epoch"] != lease.owner_epoch
            or row["expires_at"] <= row["current_time"]
        ):
            raise ForestError("STALE_HOST", "Host lease is stale", 409)

    async def _guard_graph_replay_in(
        self,
        conn,
        lease: HostLease,
        existing,
        graph_id: str,
        graph_digest: str,
        generations: dict[str, str],
    ) -> None:
        members = await self._members_for_id_in(conn, graph_id, lock=False)
        actual = {item["component_id"]: item["generation_id"] for item in members}
        if (
            existing["graph_id"] != graph_id
            or existing["graph_digest"] != graph_digest
            or actual != generations
            or any(
                item["host_incarnation"] != lease.incarnation
                or item["host_owner_epoch"] != lease.owner_epoch
                for item in members
            )
        ):
            raise ForestError(
                "RUNTIME_ACTIVATION_CONFLICT",
                "Activation identity names different prepared facts",
                409,
            )

    async def _graph_in(self, conn, graph: RuntimeGenerationGraph, *, lock: bool):
        suffix = sql.SQL(" FOR UPDATE") if lock else sql.SQL("")
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND graph_id=%s").format(
                    self._table("runtime_graphs")
                )
                + suffix,
                (self.store.bot_id, graph.graph_id),
            )
        ).fetchone()
        if row is None or any(
            row[key] != value
            for key, value in {
                "graph_revision": graph.graph_revision,
                "graph_digest": graph.graph_digest,
                "activation_id": graph.activation_id,
            }.items()
        ):
            raise ForestError(
                "RUNTIME_GRAPH_IDENTITY_CONFLICT", "Runtime graph identity is not exact", 409
            )
        return row

    async def _members_in(self, conn, graph: RuntimeGenerationGraph, *, lock: bool):
        members = await self._members_for_id_in(conn, graph.graph_id, lock=lock)
        actual = {item["component_id"]: item["generation_id"] for item in members}
        if actual != graph.generations:
            raise ForestError(
                "RUNTIME_GRAPH_IDENTITY_CONFLICT", "Runtime graph membership is not exact", 409
            )
        return members

    async def _members_for_id_in(self, conn, graph_id: str, *, lock: bool):
        suffix = sql.SQL(" FOR UPDATE OF g") if lock else sql.SQL("")
        return await (
            await conn.execute(
                sql.SQL(
                    "SELECT m.*,g.status,g.host_incarnation,g.host_owner_epoch,"
                    "g.config_revision,g.config_payload_id,g.config_payload_digest "
                    "FROM {} m JOIN {} g ON (g.bot_id,g.generation_id)="
                    "(m.bot_id,m.generation_id) WHERE m.bot_id=%s AND m.graph_id=%s "
                    "ORDER BY m.component_id"
                ).format(self._table("runtime_graph_modules"), self._table("generations"))
                + suffix,
                (self.store.bot_id, graph_id),
            )
        ).fetchall()

    async def _append_lifecycle_in(
        self,
        conn,
        graph_id: str,
        component_id: str,
        state: str,
        *,
        error_code: str | None = None,
    ) -> None:
        latest = await self._latest_lifecycle_in(conn, graph_id, component_id)
        if latest["state"] == state:
            return
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,graph_id,component_id,sequence,state,error_code) "
                "VALUES (%s,%s,%s,%s,%s,%s)"
            ).format(self._table("runtime_graph_lifecycle_facts")),
            (
                self.store.bot_id,
                graph_id,
                component_id,
                latest["sequence"] + 1,
                state,
                error_code,
            ),
        )

    async def _latest_lifecycle_in(self, conn, graph_id: str, component_id: str):
        latest = await (
            await conn.execute(
                sql.SQL(
                    "SELECT sequence,state,error_code FROM {} WHERE bot_id=%s AND graph_id=%s "
                    "AND component_id=%s ORDER BY sequence DESC LIMIT 1 FOR UPDATE"
                ).format(self._table("runtime_graph_lifecycle_facts")),
                (self.store.bot_id, graph_id, component_id),
            )
        ).fetchone()
        if latest is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_PREPARED", "Generation lifecycle is absent", 409
            )
        return latest

    @staticmethod
    def _lifecycle_path(current: str, target: str) -> tuple[str, ...]:
        if current == target:
            return ()
        if target == "FAILED" and current not in {"STOPPED", "FAILED"}:
            return ("FAILED",)
        paths = {
            ("PREPARED", "ACTIVE"): ("ACTIVE",),
            ("PREPARED", "STOPPED"): ("STOPPED",),
            ("ACTIVE", "QUIESCING"): ("QUIESCING",),
            ("ACTIVE", "QUIESCENT"): ("QUIESCING", "QUIESCENT"),
            ("ACTIVE", "STOPPED"): ("QUIESCING", "QUIESCENT", "STOPPED"),
            ("QUIESCING", "QUIESCENT"): ("QUIESCENT",),
            ("QUIESCING", "STOPPED"): ("QUIESCENT", "STOPPED"),
            ("QUIESCENT", "STOPPED"): ("STOPPED",),
            ("FAILED", "STOPPED"): ("STOPPED",),
        }
        path = paths.get((current, target))
        if path is None:
            raise ForestError(
                "RUNTIME_LIFECYCLE_INVALID",
                f"Runtime lifecycle cannot move from {current} to {target}",
                409,
            )
        return path

    @staticmethod
    def _lease(row) -> HostLease:
        return HostLease(
            row["bot_id"],
            row["host_id"],
            row["incarnation"],
            row["owner_epoch"],
            row["expires_at"],
        )

    @staticmethod
    def _validate_bindings(
        bindings: tuple[PreparedGenerationBinding, ...] | list[PreparedGenerationBinding],
        activation_id: str,
    ) -> list[PreparedGenerationBinding]:
        try:
            UUID(activation_id)
        except (TypeError, ValueError, AttributeError):
            raise ForestError(
                "RUNTIME_ACTIVATION_ID_INVALID", "Runtime activation identity must be a UUID", 422
            ) from None
        ordered = sorted(bindings, key=lambda item: item.component_id)
        components = {item.component_id for item in ordered}
        if not ordered or len(components) != len(ordered):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_INVALID",
                "Runtime generation bindings must be non-empty and unique",
                422,
            )
        for item in ordered:
            storage = item.storage
            strings = (
                item.component_id,
                item.package_id,
                item.package_version,
                item.driver,
                item.config_payload_id,
                item.instance_id,
                item.worker_incarnation,
            )
            digests = (
                item.package_digest,
                item.descriptor_digest,
                item.route_contract_digest,
                item.management_contract_digest,
                item.management_ui_digest,
                item.config_payload_digest,
                item.state_contract_digest,
                item.capability_grants_digest,
            )
            invalid_storage = storage is not None and (
                not all(
                    isinstance(value, str) and value
                    for value in (
                        storage.schema_id,
                        storage.schema_version,
                        storage.namespace,
                        storage.repository_contract,
                    )
                )
                or not _RAW_DIGEST.fullmatch(storage.baseline_sha256)
                or storage.execution_profile not in {"declarative", "official"}
            )
            if (
                not _IDENTIFIER.fullmatch(item.component_id)
                or any(not isinstance(value, str) or not value for value in strings)
                or any(not _DIGEST.fullmatch(value) for value in digests)
                or item.trust_level not in {"official", "installed"}
                or type(item.config_revision) is not int
                or item.config_revision <= 0
                or type(item.state_schema) is not int
                or item.state_schema <= 0
                or type(item.data_admission) is not bool
                or type(item.control_admission) is not bool
                or len(item.requires) != len(set(item.requires))
                or item.component_id in item.requires
                or set(item.requires) - components
                or invalid_storage
            ):
                raise ForestError(
                    "RUNTIME_GENERATION_BINDING_INVALID",
                    f"Prepared Generation binding is invalid for {item.component_id}",
                    422,
                )
        return ordered

"""Persistent graph identity for native runtime modules."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.bot import HostLease


@dataclass(frozen=True)
class RuntimeGraphIdentity:
    graph_id: str
    descriptor_set_digest: str
    generations: dict[str, str]


class RuntimeModuleGraphStore:
    """Append native module generations and atomically publish one graph head."""

    def __init__(self, store):
        self.store = store

    @staticmethod
    def identity(lease: HostLease, descriptors: list[dict]) -> RuntimeGraphIdentity:
        ordered = sorted(descriptors, key=lambda item: item["component_id"])
        set_digest = digest_json(ordered)
        graph_id = f"runtime-graph:{lease.incarnation}:{set_digest[-16:]}"
        generations = {
            item["component_id"]: f"runtime:{lease.incarnation}:{item['descriptor_digest'][-32:]}"
            for item in ordered
        }
        return RuntimeGraphIdentity(graph_id, set_digest, generations)

    async def prepare(self, lease: HostLease, descriptors: list[dict]) -> RuntimeGraphIdentity:
        ordered = sorted(descriptors, key=lambda item: item["component_id"])
        components = {item["component_id"] for item in ordered}
        if len(components) != len(ordered):
            raise ForestError("DUPLICATE_RUNTIME_MODULE", "Runtime module identities must be unique")
        if any(
            item.get("descriptor_digest")
            != digest_json({key: value for key, value in item.items() if key != "descriptor_digest"})
            for item in ordered
        ):
            raise ForestError(
                "RUNTIME_MODULE_DESCRIPTOR_DIGEST_INVALID",
                "Runtime module descriptor digest does not match its canonical facts",
            )
        if any(set(item["requires"]) - components for item in ordered):
            raise ForestError(
                "RUNTIME_MODULE_DEPENDENCY_CYCLE",
                "Runtime module dependencies are missing or cyclic",
            )
        identity = self.identity(lease, ordered)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            await self.store._host(conn, lease)
            await conn.execute(
                "INSERT INTO public.bot_runtime_graph_clock(bot_id) VALUES (%s) "
                "ON CONFLICT DO NOTHING",
                (self.store.bot_id,),
            )
            await conn.execute(
                "INSERT INTO public.bot_runtime_graph_heads(bot_id) VALUES (%s) "
                "ON CONFLICT DO NOTHING",
                (self.store.bot_id,),
            )
            await conn.execute(
                "INSERT INTO public.bot_runtime_graphs("
                "bot_id,graph_id,descriptor_set_digest,host_id,host_incarnation,host_owner_epoch) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    self.store.bot_id,
                    identity.graph_id,
                    identity.descriptor_set_digest,
                    lease.host_id,
                    lease.incarnation,
                    lease.owner_epoch,
                ),
            )
            graph = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_graphs WHERE bot_id=%s AND graph_id=%s",
                    (self.store.bot_id, identity.graph_id),
                )
            ).fetchone()
            if graph is None or any(
                graph[key] != value
                for key, value in {
                    "descriptor_set_digest": identity.descriptor_set_digest,
                    "host_id": lease.host_id,
                    "host_incarnation": lease.incarnation,
                    "host_owner_epoch": lease.owner_epoch,
                }.items()
            ):
                raise ForestError(
                    "RUNTIME_GRAPH_IDENTITY_CONFLICT",
                    "Runtime graph identity names different immutable facts",
                    409,
                )
            for item in ordered:
                component_id = item["component_id"]
                generation_id = identity.generations[component_id]
                await conn.execute(
                    "INSERT INTO public.bot_runtime_module_generations("
                    "bot_id,generation_id,graph_id,component_id,descriptor_digest,descriptor) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        self.store.bot_id,
                        generation_id,
                        identity.graph_id,
                        component_id,
                        item["descriptor_digest"],
                        Jsonb(item),
                    ),
                )
                generation = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_runtime_module_generations "
                        "WHERE bot_id=%s AND generation_id=%s",
                        (self.store.bot_id, generation_id),
                    )
                ).fetchone()
                if generation is None or any(
                    generation[key] != value
                    for key, value in {
                        "graph_id": identity.graph_id,
                        "component_id": component_id,
                        "descriptor_digest": item["descriptor_digest"],
                        "descriptor": item,
                    }.items()
                ):
                    raise ForestError(
                        "RUNTIME_GENERATION_IDENTITY_CONFLICT",
                        "Runtime generation identity names different immutable facts",
                        409,
                    )
                await conn.execute(
                    "INSERT INTO public.bot_runtime_module_lifecycle_facts("
                    "bot_id,generation_id,sequence,state,host_incarnation,host_owner_epoch) "
                    "VALUES (%s,%s,1,'PREPARED',%s,%s) ON CONFLICT DO NOTHING",
                    (self.store.bot_id, generation_id, lease.incarnation, lease.owner_epoch),
                )
            for item in ordered:
                for required in item["requires"]:
                    await conn.execute(
                        "INSERT INTO public.bot_runtime_module_dependencies("
                        "bot_id,graph_id,source_generation_id,source_component_id,"
                        "target_generation_id,target_component_id) VALUES (%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT DO NOTHING",
                        (
                            self.store.bot_id,
                            identity.graph_id,
                            identity.generations[item["component_id"]],
                            item["component_id"],
                            identity.generations[required],
                            required,
                        ),
                    )
        return identity

    async def _append_states_in(
        self,
        conn,
        lease: HostLease,
        identity: RuntimeGraphIdentity,
        states: dict[str, tuple[str, str | None]],
    ) -> None:
        for component_id, (state, error_code) in sorted(states.items()):
            generation_id = identity.generations[component_id]
            latest = await (
                await conn.execute(
                    "SELECT sequence,state,error_code FROM public.bot_runtime_module_lifecycle_facts "
                    "WHERE bot_id=%s AND generation_id=%s ORDER BY sequence DESC LIMIT 1 FOR UPDATE",
                    (self.store.bot_id, generation_id),
                )
            ).fetchone()
            if latest is None:
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_PREPARED", "Runtime generation is not prepared", 409
                )
            if latest["state"] == state and latest["error_code"] == error_code:
                continue
            await conn.execute(
                "INSERT INTO public.bot_runtime_module_lifecycle_facts("
                "bot_id,generation_id,sequence,state,error_code,host_incarnation,host_owner_epoch) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    generation_id,
                    latest["sequence"] + 1,
                    state,
                    error_code,
                    lease.incarnation,
                    lease.owner_epoch,
                ),
            )

    async def publish(self, lease: HostLease, identity: RuntimeGraphIdentity) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            await self.store._host(conn, lease)
            graph = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_graphs WHERE bot_id=%s AND graph_id=%s",
                    (self.store.bot_id, identity.graph_id),
                )
            ).fetchone()
            if (
                graph is None
                or graph["host_incarnation"] != lease.incarnation
                or graph["host_owner_epoch"] != lease.owner_epoch
                or graph["descriptor_set_digest"] != identity.descriptor_set_digest
            ):
                raise ForestError("RUNTIME_GRAPH_STALE", "Runtime graph belongs to another host", 409)
            clock = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_graph_clock WHERE bot_id=%s FOR UPDATE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_graph_heads WHERE bot_id=%s FOR UPDATE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            if clock is None or head is None:
                raise ForestError("RUNTIME_GRAPH_NOT_PREPARED", "Runtime graph head is absent", 409)
            if head["graph_id"] == identity.graph_id:
                return head["revision"]
            if head["graph_id"] is not None:
                prior = await (
                    await conn.execute(
                        "SELECT component_id,generation_id FROM public.bot_runtime_module_generations "
                        "WHERE bot_id=%s AND graph_id=%s",
                        (self.store.bot_id, head["graph_id"]),
                    )
                ).fetchall()
                prior_identity = RuntimeGraphIdentity(
                    head["graph_id"], "", {row["component_id"]: row["generation_id"] for row in prior}
                )
                await self._append_states_in(
                    conn,
                    lease,
                    prior_identity,
                    {row["component_id"]: ("STOPPED", None) for row in prior},
                )
            await self._append_states_in(
                conn,
                lease,
                identity,
                {component_id: ("ACTIVE", None) for component_id in identity.generations},
            )
            revision = clock["revision"] + 1
            await conn.execute(
                "UPDATE public.bot_runtime_graph_clock SET revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s",
                (revision, self.store.bot_id),
            )
            await conn.execute(
                "INSERT INTO public.bot_runtime_graph_publications("
                "bot_id,revision,graph_id,operation_kind,host_incarnation,host_owner_epoch) "
                "VALUES (%s,%s,%s,'ACTIVATE',%s,%s)",
                (
                    self.store.bot_id,
                    revision,
                    identity.graph_id,
                    lease.incarnation,
                    lease.owner_epoch,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_runtime_graph_heads SET graph_id=%s,revision=%s,"
                "updated_at=clock_timestamp() WHERE bot_id=%s",
                (identity.graph_id, revision, self.store.bot_id),
            )
            return revision

    async def retire(self, lease: HostLease, identity: RuntimeGraphIdentity) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            await self.store._host(conn, lease)
            clock = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_graph_clock WHERE bot_id=%s FOR UPDATE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_graph_heads WHERE bot_id=%s FOR UPDATE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            if clock is None or head is None or head["graph_id"] != identity.graph_id:
                return head["revision"] if head is not None else 0
            await self._append_states_in(
                conn,
                lease,
                identity,
                {component_id: ("QUIESCING", None) for component_id in identity.generations},
            )
            revision = clock["revision"] + 1
            await conn.execute(
                "UPDATE public.bot_runtime_graph_clock SET revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s",
                (revision, self.store.bot_id),
            )
            await conn.execute(
                "INSERT INTO public.bot_runtime_graph_publications("
                "bot_id,revision,graph_id,operation_kind,host_incarnation,host_owner_epoch) "
                "VALUES (%s,%s,%s,'RETIRE',%s,%s)",
                (
                    self.store.bot_id,
                    revision,
                    identity.graph_id,
                    lease.incarnation,
                    lease.owner_epoch,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_runtime_graph_heads SET graph_id=NULL,revision=%s,"
                "updated_at=clock_timestamp() WHERE bot_id=%s",
                (revision, self.store.bot_id),
            )
            return revision

    async def record_states(
        self,
        lease: HostLease,
        identity: RuntimeGraphIdentity,
        states: dict[str, tuple[str, str | None]],
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            await self.store._host(conn, lease)
            await self._append_states_in(conn, lease, identity, states)

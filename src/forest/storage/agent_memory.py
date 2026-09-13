"""Bind one immutable memory query to a canonical Agent input cut.

Binding/config decisions are ordered CAS facts, never mergeable contributions.
The shared publication guard reads metadata and current source authority only.
It is also usable inside a Kernel tool transaction that already owns Bot locks.
"""

from types import SimpleNamespace

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_payloads import CONTENT_UNAVAILABLE_CODES
from forest.storage.memory_config import MemoryConfig
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore


class AgentMemoryStore:
    def __init__(self, agent, queries):
        self.agent, self.queries = agent, queries
        self.messages, self.store = agent.messages, agent.store

    async def _step_in(self, conn, actor, snapshot):
        binding, conversation, _, _, head = await self.agent._guard(conn, actor, snapshot["run_id"])
        step = await (
            await conn.execute(
                "SELECT * FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                (self.store.bot_id, head["current_step_id"]),
            )
        ).fetchone()
        if (
            head["phase"] != "PREPARING_MODEL"
            or step is None
            or step["logical_step_id"] != snapshot["logical_step_id"]
            or step["snapshot_digest"] != digest_json(snapshot)
        ):
            raise ForestError(
                "AGENT_MEMORY_INPUT_CHANGED", "Memory must use the frozen current model step", 409
            )
        return binding, conversation, step

    async def begin(self, actor, snapshot):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _, step = await self._step_in(conn, actor, snapshot)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_memory_bindings WHERE bot_id=%s AND logical_step_id=%s",
                    (self.store.bot_id, step["logical_step_id"]),
                )
            ).fetchone()
            if row is None:
                cfg = await (
                    await conn.execute(
                        "SELECT r.* FROM public.bot_memory_config_heads h JOIN public.bot_memory_config_revisions r "
                        "ON (r.bot_id,r.revision)=(h.bot_id,h.revision) WHERE h.bot_id=%s FOR SHARE OF h",
                        (self.store.bot_id,),
                    )
                ).fetchone()
                enabled = bool(cfg and cfg["values"]["enabled"])
                revision = cfg["revision"] if cfg else 0
                if enabled:
                    await MemoryConfig.guard_in(conn, actor.host, revision)
                # Only actual triggers already present in this immutable snapshot
                # may select query data; trailing inbox messages cannot advance it.
                trigger = max(
                    snapshot.get("triggers", []),
                    key=lambda t: (t["ingress_seq"], t["trigger_id"]),
                    default=None,
                )
                sources = []
                if enabled and trigger:
                    sources = [
                        r["source_unit_id"]
                        for r in await (
                            await conn.execute(
                                "SELECT source_unit_id FROM public.bot_memory_source_occurrences "
                                "WHERE bot_id=%s AND conversation_id=%s AND event_id=%s AND ingress_seq<=%s ORDER BY source_unit_id",
                                (
                                    self.store.bot_id,
                                    actor.conversation_id,
                                    trigger["event_id"],
                                    step["selected_input_end_seq"],
                                ),
                            )
                        ).fetchall()
                    ]
                query_id = (
                    (
                        "agent-query:"
                        + digest_json(
                            [
                                self.store.bot_id,
                                snapshot["run_id"],
                                step["input_revision"],
                                step["selected_input_end_seq"],
                                revision,
                                sources,
                            ]
                        )
                    )
                    if enabled
                    else None
                )
                row = await (
                    await conn.execute(
                        "INSERT INTO public.bot_agent_memory_bindings(bot_id,logical_step_id,run_id,snapshot_digest,input_cut,input_revision,"
                        "config_revision,query_id,source_unit_ids,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                        (
                            self.store.bot_id,
                            step["logical_step_id"],
                            snapshot["run_id"],
                            step["snapshot_digest"],
                            step["selected_input_end_seq"],
                            step["input_revision"],
                            revision,
                            query_id,
                            Jsonb(sources),
                            "PENDING" if enabled else "DISABLED",
                        ),
                    )
                ).fetchone()
            await self.messages._actor_final(conn, actor, binding)
            return row

    async def finish(self, actor, snapshot, *, error_code=None):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation, step = await self._step_in(conn, actor, snapshot)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_memory_bindings WHERE bot_id=%s AND logical_step_id=%s FOR UPDATE",
                    (self.store.bot_id, step["logical_step_id"]),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "AGENT_MEMORY_NOT_PREPARED", "Prepare the retrieval decision first", 409
                )
            bundle = None
            if row["status"] in {"PENDING", "BOUND"}:
                if error_code is None or row["status"] == "BOUND":
                    try:
                        bundle = await self.queries.read_in(
                            conn,
                            row["query_id"],
                            conversation=conversation,
                            input_cut=row["input_cut"],
                        )
                    except ForestError as exc:
                        if exc.code not in CONTENT_UNAVAILABLE_CODES:
                            raise
                        raise ForestError(
                            "AGENT_MEMORY_REVOKED", "Frozen evidence content is unavailable", 409
                        ) from None
                    if bundle["config_revision"] != row["config_revision"]:
                        raise ForestError(
                            "AGENT_MEMORY_INPUT_CHANGED",
                            "Evidence configuration differs from its decision",
                            409,
                        )
                elif (
                    not isinstance(error_code, str)
                    or not error_code.isascii()
                    or not 1 <= len(error_code) <= 128
                ):
                    raise ValueError("Memory failure code")
                if row["status"] == "PENDING":
                    await conn.execute(
                        "UPDATE public.bot_agent_memory_bindings SET status=%s,bundle_id=%s,bundle_digest=%s,error_code=%s "
                        "WHERE bot_id=%s AND logical_step_id=%s",
                        (
                            "BOUND" if bundle else "BYPASSED",
                            bundle["bundle_id"] if bundle else None,
                            bundle["digest"] if bundle else None,
                            error_code,
                            self.store.bot_id,
                            step["logical_step_id"],
                        ),
                    )
                elif (row["bundle_id"], row["bundle_digest"]) != (
                    bundle["bundle_id"],
                    bundle["digest"],
                ):
                    raise ForestError(
                        "AGENT_MEMORY_INPUT_CHANGED", "Evidence changed after binding", 409
                    )
            media = {}
            if bundle:
                refs = [
                    (item["source_unit_id"], part["part_id"])
                    for item in bundle["evidence"]
                    for part in item["parts"]
                    if part["type"] == "image" and part.get("availability") == "AVAILABLE"
                ]
                if refs:
                    try:
                        media = await self.queries.media.materialize_in(
                            conn,
                            sorted({unit for unit, _ in refs}),
                            part_refs=refs,
                            expected_deletion_epoch=bundle["deletion_epoch"],
                        )
                    except ForestError as exc:
                        if exc.code not in CONTENT_UNAVAILABLE_CODES:
                            raise
                        raise ForestError(
                            "AGENT_MEMORY_REVOKED", "Frozen evidence media is unavailable", 409
                        ) from None
            await self.messages._actor_final(conn, actor, binding)
            return bundle, media

    @staticmethod
    async def guard_run_in(conn, bot_id, run_id):
        """Caller owns the current Bot runtime lock; no transport or new selection.

        Source helpers need only a Bot identifier for this in-transaction read.
        This scope intentionally has no pool, Host acquisition or mutation API.
        """
        from forest.storage.agent_payloads import guard_run_in

        await guard_run_in(conn, bot_id, run_id)
        rows = await (
            await conn.execute(
                "SELECT DISTINCT q.query_id,q.config_revision,q.input_cut,q.conversation_id,q.source_unit_ids AS query_sources,"
                "q.runtime_fence,q.deletion_epoch,b.payload_id,b.source_unit_ids AS evidence_sources,b.digest "
                "FROM public.bot_agent_memory_bindings a JOIN public.bot_memory_queries q USING(bot_id,query_id) "
                "JOIN public.bot_memory_evidence_bundles b USING(bot_id,query_id) "
                "WHERE a.bot_id=%s AND a.run_id=%s AND a.status='BOUND'",
                (bot_id, run_id),
            )
        ).fetchall()
        if not rows:
            return
        scope = SimpleNamespace(bot_id=bot_id)
        sources = MemorySourceStore(SimpleNamespace(store=scope))
        payloads = PayloadStore(scope)
        runtime = await (
            await conn.execute("SELECT * FROM public.bot_runtime_state WHERE bot_id=%s", (bot_id,))
        ).fetchone()
        for row in rows:
            try:
                await MemoryConfig.guard_in(conn, scope, row["config_revision"])
                if not runtime["data_admission"] or any(
                    runtime[k] != v for k, v in row["runtime_fence"].items()
                ):
                    raise ForestError(
                        "MEMORY_QUERY_REVOKED", "Memory runtime authority changed", 409
                    )
                clock = await payloads.lock_in(conn)
                if clock["deletion_epoch"] != row["deletion_epoch"]:
                    raise ForestError("MEMORY_QUERY_REVOKED", "Memory deletion epoch changed", 409)
                conversation = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_conversations WHERE bot_id=%s AND conversation_id=%s",
                        (bot_id, row["conversation_id"]),
                    )
                ).fetchone()
                required = sorted(set(row["query_sources"]) | set(row["evidence_sources"]))
                current = await sources.authorize_sources_in(
                    conn,
                    conversation,
                    input_cut=row["input_cut"],
                    source_unit_ids=required,
                    limit=256,
                    include_payload=False,
                )
                if {s["source_unit_id"] for s in current["sources"]} != set(required):
                    raise ForestError(
                        "MEMORY_EVIDENCE_REVOKED",
                        "Memory evidence source is no longer authorized",
                        409,
                    )
                await payloads.require_available_in(
                    conn, [row["payload_id"]], expected_deletion_epoch=clock["deletion_epoch"]
                )
            except ForestError as exc:
                raise ForestError(
                    "AGENT_MEMORY_REVOKED",
                    "Frozen memory evidence lost authority: " + exc.code,
                    409,
                ) from None

    @staticmethod
    async def check_manifest_in(conn, bot_id, step, manifest):
        row = await (
            await conn.execute(
                "SELECT a.*,b.payload_id,b.deletion_epoch,b.status AS evidence_status,q.space_id,q.conversation_id "
                "FROM public.bot_agent_memory_bindings a LEFT JOIN public.bot_memory_queries q USING(bot_id,query_id) "
                "LEFT JOIN public.bot_memory_evidence_bundles b USING(bot_id,query_id) "
                "WHERE a.bot_id=%s AND a.logical_step_id=%s",
                (bot_id, step["logical_step_id"]),
            )
        ).fetchone()
        reference = manifest.get("memory_evidence")
        if row is None or row["status"] in {"DISABLED", "BYPASSED"}:
            if reference is not None:
                raise ForestError(
                    "AGENT_MEMORY_INPUT_CHANGED", "Model included unbound memory evidence", 409
                )
            return
        if row["status"] != "BOUND" or not isinstance(reference, dict):
            raise ForestError(
                "AGENT_MEMORY_NOT_READY", "Model needs its frozen memory decision", 409
            )
        expected = {
            k: row[k]
            for k in (
                "bundle_id",
                "payload_id",
                "query_id",
                "config_revision",
                "deletion_epoch",
                "input_cut",
                "conversation_id",
                "space_id",
            )
        }
        expected.update(digest=row["bundle_digest"], status=row["evidence_status"])
        if (
            any(reference.get(k) != v for k, v in expected.items())
            or row["snapshot_digest"] != step["snapshot_digest"]
        ):
            raise ForestError(
                "AGENT_MEMORY_INPUT_CHANGED", "Model evidence differs from its frozen binding", 409
            )
        await AgentMemoryStore.guard_run_in(conn, bot_id, row["run_id"])

"""Removable Agent bodies and host-owned transitive input dependencies.

Callers already own their Bot/runtime and business locks. No method here grants
execution authority or imports a body from a retired plaintext column.
"""

from types import SimpleNamespace

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore

CONTENT_UNAVAILABLE_CODES = frozenset(
    {
        "PAYLOAD_ERASED",
        "SOURCE_SUPPRESSED",
        "PAYLOAD_INCOMPLETE",
        "SOURCE_LINK_SCOPE_LIMIT",
    }
)


class AgentPayloadStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)

    async def epoch_in(self, conn):
        return (await self.payloads.lock_in(conn))["deletion_epoch"]

    async def read_in(self, conn, payload_id, *, required=True, binary=False):
        if payload_id is None:
            if required:
                raise ForestError(
                    "AGENT_PAYLOAD_MISSING", "Agent body has no payload identity", 409
                )
            return None
        try:
            epoch = await self.epoch_in(conn)
            reader = self.payloads.read_bytes_in if binary else self.payloads.read_json_in
            return await reader(conn, payload_id, expected_deletion_epoch=epoch)
        except ForestError as exc:
            if exc.code not in CONTENT_UNAVAILABLE_CODES:
                raise
            if required:
                raise ForestError(
                    "AGENT_MEMORY_REVOKED", "Agent source content is unavailable", 409
                ) from None
            return None

    async def put_in(self, conn, kind, owner, value, *, parents=(), binary=False):
        epoch = await self.epoch_in(conn)
        writer = self.payloads.put_bytes_in if binary else self.payloads.put_json_in
        try:
            ref = await writer(
                conn,
                owner_kind=kind,
                owner_id=owner,
                value=value,
                parents=tuple(sorted(set(parents))),
                expected_deletion_epoch=epoch,
            )
            # An immutable receipt can survive erasure; it is never a new permission.
            await self.payloads.require_available_in(
                conn, (ref.payload_id,), expected_deletion_epoch=epoch
            )
        except ForestError as exc:
            if exc.code in CONTENT_UNAVAILABLE_CODES:
                raise ForestError(
                    "AGENT_MEMORY_REVOKED", "Agent source content is unavailable", 409
                ) from None
            raise
        return ref.payload_id

    async def require_in(self, conn, refs):
        try:
            await self.payloads.require_available_in(
                conn, refs, expected_deletion_epoch=await self.epoch_in(conn)
            )
        except ForestError as exc:
            if exc.code in CONTENT_UNAVAILABLE_CODES:
                raise ForestError(
                    "AGENT_MEMORY_REVOKED", "Agent content is unavailable", 409
                ) from None
            raise

    async def hydrate_step_in(self, conn, step, *, required=True):
        if step is None:
            return None
        result = dict(step)
        for field in ("snapshot", "output", "closed_parts"):
            ref = step.get(field + "_payload_id")
            result[field] = await self.read_in(
                conn, ref, required=required and (field == "snapshot" or ref is not None)
            )
        return result

    async def hydrate_tool_in(self, conn, tool, *, required=True):
        if tool is None:
            return None
        result = dict(tool)
        for field in ("arguments", "result"):
            ref = tool.get(field + "_payload_id")
            result[field] = await self.read_in(
                conn, ref, required=required and (field == "arguments" or ref is not None)
            )
        if tool.get("result_digest") is not None and not tool.get("result_metadata"):
            # Scheduling status may already be DISPATCHING/CANCELLED after a
            # NOT_SENT result. Verify against that observation's frozen outcome.
            # A non-required erased body remains metadata-only, never a value.
            if required or result["result"] is not None or tool.get("result_payload_id") is None:
                if (
                    tool.get("result_outcome") not in {"SUCCEEDED", "FAILED", "NOT_SENT", "UNKNOWN"}
                    or digest_json([tool["result_outcome"], result["result"]])
                    != tool["result_digest"]
                ):
                    raise ForestError(
                        "AGENT_PAYLOAD_IDENTITY_CONFLICT",
                        "Tool body differs from its observed result",
                        409,
                    )
        return result

    async def hydrate_attempt_in(self, conn, attempt, *, required=True):
        if attempt is None:
            return None
        result = dict(attempt)
        result["request_body"] = await self.read_in(
            conn, attempt["request_payload_id"], required=required, binary=True
        )
        if "manifest_payload_id" in attempt:
            result["manifest"] = await self.read_in(
                conn, attempt["manifest_payload_id"], required=required
            )
        result["result"] = await self.read_in(
            conn,
            attempt.get("result_payload_id"),
            required=required and attempt.get("result_payload_id") is not None,
        )
        if result["result"] is None and attempt.get("result_metadata"):
            result["result"] = attempt["result_metadata"]
        return result

    async def read_output_in(self, conn, logical_step_id):
        row = await (
            await conn.execute(
                "SELECT output_payload_id FROM public.bot_agent_steps WHERE bot_id=%s AND logical_step_id=%s",
                (self.store.bot_id, logical_step_id),
            )
        ).fetchone()
        return await self.read_in(conn, row["output_payload_id"] if row else None)

    async def step_parents_in(self, conn, step_id, *, include_output=False):
        row = await (
            await conn.execute(
                "SELECT snapshot_payload_id,output_payload_id FROM public.bot_agent_steps "
                "WHERE bot_id=%s AND logical_step_id=%s",
                (self.store.bot_id, step_id),
            )
        ).fetchone()
        if row is None or row["snapshot_payload_id"] is None:
            raise ForestError("AGENT_PAYLOAD_MISSING", "Step source manifest is missing", 409)
        refs = [row["snapshot_payload_id"]]
        if include_output and row["output_payload_id"]:
            refs.append(row["output_payload_id"])
        evidence = await (
            await conn.execute(
                "SELECT b.payload_id FROM public.bot_agent_memory_bindings a "
                "JOIN public.bot_memory_evidence_bundles b USING(bot_id,bundle_id) "
                "WHERE a.bot_id=%s AND a.logical_step_id=%s AND a.status='BOUND'",
                (self.store.bot_id, step_id),
            )
        ).fetchall()
        refs.extend(r["payload_id"] for r in evidence)
        return tuple(sorted(set(refs)))

    async def guard_run_in(self, conn, run_id):
        # A Run is bounded by max_steps (<=128). Retain all admitted dependency
        # closures, including steps whose context has since been clipped.
        rows = await (
            await conn.execute(
                "SELECT snapshot_payload_id,output_payload_id,closed_parts_payload_id "
                "FROM public.bot_agent_steps WHERE bot_id=%s AND run_id=%s ORDER BY step_seq",
                (self.store.bot_id, run_id),
            )
        ).fetchall()
        epoch = await self.epoch_in(conn)
        try:
            for row in rows:
                if row["snapshot_payload_id"] is None:
                    raise ForestError(
                        "AGENT_PAYLOAD_MISSING", "Step source manifest is missing", 409
                    )
                await self.payloads.require_available_in(
                    conn, [r for r in row.values() if r is not None], expected_deletion_epoch=epoch
                )
            tools = await (
                await conn.execute(
                    "SELECT arguments_payload_id,result_payload_id FROM public.bot_agent_tools "
                    "WHERE bot_id=%s AND run_id=%s ORDER BY logical_step_id,call_index",
                    (self.store.bot_id, run_id),
                )
            ).fetchall()
            for row in tools:
                await self.payloads.require_available_in(
                    conn, [r for r in row.values() if r is not None], expected_deletion_epoch=epoch
                )
        except ForestError as exc:
            if exc.code in CONTENT_UNAVAILABLE_CODES:
                raise ForestError(
                    "AGENT_MEMORY_REVOKED", "A frozen Agent dependency is unavailable", 409
                ) from None
            raise

    async def tool_result_in(self, conn, tool, value, *, status, parents=()):
        inherited = list(parents) + list(
            await self.step_parents_in(conn, tool["logical_step_id"], include_output=True)
        )
        if tool.get("arguments_payload_id"):
            inherited.append(tool["arguments_payload_id"])
        return await self.put_in(
            conn,
            "agent.tool",
            "result:" + digest_json([tool["operation_id"], status, value]),
            value,
            parents=inherited,
        )


async def guard_run_in(conn, bot_id, run_id):
    """Kernel-safe: caller holds runtime; acquire no Binding/Conversation locks."""
    await AgentPayloadStore(SimpleNamespace(bot_id=bot_id)).guard_run_in(conn, run_id)

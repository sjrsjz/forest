"""Trusted Kernel content boundaries and long-lived Worker exposure fences.

The BotStore owns authority and transactions. This helper acquires only state
heads, exposure heads and the memory clock, in that order, and performs no IPC.
Worker RPC arguments cannot select payload identities or source domains.
"""

from __future__ import annotations

from functools import wraps

from forest.errors import ForestError
from forest.packages import digest_json


async def _invalidate_capacity(store, lease, proof):
    """Reprove a monotone set overflow after racing same-member deliveries.

    A delivery of an already known payload advances the exposure receipt revision
    without freeing capacity. Re-read that revision under current Host authority
    and retry a bounded number of times; never reuse an unchecked head snapshot.
    """
    from forest.storage.exposures import ExposureStore

    exposures = ExposureStore(store)
    revision = proof._revision
    candidates = (
        {"candidate_payload_refs": proof._candidate_payload_refs}
        if proof._reason == "CAPACITY"
        else {"candidate_source_refs": proof._candidate_source_refs}
    )
    for attempt in range(3):
        try:
            await exposures.invalidate(
                lease, proof._generation_id, expected_revision=revision,
                reason=proof._reason, **candidates,
            )
            return
        except ForestError as exc:
            if exc.code != "EXPOSURE_CONFLICT" or attempt == 2:
                raise
        async with store.pool.connection() as conn, conn.transaction():
            generation = await store._generation(conn, proof._generation_id)
            await store._routes(conn, [generation["component_id"]])
            await store._host(conn, lease)
            head = await exposures.ensure_generation_in(conn, proof._generation_id)
            revision = head["revision"]
            await store._host(conn, lease)


def capacity_boundary(method):
    """Persist a proven capacity failure after the content transaction rolls back."""
    @wraps(method)
    async def boundary(self, authority, *args, **kwargs):
        try:
            return await method(self, authority, *args, **kwargs)
        except ForestError as exc:
            from forest.storage.exposures import ExposureCapacityError

            # A Worker may return a public error with the same code/details.
            # Only our actual in-process content guard carries trusted proof.
            if not isinstance(exc, ExposureCapacityError):
                raise
            from forest.storage.bot import CallGrant, HostLease

            lease = authority
            if isinstance(authority, CallGrant):
                lease = HostLease(authority.bot_id, authority.host_id,
                                  authority.host_incarnation, authority.owner_epoch,
                                  authority.deadline_at)
            if not isinstance(lease, HostLease):
                raise
            try:
                await _invalidate_capacity(self, lease, exc)
            except ForestError as invalidation:
                # A revoked owner or exhausted bounded retry leaves this input
                # rejected. Never replace that failure with a successful receipt.
                if invalidation.code not in {
                    "EXPOSURE_CONFLICT", "STALE_HOST", "STALE_GENERATION",
                    "GENERATION_EXPOSURE_REVOKED", "EXPOSURE_INVALIDATION_UNPROVEN",
                }:
                    raise
            raise
    return boundary


class KernelContentStore:
    def __init__(self, store):
        from forest.storage.exposures import ExposureStore
        from forest.storage.kernel_payloads import KernelPayloadStore
        from forest.storage.payloads import PayloadStore

        self.store = store
        self.payloads = PayloadStore(store)
        self.bodies = KernelPayloadStore(store)
        self.exposures = ExposureStore(store)

    async def lock_in(self, conn, generation_ids, component_ids=()):
        for component in sorted(set(component_ids)):
            await conn.execute(
                "SELECT component_id FROM public.bot_component_state_heads "
                "WHERE bot_id=%s AND component_id=%s FOR UPDATE",
                (self.store.bot_id, component),
            )
        return await self.exposures.lock_generations_in(conn, tuple(set(generation_ids)))

    async def epoch_in(self, conn):
        return (await self.payloads.lock_in(conn))["deletion_epoch"]

    async def guard_in(self, conn, generation_ids, *, phase="commit"):
        for generation in sorted(set(generation_ids)):
            await self.exposures.guard_generation_in(conn, generation, phase=phase)

    async def parents_in(self, conn, generation_id):
        return await self.exposures.generation_parents_in(conn, generation_id)

    async def expose_in(self, conn, generation_id, kind, subject, refs):
        refs = tuple(sorted(set(refs)))
        if not refs:
            return
        return await self.exposures.expose_in(
            conn, generation_id,
            "exposure:" + digest_json([generation_id, kind, subject]),
            kind, refs, subject,
        )

    async def input_in(self, conn, generation_id, call_id, value, *, parents=(),
                       parent_generation=None, agent_tool_id=None):
        from forest.storage.source_domains import SourceDomainStore

        await self.guard_in(conn, [generation_id], phase="admission")
        inherited = set(parents)
        if parent_generation is not None:
            inherited.update(await self.parents_in(conn, parent_generation))
        if agent_tool_id is not None:
            from forest.storage.agent_payloads import AgentPayloadStore

            tool = await (await conn.execute(
                "SELECT logical_step_id,arguments_payload_id FROM public.bot_agent_tools "
                "WHERE bot_id=%s AND operation_id=%s",
                (self.store.bot_id, agent_tool_id),
            )).fetchone()
            if tool is None or tool["arguments_payload_id"] is None:
                raise ForestError("AGENT_TOOL_REVOKED", "Tool input has no admitted body", 403)
            inherited.add(tool["arguments_payload_id"])
            inherited.update(await AgentPayloadStore(self.store).step_parents_in(
                conn, tool["logical_step_id"], include_output=True,
            ))
        epoch = await self.epoch_in(conn)
        source = await SourceDomainStore(self.store).source_in(conn, "SERVICE_INPUT", call_id)
        ref = await self.bodies.put_call_input_in(
            conn, call_id, value, parents=tuple(sorted(inherited)), sources=(source,), epoch=epoch,
        )
        await self.expose_in(conn, generation_id, "CALL_INPUT", call_id, [ref.payload_id])
        return ref

    async def hydrate_call_in(self, conn, row):
        return await self.bodies.hydrate_call_in(conn, row, epoch=await self.epoch_in(conn))

    async def nested_receipt_in(self, conn, parent, nested):
        nested = await self.hydrate_call_in(conn, nested)
        if nested is not None and nested["status"] == "COMPLETED":
            await self.expose_in(conn, parent.generation_id, "NESTED_RESULT", nested["call_id"],
                                 [nested["result_payload_id"]])
        return nested

    async def state_rows_in(self, conn, component_id, *, required=False):
        """Removed values preserve revision fences but never become JSON null."""
        epoch = await self.epoch_in(conn)
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_component_state WHERE bot_id=%s AND component_id=%s ORDER BY key",
            (self.store.bot_id, component_id),
        )).fetchall()
        return [await self.bodies.hydrate_state_in(conn, row, epoch=epoch, required=required)
                for row in rows]

    async def state_snapshot_parents_in(self, conn, generation_id, subject, rows):
        """Freeze at most four deterministic pages of actual readable values.

        The pages carry every value's exact parent identity. They reduce direct
        exposure membership, never the transitive source/deletion dependency.
        Existing tombstones and unavailable values provide no restorable body.
        """
        active = sorted(
            (row for row in rows if not row["deleted"] and row["payload_state"] == "ACTIVE"),
            key=lambda row: row["key"],
        )
        if len(active) > 512:
            raise ForestError("STATE_MIGRATION_INVALID", "State snapshot exceeds 512 live values", 422)
        epoch = await self.epoch_in(conn)
        refs = []
        for offset in range(0, len(active), 128):
            # Re-hydrate inside the final transaction before copying content;
            # stale caller values cannot become a new source-free snapshot.
            page_rows = [await self.bodies.hydrate_state_in(conn, row, epoch=epoch)
                         for row in active[offset:offset + 128]]
            page = {row["key"]: {"revision": row["revision"], "value": row["value"]}
                    for row in page_rows}
            ref = await self.bodies.put_in(
                conn, "state.migration",
                "snapshot:" + digest_json([generation_id, subject, offset // 128, digest_json(page)]),
                page, parents=tuple(row["value_payload_id"] for row in page_rows), epoch=epoch,
            )
            refs.append(ref.payload_id)
        return tuple(refs)

    @staticmethod
    def source_rows(rows):
        result = {}
        for row in rows:
            value = {"revision": row["revision"], "value": row["value"], "deleted": row["deleted"]}
            if row["payload_state"] == "UNAVAILABLE":
                value.update(unavailable=True, value_digest=row["value_digest"])
            result[row["key"]] = value
        return result

    async def configuration_in(self, conn, generation):
        epoch = await self.epoch_in(conn)
        value = await self.bodies.read_in(conn, generation["config_payload_id"], epoch=epoch)
        self.bodies._verify(value, generation["config_payload_digest"])
        return {**generation, "config": value["config"], "state_value_schema": value["state_value_schema"]}

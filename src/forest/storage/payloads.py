"""Host-owned removable content with permanent identities and source closure.

The caller supplies *trusted* source dependencies; this layer does not authorize
Conversation input or grant a Worker access to arbitrary payload IDs. All `_in`
methods require an existing transaction and the caller's normal ownership locks.
Lock order: existing route/runtime/host/Conversation -> memory clock -> payload.
Legacy repositories still have plaintext copies until their retention migration.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore, HostLease

MAX_BYTES = 32 * 1024 * 1024
MAX_SOURCES = 4096
MAX_PARENTS = 256
OWNER_KINDS = frozenset({
    "memory.source", "memory.assertion", "memory.artifact", "memory.evidence",
    "memory.query", "memory.embedding", "memory.vector", "memory.index",
    "media.source", "media.capture", "media.variant", "model.snapshot", "model.manifest",
    "model.request", "model.result", "agent.tool", "agent.observation", "agent.child",
    "context.parts", "reply.parts", "delivery.payload", "delivery.request", "delivery.receipt",
    "kernel.input", "kernel.result", "state.value", "state.migration", "control.input", "control.result", "control.config",
    "control.audit", "control.outbox", "job.definition", "shell.arguments", "component.event",
    "component.job",
})


def _identifier(value: str) -> str:
    if (not isinstance(value, str) or not value or len(value.encode()) > 256
            or any(ord(c) < 32 for c in value)):
        raise ForestError("INVALID_PAYLOAD_REFERENCE", "Invalid payload reference", 422)
    return value


def _json(value: Any) -> bytes:
    try:
        result = json.dumps(value, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False, allow_nan=False).encode()
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ForestError("INVALID_PAYLOAD", "Payload must be bounded JSON", 422) from exc
    if len(result) > MAX_BYTES:
        raise ForestError("PAYLOAD_TOO_LARGE", "Payload exceeds storage limit", 413)
    return result


@dataclass(frozen=True)
class PayloadRef:
    payload_id: str
    digest: str
    encoding: str
    byte_length: int
    state: str


class PayloadStore:
    def __init__(self, store: BotStore):
        self.store = store

    async def lock_in(self, conn) -> dict:
        await conn.execute("INSERT INTO public.bot_memory_clock(bot_id) VALUES (%s) "
                           "ON CONFLICT DO NOTHING", (self.store.bot_id,))
        return await (await conn.execute(
            "SELECT * FROM public.bot_memory_clock WHERE bot_id=%s FOR UPDATE",
            (self.store.bot_id,),
        )).fetchone()

    async def _epoch_in(self, conn, expected: int) -> dict:
        if type(expected) is not int or expected < 0:
            raise ForestError("INVALID_DELETION_EPOCH", "Expected deletion epoch is required", 422)
        clock = await self.lock_in(conn)
        if clock["deletion_epoch"] != expected:
            raise ForestError("DELETION_CONFLICT", "Memory deletion epoch changed", 409)
        return clock

    async def epoch(self, lease: HostLease) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            clock = await self.lock_in(conn)
            await self.store._host(conn, lease)
            return clock["deletion_epoch"]

    @staticmethod
    def _sources(sources) -> tuple[SourceRef, ...]:
        values = tuple(sources)
        if len(values) > MAX_SOURCES or any(type(value) is not SourceRef for value in values):
            raise ForestError("INVALID_SOURCE_MANIFEST", "Invalid source dependency set", 422)
        return tuple(sorted(set(values)))

    async def _check_sources_in(self, conn, sources: tuple[SourceRef, ...]):
        if not sources:
            return
        # Exact platform aliases discovered after body publication live in a
        # separate immutable ledger. Reading or writing an old manifest must
        # still consult their transitive prerequisites; the manifest is not edited.
        sources = await self.source_dependency_closure_in(conn, sources)
        rows = await (await conn.execute(
            "SELECT source_unit_id,part_id FROM public.bot_payload_suppression "
            "WHERE bot_id=%s AND source_unit_id=ANY(%s)",
            (self.store.bot_id, sorted({s.source_unit_id for s in sources})),
        )).fetchall()
        targets = {(row["source_unit_id"], row["part_id"]) for row in rows}
        for source in sources:
            if any(unit == source.source_unit_id and
                   (part == "*" or source.part_id == "*" or part == source.part_id)
                   for unit, part in targets):
                raise ForestError("SOURCE_SUPPRESSED", "A source dependency was removed", 409)

    async def source_dependency_closure_in(
        self, conn, sources, *, maximum=None, fail_on_limit=True
    ):
        """Expand late source links, avoiding recursive SQL when none can match."""
        maximum = MAX_SOURCES if maximum is None else maximum
        sources = tuple(sorted(set(sources)))
        if not sources:
            return ()
        linked = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_payload_source_links WHERE bot_id=%s "
                "AND dependent_source_unit_id=ANY(%s) LIMIT 1",
                (self.store.bot_id, sorted({source.source_unit_id for source in sources})),
            )
        ).fetchone()
        if linked is None:
            return sources
        rows = await (
            await conn.execute(
                "SELECT * FROM public.forest_source_dependency_closure(%s,%s,%s)",
                (self.store.bot_id, Jsonb([asdict(source) for source in sources]), maximum),
            )
        ).fetchall()
        if len(rows) > maximum and fail_on_limit:
            raise ForestError(
                "SOURCE_LINK_SCOPE_LIMIT", "Source dependency closure exceeds its limit", 422
            )
        return tuple(sorted(SourceRef(**row) for row in rows))

    async def _header_in(self, conn, payload_id: str) -> dict:
        _identifier(payload_id)
        row = await (await conn.execute(
            "SELECT * FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
            (self.store.bot_id, payload_id),
        )).fetchone()
        if row is None:
            raise ForestError("PAYLOAD_NOT_FOUND", "Payload does not exist in this Bot", 404)
        return row

    @staticmethod
    async def _effective_source_relation_in(conn):
        row = await (
            await conn.execute(
                "SELECT to_regclass('public.forest_payload_effective_sources') AS name"
            )
        ).fetchone()
        return (
            "public.forest_payload_effective_sources"
            if row["name"] is not None
            else "public.bot_payload_sources"
        )

    async def effective_sources_in(self, conn, payload_id):
        """Original sources plus proven prerequisites of frozen old copies."""
        relation = await self._effective_source_relation_in(conn)
        rows = await (await conn.execute(
            f"SELECT source_unit_id,source_revision,part_id FROM {relation} "
            "WHERE bot_id=%s AND payload_id=%s LIMIT %s",
            (self.store.bot_id, payload_id, MAX_SOURCES + 1),
        )).fetchall()
        if len(rows) > MAX_SOURCES:
            raise ForestError("SOURCE_LINK_SCOPE_LIMIT", "Source dependency union exceeds its limit", 422)
        return tuple(SourceRef(**row) for row in rows)

    async def require_available_in(self, conn, payload_ids, *, expected_deletion_epoch: int):
        await self._epoch_in(conn, expected_deletion_epoch)
        ids = tuple(payload_ids)
        if len(ids) > MAX_PARENTS:
            raise ForestError("PAYLOAD_DEPENDENCY_LIMIT", "Too many payload dependencies", 422)
        identities = tuple(sorted(set(ids)))
        for payload_id in identities:
            _identifier(payload_id)
        if not identities:
            return
        legacy = {
            row["payload_id"]
            for row in await (
                await conn.execute(
                    "SELECT DISTINCT payload_id FROM public.bot_payload_legacy_closure "
                    "WHERE bot_id=%s AND payload_id=ANY(%s)",
                    (self.store.bot_id, list(identities)),
                )
            ).fetchall()
        }
        for payload_id in identities:
            row = await self._header_in(conn, payload_id)
            if row["state"] != "ACTIVE":
                raise ForestError("PAYLOAD_ERASED", "Payload content is no longer available", 410)
            await self._check_sources_in(conn, await self.effective_sources_in(conn, payload_id))
            if payload_id in legacy:
                available = await (await conn.execute(
                    "SELECT public.forest_payload_dependencies_available(%s,%s) AS available",
                    (self.store.bot_id, payload_id),
                )).fetchone()
                if not available["available"]:
                    raise ForestError(
                        "PAYLOAD_ERASED", "A legacy content prerequisite is unavailable", 410
                    )
            exists = await (await conn.execute(
                "SELECT 1 FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=%s",
                (self.store.bot_id, payload_id),
            )).fetchone()
            if not exists:
                raise ForestError("PAYLOAD_INCOMPLETE", "Payload body was not admitted", 409)

    async def _put_in(self, conn, *, owner_kind, owner_id, content, encoding, sources,
                      parents, expected_deletion_epoch) -> PayloadRef:
        if owner_kind not in OWNER_KINDS:
            raise ForestError("INVALID_PAYLOAD_OWNER", "Unsupported payload owner kind", 422)
        _identifier(owner_id)
        if type(content) is not bytes or len(content) > MAX_BYTES:
            raise ForestError("PAYLOAD_TOO_LARGE", "Payload exceeds storage limit", 413)
        source_set = self._sources(sources)
        parent_set = tuple(sorted(set(_identifier(p) for p in parents)))
        if len(parent_set) > MAX_PARENTS:
            raise ForestError("PAYLOAD_DEPENDENCY_LIMIT", "Too many payload dependencies", 422)
        # Serialize all content admissions and suppressions. Parent references
        # are flattened once, so deleting a source finds the transitive closure
        # without graph traversal while holding database locks.
        await self.lock_in(conn)
        # The caller's direct manifest is bounded above. Inherited manifests
        # overlap heavily across long-lived Worker inputs, so budget their
        # unique union while accumulating rather than an expanded duplicate list.
        source_union = set(source_set)
        original_union = set(source_set)
        for parent in parent_set:
            parent_header = await self._header_in(conn, parent)
            original_union.update(SourceRef(**item) for item in parent_header["manifest"]["sources"])
            for source in await self.effective_sources_in(conn, parent):
                source_union.add(source)
                if len(source_union) > MAX_SOURCES:
                    raise ForestError("SOURCE_LINK_SCOPE_LIMIT", "Source dependency union exceeds its limit", 422)
        source_set = tuple(sorted(source_union))
        manifest = {"sources": [asdict(s) for s in source_set], "parents": list(parent_set)}
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        input_digest = digest_json([owner_kind, owner_id, encoding, digest, manifest])
        payload_id = "payload:" + digest_json([self.store.bot_id, owner_kind, owner_id])
        row = await (await conn.execute(
            "SELECT * FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
            (self.store.bot_id, payload_id),
        )).fetchone()
        if row:
            if row["input_digest"] != input_digest:
                # A pre-044 parent's immutable manifest can gain a separately
                # proven configuration prerequisite. Never rewrite that old
                # child receipt to match a new flattening. Accept only when the
                # original algorithm reproduces its complete admitted digest:
                # same bytes, encoding, owner, direct parents and direct input
                # sources plus those parents' actual original manifests.
                original_manifest = {
                    "sources": [asdict(source) for source in sorted(original_union)],
                    "parents": list(parent_set),
                }
                original_digest = digest_json([owner_kind, owner_id, encoding, digest, original_manifest])
                if row["input_digest"] != original_digest:
                    raise ForestError("PAYLOAD_CONFLICT", "Payload owner already has different content", 409)
            # A receipt survives deletion. Returning its state never restores a
            # body and does not authorize reading or dispatching old content.
            return PayloadRef(payload_id, row["digest"], row["encoding"], row["byte_length"], row["state"])
        await self._epoch_in(conn, expected_deletion_epoch)
        await self.require_available_in(conn, parent_set, expected_deletion_epoch=expected_deletion_epoch)
        await self._check_sources_in(conn, source_set)
        if payload_id in parent_set:
            raise ForestError("PAYLOAD_DEPENDENCY_CYCLE", "Payload cannot depend on itself", 422)
        await conn.execute(
            "INSERT INTO public.bot_payload_headers(bot_id,payload_id,owner_kind,owner_id,encoding,digest,"
            "byte_length,input_digest,manifest,accepted_deletion_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (self.store.bot_id, payload_id, owner_kind, owner_id, encoding, digest, len(content),
             input_digest, Jsonb(manifest), expected_deletion_epoch),
        )
        for source in source_set:
            await conn.execute(
                "INSERT INTO public.bot_payload_sources(bot_id,payload_id,source_unit_id,source_revision,part_id) "
                "VALUES (%s,%s,%s,%s,%s)",
                (self.store.bot_id, payload_id, source.source_unit_id, source.source_revision, source.part_id),
            )
        for parent in parent_set:
            await conn.execute(
                "INSERT INTO public.bot_payload_derivations(bot_id,payload_id,parent_payload_id) VALUES (%s,%s,%s)",
                (self.store.bot_id, payload_id, parent),
            )
        await conn.execute(
            "INSERT INTO public.bot_payload_bodies(bot_id,payload_id,content) VALUES (%s,%s,%s)",
            (self.store.bot_id, payload_id, content),
        )
        return PayloadRef(payload_id, digest, encoding, len(content), "ACTIVE")

    async def put_json_in(self, conn, *, owner_kind, owner_id, value, sources=(), parents=(),
                          expected_deletion_epoch: int) -> PayloadRef:
        return await self._put_in(conn, owner_kind=owner_kind, owner_id=owner_id, content=_json(value),
                                  encoding="json", sources=sources, parents=parents,
                                  expected_deletion_epoch=expected_deletion_epoch)

    async def import_legacy_json_in(self, conn, *, owner_kind, owner_id, value,
                                     sources=(), parents=()) -> PayloadRef:
        """Migration-only admission, including permanently suppressed identities."""
        from forest.storage.migration_v1 import import_payload_in

        return await import_payload_in(conn, self.store, owner_kind=owner_kind, owner_id=owner_id,
                                       value=value, encoding="json", sources=sources, parents=parents)

    async def import_legacy_bytes_in(self, conn, *, owner_kind, owner_id, value: bytes,
                                      sources=(), parents=()) -> PayloadRef:
        from forest.storage.migration_v1 import import_payload_in

        return await import_payload_in(conn, self.store, owner_kind=owner_kind, owner_id=owner_id,
                                       value=value, encoding="bytes", sources=sources, parents=parents)

    async def put_bytes_in(self, conn, *, owner_kind, owner_id, value: bytes, sources=(), parents=(),
                           expected_deletion_epoch: int) -> PayloadRef:
        return await self._put_in(conn, owner_kind=owner_kind, owner_id=owner_id, content=value,
                                  encoding="bytes", sources=sources, parents=parents,
                                  expected_deletion_epoch=expected_deletion_epoch)

    async def put_json(self, lease: HostLease, **kwargs) -> PayloadRef:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            result = await self.put_json_in(conn, **kwargs)
            await self.store._host(conn, lease)
            return result

    async def put_bytes(self, lease: HostLease, **kwargs) -> PayloadRef:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            result = await self.put_bytes_in(conn, **kwargs)
            await self.store._host(conn, lease)
            return result

    async def read_bytes_in(self, conn, payload_id: str, *, expected_deletion_epoch: int) -> bytes:
        await self.require_available_in(conn, (payload_id,), expected_deletion_epoch=expected_deletion_epoch)
        row = await (await conn.execute(
            "SELECT content FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=%s",
            (self.store.bot_id, payload_id),
        )).fetchone()
        return bytes(row["content"])

    async def read_json_in(self, conn, payload_id: str, *, expected_deletion_epoch: int) -> Any:
        data = await self.read_bytes_in(conn, payload_id, expected_deletion_epoch=expected_deletion_epoch)
        header = await self._header_in(conn, payload_id)
        if header["encoding"] != "json":
            raise ForestError("PAYLOAD_ENCODING_MISMATCH", "Payload is not JSON", 409)
        return json.loads(data)

    async def read_json(self, lease: HostLease, payload_id: str, *, expected_deletion_epoch: int):
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            result = await self.read_json_in(conn, payload_id, expected_deletion_epoch=expected_deletion_epoch)
            await self.store._host(conn, lease)
            return result

    def _forget_input(self, targets, expected_deletion_epoch):
        refs = self._sources(targets)
        values = [{"source_unit_id": unit, "part_id": part}
                  for unit, part in sorted({(s.source_unit_id, s.part_id) for s in refs})]
        if not values:
            raise ForestError("INVALID_FORGET_TARGET", "At least one source is required", 422)
        if type(expected_deletion_epoch) is not int or expected_deletion_epoch < 0:
            raise ForestError("INVALID_DELETION_EPOCH", "Expected deletion epoch is required", 422)
        return values, digest_json([expected_deletion_epoch, values])

    async def preview_forget_in(self, conn, targets, *, expected_deletion_epoch: int) -> dict:
        """Resolve the exact durable target set while holding the deletion clock."""
        requested, digest = self._forget_input(targets, expected_deletion_epoch)
        from forest.storage.migration_v1 import legacy_source_ref

        legacy = legacy_source_ref(self.store.bot_id).source_unit_id
        clock = await self._epoch_in(conn, expected_deletion_epoch)
        values = list(requested)
        legacy_used = await (await conn.execute(
            "SELECT 1 FROM public.bot_payload_sources WHERE bot_id=%s AND source_unit_id=%s LIMIT 1",
            (self.store.bot_id, legacy),
        )).fetchone()
        # Older SourcePayloads already have immutable manifests, so an
        # incomplete reply cannot have this marker appended in place.
        # Name its own source explicitly in the durable deletion plan.
        source_table = await (await conn.execute(
            "SELECT to_regclass('public.bot_memory_source_units') AS relation"
        )).fetchone()
        incomplete = []
        if source_table["relation"]:
            incomplete = await (await conn.execute(
                "SELECT u.source_unit_id FROM public.bot_memory_source_units u "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE u.bot_id=%s AND NOT u.lineage_complete AND h.state='ACTIVE' "
                "ORDER BY u.source_unit_id LIMIT %s",
                (self.store.bot_id, MAX_SOURCES + 1),
            )).fetchall()
        conservative = bool(legacy_used or incomplete)
        if conservative:
            values = sorted({(t["source_unit_id"], t["part_id"]) for t in values}
                            | {(legacy, "*")}
                            | {(r["source_unit_id"], "*") for r in incomplete})
            values = [{"source_unit_id": unit, "part_id": part} for unit, part in values]
            if len(values) > MAX_SOURCES:
                raise ForestError("FORGET_SCOPE_LIMIT", "Legacy deletion closure exceeds source limit", 422)
        linked = await (await conn.execute(
            "SELECT * FROM public.forest_source_dependent_closure(%s,%s,%s)",
            (self.store.bot_id, Jsonb(values), MAX_SOURCES),
        )).fetchall()
        if len(linked) > MAX_SOURCES:
            raise ForestError("FORGET_SCOPE_LIMIT", "Exact alias deletion closure exceeds source limit", 422)
        values = sorted(
            ({"source_unit_id": row["source_unit_id"], "part_id": row["part_id"]}
             for row in linked),
            key=lambda item: (item["source_unit_id"], item["part_id"]),
        )
        source_relation = await self._effective_source_relation_in(conn)
        affected = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_payload_headers h WHERE h.bot_id=%s "
            "AND h.state='ACTIVE' AND EXISTS(SELECT 1 FROM " + source_relation + " s,"
            "jsonb_array_elements(%s::jsonb) t WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id "
            "AND s.source_unit_id=t->>'source_unit_id' "
            "AND (s.part_id='*' OR t->>'part_id'='*' OR s.part_id=t->>'part_id'))",
            (self.store.bot_id, Jsonb(values)),
        )).fetchone()
        return {
            "expected_deletion_epoch": clock["deletion_epoch"],
            "input_digest": digest,
            "requested_targets": requested,
            "resolved_targets": values,
            "legacy_conservative": conservative,
            "affected_payloads": affected["count"],
        }

    async def forget_sources_in(self, conn, lease: HostLease, operation_id: str, targets,
                                *, expected_deletion_epoch: int,
                                expected_resolved_targets=None,
                                expected_affected_payloads=None) -> dict:
        """Commit suppression in the caller's transaction with Host fencing."""
        _identifier(operation_id)
        requested, digest = self._forget_input(targets, expected_deletion_epoch)
        from forest.storage.migration_v1 import legacy_source_ref

        legacy = legacy_source_ref(self.store.bot_id).source_unit_id
        await self.store._host(conn, lease)
        await self.lock_in(conn)
        previous = await (await conn.execute(
            "SELECT * FROM public.bot_payload_forget_requests WHERE bot_id=%s AND operation_id=%s",
            (self.store.bot_id, operation_id),
        )).fetchone()
        if previous:
            if previous["input_digest"] != digest:
                raise ForestError("FORGET_CONFLICT", "Forget operation has different targets", 409)
            result = {"operation_id": operation_id, "deletion_epoch": previous["deletion_epoch"],
                      "legacy_conservative": any(t["source_unit_id"] == legacy
                                                 for t in previous["targets"])}
            await self.store._host(conn, lease)
            return result
        preview = await self.preview_forget_in(
            conn,
            tuple(SourceRef(item["source_unit_id"], part_id=item["part_id"])
                  for item in requested),
            expected_deletion_epoch=expected_deletion_epoch,
        )
        values = preview["resolved_targets"]
        if expected_resolved_targets is not None and values != list(expected_resolved_targets):
            raise ForestError(
                "FORGET_PLAN_CHANGED", "Forget target closure changed; preview it again", 409
            )
        if (expected_affected_payloads is not None
                and preview["affected_payloads"] != expected_affected_payloads):
            raise ForestError(
                "FORGET_PLAN_CHANGED", "Forget impact changed; preview it again", 409
            )
        epoch = expected_deletion_epoch + 1
        await conn.execute(
            "INSERT INTO public.bot_payload_forget_requests(bot_id,operation_id,input_digest,targets,"
            "deletion_epoch) VALUES (%s,%s,%s,%s,%s)",
            (self.store.bot_id, operation_id, digest, Jsonb(values), epoch),
        )
        for target in values:
            await conn.execute(
                "INSERT INTO public.bot_payload_suppression(bot_id,source_unit_id,part_id,operation_id,"
                "deletion_epoch) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, target["source_unit_id"], target["part_id"], operation_id, epoch),
            )
        await conn.execute(
            "UPDATE public.bot_memory_clock SET deletion_epoch=%s,fact_revision=fact_revision+1 "
            "WHERE bot_id=%s", (epoch, self.store.bot_id),
        )
        source_relation = await self._effective_source_relation_in(conn)
        await conn.execute(
            "UPDATE public.bot_payload_headers h SET state='REVOKED',forget_operation_id=%s "
            f"WHERE h.bot_id=%s AND h.state='ACTIVE' AND EXISTS(SELECT 1 FROM {source_relation} s,"
            "jsonb_array_elements(%s::jsonb) t WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id "
            "AND s.source_unit_id=t->>'source_unit_id' "
            "AND (s.part_id='*' OR t->>'part_id'='*' OR s.part_id=t->>'part_id'))",
            (operation_id, self.store.bot_id, Jsonb(values)),
        )
        result = {"operation_id": operation_id, "deletion_epoch": epoch,
                  "legacy_conservative": preview["legacy_conservative"]}
        await self.store._event(conn, "memory.payload_sources_revoked", operation_id, result)
        await self.store._host(conn, lease)
        return result

    async def forget_sources(self, lease: HostLease, operation_id: str, targets,
                              *, expected_deletion_epoch: int) -> dict:
        """Internal storage primitive; public callers use a reviewed plan/apply flow."""
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.forget_sources_in(
                conn, lease, operation_id, targets,
                expected_deletion_epoch=expected_deletion_epoch,
            )

    async def scrub(self, lease: HostLease, *, limit: int = 64) -> dict:
        """One bounded propagation/erasure batch; ownership recovery needs no I/O.

        `remaining` includes proven source links whose parent was revoked, even
        when this batch removed the last directly revoked body. A caller can
        therefore drain until zero without stranding downstream echo copies.
        """
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ForestError("INVALID_SCRUB_LIMIT", "Scrub limit must be 1 to 256", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            await self.lock_in(conn)
            propagated = await (await conn.execute(
                "SELECT public.forest_reconcile_source_links(%s,%s) AS count",
                (self.store.bot_id, limit),
            )).fetchone()
            rows = await (await conn.execute(
                "SELECT payload_id FROM public.bot_payload_headers WHERE bot_id=%s AND state='REVOKED' "
                "ORDER BY payload_id LIMIT %s FOR UPDATE", (self.store.bot_id, limit),
            )).fetchall()
            ids = [row["payload_id"] for row in rows]
            if ids:
                await conn.execute("DELETE FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=ANY(%s)",
                                   (self.store.bot_id, ids))
                await conn.execute("UPDATE public.bot_payload_headers SET state='SCRUBBED' "
                                   "WHERE bot_id=%s AND payload_id=ANY(%s)", (self.store.bot_id, ids))
            remaining = await (await conn.execute(
                "SELECT count(*) AS count FROM public.bot_payload_headers WHERE bot_id=%s AND state='REVOKED'",
                (self.store.bot_id,),
            )).fetchone()
            pending = await (await conn.execute(
                "SELECT count(*) AS count FROM public.bot_payload_source_links l "
                "JOIN public.bot_payload_headers p ON p.bot_id=l.bot_id AND p.payload_id=l.parent_payload_id "
                "WHERE l.bot_id=%s AND p.state IN ('REVOKED','SCRUBBED') AND NOT EXISTS("
                "SELECT 1 FROM public.bot_payload_suppression x WHERE x.bot_id=l.bot_id "
                "AND x.source_unit_id=l.dependent_source_unit_id AND x.part_id='*')",
                (self.store.bot_id,),
            )).fetchone()
            await self.store._host(conn, lease)
            return {"removed": len(ids), "remaining": remaining["count"] + pending["count"],
                    "reconciled_links": propagated["count"], "pending_links": pending["count"],
                    "scope": "managed_payload_bodies", "storage_reclaim_required": True}

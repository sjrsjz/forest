"""Host-only accounting for content delivered to a long-lived Worker.

All ``*_in`` calls require the caller's route/runtime/Host and Call/state locks.
Lock every participating generation head in sorted order *before* memory clock
or payload access. These helpers never acquire earlier locks or perform IPC.
An exception does not persist invalidation: supervisors use scan/invalidate in
their own transactions, then drain/reap the returned concrete Worker identities.

CAPACITY invalidation accepts trusted Host preallocated payload IDs, including
an input whose creating transaction rolled back. It only proves set capacity;
it never reads/creates that candidate's body or grants any execution authority.
SOURCE_CAPACITY similarly accepts only the Host's private source witness read
from actual candidate manifests/closure before rollback. It rechecks the whole
current source union and its late-link closure, with no source registration.
No SDK, Worker RPC or public management API may expose that internal operation.
"""

from __future__ import annotations

from dataclasses import asdict

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.payloads import MAX_SOURCES, PayloadStore

MAX_PAYLOADS = 256
KINDS = frozenset(
    {
        "CALL_INPUT",
        "STATE_READ",
        "CHILD_RESULT",
        "CONFIG",
        "PREPARE_STATE",
        "MIGRATION_SOURCE",
        "RESTORE_STATE",
    }
)
_GENERATION_STATUSES = ("PREPARING", "READY", "ACTIVE", "QUIESCING")
_UNAVAILABLE = frozenset(
    {
        "PAYLOAD_NOT_FOUND",
        "PAYLOAD_ERASED",
        "PAYLOAD_INCOMPLETE",
        "SOURCE_SUPPRESSED",
        "SOURCE_LINK_SCOPE_LIMIT",
    }
)


def _identifier(value):
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode()) <= 256
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ForestError("INVALID_EXPOSURE", "Exposure identities must be bounded IDs", 422)
    return value


def _refs(values):
    if (
        not isinstance(values, (list, tuple, set, frozenset))
        or not 1 <= len(values) <= MAX_PAYLOADS
    ):
        raise ForestError("INVALID_EXPOSURE", "Exposure requires 1 to 256 payload references", 422)
    return tuple(sorted({_identifier(value) for value in values}))


class ExposureCapacityError(ForestError):
    """Trusted in-process proof, deliberately absent from the public error DTO.

    Candidate headers may roll back with the rejected Call. Sources are a
    bounded witness read from their real immutable manifests/closure while the
    memory clock is held. Only the Host boundary may carry it to invalidate().
    """

    def __init__(self, generation_id, revision, reason, payload_refs=(), source_refs=()):
        super().__init__(
            "GENERATION_EXPOSURE_LIMIT",
            "Worker content exposure capacity is exhausted",
            429,
            details={"generation_id": generation_id, "revision": revision, "reason": reason},
        )
        self._generation_id = generation_id
        self._revision = revision
        self._reason = reason
        self._candidate_payload_refs = tuple(payload_refs)
        self._candidate_source_refs = tuple(source_refs)


class ExposureStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)

    async def _current_in(self, conn, generation_id):
        row = await (
            await conn.execute(
                "SELECT g.*,h.incarnation AS current_host,h.owner_epoch AS current_owner,"
                "h.expires_at,clock_timestamp() AS now,r.status AS runtime_status "
                "FROM public.bot_generations g JOIN public.bot_host_lease h USING(bot_id) "
                "JOIN public.bot_runtime_state r USING(bot_id) WHERE g.bot_id=%s AND g.generation_id=%s",
                (self.store.bot_id, _identifier(generation_id)),
            )
        ).fetchone()
        if row is None:
            raise ForestError("GENERATION_NOT_FOUND", "Generation is absent from this Bot", 404)
        if row["runtime_status"] != "ACTIVE":
            raise ForestError("STORAGE_SEALED", "Bot storage is sealed", 409)
        if (
            row["current_host"] != row["host_incarnation"]
            or row["current_owner"] != row["host_owner_epoch"]
            or row["expires_at"] <= row["now"]
        ):
            raise ForestError("STALE_HOST", "Generation belongs to an expired Host", 409)
        if row["status"] not in _GENERATION_STATUSES:
            raise ForestError("STALE_GENERATION", "Generation cannot receive content", 409)
        return row

    async def lock_generations_in(self, conn, generation_ids):
        """Acquire the full sorted exposure fence set before any payload helper."""
        if (
            not isinstance(generation_ids, (list, tuple, set, frozenset))
            or len(generation_ids) > 64
        ):
            raise ForestError("INVALID_EXPOSURE", "Exposure generation set is unbounded", 422)
        result = {}
        for generation_id in sorted({_identifier(value) for value in generation_ids}):
            generation = await self._current_in(conn, generation_id)
            # New generations receive this head in the generation INSERT trigger;
            # the pure SQL migration initializes all pre-existing identities.
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_generation_exposure_heads WHERE bot_id=%s "
                    "AND generation_id=%s FOR UPDATE",
                    (self.store.bot_id, generation_id),
                )
            ).fetchone()
            if head is None:
                raise ForestError(
                    "EXPOSURE_HEAD_MISSING", "Generation exposure head is missing", 409
                )
            if any(
                head[key] != generation[key]
                for key in (
                    "component_id",
                    "instance_id",
                    "worker_incarnation",
                    "host_incarnation",
                    "host_owner_epoch",
                )
            ):
                raise ForestError(
                    "EXPOSURE_IDENTITY_CONFLICT", "Generation exposure identity changed", 409
                )
            result[generation_id] = head
        return result

    async def ensure_generation_in(self, conn, generation_id):
        return (await self.lock_generations_in(conn, [generation_id]))[generation_id]

    async def _members_in(self, conn, generation_id):
        rows = await (
            await conn.execute(
                "SELECT payload_id FROM public.bot_generation_exposure_members "
                "WHERE bot_id=%s AND generation_id=%s ORDER BY payload_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        return tuple(row["payload_id"] for row in rows)

    @staticmethod
    def _view(head, refs):
        return dict(head, payload_ids=list(refs), digest=digest_json(list(refs)))

    async def _source_union_in(self, conn, refs, candidate_sources=()):
        """Return the complete closure, or 4097 distinct witnesses of overflow.

        The caller holds the memory clock. Cap the database result, not each
        manifest independently: overlapping payloads consume one source slot.
        """
        rows = await (
            await conn.execute(
                "SELECT DISTINCT source_unit_id,source_revision,part_id "
                "FROM public.bot_payload_sources WHERE bot_id=%s AND payload_id=ANY(%s) "
                "ORDER BY source_unit_id,source_revision,part_id LIMIT %s",
                (self.store.bot_id, list(refs), MAX_SOURCES + 1),
            )
        ).fetchall()
        sources = tuple(sorted({SourceRef(**row) for row in rows} | set(candidate_sources)))
        if len(sources) > MAX_SOURCES:
            return sources[: MAX_SOURCES + 1]
        linked = await (
            await conn.execute(
                "SELECT * FROM public.forest_source_dependency_closure(%s,%s,%s)",
                (self.store.bot_id, Jsonb([asdict(source) for source in sources]), MAX_SOURCES),
            )
        ).fetchall()
        return tuple(sorted(SourceRef(**row) for row in linked))

    async def _availability_in(self, conn, refs):
        clock = await self.payloads.lock_in(conn)
        try:
            await self.payloads.require_available_in(
                conn, refs, expected_deletion_epoch=clock["deletion_epoch"]
            )
        except ForestError as exc:
            if exc.code not in _UNAVAILABLE:
                raise
            return (
                "SOURCE_SCOPE_LIMIT"
                if exc.code == "SOURCE_LINK_SCOPE_LIMIT"
                else "CONTENT_UNAVAILABLE",
                (),
            )
        sources = await self._source_union_in(conn, refs)
        return ("SOURCE_SCOPE_LIMIT" if len(sources) > MAX_SOURCES else None, sources)

    async def _unavailable_in(self, conn, refs):
        return (await self._availability_in(conn, refs))[0]

    async def _available_in(self, conn, refs):
        reason, sources = await self._availability_in(conn, refs)
        if reason:
            raise ForestError(
                "GENERATION_EXPOSURE_REVOKED",
                "Worker content dependencies are unavailable",
                410,
                details={"reason_code": reason},
            )
        return sources

    async def guard_generation_in(self, conn, generation_id, *, phase="admission"):
        if phase not in {"admission", "commit"}:
            raise ForestError("INVALID_EXPOSURE_PHASE", "Unknown internal exposure phase", 422)
        head = await self.ensure_generation_in(conn, generation_id)
        if head["status"] == "REVOKED":
            raise ForestError(
                "GENERATION_EXPOSURE_REVOKED", "Worker dependencies were invalidated", 410
            )
        if phase == "admission" and head["status"] == "EXHAUSTED":
            raise ForestError(
                "GENERATION_EXPOSURE_EXHAUSTED", "Worker must drain before replacement", 429
            )
        refs = await self._members_in(conn, generation_id)
        await self._available_in(conn, refs)
        await self._current_in(conn, generation_id)
        return self._view(head, refs)

    async def generation_parents_in(self, conn, generation_id):
        return tuple(
            (await self.guard_generation_in(conn, generation_id, phase="commit"))["payload_ids"]
        )

    async def expose_in(self, conn, generation_id, exposure_id, kind, payload_refs, subject_ref):
        refs = _refs(payload_refs)
        _identifier(exposure_id)
        _identifier(subject_ref)
        if kind not in KINDS:
            raise ForestError("INVALID_EXPOSURE", "Unknown exposure kind", 422)
        head = await self.ensure_generation_in(conn, generation_id)
        digest = digest_json([generation_id, exposure_id, kind, list(refs), subject_ref])
        previous = await (
            await conn.execute(
                "SELECT * FROM public.bot_generation_exposures WHERE bot_id=%s AND generation_id=%s "
                "AND exposure_id=%s",
                (self.store.bot_id, generation_id, exposure_id),
            )
        ).fetchone()
        if previous and previous["input_digest"] != digest:
            raise ForestError(
                "EXPOSURE_CONFLICT", "Exposure identity already has different input", 409
            )
        if head["status"] == "REVOKED":
            raise ForestError(
                "GENERATION_EXPOSURE_REVOKED", "Worker dependencies were invalidated", 410
            )
        current = await self._members_in(conn, generation_id)
        current_sources = await self._available_in(conn, current)
        combined = tuple(sorted(set(current) | set(refs)))
        if len(combined) > MAX_PAYLOADS:
            raise ExposureCapacityError(
                generation_id,
                head["revision"],
                "CAPACITY",
                payload_refs=refs,
            )
        if head["status"] == "EXHAUSTED" and len(combined) != len(current):
            raise ForestError(
                "GENERATION_EXPOSURE_EXHAUSTED", "Draining Worker cannot receive new content", 429
            )
        # A new unavailable payload rejects only this delivery; the already
        # exposed valid set may still finish. Do not call the whole-generation
        # revoked guard on the union before distinguishing a capacity failure.
        clock = await self.payloads.lock_in(conn)
        try:
            await self.payloads.require_available_in(
                conn, refs, expected_deletion_epoch=clock["deletion_epoch"]
            )
        except ForestError as exc:
            if exc.code not in _UNAVAILABLE:
                raise
            raise ForestError(
                "GENERATION_EXPOSURE_REVOKED", "Candidate content dependencies are unavailable", 410
            ) from exc
        combined_sources = await self._source_union_in(conn, combined)
        if len(combined_sources) > MAX_SOURCES:
            witness = tuple(sorted(set(combined_sources) - set(current_sources)))[
                : MAX_SOURCES + 1 - len(current_sources)
            ]
            if len(witness) > MAX_SOURCES:
                # A legacy empty head cannot prove 4097 unrelated sources with
                # a 4096-source proof. Reject the delivery without inventing a
                # durable capacity decision. Current generations have CONFIG.
                raise ForestError(
                    "SOURCE_LINK_SCOPE_LIMIT", "Candidate source union exceeds its limit", 422
                )
            raise ExposureCapacityError(
                generation_id,
                head["revision"],
                "SOURCE_CAPACITY",
                payload_refs=refs,
                source_refs=witness,
            )
        if previous is None:
            previous = await (
                await conn.execute(
                    "INSERT INTO public.bot_generation_exposures(bot_id,generation_id,exposure_id,revision,"
                    "kind,subject_ref,payload_refs,input_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        generation_id,
                        exposure_id,
                        head["revision"] + 1,
                        kind,
                        subject_ref,
                        list(refs),
                        digest,
                    ),
                )
            ).fetchone()
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_generation_exposure_heads WHERE bot_id=%s AND generation_id=%s",
                    (self.store.bot_id, generation_id),
                )
            ).fetchone()
            duplicate = False
        else:
            duplicate = True
        await self._current_in(conn, generation_id)
        return dict(
            self._view(head, combined),
            exposure_id=exposure_id,
            exposure_revision=previous["revision"],
            input_digest=digest,
            duplicate=duplicate,
        )

    async def _mark_in(self, conn, head, status, reason):
        if head["status"] == status:
            return head
        return await (
            await conn.execute(
                "UPDATE public.bot_generation_exposure_heads SET status=%s,reason_code=%s,"
                "invalidated_at=clock_timestamp() WHERE bot_id=%s AND generation_id=%s RETURNING *",
                (status, reason, self.store.bot_id, head["generation_id"]),
            )
        ).fetchone()

    async def invalidate(
        self,
        lease,
        generation_id,
        expected_revision,
        *,
        reason="CAPACITY",
        candidate_payload_refs=(),
        candidate_source_refs=(),
    ):
        """Persist a separately proven supervisor decision; never call inside a business TX."""
        if (
            type(expected_revision) is not int
            or expected_revision < 0
            or reason not in {"CAPACITY", "SOURCE_CAPACITY", "CONTENT"}
        ):
            raise ForestError("INVALID_EXPOSURE", "Invalid supervisor invalidation request", 422)
        proposed = _refs(candidate_payload_refs) if reason == "CAPACITY" else ()
        proposed_sources = (
            self.payloads._sources(candidate_source_refs) if reason == "SOURCE_CAPACITY" else ()
        )
        if (
            (reason != "CAPACITY" and candidate_payload_refs)
            or (reason != "SOURCE_CAPACITY" and candidate_source_refs)
            or (reason == "SOURCE_CAPACITY" and not proposed_sources)
        ):
            raise ForestError(
                "INVALID_EXPOSURE", "Content invalidation only examines actual deliveries", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            generation = await self.store._generation(conn, generation_id)
            await self.store._routes(conn, [generation["component_id"]])
            await self.store._host(conn, lease)
            head = await self.ensure_generation_in(conn, generation_id)
            if head["revision"] != expected_revision:
                raise ForestError("EXPOSURE_CONFLICT", "Worker exposure revision changed", 409)
            refs = await self._members_in(conn, generation_id)
            unavailable = await self._unavailable_in(conn, refs)
            if unavailable:
                head = await self._mark_in(conn, head, "REVOKED", unavailable)
            elif head["status"] == "REVOKED":
                pass
            elif head["status"] == "EXHAUSTED":
                pass
            elif reason == "CAPACITY" and len(set(refs) | set(proposed)) > MAX_PAYLOADS:
                head = await self._mark_in(conn, head, "EXHAUSTED", "CAPACITY")
            elif (
                reason == "SOURCE_CAPACITY"
                and len(await self._source_union_in(conn, refs, proposed_sources)) > MAX_SOURCES
            ):
                head = await self._mark_in(conn, head, "EXHAUSTED", "SOURCE_CAPACITY")
            else:
                raise ForestError(
                    "EXPOSURE_INVALIDATION_UNPROVEN",
                    "No exposure invalidation condition holds",
                    409,
                )
            await self.store._host(conn, lease)
            return self._view(head, refs)

    async def scan(self, lease, *, limit=64, after=None):
        """Bounded, resumable current-Host sweep; repeats unreaped decisions after lost ACK."""
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ForestError("INVALID_EXPOSURE", "Exposure scan limit must be 1 to 64", 422)
        if after is not None:
            _identifier(after)
        self.store._scope(lease)
        async with self.store.pool.connection() as conn, conn.transaction():
            rows = await (
                await conn.execute(
                    "SELECT generation_id,component_id FROM public.bot_generations WHERE bot_id=%s "
                    "AND host_incarnation=%s AND host_owner_epoch=%s AND status=ANY(%s) "
                    "AND (%s::text IS NULL OR generation_id>%s) ORDER BY generation_id LIMIT %s",
                    (
                        self.store.bot_id,
                        lease.incarnation,
                        lease.owner_epoch,
                        list(_GENERATION_STATUSES),
                        after,
                        after,
                        limit,
                    ),
                )
            ).fetchall()
            await self.store._routes(conn, [r["component_id"] for r in rows])
            await self.store._host(conn, lease)
            # A generation may retire between the unlocked selection and route
            # acquisition. Re-select under Host authority before taking heads.
            live = []
            for row in rows:
                current = await self.store._generation(conn, row["generation_id"])
                if current["status"] in _GENERATION_STATUSES:
                    live.append(row["generation_id"])
            heads = await self.lock_generations_in(conn, live)
            invalid = []
            for generation_id, head in heads.items():
                refs = await self._members_in(conn, generation_id)
                unavailable = await self._unavailable_in(conn, refs)
                if unavailable:
                    head = await self._mark_in(conn, head, "REVOKED", unavailable)
                if head["status"] != "LIVE":
                    invalid.append(self._view(head, refs))
            await self.store._host(conn, lease)
            return {
                "generations": invalid,
                "scanned": len(rows),
                "next_cursor": rows[-1]["generation_id"] if len(rows) == limit else None,
            }

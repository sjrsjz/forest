"""Durable embedding attempts and source-bound exact-search projections.

All network work belongs to the runtime. This repository freezes exact bytes,
reserves cost before dispatch and publishes one vector per immutable facet.
"""

from __future__ import annotations

import json
from datetime import UTC
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.embeddings import EmbeddingInput, PreparedEmbeddingRequest, prepare_embedding_request
from forest.errors import ForestError
from forest.memory_index import checked_vector, facets, profile
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.memory_config import MemoryConfig
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore


class MemoryIndexStore:
    def __init__(self, messages, media):
        self.messages, self.store, self.media = messages, messages.store, media
        self.sources, self.payloads = MemorySourceStore(messages), PayloadStore(messages.store)

    async def _guard(self, conn, lease, revision, conversation_id=None):
        binding = conversation = None
        if conversation_id is not None:
            location = await self.messages._locate(conn, conversation_id)
            binding = await self.messages.bindings._guard(
                conn, lease, location["binding_id"], data=False
            )
            conversation = await self.messages._conversation(conn, conversation_id)
        else:
            await self.store._host(conn, lease)
        configuration = await MemoryConfig.guard_in(conn, lease, revision)
        clock = await self.payloads.lock_in(conn)
        return configuration, clock, conversation, binding

    async def _final(self, conn, lease, binding=None):
        if binding is not None:
            await self.messages.bindings._final_guard(conn, lease, binding, data=False)
        else:
            await self.store._host(conn, lease)

    async def enroll(self, lease, revision, *, limit=32):
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("Index scan limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            cfg, clock, _, _ = await self._guard(conn, lease, revision)
            space = profile(cfg["values"]).space_id
            await conn.execute(
                "INSERT INTO public.bot_memory_index_cursors(bot_id,space_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (lease.bot_id, space),
            )
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_index_cursors WHERE bot_id=%s AND space_id=%s FOR UPDATE",
                    (lease.bot_id, space),
                )
            ).fetchone()
            view = await self.sources.authorize_sources_in(
                conn,
                source_unit_ids=None,
                after_source_unit_id=head["cursor_id"],
                limit=limit,
            )
            added = 0
            for source in view["sources"]:
                for facet in facets(source, space):
                    result = await conn.execute(
                        "INSERT INTO public.bot_memory_index_facets(bot_id,facet_id,space_id,source_unit_id,source_revision,part_id,selector,state) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (
                            lease.bot_id,
                            facet["facet_id"],
                            space,
                            source["source_unit_id"],
                            source["source_revision"],
                            facet["part_id"],
                            Jsonb(facet),
                            "WAIT_MEDIA" if facet["modality"] == "image" else "QUEUED",
                        ),
                    )
                    added += result.rowcount
                    # A new configuration is a new admission decision for known
                    # failures, never an automatic retry of uncertain effects.
                    await conn.execute(
                        "UPDATE public.bot_memory_index_facets f SET state=%s,request_id=NULL,error_code=NULL "
                        "FROM public.bot_memory_embedding_requests r WHERE f.bot_id=%s AND f.facet_id=%s "
                        "AND r.bot_id=f.bot_id AND r.request_id=f.request_id AND r.config_revision<>%s "
                        "AND f.vector_payload_id IS NULL AND f.state IN ('REJECTED','SUPPRESSED','PENDING') "
                        "AND (SELECT a.state FROM public.bot_memory_embedding_attempts a WHERE a.bot_id=r.bot_id AND a.request_id=r.request_id ORDER BY ordinal DESC LIMIT 1) IN ('NOT_SENT','RESPONDED','REJECTED')",
                        (
                            "WAIT_MEDIA" if facet["modality"] == "image" else "QUEUED",
                            lease.bot_id,
                            facet["facet_id"],
                            revision,
                        ),
                    )
            done = len(view["sources"]) < limit
            await conn.execute(
                "UPDATE public.bot_memory_index_cursors SET cursor_id=%s,cycles=cycles+%s,observed_fact_revision=%s WHERE bot_id=%s AND space_id=%s",
                (
                    "" if done else view["sources"][-1]["source_unit_id"],
                    int(done),
                    clock["fact_revision"],
                    lease.bot_id,
                    space,
                ),
            )
            await self._final(conn, lease)
            return {
                "added": added,
                "scanned": len(view["sources"]),
                "cycle_completed": done,
                "space_id": space,
            }

    async def _facets_in(self, conn, ids):
        if (
            not isinstance(ids, (list, tuple))
            or not 1 <= len(ids) <= 32
            or len(set(ids)) != len(ids)
        ):
            raise ForestError("MEMORY_FACET_INVALID", "Choose one to 32 distinct facets", 422)
        rows = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_index_facets WHERE bot_id=%s AND facet_id=ANY(%s)",
                (self.store.bot_id, list(ids)),
            )
        ).fetchall()
        lookup = {row["facet_id"]: row for row in rows}
        if len(lookup) != len(ids):
            raise ForestError("MEMORY_FACET_UNAVAILABLE", "A memory facet is unavailable", 409)
        return [lookup[key] for key in ids]

    async def retryable(self, lease, revision):
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, lease, revision)
            request = await (
                await conn.execute(
                    "SELECT r.* FROM public.bot_memory_embedding_requests r JOIN LATERAL "
                    "(SELECT a.* FROM public.bot_memory_embedding_attempts a WHERE a.bot_id=r.bot_id AND a.request_id=r.request_id ORDER BY ordinal DESC LIMIT 1) a ON true "
                    "WHERE r.bot_id=%s AND r.config_revision=%s AND r.purpose='document' AND a.state='NOT_SENT' AND a.ordinal<3 ORDER BY a.created_at LIMIT 1",
                    (lease.bot_id, revision),
                )
            ).fetchone()
            await self._final(conn, lease)
            return request

    async def _inputs_in(self, conn, cfg, clock, rows, conversation=None, cut=None):
        ids = sorted({row["source_unit_id"] for row in rows})
        if conversation is not None and (
            type(cut) is not int or not 0 <= cut <= conversation["context_applied_seq"]
        ):
            raise ForestError("MEMORY_INPUT_CUT", "Query exceeds its committed cut", 409)
        view = await self.sources.authorize_sources_in(
            conn,
            conversation,
            input_cut=cut,
            source_unit_ids=ids,
            limit=256,
        )
        sources = {row["source_unit_id"]: row for row in view["sources"]}
        if len(sources) != len(ids):
            raise ForestError(
                "MEMORY_SOURCE_UNAVAILABLE", "Memory sources lost their eligibility", 409
            )
        image_refs = [
            (row["source_unit_id"], row["part_id"])
            for row in rows
            if row["selector"]["modality"] == "image"
        ]
        captures = (
            (
                await self.media.materialize_in(
                    conn,
                    ids,
                    expected_deletion_epoch=clock["deletion_epoch"],
                    part_refs=image_refs,
                    byte_limit=8 * 1024 * 1024,
                )
            )
            if image_refs
            else {}
        )
        inputs = []
        for row in rows:
            source, selector = sources[row["source_unit_id"]], row["selector"]
            if row["space_id"] != profile(cfg["values"]).space_id or selector not in facets(
                source, row["space_id"]
            ):
                raise ForestError(
                    "MEMORY_FACET_CHANGED", "Facet does not match its fixed source and space", 409
                )
            part = next(part for part in source["parts"] if part["part_id"] == row["part_id"])
            attribution = {
                "bot_id": self.store.bot_id,
                "source_unit_id": source["source_unit_id"],
                "source_revision": source["source_revision"],
                "source_digest": source["content_digest"],
                "source_id": source["source_id"],
                "source_kind": source["source_kind"],
                "realm_id": source["realm_id"],
                "identity_schema": source["identity_schema"],
                "platform": source["platform"],
                "channel_kind": source["channel_kind"],
                "channel_id": source["channel_id"],
                "part_id": row["part_id"],
                "principal_ref": source["principal_ref"],
            }
            if source["source_kind"] == "GROUP":
                attribution["group_source_id"] = source["source_id"]
            if selector["modality"] == "text":
                value = part["text"][selector["start_char"] : selector["end_char"]]
                inputs.append(
                    EmbeddingInput(row["facet_id"], text=value, source_json=json.dumps(attribution))
                )
            else:
                image = captures.get((source["source_unit_id"], row["part_id"]))
                if image is None:
                    raise ForestError(
                        "MEMORY_MEDIA_PENDING", "Original image is not available yet", 409
                    )
                inputs.append(
                    EmbeddingInput(
                        row["facet_id"], image=image, source_json=json.dumps(attribution)
                    )
                )
        return inputs

    async def pending(self, lease, revision, *, limit=32):
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("Index pending limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            cfg, clock, _, _ = await self._guard(conn, lease, revision)
            space = profile(cfg["values"]).space_id
            await conn.execute(
                "INSERT INTO public.bot_memory_index_cursors(bot_id,space_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (lease.bot_id, space),
            )
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_index_cursors WHERE bot_id=%s AND space_id=%s FOR UPDATE",
                    (lease.bot_id, space),
                )
            ).fetchone()
            rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_index_facets WHERE bot_id=%s AND space_id=%s AND state IN ('QUEUED','WAIT_MEDIA') AND facet_id>%s ORDER BY facet_id LIMIT %s",
                    (lease.bot_id, space, head["pending_cursor_id"], limit),
                )
            ).fetchall()
            ready = []
            for row in rows:
                try:
                    await self._inputs_in(conn, cfg, clock, [row])
                except ForestError as exc:
                    if exc.code == "MEMORY_MEDIA_PENDING":
                        continue
                    if exc.code == "MEMORY_SOURCE_UNAVAILABLE":
                        await conn.execute(
                            "UPDATE public.bot_memory_index_facets SET state='SUPPRESSED',error_code=%s WHERE bot_id=%s AND facet_id=%s",
                            (exc.code, lease.bot_id, row["facet_id"]),
                        )
                        continue
                    raise
                ready.append(row["selector"])
            await conn.execute(
                "UPDATE public.bot_memory_index_cursors SET pending_cursor_id=%s WHERE bot_id=%s AND space_id=%s",
                (rows[-1]["facet_id"] if len(rows) == limit else "", lease.bot_id, space),
            )
            await self._final(conn, lease)
            return ready

    async def prepare(
        self,
        lease,
        revision,
        facet_ids,
        *,
        request_id=None,
        purpose="document",
        conversation_id=None,
        input_cut=None,
    ):
        if purpose not in {"document", "query"} or (purpose == "query") != (
            conversation_id is not None
        ):
            raise ValueError("Embedding purpose requires a matching query scope")
        request_id = request_id or "embedding:" + uuid4().hex
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 200:
            raise ValueError("Embedding request identity")
        async with self.store.pool.connection() as conn, conn.transaction():
            cfg, clock, conversation, binding = await self._guard(
                conn, lease, revision, conversation_id
            )
            old = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_requests WHERE bot_id=%s AND request_id=%s",
                    (lease.bot_id, request_id),
                )
            ).fetchone()
            if old:
                if (
                    old["facet_ids"] != list(facet_ids)
                    or old["purpose"] != purpose
                    or old["conversation_id"] != conversation_id
                    or old["input_cut"] != input_cut
                ):
                    raise ForestError(
                        "MEMORY_REQUEST_CONFLICT",
                        "Embedding request identity has different input",
                        409,
                    )
                attempts = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_memory_embedding_attempts WHERE bot_id=%s AND request_id=%s ORDER BY ordinal DESC LIMIT 1",
                        (lease.bot_id, request_id),
                    )
                ).fetchone()
                if attempts["state"] != "NOT_SENT" or attempts["ordinal"] >= 3:
                    await self._final(conn, lease, binding)
                    return {
                        "status": attempts["state"],
                        "request_id": request_id,
                        "attempt_id": attempts["attempt_id"],
                    }
                if old["config_revision"] != revision:
                    raise ForestError(
                        "MEMORY_CONFIGURATION_REVOKED",
                        "Retry cannot change its frozen configuration",
                        409,
                    )
                prepared = await self._prepared_in(conn, old, uuid4().hex, clock)
                rows = await self._facets_in(conn, old["facet_ids"])
                inputs = await self._inputs_in(conn, cfg, clock, rows, conversation, input_cut)
                expected = prepare_embedding_request(
                    profile(cfg["values"]),
                    inputs,
                    request_id=request_id,
                    transport_attempt_id=prepared.transport_attempt_id,
                    purpose=purpose,
                )
                if expected != prepared:
                    raise ForestError(
                        "MEMORY_REQUEST_CHANGED", "Retry input differs from the frozen request", 409
                    )
                ordinal = attempts["ordinal"] + 1
            else:
                rows = await self._facets_in(conn, facet_ids)
                if purpose == "document" and any(
                    row["state"] not in {"QUEUED", "WAIT_MEDIA"} for row in rows
                ):
                    raise ForestError(
                        "MEMORY_FACET_PENDING", "Another attempt already owns a facet", 409
                    )
                inputs = await self._inputs_in(conn, cfg, clock, rows, conversation, input_cut)
                prepared = prepare_embedding_request(
                    profile(cfg["values"]),
                    inputs,
                    request_id=request_id,
                    transport_attempt_id=uuid4().hex,
                    purpose=purpose,
                )
                refs = tuple(
                    SourceRef(row["source_unit_id"], row["source_revision"], row["part_id"])
                    for row in rows
                )
                wire = await self.payloads.put_bytes_in(
                    conn,
                    owner_kind="memory.embedding",
                    owner_id=request_id + ":wire",
                    value=prepared.body,
                    sources=refs,
                    expected_deletion_epoch=clock["deletion_epoch"],
                )
                manifest = await self.payloads.put_json_in(
                    conn,
                    owner_kind="memory.embedding",
                    owner_id=request_id + ":manifest",
                    value=prepared.manifest,
                    sources=refs,
                    expected_deletion_epoch=clock["deletion_epoch"],
                )
                runtime = await self.store._runtime(conn)
                if not runtime["data_admission"]:
                    raise ForestError("MEMORY_ADMISSION_CLOSED", "Memory execution is paused", 409)
                fence = {
                    key: runtime[key]
                    for key in ("policy_revision", "maintenance_epoch", "storage_generation")
                }
                await conn.execute(
                    "INSERT INTO public.bot_memory_embedding_requests(bot_id,request_id,purpose,config_revision,space_id,facet_ids,conversation_id,input_cut,deletion_epoch,body_payload_id,manifest_payload_id,wire_digest,profile,runtime_fence) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        lease.bot_id,
                        request_id,
                        purpose,
                        revision,
                        prepared.space_id,
                        Jsonb(list(facet_ids)),
                        conversation_id,
                        input_cut,
                        clock["deletion_epoch"],
                        wire.payload_id,
                        manifest.payload_id,
                        prepared.payload_digest,
                        Jsonb(profile(cfg["values"]).as_dict()),
                        Jsonb(fence),
                    ),
                )
                ordinal = 1
            day = (await self.store._now(conn)).astimezone(UTC).date()
            await conn.execute(
                "INSERT INTO public.bot_memory_embedding_budget_days(bot_id,budget_day) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (lease.bot_id, day),
            )
            budget = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_budget_days WHERE bot_id=%s AND budget_day=%s FOR UPDATE",
                    (lease.bot_id, day),
                )
            ).fetchone()
            reservation = cfg["values"]["request_token_reservation"]
            if (
                budget["requests"] >= cfg["values"]["daily_request_budget"]
                or budget["reserved_tokens"] + budget["charged_tokens"] + reservation
                > cfg["values"]["daily_token_budget"]
            ):
                raise ForestError(
                    "MEMORY_BUDGET_EXCEEDED", "Memory request budget is reserved or consumed", 409
                )
            await self._capacity_in(conn, lease.bot_id, cfg["values"]["index_concurrency"])
            await conn.execute(
                "UPDATE public.bot_memory_embedding_budget_days SET reserved_tokens=reserved_tokens+%s,requests=requests+1 WHERE bot_id=%s AND budget_day=%s",
                (reservation, lease.bot_id, day),
            )
            await conn.execute(
                "INSERT INTO public.bot_memory_embedding_attempts(bot_id,attempt_id,request_id,ordinal,host_incarnation,host_owner_epoch,state,budget_day,reserved_tokens,deadline_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,'PREPARED',%s,%s,clock_timestamp()+%s*interval '1 second')",
                (
                    lease.bot_id,
                    prepared.transport_attempt_id,
                    request_id,
                    ordinal,
                    lease.incarnation,
                    lease.owner_epoch,
                    day,
                    reservation,
                    cfg["values"]["request_timeout"] + 15,
                ),
            )
            if purpose == "document":
                await conn.execute(
                    "UPDATE public.bot_memory_index_facets SET state='PENDING',request_id=%s WHERE bot_id=%s AND facet_id=ANY(%s)",
                    (request_id, lease.bot_id, list(facet_ids)),
                )
            await self._final(conn, lease, binding)
            return {
                "status": "PREPARED",
                "request_id": request_id,
                "attempt_id": prepared.transport_attempt_id,
                "prepared": prepared,
            }

    async def _prepared_in(self, conn, request, attempt_id, clock):
        body = await self.payloads.read_bytes_in(
            conn, request["body_payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        manifest = await self.payloads.read_json_in(
            conn, request["manifest_payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        return PreparedEmbeddingRequest(
            request["request_id"],
            attempt_id,
            body,
            json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
            request["wire_digest"],
            request["space_id"],
        )

    @staticmethod
    async def _capacity_in(conn, bot_id, maximum, *, excluding=None):
        # Caller holds the Bot runtime lock. Queued reservations occupy the
        # same durable pool as foreground queries and dispatched work.
        row = await (
            await conn.execute(
                "SELECT count(*) AS count FROM public.bot_memory_embedding_attempts WHERE bot_id=%s "
                "AND state IN ('PREPARED','DISPATCHING') AND (%s::text IS NULL OR attempt_id<>%s)",
                (bot_id, excluding, excluding),
            )
        ).fetchone()
        if row["count"] >= maximum:
            raise ForestError(
                "MEMORY_CONCURRENCY_EXCEEDED", "Bot embedding capacity is reserved", 409
            )

    async def check_permit(self, lease, prepared, *, dispatch_token):
        prepared.verify()
        if not isinstance(dispatch_token, str) or not 1 <= len(dispatch_token) <= 128:
            raise ValueError("A transport owner token is required")
        async with self.store.pool.connection() as conn, conn.transaction():
            request = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_requests WHERE bot_id=%s AND request_id=%s",
                    (lease.bot_id, prepared.request_id),
                )
            ).fetchone()
            if request is None:
                raise ForestError(
                    "MEMORY_REQUEST_UNAVAILABLE", "Embedding request is unavailable", 409
                )
            cfg, clock, conversation, binding = await self._guard(
                conn, lease, request["config_revision"], request["conversation_id"]
            )
            runtime = await self.store._runtime(conn)
            if not runtime["data_admission"] or any(
                runtime[k] != v for k, v in request["runtime_fence"].items()
            ):
                raise ForestError("MEMORY_PERMIT_REVOKED", "Memory runtime authority changed", 409)
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_attempts WHERE bot_id=%s AND attempt_id=%s FOR UPDATE",
                    (lease.bot_id, prepared.transport_attempt_id),
                )
            ).fetchone()
            if (
                not attempt
                or attempt["request_id"] != request["request_id"]
                or attempt["state"] != "PREPARED"
                or attempt["host_incarnation"] != lease.incarnation
                or attempt["host_owner_epoch"] != lease.owner_epoch
                or attempt["deadline_at"] <= await self.store._now(conn)
                or request["deletion_epoch"] != clock["deletion_epoch"]
            ):
                raise ForestError(
                    "MEMORY_PERMIT_REVOKED", "Embedding dispatch authority expired", 409
                )
            if prepared != await self._prepared_in(conn, request, attempt["attempt_id"], clock):
                raise ForestError(
                    "MEMORY_REQUEST_CHANGED", "Embedding transport changed its frozen request", 409
                )
            await self._capacity_in(
                conn,
                lease.bot_id,
                cfg["values"]["index_concurrency"],
                excluding=attempt["attempt_id"],
            )
            rows = await self._facets_in(conn, request["facet_ids"])
            await self._inputs_in(conn, cfg, clock, rows, conversation, request["input_cut"])
            if attempt["deadline_at"] <= await self.store._now(conn):
                raise ForestError(
                    "MEMORY_PERMIT_REVOKED", "Embedding dispatch expired during validation", 409
                )
            await conn.execute(
                "UPDATE public.bot_memory_embedding_attempts SET state='DISPATCHING',dispatch_token=%s WHERE bot_id=%s AND attempt_id=%s",
                (dispatch_token, lease.bot_id, attempt["attempt_id"]),
            )
            if attempt["deadline_at"] <= await self.store._now(conn):
                raise ForestError(
                    "MEMORY_PERMIT_REVOKED", "Embedding dispatch expired before commit", 409
                )
            await self._final(conn, lease, binding)

    async def complete(self, lease, prepared, result, *, dispatch_token=None):
        """Observe once; a revoked result may settle cost but cannot publish content."""
        prepared.verify()
        if (
            result.request_id,
            result.transport_attempt_id,
            result.payload_digest,
            result.space_id,
        ) != (
            prepared.request_id,
            prepared.transport_attempt_id,
            prepared.payload_digest,
            prepared.space_id,
        ):
            raise ForestError(
                "MEMORY_RESULT_MISMATCH", "Embedding result differs from its request", 409
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            # Read-only location precedes the normal Binding -> runtime -> Host chain.
            request = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_requests WHERE bot_id=%s AND request_id=%s",
                    (lease.bot_id, prepared.request_id),
                )
            ).fetchone()
            if request is None:
                raise ForestError(
                    "MEMORY_REQUEST_UNAVAILABLE", "Embedding request is unavailable", 409
                )
            if (prepared.payload_digest, prepared.space_id) != (
                request["wire_digest"],
                request["space_id"],
            ):
                raise ForestError(
                    "MEMORY_RESULT_MISMATCH", "Result changed its persisted request identity", 409
                )
            manifest_header = await self.payloads._header_in(conn, request["manifest_payload_id"])
            if digest_json(prepared.manifest) != manifest_header["digest"]:
                raise ForestError(
                    "MEMORY_RESULT_MISMATCH", "Result changed its persisted provenance", 409
                )
            binding = conversation = None
            if request["conversation_id"]:
                location = await self.messages._locate(conn, request["conversation_id"])
                await conn.execute(
                    "SELECT binding_id FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s FOR UPDATE",
                    (lease.bot_id, location["binding_id"]),
                )
            await self.store._host(conn, lease)
            if request["conversation_id"]:
                conversation = await self.messages._conversation(conn, request["conversation_id"])
            cfg = None
            try:
                cfg = await MemoryConfig.guard_in(conn, lease, request["config_revision"])
            except ForestError:
                pass
            clock = await self.payloads.lock_in(conn)
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_attempts WHERE bot_id=%s AND attempt_id=%s FOR UPDATE",
                    (lease.bot_id, prepared.transport_attempt_id),
                )
            ).fetchone()
            if (
                not attempt
                or attempt["request_id"] != request["request_id"]
                or attempt["host_incarnation"] != lease.incarnation
                or attempt["host_owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError(
                    "MEMORY_ATTEMPT_REVOKED", "Another host owns the embedding attempt", 409
                )
            if attempt["dispatch_token"] != dispatch_token:
                raise ForestError(
                    "MEMORY_ATTEMPT_REVOKED", "Another transport owns the dispatch", 409
                )
            result_digest = digest_json(result.as_dict())
            if attempt["state"] not in {"PREPARED", "DISPATCHING"}:
                if attempt["result_digest"] and attempt["result_digest"] != result_digest:
                    raise ForestError(
                        "MEMORY_RESULT_CONFLICT", "An attempt already has another observation", 409
                    )
                await self._final(conn, lease)
                return {"status": attempt["state"], "request_id": request["request_id"]}
            if result.status not in {"RESPONDED", "REJECTED", "NOT_SENT", "UNKNOWN"}:
                raise ForestError(
                    "MEMORY_RESULT_INVALID", "Embedding result has an unsupported state", 422
                )
            if result.status != "NOT_SENT" and attempt["state"] != "DISPATCHING":
                raise ForestError(
                    "MEMORY_RESULT_INVALID", "A response requires a committed dispatch permit", 409
                )
            runtime = await self.store._runtime(conn)
            publish = (
                cfg is not None
                and request["deletion_epoch"] == clock["deletion_epoch"]
                and runtime["data_admission"]
                and all(runtime[k] == v for k, v in request["runtime_fence"].items())
                and attempt["deadline_at"] > await self.store._now(conn)
            )
            rows = await self._facets_in(conn, request["facet_ids"])
            if publish:
                try:
                    await self._inputs_in(
                        conn, cfg, clock, rows, conversation, request["input_cut"]
                    )
                except ForestError:
                    publish = False
            publish = publish and attempt["deadline_at"] > await self.store._now(conn)
            vector_lookup = {}
            usage = result.usage
            if result.status == "RESPONDED":
                if (
                    not usage
                    or type(usage.get("total_tokens")) is not int
                    or not 0 <= usage["total_tokens"] <= 2**53 - 1
                ):
                    raise ForestError(
                        "MEMORY_USAGE_INVALID", "Embedding response requires finite usage", 422
                    )
                if len(result.vectors) != len(rows) or {v.facet_id for v in result.vectors} != set(
                    request["facet_ids"]
                ):
                    raise ForestError(
                        "MEMORY_VECTOR_INVALID", "Embedding response has different facets", 422
                    )
                for vector in result.vectors:
                    if type(vector.index) is not int or vector.index != request["facet_ids"].index(
                        vector.facet_id
                    ):
                        raise ForestError(
                            "MEMORY_VECTOR_INVALID",
                            "Embedding response changed facet positions",
                            422,
                        )
                    vector_lookup[vector.facet_id] = checked_vector(
                        vector.values, request["profile"]["dimensions"]
                    )
            charged = (
                usage["total_tokens"]
                if result.status == "RESPONDED"
                else (0 if result.status == "NOT_SENT" else None)
            )
            if charged is not None:
                await conn.execute(
                    "UPDATE public.bot_memory_embedding_budget_days SET reserved_tokens=reserved_tokens-%s,charged_tokens=charged_tokens+%s WHERE bot_id=%s AND budget_day=%s",
                    (attempt["reserved_tokens"], charged, lease.bot_id, attempt["budget_day"]),
                )
            result_ref = None
            if publish and result.status == "RESPONDED":
                refs = tuple(
                    SourceRef(row["source_unit_id"], row["source_revision"], row["part_id"])
                    for row in rows
                )
                saved = await self.payloads.put_json_in(
                    conn,
                    owner_kind="memory.embedding",
                    owner_id=attempt["attempt_id"] + ":result",
                    value=result.as_dict(),
                    sources=refs,
                    expected_deletion_epoch=clock["deletion_epoch"],
                )
                result_ref = saved.payload_id
                if request["purpose"] == "document":
                    for row in rows:
                        vector = vector_lookup[row["facet_id"]]
                        payload = await self.payloads.put_json_in(
                            conn,
                            owner_kind="memory.vector",
                            owner_id=row["facet_id"],
                            value=vector,
                            sources=(
                                SourceRef(
                                    row["source_unit_id"], row["source_revision"], row["part_id"]
                                ),
                            ),
                            expected_deletion_epoch=clock["deletion_epoch"],
                        )
                        await conn.execute(
                            "INSERT INTO public.bot_memory_vectors(bot_id,facet_id,space_id,source_unit_id,payload_id,vector) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (
                                lease.bot_id,
                                row["facet_id"],
                                row["space_id"],
                                row["source_unit_id"],
                                payload.payload_id,
                                vector,
                            ),
                        )
                        await conn.execute(
                            "UPDATE public.bot_memory_index_facets SET state='READY',vector_payload_id=%s,error_code=NULL WHERE bot_id=%s AND facet_id=%s",
                            (payload.payload_id, lease.bot_id, row["facet_id"]),
                        )
            elif request["purpose"] == "document":
                state = (
                    ("PENDING" if attempt["ordinal"] < 3 else "REJECTED")
                    if result.status == "NOT_SENT"
                    else ("SUPPRESSED" if not publish else result.status)
                )
                if state == "RESPONDED":
                    state = "SUPPRESSED"
                await conn.execute(
                    "UPDATE public.bot_memory_index_facets SET state=%s,error_code=%s WHERE bot_id=%s AND request_id=%s",
                    (
                        state,
                        "MEMORY_PUBLICATION_REVOKED" if not publish else result.error_code,
                        lease.bot_id,
                        request["request_id"],
                    ),
                )
            await conn.execute(
                "UPDATE public.bot_memory_embedding_attempts SET state=%s,charged_tokens=%s,result_payload_id=%s,result_digest=%s,error_code=%s WHERE bot_id=%s AND attempt_id=%s",
                (
                    result.status,
                    charged,
                    result_ref,
                    result_digest,
                    result.error_code if publish else "MEMORY_PUBLICATION_REVOKED",
                    lease.bot_id,
                    attempt["attempt_id"],
                ),
            )
            if result_ref and attempt["deadline_at"] <= await self.store._now(conn):
                # Roll back the whole publication. Recovery conservatively keeps
                # the cost reservation if this exceptionally late commit fails.
                raise ForestError(
                    "MEMORY_PUBLICATION_EXPIRED", "Embedding publication expired before commit", 409
                )
            await self._final(conn, lease, binding)
            return {
                "status": result.status,
                "published": bool(result_ref),
                "request_id": request["request_id"],
                "result_payload_id": result_ref,
            }

    async def recover(self, lease, *, limit=32):
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            await self.payloads.lock_in(conn)
            attempts = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_embedding_attempts WHERE bot_id=%s AND state IN ('PREPARED','DISPATCHING') "
                    "AND (deadline_at<=clock_timestamp() OR host_incarnation<>%s OR host_owner_epoch<>%s) ORDER BY deadline_at LIMIT %s FOR UPDATE",
                    (lease.bot_id, lease.incarnation, lease.owner_epoch, limit),
                )
            ).fetchall()
            for attempt in attempts:
                status = "NOT_SENT" if attempt["state"] == "PREPARED" else "UNKNOWN"
                if status == "NOT_SENT":
                    await conn.execute(
                        "UPDATE public.bot_memory_embedding_budget_days SET reserved_tokens=reserved_tokens-%s WHERE bot_id=%s AND budget_day=%s",
                        (attempt["reserved_tokens"], lease.bot_id, attempt["budget_day"]),
                    )
                await conn.execute(
                    "UPDATE public.bot_memory_embedding_attempts SET state=%s,error_code='MEMORY_OWNER_LOST' WHERE bot_id=%s AND attempt_id=%s",
                    (status, lease.bot_id, attempt["attempt_id"]),
                )
                if status == "UNKNOWN":
                    await conn.execute(
                        "UPDATE public.bot_memory_index_facets SET state='UNKNOWN',error_code='MEMORY_OWNER_LOST' WHERE bot_id=%s AND request_id=%s",
                        (lease.bot_id, attempt["request_id"]),
                    )
            await self._final(conn, lease)
            return len(attempts)

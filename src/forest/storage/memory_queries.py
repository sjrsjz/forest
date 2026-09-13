"""Frozen native multimodal query/evidence, with exact PostgreSQL rank search.

The vector scan holds no Host/Conversation locks. Its results become evidence
only after current source authorization and the final deletion/config fence.
"""

from __future__ import annotations

import asyncio
import json
import math
import re

import psycopg
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_index import FUSION_REVISION, checked_vector, facets, fuse, profile
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.memory_config import MemoryConfig


class MemoryQueryStore:
    def __init__(self, index):
        self.index = index
        self.messages, self.store = index.messages, index.store
        self.sources, self.payloads, self.media = index.sources, index.payloads, index.media

    @staticmethod
    def _identifier(value):
        if (
            not isinstance(value, str)
            or not 1 <= len(value.encode()) <= 200
            or any(ord(c) < 32 for c in value)
        ):
            raise ValueError("Query identifier")
        return value

    @staticmethod
    def _receipt(row):
        return {
            key: row[key]
            for key in (
                "query_id",
                "config_revision",
                "conversation_id",
                "input_cut",
                "space_id",
                "source_unit_ids",
                "facet_ids",
                "query_modalities",
                "deletion_epoch",
            )
        }

    async def _query_in(self, conn, query_id, *, lock=False):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_queries WHERE bot_id=%s AND query_id=%s"
                + (" FOR UPDATE" if lock else ""),
                (self.store.bot_id, query_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("MEMORY_QUERY_NOT_FOUND", "Query does not exist in this Bot", 404)
        return row

    async def _lock_in(self, conn, lease, query_id):
        initial = await self._query_in(conn, query_id)
        location = await self.messages._locate(conn, initial["conversation_id"])
        binding = await self.messages.bindings._guard(
            conn, lease, location["binding_id"], data=False
        )
        conversation = await self.messages._conversation(conn, initial["conversation_id"])
        return await self._query_in(conn, query_id, lock=True), conversation, binding

    async def _authority_in(self, conn, query, conversation, input_cut):
        if (
            conversation["bot_id"] != self.store.bot_id
            or query["conversation_id"] != conversation["conversation_id"]
            or type(input_cut) is not int
            or query["input_cut"] != input_cut
            or not 0 <= input_cut <= conversation["context_applied_seq"]
        ):
            raise ForestError(
                "MEMORY_QUERY_SCOPE", "Evidence belongs to another frozen conversation cut", 403
            )
        # Config.guard_in consumes Bot scope, not an Actor capability. Its caller
        # already owns current Host/runtime and the Conversation row lock.
        cfg = await MemoryConfig.guard_in(conn, self.store, query["config_revision"])
        runtime = await self.store._runtime(conn)
        if not runtime["data_admission"] or any(
            runtime[key] != value for key, value in query["runtime_fence"].items()
        ):
            raise ForestError("MEMORY_QUERY_REVOKED", "Query runtime authority changed", 409)
        clock = await self.payloads.lock_in(conn)
        if clock["deletion_epoch"] != query["deletion_epoch"]:
            raise ForestError("MEMORY_QUERY_REVOKED", "Query deletion epoch changed", 409)
        view = await self.sources.authorize_sources_in(
            conn,
            conversation,
            input_cut=input_cut,
            source_unit_ids=query["source_unit_ids"],
            limit=256,
        )
        if {row["source_unit_id"] for row in view["sources"]} != set(query["source_unit_ids"]):
            raise ForestError("MEMORY_QUERY_REVOKED", "Query source lost its eligibility", 409)
        return cfg, clock, view["sources"]

    async def prepare(
        self, lease, revision, *, query_id, conversation_id, input_cut, source_unit_ids
    ):
        query_id = self._identifier(query_id)
        ids = list(source_unit_ids) if isinstance(source_unit_ids, (list, tuple)) else []
        if (
            not 1 <= len(ids) <= 32
            or any(not isinstance(value, str) or not 1 <= len(value) <= 256 for value in ids)
            or len(set(ids)) != len(ids)
            or type(input_cut) is not int
            or input_cut < 0
        ):
            raise ForestError(
                "MEMORY_QUERY_INVALID",
                "Query requires bounded distinct source IDs and a frozen cut",
                422,
            )
        basis = digest_json(
            {
                "config_revision": revision,
                "conversation_id": conversation_id,
                "input_cut": input_cut,
                "source_unit_ids": ids,
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            cfg, clock, conversation, binding = await self.index._guard(
                conn, lease, revision, conversation_id
            )
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_memory_queries WHERE bot_id=%s AND query_id=%s",
                    (self.store.bot_id, query_id),
                )
            ).fetchone()
            if existing:
                if existing["input_digest"] != basis:
                    raise ForestError(
                        "MEMORY_QUERY_CONFLICT", "Query identity already has different input", 409
                    )
                await self._authority_in(conn, existing, conversation, input_cut)
                await self.index._final(conn, lease, binding)
                return self._receipt(existing)
            view = await self.sources.authorize_sources_in(
                conn, conversation, input_cut=input_cut, source_unit_ids=ids, limit=256
            )
            sources = {row["source_unit_id"]: row for row in view["sources"]}
            if set(sources) != set(ids):
                raise ForestError(
                    "MEMORY_QUERY_SOURCE_UNAVAILABLE",
                    "Query source is unaccepted, outside its cut or restricted",
                    403,
                )
            space = profile(cfg["values"]).space_id
            selected = [facet for unit_id in ids for facet in facets(sources[unit_id], space)]
            if not selected:
                raise ForestError(
                    "MEMORY_QUERY_EMPTY", "Query has no supported native text or image facets", 422
                )
            if (
                len(selected) > cfg["values"]["query_max_parts"]
                or sum(facet["modality"] == "image" for facet in selected)
                > cfg["values"]["query_max_images"]
            ):
                raise ForestError(
                    "MEMORY_QUERY_BUDGET",
                    "Query exceeds its configured text/image facet limit",
                    422,
                )
            runtime = await self.store._runtime(conn)
            if not runtime["data_admission"]:
                raise ForestError("MEMORY_QUERY_REVOKED", "Memory query execution is paused", 409)
            for facet in selected:
                await conn.execute(
                    "INSERT INTO public.bot_memory_index_facets(bot_id,facet_id,space_id,source_unit_id,source_revision,part_id,selector,state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        self.store.bot_id,
                        facet["facet_id"],
                        space,
                        facet["source_unit_id"],
                        facet["source_revision"],
                        facet["part_id"],
                        Jsonb(facet),
                        "WAIT_MEDIA" if facet["modality"] == "image" else "QUEUED",
                    ),
                )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_memory_queries(bot_id,query_id,input_digest,config_revision,conversation_id,input_cut,space_id,source_unit_ids,facet_ids,query_modalities,deletion_epoch,runtime_fence) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        query_id,
                        basis,
                        revision,
                        conversation_id,
                        input_cut,
                        space,
                        Jsonb(ids),
                        Jsonb([facet["facet_id"] for facet in selected]),
                        Jsonb([facet["modality"] for facet in selected]),
                        clock["deletion_epoch"],
                        Jsonb(
                            {
                                key: runtime[key]
                                for key in (
                                    "policy_revision",
                                    "maintenance_epoch",
                                    "storage_generation",
                                )
                            }
                        ),
                    ),
                )
            ).fetchone()
            await self.index._final(conn, lease, binding)
            return self._receipt(row)

    async def _embedding_in(self, conn, query, clock):
        request = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_embedding_requests WHERE bot_id=%s AND request_id=%s",
                (self.store.bot_id, query["query_id"]),
            )
        ).fetchone()
        if request is None:
            return None, "MEMORY_QUERY_NO_EMBEDDING"
        if (
            request["purpose"] != "query"
            or request["facet_ids"] != query["facet_ids"]
            or request["space_id"] != query["space_id"]
            or request["config_revision"] != query["config_revision"]
            or request["conversation_id"] != query["conversation_id"]
            or request["input_cut"] != query["input_cut"]
            or request["deletion_epoch"] != query["deletion_epoch"]
        ):
            return None, "MEMORY_QUERY_EMBEDDING_CONFLICT"
        attempt = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_embedding_attempts WHERE bot_id=%s AND request_id=%s ORDER BY ordinal DESC LIMIT 1",
                (self.store.bot_id, query["query_id"]),
            )
        ).fetchone()
        if not attempt or attempt["state"] != "RESPONDED" or not attempt["result_payload_id"]:
            state = attempt["state"] if attempt else "MISSING"
            return None, "MEMORY_QUERY_EMBEDDING_" + state
        result = await self.payloads.read_json_in(
            conn, attempt["result_payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        if (
            result["space_id"] != query["space_id"]
            or result["status"] != "RESPONDED"
            or [vector["facet_id"] for vector in result["vectors"]] != query["facet_ids"]
        ):
            return None, "MEMORY_QUERY_EMBEDDING_INCOMPLETE"
        for vector in result["vectors"]:
            checked_vector(vector["values"], request["profile"]["dimensions"])
        return {
            "vectors": result["vectors"],
            "result_payload_id": attempt["result_payload_id"],
            "dimensions": request["profile"]["dimensions"],
        }, None

    async def _search(self, query, embedding, timeout):
        """One read-only snapshot; no Host, configuration, or source row locks."""
        ranked = []
        per_route = min(64, max(1, 256 // len(embedding["vectors"])))
        async with asyncio.timeout(timeout):
            async with self.store.pool.connection() as conn, conn.transaction():
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                await conn.execute(
                    "SELECT set_config('statement_timeout',%s,true)",
                    (str(min(5000, timeout * 1000)),),
                )
                for query_vector in embedding["vectors"]:
                    rows = await (
                        await conn.execute(
                            "SELECT v.facet_id,v.source_unit_id,v.payload_id, "
                            "(SELECT sum(d.value::double precision*q.value::double precision) "
                            "FROM unnest(v.vector) WITH ORDINALITY d(value,n) "
                            "JOIN unnest(%s::real[]) WITH ORDINALITY q(value,n) USING(n)) AS score "
                            "FROM public.bot_memory_vectors v JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                            "WHERE v.bot_id=%s AND v.space_id=%s AND h.state='ACTIVE' AND cardinality(v.vector)=%s "
                            "AND NOT(v.source_unit_id=ANY(%s)) ORDER BY score DESC,v.facet_id LIMIT %s",
                            (
                                query_vector["values"],
                                self.store.bot_id,
                                query["space_id"],
                                embedding["dimensions"],
                                query["source_unit_ids"],
                                per_route,
                            ),
                        )
                    ).fetchall()
                    if any(not math.isfinite(row["score"]) for row in rows):
                        raise ForestError(
                            "MEMORY_QUERY_VECTOR_INVALID",
                            "Search encountered an invalid vector",
                            409,
                        )
                    ranked.append((query_vector["facet_id"], rows))
        return ranked

    @staticmethod
    def _coverage(query, *, available, searched, reason=None):
        return {
            "query_facets_expected": len(query["facet_ids"]),
            "query_facets_available": available,
            "query_modalities": query["query_modalities"],
            "search_complete": searched,
            "index_consistency": "eventual",
            "candidate_limit": 256,
            "per_route_limit": 64,
            "fusion_revision": FUSION_REVISION,
            "reason_code": reason,
        }

    def _body(self, query, clock, *, status, reason, evidence, completeness):
        return {
            "bundle_id": "evidence-bundle:" + digest_json([self.store.bot_id, query["query_id"]]),
            "query_id": query["query_id"],
            "conversation_id": query["conversation_id"],
            "input_cut": query["input_cut"],
            "config_revision": query["config_revision"],
            "deletion_epoch": clock["deletion_epoch"],
            "fact_revision": clock["fact_revision"],
            "space_id": query["space_id"],
            "status": status,
            "reason_code": reason,
            "evidence": evidence,
            "completeness": completeness,
        }

    async def _freeze_in(self, conn, query, clock, body, *, parents=(), refs=(), writable=True):
        payload = None
        if writable:
            payload = await self.payloads.put_json_in(
                conn,
                owner_kind="memory.evidence",
                owner_id=body["bundle_id"],
                value=body,
                sources=tuple(refs),
                parents=tuple(parents),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            if payload.state != "ACTIVE":
                raise ForestError("MEMORY_EVIDENCE_REVOKED", "Evidence was already removed", 409)
        row = await (
            await conn.execute(
                "INSERT INTO public.bot_memory_evidence_bundles(bot_id,query_id,bundle_id,payload_id,digest,status,reason_code,deletion_epoch,fact_revision,source_unit_ids,completeness) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    self.store.bot_id,
                    query["query_id"],
                    body["bundle_id"],
                    payload.payload_id if payload else None,
                    payload.digest if payload else digest_json(body),
                    body["status"],
                    body["reason_code"],
                    clock["deletion_epoch"],
                    clock["fact_revision"],
                    Jsonb([item["source_unit_id"] for item in body["evidence"]]),
                    Jsonb(body["completeness"]),
                ),
            )
        ).fetchone()
        return {**body, "payload_id": row["payload_id"], "digest": row["digest"]}

    async def _saved_in(self, conn, query):
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_evidence_bundles WHERE bot_id=%s AND query_id=%s",
                (self.store.bot_id, query["query_id"]),
            )
        ).fetchone()

    def _metadata_only(self, query, row):
        return {
            **self._body(
                query,
                row,
                status=row["status"],
                reason=row["reason_code"],
                evidence=[],
                completeness=row["completeness"],
            ),
            "payload_id": row["payload_id"],
            "digest": row["digest"],
        }

    async def read_in(self, conn, query_id, *, conversation, input_cut):
        """Caller owns current Host/runtime and Conversation authority in this TX."""
        query = await self._query_in(conn, query_id)
        _, clock, _ = await self._authority_in(conn, query, conversation, input_cut)
        bundle = await self._saved_in(conn, query)
        if not bundle:
            raise ForestError(
                "MEMORY_EVIDENCE_NOT_READY", "Query evidence has not been frozen", 409
            )
        if not bundle["payload_id"] or bundle["deletion_epoch"] != clock["deletion_epoch"]:
            raise ForestError("MEMORY_EVIDENCE_REVOKED", "Evidence is no longer dispatchable", 409)
        required = sorted(set(query["source_unit_ids"]) | set(bundle["source_unit_ids"]))
        authorized = await self.sources.authorize_sources_in(
            conn, conversation, input_cut=input_cut, source_unit_ids=required, limit=256
        )
        if {row["source_unit_id"] for row in authorized["sources"]} != set(required):
            raise ForestError(
                "MEMORY_EVIDENCE_REVOKED", "A frozen evidence source lost authorization", 409
            )
        body = await self.payloads.read_json_in(
            conn, bundle["payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        if digest_json(body) != bundle["digest"]:
            raise ForestError(
                "MEMORY_EVIDENCE_CONFLICT",
                "Evidence content differs from its immutable receipt",
                409,
            )
        return {**body, "payload_id": bundle["payload_id"], "digest": bundle["digest"]}

    async def retrieve(self, lease, query_id, *, error_code=None):
        query_id = self._identifier(query_id)
        if error_code is not None and (
            not isinstance(error_code, str)
            or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", error_code)
        ):
            raise ValueError("Query failure code")
        embedding, reason, cfg = None, error_code, None
        async with self.store.pool.connection() as conn, conn.transaction():
            query, conversation, binding = await self._lock_in(conn, lease, query_id)
            existing = await self._saved_in(conn, query)
            if existing:
                result = (
                    (
                        await self.read_in(
                            conn, query_id, conversation=conversation, input_cut=query["input_cut"]
                        )
                    )
                    if existing["payload_id"]
                    else self._metadata_only(query, existing)
                )
                await self.index._final(conn, lease, binding)
                return result
            try:
                cfg, clock, _ = await self._authority_in(
                    conn, query, conversation, query["input_cut"]
                )
                if reason is None:
                    embedding, reason = await self._embedding_in(conn, query, clock)
            except ForestError as exc:
                reason = exc.code
            await self.index._final(conn, lease, binding)
        ranked = []
        if embedding and reason is None:
            try:
                ranked = await self._search(query, embedding, cfg["values"]["query_timeout"])
            except (TimeoutError, psycopg.errors.QueryCanceled):
                reason = "MEMORY_QUERY_SEARCH_TIMEOUT"
            except (ForestError, psycopg.Error) as exc:
                reason = exc.code if isinstance(exc, ForestError) else "MEMORY_QUERY_SEARCH_FAILED"
        async with self.store.pool.connection() as conn, conn.transaction():
            query, conversation, binding = await self._lock_in(conn, lease, query_id)
            existing = await self._saved_in(conn, query)
            if existing:
                result = (
                    (
                        await self.read_in(
                            conn, query_id, conversation=conversation, input_cut=query["input_cut"]
                        )
                    )
                    if existing["payload_id"]
                    else self._metadata_only(query, existing)
                )
                await self.index._final(conn, lease, binding)
                return result
            writable = True
            try:
                cfg, clock, query_sources = await self._authority_in(
                    conn, query, conversation, query["input_cut"]
                )
            except ForestError as exc:
                clock, query_sources, writable, reason = (
                    await self.payloads.lock_in(conn),
                    [],
                    False,
                    exc.code,
                )
            evidence, parents = [], set()
            refs = {
                SourceRef(row["source_unit_id"], row["source_revision"]) for row in query_sources
            }
            parents.update(row["payload_id"] for row in query_sources)
            completeness = self._coverage(
                query,
                available=len(embedding["vectors"]) if embedding else 0,
                searched=bool(embedding and reason is None),
                reason=reason,
            )
            if reason is None and embedding and writable:
                try:
                    await self.payloads.require_available_in(
                        conn,
                        [embedding["result_payload_id"]],
                        expected_deletion_epoch=clock["deletion_epoch"],
                    )
                    evidence, extra_parents, extra_refs, detail = await self._select_in(
                        conn, query, conversation, cfg, clock, ranked
                    )
                    parents.update(extra_parents)
                    parents.add(embedding["result_payload_id"])
                    refs.update(extra_refs)
                    completeness.update(detail)
                except ForestError as exc:
                    reason, evidence = exc.code, []
                    completeness = self._coverage(
                        query, available=len(embedding["vectors"]), searched=False, reason=reason
                    )
            body = self._body(
                query,
                clock,
                status="DEGRADED" if reason else "READY",
                reason=reason,
                evidence=evidence,
                completeness=completeness,
            )
            result = await self._freeze_in(
                conn, query, clock, body, parents=parents, refs=refs, writable=writable
            )
            await self.index._final(conn, lease, binding)
            return result

    async def _select_in(self, conn, query, conversation, cfg, clock, ranked):
        merged = fuse(ranked, limit=256)
        candidate_ids = [row["source_unit_id"] for row in merged]
        authorized = await self.sources.authorize_sources_in(
            conn,
            conversation,
            input_cut=query["input_cut"],
            source_unit_ids=candidate_ids,
            limit=256,
            include_payload=False,
        )
        sources = {row["source_unit_id"]: row for row in authorized["sources"]}
        sizes = {
            row["payload_id"]: row["byte_length"]
            for row in await (
                await conn.execute(
                    "SELECT payload_id,byte_length FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=ANY(%s)",
                    (self.store.bot_id, [row["payload_id"] for row in sources.values()]),
                )
            ).fetchall()
        }
        vector_rows = {row["facet_id"]: row for _, rows in ranked for row in rows}
        captures = await (
            await conn.execute(
                "SELECT h.* FROM public.bot_memory_media_heads h JOIN public.bot_payload_headers p USING(bot_id,payload_id) WHERE h.bot_id=%s AND h.source_unit_id=ANY(%s) "
                "AND h.status='CAPTURED' AND p.state='ACTIVE' AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id) ORDER BY h.source_unit_id,h.part_id LIMIT 256",
                (self.store.bot_id, candidate_ids),
            )
        ).fetchall()
        media = {(row["source_unit_id"], row["part_id"]): row for row in captures}
        result, parents, refs, skipped = [], set(), set(), []
        bytes_left, images_left, tokens_left = (
            cfg["values"][key]
            for key in ("max_result_bytes", "max_result_images", "max_result_tokens")
        )
        for match in merged:
            source = sources.get(match["source_unit_id"])
            if source is None:
                continue
            if sizes[source["payload_id"]] > min(bytes_left, tokens_left):
                skipped.append(source["source_unit_id"])
                continue
            value = await self.payloads.read_json_in(
                conn, source["payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
            )
            source = {**source, "author": value["author"], "parts": value["parts"]}
            parts, current_parents, image_bytes, images = [], {source["payload_id"]}, 0, 0
            for original in source["parts"]:
                part = dict(original)
                if part["type"] == "image":
                    capture = media.get((source["source_unit_id"], part["part_id"]))
                    part["availability"] = "AVAILABLE" if capture else "UNAVAILABLE"
                    if capture:
                        part["media_revision"] = 1
                        current_parents.add(capture["payload_id"])
                        image_bytes += capture["byte_length"]
                        images += 1
                parts.append(part)
            current_parents.update(
                vector_rows[facet_id]["payload_id"] for _, facet_id in match["matches"]
            )
            item = {
                "unit_id": "evidence:" + source["source_unit_id"],
                "source_unit_id": source["source_unit_id"],
                "source_revision": source["source_revision"],
                "source": {**source["source"], "source_event_id": source["source_event_id"]},
                "author": source["author"],
                "parts": parts,
                "rank_score": match["rank_score"],
                "matched_facets": [
                    {"query_facet_id": q, "facet_id": d} for q, d in match["matches"]
                ],
            }
            size = len(json.dumps(item, ensure_ascii=False, allow_nan=False).encode())
            if (
                images > images_left
                or size + image_bytes > bytes_left
                or size > tokens_left
                or len(parents | current_parents) > 192
            ):
                skipped.append(source["source_unit_id"])
                continue
            await self.payloads.require_available_in(
                conn, current_parents, expected_deletion_epoch=clock["deletion_epoch"]
            )
            result.append(item)
            parents.update(current_parents)
            refs.add(SourceRef(source["source_unit_id"], source["source_revision"]))
            bytes_left -= size + image_bytes
            images_left -= images
            tokens_left -= size
            if len(result) >= cfg["values"]["query_limit"]:
                break
        return (
            result,
            parents,
            refs,
            {
                "candidates_considered": len(merged),
                "authorized_candidates": len(sources),
                "result_budget_exclusions": skipped,
                "returned_sources": len(result),
            },
        )

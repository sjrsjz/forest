"""Package-owned vector service boundary and replay evidence validation."""

import hashlib
import json
import math
import struct

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _physical_digest(values):
    try:
        packed = struct.pack("!" + "f" * len(values), *values)
    except (OverflowError, struct.error) as exc:
        raise ForestError(
            "VECTOR_REPLAY_VECTOR_INVALID",
            "Vector cannot be represented by the physical index", 422,
        ) from exc
    return "f32-sha256:" + hashlib.sha256(packed).hexdigest()


class MilvusVectorIndex(Plugin):
    """Own public vector contracts; the bound effect performs Milvus I/O."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.space_id = config["space_id"]
        self.dimensions = config["dimensions"]

    def replacement_facts(self):
        return {"space": {
            "space_id": self.space_id,
            "dimensions": self.dimensions,
        }}

    @service(
        "vector.index.upsert", semantics="idempotent", contract="3.0",
        input_schema="schemas/upsert-input.json",
        output_schema="schemas/upsert-output.json",
    )
    async def upsert(self, payload, ctx):
        request = _plain(payload)
        replay = request["replay"]
        reference = replay["result"]
        if (
            request["space_id"] != self.space_id
            or reference["owner_component_id"] != "embedding.multimodal"
            or reference["owner_kind"] != "embedding.result"
        ):
            raise ForestError(
                "VECTOR_REPLAY_IDENTITY_INVALID",
                "Vector replay source belongs to another space or owner", 409,
            )
        result = _plain(await ctx.payloads.read_json(reference))
        index = replay["item_index"]
        vectors = result.get("vectors")
        if (
            result.get("protocol") != "forest.embedding-result/1"
            or result.get("status") != "RESPONDED"
            or result.get("space_id") != self.space_id
            or not isinstance(vectors, list)
            or index >= len(vectors)
        ):
            raise ForestError(
                "VECTOR_REPLAY_RESULT_INVALID",
                "Vector replay result does not contain the requested item", 409,
            )
        item = vectors[index]
        values = item.get("values") if isinstance(item, dict) else None
        if (
            not isinstance(item, dict)
            or item.get("index") != index
            or item.get("facet_id") != request["ref_id"]
            or not isinstance(values, list)
            or len(values) != self.dimensions
            or any(type(value) not in (int, float) or not math.isfinite(value)
                   for value in values)
            or values != request["vector"]
            or _digest(values) != replay["vector_digest"]
        ):
            raise ForestError(
                "VECTOR_REPLAY_VECTOR_INVALID",
                "Vector differs from its immutable replay result", 409,
            )
        reserved = _plain(await ctx.commands.call(
            "vector.ref.reserve",
            {**request, "source_input_digest": _digest(request),
             "physical_digest": _physical_digest(values)},
            operation_id=ctx.operations.stable("reserve-ref:" + request["ref_id"]),
            timeout=10,
        ))
        if reserved["ref_id"] != request["ref_id"] or reserved["space_id"] != self.space_id:
            raise ForestError(
                "VECTOR_REF_RESERVATION_INVALID",
                "Ref reservation confirmed another vector", 502,
            )
        written = _plain(await ctx.services.call(
            "vector.transport.upsert",
            {**request, "input_digest": reserved["input_digest"]},
            operation_id=ctx.operations.stable("milvus-upsert:" + request["ref_id"]),
            contract_major=2,
            timeout=30,
        ))
        if written != {"ref_id": request["ref_id"], "space_id": self.space_id}:
            raise ForestError(
                "VECTOR_TRANSPORT_RESULT_INVALID",
                "Milvus transport confirmed another vector", 502,
            )
        activated = _plain(await ctx.commands.call(
            "vector.ref.activate",
            {"ref_id": request["ref_id"], "input_digest": reserved["input_digest"]},
            operation_id=ctx.operations.stable("activate-ref:" + request["ref_id"]),
            timeout=10,
        ))
        if activated != {"ref_id": request["ref_id"],
                         "space_id": self.space_id, "state": "ACTIVE"}:
            raise ForestError(
                "VECTOR_REF_ACTIVATION_INVALID",
                "Ref activation confirmed another vector", 502,
            )
        return {"ref_id": request["ref_id"], "space_id": self.space_id}

    @service(
        "vector.index.search", semantics="read", contract="2.0",
        input_schema="schemas/search-input.json",
        output_schema="schemas/search-output.json",
    )
    async def search(self, payload, ctx):
        request = _plain(payload)
        await ctx.queries.call(
            "vector.ref.require-query", request, timeout=10,
        )
        found = _plain(await ctx.services.call(
            "vector.transport.search", request,
            operation_id=ctx.operations.stable(
                "milvus-search:" + _digest(request)[7:]
            ),
            timeout=30,
        ))
        if (found["query_ref_id"] != request["query_ref_id"]
                or found["space_id"] != self.space_id):
            raise ForestError(
                "VECTOR_TRANSPORT_RESULT_INVALID",
                "Milvus transport confirmed another query", 502,
            )
        return await ctx.queries.call(
            "vector.ref.filter-active", found, timeout=10,
        )

    @service(
        "vector.index.delete", semantics="idempotent", contract="1.0",
        input_schema="schemas/delete-input.json",
        output_schema="schemas/delete-output.json",
    )
    async def delete(self, payload, ctx):
        request = _plain(payload)
        if request["space_id"] != self.space_id:
            raise ForestError(
                "VECTOR_INDEX_SPACE_MISMATCH",
                "Vector belongs to another index space", 409,
            )
        return await ctx.commands.call(
            "vector.ref.tombstone", request,
            operation_id=ctx.operations.stable("tombstone:" + _digest(request)[7:]),
            timeout=10,
        )

    @job("vector.refs.audit")
    async def audit_refs(self, task, ctx):
        request = _plain(task["payload"])
        if request["space_id"] != self.space_id:
            raise ForestError(
                "VECTOR_AUDIT_SPACE_MISMATCH",
                "Audit work belongs to another vector space", 409,
            )
        phase = request["phase"]
        if phase == "REF":
            refs = request["refs"]
            if not refs or refs != sorted(set(refs)):
                raise ForestError(
                    "VECTOR_AUDIT_CLAIM_INVALID",
                    "Audit ref page is not a bounded ordered page", 502,
                )
            observed = _plain(await ctx.services.call(
                "vector.transport.audit",
                {"space_id": self.space_id, "mode": "GET", "ref_ids": refs},
                operation_id=ctx.operations.stable(
                    "audit-get:" + task["work_id"]
                ),
                timeout=35,
            ))
            if observed["space_id"] != self.space_id or observed["mode"] != "GET":
                raise ForestError(
                    "VECTOR_AUDIT_TRANSPORT_INVALID",
                    "Milvus audit returned another ref page", 502,
                )
            rows = {row["ref_id"]: row for row in observed["rows"]}
            if len(rows) != len(observed["rows"]) or set(rows) - set(refs):
                raise ForestError(
                    "VECTOR_AUDIT_TRANSPORT_INVALID",
                    "Milvus audit returned duplicate or unrelated refs", 502,
                )
            observations = [
                {"ref_id": ref_id, "physical": rows.get(ref_id)}
                for ref_id in refs
            ]
            next_ref = refs[-1]
            done = len(refs) < request["limit"]
        elif phase == "PHYSICAL":
            observed = _plain(await ctx.services.call(
                "vector.transport.audit",
                {"space_id": self.space_id, "mode": "SCAN",
                 "after_ref": request["after_ref"], "limit": request["limit"]},
                operation_id=ctx.operations.stable(
                    "audit-scan:" + task["work_id"]
                ),
                timeout=35,
            ))
            if observed["space_id"] != self.space_id or observed["mode"] != "SCAN":
                raise ForestError(
                    "VECTOR_AUDIT_TRANSPORT_INVALID",
                    "Milvus audit returned another physical page", 502,
                )
            rows = observed["rows"]
            refs = [row["ref_id"] for row in rows]
            if refs != sorted(set(refs)):
                raise ForestError(
                    "VECTOR_AUDIT_TRANSPORT_INVALID",
                    "Milvus audit returned an unordered page", 502,
                )
            observations = [
                {"ref_id": row["ref_id"], "physical": row}
                for row in rows
            ]
            next_ref = observed["next_ref"]
            done = observed["done"]
        else:
            raise ForestError(
                "VECTOR_AUDIT_CLAIM_INVALID", "Unknown audit phase", 502,
            )
        quarantined = False
        if observations:
            decision = _plain(await ctx.commands.call(
                "vector.audit.adjudicate",
                {
                    "space_id": self.space_id,
                    "binding_revision": request["binding_revision"],
                    "collection_id": request["collection_id"],
                    "cycle_seq": request["cycle_seq"],
                    "claim_token": request["claim_token"],
                    "observations": observations,
                },
                operation_id=ctx.operations.stable(
                    "audit-decide:" + task["work_id"] + ":" +
                    _digest(observations)[7:]
                ),
                timeout=15,
            ))
            if decision["state"] not in ("READY", "REBUILD_REQUIRED"):
                raise ForestError(
                    "VECTOR_AUDIT_DECISION_INVALID",
                    "Package audit command returned another state", 502,
                )
            quarantined = decision["state"] == "REBUILD_REQUIRED"
        return {
            "space_id": self.space_id, "phase": phase,
            "binding_revision": request["binding_revision"],
            "collection_id": request["collection_id"],
            "next_ref": next_ref, "done": done,
            "quarantined": quarantined,
        }

    @job("vector.tombstones.sweep")
    async def sweep_tombstones(self, task, ctx):
        request = _plain(task["payload"])
        if request["space_id"] != self.space_id:
            raise ForestError(
                "VECTOR_SWEEP_SPACE_MISMATCH",
                "Tombstone sweep belongs to another vector space", 409,
            )
        response = _plain(await ctx.services.call(
            "vector.transport.prune", request,
            operation_id=ctx.operations.stable("milvus-prune:" + _digest(request)[7:]),
            timeout=30,
        ))
        if response != request:
            raise ForestError(
                "VECTOR_SWEEP_RESULT_INVALID",
                "Milvus prune confirmed another tombstone batch", 502,
            )
        return response

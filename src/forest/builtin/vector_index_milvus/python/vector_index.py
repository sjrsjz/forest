"""Package-owned vector service boundary and replay evidence validation."""

import hashlib
import json
import math

from forest.errors import ForestError
from forest.sdk import Plugin, service


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
        return await ctx.services.call(
            "vector.transport.upsert", request,
            operation_id=ctx.operations.stable("milvus-upsert:" + request["ref_id"]),
            timeout=30,
        )

    @service(
        "vector.index.search", semantics="read", contract="2.0",
        input_schema="schemas/search-input.json",
        output_schema="schemas/search-output.json",
    )
    async def search(self, payload, ctx):
        request = _plain(payload)
        return await ctx.services.call(
            "vector.transport.search", request,
            operation_id=ctx.operations.stable(
                "milvus-search:" + _digest(request)[7:]
            ),
            timeout=30,
        )

    @service(
        "vector.index.delete", semantics="idempotent", contract="1.0",
        input_schema="schemas/delete-input.json",
        output_schema="schemas/delete-output.json",
    )
    async def delete(self, payload, ctx):
        request = _plain(payload)
        return await ctx.services.call(
            "vector.transport.delete", request,
            operation_id=ctx.operations.stable("milvus-delete:" + _digest(request)[7:]),
            timeout=30,
        )

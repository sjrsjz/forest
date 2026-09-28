"""Bounded, source-attributed management projection of trial evidence."""

import base64
import json

from forest.errors import ForestError


async def _authorized_evidence(ctx, payload):
    query_id = payload["query_id"]
    authority_input = {"query_id": query_id, "requested_by": payload["requested_by"]}
    state = await ctx.queries.call(
        "memory.read-management-trial", authority_input, timeout=10
    )
    if state["state"] != "READY" or state["evidence_ref"] is None:
        raise ForestError("MEMORY_TRIAL_NOT_READY", "Trial evidence is not ready", 409)
    reference = state["evidence_ref"]
    raw = await ctx.payloads.read_bytes(reference)
    try:
        evidence = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ForestError("MEMORY_EVIDENCE_INVALID", "Trial evidence is not JSON", 409) from exc
    if (
        not isinstance(evidence, dict)
        or evidence.get("protocol") != "forest.memory-trial-evidence/1"
        or evidence.get("input_kind") != "ADMIN_TRIAL"
        or evidence.get("current_source") is not None
        or evidence.get("bot_id") != ctx.identity["bot_id"]
        or evidence.get("query_id") != query_id
        or not isinstance(evidence.get("evidence"), list)
        or len(evidence["evidence"]) > 64
    ):
        raise ForestError("MEMORY_EVIDENCE_INVALID", "Trial evidence identity differs", 409)
    current = await ctx.queries.call(
        "memory.read-management-trial", authority_input, timeout=10
    )
    if current["state"] != "READY" or current["evidence_ref"] != reference:
        raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Trial evidence changed", 409)
    return evidence, reference


async def read_page(ctx, payload):
    evidence, _reference = await _authorized_evidence(ctx, payload)
    query_id = payload["query_id"]
    after = payload.get("after_index", -1)
    limit = payload.get("limit", 4)
    values = evidence["evidence"]
    selected = values[after + 1:after + 1 + limit]
    items = []
    for offset, row in enumerate(selected, start=after + 1):
        if not isinstance(row, dict) or not isinstance(row.get("parts"), list):
            raise ForestError("MEMORY_EVIDENCE_INVALID", "Trial evidence item is invalid", 409)
        text = "\n".join(
            part.get("text", "") for part in row["parts"]
            if isinstance(part, dict) and part.get("kind") == "text"
        )
        images = [
            {
                "part_id": part["part_id"], "media_type": part["media_type"],
                "width": part["width"], "height": part["height"],
                "media_ref": part["media_ref"],
            }
            for part in row["parts"]
            if isinstance(part, dict) and part.get("kind") == "image"
        ]
        items.append({
            "item_index": offset,
            "source_unit_id": row["source_unit_id"],
            "source_id": row["source_id"],
            "source_kind": row["source_kind"],
            "platform": row.get("platform"),
            "external_group_id": row.get("external_group_id"),
            "trunk_id": row.get("trunk_id"),
            "author_subject_ref": row["author_subject_ref"],
            "occurred_at": row.get("occurred_at"),
            "is_seed": row["is_seed"],
            "rank_score": row["rank_score"],
            "excerpt": text[:2048],
            "excerpt_truncated": len(text) > 2048 or row.get("content_truncated", False),
            "images": images[:8],
            "image_count": len(images),
        })
    end = after + len(items)
    return {
        "protocol": "forest.memory-trial-result-page/1",
        "query_id": query_id,
        "bundle_id": evidence["bundle_id"],
        "graph_snapshot_ref": evidence["graph_snapshot_ref"],
        "total_count": len(values),
        "missing_count": evidence["missing_count"],
        "content_truncated": evidence["content_truncated"],
        "truncated": evidence["truncated"],
        "association_sampled": evidence["association_sampled"],
        "items": items,
        "next_index": end if end + 1 < len(values) else None,
        "has_more": end + 1 < len(values),
    }


async def read_image(ctx, payload):
    """Return one exact bounded image chunk from this actor's live evidence."""

    evidence, evidence_ref = await _authorized_evidence(ctx, payload)
    image_part = next((
        part
        for row in evidence["evidence"]
        if isinstance(row, dict)
        and row.get("source_unit_id") == payload["source_unit_id"]
        and isinstance(row.get("parts"), list)
        for part in row["parts"]
        if isinstance(part, dict)
        and part.get("kind") == "image"
        and part.get("part_id") == payload["part_id"]
    ), None)
    if image_part is None:
        raise ForestError("MEMORY_TRIAL_IMAGE_NOT_FOUND", "Image is not in trial evidence", 404)
    media_type = image_part.get("media_type")
    if media_type not in ("image/png", "image/jpeg", "image/webp"):
        raise ForestError("MEMORY_EVIDENCE_INVALID", "Evidence image type is invalid", 409)
    image_ref = image_part.get("media_ref")
    if not isinstance(image_ref, dict):
        raise ForestError("MEMORY_EVIDENCE_INVALID", "Evidence image ref is invalid", 409)
    offset = payload.get("offset", 0)
    chunk = await ctx.payloads.read_chunk(image_ref, offset=offset)
    if chunk["byte_length"] > 32 * 1024 * 1024:
        raise ForestError("MEMORY_TRIAL_IMAGE_TOO_LARGE", "Image exceeds display bound", 413)
    if offset >= chunk["byte_length"]:
        raise ForestError("PAYLOAD_RANGE_INVALID", "Image offset exceeds its bytes", 416)
    current = await ctx.queries.call(
        "memory.read-management-trial",
        {"query_id": payload["query_id"], "requested_by": payload["requested_by"]},
        timeout=10,
    )
    if current["state"] != "READY" or current["evidence_ref"] != evidence_ref:
        raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Trial evidence changed", 409)
    return {
        "protocol": "forest.memory-trial-image-chunk/1",
        "query_id": payload["query_id"],
        "source_unit_id": payload["source_unit_id"],
        "part_id": payload["part_id"],
        "media_type": media_type,
        "width": image_part["width"], "height": image_part["height"],
        "digest": image_ref["digest"],
        "offset": chunk["offset"], "byte_length": chunk["byte_length"],
        "eof": chunk["eof"],
        "data_base64": base64.b64encode(chunk["content"]).decode("ascii"),
    }

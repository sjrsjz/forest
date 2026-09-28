"""Bounded, source-attributed management projection of trial evidence."""

import base64
import io
import json

from PIL import Image, ImageOps, UnidentifiedImageError

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
    """Return one small preview only when it belongs to this actor's live trial."""

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
    raw = await ctx.payloads.read_bytes(image_ref)
    if len(raw) > 32 * 1024 * 1024:
        raise ForestError("MEMORY_TRIAL_IMAGE_TOO_LARGE", "Image exceeds preview input bound", 413)
    try:
        with Image.open(io.BytesIO(raw)) as image:
            width, height = image.size
            if (
                image.format != {"image/png": "PNG", "image/jpeg": "JPEG", "image/webp": "WEBP"}[media_type]
                or width != image_part.get("width")
                or height != image_part.get("height")
                or width * height > 20_000_000
            ):
                raise ForestError("MEMORY_EVIDENCE_INVALID", "Evidence image metadata differs", 409)
            image.load()
            image = ImageOps.exif_transpose(image)
            if image.mode in ("RGBA", "LA") or "transparency" in image.info:
                rgba = image.convert("RGBA")
                canvas = Image.new("RGB", rgba.size, "white")
                canvas.paste(rgba, mask=rgba.getchannel("A"))
                image = canvas
            else:
                image = image.convert("RGB")
            preview = image.copy()
            for side in (1024, 768, 512, 384, 256, 192):
                preview.thumbnail((side, side), Image.Resampling.LANCZOS)
                for quality in (76, 62):
                    buffer = io.BytesIO()
                    preview.save(buffer, format="JPEG", quality=quality, optimize=True)
                    if buffer.tell() <= 96 * 1024:
                        current = await ctx.queries.call(
                            "memory.read-management-trial",
                            {"query_id": payload["query_id"], "requested_by": payload["requested_by"]},
                            timeout=10,
                        )
                        if current["state"] != "READY" or current["evidence_ref"] != evidence_ref:
                            raise ForestError(
                                "MEMORY_EVIDENCE_UNAVAILABLE", "Trial evidence changed", 409
                            )
                        return {
                            "protocol": "forest.memory-trial-image/1",
                            "query_id": payload["query_id"],
                            "source_unit_id": payload["source_unit_id"],
                            "part_id": payload["part_id"],
                            "media_type": "image/jpeg",
                            "width": preview.width,
                            "height": preview.height,
                            "data_base64": base64.b64encode(buffer.getvalue()).decode("ascii"),
                        }
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError) as exc:
        raise ForestError("MEMORY_EVIDENCE_INVALID", "Evidence image bytes are invalid", 409) from exc
    raise ForestError("MEMORY_TRIAL_IMAGE_TOO_LARGE", "Image cannot fit preview bound", 413)

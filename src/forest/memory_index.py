"""Deterministic index facets and rank fusion, without provider or DB authority."""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict

from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.packages import digest_json

CHUNKER_REVISION = "utf8-contiguous-4096-v1"
FUSION_REVISION = "equal-facet-rrf-k60-v1"


def profile(values):
    return EmbeddingProfile(**{key: values[key] for key in (
        "deployment_id", "model_revision", "model", "dimensions"
    )})


def text_chunks(text, maximum=4096):
    """Contiguous character spans; no lost whitespace or partial UTF-8 codepoint."""
    if not isinstance(text, str) or type(maximum) is not int or not 4 <= maximum <= 8192:
        raise ValueError("Invalid text chunk boundary")
    start, count = 0, 0
    for index, char in enumerate(text):
        size = len(char.encode("utf-8"))
        if count + size > maximum:
            yield start, index, text[start:index]
            start, count = index, 0
        count += size
    if start < len(text):
        yield start, len(text), text[start:]


def facets(source, space_id):
    result = []
    for part in source["parts"]:
        if part.get("type") not in {"text", "image"}:
            continue
        spans = (text_chunks(part["text"]) if part["type"] == "text" else [(0, 0, None)])
        for start, end, content in spans:
            selector = {
                "source_unit_id": source["source_unit_id"],
                "source_revision": source["source_revision"],
                "source_digest": source["content_digest"],
                "part_id": part["part_id"], "modality": part["type"],
                "start_char": start, "end_char": end, "chunker_revision": CHUNKER_REVISION,
                "media_ref": part.get("media_ref"),
            }
            result.append({
                **selector, "space_id": space_id,
                "facet_id": "facet:" + digest_json([space_id, selector]),
                "text_digest": ("sha256:" + hashlib.sha256(content.encode()).hexdigest()
                                if content is not None else None),
            })
    return result


def fuse(ranked_lists, limit=8):
    """Merge ranks of distinct query facets; vector scores are never averaged."""
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("Invalid retrieval bound")
    scores, matches = defaultdict(float), defaultdict(set)
    for query_facet_id, rows in ranked_lists:
        seen = set()
        for row in rows:
            source = row["source_unit_id"]
            if source in seen:
                continue
            seen.add(source)
            scores[source] += 1 / (60 + len(seen))
            matches[source].add((query_facet_id, row["facet_id"]))
    return [{"source_unit_id": source, "rank_score": scores[source],
             "matches": sorted(matches[source])}
            for source in sorted(scores, key=lambda item: (-scores[item], item))[:limit]]


def checked_vector(value, dimensions):
    if (not isinstance(value, (list, tuple)) or len(value) != dimensions
            or any(type(v) not in {int, float} or not math.isfinite(v) for v in value)):
        raise ForestError("MEMORY_VECTOR_INVALID", "Vector does not match its space", 422)
    norm = math.sqrt(sum(v * v for v in value))
    if not 0.999 <= norm <= 1.001:
        raise ForestError("MEMORY_VECTOR_INVALID", "Vector must use the declared normalization", 422)
    return list(value)

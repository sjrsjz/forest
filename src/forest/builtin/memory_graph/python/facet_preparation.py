"""Deterministic facets over immutable source parts."""

import hashlib
import json

TEXT_WINDOW_CHARS = 2048
TEXT_OVERLAP_CHARS = 128
MAX_DOCUMENT_FACETS = 4096
FACET_BATCH_SIZE = 64
MAX_QUERY_FACETS = 256
MAX_QUERY_IMAGE_BYTES = 33554432
MAX_QUERY_SINGLE_IMAGE_BYTES = 8388608
CHUNKER_VERSION = "unicode-codepoints-2048-128/1"


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _facet_id(source_unit_id, source_revision, space_id, part, start, end):
    return "facet:" + _digest([
        CHUNKER_VERSION, source_unit_id, source_revision, space_id,
        part["part_id"], part["payload"]["digest"], start, end,
    ])[7:]


def document_facets(
    source_unit_id, source_revision, space_id, parts, text_parts,
    *, max_facets=MAX_DOCUMENT_FACETS,
):
    """Build stable text spans and one image facet per image part."""

    facets = []
    for part in parts:
        reference = part["payload"]
        if part["part_kind"] == "IMAGE":
            byte_length = part["byte_length"]
            if type(byte_length) is not int or not 0 < byte_length <= 33554432:
                raise ValueError("Source image length is invalid")
            facets.append({
                "facet_id": _facet_id(
                    source_unit_id, source_revision, space_id, part, None, None
                ),
                "part_id": part["part_id"],
                "modality": "IMAGE",
                "selector_start_char": None,
                "selector_end_char": None,
                "facet_content_digest": reference["digest"],
                "input_byte_length": byte_length,
                "input_payload": reference,
            })
        elif part["part_kind"] == "TEXT":
            content = text_parts[part["part_id"]]
            start = 0
            while start < len(content):
                end = min(start + TEXT_WINDOW_CHARS, len(content))
                chunk = content[start:end]
                if chunk.strip():
                    chunk_bytes = chunk.encode("utf-8")
                    facets.append({
                        "facet_id": _facet_id(
                            source_unit_id, source_revision, space_id, part, start, end
                        ),
                        "part_id": part["part_id"],
                        "modality": "TEXT",
                        "selector_start_char": start,
                        "selector_end_char": end,
                        "facet_content_digest": "sha256:" + hashlib.sha256(
                            chunk_bytes
                        ).hexdigest(),
                        "input_byte_length": len(chunk_bytes),
                        "input_payload": reference,
                    })
                    if len(facets) > max_facets:
                        raise ValueError("Source exceeds the facet limit")
                if end == len(content):
                    break
                start = end - TEXT_OVERLAP_CHARS
        else:
            raise ValueError("Source part modality is unsupported")
        if len(facets) > max_facets:
            raise ValueError("Source exceeds the facet limit")
    return facets


def batch_operation_id(source_unit_id, source_revision, space_id, batch_index):
    return "facet-batch:" + _digest([
        CHUNKER_VERSION, source_unit_id, source_revision, space_id, batch_index,
    ])[7:]


def query_part_facets(query_id, space_id, part, text_parts):
    identity = part["input_identity"]
    if identity["kind"] not in {"SOURCE_UNIT", "ADMIN_TRIAL"}:
        raise ValueError("Query input identity kind is unsupported")
    input_ref = identity["ref"]
    input_revision = identity["revision"]
    if not isinstance(input_ref, str) or not input_ref or type(input_revision) is not int or input_revision < 1:
        raise ValueError("Query input identity is invalid")
    if identity["kind"] == "SOURCE_UNIT" and (
        input_ref != part["source_unit_id"] or input_revision != part["source_revision"]
    ):
        raise ValueError("Query source identity differs from its admitted part")
    if identity["kind"] == "ADMIN_TRIAL" and any(
        field in part for field in ("source_unit_id", "source_revision", "occurrence_id")
    ):
        raise ValueError("Trial input must not carry admitted source identity")
    facets = document_facets(
        input_ref, input_revision, space_id, [part], text_parts,
        max_facets=FACET_BATCH_SIZE,
    )
    if part["part_kind"] == "IMAGE" and part["byte_length"] > MAX_QUERY_SINGLE_IMAGE_BYTES:
        raise ValueError("Query image exceeds the embedding batch limit")
    return [
        {**facet, "facet_id": "query-facet:" + _digest([
            query_id, identity["kind"], facet["facet_id"]
        ])[7:]}
        for facet in facets
    ]


def query_part_operation_id(query_id, space_id, part_index):
    return "query-facet-part:" + _digest([CHUNKER_VERSION, query_id, space_id, part_index])[7:]

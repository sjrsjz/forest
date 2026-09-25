"""Deterministic document facets over archived source parts."""

import hashlib
import json

TEXT_WINDOW_CHARS = 2048
TEXT_OVERLAP_CHARS = 128
MAX_DOCUMENT_FACETS = 4096
FACET_BATCH_SIZE = 64
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


def document_facets(source_unit_id, source_revision, space_id, parts, text_parts):
    """Build stable text spans and one image facet per image part."""

    facets = []
    for part in parts:
        reference = part["payload"]
        if part["part_kind"] == "IMAGE":
            byte_length = part["byte_length"]
            if type(byte_length) is not int or not 0 < byte_length <= 33554432:
                raise ValueError("Archived image length is invalid")
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
                if end == len(content):
                    break
                start = end - TEXT_OVERLAP_CHARS
        else:
            raise ValueError("Archived part modality is unsupported")
        if len(facets) > MAX_DOCUMENT_FACETS:
            raise ValueError("Archived source exceeds the document facet limit")
    return facets


def batch_operation_id(source_unit_id, source_revision, space_id, batch_index):
    return "facet-batch:" + _digest([
        CHUNKER_VERSION, source_unit_id, source_revision, space_id, batch_index,
    ])[7:]

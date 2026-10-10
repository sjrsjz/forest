"""Read one eligible source and derive deterministic document facets."""

from facet_preparation import MAX_DOCUMENT_FACETS, document_facets

from forest.errors import ForestError


async def collect_document_facets(ctx, source_unit_id, source_revision, space_id):
    facets = []
    after_index = -1
    for _ in range(64):
        page = await ctx.queries.call(
            "memory.read-facet-source",
            {
                "source_unit_id": source_unit_id,
                "after_part_index": after_index,
                "limit": 64,
            },
            timeout=10,
        )
        if page["status"] == "UNAVAILABLE":
            return {"disposition": "SUPPRESSED", "facet_count": 0, "error_code": None}
        if page["source_revision"] != source_revision:
            return {
                "disposition": "FAILED", "facet_count": 0,
                "error_code": "MEMORY_FACET_REVISION_MISMATCH",
            }
        for part in page["parts"]:
            text_parts = {}
            if part["part_kind"] == "TEXT":
                raw = await ctx.payloads.read_bytes(part["payload"])
                try:
                    text_parts[part["part_id"]] = raw.decode("utf-8")
                except UnicodeDecodeError:
                    return {
                        "disposition": "FAILED", "facet_count": 0,
                        "error_code": "MEMORY_TEXT_INVALID",
                    }
            try:
                part_facets = document_facets(
                    source_unit_id, source_revision, space_id, [part], text_parts
                )
            except ValueError:
                return {
                    "disposition": "FAILED", "facet_count": 0,
                    "error_code": "MEMORY_FACET_SOURCE_INVALID",
                }
            if part["part_kind"] == "TEXT":
                content = text_parts[part["part_id"]]
                for facet in part_facets:
                    facet["search_text"] = content[
                        facet["selector_start_char"]:facet["selector_end_char"]
                    ]
            facets.extend(part_facets)
            if len(facets) > MAX_DOCUMENT_FACETS:
                return {
                    "disposition": "FAILED", "facet_count": 0,
                    "error_code": "MEMORY_FACET_LIMIT_EXCEEDED",
                }
        if not page["has_more"]:
            break
        next_index = page["next_part_index"]
        if next_index is None or next_index <= after_index:
            raise ForestError(
                "MEMORY_FACET_PAGE_INVALID",
                "Facet source pagination did not advance",
                502,
            )
        after_index = next_index
    else:
        raise ForestError(
            "MEMORY_FACET_PAGE_LIMIT",
            "Archived source has too many parts for facet preparation",
            409,
        )
    return {"disposition": "READY", "facets": facets, "error_code": None}

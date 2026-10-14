"""Materialize fixed retrieval items; all writes have explicit Payload lineage."""

import asyncio
import json

from forest.errors import ForestError

MAX_PARTS_PER_UNIT = 64
MAX_DEPENDENCIES = 64 * 65
DEPENDENCY_GROUP = 64
MAX_AUTHOR_BYTES = 64 * 1024


async def materialize(ctx, query_id, digest):
    async with asyncio.timeout(120):
        state = await ctx.queries.call("memory.read-retrieval-materialization", {"query_id": query_id}, timeout=30)
        if state["evidence_ref"] is not None:
            await ctx.payloads.read_bytes(state["evidence_ref"])
            return receipt(query_id, state)
        bundle_id = state["bundle_id"]

        async def put(value, kind, parents):
            unique = {ref["payload_id"]: ref for ref in parents}
            ordered = [unique[key] for key in sorted(unique)]
            owner_id = bundle_id + "/" + digest({"value": value, "parents": ordered})[7:]
            return await ctx.payloads.put_json(value, owner_kind=kind, owner_id=owner_id, parents=tuple(ordered))

        query_parts = []
        query_leaves = []
        after = -1
        for _ in range(8):
            page = await ctx.queries.call("memory.read-query-parts", {
                "query_id": query_id, "after_part_index": after, "limit": 32,
            }, timeout=10)
            query_parts.extend(page["parts"])
            query_leaves.append(await put({"parts": page["parts"]}, "retrieval.query-parts", [
                part["payload"] for part in page["parts"]
            ]))
            if not page["has_more"]:
                break
            after = page["next_part_index"]
        query_lineage = await put({"query_id": query_id, "parts": query_parts}, "retrieval.query", query_leaves)
        evidence = []
        unit_refs = []
        selected = []
        missing_count = 0
        remaining_chars = state["max_retrieval_chars"]
        remaining_media_bytes = state["max_retrieval_media_bytes"]
        selected_media_bytes = 0
        content_truncated = False
        for index in range(state["result_count"]):
            after = -1
            node = None
            parts = []
            media_omitted = False
            parents = []
            for _ in range(MAX_PARTS_PER_UNIT // 32):
                page = await ctx.queries.call("memory.read-retrieval-parts", {
                    "query_id": query_id, "item_index": index, "after_part_index": after, "limit": 32,
                }, timeout=10)
                if page["node"] is None:
                    if node is not None:
                        raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Source changed during evidence preparation", 409)
                    break
                node = page["node"]
                for part in page["parts"]:
                    parents.append(part["payload"])
                    common = {"part_id": part["part_id"], "part_index": part["part_index"]}
                    if part["part_kind"] == "TEXT":
                        try:
                            text = (await ctx.payloads.read_bytes(part["payload"])).decode("utf-8")
                        except UnicodeDecodeError as exc:
                            raise ForestError("MEMORY_TEXT_INVALID", "Memory text is not UTF-8", 409) from exc
                        kept = text[:remaining_chars]
                        remaining_chars -= len(kept)
                        truncated = len(kept) != len(text)
                        parts.append({**common, "kind": "text", "text": kept, "truncated": truncated})
                    else:
                        byte_length = part["byte_length"]
                        if type(byte_length) is not int or not 0 < byte_length <= 33554432:
                            raise ForestError(
                                "MEMORY_EVIDENCE_INVALID", "Source image length is invalid", 409,
                            )
                        include_media = byte_length <= remaining_media_bytes
                        if include_media:
                            remaining_media_bytes -= byte_length
                            selected_media_bytes += byte_length
                        else:
                            media_omitted = True
                        parts.append({**common, "kind": "image", "media_ref": part["payload"],
                                      "media_type": part["media_type"], "width": part["width"],
                                      "height": part["height"], "byte_length": byte_length,
                                      "decoded_pixels": part["decoded_pixels"],
                                      "projection_state": "READY" if include_media else "OMITTED_BUDGET"})
                if not page["has_more"]:
                    break
                after = page["last_part_index"]
            if node is None:
                missing_count += 1
                continue
            # Truncation is determined by the final part page, plus text cuts.
            unit_truncated = (
                media_omitted or any(part.get("truncated", False) for part in parts)
                or page["has_more"]
            )
            dependency_refs = []
            dependency_after = None
            for _ in range((MAX_DEPENDENCIES + 31) // 32):
                deps = await ctx.queries.call("memory.read-retrieval-dependencies", {
                    "query_id": query_id, "item_index": index, "after_source_unit_id": dependency_after,
                }, timeout=10)
                if not deps["available"]:
                    raise ForestError("MEMORY_EVIDENCE_UNAVAILABLE", "Evidence path changed during preparation", 409)
                dependency_refs.extend(ref["payload"] for ref in deps["refs"])
                if not deps["has_more"]:
                    break
                dependency_after = deps["next_source_unit_id"]
            for offset in range(0, len(dependency_refs), DEPENDENCY_GROUP):
                group = dependency_refs[offset:offset + DEPENDENCY_GROUP]
                parents.append(await put({"source_anchors": group}, "retrieval.dependencies", group))
            author_bytes = await ctx.payloads.read_bytes(node["author_payload"])
            if len(author_bytes) > MAX_AUTHOR_BYTES:
                raise ForestError("MEMORY_AUTHOR_TOO_LARGE", "Author identity exceeds its 64 KiB bound", 413)
            try:
                author = json.loads(author_bytes.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise ForestError("MEMORY_AUTHOR_INVALID", "Author identity is not UTF-8 JSON", 409) from exc
            if type(author) is not dict:
                raise ForestError("MEMORY_AUTHOR_INVALID", "Author identity must be a JSON object", 409)
            metadata = {key: value for key, value in node.items() if key != "author_payload"}
            unit = {**metadata, "author_identity": author, "parts": parts, "content_truncated": unit_truncated}
            unit_refs.append(await put(unit, "retrieval.unit", parents))
            evidence.append(unit)
            selected.append(index)
            content_truncated |= unit_truncated
        trial_input = state["input_kind"] == "ADMIN_TRIAL"
        document = {
            "protocol": "forest.memory-trial-evidence/2" if trial_input else "forest.memory-evidence/3",
            "bot_id": ctx.identity["bot_id"],
            "query_id": query_id, "bundle_id": bundle_id, "graph_snapshot_ref": "memory-query:" + query_id,
            "current_source": state["current_source"],
            "content_truncated": content_truncated, "missing_count": missing_count,
            "truncated": state["truncated"], "truncation_reason": state["truncation_reason"],
            "association_sampled": state["association_sampled"],
            "read_count": state["read_count"], "max_retrieval_chars": state["max_retrieval_chars"],
            "max_retrieval_media_bytes": state["max_retrieval_media_bytes"],
            "selected_media_bytes": selected_media_bytes,
        }
        if trial_input:
            document["input_kind"] = "ADMIN_TRIAL"
            document["evidence_refs"] = unit_refs
            document["item_indexes"] = selected
        else:
            document["evidence"] = evidence
        evidence_ref = await put(
            document, "retrieval.evidence",
            [query_lineage] if trial_input else [query_lineage, *unit_refs],
        )
        body = {"protocol": "forest.memory-evidence-publish/1", "query_id": query_id,
                "evidence_ref": evidence_ref, "item_indexes": selected, "missing_count": missing_count,
                "content_truncated": content_truncated, "deletion_epoch_step_id": "payload_epoch"}
        published = await ctx.uow.call("memory.publish-retrieval-evidence", {
            "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"}, "memory": body,
        }, operation_id=ctx.operations.stable("publish-evidence:" + digest(body)[7:]), timeout=30)
        return receipt(query_id, published)


def receipt(query_id, state):
    return {"protocol": "forest.memory-evidence-receipt/2", "query_id": query_id,
            "bundle_id": state["bundle_id"], "evidence_ref": state["evidence_ref"],
            "evidence_count": len(state["item_indexes"]), "missing_count": state["missing_count"],
            "content_truncated": state["content_truncated"], "truncated": state["truncated"],
            "truncation_reason": state["truncation_reason"],
            "association_sampled": state["association_sampled"]}

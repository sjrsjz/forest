import hashlib
import json

from embedding_work import dispatch as dispatch_embedding
from embedding_work import recover as recover_embedding
from evidence_materialization import materialize as materialize_evidence
from facet_preparation import (
    FACET_BATCH_SIZE,
    MAX_DOCUMENT_FACETS,
    MAX_QUERY_FACETS,
    MAX_QUERY_IMAGE_BYTES,
    batch_operation_id,
    document_facets,
    query_part_facets,
    query_part_operation_id,
)
from graph_region import expand as expand_graph
from query_matching import prepare as prepare_matching
from recall_work import advance as advance_recall
from trial_result import read_image as read_trial_image
from trial_result import read_page as read_trial_result_page
from vector_work import delete as delete_vector
from vector_work import publish as publish_vectors

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
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class MemoryGraph(Plugin):
    """Own the Bot-global, per-conversation-trunk multimodal Memory domain."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = _plain(config)
        if value.get("protocol") != "forest.memory.graph/2":
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID",
                "Memory Graph configuration protocol is unsupported",
                422,
            )
        embedding = value["embedding"]
        if embedding["model_kind"] != "TEXT_IMAGE":
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID",
                "Memory Graph requires one lifecycle-bound text-image embedding kind",
                422,
            )
        self.profile = value
        self.profile_digest = _digest(value)

    @service(
        "memory.domain.ensure",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/domain-ensure-input.json",
        output_schema="schemas/domain-record-output.json",
    )
    async def ensure_domain(self, payload, ctx):
        request_id = payload["request_id"]
        bound = await ctx.services.call(
            "embedding.profile.bind",
            {"protocol": "forest.embedding-bind-request/1", "request_id": request_id},
            operation_id=ctx.operations.stable("embedding-profile:" + request_id),
            timeout=10,
        )
        expected = {
            key: self.profile["embedding"][key]
            for key in ("deployment_id", "model_revision", "model", "dimensions")
        }
        if (
            bound.get("protocol") != "forest.embedding-profile/1"
            or bound.get("request_id") != request_id
            or bound.get("component_id") != "embedding.multimodal"
            or _plain(bound.get("profile")) != expected
        ):
            raise ForestError(
                "MEMORY_EMBEDDING_PROFILE_MISMATCH",
                "Memory model identity differs from the bound Embedding provider",
                409,
            )
        return await ctx.uow.call(
            "memory.bind-domain",
            {
                "protocol": "forest.memory-domain-bind/1",
                "space_id": bound["space_id"],
                "embedding_model_kind": self.profile["embedding"]["model_kind"],
                "model_revision_digest": _digest(expected),
                "dimensions": expected["dimensions"],
                "normalization": self.profile["embedding"]["normalization"],
                "fusion_policy_revision": self.profile["fusion_policy_revision"],
            },
            operation_id=ctx.operations.stable("memory-domain:" + bound["space_id"]),
            timeout=10,
        )

    @job("memory.domain.bind")
    async def bind_domain_work(self, task, ctx):
        await self.ensure_domain(task["payload"], ctx)
        return {"bound": True}

    @service(
        "memory.query.begin",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/query-begin-input.json",
        output_schema="schemas/query-begin-output.json",
    )
    async def begin_query(self, payload, ctx):
        return await ctx.uow.call(
            "memory.begin-query",
            {
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    **_plain(payload),
                    "protocol": "forest.memory-query-begin/1",
                    "deletion_epoch_step_id": "payload_epoch",
                },
            },
            operation_id="query-begin:" + _digest(payload["query_id"])[7:],
            timeout=20,
        )

    @service(
        "memory.query.progress",
        semantics="read",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/query-progress-output.json",
    )
    async def query_progress(self, payload, ctx):
        await ctx.queries.call(
            "memory.assert-public-query", {"query_id": payload["query_id"]}, timeout=10
        )
        progress = await ctx.queries.call(
            "memory.read-query-progress", {"query_id": payload["query_id"]}, timeout=30
        )
        if progress["phase"] == "READY":
            # Database publication is not a permanent Payload read permit.
            await ctx.payloads.read_bytes(progress["evidence_ref"])
        return progress

    @service(
        "memory.trial.result",
        semantics="read",
        contract="1.0",
        input_schema="schemas/trial-result-input.json",
        output_schema="schemas/trial-result-output.json",
    )
    async def trial_result(self, payload, ctx):
        return await read_trial_result_page(ctx, payload)

    @service(
        "memory.trial.image",
        semantics="read",
        contract="1.0",
        input_schema="schemas/trial-image-input.json",
        output_schema="schemas/trial-image-output.json",
    )
    async def trial_image(self, payload, ctx):
        return await read_trial_image(ctx, payload)

    @service(
        "memory.query.request",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/recall-request-input.json",
        output_schema="schemas/recall-request-output.json",
    )
    async def request_recall(self, payload, ctx):
        body = {"protocol": "forest.memory-recall-request/1", **_plain(payload)}
        return await ctx.uow.call(
            "memory.request-recall", {"memory": body},
            operation_id="recall-request:" + _digest(body)[7:], timeout=20,
        )

    @service(
        "memory.query.cancel",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/recall-cancel-input.json",
        output_schema="schemas/recall-cancel-output.json",
    )
    async def cancel_recall(self, payload, ctx):
        body = {"protocol": "forest.memory-recall-cancel/1", **_plain(payload)}
        return await ctx.uow.call(
            "memory.cancel-recall", {"memory": body},
            operation_id="recall-cancel:" + _digest(body)[7:], timeout=20,
        )

    @service(
        "memory.query.work-status",
        semantics="read",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/recall-work-output.json",
    )
    async def recall_status(self, payload, ctx):
        await ctx.queries.call(
            "memory.assert-public-query", {"query_id": payload["query_id"]}, timeout=10
        )
        state = await ctx.queries.call(
            "memory.read-recall-work", {"query_id": payload["query_id"]}, timeout=30
        )
        if state["state"] == "READY":
            await ctx.payloads.read_bytes(state["evidence_ref"])
        return state

    @job("memory.query.advance")
    async def advance_recall(self, task, ctx):
        return await advance_recall(self, task, ctx)

    @service(
        "memory.query.prepare",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/query-prepare-output.json",
    )
    async def prepare_query(self, payload, ctx):
        query_id = payload["query_id"]
        prepared_parts = []
        facet_count = 0
        image_bytes = 0
        after_index = -1
        for _ in range(8):
            page = await ctx.queries.call(
                "memory.read-query-parts",
                {"query_id": query_id, "after_part_index": after_index, "limit": 32},
                timeout=10,
            )
            space_id = page["space_id"]
            for part in page["parts"]:
                text_parts = {}
                if part["part_kind"] == "TEXT":
                    raw = await ctx.payloads.read_bytes(part["payload"])
                    try:
                        text_parts[part["part_id"]] = raw.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise ForestError("MEMORY_TEXT_INVALID", "Query text is not UTF-8", 422) from exc
                try:
                    facets = query_part_facets(query_id, space_id, part, text_parts)
                except ValueError as exc:
                    raise ForestError("MEMORY_QUERY_FACET_LIMIT", str(exc), 422) from exc
                facet_count += len(facets)
                if part["part_kind"] == "IMAGE":
                    image_bytes += part["byte_length"]
                if facet_count > MAX_QUERY_FACETS or image_bytes > MAX_QUERY_IMAGE_BYTES:
                    raise ForestError(
                        "MEMORY_QUERY_FACET_LIMIT", "Query exceeds its facet or image budget", 422
                    )
                prepared_parts.append({
                    "protocol": "forest.memory-query-part-facets/1",
                    "query_id": query_id, "input_identity": part["input_identity"],
                    **({
                        "source_unit_id": part["source_unit_id"],
                        "source_revision": part["source_revision"],
                    } if part["input_identity"]["kind"] == "SOURCE_UNIT" else {}),
                    "space_id": space_id,
                    "batch_index": part["query_part_index"], "part_id": part["part_id"],
                    "facets": facets,
                })
            if not page["has_more"]:
                break
            after_index = page["next_part_index"]
        else:
            raise ForestError("MEMORY_QUERY_PART_LIMIT", "Query exceeds 256 source parts", 422)
        for part in prepared_parts:
            await ctx.uow.call(
                "memory.prepare-query-part-facets", part,
                operation_id=query_part_operation_id(query_id, space_id, part["batch_index"]),
                timeout=20,
            )
        return await ctx.uow.call(
            "memory.complete-query-facets",
            {
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "protocol": "forest.memory-query-complete/1", "query_id": query_id,
                    "facet_count": facet_count, "deletion_epoch_step_id": "payload_epoch",
                },
            },
            operation_id="query-facets-complete:" + _digest(query_id)[7:], timeout=20,
        )

    @service(
        "memory.query.seeds",
        semantics="read",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/query-seeds-output.json",
    )
    async def query_seeds(self, payload, ctx):
        prepared = await ctx.queries.call(
            "memory.read-matched-seeds", {"query_id": payload["query_id"]}, timeout=10
        )
        return prepared["result"]

    @service(
        "memory.query.match",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/query-matched-output.json",
    )
    async def match_query(self, payload, ctx):
        return await prepare_matching(ctx, payload["query_id"], self.profile["retrieval"])

    @service(
        "memory.query.freeze",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/retrieval-record-output.json",
    )
    async def freeze_query(self, payload, ctx):
        query_id = payload["query_id"]
        recorded = await ctx.queries.call("memory.read-retrieval-record", {"query_id": query_id}, timeout=10)
        if recorded is not None:
            return recorded
        region = await self.query_region(payload, ctx)
        fields = (
            "source_unit_id", "source_revision", "rank_score", "activation", "is_seed",
            "path_parent_source_unit_id", "path_edge_id",
        )
        body = {
            "protocol": "forest.memory-retrieval-freeze/1", "query_id": query_id,
            "nodes": [{key: node[key] for key in fields} for node in region["nodes"]],
            "read_count": region["read_count"], "truncated": region["truncated"],
            "association_sampled": region["association_sampled"],
            "deletion_epoch_step_id": "payload_epoch",
        }
        return await ctx.uow.call(
            "memory.freeze-retrieval",
            {"payload_epoch": {"protocol": "forest.payload-epoch-capture/1"}, "memory": body},
            operation_id=ctx.operations.stable("retrieval-freeze:" + _digest(body)[7:]), timeout=30,
        )

    @service(
        "memory.query.materialize",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/evidence-receipt-output.json",
    )
    async def materialize_query(self, payload, ctx):
        try:
            return await materialize_evidence(ctx, payload["query_id"], _digest)
        except TimeoutError as exc:
            raise ForestError("MEMORY_EVIDENCE_DEADLINE", "Evidence preparation exceeded its deadline", 504) from exc

    @service(
        "memory.query.region",
        semantics="read",
        contract="1.0",
        input_schema="schemas/query-prepare-input.json",
        output_schema="schemas/query-region-output.json",
    )
    async def query_region(self, payload, ctx):
        query_id = payload["query_id"]
        prepared = await ctx.queries.call("memory.read-matched-seeds", {"query_id": query_id}, timeout=10)
        policy = prepared["policy"]
        seeds = prepared["result"]
        try:
            region = await expand_graph(ctx, query_id, seeds["seeds"], policy)
        except TimeoutError as exc:
            raise ForestError(
                "MEMORY_GRAPH_DEADLINE", "Graph expansion exceeded its deadline", 504
            ) from exc
        return {
            "protocol": "forest.memory-query-region/1", "query_id": query_id,
            "space_id": seeds["space_id"], "policy_revision": policy["policy_revision"],
            **region,
        }

    @job("memory.facets.prepare")
    async def prepare_document_facets(self, task, ctx):
        request = task["payload"]
        source_unit_id = request["source_unit_id"]
        source_revision = request["source_revision"]
        space_id = request["space_id"]
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
                    facets.extend(document_facets(
                        source_unit_id, source_revision, space_id, [part], text_parts
                    ))
                except ValueError:
                    return {
                        "disposition": "FAILED", "facet_count": 0,
                        "error_code": "MEMORY_FACET_SOURCE_INVALID",
                    }
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
        for batch_index, offset in enumerate(range(0, len(facets), FACET_BATCH_SIZE)):
            batch = facets[offset:offset + FACET_BATCH_SIZE]
            await ctx.uow.call(
                "memory.prepare-document-facets",
                {
                    "protocol": "forest.memory-document-facets/1",
                    "source_unit_id": source_unit_id,
                    "source_revision": source_revision,
                    "space_id": space_id,
                    "batch_index": batch_index,
                    "facets": batch,
                },
                operation_id=batch_operation_id(
                    source_unit_id, source_revision, space_id, batch_index
                ),
                timeout=20,
            )
        return {"disposition": "READY", "facet_count": len(facets), "error_code": None}

    @job("memory.embedding.dispatch")
    async def dispatch_embedding_batch(self, task, ctx):
        return await dispatch_embedding(ctx, task, self.profile)

    @job("memory.embedding.recover")
    async def recover_embedding_batch(self, task, ctx):
        return await recover_embedding(ctx, task)

    @job("memory.vector.publish")
    async def publish_vector_batch(self, task, ctx):
        return await publish_vectors(ctx, task, self.profile)

    @job("memory.vector.delete")
    async def delete_vector_ref(self, task, ctx):
        return await delete_vector(ctx, task)

    @service(
        "memory.profile.describe",
        semantics="read",
        contract="2.0",
        input_schema="schemas/profile-input.json",
        output_schema="schemas/profile-output.json",
    )
    async def describe_profile(self, payload, ctx):
        return {
            "protocol": "forest.memory-profile/2",
            "component_id": "memory.graph",
            "generation_id": ctx.identity["generation_id"],
            "profile_digest": self.profile_digest,
            "embedding": self.profile["embedding"],
            "fusion_policy_revision": self.profile["fusion_policy_revision"],
            "retrieval": self.profile["retrieval"],
        }

    @service(
        "shell.memory.inspect",
        semantics="read",
        contract="1.0",
        input_schema="schemas/shell-input.json",
        output_schema="schemas/shell-output.json",
    )
    async def inspect_shell(self, payload, ctx):
        command = payload["command_id"]
        arguments = _plain(payload["arguments"])
        if command == "memory.trunks":
            result = await ctx.queries.call("memory.list-trunks", {})
            lines = [f"Memory 主干（{len(result['items'])}）"]
            lines.extend(
                f"{row['trunk_id']} | 群 {row['external_group_id']} | "
                f"来源 {row['registered_sources']} / 可召回 {row['eligible_sources']}"
                for row in result["items"][:10]
            )
            if len(result["items"]) > 10:
                lines.append("还有更多主干，请在 WebUI 查询。")
        elif command == "memory.sources":
            result = await ctx.queries.call("memory.query-sources", arguments)
            lines = [f"Memory 来源（本页 {len(result['items'])}）"]
            lines.extend(
                f"{row['source_unit_id']} | 群 {row['external_group_id']} | "
                f"{row['author_subject_ref']} | {row['modality']} | {row['unit_kind']}"
                for row in result["items"]
            )
            if result["has_more"]:
                lines.append("还有后续结果，请在 WebUI 使用稳定游标查询。")
        elif command == "memory.relations":
            result = await ctx.queries.call("memory.query-relations", arguments)
            lines = [f"Memory 关系（本页 {len(result['items'])}）"]
            for row in result["items"]:
                groups = sorted({
                    item["external_group_id"] for item in row["evidence"]
                    if item["external_group_id"] is not None
                })
                origin = ",".join(groups) if groups else "无可见群来源"
                lines.append(
                    f"{row['relation_id']} {row['relation_kind']} | "
                    f"{row['left_kind']}:{row['left_id']} -> "
                    f"{row['right_kind']}:{row['right_id']} | 来源群 {origin}"
                )
            if result["has_more"]:
                lines.append("还有后续结果，请在 WebUI 使用稳定游标查询。")
        else:
            raise ForestError("SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409)
        rendered = "\n".join(lines)
        return {"text": rendered[:3500]}

    @service(
        "memory.source.capture",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/capture-service-input.json",
        output_schema="schemas/capture-output.json",
    )
    async def capture_source(self, payload, ctx):
        value = _plain(payload)
        capture = {
            key: value[key]
            for key in (
                "identity",
                "source_unit_id",
                "source_revision",
                "unit_kind",
                "author_subject_ref",
                "author",
                "occurrence",
                "occurred_at",
            )
        }
        capture["parts"] = [
            {
                key: part[key]
                for key in (
                    "part_id",
                    "part_index",
                    "part_kind",
                    "media_type",
                    "payload",
                    "width",
                    "height",
                    "byte_length",
                )
            }
            for part in value["parts"]
        ]
        return await ctx.uow.call(
            "memory.capture-source",
            {
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "capture": capture,
                    "conversation_step_id": None,
                    "deletion_epoch_step_id": "payload_epoch",
                },
            },
            operation_id=value["operation_id"],
            timeout=20,
        )

    @service(
        "memory.source.unavailable",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/unavailable-input.json",
        output_schema="schemas/unavailable-output.json",
    )
    async def mark_source_unavailable(self, payload, ctx):
        value = _plain(payload)
        return await ctx.uow.call(
            "memory.mark-source-unavailable",
            value,
            operation_id=ctx.operations.stable(
                f"memory-unavailable:{value['conversation_ref']}:{value['event_ref']}"
            ),
            timeout=15,
        )

    @service(
        "memory.source.forget.preview",
        semantics="read",
        contract="1.0",
        input_schema="schemas/forget-preview-service-input.json",
        output_schema="schemas/forget-preview-output.json",
    )
    async def preview_source_forget(self, payload, ctx):
        source_unit_id = payload["source_unit_id"]
        operation_id = ctx.operations.stable("memory-forget-preview")
        return await ctx.uow.call(
            "memory.preview-source-forget",
            {
                "prepare": {
                    "operation_id": operation_id,
                    "source_unit_id": source_unit_id,
                },
                "payload": {
                    "protocol": "forest.payload-forget-anchor-preview/1",
                    "prior_step_id": "prepare",
                },
                "memory": {
                    "source_unit_id": source_unit_id,
                    "prepare_step_id": "prepare",
                    "preview_step_id": "payload",
                },
            },
            operation_id=operation_id,
            timeout=30,
        )

    @service(
        "memory.source.forget",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/forget-service-input.json",
        output_schema="schemas/forget-output.json",
    )
    async def forget_source(self, payload, ctx):
        request = _plain(payload)
        operation_id = request["operation_id"]
        source_unit_id = request["source_unit_id"]
        plan = request.get("plan")
        payload_step = {
            "protocol": "forest.payload-forget-anchor/1",
            "prior_step_id": "prepare",
            "operation_id": operation_id,
        }
        memory_step = {
            "operation_id": operation_id,
            "source_unit_id": source_unit_id,
            "prepare_step_id": "prepare",
            "forget_step_id": "payload",
        }
        if plan is not None:
            payload_step["expected_deletion_epoch"] = plan["deletion_epoch"]
            payload_step["expected_selection_digest"] = plan["selection_digest"]
            memory_step["plan"] = plan
        return await ctx.uow.call(
            "memory.forget-source",
            {
                "prepare": {"operation_id": operation_id, "source_unit_id": source_unit_id},
                "payload": payload_step,
                "memory": memory_step,
            },
            operation_id=operation_id,
            timeout=30,
        )

    @job("memory.forget.manage-work")
    async def execute_management_forget(self, task, ctx):
        request = task["payload"]
        return await self.forget_source(
            {
                "operation_id": request["request_id"],
                "source_unit_id": request["source_unit_id"],
                "plan": request["plan"],
            },
            ctx,
        )

    @service(
        "agent.tool.memory-exact-recall",
        semantics="idempotent",
        contract="1.0",
        input_schema="schemas/tool-execute-input.json",
        output_schema="schemas/tool-execute-output.json",
    )
    async def exact_recall(self, payload, ctx):
        if (
            payload["generation_id"] != ctx.identity["generation_id"]
            or payload["component_id"] != "memory.graph"
            or payload["service_id"] != "agent.tool.memory-exact-recall"
        ):
            raise ForestError(
                "MEMORY_TOOL_FENCE_MISMATCH",
                "Exact recall command belongs to another Memory Generation",
                409,
            )
        exact = await ctx.queries.call(
            "memory.read-exact-sources",
            {"source_unit_ids": payload["arguments"]["source_unit_ids"]},
            timeout=15,
        )
        parents = []
        evidence = []
        for unit in exact["evidence"]:
            parts = []
            for part in unit["parts"]:
                reference = part["payload"]
                parents.append(reference)
                if part["part_kind"] == "TEXT":
                    try:
                        text = (await ctx.payloads.read_bytes(reference)).decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise ForestError(
                            "MEMORY_TEXT_INVALID",
                            "Archived text memory is not valid UTF-8",
                            409,
                        ) from exc
                    parts.append(
                        {
                            "part_id": part["part_id"],
                            "type": "text",
                            "text": text,
                        }
                    )
                else:
                    parts.append(
                        {
                            "part_id": part["part_id"],
                            "type": "image",
                            "media_ref": reference,
                            "media_type": part["media_type"],
                            "width": part["width"],
                            "height": part["height"],
                        }
                    )
            source = {
                "source_id": unit["source_id"],
                "source_kind": unit["source_kind"],
                "platform": unit["platform"],
                "realm_id": unit["realm_id"],
                "group_id": unit["external_group_id"],
                "trunk_id": unit["trunk_id"],
            }
            evidence.append(
                {
                    "source_unit_id": unit["source_unit_id"],
                    "source_revision": unit["source_revision"],
                    "source": source,
                    "author": {
                        "principal_ref": unit["author_subject_ref"],
                        "identity_origin": "memory_verified",
                    },
                    "occurred_at": unit["occurred_at"],
                    "parts": parts,
                }
            )
        result = await ctx.payloads.put_json(
            {
                "protocol": "forest.memory-exact-recall-result/2",
                "retrieval_method": "exact_source_unit_id",
                "evidence": evidence,
                "missing_source_unit_ids": exact["missing_source_unit_ids"],
            },
            owner_kind="tool.result",
            owner_id=payload["tool_call_id"],
            parents=tuple(parents),
        )
        return {
            "protocol": "forest.agent-tool-receipt/1",
            "status": "SUCCEEDED",
            "tool_call_id": payload["tool_call_id"],
            "effect_operation_id": payload["effect_operation_id"],
            "effect_attempt_id": payload["effect_attempt_id"],
            "result": result,
            "error_code": None,
            "observation_ref": "memory-exact-recall:" + payload["tool_call_id"],
        }

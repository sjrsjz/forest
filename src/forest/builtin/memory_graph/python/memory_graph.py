import hashlib
import json

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

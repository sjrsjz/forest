"""Read and project one immutable Conversation Context snapshot inside Agent Worker."""

from __future__ import annotations

import asyncio
import json

from forest.errors import ForestError

_REF_FIELDS = {
    "payload_id",
    "digest",
    "owner_component_id",
    "owner_kind",
    "owner_id",
}
_MANIFEST_FIELDS = {
    "protocol",
    "conversation_id",
    "context_applied_seq",
    "classified_through_seq",
    "context_revision",
    "context_view_revision",
    "source_component_id",
    "binding_ref",
    "platform",
    "channel_kind",
    "channel_ref",
    "thread_ref",
    "context_view_unit_count",
    "context_view_media_bytes",
    "media_cost_revision",
    "context_archived_unit_count",
    "archive",
    "total_units",
    "unit_count",
    "tree_depth",
    "roots",
}
_PAGE_FIELDS = {
    "protocol",
    "conversation_id",
    "context_revision",
    "context_view_revision",
    "offset",
    "units",
}


def claim_summaries(inputs: list[dict]) -> list[dict]:
    """Project each durable input cut without copying its trigger members."""
    return [
        {
            "input_batch_ref": item["input_batch_ref"],
            "trigger_claim_ref": item["trigger_claim_ref"],
            "trigger_claim_digest": item["trigger_claim_digest"],
            "trigger_count": item["trigger_count"],
            "last_trigger_ref": item["last_trigger_ref"],
            "last_trigger_ingress_seq": item["last_trigger_ingress_seq"],
            "source_manifest_digest": item["source_manifest_digest"],
        }
        for item in inputs
    ]


def _exact_ref(value, *, component: str | None = None, kinds=()) -> dict:
    if not isinstance(value, dict) or set(value) != _REF_FIELDS:
        raise ForestError(
            "AGENT_CONTEXT_REFERENCE_INVALID",
            "Context snapshot contains an incomplete Payload reference",
            409,
        )
    result = dict(value)
    if any(not isinstance(item, str) or not item for item in result.values()):
        raise ForestError(
            "AGENT_CONTEXT_REFERENCE_INVALID",
            "Context snapshot Payload identity is invalid",
            409,
        )
    digest = result["digest"]
    if (
        len(digest) != 71
        or not digest.startswith("sha256:")
        or any(char not in "0123456789abcdef" for char in digest[7:])
        or (component is not None and result["owner_component_id"] != component)
        or (kinds and result["owner_kind"] not in kinds)
    ):
        raise ForestError(
            "AGENT_CONTEXT_REFERENCE_INVALID",
            "Context snapshot Payload fence is invalid",
            409,
        )
    return result


def _integer(value, *, minimum: int = 0, maximum: int = 1_000_001) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _json_object(value: bytes, kind: str) -> dict:
    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForestError(
            "AGENT_CONTEXT_PAYLOAD_INVALID",
            f"{kind} Payload is not valid UTF-8 JSON",
            409,
        ) from exc
    if not isinstance(decoded, dict):
        raise ForestError(
            "AGENT_CONTEXT_PAYLOAD_INVALID",
            f"{kind} Payload must contain a JSON object",
            409,
        )
    return decoded


def _unit_ref(unit: dict, prefix: str) -> dict | None:
    if prefix == "payload":
        values = {
            "payload_id": unit.get("payload_id"),
            "digest": unit.get("payload_digest"),
            "owner_component_id": unit.get("payload_owner_component_id"),
            "owner_kind": unit.get("payload_owner_kind"),
            "owner_id": unit.get("payload_owner_id"),
        }
    else:
        if unit.get("author_payload_id") is None:
            return None
        values = {
            "payload_id": unit.get("author_payload_id"),
            "digest": unit.get("author_payload_digest"),
            "owner_component_id": unit.get("author_owner_component_id"),
            "owner_kind": unit.get("author_owner_kind"),
            "owner_id": unit.get("author_owner_id"),
        }
    return _exact_ref(values)


def _part_id(unit_id: str, index: int) -> str:
    return f"{unit_id}:{index}"


def _parts(unit_id: str, value: dict) -> list[dict]:
    raw = value.get("parts")
    if isinstance(raw, list):
        if len(raw) > 256:
            raise ForestError(
                "AGENT_CONTEXT_PART_LIMIT", "Context unit exceeds 256 parts", 413,
            )
        result = []
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                result.append(
                    {
                        "part_id": _part_id(unit_id, index),
                        "type": "unavailable",
                        "reason": "INVALID_PART",
                    }
                )
                continue
            kind = item.get("type")
            part_id = item.get("part_id", _part_id(unit_id, index))
            if not isinstance(part_id, str) or not part_id:
                part_id = _part_id(unit_id, index)
            if kind == "text" and isinstance(item.get("text"), str):
                result.append({"part_id": part_id, "type": "text", "text": item["text"]})
            elif kind == "image":
                media_ref = item.get("media_ref")
                if not isinstance(media_ref, str) or not media_ref:
                    media_ref = f"pending-media:{unit_id}:{index}"
                projected = {"part_id": part_id, "type": "image", "media_ref": media_ref}
                if type(item.get("media_revision")) is int:
                    projected["media_revision"] = item["media_revision"]
                result.append(projected)
            elif kind == "mention" and isinstance(item.get("external_user_id"), str):
                result.append(
                    {
                        "part_id": part_id,
                        "type": "mention",
                        "external_user_id": item["external_user_id"],
                    }
                )
            elif kind == "reply" and isinstance(item.get("platform_message_id"), str):
                result.append(
                    {
                        "part_id": part_id,
                        "type": "reply",
                        "platform_message_id": item["platform_message_id"],
                    }
                )
            else:
                result.append(
                    {
                        "part_id": part_id,
                        "type": "unavailable",
                        "reason": "UNSUPPORTED_PART",
                    }
                )
        return result
    if isinstance(value.get("text"), str):
        return [{"part_id": _part_id(unit_id, 0), "type": "text", "text": value["text"]}]
    return [{"part_id": _part_id(unit_id, 0), "type": "json", "value": value}]


class ContextSnapshotReader:
    """Verify the snapshot tree and materialize author-labelled model Context."""

    def __init__(self, ctx, *, bot_id: str) -> None:
        self.ctx = ctx
        self.bot_id = bot_id
        self._seen: set[str] = set()
        self._node_count = 0

    async def _read(self, reference: dict, kind: str) -> dict:
        exact = _exact_ref(reference, component="conversation.scheduler")
        payload_id = exact["payload_id"]
        if payload_id in self._seen:
            raise ForestError(
                "AGENT_CONTEXT_TREE_INVALID",
                "Context snapshot tree repeats a Payload node",
                409,
            )
        self._seen.add(payload_id)
        self._node_count += 1
        if self._node_count > 40_000:
            raise ForestError(
                "AGENT_CONTEXT_TREE_LIMIT",
                "Context snapshot tree exceeds its bounded node count",
                413,
            )
        return _json_object(await self.ctx.payloads.read_bytes(exact), kind)

    async def _pages(self, nodes: list, depth: int) -> list[dict]:
        if not isinstance(nodes, list) or len(nodes) > 128:
            raise ForestError(
                "AGENT_CONTEXT_TREE_INVALID",
                "Context snapshot tree fanout is invalid",
                409,
            )
        if depth == 0:
            pages = []
            for reference in nodes:
                exact = _exact_ref(
                    reference,
                    component="conversation.scheduler",
                    kinds=("context.snapshot.page",),
                )
                pages.append(await self._read(exact, "Context page"))
            return pages
        pages = []
        for reference in nodes:
            exact = _exact_ref(
                reference,
                component="conversation.scheduler",
                kinds=("context.snapshot.index",),
            )
            index = await self._read(exact, "Context index")
            if (
                set(index) != {"protocol", "level", "children"}
                or index["protocol"] != "forest.conversation-context-index/1"
                or index["level"] != depth - 1
            ):
                raise ForestError(
                    "AGENT_CONTEXT_TREE_INVALID",
                    "Context snapshot index differs from its declared depth",
                    409,
                )
            pages.extend(await self._pages(index["children"], depth - 1))
        return pages

    async def load(self, envelope: dict, *, expected: dict) -> dict:
        if (
            not isinstance(envelope, dict)
            or envelope.get("protocol") != "forest.conversation-context-snapshot/3"
            or set(expected)
            != {
                "conversation_id",
                "context_applied_seq",
                "context_revision",
                "context_view_revision",
            }
        ):
            raise ForestError(
                "AGENT_CONTEXT_FENCE_MISMATCH",
                "Conversation snapshot envelope is invalid",
                409,
            )
        reference = _exact_ref(
            envelope.get("snapshot"),
            component="conversation.scheduler",
            kinds=("context.snapshot",),
        )
        manifest = await self._read(reference, "Context snapshot")
        if set(manifest) != _MANIFEST_FIELDS or manifest.get("protocol") != envelope["protocol"]:
            raise ForestError(
                "AGENT_CONTEXT_MANIFEST_INVALID",
                "Conversation snapshot manifest shape is invalid",
                409,
            )
        for name, value in expected.items():
            if manifest.get(name) != value or envelope.get(name) != value:
                raise ForestError(
                    "AGENT_CONTEXT_FENCE_MISMATCH",
                    "Conversation snapshot differs from the durable Agent input fence",
                    409,
                )
        for name in ("media_cost_revision", "context_view_media_bytes"):
            if manifest.get(name) != envelope.get(name) or not _integer(
                manifest.get(name), maximum=1_000_000_000_000
            ):
                raise ForestError(
                    "AGENT_CONTEXT_FENCE_MISMATCH",
                    "Conversation media view differs from its immutable snapshot", 409,
                )
        depth = manifest["tree_depth"]
        if not _integer(depth, maximum=8):
            raise ForestError(
                "AGENT_CONTEXT_TREE_INVALID", "Context snapshot depth is invalid", 409
            )
        pages = await self._pages(manifest["roots"], depth)
        units = []
        active_offset = 0
        media_bytes = 0
        archive_seen = False
        for page_index, page in enumerate(pages):
            if (
                not isinstance(page, dict)
                or set(page) != _PAGE_FIELDS
                or page.get("protocol") != "forest.conversation-context-page/1"
                or page.get("conversation_id") != expected["conversation_id"]
                or page.get("context_revision") != expected["context_revision"]
                or page.get("context_view_revision") != expected["context_view_revision"]
                or page.get("offset") != active_offset
                or not isinstance(page.get("units"), list)
                or not 1 <= len(page["units"]) <= 33
            ):
                raise ForestError(
                    "AGENT_CONTEXT_PAGE_INVALID",
                    "Conversation snapshot page violates its immutable fence",
                    409,
                )
            for unit_index, unit in enumerate(page["units"]):
                if not isinstance(unit, dict):
                    raise ForestError(
                        "AGENT_CONTEXT_PAGE_INVALID", "Context unit is invalid", 409
                    )
                is_archive = unit.get("source_kind") == "context.archive"
                if is_archive:
                    if archive_seen or page_index != 0 or unit_index != 0:
                        raise ForestError(
                            "AGENT_CONTEXT_PAGE_INVALID",
                            "Context archive must be the first unique snapshot unit",
                            409,
                        )
                    archive_seen = True
                else:
                    active_offset += 1
                _unit_ref(unit, "payload")
                _unit_ref(unit, "author")
                media_parts = unit.get("media_parts")
                if not isinstance(media_parts, list) or len(media_parts) > 256:
                    raise ForestError(
                        "AGENT_CONTEXT_PAGE_INVALID", "Context media facts are invalid", 409,
                    )
                media_bytes += sum(
                    part.get("byte_length", 0) for part in media_parts
                    if isinstance(part, dict) and type(part.get("byte_length")) is int
                )
                units.append(unit)
        if (
            active_offset != manifest["total_units"]
            or len(units) != manifest["unit_count"]
            or envelope.get("unit_count") != len(units)
            or manifest["context_view_unit_count"] != active_offset
            or manifest["context_view_media_bytes"] != media_bytes
        ):
            raise ForestError(
                "AGENT_CONTEXT_COUNT_CONFLICT",
                "Conversation snapshot counts differ from its page tree",
                409,
            )
        return {**manifest, "units": units, "snapshot": reference}

    async def project(self, snapshot: dict) -> dict:
        units = snapshot["units"]

        async def load_unit(unit):
            content_ref = _unit_ref(unit, "payload")
            author_ref = _unit_ref(unit, "author")
            content_task = asyncio.create_task(self.ctx.payloads.read_bytes(content_ref))
            author_task = (
                asyncio.create_task(self.ctx.payloads.read_bytes(author_ref))
                if author_ref is not None
                else None
            )
            content = _json_object(await content_task, "Context unit")
            author = (
                _json_object(await author_task, "Context author")
                if author_task is not None
                else {}
            )
            return content, author

        loaded = []
        for start in range(0, len(units), 8):
            loaded.extend(
                await asyncio.gather(*(load_unit(unit) for unit in units[start : start + 8]))
            )
        projected = []
        for unit, (content, author) in zip(units, loaded, strict=True):
            unit_id = unit.get("unit_id")
            kind = unit.get("kind")
            if not isinstance(unit_id, str) or kind not in {
                "INPUT",
                "REPLY",
                "AGENT_STEP",
                "CONTROL",
                "EFFECT",
            }:
                raise ForestError(
                    "AGENT_CONTEXT_PAGE_INVALID", "Context unit identity is invalid", 409
                )
            author["principal_ref"] = unit.get("author_subject_ref") or f"bot:{self.bot_id}"
            author.setdefault(
                "identity_origin",
                "host_model_output" if kind in {"REPLY", "AGENT_STEP"} else "adapter_verified",
            )
            source = {
                "conversation_id": snapshot["conversation_id"],
                "binding_ref": snapshot["binding_ref"],
                "platform": snapshot["platform"],
                "channel_kind": snapshot["channel_kind"].lower(),
                "channel_id": snapshot["channel_ref"],
                "source_event_id": unit.get("event_id") or unit.get("source_id"),
                "source_kind": unit.get("source_kind"),
            }
            if snapshot["channel_kind"] == "GROUP":
                source["group_source_id"] = snapshot["conversation_id"]
            if snapshot["thread_ref"]:
                source["thread_id"] = snapshot["thread_ref"]
            parts = _parts(unit_id, content)
            seen_indexes = set()
            for media in unit["media_parts"]:
                index = media.get("part_index") if isinstance(media, dict) else None
                if (
                    type(index) is not int or not 0 <= index < len(parts)
                    or index in seen_indexes or parts[index].get("type") != "image"
                    or media.get("media_ref") != parts[index].get("media_ref")
                    or media.get("media_revision") != parts[index].get("media_revision")
                    or type(media.get("byte_length")) is not int
                    or not 0 < media["byte_length"] <= 8_388_608
                ):
                    raise ForestError(
                        "AGENT_CONTEXT_MEDIA_SOURCE_MISMATCH",
                        "Context media fact does not match its source part", 409,
                    )
                reference = _exact_ref(
                    media.get("payload"), component="media.capture",
                    kinds=("media.asset",),
                )
                if reference["owner_id"] != media["media_ref"]:
                    raise ForestError(
                        "AGENT_CONTEXT_MEDIA_SOURCE_MISMATCH",
                        "Context media Payload belongs to another asset", 409,
                    )
                seen_indexes.add(index)
                parts[index]["materialization"] = {
                    "state": "READY", "payload": reference,
                    "byte_length": media["byte_length"],
                    "width": media["width"], "height": media["height"],
                }
            for part in parts:
                if part["type"] == "image" and "materialization" not in part:
                    part["materialization"] = {"state": "PENDING"}
            projected.append(
                {
                    "unit_id": unit_id,
                    "role": {
                        "INPUT": "user",
                        "REPLY": "assistant",
                        "AGENT_STEP": "assistant",
                        "CONTROL": "control_data",
                        "EFFECT": "control_data",
                    }[kind],
                    "source": source,
                    "author": author,
                    "parts": parts,
                    "input_batch_id": unit.get("input_batch_id"),
                    "ingress_seq": unit.get("ingress_seq"),
                }
            )
        return {
            "bot_id": self.bot_id,
            "conversation_id": snapshot["conversation_id"],
            "context_applied_seq": snapshot["context_applied_seq"],
            "context_revision": snapshot["context_revision"],
            "context_view_revision": snapshot["context_view_revision"],
            "media_cost_revision": snapshot["media_cost_revision"],
            "context_view_media_bytes": snapshot["context_view_media_bytes"],
            "source_manifest": snapshot["snapshot"],
            "units": projected,
        }

    @staticmethod
    def _history_ref(row: dict, role: str) -> dict:
        if role == "model":
            values = {
                "payload_id": row.get("model_result_payload_id"),
                "digest": row.get("model_result_payload_digest"),
                "owner_component_id": row.get("provider_component_id"),
                "owner_kind": "model.result",
                "owner_id": row.get("model_attempt_id"),
            }
        elif role == "arguments":
            values = {
                "payload_id": row.get("function_call_payload_id"),
                "digest": row.get("function_call_payload_digest"),
                "owner_component_id": "agent.runtime",
                "owner_kind": "tool.arguments",
                "owner_id": row.get("tool_call_id"),
            }
        elif role == "tool":
            values = {
                "payload_id": row.get("result_payload_id"),
                "digest": row.get("result_payload_digest"),
                "owner_component_id": row.get("tool_component_id"),
                "owner_kind": "tool.result",
                "owner_id": row.get("tool_call_id"),
            }
        else:
            raise AssertionError("unknown Agent history payload role")
        return _exact_ref(values)

    async def model_view(
        self,
        snapshot: dict,
        *,
        run: dict,
        history: dict,
        logical_step_id: str,
    ) -> dict:
        """Interleave Conversation Context with this Run's committed causal history."""
        base = await self.project(snapshot)
        progress = run.get("progress")
        inputs = run.get("inputs")
        history_protocol = history.get("protocol") if isinstance(history, dict) else None
        history_run_id = history.get("run_id") if isinstance(history, dict) else None
        turns = history.get("turns") if isinstance(history, dict) else None
        if (
            not isinstance(progress, dict)
            or not isinstance(inputs, list)
            or history_protocol != "forest.agent-execution-history/1"
            or history_run_id != run.get("run_id")
            or not isinstance(turns, list)
            or not isinstance(logical_step_id, str)
            or not logical_step_id
        ):
            raise ForestError(
                "AGENT_HISTORY_INVALID",
                "Agent model view requires one exact Run and execution history",
                409,
            )
        expected = {
            "conversation_id": run.get("conversation_ref"),
            "context_applied_seq": progress.get("selected_input_end_seq"),
            "context_revision": progress.get("context_revision"),
            "context_view_revision": progress.get("context_view_revision"),
        }
        if any(base.get(name) != value for name, value in expected.items()):
            raise ForestError(
                "AGENT_CONTEXT_FENCE_MISMATCH",
                "Conversation snapshot differs from the durable Agent input fence",
                409,
            )

        refs = []
        roles = []
        for row in turns:
            if not isinstance(row, dict):
                raise ForestError("AGENT_HISTORY_INVALID", "Agent history row is invalid", 409)
            refs.append(self._history_ref(row, "model"))
            roles.append((row, "model"))
            if row.get("tool_call_id") is not None:
                refs.append(self._history_ref(row, "arguments"))
                roles.append((row, "arguments"))
                if row.get("result_payload_id") is not None:
                    refs.append(self._history_ref(row, "tool"))
                    roles.append((row, "tool"))

        documents = []
        for start in range(0, len(refs), 8):
            bodies = await asyncio.gather(
                *(self.ctx.payloads.read_bytes(ref) for ref in refs[start : start + 8])
            )
            documents.extend(_json_object(body, "Agent history") for body in bodies)
        bodies_by_role = {
            (row["turn_id"], role): document
            for (row, role), document in zip(roles, documents, strict=True)
        }

        source_base = {
            "conversation_id": snapshot["conversation_id"],
            "binding_ref": snapshot["binding_ref"],
            "platform": snapshot["platform"],
            "channel_kind": snapshot["channel_kind"].lower(),
            "channel_id": snapshot["channel_ref"],
        }
        if snapshot["channel_kind"] == "GROUP":
            source_base["group_source_id"] = snapshot["conversation_id"]
        if snapshot["thread_ref"]:
            source_base["thread_id"] = snapshot["thread_ref"]

        history_by_revision: dict[int, list[dict]] = {}
        for row in turns:
            turn_id = row["turn_id"]
            model = bodies_by_role[(turn_id, "model")]
            candidate = model.get("text")
            if (
                model.get("protocol") != "forest.provider-model-result/1"
                or not isinstance(candidate, str)
                or not candidate
            ):
                raise ForestError(
                    "AGENT_HISTORY_INCOMPLETE",
                    "Committed ModelTurn has no exact Provider candidate",
                    500,
                )
            units = history_by_revision.setdefault(row["input_revision"], [])
            assistant = {
                "unit_id": f"agent-turn:{turn_id}",
                "role": "assistant",
                "source": {
                    **source_base,
                    "source_event_id": turn_id,
                    "source_kind": "agent.model-turn",
                },
                "author": {
                    "principal_ref": f"bot:{self.bot_id}",
                    "identity_origin": "host_model_output",
                },
                "parts": [
                    {
                        "part_id": f"agent-turn:{turn_id}:0",
                        "type": "text",
                        "text": candidate,
                    }
                ],
            }
            tool_call_id = row.get("tool_call_id")
            if tool_call_id is None:
                units.append(assistant)
                continue
            arguments = bodies_by_role[(turn_id, "arguments")]
            assistant["tool_calls"] = [
                {
                    "id": tool_call_id,
                    "name": row["tool_name"],
                    "arguments": arguments,
                }
            ]
            units.append(assistant)
            if row.get("tool_state") not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                raise ForestError(
                    "AGENT_HISTORY_INCOMPLETE",
                    "Tool causal group is open at the next model boundary",
                    409,
                )
            tool_value = (
                bodies_by_role[(turn_id, "tool")]
                if row.get("result_payload_id") is not None
                else {
                    "status": row["tool_state"],
                    "error_code": row.get("tool_error_code"),
                }
            )
            units.append(
                {
                    "unit_id": f"tool-result:{tool_call_id}",
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "source": {
                        **source_base,
                        "source_event_id": tool_call_id,
                        "source_kind": "agent.tool-result",
                    },
                    "author": {
                        "principal_ref": f"tool:{tool_call_id}",
                        "identity_origin": "host_tool_output",
                    },
                    "parts": [
                        {
                            "part_id": f"tool-result:{tool_call_id}:0",
                            "type": "json",
                            "value": tool_value,
                        }
                    ],
                }
            )

        current_batches = {item["input_batch_ref"] for item in inputs}
        context_units = base["units"]
        ordered = [
            unit for unit in context_units if unit.get("input_batch_id") not in current_batches
        ]
        for input_row in inputs:
            ordered.extend(
                unit
                for unit in context_units
                if unit.get("input_batch_id") == input_row["input_batch_ref"]
            )
            ordered.extend(history_by_revision.get(input_row["input_revision"], ()))
        expected_count = len(context_units) + sum(len(items) for items in history_by_revision.values())
        if len(ordered) != expected_count:
            raise ForestError(
                "AGENT_HISTORY_INCOMPLETE",
                "Agent history does not belong to a frozen input revision",
                500,
            )

        latest_revision = progress["input_revision"]
        latest_trigger_seqs = {
            item["last_trigger_ingress_seq"]
            for item in inputs
            if item["input_revision"] == latest_revision
        }
        required = [
            unit["unit_id"]
            for unit in context_units
            if unit.get("ingress_seq") in latest_trigger_seqs
        ]
        if not required and context_units:
            latest_batches = {
                item["input_batch_ref"]
                for item in inputs
                if item["input_revision"] == latest_revision
            }
            latest = [
                unit for unit in context_units if unit.get("input_batch_id") in latest_batches
            ]
            required = [(latest or context_units)[-1]["unit_id"]]
        conversation = {
            **source_base,
            "source_event_id": f"conversation:{snapshot['conversation_id']}",
            "source_kind": "conversation.current",
        }
        return {
            "bot_id": self.bot_id,
            "run_id": run["run_id"],
            "logical_step_id": logical_step_id,
            "context_snapshot_ref": base["source_manifest"],
            "input_revision": progress["input_revision"],
            "context_revision": progress["context_revision"],
            "context_view_revision": progress["context_view_revision"],
            "media_cost_revision": base["media_cost_revision"],
            "context_view_media_bytes": base["context_view_media_bytes"],
            "selected_input_end_seq": progress["selected_input_end_seq"],
            "trigger_claims": claim_summaries(inputs),
            "input_batch_ids": [item["input_batch_ref"] for item in inputs],
            "units": ordered,
            "conversation": conversation,
            "required_unit_ids": required,
        }

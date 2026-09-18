"""Build one immutable model snapshot from Conversation and Agent authorities."""

from __future__ import annotations

import json
from dataclasses import dataclass

from forest.agent_model_executor import AgentToolBinding
from forest.errors import ForestError
from forest.storage.agent_runtime_authority import AgentPayloadRef
from forest.storage.conversation_scheduler import SchedulerPayloadRef


def _json_object(body: bytes, *, kind: str) -> dict:
    try:
        value = json.loads(body)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForestError(
            "AGENT_CONTEXT_PAYLOAD_INVALID",
            f"{kind} payload is not valid JSON",
            500,
        ) from exc
    if not isinstance(value, dict):
        raise ForestError(
            "AGENT_CONTEXT_PAYLOAD_INVALID",
            f"{kind} payload is not an object",
            500,
        )
    return value


def _scheduler_ref(row: dict, prefix: str) -> SchedulerPayloadRef:
    payload_stem = "payload_" if prefix == "payload" else prefix + "_payload_"
    owner_stem = "payload_owner_" if prefix == "payload" else prefix + "_owner_"
    return SchedulerPayloadRef(
        row[f"{payload_stem}id"],
        row[f"{payload_stem}digest"],
        row[f"{owner_stem}component_id"],
        row[f"{owner_stem}kind"],
        row[f"{owner_stem}id"],
    )


def _part_id(unit_id: str, index: int) -> str:
    return f"{unit_id}:{index}"


def _parts(unit_id: str, value: dict) -> list[dict]:
    raw = value.get("parts")
    if isinstance(raw, list):
        result = []
        for index, item in enumerate(raw[:256]):
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
            if kind == "text" and isinstance(item.get("text"), str):
                result.append({"part_id": part_id, "type": "text", "text": item["text"]})
            elif kind == "image":
                media_ref = item.get("media_ref")
                if not isinstance(media_ref, str):
                    media_ref = f"pending-media:{unit_id}:{index}"
                projected = {
                    "part_id": part_id,
                    "type": "image",
                    "media_ref": media_ref,
                }
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


@dataclass(frozen=True)
class _HistoryPayload:
    reference: AgentPayloadRef
    kind: str


class ConversationContextProjector:
    """Merge ordered group Context with the current Run's atomic history."""

    def __init__(self, *, bot_id: str, payloads, agent) -> None:
        self.bot_id = bot_id
        self.payloads = payloads
        self.agent = agent

    @staticmethod
    def _source(context: dict, *, source_event_id: str, source_kind: str) -> dict:
        channel_kind = context["channel_kind"].lower()
        source = {
            "conversation_id": context["conversation_id"],
            "binding_ref": context["binding_ref"],
            "platform": context["platform"],
            "channel_kind": channel_kind,
            "channel_id": context["channel_ref"],
            "source_event_id": source_event_id,
            "source_kind": source_kind,
        }
        if channel_kind == "group":
            source["group_source_id"] = context["conversation_id"]
        if context.get("thread_ref"):
            source["thread_id"] = context["thread_ref"]
        return source

    async def project(
        self,
        *,
        run: dict,
        context: dict,
        logical_step_id: str,
        tool_bindings: tuple[AgentToolBinding, ...] = (),
    ) -> dict:
        expected = {
            "conversation_id": run["session"]["conversation_ref"],
            "context_applied_seq": run["selected_input_end_seq"],
            "context_revision": run["context_revision"],
            "context_view_revision": run["context_view_revision"],
        }
        if any(context.get(key) != value for key, value in expected.items()):
            raise ForestError(
                "AGENT_CONTEXT_FENCE_MISMATCH",
                "Conversation snapshot differs from the durable Run input fence",
                409,
            )
        for field in ("binding_ref", "platform", "channel_kind", "channel_ref"):
            if not isinstance(context.get(field), str) or not context[field]:
                raise ForestError(
                    "AGENT_CONTEXT_FENCE_MISMATCH",
                    "Conversation lane identity is incomplete",
                    409,
                )
        rows = context.get("units")
        if not isinstance(rows, list):
            raise ForestError(
                "AGENT_CONTEXT_PAYLOAD_INVALID", "Conversation units are invalid", 500
            )
        history = await self.agent.execution_history(run["run_id"])
        bindings = {item.name: item for item in tool_bindings}
        if len(bindings) != len(tool_bindings):
            raise ForestError(
                "AGENT_TOOL_BINDING_INVALID", "Tool binding names repeat", 422
            )

        references = []
        row_reference_indexes = []
        for row in rows:
            payload = _scheduler_ref(row, "payload")
            payload_index = len(references)
            references.append(payload)
            author_index = None
            if row.get("author_payload_id") is not None:
                author_index = len(references)
                references.append(_scheduler_ref(row, "author"))
            row_reference_indexes.append((payload_index, author_index))

        history_payloads: list[_HistoryPayload] = []
        for row in history:
            result_id = row.get("model_result_payload_id")
            attempt_id = row.get("model_attempt_id")
            if result_id is None or attempt_id is None:
                raise ForestError(
                    "AGENT_HISTORY_INCOMPLETE",
                    "A committed ModelTurn has no successful model result",
                    500,
                )
            history_payloads.append(
                _HistoryPayload(
                    AgentPayloadRef(
                        result_id,
                        row["model_result_payload_digest"],
                        row["provider_component_id"],
                        "model.result",
                        attempt_id,
                    ),
                    "model",
                )
            )
            references.append(history_payloads[-1].reference)
            if row.get("function_call_payload_id") is not None:
                history_payloads.append(
                    _HistoryPayload(
                        AgentPayloadRef(
                            row["function_call_payload_id"],
                            row["function_call_payload_digest"],
                            "agent.runtime",
                            "tool.arguments",
                            row["tool_call_id"],
                        ),
                        "arguments",
                    )
                )
                references.append(history_payloads[-1].reference)
            if row.get("result_payload_id") is not None:
                binding = bindings.get(row["tool_name"])
                if binding is None:
                    raise ForestError(
                        "AGENT_TOOL_PORT_UNAVAILABLE",
                        "Tool history has no frozen typed binding",
                        503,
                    )
                history_payloads.append(
                    _HistoryPayload(
                        AgentPayloadRef(
                            row["result_payload_id"],
                            row["result_payload_digest"],
                            binding.component_id,
                            "tool.result",
                            row["tool_call_id"],
                        ),
                        "tool",
                    )
                )
                references.append(history_payloads[-1].reference)

        bodies = await self.payloads.read_verified_batch(references)
        body_by_id = {
            reference.payload_id: body for reference, body in zip(references, bodies, strict=True)
        }
        context_units = []
        for row, (payload_index, author_index) in zip(
            rows, row_reference_indexes, strict=True
        ):
            unit_id = row["unit_id"]
            payload = _json_object(bodies[payload_index], kind="Context")
            author = (
                _json_object(bodies[author_index], kind="author")
                if author_index is not None
                else {}
            )
            author["principal_ref"] = row.get("author_subject_ref") or f"bot:{self.bot_id}"
            author.setdefault(
                "identity_origin",
                "host_model_output" if row["kind"] in {"REPLY", "AGENT_STEP"} else "adapter_verified",
            )
            role = {
                "INPUT": "user",
                "REPLY": "assistant",
                "AGENT_STEP": "assistant",
                "CONTROL": "control_data",
                "EFFECT": "control_data",
            }[row["kind"]]
            context_units.append(
                {
                    "unit_id": unit_id,
                    "role": role,
                    "source": self._source(
                        context,
                        source_event_id=row.get("event_id") or row["source_id"],
                        source_kind=row["source_kind"],
                    ),
                    "author": author,
                    "parts": _parts(unit_id, payload),
                    "input_batch_id": row.get("input_batch_id"),
                    "ingress_seq": row.get("ingress_seq"),
                }
            )

        payload_cursor = 0
        history_units_by_revision: dict[int, list[dict]] = {}
        for row in history:
            model_payload = _json_object(
                body_by_id[history_payloads[payload_cursor].reference.payload_id],
                kind="model result",
            )
            payload_cursor += 1
            text = model_payload.get("text")
            if not isinstance(text, str) or not text:
                raise ForestError(
                    "AGENT_HISTORY_INCOMPLETE",
                    "Model result omitted its protocol candidate",
                    500,
                )
            assistant = {
                "unit_id": f"agent-turn:{row['turn_id']}",
                "role": "assistant",
                "source": self._source(
                    context,
                    source_event_id=row["turn_id"],
                    source_kind="agent.model-turn",
                ),
                "author": {
                    "principal_ref": f"bot:{self.bot_id}",
                    "identity_origin": "host_model_output",
                },
                "parts": [
                    {
                        "part_id": f"agent-turn:{row['turn_id']}:0",
                        "type": "text",
                        "text": text,
                    }
                ],
            }
            units = history_units_by_revision.setdefault(row["input_revision"], [])
            if row.get("tool_call_id") is not None:
                arguments = _json_object(
                    body_by_id[history_payloads[payload_cursor].reference.payload_id],
                    kind="tool arguments",
                )
                payload_cursor += 1
                assistant["tool_calls"] = [
                    {
                        "id": row["tool_call_id"],
                        "name": row["tool_name"],
                        "arguments": arguments,
                    }
                ]
                units.append(assistant)
                if row["tool_state"] not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    raise ForestError(
                        "AGENT_HISTORY_INCOMPLETE",
                        "Tool causal group is not closed before the next model call",
                        409,
                    )
                if row.get("result_payload_id") is not None:
                    tool_value = _json_object(
                        body_by_id[history_payloads[payload_cursor].reference.payload_id],
                        kind="tool result",
                    )
                    payload_cursor += 1
                else:
                    tool_value = {
                        "status": row["tool_state"],
                        "error_code": row.get("tool_error_code"),
                    }
                units.append(
                    {
                        "unit_id": f"tool-result:{row['tool_call_id']}",
                        "role": "tool",
                        "tool_call_id": row["tool_call_id"],
                        "source": self._source(
                            context,
                            source_event_id=row["tool_call_id"],
                            source_kind="agent.tool-result",
                        ),
                        "author": {
                            "principal_ref": f"tool:{row['tool_call_id']}",
                            "identity_origin": "host_tool_output",
                        },
                        "parts": [
                            {
                                "part_id": f"tool-result:{row['tool_call_id']}:0",
                                "type": "json",
                                "value": tool_value,
                            }
                        ],
                    }
                )
            else:
                units.append(assistant)

        current_batches = {item["input_batch_ref"] for item in run["inputs"]}
        ordered = [
            unit for unit in context_units if unit.get("input_batch_id") not in current_batches
        ]
        for input_row in run["inputs"]:
            ordered.extend(
                unit
                for unit in context_units
                if unit.get("input_batch_id") == input_row["input_batch_ref"]
            )
            ordered.extend(history_units_by_revision.get(input_row["input_revision"], ()))
        if len(ordered) != len(context_units) + sum(
            len(value) for value in history_units_by_revision.values()
        ):
            raise ForestError(
                "AGENT_HISTORY_INCOMPLETE",
                "Agent history does not belong to a frozen input revision",
                500,
            )
        latest_revision = run["input_revision"]
        latest_batches = {
            item["input_batch_ref"]
            for item in run["inputs"]
            if item["input_revision"] == latest_revision
        }
        latest_trigger_seqs = {
            item["ingress_seq"]
            for item in run["triggers"]
            if item["input_revision"] == latest_revision
        }
        required = [
            unit["unit_id"]
            for unit in context_units
            if unit.get("input_batch_id") in latest_batches
            or unit.get("ingress_seq") in latest_trigger_seqs
        ]
        if not required and context_units:
            required = [context_units[-1]["unit_id"]]
        conversation = self._source(
            context,
            source_event_id=f"conversation:{context['conversation_id']}",
            source_kind="conversation.current",
        )
        return {
            "bot_id": self.bot_id,
            "run_id": run["run_id"],
            "logical_step_id": logical_step_id,
            "input_revision": run["input_revision"],
            "context_revision": run["context_revision"],
            "context_view_revision": run["context_view_revision"],
            "selected_input_end_seq": run["selected_input_end_seq"],
            "trigger_ids": [item["trigger_ref"] for item in run["triggers"]],
            "input_batch_ids": [item["input_batch_ref"] for item in run["inputs"]],
            "units": ordered,
            "conversation": conversation,
            "required_unit_ids": required,
        }

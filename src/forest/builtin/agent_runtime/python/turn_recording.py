"""Commit Provider candidates through the Agent package's durable protocol."""

from __future__ import annotations

import hashlib
import json

from agent_protocol import ProtocolRepairView, parse_model_turn

from forest.errors import ForestError


def stable(kind: str, *parts: object) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{kind}:{hashlib.sha256(encoded).hexdigest()}"


def digest_json(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _bounded_excerpt(value: str, limit: int = 8192) -> str:
    raw = value.encode("utf-8", errors="replace")
    if len(raw) <= limit:
        return value
    marker = b"\n...[candidate truncated by Agent]...\n"
    remaining = max(0, limit - len(marker))
    prefix_size = remaining // 2
    suffix_size = remaining - prefix_size
    prefix = raw[:prefix_size].decode("utf-8", errors="ignore").encode("utf-8")
    suffix = raw[-suffix_size:].decode("utf-8", errors="ignore").encode("utf-8")
    return (prefix + marker + suffix).decode("utf-8")


def _payload_ref(value: dict, *, component: str, kind: str, owner_id: str) -> dict:
    fields = {
        "payload_id",
        "digest",
        "owner_component_id",
        "owner_kind",
        "owner_id",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("owner_component_id") != component
        or value.get("owner_kind") != kind
        or value.get("owner_id") != owner_id
    ):
        raise ForestError(
            "AGENT_PAYLOAD_REFERENCE_INVALID",
            "Agent artifact has an invalid exact Payload reference",
            409,
        )
    return value


def tool_directory(value: dict, *, identity: dict) -> tuple[list[dict], dict[str, dict]]:
    if (
        not isinstance(value, dict)
        or value.get("protocol") != "forest.contribution-directory/1"
        or value.get("kind") != "agent-tools"
        or value.get("graph_id") != identity.get("graph_id")
        or value.get("graph_revision") != identity.get("graph_revision")
        or not isinstance(value.get("items"), list)
    ):
        raise ForestError(
            "AGENT_TOOL_DIRECTORY_INVALID",
            "Agent tool directory differs from the frozen RuntimeGraph",
            409,
        )
    declarations: list[dict] = []
    bindings: dict[str, dict] = {}
    expected = {
        "protocol",
        "name",
        "service_id",
        "version",
        "declaration",
        "contract_digest",
        "approval_required",
        "supports_query",
        "owner_component_id",
        "target_generation_id",
    }
    for item in value["items"]:
        if (
            not isinstance(item, dict)
            or set(item) != expected
            or item.get("protocol") != "forest.agent-tool-contribution/1"
            or not isinstance(item.get("declaration"), dict)
            or item["declaration"].get("name") != item.get("name")
            or type(item.get("version")) is not int
            or type(item.get("approval_required")) is not bool
            or type(item.get("supports_query")) is not bool
        ):
            raise ForestError(
                "AGENT_TOOL_DIRECTORY_INVALID",
                "Agent tool directory contains an invalid binding",
                409,
            )
        name = item["name"]
        if name in bindings:
            raise ForestError(
                "AGENT_TOOL_DIRECTORY_INVALID",
                "Agent tool names must be unique in one RuntimeGraph",
                409,
            )
        binding = {
            "name": name,
            "component_id": item["owner_component_id"],
            "generation_id": item["target_generation_id"],
            "service_id": item["service_id"],
            "service_version": item["version"],
            "contract_digest": item["contract_digest"],
            "approval_required": item["approval_required"],
            "supports_query": item["supports_query"],
        }
        binding["binding_digest"] = digest_json(
            {key: binding[key] for key in binding if key != "service_version"}
        )
        bindings[name] = binding
        declarations.append(item["declaration"])
    return declarations, bindings


class TurnRecorder:
    """Translate one immutable Provider result into Agent-owned facts."""

    def __init__(self, ctx) -> None:
        self.ctx = ctx

    async def _protocol_failure(
        self,
        *,
        owner: dict,
        request_id: str,
        attempt_id: str,
        candidate: str,
        error: ForestError,
        result_ref: dict,
        allow_repair: bool,
    ) -> dict:
        candidate_digest = "sha256:" + hashlib.sha256(
            candidate.encode("utf-8", errors="surrogatepass")
        ).hexdigest()
        failure_id = stable("protocol-failure", owner["run_id"], request_id)
        feedback = await self.ctx.payloads.put_json(
            {
                "repair_count": 1,
                "error_code": error.code,
                "validation_reason": error.message,
                "candidate_digest": candidate_digest,
                "feedback": _bounded_excerpt(candidate),
            },
            owner_kind="protocol.repair.feedback",
            owner_id=failure_id,
            parents=(result_ref,),
        )
        target = "REPAIR_READY" if allow_repair else "FAILED"
        checkpoint_id = stable("checkpoint-protocol-failure", failure_id, target)
        checkpoint = await self.ctx.payloads.put_json(
            {
                "phase": target,
                "failure_id": failure_id,
                "failed_request_id": request_id,
                "error_code": error.code,
                "candidate_digest": candidate_digest,
            },
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            parents=(feedback,),
        )
        saved = await self.ctx.uow.call(
            "agent.record-protocol-failure",
            {
                "owner": owner,
                "failure_id": failure_id,
                "request_id": request_id,
                "attempt_id": attempt_id,
                "error_code": error.code,
                "candidate_digest": candidate_digest,
                "feedback": feedback,
                "checkpoint_id": checkpoint_id,
                "checkpoint_state": checkpoint,
                "allow_repair": allow_repair,
            },
            operation_id=self.ctx.operations.stable(f"protocol-failure:{request_id}"),
            timeout=15,
        )
        return {"status": saved["resulting_state"], "error_code": error.code}

    async def record(
        self,
        *,
        owner: dict,
        request_id: str,
        attempt_id: str,
        turn_id: str,
        candidate: str,
        result_ref: dict,
        provider_error_code: str | None,
        declarations: list[dict],
        bindings: dict[str, dict],
        repair_of_request_id: str | None,
        allow_repair: bool,
    ) -> dict:
        if provider_error_code is not None:
            return await self._protocol_failure(
                owner=owner,
                request_id=request_id,
                attempt_id=attempt_id,
                candidate=candidate,
                error=ForestError(
                    "AGENT_PROTOCOL_" + provider_error_code.removeprefix("MODEL_"),
                    f"Completed model response is unusable: {provider_error_code}",
                    422,
                ),
                result_ref=result_ref,
                allow_repair=allow_repair,
            )
        try:
            turn = parse_model_turn(candidate, allowed_tools=tuple(bindings))
        except ForestError as exc:
            if not exc.code.startswith("AGENT_PROTOCOL_"):
                raise
            return await self._protocol_failure(
                owner=owner,
                request_id=request_id,
                attempt_id=attempt_id,
                candidate=candidate,
                error=exc,
                result_ref=result_ref,
                allow_repair=allow_repair,
            )
        if {item.get("name") for item in declarations} != set(bindings):
            raise ForestError(
                "AGENT_TOOL_DIRECTORY_INVALID",
                "Tool declarations and frozen bindings differ",
                409,
            )
        turn_ref = await self.ctx.payloads.put_json(
            turn.as_dict(),
            owner_kind="model.turn",
            owner_id=turn_id,
            parents=(result_ref,),
        )
        artifacts = [{"role": "TURN", "payload": turn_ref}]
        assistant_ref = None
        if turn.assistant_text:
            assistant_ref = await self.ctx.payloads.put_json(
                {"text": turn.assistant_text},
                owner_kind="assistant.candidate",
                owner_id=turn_id,
                parents=(turn_ref,),
            )
            artifacts.append({"role": "ASSISTANT", "payload": assistant_ref})
        proposal = None
        if turn.function_call is not None:
            binding = bindings.get(turn.function_call.name)
            if binding is None:
                raise ForestError(
                    "AGENT_TOOL_PORT_UNAVAILABLE",
                    "Model proposed a tool without a frozen typed binding",
                    503,
                )
            wrapper = turn.function_call.arguments
            if (
                set(wrapper) != {"authority_trigger_id", "arguments"}
                or not isinstance(wrapper.get("authority_trigger_id"), str)
                or not wrapper["authority_trigger_id"]
                or not isinstance(wrapper.get("arguments"), dict)
            ):
                raise ForestError(
                    "AGENT_TOOL_AUTHORITY_INVALID",
                    "Tool arguments must bind to one durable trigger",
                    409,
                )
            call_digest = digest_json(
                {
                    "turn_id": turn_id,
                    "binding": {
                        key: binding[key]
                        for key in (
                            "name",
                            "component_id",
                            "generation_id",
                            "service_id",
                            "contract_digest",
                            "approval_required",
                            "supports_query",
                        )
                    },
                    "arguments": wrapper,
                }
            )[7:]
            tool_call_id = "tool-call:" + call_digest
            arguments_ref = await self.ctx.payloads.put_json(
                wrapper,
                owner_kind="tool.arguments",
                owner_id=tool_call_id,
                parents=(turn_ref,),
            )
            artifacts.append({"role": "TOOL_ARGUMENTS", "payload": arguments_ref})
            proposal = {
                "tool_call_id": tool_call_id,
                "component_id": binding["component_id"],
                "generation_id": binding["generation_id"],
                "service_id": binding["service_id"],
                "tool_contract_digest": binding["contract_digest"],
                "tool_binding_digest": binding["binding_digest"],
                "approval_required": binding["approval_required"],
                "supports_query": binding["supports_query"],
                "authority_trigger_ref": wrapper["authority_trigger_id"],
                "effect_operation_ref": "tool-effect:" + call_digest,
            }
        saved = await self.ctx.uow.call(
            "agent.record-model-turn",
            {
                "authority": {
                    "run_ref": owner["run_id"],
                    "trigger_ref": (
                        proposal["authority_trigger_ref"] if proposal is not None else None
                    ),
                },
                "agent": {
                    "owner": owner,
                    "turn_id": turn_id,
                    "request_id": request_id,
                    "turn": turn.as_dict(),
                    "artifacts": artifacts,
                    "tool": proposal,
                    "repair_of_request_id": repair_of_request_id,
                },
            },
            operation_id=self.ctx.operations.stable(f"model-turn:{request_id}"),
            timeout=15,
        )
        return {
            "status": saved["next_state"],
            "turn": saved,
            "assistant": assistant_ref,
            "tool_call_id": saved["tool_call_id"],
        }

    async def repair_view(self, recovery: dict) -> tuple[dict, str]:
        failure = recovery.get("protocol_failure")
        if (
            not isinstance(failure, dict)
            or failure.get("resulting_state") != "REPAIR_READY"
            or failure.get("repair_step_seq") != recovery.get("next_step_seq")
        ):
            raise ForestError(
                "AGENT_HISTORY_INCOMPLETE",
                "Repair boundary has no durable protocol failure",
                500,
            )
        feedback_ref = _payload_ref(
            {
                "payload_id": failure.get("feedback_payload_id"),
                "digest": failure.get("feedback_payload_digest"),
                "owner_component_id": "agent.runtime",
                "owner_kind": "protocol.repair.feedback",
                "owner_id": failure.get("failure_id"),
            },
            component="agent.runtime",
            kind="protocol.repair.feedback",
            owner_id=failure.get("failure_id"),
        )
        document = await self.ctx.payloads.read_json(feedback_ref)
        view = ProtocolRepairView.from_record(document)
        if (
            view.repair_count != recovery.get("repair_count")
            or view.error_code != failure.get("error_code")
            or view.candidate_digest != failure.get("candidate_digest")
        ):
            raise ForestError(
                "AGENT_PROTOCOL_REPAIR_PAYLOAD_CONFLICT",
                "Protocol repair payload differs from its durable boundary",
                409,
            )
        return document, failure["failed_request_id"]

    async def finalize(self, *, owner: dict, recovery: dict) -> dict:
        latest = recovery.get("latest_turn")
        if (
            recovery.get("state") != "SAFE_BOUNDARY"
            or not isinstance(latest, dict)
            or latest.get("terminal") is not True
            or not isinstance(latest.get("assistant_payload_id"), str)
        ):
            raise ForestError(
                "AGENT_RECOVERY_BOUNDARY_INVALID",
                "Run has no terminal ModelTurn at its safe boundary",
                409,
            )
        turn_id = latest["turn_id"]
        source = _payload_ref(
            {
                "payload_id": latest["assistant_payload_id"],
                "digest": latest.get("assistant_payload_digest"),
                "owner_component_id": "agent.runtime",
                "owner_kind": "assistant.candidate",
                "owner_id": turn_id,
            },
            component="agent.runtime",
            kind="assistant.candidate",
            owner_id=turn_id,
        )
        assistant = await self.ctx.payloads.read_json(source)
        if (
            not isinstance(assistant, dict)
            or set(assistant) != {"text"}
            or not isinstance(assistant["text"], str)
        ):
            raise ForestError(
                "MODEL_TURN_PAYLOAD_CONFLICT",
                "Terminal assistant candidate is invalid",
                409,
            )
        reply_ref = stable("reply", owner["run_id"])
        candidate = await self.ctx.payloads.put_json(
            assistant,
            owner_kind="final.reply",
            owner_id=reply_ref,
            parents=(source,),
        )
        run = await self.ctx.uow.call(
            "agent.finalize-candidate",
            {
                "owner": owner,
                "turn_id": turn_id,
                "source_candidate": source,
                "candidate": candidate,
            },
            operation_id=self.ctx.operations.stable(f"finalize-candidate:{turn_id}"),
            timeout=15,
        )
        return {"run": run, "candidate": candidate, "text": assistant["text"]}

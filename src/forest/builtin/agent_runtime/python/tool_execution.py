"""One frozen Agent ToolCall over Authorization, Kernel Effect and package ports."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from turn_recording import digest_json

from forest.errors import ForestError


class ToolExecution:
    def __init__(self, ctx) -> None:
        self.ctx = ctx

    def _identity(self) -> tuple[str, int, str]:
        graph_id = self.ctx.identity.get("graph_id")
        graph_revision = self.ctx.identity.get("graph_revision")
        bot_id = self.ctx.identity.get("bot_id")
        if (
            not isinstance(graph_id, str)
            or not graph_id
            or type(graph_revision) is not int
            or graph_revision < 1
            or not isinstance(bot_id, str)
            or not bot_id
        ):
            raise ForestError(
                "AGENT_GENERATION_FENCE_MISSING",
                "Agent Worker has no frozen RuntimeGraph identity",
                409,
            )
        return graph_id, graph_revision, bot_id

    @staticmethod
    def _receipt(value, *, tool: dict, attempt_id: str) -> dict:
        fields = {
            "protocol",
            "status",
            "tool_call_id",
            "effect_operation_id",
            "effect_attempt_id",
            "result",
            "error_code",
            "observation_ref",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or value.get("protocol") != "forest.agent-tool-receipt/1"
            or value.get("status") not in {"SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"}
            or value.get("tool_call_id") != tool["tool_call_id"]
            or value.get("effect_operation_id") != tool["effect_operation_ref"]
            or value.get("effect_attempt_id") != attempt_id
            or not isinstance(value.get("observation_ref"), str)
            or not value["observation_ref"]
        ):
            raise ForestError(
                "AGENT_TOOL_RESULT_INVALID",
                "Tool receipt differs from the frozen ToolCall",
                502,
            )
        result = value["result"]
        error = value["error_code"]
        if value["status"] == "SUCCEEDED":
            if (
                not isinstance(result, dict)
                or result.get("owner_component_id") != tool["tool_component_id"]
                or result.get("owner_kind") != "tool.result"
                or result.get("owner_id") != tool["tool_call_id"]
                or error is not None
            ):
                raise ForestError(
                    "AGENT_TOOL_RESULT_INVALID",
                    "Successful tool receipt has no valid owned result",
                    502,
                )
        elif result is not None or (
            value["status"] in {"FAILED", "CANCELLED"}
            and (not isinstance(error, str) or not error)
        ):
            raise ForestError(
                "AGENT_TOOL_RESULT_INVALID", "Tool receipt state is invalid", 502
            )
        return value

    async def invoke(self, *, owner: dict, tool: dict, binding: dict) -> dict:
        graph_id, graph_revision, bot_id = self._identity()
        if tool["state"] == "DISPATCHING":
            return {"status": "UNKNOWN", "error_code": "AGENT_TOOL_RESULT_UNKNOWN"}
        if tool["state"] == "UNKNOWN":
            return {"status": "UNKNOWN", "error_code": "AGENT_TOOL_RESULT_UNKNOWN"}
        if tool["state"] == "WAIT_APPROVAL" or tool["approval_required"]:
            return {
                "status": "WAIT_APPROVAL",
                "approval_ref": tool.get("approval_ref"),
                "error_code": None,
            }
        if tool["state"] != "PROPOSED":
            raise ForestError(
                "AGENT_TOOL_STATE_INVALID", "ToolCall is not executable", 409
            )
        frozen = {
            "name": tool["tool_name"],
            "component_id": tool["tool_component_id"],
            "generation_id": tool["tool_generation_id"],
            "service_id": tool["tool_service_id"],
            "contract_digest": tool["tool_contract_digest"],
            "approval_required": tool["approval_required"],
            "supports_query": tool["supports_query"],
        }
        if (
            any(binding.get(key) != value for key, value in frozen.items())
            or binding.get("binding_digest") != tool["tool_binding_digest"]
        ):
            raise ForestError(
                "AGENT_TOOL_ROUTE_STALE",
                "Current tool directory differs from the durable ToolCall",
                409,
            )
        arguments_ref = {
            "payload_id": tool["arguments_payload_id"],
            "digest": tool["arguments_payload_digest"],
            "owner_component_id": "agent.runtime",
            "owner_kind": "tool.arguments",
            "owner_id": tool["tool_call_id"],
        }
        wrapper = await self.ctx.payloads.read_json(arguments_ref)
        if (
            not isinstance(wrapper, dict)
            or set(wrapper) != {"authority_trigger_id", "arguments"}
            or wrapper.get("authority_trigger_id") != tool["authority_trigger_ref"]
            or not isinstance(wrapper.get("arguments"), dict)
        ):
            raise ForestError(
                "AGENT_TOOL_ARGUMENTS_INVALID",
                "Tool arguments differ from the durable authority trigger",
                409,
            )
        attempt_id = self.ctx.operations.stable(
            f"tool-attempt:{tool['tool_call_id']}"
        )
        command = {
            "protocol": "forest.agent-tool-execute/1",
            "tool_call_id": tool["tool_call_id"],
            "effect_operation_id": tool["effect_operation_ref"],
            "component_id": tool["tool_component_id"],
            "generation_id": tool["tool_generation_id"],
            "service_id": tool["tool_service_id"],
            "contract_digest": tool["tool_contract_digest"],
            "binding_digest": tool["tool_binding_digest"],
            "authority_trigger_ref": tool["authority_trigger_ref"],
            "authority_principal_ref": tool["authority_principal_ref"],
            "arguments": wrapper["arguments"],
        }
        command_digest = digest_json(command)
        permit_operation_id = self.ctx.operations.stable(
            f"tool-permit:{tool['tool_call_id']}"
        )
        ticket_id = self.ctx.operations.stable(
            f"tool-authorization:{tool['tool_call_id']}"
        )
        expires_at = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        prepared = await self.ctx.uow.call(
            "agent.prepare-tool-effect",
            {
                "authorization": {
                    "protocol": "forest.authorization-issue/1",
                    "ticket_id": ticket_id,
                    "issuance_operation_id": self.ctx.operations.stable(
                        f"tool-authorization-issue:{tool['tool_call_id']}"
                    ),
                    "operation_id": permit_operation_id,
                    "action": "tool.invoke",
                    "resource_kind": "bot",
                    "resource_id": bot_id,
                    "input_digest": command_digest,
                    "plan_digest": tool["tool_binding_digest"],
                    "expires_at": expires_at,
                    "audience_component_id": tool["tool_component_id"],
                    "audience_generation_id": tool["tool_generation_id"],
                },
                "effect": {
                    "protocol": "forest.effect-command-prepare/1",
                    "effect_operation_id": tool["effect_operation_ref"],
                    "input_digest": command_digest,
                    "effect_kind": "tool.dispatch",
                    "target_kind": "component.service",
                    "target_id": f"{tool['tool_component_id']}/{tool['tool_service_id']}",
                    "attempt_id": attempt_id,
                    "permit_operation_id": permit_operation_id,
                    "authorization_step_id": "authorization",
                    "authorization": {
                        "action": "tool.invoke",
                        "resource_kind": "bot",
                        "resource_id": bot_id,
                        "plan_digest": tool["tool_binding_digest"],
                    },
                    "executor": {
                        "component_id": tool["tool_component_id"],
                        "generation_id": tool["tool_generation_id"],
                        "fence_kind": "runtime.graph",
                        "fence_id": graph_id,
                        "fence_epoch": graph_revision,
                    },
                    "deadline_at": expires_at,
                    "command": {"value": command, "parents": [arguments_ref]},
                },
                "agent": {
                    "owner": owner,
                    "tool_call_id": tool["tool_call_id"],
                    "effect_operation_ref": tool["effect_operation_ref"],
                    "effect_attempt_id": attempt_id,
                    "command_digest": command_digest,
                    "effect_step_id": "effect",
                },
            },
            operation_id=self.ctx.operations.stable(
                f"prepare-tool-effect:{tool['tool_call_id']}"
            ),
            timeout=15,
        )
        observation_id = self.ctx.operations.stable(
            f"tool-handoff-observation:{tool['tool_call_id']}"
        )
        await self.ctx.uow.call(
            "agent.handoff-tool-effect",
            {
                "effect": {
                    "protocol": "forest.effect-handoff/1",
                    "permit": prepared["permit"],
                    "handoff_operation_id": self.ctx.operations.stable(
                        f"tool-handoff:{tool['tool_call_id']}"
                    ),
                },
                "agent": {
                    "owner": owner,
                    "observation_operation_id": observation_id,
                    "tool_call_id": tool["tool_call_id"],
                    "effect_attempt_id": attempt_id,
                    "effect_step_id": "effect",
                },
            },
            operation_id=self.ctx.operations.stable(
                f"handoff-tool-effect:{tool['tool_call_id']}"
            ),
            timeout=15,
        )
        try:
            raw = await self.ctx.agent_tools.dispatch_prepared(
                {
                    "tool_name": tool["tool_name"],
                    "target_component_id": tool["tool_component_id"],
                    "target_generation_id": tool["tool_generation_id"],
                    "target_service_id": tool["tool_service_id"],
                    "target_service_version": binding["service_version"],
                    "contract_digest": tool["tool_contract_digest"],
                    "binding_digest": tool["tool_binding_digest"],
                    "effect_operation_id": tool["effect_operation_ref"],
                    "attempt_id": attempt_id,
                    "permit": prepared["permit"],
                    "command": command,
                    "operation_id": tool["effect_operation_ref"],
                    "timeout": 30,
                }
            )
            receipt = self._receipt(raw, tool=tool, attempt_id=attempt_id)
        except Exception:
            receipt = {
                "protocol": "forest.agent-tool-receipt/1",
                "status": "UNKNOWN",
                "tool_call_id": tool["tool_call_id"],
                "effect_operation_id": tool["effect_operation_ref"],
                "effect_attempt_id": attempt_id,
                "result": None,
                "error_code": None,
                "observation_ref": attempt_id,
            }
        observed_state = receipt["status"]
        result = receipt["result"]
        result_observation_id = self.ctx.operations.stable(
            f"tool-result-observation:{tool['tool_call_id']}"
        )
        await self.ctx.uow.call(
            "agent.observe-tool-effect",
            {
                "effect": {
                    "protocol": "forest.effect-command-result/1",
                    "effect_operation_id": tool["effect_operation_ref"],
                    "attempt_id": attempt_id,
                    "observation_operation_id": result_observation_id,
                    "status": "UNKNOWN" if observed_state == "UNKNOWN" else "SUCCEEDED",
                    "evidence_kind": "executor",
                    "evidence": {
                        "value": receipt,
                        "parents": [] if result is None else [result],
                    },
                },
                "agent": {
                    "owner": owner,
                    "observation_operation_id": result_observation_id,
                    "tool_call_id": tool["tool_call_id"],
                    "effect_attempt_id": attempt_id,
                    "observed_state": observed_state,
                    "evidence_kind": "EXECUTOR",
                    "effect_observation_ref": receipt["observation_ref"],
                    "approval_ref": None,
                    "error_code": receipt["error_code"],
                    "results": [] if result is None else [result],
                    "effect_step_id": "effect",
                    "evidence_digest": digest_json(receipt),
                },
            },
            operation_id=self.ctx.operations.stable(
                f"observe-tool-effect:{tool['tool_call_id']}"
            ),
            timeout=15,
        )
        return {
            "status": observed_state,
            "approval_ref": None,
            "error_code": receipt["error_code"],
        }

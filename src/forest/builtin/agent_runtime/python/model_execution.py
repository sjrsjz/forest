"""One Agent-owned model attempt over typed package and kernel ports."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

from forest.errors import ForestError


def _digest(value) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _receipt(value, *, request_id: str, attempt_id: str, payload_digest: str) -> dict:
    fields = {
        "status",
        "request_id",
        "transport_attempt_id",
        "payload_digest",
        "result",
        "usage",
        "error_code",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("status") not in {"RESPONDED", "NOT_SENT", "UNKNOWN"}
        or value.get("request_id") != request_id
        or value.get("transport_attempt_id") != attempt_id
        or value.get("payload_digest") != payload_digest
    ):
        raise ForestError(
            "PROVIDER_RESULT_FENCE_MISMATCH",
            "Provider receipt differs from the frozen model attempt",
            502,
        )
    if value["status"] == "RESPONDED":
        if value["result"] is None or value["error_code"] is not None:
            raise ForestError(
                "PROVIDER_RESULT_INVALID", "Successful Provider receipt is incomplete", 502
            )
    elif value["result"] is not None or value["usage"] is not None:
        raise ForestError(
            "PROVIDER_RESULT_INVALID", "Unsettled Provider receipt carries payloads", 502
        )
    return value


def _provider_result(value, *, receipt: dict) -> dict:
    fields = {
        "protocol",
        "status",
        "request_id",
        "transport_attempt_id",
        "payload_digest",
        "text",
        "tool_calls",
        "finish_reason",
        "provider_response_id",
        "provider_model",
        "error_code",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("protocol") != "forest.provider-model-result/1"
        or value.get("status") != "RESPONDED"
        or value.get("request_id") != receipt["request_id"]
        or value.get("transport_attempt_id") != receipt["transport_attempt_id"]
        or value.get("payload_digest") != receipt["payload_digest"]
        or not isinstance(value.get("text"), str)
        or not isinstance(value.get("tool_calls"), list)
        or value.get("error_code") is not None
    ):
        raise ForestError(
            "PROVIDER_RESULT_INVALID", "Provider result Payload is invalid", 502
        )
    return value


class ModelExecution:
    """Freeze, authorize, hand off and observe exactly one Provider attempt."""

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

    async def invoke(
        self,
        *,
        owner: dict,
        request_id: str,
        attempt_id: str,
        effect_operation_id: str,
        snapshot: dict,
        preset_context: list[str],
        tools: list[dict],
        media: list[dict],
        evidence: dict | None,
        repair: dict | None,
        output_tokens: int,
        stream: bool,
        request_kind: str,
    ) -> dict:
        graph_id, graph_revision, bot_id = self._identity()
        profile = await self.ctx.services.call(
            "provider.profile.bind",
            {"protocol": "forest.provider-bind-request/1", "request_id": request_id},
            operation_id=self.ctx.operations.stable(f"provider-bind:{request_id}"),
            timeout=15,
        )
        if (
            profile.get("protocol") != "forest.provider-profile/1"
            or profile.get("request_id") != request_id
            or profile.get("component_id") != "provider.chat-completions"
        ):
            raise ForestError(
                "PROVIDER_PROFILE_FENCE_MISMATCH",
                "Provider profile differs from the requested model step",
                409,
            )
        capabilities = profile["capabilities"]
        model_input = {
            "protocol": "forest.agent-model-input/1",
            "request_id": request_id,
            "transport_attempt_id": attempt_id,
            "snapshot": snapshot,
            "preset_context": preset_context,
            "tools": tools,
            "evidence": evidence,
            "repair": repair,
            "output_tokens": output_tokens,
            "stream": stream,
        }
        model_input_ref = await self.ctx.payloads.put_json(
            model_input,
            owner_kind="model.input",
            owner_id=attempt_id,
            parents=(),
        )
        materialized = await self.ctx.services.call(
            "provider.request.materialize",
            {
                "protocol": "forest.provider-materialize/1",
                "component_id": profile["component_id"],
                "generation_id": profile["generation_id"],
                "profile_ref": profile["profile_ref"],
                "capability_digest": profile["capability_digest"],
                "request_id": request_id,
                "transport_attempt_id": attempt_id,
                "input": model_input_ref,
                "media": media,
            },
            operation_id=self.ctx.operations.stable(f"provider-materialize:{attempt_id}"),
            timeout=120,
        )
        if any(
            materialized.get(name) != profile[name]
            for name in ("component_id", "generation_id", "profile_ref", "capability_digest")
        ) or (
            materialized.get("request_id") != request_id
            or materialized.get("transport_attempt_id") != attempt_id
        ):
            raise ForestError(
                "PROVIDER_REQUEST_FENCE_MISMATCH",
                "Materialized request belongs to another Provider binding",
                409,
            )
        await self.ctx.uow.call(
            "agent.prepare-model-request",
            {
                "owner": owner,
                "request": {
                    "request_id": request_id,
                    "request_kind": request_kind,
                    "provider_component_id": profile["component_id"],
                    "provider_generation_id": profile["generation_id"],
                    "provider_profile_ref": profile["profile_ref"],
                    "model_ref": capabilities["model"],
                    "instruction_channel": {
                        "system": "SYSTEM",
                        "developer": "DEVELOPER",
                    }[capabilities["system_role"]],
                    "capability_digest": profile["capability_digest"],
                    "projection": materialized["projection"],
                    "projected_content_digest": materialized["projected_content_digest"],
                    "source_manifest_digest": materialized["source_manifest_digest"],
                    "output_token_budget": materialized["max_output_tokens"],
                },
            },
            operation_id=self.ctx.operations.stable(f"model-request:{request_id}"),
            timeout=15,
        )
        input_digest = _digest(
            {
                "profile_ref": profile["profile_ref"],
                "capability_digest": profile["capability_digest"],
                "payload_digest": materialized["wire"]["digest"],
            }
        )
        permit_operation_id = self.ctx.operations.stable(f"effect-permit:{attempt_id}")
        ticket_id = self.ctx.operations.stable(f"authorization-ticket:{attempt_id}")
        expires_at = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        dispatch = {
            "protocol": "forest.provider-dispatch/1",
            "effect_operation_id": effect_operation_id,
            "authorization_ticket_id": ticket_id,
            "component_id": profile["component_id"],
            "generation_id": profile["generation_id"],
            "profile_ref": profile["profile_ref"],
            "capability_digest": profile["capability_digest"],
            "prepared": {
                "request_id": request_id,
                "transport_attempt_id": attempt_id,
                "projection": materialized["projection"],
                "wire": materialized["wire"],
                "payload_digest": materialized["wire"]["digest"],
                "max_output_tokens": materialized["max_output_tokens"],
                "stream": materialized["stream"],
                "serializer_revision": materialized["serializer_revision"],
            },
        }
        prepared_effect = await self.ctx.uow.call(
            "agent.prepare-model-effect",
            {
                "authorization": {
                    "protocol": "forest.authorization-issue/1",
                    "ticket_id": ticket_id,
                    "issuance_operation_id": self.ctx.operations.stable(
                        f"authorization-issue:{attempt_id}"
                    ),
                    "operation_id": permit_operation_id,
                    "action": "provider.invoke",
                    "resource_kind": "bot",
                    "resource_id": bot_id,
                    "input_digest": input_digest,
                    "plan_digest": profile["capability_digest"],
                    "expires_at": expires_at,
                    "audience_component_id": profile["component_id"],
                    "audience_generation_id": profile["generation_id"],
                },
                "effect": {
                    "protocol": "forest.effect-command-prepare/1",
                    "effect_operation_id": effect_operation_id,
                    "input_digest": input_digest,
                    "effect_kind": "provider.model.dispatch",
                    "target_kind": "component.service",
                    "target_id": "provider.chat-completions/provider.model.dispatch",
                    "attempt_id": attempt_id,
                    "permit_operation_id": permit_operation_id,
                    "authorization_step_id": "authorization",
                    "authorization": {
                        "action": "provider.invoke",
                        "resource_kind": "bot",
                        "resource_id": bot_id,
                        "plan_digest": profile["capability_digest"],
                    },
                    "executor": {
                        "component_id": profile["component_id"],
                        "generation_id": profile["generation_id"],
                        "fence_kind": "runtime.graph",
                        "fence_id": graph_id,
                        "fence_epoch": graph_revision,
                    },
                    "deadline_at": expires_at,
                    "command": {
                        "value": dispatch,
                        "parents": [materialized["projection"], materialized["wire"]],
                    },
                },
                "agent": {
                    "owner": owner,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "effect_operation_ref": effect_operation_id,
                    "provider_service_id": "provider.model.dispatch",
                    "effect_step_id": "effect",
                },
            },
            operation_id=self.ctx.operations.stable(f"prepare-model-effect:{attempt_id}"),
            timeout=15,
        )
        if prepared_effect.get("authorization_ticket_id") != ticket_id:
            raise ForestError(
                "AGENT_EFFECT_AUTHORIZATION_CONFLICT",
                "Prepared Effect returned another authorization ticket",
                409,
            )
        await self.ctx.uow.call(
            "agent.handoff-model-effect",
            {
                "effect": {
                    "protocol": "forest.effect-handoff/1",
                    "permit": prepared_effect["permit"],
                    "handoff_operation_id": self.ctx.operations.stable(
                        f"effect-handoff:{attempt_id}"
                    ),
                },
                "agent": {
                    "owner": owner,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                },
            },
            operation_id=self.ctx.operations.stable(f"handoff-model-effect:{attempt_id}"),
            timeout=15,
        )
        try:
            raw_receipt = await self.ctx.services.call(
                "provider.model.dispatch",
                dispatch,
                operation_id=effect_operation_id,
                contract_major=2,
                timeout=120,
            )
            receipt = _receipt(
                raw_receipt,
                request_id=request_id,
                attempt_id=attempt_id,
                payload_digest=materialized["wire"]["digest"],
            )
        except Exception:
            receipt = {
                "status": "UNKNOWN",
                "request_id": request_id,
                "transport_attempt_id": attempt_id,
                "payload_digest": materialized["wire"]["digest"],
                "result": None,
                "usage": None,
                "error_code": "PROVIDER_DISPATCH_RESULT_UNKNOWN",
            }
        observed_state = {
            "RESPONDED": "SUCCEEDED",
            "NOT_SENT": "NOT_APPLIED",
            "UNKNOWN": "UNKNOWN",
        }[receipt["status"]]
        parents = [item for item in (receipt["result"], receipt["usage"]) if item is not None]
        payloads = [
            {"role": role, "payload": item}
            for role, item in (("RESULT", receipt["result"]), ("USAGE", receipt["usage"]))
            if item is not None
        ]
        observation_id = self.ctx.operations.stable(f"model-observation:{attempt_id}")
        await self.ctx.uow.call(
            "agent.observe-model-effect",
            {
                "effect": {
                    "protocol": "forest.effect-command-result/1",
                    "effect_operation_id": effect_operation_id,
                    "attempt_id": attempt_id,
                    "observation_operation_id": observation_id,
                    "status": observed_state,
                    "evidence_kind": "executor",
                    "evidence": {"value": receipt, "parents": parents},
                },
                "agent": {
                    "owner": owner,
                    "observation_operation_id": observation_id,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "observed_state": observed_state,
                    "evidence_kind": "EXECUTOR",
                    "effect_observation_ref": self.ctx.operations.stable(
                        f"effect-observation:{attempt_id}"
                    ),
                    "effect_step_id": "effect",
                    "evidence_digest": _digest(receipt),
                    "payloads": payloads,
                },
            },
            operation_id=self.ctx.operations.stable(f"observe-model-effect:{attempt_id}"),
            timeout=15,
        )
        if receipt["status"] != "RESPONDED":
            return {
                "status": receipt["status"],
                "request_id": request_id,
                "attempt_id": attempt_id,
                "error_code": receipt["error_code"],
                "result": None,
            }
        result = _provider_result(
            await self.ctx.payloads.read_json(receipt["result"]), receipt=receipt
        )
        return {
            "status": "RESPONDED",
            "request_id": request_id,
            "attempt_id": attempt_id,
            "error_code": None,
            "result": result,
        }

"""Kernel-owned runtime inspection tool contributed to the Agent directory."""

from __future__ import annotations

import json

from forest.errors import ForestError
from forest.runtime_modules import RuntimeAgentToolContribution
from forest.storage.payload_authority import PayloadSource

RUNTIME_STATUS_TOOL = RuntimeAgentToolContribution(
    name="runtime_status",
    service_id="agent.tool.runtime-status",
    version=1,
    declaration={
        "name": "runtime_status",
        "description": "Read the current Bot RuntimeGraph and module lifecycle states.",
        "parameters": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
        "strict": True,
    },
    approval_required=False,
    supports_query=False,
)


def runtime_status_tool_service(context):
    """Build the typed kernel coordinator behind ``runtime_status``."""

    runtime = context.participant("kernel.runtime").service
    payloads = context.participant("kernel.payload").service

    async def dispatch(request):
        if (
            not isinstance(request, dict)
            or request.get("protocol") != "forest.agent-tool-execute/1"
            or request.get("component_id") != "kernel.runtime"
            or request.get("service_id") != RUNTIME_STATUS_TOOL.service_id
            or request.get("arguments") != {}
        ):
            raise ForestError(
                "RUNTIME_STATUS_TOOL_INPUT_INVALID",
                "Runtime status tool command is invalid",
                422,
            )
        status = await runtime.status()
        projected = {
            "protocol": "forest.runtime-status-tool-result/1",
            "bot_id": status["bot_id"],
            "status": status["status"],
            "maintenance_epoch": status["maintenance_epoch"],
            "data_admission": status["data_admission"],
            "control_admission": status["control_admission"],
            "graph_id": status["graph_id"],
            "graph_revision": status["graph_revision"],
            "graph_digest": status["graph_digest"],
            "graph_state": status["graph_state"],
            "members": [
                {
                    "component_id": member["component_id"],
                    "generation_id": member["generation_id"],
                    "status": member["status"],
                    "config_revision": member["config_revision"],
                }
                for member in status["members"]
            ],
        }
        epoch = await payloads.epoch()
        saved = await payloads.put_bytes(
            owner_component_id="kernel.runtime",
            owner_kind="tool.result",
            owner_id=request["tool_call_id"],
            media_type="application/json",
            content=json.dumps(
                projected,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            sources=(
                PayloadSource(
                    "agent.runtime",
                    "agent.tool-call",
                    request["tool_call_id"],
                    1,
                    author_subject_id=request["authority_principal_ref"],
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        return {
            "protocol": "forest.agent-tool-receipt/1",
            "status": "SUCCEEDED",
            "tool_call_id": request["tool_call_id"],
            "effect_operation_id": request["effect_operation_id"],
            "effect_attempt_id": request["effect_attempt_id"],
            "result": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": "kernel.runtime",
                "owner_kind": "tool.result",
                "owner_id": request["tool_call_id"],
            },
            "error_code": None,
            "observation_ref": "runtime-status:" + request["tool_call_id"],
        }

    return dispatch

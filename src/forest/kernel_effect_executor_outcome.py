"""Read one executor result already committed to the Kernel service ledger."""

from __future__ import annotations

import json

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.service_operations import KernelServiceOperationLedger


def effect_executor_outcome_service(context):
    effects = context.participant("kernel.effect").runtime.service
    runtime = context.participant("kernel.runtime").service
    payloads = context.participant("kernel.payload").service
    ledger = KernelServiceOperationLedger(runtime, payloads)

    async def read(request):
        if (
            type(request) is not dict
            or set(request) != {"protocol", "effect_operation_id", "attempt_id"}
            or request["protocol"] != "forest.effect-executor-outcome-request/1"
            or not isinstance(request["effect_operation_id"], str)
            or not isinstance(request["attempt_id"], str)
        ):
            raise ForestError(
                "EFFECT_OUTCOME_INPUT_INVALID", "Executor outcome request is invalid", 422
            )
        loaded = await effects.load(request["effect_operation_id"])
        effect, attempt = loaded["effect"], loaded["attempt"]
        if (
            effect["producer_component_id"] != "agent.runtime"
            or effect["effect_kind"] != "provider.model.dispatch"
            or effect["target_kind"] != "component.service"
            or effect["target_id"]
                != "provider.chat-completions/provider.model.dispatch"
            or attempt is None
            or attempt["attempt_id"] != request["attempt_id"]
            or attempt["executor_component_id"] != "provider.chat-completions"
            or attempt["state"] not in {
                "DISPATCHING", "SUCCEEDED", "NOT_APPLIED", "UNKNOWN"
            }
            or effect["input_payload_id"] is None
        ):
            raise ForestError(
                "EFFECT_OUTCOME_BOUNDARY_CLOSED",
                "Effect is not one handed-off Agent model operation", 409,
            )
        async with context.store.pool.connection() as conn, conn.transaction():
            ref = await payloads.verify_in(
                conn,
                payload_id=effect["input_payload_id"],
                digest=effect["input_payload_digest"],
                owner_component_id="kernel.effect",
                owner_kind="effect.input",
                owner_id=effect["effect_operation_id"],
            )
            content = await payloads.read_bytes_in(conn, ref.payload_id)
        try:
            command = json.loads(content)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ForestError(
                "EFFECT_OUTCOME_CORRUPT", "Frozen Effect command is invalid", 500
            ) from exc
        if (
            type(command) is not dict
            or command.get("protocol") != "forest.provider-dispatch/1"
            or command.get("effect_operation_id") != effect["effect_operation_id"]
            or command.get("component_id") != attempt["executor_component_id"]
            or command.get("generation_id") != attempt["executor_generation_id"]
            or not isinstance(command.get("prepared"), dict)
            or command["prepared"].get("transport_attempt_id")
                != attempt["attempt_id"]
            or not isinstance(command["prepared"].get("request_id"), str)
            or not isinstance(command["prepared"].get("payload_digest"), str)
        ):
            raise ForestError(
                "EFFECT_OUTCOME_CORRUPT", "Frozen Effect command identity differs", 500
            )
        result = await ledger.inspect(
            component_id=attempt["executor_component_id"],
            operation_id=effect["effect_operation_id"],
            service_name="provider.model.dispatch",
            input_digest=digest_json({
                "service_id": "provider.model.dispatch", "input": command,
            }),
            generation_id=attempt["executor_generation_id"],
            causal_root_kind=effect["root_operation_kind"],
            causal_root_id=effect["root_operation_id"],
        )
        return {
            "protocol": "forest.effect-executor-outcome/1",
            "effect_operation_id": effect["effect_operation_id"],
            "attempt_id": attempt["attempt_id"],
            "request_id": command["prepared"]["request_id"],
            "payload_digest": command["prepared"]["payload_digest"],
            "state": result["state"],
            "receipt": result["result"],
        }

    return read

"""Read a committed executor service outcome through a producer-bound Effect."""

from __future__ import annotations

import json

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_contracts import RoutedServiceEndpoint
from forest.storage.service_operations import KernelServiceOperationLedger


async def _load_handed_off_service_command(effects, payloads, request, producer_component_id):
    if (
        type(request) is not dict
        or set(request) != {"protocol", "effect_operation_id", "attempt_id"}
        or request["protocol"] != "forest.effect-executor-outcome-request/2"
        or not isinstance(request["effect_operation_id"], str)
        or not isinstance(request["attempt_id"], str)
    ):
        raise ForestError(
            "EFFECT_OUTCOME_INPUT_INVALID", "Executor outcome request is invalid", 422
        )
    loaded = await effects.load(request["effect_operation_id"])
    effect, attempt = loaded["effect"], loaded["attempt"]
    target = effect["target_id"]
    target_component, separator, service_name = (
        target.partition("/") if isinstance(target, str) else ("", "", "")
    )
    if (
        effect["producer_component_id"] != producer_component_id
        or effect["target_kind"] != "component.service"
        or not separator
        or not service_name
        or attempt is None
        or attempt["attempt_id"] != request["attempt_id"]
        or attempt["executor_component_id"] != target_component
        or attempt["state"] not in {
            "DISPATCHING", "SUCCEEDED", "NOT_APPLIED", "UNKNOWN"
        }
        or effect["input_payload_id"] is None
    ):
        raise ForestError(
            "EFFECT_OUTCOME_BOUNDARY_CLOSED",
            "Effect is not a handed-off service operation owned by this caller", 409,
        )
    async with payloads.store.pool.connection() as conn, conn.transaction():
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
            "EFFECT_OUTCOME_CORRUPT", "Frozen service command is invalid", 500
        ) from exc
    if type(command) is not dict:
        raise ForestError(
            "EFFECT_OUTCOME_CORRUPT", "Frozen service command is invalid", 500
        )
    return effect, attempt, service_name, command


def effect_executor_outcome_service(context):
    effects = context.participant("kernel.effect").runtime.service
    runtime = context.participant("kernel.runtime").service
    payloads = context.participant("kernel.payload").service
    ledger = KernelServiceOperationLedger(runtime, payloads)

    async def read(route, _invocation, *args, **kwargs):
        if len(args) != 1 or kwargs:
            raise ForestError(
                "EFFECT_OUTCOME_INPUT_INVALID", "Executor outcome requires one input", 422
            )
        effect, attempt, service_name, command = await _load_handed_off_service_command(
            effects, payloads, args[0], route.caller_component_id
        )
        result = await ledger.inspect(
            component_id=attempt["executor_component_id"],
            operation_id=effect["effect_operation_id"],
            service_name=service_name,
            input_digest=digest_json({
                "service_id": service_name, "input": command,
            }),
            generation_id=attempt["executor_generation_id"],
            causal_root_kind=effect["root_operation_kind"],
            causal_root_id=effect["root_operation_id"],
        )
        return {
            "protocol": "forest.effect-executor-outcome/2",
            "effect_operation_id": effect["effect_operation_id"],
            "attempt_id": attempt["attempt_id"],
            "state": result["state"],
            "receipt": result["result"],
        }

    return RoutedServiceEndpoint(read)

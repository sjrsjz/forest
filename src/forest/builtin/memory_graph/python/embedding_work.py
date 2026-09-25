"""Memory-owned embedding Effect orchestration.

The Provider owns materialization and HTTP. Memory owns the durable claim,
budget, Effect boundary, and facet state. Recovery never repeats HTTP.
"""

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta

from forest.errors import ForestError


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _unknown(request_id, attempt_id, prepared, code):
    return {
        "status": "UNKNOWN",
        "request_id": request_id,
        "transport_attempt_id": attempt_id,
        "payload_digest": prepared["payload_digest"],
        "space_id": prepared["space_id"],
        "result": None,
        "usage": None,
        "error_code": code,
        "http_status": None,
    }


def _vectors(result, receipt, facets, dimensions):
    if (
        not isinstance(result, dict)
        or result.get("protocol") != "forest.embedding-result/1"
        or result.get("status") != "RESPONDED"
        or any(result.get(field) != receipt[field] for field in (
            "request_id", "transport_attempt_id", "payload_digest", "space_id"
        ))
        or result.get("error_code") is not None
        or result.get("usage") != receipt["usage"]
        or not isinstance(result.get("vectors"), list)
        or len(result["vectors"]) != len(facets)
        or not isinstance(result.get("usage"), dict)
        or type(result["usage"].get("total_tokens")) is not int
        or result["usage"]["total_tokens"] < 0
    ):
        raise ForestError(
            "MEMORY_EMBEDDING_RESULT_INVALID",
            "Embedding result differs from the dispatched batch", 502,
        )
    vectors = []
    for index, (vector, facet) in enumerate(zip(result["vectors"], facets, strict=True)):
        values = vector.get("values") if isinstance(vector, dict) else None
        if (
            not isinstance(vector, dict)
            or vector.get("index") != index
            or vector.get("facet_id") != facet["facet_id"]
            or not isinstance(values, list)
            or len(values) != dimensions
            or any(type(value) not in (int, float) or not math.isfinite(value)
                   for value in values)
        ):
            raise ForestError(
                "MEMORY_EMBEDDING_VECTOR_INVALID",
                "Embedding vector differs from the claimed facet", 502,
            )
        vectors.append({
            "facet_id": facet["facet_id"],
            "index": index,
            "vector_digest": _digest(values),
        })
    return vectors, result["usage"]["total_tokens"]


async def _observe(ctx, work, receipt, vectors, charged_tokens):
    attempt_id = work["attempt_id"]
    status = {
        "RESPONDED": "SUCCEEDED",
        "NOT_SENT": "NOT_APPLIED",
        "UNKNOWN": "UNKNOWN",
        "REJECTED": "UNKNOWN",
    }[receipt["status"]]
    if status != "SUCCEEDED":
        receipt = {**receipt, "result": None}
    observation_id = ctx.operations.stable("embedding-observation:" + attempt_id)
    result = receipt["result"] if status == "SUCCEEDED" else None
    return await ctx.uow.call(
        "memory.observe-embedding-effect",
        {
            "effect": {
                "protocol": "forest.effect-command-result/1",
                "effect_operation_id": work["effect_operation_id"],
                "attempt_id": attempt_id,
                "observation_operation_id": observation_id,
                "status": status,
                "evidence_kind": "executor",
                "evidence": {
                    "value": receipt,
                    "parents": [] if result is None else [result],
                },
            },
            "budget": {
                "protocol": "forest.memory-embedding-budget-settle/1",
                "reservation_id": work["reservation_id"],
                "state": {
                    "SUCCEEDED": "CHARGED",
                    "NOT_APPLIED": "RELEASED",
                    "UNKNOWN": "UNKNOWN",
                }[status],
                "charged_tokens": charged_tokens if status == "SUCCEEDED" else None,
                "error_code": None if status == "SUCCEEDED" else (
                    receipt.get("error_code") or "MEMORY_EMBEDDING_RESULT_UNKNOWN"
                ),
            },
            "memory": {
                "protocol": "forest.memory-embedding-observe/1",
                "claim_id": work["claim_id"],
                "attempt_id": attempt_id,
                "effect_operation_id": work["effect_operation_id"],
                "observation_operation_id": observation_id,
                "reservation_id": work["reservation_id"],
                "observed_state": status,
                "evidence_digest": _digest(receipt),
                "result": result,
                "vectors": vectors if status == "SUCCEEDED" else [],
                "charged_tokens": charged_tokens if status == "SUCCEEDED" else None,
                "error_code": None if status == "SUCCEEDED" else (
                    receipt.get("error_code") or "MEMORY_EMBEDDING_RESULT_UNKNOWN"
                ),
            },
        },
        operation_id=ctx.operations.stable("embedding-observe-transaction:" + attempt_id),
        timeout=25,
    )


async def _abandon(ctx, work):
    abandonment_id = ctx.operations.stable("embedding-abandon:" + work["attempt_id"])
    reason = "MEMORY_EMBEDDING_NOT_HANDED_OFF"
    return await ctx.uow.call(
        "memory.abandon-embedding-effect",
        {
            "effect": {
                "protocol": "forest.effect-abandon/1",
                "effect_operation_id": work["effect_operation_id"],
                "attempt_id": work["attempt_id"],
                "abandonment_operation_id": abandonment_id,
                "reason_code": reason,
                "cancel_effect": False,
            },
            "budget": {
                "protocol": "forest.memory-embedding-budget-settle/1",
                "reservation_id": work["reservation_id"],
                "state": "RELEASED",
                "charged_tokens": None,
                "error_code": reason,
            },
            "memory": {
                "protocol": "forest.memory-embedding-abandon/1",
                "claim_id": work["claim_id"],
                "attempt_id": work["attempt_id"],
                "effect_operation_id": work["effect_operation_id"],
                "abandonment_operation_id": abandonment_id,
                "reservation_id": work["reservation_id"],
                "reason_code": reason,
            },
        },
        operation_id=ctx.operations.stable("embedding-abandon-transaction:" + work["attempt_id"]),
        timeout=20,
    )


async def dispatch(ctx, task, profile):
    claim = _plain(task["payload"])
    claim_id = claim["claim_id"]
    request_id = ctx.operations.stable("embedding-request:" + claim_id)
    attempt_id = ctx.operations.stable("embedding-attempt:" + claim_id)
    effect_id = ctx.operations.stable("embedding-effect:" + claim_id)
    reservation_id = ctx.operations.stable("embedding-reservation:" + claim_id)
    work = {
        "claim_id": claim_id,
        "attempt_id": attempt_id,
        "effect_operation_id": effect_id,
        "reservation_id": reservation_id,
    }
    bound = await ctx.services.call(
        "embedding.profile.bind",
        {"protocol": "forest.embedding-bind-request/1", "request_id": request_id},
        operation_id=ctx.operations.stable("embedding-profile:" + claim_id),
        timeout=10,
    )
    expected = {
        key: profile["embedding"][key]
        for key in ("deployment_id", "model_revision", "model", "dimensions")
    }
    if (
        bound.get("protocol") != "forest.embedding-profile/1"
        or bound.get("request_id") != request_id
        or bound.get("component_id") != "embedding.multimodal"
        or _plain(bound.get("profile")) != expected
        or bound.get("space_id") != claim["space_id"]
    ):
        raise ForestError(
            "MEMORY_EMBEDDING_PROFILE_MISMATCH",
            "Claim space differs from the bound embedding Generation", 409,
        )
    materialized = await ctx.services.call(
        "embedding.request.materialize",
        {
            "protocol": "forest.embedding-materialize/1",
            "request_id": request_id,
            "transport_attempt_id": attempt_id,
            "component_id": bound["component_id"],
            "generation_id": bound["generation_id"],
            "profile_ref": bound["profile_ref"],
            "space_id": claim["space_id"],
            "purpose": "DOCUMENT",
            "facets": claim["facets"],
        },
        operation_id=ctx.operations.stable("embedding-materialize:" + claim_id),
        timeout=120,
    )
    if (
        materialized.get("protocol") != "forest.embedding-materialized/1"
        or any(materialized.get(key) != bound[key] for key in (
            "component_id", "generation_id", "profile_ref", "space_id"
        ))
        or materialized.get("request_id") != request_id
        or materialized.get("transport_attempt_id") != attempt_id
    ):
        raise ForestError(
            "MEMORY_EMBEDDING_MATERIALIZATION_MISMATCH",
            "Materialized batch belongs to another embedding Generation", 409,
        )
    prepared = _plain(materialized["prepared"])
    ticket_id = ctx.operations.stable("embedding-ticket:" + claim_id)
    permit_id = ctx.operations.stable("embedding-permit:" + claim_id)
    deadline = (datetime.now(UTC) + timedelta(seconds=180)).isoformat()
    plan_digest = _digest({
        "profile_ref": bound["profile_ref"],
        "space_id": claim["space_id"],
        "payload_digest": prepared["payload_digest"],
    })
    dispatch_payload = {
        "protocol": "forest.embedding-dispatch/2",
        "effect_operation_id": effect_id,
        "authorization_ticket_id": ticket_id,
        "component_id": bound["component_id"],
        "generation_id": bound["generation_id"],
        "profile_ref": bound["profile_ref"],
        "prepared": prepared,
    }
    budget = profile["embedding_budget"]
    effect = await ctx.uow.call(
        "memory.prepare-embedding-effect",
        {
            "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
            "budget": {
                "protocol": "forest.memory-embedding-budget-reserve/1",
                "reservation_id": reservation_id,
                "batch_ref": claim_id,
                "policy_revision": budget["policy_revision"],
                "request_limit": budget["daily_request_limit"],
                "token_limit": budget["daily_token_limit"],
                "reserved_tokens": budget["reservation_tokens"],
            },
            "authorization": {
                "protocol": "forest.authorization-issue/1",
                "ticket_id": ticket_id,
                "issuance_operation_id": ctx.operations.stable("embedding-ticket-issue:" + claim_id),
                "operation_id": permit_id,
                "action": "embedding.invoke",
                "resource_kind": "bot",
                "resource_id": ctx.identity["bot_id"],
                "input_digest": plan_digest,
                "plan_digest": plan_digest,
                "expires_at": deadline,
                "audience_component_id": bound["component_id"],
                "audience_generation_id": bound["generation_id"],
            },
            "effect": {
                "protocol": "forest.effect-command-prepare/1",
                "effect_operation_id": effect_id,
                "input_digest": plan_digest,
                "effect_kind": "embedding.multimodal.http",
                "target_kind": "component.service",
                "target_id": "embedding.multimodal/embedding.encode",
                "attempt_id": attempt_id,
                "permit_operation_id": permit_id,
                "authorization_step_id": "authorization",
                "authorization": {
                    "action": "embedding.invoke",
                    "resource_kind": "bot",
                    "resource_id": ctx.identity["bot_id"],
                    "plan_digest": plan_digest,
                },
                "executor": {
                    "component_id": bound["component_id"],
                    "generation_id": bound["generation_id"],
                    "fence_kind": "runtime.graph",
                    "fence_id": ctx.identity["graph_id"],
                    "fence_epoch": ctx.identity["graph_revision"],
                },
                "deadline_at": deadline,
                "command": {
                    "value": dispatch_payload,
                    "parents": [prepared["body"], prepared["manifest"]],
                },
            },
            "memory": {
                "protocol": "forest.memory-embedding-attempt-prepare/1",
                **work,
                "space_id": claim["space_id"],
            },
        },
        operation_id=ctx.operations.stable("embedding-prepare-transaction:" + claim_id),
        timeout=20,
    )
    if (
        effect.get("state") != "PREPARED"
        or effect.get("attempt_id") != attempt_id
        or effect.get("authorization_ticket_id") != ticket_id
    ):
        raise ForestError(
            "MEMORY_EMBEDDING_EFFECT_CONFLICT",
            "Prepared embedding Effect changed before handoff", 409,
        )
    handoff_id = ctx.operations.stable("embedding-handoff:" + claim_id)
    await ctx.uow.call(
        "memory.handoff-embedding-effect",
        {
            "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
            "effect": {
                "protocol": "forest.effect-handoff/1",
                "permit": effect["permit"],
                "handoff_operation_id": handoff_id,
            },
            "memory": {
                "protocol": "forest.memory-embedding-handoff/1",
                "claim_id": claim_id,
                "attempt_id": attempt_id,
                "effect_operation_id": effect_id,
                "handoff_operation_id": handoff_id,
            },
        },
        operation_id=ctx.operations.stable("embedding-handoff-transaction:" + claim_id),
        timeout=20,
    )
    try:
        receipt = _plain(await ctx.services.call(
            "embedding.encode", dispatch_payload,
            operation_id=effect_id, contract_major=2, timeout=150,
        ))
        if (
            receipt.get("request_id") != request_id
            or receipt.get("transport_attempt_id") != attempt_id
            or receipt.get("payload_digest") != prepared["payload_digest"]
            or receipt.get("space_id") != claim["space_id"]
            or receipt.get("status") not in ("RESPONDED", "REJECTED", "NOT_SENT", "UNKNOWN")
        ):
            raise ForestError(
                "MEMORY_EMBEDDING_RECEIPT_INVALID",
                "Embedding receipt belongs to another request", 502,
            )
        if receipt["status"] == "RESPONDED":
            result = _plain(await ctx.payloads.read_json(receipt["result"]))
            vectors, charged_tokens = _vectors(
                result, receipt, claim["facets"], expected["dimensions"]
            )
        else:
            vectors, charged_tokens = [], None
    except Exception:
        receipt = _unknown(
            request_id, attempt_id, prepared, "MEMORY_EMBEDDING_DISPATCH_UNKNOWN"
        )
        vectors, charged_tokens = [], None
    await _observe(ctx, work, receipt, vectors, charged_tokens)
    return {
        "claim_id": claim_id,
        "state": {
            "RESPONDED": "SUCCEEDED",
            "NOT_SENT": "NOT_SENT",
            "REJECTED": "UNKNOWN",
            "UNKNOWN": "UNKNOWN",
        }[receipt["status"]],
    }


async def recover(ctx, task):
    work = _plain(task["payload"])
    if work["mode"] == "ABANDON":
        result = await _abandon(ctx, work)
        return {"claim_id": work["claim_id"], "state": result["state"]}
    if work["mode"] != "OBSERVE_UNKNOWN":
        raise ForestError(
            "MEMORY_EMBEDDING_RECOVERY_INVALID", "Recovery mode is unsupported", 422,
        )
    # No HTTP request is made on a recovered DISPATCHED attempt.
    receipt = {
        "status": "UNKNOWN",
        "attempt_id": work["attempt_id"],
        "result": None,
        "error_code": "MEMORY_EMBEDDING_RESULT_UNKNOWN_AFTER_RESTART",
    }
    result = await _observe(ctx, work, receipt, [], None)
    return {"claim_id": work["claim_id"], "state": result["state"]}

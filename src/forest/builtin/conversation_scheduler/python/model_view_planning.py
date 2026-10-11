"""Conversation-owned selection and publication of one immutable model view."""

from __future__ import annotations

import hashlib
import json

from model_view_selection import ViewCost, ViewSelectionError, select_context_units

from forest.errors import ForestError


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def exact(reference: dict) -> dict:
    return {key: reference[key] for key in (
        "payload_id", "digest", "owner_component_id", "owner_kind", "owner_id",
    )}


def estimated_tokens(value: object, per_token: int) -> int:
    size = len(canonical_bytes(value))
    return (size + per_token - 1) // per_token


def fixed_cost(quote: dict, *, per_token: int, overhead: int, bot_id: str) -> ViewCost:
    """Charge Agent causal input and Memory evidence before optional Context."""
    if quote.get("protocol") != "forest.model-view-quote/1" or quote.get("bot_id") != bot_id:
        raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Model view quote identity is invalid", 409)
    history = quote.get("fixed_units")
    if not isinstance(history, list) or not isinstance(quote.get("preset_context"), list):
        raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Model view fixed sources are invalid", 409)
    output = quote.get("output_tokens")
    if type(output) is not int or not 1 <= output <= 1_048_576:
        raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Model output reservation is invalid", 409)
    evidence = quote.get("evidence")
    if evidence is not None:
        if not isinstance(evidence, dict) or evidence.get("protocol") != "forest.memory-evidence/3" or evidence.get("bot_id") != bot_id:
            raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Memory evidence identity is invalid", 409)
    source = {
        "core_prompt": quote.get("core_prompt"),
        "preset_context": quote["preset_context"],
        "tools": quote.get("tools"),
        "fixed_units": history,
        "evidence": evidence,
    }
    if not isinstance(source["core_prompt"], str) or not isinstance(source["tools"], list):
        raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Model instructions are invalid", 409)
    media = decoded = 0
    if evidence is not None:
        for unit in evidence.get("evidence", ()):
            for part in unit.get("parts", ()):
                if part.get("kind") == "image" and part.get("projection_state") == "READY":
                    media += part["byte_length"]
                    decoded += part["width"] * part["height"] * 4
    return ViewCost(
        output + estimated_tokens(source, per_token) + len(history) * overhead,
        media, decoded,
    )


async def publish_model_view(scheduler, payload: dict, ctx) -> dict:
    actor = payload["actor"]
    run_ref = payload["run_ref"]
    step_seq = payload["step_seq"]
    quote_ref = exact(payload["quote"])
    if quote_ref["owner_component_id"] != "agent.runtime" or quote_ref["owner_kind"] != "model.view.quote":
        raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Quote owner is invalid", 409)
    quote = await ctx.payloads.read_json(quote_ref)
    expected = {
        "bot_id": ctx.identity["bot_id"],
        "conversation_id": actor["conversation_id"],
        "session_ref": payload["session_ref"],
        "run_ref": run_ref,
        "step_seq": step_seq,
    }
    if not isinstance(quote, dict) or any(quote.get(key) != value for key, value in expected.items()):
        raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Quote differs from its Run", 409)
    previous = await ctx.queries.call(
        "conversation.model-view.prior",
        {"actor": actor, "session_ref": payload["session_ref"], "run_ref": run_ref},
    )
    if previous["found"] and previous["step_seq"] == step_seq:
        prior_plan = await ctx.payloads.read_json(previous["plan"])
        if prior_plan.get("quote") != quote_ref:
            raise ForestError("CONTEXT_MODEL_VIEW_PLAN_STEP_CONFLICT", "Model step quote changed", 409)
        return {"plan": previous["plan"], "step_seq": step_seq,
                "source_set_digest": previous["source_set_digest"]}
    bound = await scheduler._bound_view(ctx, actor, payload["session_ref"], run_ref)
    profile = bound["profile"]
    pages = []
    offset = 0
    metadata = None
    while True:
        page = await ctx.queries.call(
            "conversation.context-page",
            {"actor": actor, "run_ref": run_ref, "offset": offset, "limit": 32},
            contract_major=4,
        )
        facts = {key: page[key] for key in (
            "conversation_id", "run_ref", "context_revision", "context_view_revision",
            "context_applied_seq", "media_cost_revision", "total_units",
            "context_view_unit_count", "context_view_media_bytes",
            "context_view_decoded_bytes", "claimed_trigger_count",
        )}
        if metadata is None:
            metadata = facts
        elif facts != metadata:
            raise ForestError("CONTEXT_MODEL_VIEW_PLAN_FENCE_CHANGED", "Context page fence changed", 409)
        pages.extend(page["units"])
        next_offset = page["next_offset"]
        if next_offset is None:
            break
        if next_offset <= offset:
            raise ForestError("CONTEXT_MODEL_VIEW_PLAN_INVALID", "Context page did not advance", 409)
        offset = next_offset
    if len(pages) != metadata["total_units"] or metadata["context_view_unit_count"] != len(pages):
        raise ForestError("CONTEXT_MODEL_VIEW_PLAN_FENCE_CHANGED", "Context source count changed", 409)
    for key in ("context_revision", "context_view_revision", "context_applied_seq", "media_cost_revision"):
        if quote.get(key) != metadata[key]:
            raise ForestError("CONTEXT_MODEL_VIEW_PLAN_FENCE_CHANGED", "Quote Context fence changed", 409)
    required = [unit["unit_id"] for unit in pages if unit["required_for_run"]]
    if len(required) != metadata["claimed_trigger_count"]:
        raise ForestError("CONTEXT_MODEL_VIEW_PLAN_FENCE_CHANGED", "Required trigger set changed", 409)
    previous_ids = [unit["unit_id"] for unit in pages[:previous["considered_count"]]] if previous["found"] else []
    if previous["found"] and previous["source_set_digest"] != (
        "sha256:" + hashlib.sha256("\x1f".join(previous_ids).encode()).hexdigest()
    ):
        raise ForestError("CONTEXT_MODEL_VIEW_PLAN_PARENT_CHANGED", "Previous source set changed", 409)
    selected_ids = [unit_id for unit_id, bit in zip(previous_ids, previous["selected_bitmap"], strict=True) if bit == "1"] if previous["found"] else []
    evidence_ref = quote.get("evidence_ref")
    if evidence_ref is not None:
        if await ctx.payloads.read_json(evidence_ref) != quote.get("evidence"):
            raise ForestError("CONTEXT_MODEL_VIEW_QUOTE_INVALID", "Memory evidence differs from its reference", 409)
    fixed = fixed_cost(
        quote, per_token=profile["utf8_bytes_per_estimated_token"],
        overhead=profile["unit_overhead_estimated_tokens"], bot_id=ctx.identity["bot_id"],
    )
    budget = ViewCost(
        profile["model_view_budget_tokens"], profile["model_view_media_bytes"],
        profile["model_view_decoded_bytes"],
    )
    try:
        selection = select_context_units(
            pages, required_unit_ids=required,
            previous_selected_unit_ids=selected_ids,
            previous_considered_unit_ids=previous_ids, fixed_cost=fixed,
            budget=budget, bytes_per_token=profile["utf8_bytes_per_estimated_token"],
            unit_overhead=profile["unit_overhead_estimated_tokens"],
        )
    except ViewSelectionError as exc:
        raise ForestError("CONTEXT_MODEL_VIEW_BUDGET_EXCEEDED", str(exc), 413) from exc
    document = {
        "protocol": "forest.model-view-plan/1", **expected,
        "quote": quote_ref,
        "context_snapshot": quote["context_snapshot"],
        "profile_digest": bound["profile_digest"],
        "context_revision": metadata["context_revision"],
        "context_view_revision": metadata["context_view_revision"],
        "context_applied_seq": metadata["context_applied_seq"],
        "media_cost_revision": metadata["media_cost_revision"],
        "source_set_digest": selection.source_set_digest,
        "selected_bitmap": selection.selected_bitmap,
        "selected_unit_ids": list(selection.selected_unit_ids),
        "omitted_unit_ids": list(selection.omitted_unit_ids),
        "required_unit_ids": required,
        "prior_plan": previous["plan"] if previous["found"] else None,
        "fixed_cost": vars_cost(fixed),
        "selected_cost": vars_cost(selection.selected_cost),
        "total_cost": vars_cost(selection.total_cost),
        "budget": vars_cost(budget),
    }
    owner_id = f"model-view:{run_ref}:step:{step_seq}:{digest(document)[7:]}"
    parents = [quote_ref, quote["context_snapshot"]]
    if previous["found"]:
        parents.append(previous["plan"])
    saved = await ctx.payloads.put_json(
        document, owner_kind="model.view.plan", owner_id=owner_id, parents=parents,
    )
    return await ctx.commands.call(
        "conversation.model-view.publish",
        {"actor": actor, "session_ref": payload["session_ref"], "run_ref": run_ref,
         "step_seq": step_seq, "context_revision": metadata["context_revision"],
         "context_view_revision": metadata["context_view_revision"],
         "media_cost_revision": metadata["media_cost_revision"],
         "context_applied_seq": metadata["context_applied_seq"],
         "profile_digest": bound["profile_digest"],
         "source_set_digest": selection.source_set_digest,
         "selected_bitmap": selection.selected_bitmap,
         "prior_plan_digest": previous["plan"]["digest"] if previous["found"] else None,
         "quote_owner_id": quote_ref["owner_id"], "plan_owner_id": owner_id,
         "quote": quote_ref, "plan": exact(saved)},
        operation_id=ctx.operations.stable(f"model-view:{run_ref}:step:{step_seq}:{digest(document)[7:]}"),
    )


def vars_cost(cost: ViewCost) -> dict:
    return {"tokens": cost.tokens, "media_bytes": cost.media_bytes,
            "decoded_bytes": cost.decoded_bytes}

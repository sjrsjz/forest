"""Agent's immutable fixed-input quote and consumption of a Conversation plan."""

from __future__ import annotations

import hashlib
import json

from agent_protocol import compose_core_instructions

from forest.errors import ForestError


def _digest(value: object) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _exact(reference: dict) -> dict:
    return {key: reference[key] for key in (
        "payload_id", "digest", "owner_component_id", "owner_kind", "owner_id",
    )}


async def quote_and_read_plan(
    ctx, *, request: dict, run: dict, step_seq: int, raw_snapshot: dict,
    fixed_view: dict, preset_ref: dict, preset_context: list[str],
    tools: list[dict], evidence_ref: dict | None, output_tokens: int,
    input_modalities: tuple[str, ...], model_capability_digest: str,
) -> dict:
    """Quote causal sources; only Conversation chooses Context membership."""
    active_ids = {
        unit["unit_id"] for unit in raw_snapshot["units"]
        if unit.get("source_kind") != "context.archive"
    }
    fixed_units = fixed_view["units"]
    if any(unit["unit_id"] in active_ids for unit in fixed_units):
        raise ForestError("AGENT_MODEL_VIEW_QUOTE_INVALID", "Fixed quote contains Context history", 409)
    evidence = await ctx.payloads.read_json(evidence_ref) if evidence_ref else None
    declarations = [{"type": "function", "function": tool} for tool in tools]
    quote = {
        "protocol": "forest.model-view-quote/1",
        "bot_id": ctx.identity["bot_id"],
        "conversation_id": run["conversation_ref"],
        "session_ref": request["session_ref"],
        "run_ref": run["run_id"],
        "step_seq": step_seq,
        "context_revision": raw_snapshot["context_revision"],
        "context_view_revision": raw_snapshot["context_view_revision"],
        "context_applied_seq": raw_snapshot["context_applied_seq"],
        "media_cost_revision": raw_snapshot["media_cost_revision"],
        "context_snapshot": fixed_view["context_snapshot_ref"],
        "input_revision": fixed_view["input_revision"],
        "core_prompt": compose_core_instructions(declarations),
        "preset_context": preset_context,
        "tools": tools,
        "fixed_units": fixed_units,
        "evidence_ref": _exact(evidence_ref) if evidence_ref else None,
        "evidence": evidence,
        "output_tokens": output_tokens,
        "input_modalities": list(input_modalities),
        "model_capability_digest": model_capability_digest,
    }
    owner_id = f"model-view:{run['run_id']}:step:{step_seq}:{_digest(quote)}"
    parents = [fixed_view["context_snapshot_ref"], preset_ref]
    if evidence_ref:
        parents.append(evidence_ref)
    stored = await ctx.payloads.put_json(
        quote, owner_kind="model.view.quote", owner_id=owner_id, parents=parents,
    )
    quote_ref = _exact(stored)
    published = await ctx.services.call(
        "conversation.model-view.plan",
        {"actor": request["actor"], "session_ref": request["session_ref"],
         "run_ref": run["run_id"], "step_seq": step_seq, "quote": quote_ref},
        operation_id=ctx.operations.stable(
            f"model-view-plan:{run['run_id']}:{step_seq}:{_digest(quote)}"
        ),
        timeout=120,
    )
    plan_ref = _exact(published["plan"])
    plan = await ctx.payloads.read_json(plan_ref)
    if (
        not isinstance(plan, dict)
        or plan.get("protocol") != "forest.model-view-plan/1"
        or plan.get("quote") != quote_ref
        or plan.get("context_snapshot") != fixed_view["context_snapshot_ref"]
        or plan.get("run_ref") != run["run_id"]
        or plan.get("step_seq") != step_seq
        or plan.get("context_revision") != raw_snapshot["context_revision"]
        or plan.get("context_view_revision") != raw_snapshot["context_view_revision"]
        or plan.get("media_cost_revision") != raw_snapshot["media_cost_revision"]
        or plan.get("context_applied_seq") != raw_snapshot["context_applied_seq"]
    ):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Conversation plan differs from the quoted model input", 409)
    selected = plan.get("selected_unit_ids")
    omitted = plan.get("omitted_unit_ids")
    if not isinstance(selected, list) or not isinstance(omitted, list):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Conversation membership is invalid", 409)
    if len(set(selected)) != len(selected) or len(set(omitted)) != len(omitted):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Conversation membership repeats a source", 409)
    if set(selected) | set(omitted) != active_ids or set(selected) & set(omitted):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Conversation membership differs from the snapshot", 409)
    if not set(fixed_view["required_unit_ids"]) <= set(selected):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Required triggers are absent", 409)
    evidence_ids = ["evidence:" + row["source_unit_id"] for row in evidence["evidence"]] if evidence else []
    selected_evidence = plan.get("selected_evidence_unit_ids")
    omitted_evidence = plan.get("omitted_evidence_unit_ids")
    if (
        not isinstance(selected_evidence, list) or not isinstance(omitted_evidence, list)
        or len(set(evidence_ids)) != len(evidence_ids)
        or len(set(selected_evidence)) != len(selected_evidence)
        or len(set(omitted_evidence)) != len(omitted_evidence)
        or set(selected_evidence) & set(omitted_evidence)
        or [unit_id for unit_id in evidence_ids if unit_id in selected_evidence] != selected_evidence
        or set(selected_evidence) | set(omitted_evidence) != set(evidence_ids)
    ):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Memory membership differs from the quote", 409)
    return {
        "plan_ref": plan_ref,
        "quote_ref": quote_ref,
        "selected_unit_ids": selected,
        "omitted_unit_ids": omitted,
        "selected_evidence_unit_ids": selected_evidence,
        "omitted_evidence_unit_ids": omitted_evidence,
    }


async def verify_model_view(
    ctx, *, snapshot: dict, preset_context: list[str], tools: list[dict],
    evidence_ref: dict | None, output_tokens: int,
    input_modalities: tuple[str, ...], model_capability_digest: str,
) -> None:
    """Reject a model call whose inputs drifted after the selected plan was published."""
    plan_ref = snapshot.get("model_view_plan_ref")
    quote_ref = snapshot.get("model_view_quote_ref")
    if not isinstance(plan_ref, dict) or not isinstance(quote_ref, dict):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Primary model call has no published view", 409)
    plan = await ctx.payloads.read_json(_exact(plan_ref))
    quote = await ctx.payloads.read_json(_exact(quote_ref))
    selected = snapshot.get("model_view_selected_unit_ids")
    omitted = snapshot.get("model_view_omitted_unit_ids")
    selected_evidence = snapshot.get("model_view_selected_evidence_unit_ids")
    omitted_evidence = snapshot.get("model_view_omitted_evidence_unit_ids")
    if (
        plan.get("protocol") != "forest.model-view-plan/1"
        or quote.get("protocol") != "forest.model-view-quote/1"
        or plan.get("quote") != quote_ref
        or plan.get("selected_unit_ids") != selected
        or plan.get("omitted_unit_ids") != omitted
        or plan.get("selected_evidence_unit_ids") != selected_evidence
        or plan.get("omitted_evidence_unit_ids") != omitted_evidence
        or quote.get("bot_id") != snapshot.get("bot_id")
        or quote.get("run_ref") != snapshot.get("run_id")
        or quote.get("input_revision") != snapshot.get("input_revision")
        or quote.get("context_snapshot") != snapshot.get("context_snapshot_ref")
        or quote.get("preset_context") != preset_context
        or quote.get("tools") != tools
        or quote.get("output_tokens") != output_tokens
        or quote.get("input_modalities") != list(input_modalities)
        or quote.get("model_capability_digest") != model_capability_digest
        or quote.get("evidence_ref") != evidence_ref
        or quote.get("core_prompt") != compose_core_instructions(
            [{"type": "function", "function": tool} for tool in tools]
        )
    ):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Model call differs from its published quote", 409)
    evidence_ids = ["evidence:" + row["source_unit_id"] for row in quote["evidence"]["evidence"]] if evidence_ref else []
    if (
        not isinstance(selected_evidence, list) or not isinstance(omitted_evidence, list)
        or len(set(evidence_ids)) != len(evidence_ids)
        or len(set(selected_evidence)) != len(selected_evidence)
        or len(set(omitted_evidence)) != len(omitted_evidence)
        or set(selected_evidence) & set(omitted_evidence)
        or [unit_id for unit_id in evidence_ids if unit_id in selected_evidence] != selected_evidence
        or set(selected_evidence) | set(omitted_evidence) != set(evidence_ids)
    ):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Memory members differ from the published plan", 409)
    units = snapshot.get("units")
    if not isinstance(units, list) or not isinstance(selected, list) or not isinstance(omitted, list):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Selected model view is invalid", 409)
    selected_set = set(selected)
    omitted_set = set(omitted)
    if (
        len(selected_set) != len(selected) or len(omitted_set) != len(omitted)
        or selected_set & omitted_set
        or [unit["unit_id"] for unit in units if unit["unit_id"] in selected_set] != selected
        or any(unit["unit_id"] in omitted_set for unit in units)
        or [unit for unit in units if unit["unit_id"] not in selected_set] != quote.get("fixed_units")
    ):
        raise ForestError("AGENT_MODEL_VIEW_PLAN_INVALID", "Model members differ from the published plan", 409)

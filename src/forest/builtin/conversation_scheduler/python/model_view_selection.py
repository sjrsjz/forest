"""Pure, deterministic selection of Context-owned model-view members.

This module accepts only frozen cost facts.  Reading Payloads, establishing a
Run fence, and publishing a selected plan belong to the Conversation service.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping, Sequence


class ViewSelectionError(ValueError):
    """A candidate cannot satisfy the frozen Context policy."""


@dataclass(frozen=True, slots=True)
class ViewCost:
    tokens: int = 0
    media_bytes: int = 0
    decoded_bytes: int = 0

    def __add__(self, other: ViewCost) -> ViewCost:
        return ViewCost(
            self.tokens + other.tokens,
            self.media_bytes + other.media_bytes,
            self.decoded_bytes + other.decoded_bytes,
        )


@dataclass(frozen=True, slots=True)
class ViewSelection:
    selected_unit_ids: tuple[str, ...]
    omitted_unit_ids: tuple[str, ...]
    considered_unit_ids: tuple[str, ...]
    fixed_cost: ViewCost
    selected_cost: ViewCost
    total_cost: ViewCost

    @property
    def selected_bitmap(self) -> str:
        selected = set(self.selected_unit_ids)
        return "".join("1" if unit_id in selected else "0" for unit_id in self.considered_unit_ids)

    @property
    def source_set_digest(self) -> str:
        return "sha256:" + hashlib.sha256(
            "\x1f".join(self.considered_unit_ids).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class JointViewSelection:
    context: ViewSelection
    selected_evidence_unit_ids: tuple[str, ...]
    omitted_evidence_unit_ids: tuple[str, ...]
    evidence_cost: ViewCost


def evidence_unit_cost(
    unit: Mapping[str, object], *, bytes_per_token: int, unit_overhead: int,
) -> ViewCost:
    """Charge one complete ranked Memory source, including its ready images."""
    if bytes_per_token < 1 or unit_overhead < 0:
        raise ViewSelectionError("Invalid frozen estimator")
    try:
        size = len(json.dumps(unit, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False, allow_nan=False).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise ViewSelectionError("Memory evidence is not canonical JSON") from exc
    media_bytes = decoded_bytes = 0
    parts = unit.get("parts")
    if not isinstance(parts, list):
        raise ViewSelectionError("Memory evidence has no typed parts")
    for part in parts:
        if not isinstance(part, dict):
            raise ViewSelectionError("Memory evidence part is invalid")
        if part.get("kind") != "image" or part.get("projection_state") != "READY":
            continue
        length, width, height, pixels = (
            part.get(key) for key in ("byte_length", "width", "height", "decoded_pixels")
        )
        if (any(type(value) is not int or value < 1
                for value in (length, width, height, pixels))
                or pixels < width * height or pixels > 16777216):
            raise ViewSelectionError("Memory image cost is invalid")
        media_bytes += length
        decoded_bytes += pixels * 4
    return ViewCost((size + bytes_per_token - 1) // bytes_per_token + unit_overhead,
                    media_bytes, decoded_bytes)


def _fits(cost: ViewCost, budget: ViewCost) -> bool:
    return (cost.tokens <= budget.tokens and cost.media_bytes <= budget.media_bytes
            and cost.decoded_bytes <= budget.decoded_bytes)


def select_joint_view(
    units: Sequence[Mapping[str, object]], evidence: Sequence[Mapping[str, object]], *,
    required_unit_ids: Sequence[str], previous_selected_unit_ids: Sequence[str],
    previous_considered_unit_ids: Sequence[str], fixed_cost: ViewCost,
    budget: ViewCost, bytes_per_token: int, unit_overhead: int,
) -> JointViewSelection:
    """Keep causal Context, then recent Context, ranked Memory, and older Context.

    One quarter of available tokens (at most 8192) is held for Memory while
    selecting recent Context. Unused reservation returns to Context. The
    previous Context bitmap remains authoritative across model steps.
    """
    if any(not isinstance(row, Mapping)
           or not isinstance(row.get("source_unit_id"), str)
           or not row["source_unit_id"] for row in evidence):
        raise ViewSelectionError("Memory evidence identities are invalid")
    evidence_ids = tuple("evidence:" + row["source_unit_id"] for row in evidence)
    if len(set(evidence_ids)) != len(evidence_ids):
        raise ViewSelectionError("Memory evidence identities are invalid")
    reserve = min(8192, max(0, budget.tokens - fixed_cost.tokens) // 4) if evidence else 0
    baseline = fixed_cost
    fixed_ids = set(required_unit_ids) | set(previous_selected_unit_ids)
    for unit in units:
        if unit.get("unit_id") in fixed_ids:
            baseline += unit_cost(unit, bytes_per_token=bytes_per_token,
                                  unit_overhead=unit_overhead)
    recent_budget = ViewCost(max(baseline.tokens, budget.tokens - reserve),
                             budget.media_bytes, budget.decoded_bytes)
    recent = select_context_units(
        units, required_unit_ids=required_unit_ids,
        previous_selected_unit_ids=previous_selected_unit_ids,
        previous_considered_unit_ids=previous_considered_unit_ids,
        fixed_cost=fixed_cost, budget=recent_budget,
        bytes_per_token=bytes_per_token, unit_overhead=unit_overhead,
    )
    selected_evidence: list[str] = []
    omitted_evidence: list[str] = []
    evidence_cost = ViewCost()
    total = recent.total_cost
    for unit_id, row in zip(evidence_ids, evidence, strict=True):
        cost = evidence_unit_cost(row, bytes_per_token=bytes_per_token,
                                  unit_overhead=unit_overhead)
        if _fits(total + cost, budget):
            selected_evidence.append(unit_id)
            evidence_cost += cost
            total += cost
        else:
            omitted_evidence.append(unit_id)
    context = select_context_units(
        units, required_unit_ids=tuple(dict.fromkeys((*required_unit_ids, *recent.selected_unit_ids))),
        previous_selected_unit_ids=previous_selected_unit_ids,
        previous_considered_unit_ids=previous_considered_unit_ids,
        fixed_cost=fixed_cost + evidence_cost, budget=budget,
        bytes_per_token=bytes_per_token, unit_overhead=unit_overhead,
    )
    return JointViewSelection(context, tuple(selected_evidence),
                              tuple(omitted_evidence), evidence_cost)


def unit_cost(
    unit: Mapping[str, object], *, bytes_per_token: int, unit_overhead: int,
) -> ViewCost:
    """Charge complete source units, including materialized media facts."""
    if bytes_per_token < 1 or unit_overhead < 0:
        raise ViewSelectionError("Invalid frozen estimator")
    byte_length = unit.get("payload_byte_length")
    media_parts = unit.get("media_parts")
    if type(byte_length) is not int or byte_length < 1 or not isinstance(media_parts, list):
        raise ViewSelectionError("Context unit has no complete cost facts")
    media_bytes = decoded_bytes = 0
    for part in media_parts:
        if not isinstance(part, dict):
            raise ViewSelectionError("Context media cost is invalid")
        length = part.get("byte_length")
        pixels = part.get("decoded_pixels")
        if type(length) is not int or length < 1 or type(pixels) is not int or pixels < 1:
            raise ViewSelectionError("Context media cost is invalid")
        media_bytes += length
        decoded_bytes += pixels * 4
    return ViewCost(
        (byte_length + bytes_per_token - 1) // bytes_per_token + unit_overhead,
        media_bytes,
        decoded_bytes,
    )


def select_context_units(
    units: Sequence[Mapping[str, object]],
    *,
    required_unit_ids: Sequence[str],
    previous_selected_unit_ids: Sequence[str],
    previous_considered_unit_ids: Sequence[str],
    fixed_cost: ViewCost,
    budget: ViewCost,
    bytes_per_token: int,
    unit_overhead: int,
) -> ViewSelection:
    """Freeze nearest complete optional units without revisiting an old choice.

    ``previous_considered_unit_ids`` is the complete active source set seen by
    the preceding plan.  An omitted old unit cannot reappear when a later step
    has a smaller fixed cost; only newly appended Context units are eligible
    for extension.  Required and previously selected units are never evicted.
    """
    if (
        min(fixed_cost.tokens, fixed_cost.media_bytes, fixed_cost.decoded_bytes) < 0
        or min(budget.tokens, budget.media_bytes, budget.decoded_bytes) < 0
    ):
        raise ViewSelectionError("Negative model-view cost or budget")
    ids = tuple(unit.get("unit_id") for unit in units)
    if any(not isinstance(unit_id, str) or not unit_id for unit_id in ids):
        raise ViewSelectionError("Context unit has no stable identity")
    if len(set(ids)) != len(ids):
        raise ViewSelectionError("Context source identities repeat")
    if any(
        type(unit.get("context_revision")) is not int
        or type(unit.get("unit_index")) is not int
        for unit in units
    ) or list(units) != sorted(
        units, key=lambda unit: (unit["context_revision"], unit["unit_index"], unit["unit_id"])
    ):
        raise ViewSelectionError("Context units are not in committed order")
    required = set(required_unit_ids)
    frozen = set(previous_selected_unit_ids)
    considered = set(previous_considered_unit_ids)
    if (
        len(required) != len(required_unit_ids)
        or len(frozen) != len(previous_selected_unit_ids)
        or len(considered) != len(previous_considered_unit_ids)
        or not (required | frozen | considered) <= set(ids)
        or not frozen <= considered
        or (considered and tuple(unit_id for unit_id in ids if unit_id in considered)
            != tuple(previous_considered_unit_ids))
    ):
        raise ViewSelectionError("Frozen source set is missing or reordered")
    costs = {
        unit_id: unit_cost(unit, bytes_per_token=bytes_per_token, unit_overhead=unit_overhead)
        for unit_id, unit in zip(ids, units, strict=True)
    }
    selected = required | frozen
    selected_cost = ViewCost()
    for unit_id in ids:
        if unit_id in selected:
            selected_cost += costs[unit_id]
    total = fixed_cost + selected_cost
    if (
        total.tokens > budget.tokens
        or total.media_bytes > budget.media_bytes
        or total.decoded_bytes > budget.decoded_bytes
    ):
        raise ViewSelectionError("Fixed and required Context exceed the model-view budget")
    for unit_id in reversed(ids):
        if unit_id in selected or unit_id in considered:
            continue
        next_cost = costs[unit_id]
        candidate = total + next_cost
        if (
            candidate.tokens > budget.tokens
            or candidate.media_bytes > budget.media_bytes
            or candidate.decoded_bytes > budget.decoded_bytes
        ):
            break
        selected.add(unit_id)
        selected_cost += next_cost
        total = candidate
    return ViewSelection(
        selected_unit_ids=tuple(unit_id for unit_id in ids if unit_id in selected),
        omitted_unit_ids=tuple(unit_id for unit_id in ids if unit_id not in selected),
        considered_unit_ids=ids,
        fixed_cost=fixed_cost,
        selected_cost=selected_cost,
        total_cost=total,
    )

"""Pure, deterministic selection of Context-owned model-view members.

This module accepts only frozen cost facts.  Reading Payloads, establishing a
Run fence, and publishing a selected plan belong to the Conversation service.
"""

from __future__ import annotations

import hashlib
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

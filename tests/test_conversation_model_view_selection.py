"""The Context planner must preserve its frozen choices across model steps."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SOURCE = (
    Path(__file__).parents[1]
    / "src/forest/builtin/conversation_scheduler/python/model_view_selection.py"
)
_SPEC = importlib.util.spec_from_file_location("forest_test_model_view_selection", _SOURCE)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
ViewCost = _MODULE.ViewCost
ViewSelectionError = _MODULE.ViewSelectionError
select_context_units = _MODULE.select_context_units


def _unit(index: int, *, bytes_: int = 20, media: int = 0) -> dict:
    return {
        "unit_id": f"input:{index}",
        "context_revision": index,
        "unit_index": 0,
        "payload_byte_length": bytes_,
        "media_parts": (
            [{"byte_length": media, "decoded_pixels": media}]
            if media else []
        ),
    }


def _select(units, required=(), selected=(), considered=(), *, fixed=0, budget=50):
    return select_context_units(
        units,
        required_unit_ids=required,
        previous_selected_unit_ids=selected,
        previous_considered_unit_ids=considered,
        fixed_cost=ViewCost(tokens=fixed),
        budget=ViewCost(tokens=budget, media_bytes=1000, decoded_bytes=4000),
        bytes_per_token=2,
        unit_overhead=0,
    )


def test_first_plan_preserves_every_trigger_and_selects_nearest_complete_history():
    units = [_unit(index) for index in range(1, 7)]
    plan = _select(units, required=("input:2", "input:5"), fixed=20, budget=50)
    assert plan.selected_unit_ids == ("input:2", "input:5", "input:6")
    assert plan.omitted_unit_ids == ("input:1", "input:3", "input:4")
    assert plan.total_cost.tokens == 50
    assert plan.selected_bitmap == "010011"


def test_next_step_never_reselects_old_omissions_or_drops_frozen_members():
    units = [_unit(index) for index in range(1, 6)]
    first = _select(units, required=("input:5",), fixed=20, budget=40)
    assert first.selected_unit_ids == ("input:4", "input:5")
    next_plan = _select(
        [*units, _unit(6)],
        required=("input:5", "input:6"),
        selected=first.selected_unit_ids,
        considered=first.considered_unit_ids,
        fixed=10,
        budget=60,
    )
    assert next_plan.selected_unit_ids == ("input:4", "input:5", "input:6")
    assert next_plan.omitted_unit_ids == ("input:1", "input:2", "input:3")
    with pytest.raises(ViewSelectionError, match="Fixed and required"):
        _select(
            [*units, _unit(6)],
            required=("input:5", "input:6"),
            selected=first.selected_unit_ids,
            considered=first.considered_unit_ids,
            fixed=30,
            budget=50,
        )


def test_media_charge_can_stop_optional_selection_without_dropping_required_image():
    units = [_unit(1, media=50), _unit(2, media=70), _unit(3, media=20)]
    plan = select_context_units(
        units,
        required_unit_ids=("input:2",),
        previous_selected_unit_ids=(),
        previous_considered_unit_ids=(),
        fixed_cost=ViewCost(),
        budget=ViewCost(tokens=100, media_bytes=90, decoded_bytes=360),
        bytes_per_token=2,
        unit_overhead=0,
    )
    assert plan.selected_unit_ids == ("input:2", "input:3")
    assert plan.total_cost.media_bytes == 90
    with pytest.raises(ViewSelectionError, match="Fixed and required"):
        select_context_units(
            units,
            required_unit_ids=("input:2",),
            previous_selected_unit_ids=(),
            previous_considered_unit_ids=(),
            fixed_cost=ViewCost(),
            budget=ViewCost(tokens=100, media_bytes=69, decoded_bytes=360),
            bytes_per_token=2,
            unit_overhead=0,
        )

"""The Context planner must preserve its frozen choices across model steps."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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
select_context_units = _MODULE.select_context_units
select_joint_view = _MODULE.select_joint_view


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
        unit_overhead=0, include_images=True,
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
    larger_causal_history = _select(
        [*units, _unit(6)], required=("input:5", "input:6"),
        selected=first.selected_unit_ids, considered=first.considered_unit_ids,
        fixed=30, budget=50,
    )
    assert larger_causal_history.selected_unit_ids == ("input:4", "input:5", "input:6")
    assert larger_causal_history.total_cost.tokens == 60


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
        unit_overhead=0, include_images=True,
    )
    assert plan.selected_unit_ids == ("input:2", "input:3")
    assert plan.total_cost.media_bytes == 90
    required_image = select_context_units(
        units, required_unit_ids=("input:2",),
        previous_selected_unit_ids=(), previous_considered_unit_ids=(),
        fixed_cost=ViewCost(), budget=ViewCost(tokens=100, media_bytes=69, decoded_bytes=360),
        bytes_per_token=2, unit_overhead=0, include_images=True,
    )
    assert required_image.selected_unit_ids == ("input:2",)
    assert required_image.total_cost.media_bytes == 70


def test_joint_view_keeps_causal_and_recent_context_before_ranked_memory():
    units = [_unit(index, bytes_=100) for index in range(1, 7)]
    evidence = [
        {"source_unit_id": "first", "parts": [{"kind": "text", "text": "recalled"}]},
        {"source_unit_id": "second", "parts": [{"kind": "image", "projection_state": "READY",
                                                 "byte_length": 100, "width": 10, "height": 10,
                                                 "decoded_pixels": 100}]},
    ]
    plan = select_joint_view(
        units, evidence, required_unit_ids=("input:1",),
        previous_selected_unit_ids=(), previous_considered_unit_ids=(),
        fixed_cost=ViewCost(),
        budget=ViewCost(tokens=33, media_bytes=20, decoded_bytes=400),
        bytes_per_token=20, unit_overhead=0, include_images=True,
    )
    assert "input:1" in plan.context.selected_unit_ids
    assert "input:6" in plan.context.selected_unit_ids
    assert plan.selected_evidence_unit_ids == ("evidence:first",)
    assert plan.omitted_evidence_unit_ids == ("evidence:second",)
    assert plan.context.total_cost.tokens <= 33
    assert plan.context.selected_bitmap.count("1") < len(units)


def test_joint_view_never_reselects_old_context_omissions_when_memory_changes():
    units = [_unit(index) for index in range(1, 6)]
    evidence = [{"source_unit_id": "recalled", "parts": [{"kind": "text", "text": "fact"}]}]
    first = select_joint_view(
        units, evidence, required_unit_ids=("input:5",),
        previous_selected_unit_ids=(), previous_considered_unit_ids=(),
        fixed_cost=ViewCost(),
        budget=ViewCost(tokens=30, media_bytes=1000, decoded_bytes=4000),
        bytes_per_token=2, unit_overhead=0, include_images=True,
    )
    assert first.context.omitted_unit_ids
    next_plan = select_joint_view(
        [*units, _unit(6)], [], required_unit_ids=("input:5", "input:6"),
        previous_selected_unit_ids=first.context.selected_unit_ids,
        previous_considered_unit_ids=first.context.considered_unit_ids,
        fixed_cost=ViewCost(),
        budget=ViewCost(tokens=70, media_bytes=1000, decoded_bytes=4000),
        bytes_per_token=2, unit_overhead=0, include_images=True,
    )
    assert "input:6" in next_plan.context.selected_unit_ids
    assert set(first.context.omitted_unit_ids).isdisjoint(next_plan.context.selected_unit_ids)
    assert set(first.context.selected_unit_ids) <= set(next_plan.context.selected_unit_ids)


def test_joint_view_returns_unused_memory_reservation_to_group_history():
    units = [_unit(index, bytes_=20) for index in range(1, 6)]
    plan = select_joint_view(
        units, [], required_unit_ids=("input:5",),
        previous_selected_unit_ids=(), previous_considered_unit_ids=(),
        fixed_cost=ViewCost(),
        budget=ViewCost(tokens=50, media_bytes=1000, decoded_bytes=4000),
        bytes_per_token=2, unit_overhead=0, include_images=True,
    )
    assert plan.context.selected_unit_ids == tuple(f"input:{index}" for index in range(1, 6))
    assert plan.selected_evidence_unit_ids == ()


def test_multiframe_memory_image_charges_all_decoded_pixels():
    evidence = [{
        "source_unit_id": "animated",
        "parts": [{
            "kind": "image", "projection_state": "READY",
            "byte_length": 100, "width": 10, "height": 10,
            "decoded_pixels": 300,
        }],
    }]
    kwargs = {
        "required_unit_ids": (),
        "previous_selected_unit_ids": (),
        "previous_considered_unit_ids": (),
        "fixed_cost": ViewCost(),
        "bytes_per_token": 4,
        "unit_overhead": 0,
        "include_images": True,
    }
    too_small = select_joint_view(
        [], evidence, budget=ViewCost(tokens=1000, media_bytes=1000, decoded_bytes=800),
        **kwargs,
    )
    assert too_small.omitted_evidence_unit_ids == ("evidence:animated",)
    enough = select_joint_view(
        [], evidence, budget=ViewCost(tokens=1000, media_bytes=1000, decoded_bytes=1200),
        **kwargs,
    )
    assert enough.selected_evidence_unit_ids == ("evidence:animated",)


def test_text_only_model_does_not_charge_unsent_image_media():
    units = [_unit(1, media=500)]
    kwargs = dict(
        required_unit_ids=("input:1",),
        previous_selected_unit_ids=(),
        previous_considered_unit_ids=(),
        fixed_cost=ViewCost(),
        budget=ViewCost(tokens=100, media_bytes=100, decoded_bytes=400),
        bytes_per_token=2,
        unit_overhead=0,
    )
    text_only = select_joint_view(units, [], include_images=False, **kwargs)
    assert text_only.context.selected_unit_ids == ("input:1",)
    assert text_only.context.total_cost.media_bytes == 0
    visual_context = select_joint_view(units, [], include_images=True, **kwargs)
    assert visual_context.context.selected_unit_ids == ("input:1",)
    assert visual_context.context.total_cost.media_bytes == 500
    image_evidence = [{
        "source_unit_id": "recalled-image",
        "parts": [{"kind": "image", "projection_state": "READY",
                   "byte_length": 500, "width": 10, "height": 10,
                   "decoded_pixels": 100}],
    }]
    no_context = {**kwargs, "required_unit_ids": ()}
    text_evidence = select_joint_view([], image_evidence, include_images=False,
                                      **no_context)
    assert text_evidence.selected_evidence_unit_ids == ("evidence:recalled-image",)
    visual_evidence = select_joint_view([], image_evidence, include_images=True,
                                        **no_context)
    assert visual_evidence.omitted_evidence_unit_ids == ("evidence:recalled-image",)

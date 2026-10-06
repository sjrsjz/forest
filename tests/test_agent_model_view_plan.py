"""The Agent must consume the exact Conversation membership before dispatch."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.packages import digest_json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/agent_runtime/python"))
from model_view_quote import select_model_view, verify_model_view


@pytest.mark.asyncio
async def test_agent_filters_only_conversation_members_and_preserves_causal_history():
    bodies = {}

    async def put_json(body, *, owner_kind, owner_id, parents):
        assert owner_kind == "model.view.quote"
        assert parents[0] == snapshot_ref
        reference = {
            "payload_id": "quote/1", "digest": digest_json(body),
            "owner_component_id": "agent.runtime", "owner_kind": owner_kind,
            "owner_id": owner_id,
        }
        bodies[reference["payload_id"]] = body
        return reference

    async def read_json(reference):
        return bodies[reference["payload_id"]]

    async def call(name, payload, **_kwargs):
        assert name == "conversation.model-view.plan"
        quote = bodies[payload["quote"]["payload_id"]]
        bodies["plan/1"] = {
            "protocol": "forest.model-view-plan/1", "quote": payload["quote"],
            "context_snapshot": quote["context_snapshot"],
            "run_ref": quote["run_ref"], "step_seq": quote["step_seq"],
            "context_revision": 2, "context_view_revision": 1,
            "media_cost_revision": 0, "context_applied_seq": 2,
            "selected_unit_ids": ["ctx:2"], "omitted_unit_ids": ["ctx:1"],
        }
        return {"plan": plan_ref, "step_seq": 1, "source_set_digest": digest_json(["ctx:1", "ctx:2"])}

    snapshot_ref = {
        "payload_id": "context/1", "digest": "sha256:" + "1" * 64,
        "owner_component_id": "conversation.scheduler", "owner_kind": "context.snapshot",
        "owner_id": "snapshot/1",
    }
    preset_ref = {
        "payload_id": "preset/1", "digest": "sha256:" + "2" * 64,
        "owner_component_id": "agent.runtime", "owner_kind": "preset.context",
        "owner_id": "preset/1",
    }
    plan_ref = {
        "payload_id": "plan/1", "digest": "sha256:" + "3" * 64,
        "owner_component_id": "conversation.scheduler", "owner_kind": "model.view.plan",
        "owner_id": "plan/1",
    }
    ctx = SimpleNamespace(
        identity={"bot_id": "bot/1"},
        payloads=SimpleNamespace(put_json=put_json, read_json=read_json),
        services=SimpleNamespace(call=call),
        operations=SimpleNamespace(stable=lambda value: value),
    )
    raw = {
        "units": [
            {"unit_id": "ctx:1", "source_kind": "onebot.message"},
            {"unit_id": "ctx:2", "source_kind": "onebot.message"},
        ],
        "context_revision": 2, "context_view_revision": 1,
        "context_applied_seq": 2, "media_cost_revision": 0,
    }
    view = {
        "bot_id": "bot/1", "run_id": "run/1", "input_revision": 1,
        "context_snapshot_ref": snapshot_ref,
        "required_unit_ids": ["ctx:2"],
        "units": [
            {"unit_id": "ctx:1", "role": "user"},
            {"unit_id": "ctx:2", "role": "user"},
            {"unit_id": "agent-turn:1", "role": "assistant"},
        ],
    }
    selected = await select_model_view(
        ctx, request={"actor": {"conversation_id": "conversation/1"},
                      "session_ref": "session/1"},
        run={"conversation_ref": "conversation/1", "run_id": "run/1"},
        step_seq=1, raw_snapshot=raw, model_snapshot=view,
        preset_ref=preset_ref, preset_context=["persona"], tools=[],
        evidence_ref=None, output_tokens=1024,
    )
    assert [unit["unit_id"] for unit in selected["units"]] == ["ctx:2", "agent-turn:1"]
    assert selected["model_view_plan_ref"] == plan_ref
    await verify_model_view(
        ctx, snapshot=selected, preset_context=["persona"], tools=[],
        evidence_ref=None, output_tokens=1024,
    )

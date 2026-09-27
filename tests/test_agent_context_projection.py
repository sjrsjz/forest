from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from forest.agent_context_projection import ConversationContextProjector
from forest.model_contract import ModelCapabilities

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/agent_runtime/python"))
from model_projection import prepare_model_request


class Payloads:
    def __init__(self, values):
        self.values = values
        self.references = ()

    async def read_verified_batch(self, references):
        self.references = tuple(references)
        return tuple(self.values[item.payload_id] for item in self.references)


class Agent:
    async def execution_history(self, run_id):
        assert run_id == "run/one"
        return ()


@pytest.mark.asyncio
async def test_context_projection_retains_lane_author_and_text_only_image_omission():
    payloads = Payloads(
        {
            "payload/message": json.dumps(
                {
                    "parts": [
                        {"type": "text", "text": "hello"},
                        {
                            "type": "image",
                            "media_ref": {"kind": "onebot-source", "state": "PENDING_CAPTURE"},
                        },
                    ]
                }
            ).encode(),
            "payload/author": json.dumps(
                {"external_user_id": "42", "display_name": "Alice"}
            ).encode(),
        }
    )
    projector = ConversationContextProjector(
        bot_id="bot/one", payloads=payloads, agent=Agent()
    )
    run = {
        "run_id": "run/one",
        "session": {"conversation_ref": "conversation/one"},
        "input_revision": 1,
        "context_revision": 1,
        "context_view_revision": 0,
        "selected_input_end_seq": 1,
        "inputs": [
            {"input_revision": 1, "input_batch_ref": "batch/one"}
        ],
        "triggers": [
            {
                "input_revision": 1,
                "trigger_ref": "trigger/one",
                "principal_ref": "principal/42",
                "ingress_seq": 1,
            }
        ],
    }
    context = {
        "conversation_id": "conversation/one",
        "context_applied_seq": 1,
        "context_revision": 1,
        "context_view_revision": 0,
        "binding_ref": "binding/qq",
        "platform": "onebot-v11",
        "channel_kind": "GROUP",
        "channel_ref": "933179609",
        "thread_ref": None,
        "units": [
            {
                "unit_id": "input/one",
                "kind": "INPUT",
                "source_kind": "conversation.input",
                "source_id": "event/one",
                "event_id": "event/one",
                "input_batch_id": "batch/one",
                "ingress_seq": 1,
                "payload_id": "payload/message",
                "payload_digest": "sha256:" + "1" * 64,
                "payload_owner_component_id": "adapter.onebot",
                "payload_owner_kind": "onebot.normalized-event",
                "payload_owner_id": "event/one",
                "author_payload_id": "payload/author",
                "author_payload_digest": "sha256:" + "2" * 64,
                "author_owner_component_id": "adapter.onebot",
                "author_owner_kind": "onebot.author-snapshot",
                "author_owner_id": "event/one",
                "author_subject_ref": "principal/42",
            }
        ],
    }
    snapshot = await projector.project(
        run=run,
        context=context,
        logical_step_id="step/one",
    )
    unit = snapshot["units"][0]
    assert snapshot["required_unit_ids"] == ["input/one"]
    assert snapshot["conversation"]["binding_ref"] == "binding/qq"
    assert unit["source"]["group_source_id"] == "conversation/one"
    assert unit["author"]["principal_ref"] == "principal/42"
    assert [part["type"] for part in unit["parts"]] == ["text", "image"]
    assert {item.owner_kind for item in payloads.references} == {
        "onebot.normalized-event",
        "onebot.author-snapshot",
    }

    prepared = prepare_model_request(
        snapshot,
        ModelCapabilities(
            deployment_id="deployment/one",
            model="text-model",
            capability_revision="capability/one",
            input_modalities=("text",),
            supports_streaming=False,
        ),
        request_id="request/one",
        transport_attempt_id="attempt/one",
        output_tokens=128,
        stream=False,
    )
    manifest = prepared.manifest
    assert manifest["source_manifest"][0]["source"]["binding_ref"] == "binding/qq"
    assert manifest["omitted_part_refs"] == [
        {"unit_id": "input/one", "part_id": "input/one:1", "reason": "MODEL_IMAGE_UNSUPPORTED"}
    ]


@pytest.mark.asyncio
async def test_only_trigger_records_are_pinned_from_a_streaming_input_batch():
    bodies = {}
    units = []
    for number in range(1, 4):
        bodies[f"payload/message/{number}"] = json.dumps(
            {"text": f"message {number}"}
        ).encode()
        bodies[f"payload/author/{number}"] = json.dumps(
            {"display_name": f"User {number}"}
        ).encode()
        units.append(
            {
                "unit_id": f"input/{number}",
                "kind": "INPUT",
                "source_kind": "conversation.input",
                "source_id": f"event/{number}",
                "event_id": f"event/{number}",
                "input_batch_id": "batch/stream",
                "ingress_seq": number,
                "payload_id": f"payload/message/{number}",
                "payload_digest": "sha256:" + str(number) * 64,
                "payload_owner_component_id": "adapter.onebot",
                "payload_owner_kind": "onebot.normalized-event",
                "payload_owner_id": f"event/{number}",
                "author_payload_id": f"payload/author/{number}",
                "author_payload_digest": "sha256:" + str(number + 3) * 64,
                "author_owner_component_id": "adapter.onebot",
                "author_owner_kind": "onebot.author-snapshot",
                "author_owner_id": f"event/{number}",
                "author_subject_ref": f"principal/{number}",
            }
        )
    snapshot = await ConversationContextProjector(
        bot_id="bot/one",
        payloads=Payloads(bodies),
        agent=Agent(),
    ).project(
        run={
            "run_id": "run/one",
            "session": {"conversation_ref": "conversation/stream"},
            "input_revision": 1,
            "context_revision": 1,
            "context_view_revision": 0,
            "selected_input_end_seq": 3,
            "inputs": [{"input_revision": 1, "input_batch_ref": "batch/stream"}],
            "triggers": [
                {
                    "input_revision": 1,
                    "trigger_ref": "trigger/three",
                    "principal_ref": "principal/3",
                    "ingress_seq": 3,
                }
            ],
        },
        context={
            "conversation_id": "conversation/stream",
            "context_applied_seq": 3,
            "context_revision": 1,
            "context_view_revision": 0,
            "binding_ref": "binding/qq",
            "platform": "onebot-v11",
            "channel_kind": "GROUP",
            "channel_ref": "933179609",
            "thread_ref": None,
            "units": units,
        },
        logical_step_id="step/stream",
    )
    assert [unit["unit_id"] for unit in snapshot["units"]] == [
        "input/1",
        "input/2",
        "input/3",
    ]
    assert snapshot["required_unit_ids"] == ["input/3"]

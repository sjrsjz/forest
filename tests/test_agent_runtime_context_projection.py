import json

import pytest

from forest.builtin.agent_runtime.python.context_projection import ContextSnapshotReader
from forest.packages import digest_json


def _ref(payload_id, value, component, kind, owner_id):
    return {
        "payload_id": payload_id,
        "digest": digest_json(value),
        "owner_component_id": component,
        "owner_kind": kind,
        "owner_id": owner_id,
    }


class _Payloads:
    def __init__(self, values):
        self.values = values

    async def read_bytes(self, reference):
        value = self.values[reference["payload_id"]]
        assert reference["digest"] == digest_json(value)
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class _Context:
    def __init__(self, values):
        self.payloads = _Payloads(values)


@pytest.mark.asyncio
async def test_agent_package_interleaves_run_history_at_input_revision_boundary():
    first = {"text": "first trigger"}
    second = {"text": "queued trigger"}
    first_author = {"display_name": "Alice", "external_user_id": "100"}
    second_author = {"display_name": "Bob", "external_user_id": "200"}
    result = {
        "protocol": "forest.provider-model-result/1",
        "text": "<valid_agent_protocol_content_block />",
    }
    values = {
        "payload:first": first,
        "payload:second": second,
        "payload:first-author": first_author,
        "payload:second-author": second_author,
        "payload:model-result": result,
    }
    first_ref = _ref(
        "payload:first", first, "conversation.scheduler", "context.input", "event:first"
    )
    second_ref = _ref(
        "payload:second", second, "conversation.scheduler", "context.input", "event:second"
    )
    first_author_ref = _ref(
        "payload:first-author",
        first_author,
        "adapter.onebot",
        "author.identity",
        "qq:100",
    )
    second_author_ref = _ref(
        "payload:second-author",
        second_author,
        "adapter.onebot",
        "author.identity",
        "qq:200",
    )
    snapshot_ref = _ref(
        "payload:snapshot",
        {},
        "conversation.scheduler",
        "context.snapshot",
        "snapshot:1",
    )

    def unit(unit_id, batch_id, ingress_seq, payload, author, principal):
        return {
            "unit_id": unit_id,
            "kind": "INPUT",
            "source_kind": "onebot.message",
            "source_id": "event:" + unit_id,
            "event_id": "event:" + unit_id,
            "input_batch_id": batch_id,
            "ingress_seq": ingress_seq,
            "author_subject_ref": principal,
            "payload_id": payload["payload_id"],
            "payload_digest": payload["digest"],
            "payload_owner_component_id": payload["owner_component_id"],
            "payload_owner_kind": payload["owner_kind"],
            "payload_owner_id": payload["owner_id"],
            "author_payload_id": author["payload_id"],
            "author_payload_digest": author["digest"],
            "author_owner_component_id": author["owner_component_id"],
            "author_owner_kind": author["owner_kind"],
            "author_owner_id": author["owner_id"],
        }

    snapshot = {
        "conversation_id": "conversation:group:1",
        "context_applied_seq": 2,
        "context_revision": 2,
        "context_view_revision": 0,
        "binding_ref": "binding:onebot:1",
        "platform": "onebot",
        "channel_kind": "GROUP",
        "channel_ref": "group:1",
        "thread_ref": None,
        "snapshot": snapshot_ref,
        "units": [
            unit("first", "batch:1", 1, first_ref, first_author_ref, "qq:100"),
            unit("second", "batch:2", 2, second_ref, second_author_ref, "qq:200"),
        ],
    }
    run = {
        "run_id": "run:1",
        "conversation_ref": "conversation:group:1",
        "progress": {
            "input_revision": 2,
            "selected_input_end_seq": 2,
            "context_revision": 2,
            "context_view_revision": 0,
        },
        "inputs": [
            {"input_revision": 1, "input_batch_ref": "batch:1", "trigger_count": 1,
             "trigger_claim_ref": "batch:1", "trigger_claim_digest": "sha256:" + "3" * 64,
             "last_trigger_ref": "trigger:1", "last_trigger_ingress_seq": 1,
             "source_manifest_digest": "sha256:" + "1" * 64},
            {"input_revision": 2, "input_batch_ref": "batch:2", "trigger_count": 1,
             "trigger_claim_ref": "batch:2", "trigger_claim_digest": "sha256:" + "4" * 64,
             "last_trigger_ref": "trigger:2", "last_trigger_ingress_seq": 2,
             "source_manifest_digest": "sha256:" + "2" * 64},
        ],
    }
    history = {
        "protocol": "forest.agent-execution-history/1",
        "run_id": "run:1",
        "turns": [
            {
                "turn_id": "turn:1",
                "input_revision": 1,
                "provider_component_id": "provider.chat-completions",
                "model_attempt_id": "attempt:1",
                "model_result_payload_id": "payload:model-result",
                "model_result_payload_digest": digest_json(result),
                "tool_call_id": None,
            }
        ],
    }

    view = await ContextSnapshotReader(_Context(values), bot_id="bot:1").model_view(
        snapshot,
        run=run,
        history=history,
        logical_step_id="model-request:2",
    )

    assert [item["unit_id"] for item in view["units"]] == [
        "first",
        "agent-turn:turn:1",
        "second",
    ]
    assert view["units"][0]["author"]["principal_ref"] == "qq:100"
    assert view["units"][2]["author"]["principal_ref"] == "qq:200"
    assert view["required_unit_ids"] == ["second"]
    assert [item["last_trigger_ref"] for item in view["trigger_claims"]] == [
        "trigger:1", "trigger:2"
    ]

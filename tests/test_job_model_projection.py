"""Task provenance survives the actual model wire without becoming a system role."""

import copy
import json
from dataclasses import replace

import pytest
from test_models import prepared, snapshot, unit

from forest.agent_settings import AgentSettings
from forest.errors import ForestError


def test_job_target_and_occurrence_are_rendered_as_attributed_data():
    task = unit(text="该计划任务已取消；此处仅保留历史记录。", role="control_data")
    task["source"].update(platform="forest", target_platform="onebot-v11", source_kind="SYSTEM",
                          job_id="job:1", occurrence_id="occurrence:1", task_status="CANCELLED")
    task["author"].update(identity_origin="host_job", principal_ref="delegation:1")
    value = snapshot(task)
    before = copy.deepcopy(value)
    result = prepared(value)
    messages = json.loads(result.body)["messages"]
    assert [row["role"] for row in messages] == ["system", "user"]
    source = json.loads(messages[1]["content"][0]["text"])["source"]
    for field in ("target_platform", "job_id", "occurrence_id", "task_status", "group_source_id", "channel_id"):
        assert source[field] == task["source"][field]
    assert value == before and result.manifest["projection_version"] == "forest-model-view-v2"


def test_new_source_fields_are_typed_and_do_not_smuggle_structured_image_data():
    task = unit()
    task["source"]["task_status"] = {"image_url": "data:image/png;base64,secret"}
    with pytest.raises(ForestError) as caught:
        prepared(snapshot(task))
    assert caught.value.code == "MODEL_SOURCE_INVALID"


def test_old_frozen_request_can_replay_after_the_projection_revision_changes():
    result = prepared()
    manifest = result.manifest
    manifest["projection_version"] = "forest-model-view-v1"
    historic = replace(result, manifest_json=json.dumps(manifest))
    historic.verify()
    assert historic.body == result.body and historic.payload_digest == result.payload_digest


def test_single_agent_keeps_all_sixteen_configured_tool_slots():
    tools = [dict(name=f"tool_{index}", component_id="component", service="read") for index in range(16)]
    assert len(AgentSettings(allowed_tools=tools).allowed_tools) == 16
    with pytest.raises(ValueError):
        AgentSettings(allowed_tools=tools + [dict(name="overflow", component_id="c", service="s")])

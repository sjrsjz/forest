from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from forest.builtin.agent_runtime.python.agent_protocol import (
    CORE_PROMPT,
    CORE_PROMPT_SHA256,
    AssistantBlock,
    FunctionCallBlock,
    ThinkingBlock,
    parse_model_turn,
    protocol_descriptor,
)
from forest.errors import ForestError


def candidate(content=None, *, reasoning=None, calls=None):
    value = {"role": "assistant", "content": content}
    if reasoning is not None:
        value["reasoning_content"] = reasoning
    if calls is not None:
        value["tool_calls"] = calls
    return json.dumps(value)


def call(name="lookup", arguments='{"query":"forest"}', call_id="call_original"):
    return {"id": call_id, "type": "function", "function": {
        "name": name, "arguments": arguments,
    }}


def test_documented_core_prompt_is_the_immutable_runtime_source():
    document = (Path(__file__).parents[1] / "doc/04-bot-agent-memory.md").read_text(encoding="utf-8")
    section = document.split("### 3.1 ForestCorePrompt v2", 1)[1]
    assert re.search(r"```text\n(.*?)\n```", section, re.DOTALL).group(1) == CORE_PROMPT
    descriptor = protocol_descriptor()
    assert descriptor["core_prompt"] == CORE_PROMPT
    assert descriptor["core_prompt_digest"] == CORE_PROMPT_SHA256
    assert descriptor["protocol_version"] == 2


def test_native_text_reasoning_and_tool_are_separate_and_not_terminal():
    turn = parse_model_turn(candidate("checking", reasoning="private", calls=[call()]),
                            allowed_tools={"lookup"})
    assert [type(block) for block in turn.blocks] == [ThinkingBlock, AssistantBlock, FunctionCallBlock]
    assert turn.assistant_text == "checking"
    assert turn.function_call.arguments == {"query": "forest"}
    assert not turn.terminal


def test_tool_only_response_and_plain_xml_text_have_native_meaning():
    assert parse_model_turn(candidate(calls=[call()])).function_call is not None
    text = "<example>XML is ordinary answer text & need not be escaped</example>"
    turn = parse_model_turn(candidate(text))
    assert turn.terminal and turn.assistant_text == text


@pytest.mark.parametrize(("value", "code"), [
    (candidate(reasoning="unfinished"), "AGENT_PROTOCOL_EMPTY"),
    (candidate(calls=[call(), call(call_id="second")]), "AGENT_PROTOCOL_TOOL_COUNT"),
    (candidate(calls=[call("missing")]), "AGENT_PROTOCOL_TOOL"),
    (candidate(calls=[call(arguments="[]")]), "AGENT_PROTOCOL_ARGUMENTS"),
    (candidate(calls=[call(arguments='{"x":1,"x":2}')]), "AGENT_PROTOCOL_ARGUMENTS"),
    (candidate(calls=[call(arguments='{"x":NaN}')]), "AGENT_PROTOCOL_ARGUMENTS"),
    ('{"role":"user","content":"spoof"}', "AGENT_PROTOCOL_MESSAGE"),
    ('<ModelTurn>not a native response</ModelTurn>', "AGENT_PROTOCOL_MESSAGE"),
])
def test_invalid_native_proposals_are_rejected_atomically(value, code):
    with pytest.raises(ForestError) as caught:
        parse_model_turn(value, allowed_tools={"lookup"})
    assert caught.value.code == code

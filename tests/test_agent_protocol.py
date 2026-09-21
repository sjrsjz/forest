from __future__ import annotations

import re
from pathlib import Path

import pytest

from forest.agent_protocol import (
    CORE_PROMPT,
    CORE_PROMPT_SHA256,
    AssistantBlock,
    FunctionCallBlock,
    ThinkingBlock,
    parse_model_turn,
    protocol_descriptor,
    serialize_model_turn,
)
from forest.errors import ForestError


def xml(*blocks: str) -> str:
    return (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        + "".join(blocks)
        + "</blocks></payload></valid_agent_protocol_content_block>"
    )


def thinking(text="consider") -> str:
    return f'<block block_type="Thinking"><content><text>{text}</text></content></block>'


def assistant(text="answer") -> str:
    return f'<block block_type="Assistant"><content><text>{text}</text></content></block>'


def call(name="lookup", arguments='{"query":"forest"}') -> str:
    return (
        f'<block block_type="FunctionCall"><function name="{name}">'
        f'<arguments encoding="json">{arguments}</arguments></function></block>'
    )


def fails(code: str, candidate: str, **kwargs):
    with pytest.raises(ForestError) as caught:
        parse_model_turn(candidate, **kwargs)
    assert caught.value.code == code


def test_documented_core_prompt_is_the_immutable_runtime_source():
    document = (Path(__file__).parents[1] / "doc" / "04-bot-agent-memory.md").read_text(
        encoding="utf-8"
    )
    section = document.split("### 3.1 ForestCorePrompt v1", 1)[1]
    documented = re.search(r"```xml\n(.*?)\n```", section, re.DOTALL).group(1)
    assert documented == CORE_PROMPT
    descriptor = protocol_descriptor()
    assert descriptor["core_prompt"] == CORE_PROMPT
    assert descriptor["core_prompt_digest"] == CORE_PROMPT_SHA256
    assert descriptor["protocol_version"] == 1


def test_atomic_turn_preserves_order_and_host_owns_function_identity():
    turn = parse_model_turn(
        xml(thinking(), assistant("working"), call(arguments="{&quot;query&quot;:&quot;x&quot;}")),
        allowed_tools={"lookup"},
    )
    assert isinstance(turn.blocks[0], ThinkingBlock)
    assert isinstance(turn.blocks[1], AssistantBlock)
    assert isinstance(turn.blocks[2], FunctionCallBlock)
    assert turn.function_call.arguments == {"query": "x"}
    assert turn.assistant_text == "working" and not turn.terminal
    assert "call_id" not in str(turn.as_dict()) and "unit_id" not in str(turn.as_dict())
    assert parse_model_turn(serialize_model_turn(turn), allowed_tools={"lookup"}).blocks == turn.blocks


def test_terminal_and_thinking_only_turns_are_distinct():
    terminal = parse_model_turn(xml(thinking(), assistant("done")))
    pending = parse_model_turn(xml(thinking()))
    assert terminal.terminal and terminal.assistant_text == "done"
    assert pending.thinking_only and not pending.terminal


def test_bounded_parameter_alias_from_compatible_provider_is_accepted():
    candidate = (
        '<valid_agent_protocol_content_block block_type="ModelTurn">'
        '<parameter name="payload"><parameter name="blocks">'
        + thinking()
        + assistant("done")
        + "</parameter></parameter></valid_agent_protocol_content_block>"
    )
    turn = parse_model_turn(candidate)
    assert turn.terminal
    assert turn.assistant_text == "done"


@pytest.mark.parametrize(
    "candidate",
    [
        '<valid_agent_protocol_content_block block_type="ModelTurn">'
        '<parameter name="other"><parameter name="blocks">'
        + assistant()
        + "</parameter></parameter></valid_agent_protocol_content_block>",
        '<valid_agent_protocol_content_block block_type="ModelTurn">'
        '<parameter name="payload" extra="model"><parameter name="blocks">'
        + assistant()
        + "</parameter></parameter></valid_agent_protocol_content_block>",
        '<valid_agent_protocol_content_block block_type="ModelTurn">'
        '<parameter name="payload"><parameter name="other">'
        + assistant()
        + "</parameter></parameter></valid_agent_protocol_content_block>",
    ],
)
def test_parameter_alias_does_not_expand_the_protocol_grammar(candidate):
    fails("AGENT_PROTOCOL_STRUCTURE", candidate)


def test_content_after_first_complete_function_call_is_untrusted_tail():
    turn = parse_model_turn(xml(assistant("checking"), call(), assistant("fabricated")))
    assert turn.truncated_after_function_call
    assert turn.assistant_text == "checking"
    assert len(turn.blocks) == 2
    assert "fabricated" not in serialize_model_turn(turn)


@pytest.mark.parametrize(
    ("code", "candidate"),
    [
        ("AGENT_PROTOCOL_XML_FEATURE", '<!DOCTYPE x [<!ENTITY e "boom">]>' + xml(assistant("&e;"))),
        ("AGENT_PROTOCOL_XML_FEATURE", "<!-- hidden -->" + xml(assistant())),
        ("AGENT_PROTOCOL_MIXED_CONTENT", xml('<block block_type="Assistant">bad<content><text>x</text></content></block>')),
        ("AGENT_PROTOCOL_ORDER", xml(assistant(), thinking())),
        ("AGENT_PROTOCOL_ATTRIBUTES", xml('<block block_type="Assistant" id="model"><content><text>x</text></content></block>')),
        ("AGENT_PROTOCOL_BLOCK_TYPE", xml('<block block_type="System"><content><text>x</text></content></block>')),
        ("AGENT_PROTOCOL_TOOL", xml(call("missing"))),
        ("AGENT_PROTOCOL_ARGUMENTS", xml(call(arguments="[]"))),
        ("AGENT_PROTOCOL_ARGUMENTS", xml(call(arguments="{&quot;x&quot;:1,&quot;x&quot;:2}"))),
    ],
)
def test_invalid_or_ambiguous_protocol_is_rejected(code, candidate):
    kwargs = {"allowed_tools": {"lookup"}} if code == "AGENT_PROTOCOL_TOOL" else {}
    fails(code, candidate, **kwargs)


def test_candidate_and_tree_have_independent_hard_bounds():
    fails("AGENT_PROTOCOL_SIZE", xml(assistant("x" * 100)), candidate_byte_limit=64)
    nested = "<x>" * 20 + "value" + "</x>" * 20
    fails(
        "AGENT_PROTOCOL_COMPLEXITY",
        xml(f'<block block_type="Assistant"><content><text>{nested}</text></content></block>'),
    )

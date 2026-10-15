"""Stable kernel recognition of the reserved group Shell prefix."""

from __future__ import annotations

from forest.errors import ForestError


def _inspect(normalized: dict) -> dict:
    segments = normalized.get("segments")
    self_id = normalized.get("self_id")
    if not isinstance(segments, list) or not isinstance(self_id, str):
        raise ForestError("SHELL_GATE_INPUT_INVALID", "Shell gate input is invalid", 422)
    index = 0
    if segments and isinstance(segments[0], dict):
        first = segments[0]
        if first.get("type") == "mention" and first.get("external_user_id") == self_id:
            index = 1
    texts = []
    text_only = True
    for segment in segments[index:]:
        if not isinstance(segment, dict) or segment.get("type") != "text":
            text_only = False
            break
        value = segment.get("text")
        if not isinstance(value, str):
            text_only = False
            break
        texts.append(value)
    value = "".join(texts).lstrip(" 	")
    if value == "#forest":
        command_text = ""
    elif value.startswith("#forest") and len(value) > 7 and value[7].isspace():
        separator = value[7]
        command_text = value[7:].lstrip(" 	") if separator in " 	" else ""
        if separator not in " 	":
            text_only = False
    else:
        return {"protocol": "forest.shell-gate/1", "claimed": False}
    if len(command_text.encode("utf-8")) > 4096:
        command_text = ""
        text_only = False
    return {
        "protocol": "forest.shell-gate/1", "claimed": True,
        "command_text": command_text, "text_only": text_only,
    }


def shell_gate_inspect_service(_context):
    async def inspect(request):
        if (
            not isinstance(request, dict)
            or request.get("protocol") != "forest.shell-gate-request/1"
            or set(request) != {"protocol", "normalized"}
            or not isinstance(request["normalized"], dict)
        ):
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Shell gate request is invalid", 422)
        return _inspect(request["normalized"])
    return inspect

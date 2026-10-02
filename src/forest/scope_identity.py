"""Canonical identity for one Bot, deployment, or host runtime scope."""

from __future__ import annotations


def bind_scope_identity(
    *, scope_kind: str, scope_id: str | None, bot_id: str | None
) -> str:
    if not isinstance(scope_kind, str) or scope_kind not in {
        "bot", "deployment", "host"
    }:
        raise ValueError("Runtime scope kind is invalid")
    if scope_kind == "bot":
        if not isinstance(bot_id, str) or not bot_id or scope_id not in (None, bot_id):
            raise ValueError("Bot scope must bind its exact Bot ID")
        scope_id = bot_id
    elif bot_id is not None:
        raise ValueError("Non-Bot scope cannot carry a Bot ID")
    if (
        not isinstance(scope_id, str)
        or not 1 <= len(scope_id.encode("utf-8")) <= 128
        or any(ord(char) < 32 or ord(char) == 127 for char in scope_id)
    ):
        raise ValueError("Runtime scope ID is invalid")
    return scope_id

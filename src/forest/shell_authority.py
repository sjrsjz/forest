"""Pure Shell principal and admission-priority decisions."""

from __future__ import annotations

from typing import Any

P0_PRIORITY_POLICY = "current-binding-admin-v1"


def is_shell_admin(author: Any, config: Any) -> bool:
    """Accept only a verified platform principal in the current explicit allowlist."""
    if not isinstance(author, dict) or not isinstance(config, dict):
        return False
    admins = config.get("admin_user_ids")
    return (
        author.get("is_self") is False
        and author.get("anonymous") is None
        and author.get("identity_origin") == "adapter_verified"
        and isinstance(admins, list)
        and all(isinstance(admin, str) for admin in admins)
        and author.get("external_user_id") in admins
    )


def qualify_priority(requested_priority: int, author: Any, config: Any) -> dict:
    """Keep P0 only for a current admin; execution must authorize again."""
    if type(requested_priority) is not int or not 0 <= requested_priority <= 2:
        raise ValueError("requested_priority")
    eligible = requested_priority == 0 and is_shell_admin(author, config)
    return {
        "requested_priority": requested_priority,
        "priority": 0 if eligible else max(1, requested_priority),
        "priority_eligible": eligible,
        "priority_policy": P0_PRIORITY_POLICY,
    }

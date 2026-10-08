"""Stable identity for one control-scope Web management mutation."""

from forest.errors import ForestError
from forest.packages import digest_json


def control_management_operation_id(
    scope_kind: str, control_id: str, component_id: str, action_id: str,
    idempotency_key: str | None,
) -> str:
    if idempotency_key is None or not 1 <= len(idempotency_key) <= 128:
        raise ForestError(
            "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422
        )
    return "web:" + digest_json([
        f"forest.{scope_kind}-management-operation/1",
        control_id, component_id, action_id, idempotency_key,
    ])

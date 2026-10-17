"""Management contract for kernel-owned Core rescue grants."""

from __future__ import annotations

from forest.errors import ForestError
from forest.module_management_contract import ModuleConfigurationContract

_IDENTITY = {
    "binding_ref": {"type": "string", "minLength": 1, "maxLength": 256},
    "group_ref": {"type": "string", "pattern": "^[1-9][0-9]{0,19}$"},
    "principal_ref": {"type": "string", "pattern": "^qq:[1-9][0-9]{0,19}$"},
}
_ACTION_INPUT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "expected_revision": {"type": "integer", "minimum": 1},
        **_IDENTITY,
        "actions": {
            "type": "array",
            "maxItems": 4,
            "uniqueItems": True,
            "items": {"enum": ["status", "cancel", "disable", "reload"]},
        },
    },
    "required": ["expected_revision", *_IDENTITY, "actions"],
    "additionalProperties": False,
}
_ACTION_OUTPUT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.core-rescue-policy-receipt/1"},
        "operation_id": {"type": "string"},
        "revision": {"type": "integer", "minimum": 2},
        "changed": {"type": "boolean"},
    },
    "required": ["protocol", "operation_id", "revision", "changed"],
    "additionalProperties": False,
}
_QUERY_INPUT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}
_QUERY_OUTPUT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.core-rescue-policy/1"},
        "revision": {"type": "integer", "minimum": 1},
        "grants": {
            "type": "array",
            "maxItems": 32,
            "items": {
                "type": "object",
                "properties": {
                    **_IDENTITY,
                    "actions": _ACTION_INPUT["properties"]["actions"],
                    "changed_revision": {"type": "integer", "minimum": 2},
                },
                "required": [*_IDENTITY, "actions", "changed_revision"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["protocol", "revision", "grants"],
    "additionalProperties": False,
}


class RuntimeRescueManagementContract(ModuleConfigurationContract):
    """Expose separate rescue policy beside ordinary runtime configuration."""

    def __init__(self, configuration, *, policy, schema: dict, validator, apply_callback):
        super().__init__(
            configuration, schema=schema, validator=validator, apply_callback=apply_callback
        )
        self.policy = policy

    def describe(self) -> dict:
        value = super().describe()
        value["queries"] = [{
            "query_id": "rescue.policy.read",
            "input_schema": _QUERY_INPUT,
            "output_schema": _QUERY_OUTPUT,
            "required_role": "ADMIN",
        }]
        value["actions"] = [{
            "action_id": "rescue.grant.put",
            "priority": "P0",
            "input_schema": _ACTION_INPUT,
            "output_schema": _ACTION_OUTPUT,
        }]
        return value

    def query_required_role(self, query_id: str) -> str:
        if query_id != "rescue.policy.read":
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Rescue query does not exist", 404
            )
        return "ADMIN"

    async def query(self, query_id: str, payload: dict) -> dict:
        self.query_required_role(query_id)
        if payload != {}:
            raise ForestError("RESCUE_POLICY_INPUT_INVALID", "Rescue query input is invalid", 422)
        return await self.policy.read()

    async def action(
        self,
        action_id: str,
        *,
        operation_id: str,
        payload: dict,
        authorization=None,
        authorization_guard=None,
    ) -> dict:
        if action_id != "rescue.grant.put":
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Rescue action does not exist", 404
            )
        if not isinstance(payload, dict) or set(payload) != set(_ACTION_INPUT["properties"]):
            raise ForestError("RESCUE_POLICY_INPUT_INVALID", "Rescue action input is invalid", 422)
        actor = getattr(authorization, "web_subject_ref", None)
        if not isinstance(actor, str) or not actor:
            raise ForestError("WEB_AUTHORITY_TICKET_REQUIRED", "Web authority is required", 403)
        return await self.policy.put(
            operation_id=operation_id,
            actor_ref=actor,
            authorization=authorization,
            authorization_guard=authorization_guard,
            **payload,
        )

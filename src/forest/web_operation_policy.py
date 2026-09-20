"""Generation-bound Bot Web operation policy owned by ``kernel.authorization``."""

from __future__ import annotations

from forest.errors import ForestError

WEB_OPERATION_POLICY_PROTOCOL = "forest.web-operation-policy/1"
WEB_OPERATION_POLICY_DEFAULT = {
    "protocol": WEB_OPERATION_POLICY_PROTOCOL,
    "rules": [],
}
WEB_OPERATION_POLICY_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": WEB_OPERATION_POLICY_PROTOCOL},
        "rules": {
            "type": "array",
            "maxItems": 512,
            "items": {
                "type": "object",
                "properties": {
                    "target_component_id": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 128,
                    },
                    "action_key": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 200,
                    },
                    "enabled": {"type": "boolean"},
                    "allowed_roles": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 2,
                        "uniqueItems": True,
                        "items": {"enum": ["ADMIN", "AUDITOR"]},
                    },
                    "allowed_subject_refs": {
                        "oneOf": [
                            {"type": "null"},
                            {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 128,
                                "uniqueItems": True,
                                "items": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 128,
                                },
                            },
                        ]
                    },
                },
                "required": [
                    "target_component_id",
                    "action_key",
                    "enabled",
                    "allowed_roles",
                    "allowed_subject_refs",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["protocol", "rules"],
    "additionalProperties": False,
}


def validate_web_operation_policy(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {"protocol", "rules"}:
        raise ValueError("policy must contain protocol and rules")
    if value["protocol"] != WEB_OPERATION_POLICY_PROTOCOL or not isinstance(
        value["rules"], list
    ):
        raise ValueError("policy protocol or rules are invalid")
    if len(value["rules"]) > 512:
        raise ValueError("policy contains too many rules")
    identities = set()
    for rule in value["rules"]:
        required = {
            "target_component_id",
            "action_key",
            "enabled",
            "allowed_roles",
            "allowed_subject_refs",
        }
        if not isinstance(rule, dict) or set(rule) != required:
            raise ValueError("policy rule shape is invalid")
        component_id = _text(rule["target_component_id"], 128)
        action_key = _text(rule["action_key"], 200)
        identity = (component_id, action_key)
        if identity in identities:
            raise ValueError("policy rule identity is duplicated")
        identities.add(identity)
        if type(rule["enabled"]) is not bool:
            raise ValueError("policy enabled flag is invalid")
        roles = rule["allowed_roles"]
        if (
            not isinstance(roles, list)
            or not 1 <= len(roles) <= 2
            or len(roles) != len(set(roles))
            or any(role not in {"ADMIN", "AUDITOR"} for role in roles)
        ):
            raise ValueError("policy roles are invalid")
        subjects = rule["allowed_subject_refs"]
        if subjects is not None and (
            not isinstance(subjects, list)
            or not 1 <= len(subjects) <= 128
            or len(subjects) != len(set(subjects))
        ):
            raise ValueError("policy subjects are invalid")
        if subjects is not None:
            for subject in subjects:
                _text(subject, 128)


def authorize_web_operation(
    value: object,
    *,
    web_subject_ref: str,
    web_role: str,
    target_component_id: str,
    action_key: str,
) -> None:
    try:
        validate_web_operation_policy(value)
    except ValueError as exc:
        raise ForestError(
            "WEB_OPERATION_POLICY_INVALID", "Bot Web operation policy is invalid", 500
        ) from exc
    for rule in value["rules"]:
        if (
            rule["target_component_id"] != target_component_id
            or rule["action_key"] != action_key
        ):
            continue
        subjects = rule["allowed_subject_refs"]
        if (
            not rule["enabled"]
            or web_role not in rule["allowed_roles"]
            or (subjects is not None and web_subject_ref not in subjects)
        ):
            raise ForestError(
                "WEB_OPERATION_POLICY_DENIED",
                "Bot Web operation policy denies this operation",
                403,
            )
        return


def _text(value: object, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= maximum
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("policy text field is invalid")
    return value

"""OneBot-owned Binding management contract."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from forest.clean_management_contract import (
    CleanManagementActionResult,
    CleanModuleConfigurationContract,
)
from forest.errors import ForestError
from forest.storage.adapter_onebot_authority import OneBotPayloadRef
from forest.storage.payload_authority import PayloadSource

_BINDING_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "binding_id": {"type": "string", "minLength": 1, "maxLength": 256},
        "expected_revision": {"type": "integer", "minimum": 0},
        "enabled": {"type": "boolean"},
        "receive_enabled": {"type": "boolean"},
        "transport": {"enum": ["FORWARD_WEBSOCKET", "HTTP_POST"]},
        "config": {"type": "object"},
        "secret_ref": {"type": "string", "readOnly": True},
        "secret_digest": {"type": "string", "readOnly": True},
    },
    "required": [
        "binding_id",
        "expected_revision",
        "enabled",
        "receive_enabled",
        "transport",
        "config",
        "secret_ref",
        "secret_digest",
    ],
    "additionalProperties": False,
}


class CleanOneBotManagementContract(CleanModuleConfigurationContract):
    protocol = "forest.onebot-management/1"

    def __init__(self, configuration, *, repository, payloads, runtime) -> None:
        super().__init__(configuration, schema=_BINDING_SCHEMA)
        self.repository = repository
        self.payloads = payloads
        self.runtime = runtime

    def describe(self) -> dict:
        value = super().describe()
        value["queries"] = [{"query_id": "onebot.bindings.list", "max_items": 64}]
        value["actions"] = [
            {"action_id": "onebot.binding.plan"},
            {"action_id": "onebot.binding.apply", "reload_policy": "generation"},
        ]
        return value

    async def query(self, query_id: str, payload: dict) -> object:
        if query_id != "onebot.bindings.list":
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "OneBot query does not exist", 404
            )
        if payload:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Binding list takes no fields", 422
            )
        rows = await self.repository.list_desired_bindings()
        if len(rows) > 64:
            raise ForestError("ONEBOT_BINDING_LIMIT", "Binding count exceeds management bound", 503)
        result = []
        for row in rows:
            result.append(
                {
                    "binding_id": row["binding_id"],
                    "platform": row["platform"],
                    "self_id": row["self_id"],
                    "state": row["state"],
                    "desired_revision": row["desired_revision"],
                    "effective_revision": row["effective_revision"],
                    "binding_epoch": row["binding_epoch"],
                    "connection_epoch": row["connection_epoch"],
                    "desired_enabled": row["desired_enabled"],
                    "effective_enabled": row["effective_enabled"],
                    "receive_enabled": row["receive_enabled"],
                    "current_client_incarnation": row["current_client_incarnation"],
                    "error_code": row["error_code"],
                    "transport": row["transport"],
                    "config": await self.runtime.binding_configuration(row),
                    "secret_ref": row["secret_ref"],
                    "secret_digest": row["secret_digest"],
                }
            )
        return {"items": result}

    async def action(
        self,
        action_id: str,
        *,
        operation_id: str,
        payload: dict,
    ) -> CleanManagementActionResult:
        if action_id == "onebot.binding.plan":
            return CleanManagementActionResult(await self._plan(operation_id, payload))
        if action_id == "onebot.binding.apply":
            return await self._apply(operation_id, payload)
        raise ForestError(
            "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "OneBot action does not exist", 404
        )

    async def _plan(self, operation_id: str, payload: dict) -> dict:
        required = {
            "binding_id",
            "expected_revision",
            "enabled",
            "receive_enabled",
            "transport",
            "config",
            "secret_ref",
            "secret_digest",
        }
        if not isinstance(payload, dict) or set(payload) != required:
            raise ForestError(
                "ONEBOT_MANAGEMENT_PROFILE_INVALID", "Binding plan input is invalid", 422
            )
        binding_id = payload["binding_id"]
        try:
            binding_size = len(binding_id.encode("utf-8")) if isinstance(binding_id, str) else 0
        except UnicodeError:
            binding_size = 0
        if (
            not isinstance(binding_id, str)
            or not 1 <= binding_size <= 256
            or type(payload["expected_revision"]) is not int
            or payload["expected_revision"] < 0
            or type(payload["enabled"]) is not bool
            or type(payload["receive_enabled"]) is not bool
            or (payload["receive_enabled"] and not payload["enabled"])
            or payload["transport"] not in {"FORWARD_WEBSOCKET", "HTTP_POST"}
            or not isinstance(payload["secret_ref"], str)
            or not isinstance(payload["secret_digest"], str)
        ):
            raise ForestError(
                "ONEBOT_MANAGEMENT_PROFILE_INVALID", "Binding plan input is invalid", 422
            )
        desired = {
            row["binding_id"]: row for row in await self.repository.list_desired_bindings()
        }.get(binding_id)
        if desired is None:
            raise ForestError("BINDING_NOT_FOUND", "OneBot Binding does not exist", 404)
        if (
            payload["secret_ref"] != desired["secret_ref"]
            or payload["secret_digest"] != desired["secret_digest"]
        ):
            raise ForestError(
                "BINDING_SECRET_CHANGE_UNAVAILABLE",
                "Use the secret authority before changing a Binding credential",
                409,
            )
        if payload["transport"] != desired["transport"]:
            raise ForestError(
                "BINDING_TRANSPORT_CHANGE_UNAVAILABLE",
                "Configure transport secrets before changing the Binding transport",
                409,
            )
        config = self._validate_config(payload["transport"], payload["config"])
        plan_id = "binding-plan:" + hashlib.sha256(operation_id.encode()).hexdigest()
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="binding.config",
            owner_id=plan_id,
            media_type="application/json",
            content=json.dumps(
                config, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"),
            sources=(
                PayloadSource(
                    "adapter.onebot",
                    "MANAGEMENT_BINDING_CONFIGURATION",
                    binding_id,
                    payload["expected_revision"] + 1,
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        plan = await self.repository.plan_configuration(
            plan_id=plan_id,
            binding_id=binding_id,
            expected_revision=payload["expected_revision"],
            enabled=payload["enabled"],
            receive_enabled=payload["receive_enabled"],
            transport=payload["transport"],
            config=OneBotPayloadRef(
                saved.payload_id,
                saved.content_digest,
                "adapter.onebot",
                "binding.config",
                plan_id,
            ),
            secret_ref=payload["secret_ref"],
            secret_digest=payload["secret_digest"],
            actor_principal_ref="management/shared-token",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        return {
            "plan_id": plan["plan_id"],
            "binding_id": plan["binding_id"],
            "expected_revision": plan["expected_revision"],
            "expires_at": plan["expires_at"].isoformat(),
        }

    async def _apply(
        self, operation_id: str, payload: dict
    ) -> CleanManagementActionResult:
        if not isinstance(payload, dict) or set(payload) != {"plan_id"}:
            raise ForestError(
                "ONEBOT_MANAGEMENT_PROFILE_INVALID", "Binding apply input is invalid", 422
            )
        row = await self.repository.apply_configuration(
            operation_id=operation_id,
            plan_id=payload["plan_id"],
        )
        head = await self.repository.load_binding(row["binding_id"])
        return CleanManagementActionResult(
            {
                "binding_id": row["binding_id"],
                "revision": row["revision"],
                "effective_revision": head["effective_revision"],
            },
            replace_runtime_graph=head["effective_revision"] != row["revision"],
        )

    @staticmethod
    def _validate_config(transport: object, config: object) -> dict:
        if transport not in {"FORWARD_WEBSOCKET", "HTTP_POST"} or not isinstance(config, dict):
            raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot Binding config is invalid", 422)
        allowed = {
            "transport",
            "api_base_url",
            "websocket_url",
            "timeout_seconds",
            "max_connections",
            "event_capacity",
            "pending_capacity",
        }
        if set(config) - allowed or config.get("transport", transport) != transport:
            raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot Binding config is invalid", 422)
        endpoint_field = (
            "websocket_url" if transport == "FORWARD_WEBSOCKET" else "api_base_url"
        )
        schemes = {"ws", "wss"} if transport == "FORWARD_WEBSOCKET" else {"http", "https"}
        endpoint = config.get(endpoint_field)
        try:
            parsed = urlsplit(endpoint)
            valid_endpoint = (
                isinstance(endpoint, str)
                and len(endpoint) <= 2048
                and parsed.scheme in schemes
                and parsed.hostname is not None
                and parsed.username is None
                and parsed.password is None
                and not parsed.fragment
            )
        except (TypeError, ValueError, UnicodeError):
            valid_endpoint = False
        if not valid_endpoint:
            raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot endpoint is invalid", 422)
        if transport == "FORWARD_WEBSOCKET" and "api_base_url" in config:
            raise ForestError("ONEBOT_CONFIG_INVALID", "HTTP endpoint contradicts transport", 422)
        if transport == "HTTP_POST" and "websocket_url" in config:
            raise ForestError("ONEBOT_CONFIG_INVALID", "WebSocket endpoint contradicts transport", 422)
        numeric = {
            "timeout_seconds": (1, 120, (int, float)),
            "max_connections": (1, 128, (int,)),
            "event_capacity": (1, 4096, (int,)),
            "pending_capacity": (1, 1024, (int,)),
        }
        for field, (minimum, maximum, kinds) in numeric.items():
            if field in config and (
                type(config[field]) not in kinds or not minimum <= config[field] <= maximum
            ):
                raise ForestError("ONEBOT_CONFIG_INVALID", f"{field} is invalid", 422)
        return dict(config)

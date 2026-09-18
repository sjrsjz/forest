"""Agent-owned management contract for its immutable protocol and mutable preset."""

from __future__ import annotations

import hashlib
import json

from forest.agent_protocol import (
    CORE_PROMPT,
    CORE_PROMPT_SHA256,
    PARSER_REVISION,
    PROTOCOL_ID,
    PROTOCOL_VERSION,
)
from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payload_authority import PayloadSource

AGENT_MANAGEMENT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.agent.management-profile/1"},
        "agent_spec_revision": {"type": "string", "minLength": 1, "maxLength": 256},
        "preset_context": {
            "type": "array",
            "maxItems": 32,
            "items": {"type": "string"},
        },
        "run_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
        "max_steps": {"type": "integer", "minimum": 1, "maximum": 256},
        "output_tokens": {"type": "integer", "minimum": 1, "maximum": 131072},
        "stream": {"type": "boolean"},
    },
    "required": [
        "protocol",
        "agent_spec_revision",
        "preset_context",
        "run_timeout_seconds",
        "max_steps",
        "output_tokens",
        "stream",
    ],
    "additionalProperties": False,
}


class CleanAgentManagementContract(CleanModuleConfigurationContract):
    """Project private preset PayloadRefs into an operator-facing Agent profile."""

    protocol = "forest.agent-management/1"

    def __init__(self, configuration, *, payloads, storage_schema: dict) -> None:
        super().__init__(configuration, schema=AGENT_MANAGEMENT_SCHEMA)
        self.payloads = payloads
        self.storage_schema_digest = digest_json(storage_schema)

    def describe(self) -> dict:
        value = super().describe()
        value["agent_protocol"] = {
            "protocol_id": PROTOCOL_ID,
            "protocol_version": PROTOCOL_VERSION,
            "parser_revision": PARSER_REVISION,
            "digest": CORE_PROMPT_SHA256,
            "mutable": False,
        }
        return value

    async def read(self) -> dict:
        value = await super().read()
        value["desired"] = await self._project(value["desired"])
        value["effective"] = await self._project(value["effective"])
        value["core_prompt"] = {
            "protocol_id": PROTOCOL_ID,
            "protocol_version": PROTOCOL_VERSION,
            "parser_revision": PARSER_REVISION,
            "digest": CORE_PROMPT_SHA256,
            "mutable": False,
            "text": CORE_PROMPT,
        }
        return value

    async def plan(
        self,
        *,
        operation_id: str,
        expected_desired_revision: int,
        value: object,
    ) -> dict:
        profile = self._validate_profile(value)
        current = await self.configuration.desired(include_value=True)
        if (
            not isinstance(current.value, dict)
            or profile["agent_spec_revision"] != current.value.get("agent_spec_revision")
        ):
            raise ForestError(
                "AGENT_SPECIFICATION_IMMUTABLE",
                "Agent specification revision is owned by the installed module",
                409,
            )
        document = json.dumps(
            {"system": profile["preset_context"]},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        owner_id = "preset:management:" + hashlib.sha256(
            (operation_id + "\0" + digest_json(profile)).encode()
        ).hexdigest()
        epoch = await self.payloads.epoch()
        preset = await self.payloads.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind="preset.context",
            owner_id=owner_id,
            media_type="application/json",
            content=document,
            sources=(
                PayloadSource(
                    "agent.runtime",
                    "MANAGEMENT_PRESET",
                    owner_id,
                    expected_desired_revision + 1,
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        stored = {
            "protocol": "forest.agent.runtime/1",
            "agent_spec_revision": profile["agent_spec_revision"],
            "preset_revision": "preset:management/"
            + preset.content_digest.removeprefix("sha256:"),
            "preset": {
                "payload_id": preset.payload_id,
                "digest": preset.content_digest,
                "owner_id": owner_id,
            },
            "run_timeout_seconds": profile["run_timeout_seconds"],
            "max_steps": profile["max_steps"],
            "output_tokens": profile["output_tokens"],
            "stream": profile["stream"],
        }
        plan = await self.configuration.plan(
            operation_id=operation_id,
            expected_desired_revision=expected_desired_revision,
            value=stored,
            validation_digest=self.storage_schema_digest,
        )
        return {
            "plan_id": plan["plan_id"],
            "expected_desired_revision": plan["expected_desired_revision"],
            "input_digest": plan["input_digest"],
            "reload_policy": self.reload_policy,
            "state": plan["state"],
        }

    async def _project(self, configuration: dict) -> dict:
        stored = configuration["value"]
        if not isinstance(stored, dict) or not isinstance(stored.get("preset"), dict):
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent configuration is invalid", 500)
        reference = stored["preset"]
        try:
            async with self.payloads.store.pool.connection() as conn, conn.transaction():
                accepted = await self.payloads.verify_in(
                    conn,
                    payload_id=reference.get("payload_id"),
                    digest=reference.get("digest"),
                    owner_component_id="agent.runtime",
                    owner_kind="preset.context",
                    owner_id=reference.get("owner_id"),
                )
                body = await self.payloads.read_bytes_in(
                    conn,
                    accepted.payload_id,
                    expected_deletion_epoch=accepted.accepted_deletion_epoch,
                )
            document = json.loads(body)
            preset_context = document["system"]
        except (KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent preset is invalid", 500) from exc
        projected = dict(configuration)
        projected["value"] = {
            "protocol": "forest.agent.management-profile/1",
            "agent_spec_revision": stored.get("agent_spec_revision"),
            "preset_context": preset_context,
            "run_timeout_seconds": stored.get("run_timeout_seconds"),
            "max_steps": stored.get("max_steps"),
            "output_tokens": stored.get("output_tokens"),
            "stream": stored.get("stream"),
        }
        projected["preset"] = {
            "revision": stored.get("preset_revision"),
            "payload_id": reference.get("payload_id"),
            "digest": reference.get("digest"),
        }
        self._validate_profile(projected["value"], status=500)
        return projected

    @staticmethod
    def _validate_profile(value: object, *, status: int = 422) -> dict:
        required = {
            "protocol",
            "agent_spec_revision",
            "preset_context",
            "run_timeout_seconds",
            "max_steps",
            "output_tokens",
            "stream",
        }
        valid = isinstance(value, dict) and set(value) == required
        if valid:
            messages = value["preset_context"]
            try:
                valid = (
                    value["protocol"] == "forest.agent.management-profile/1"
                    and isinstance(value["agent_spec_revision"], str)
                    and 1 <= len(value["agent_spec_revision"].encode("utf-8")) <= 256
                    and isinstance(messages, list)
                    and len(messages) <= 32
                    and all(isinstance(item, str) for item in messages)
                    and sum(len(item.encode("utf-8")) for item in messages) <= 32768
                    and type(value["run_timeout_seconds"]) is int
                    and 1 <= value["run_timeout_seconds"] <= 86400
                    and type(value["max_steps"]) is int
                    and 1 <= value["max_steps"] <= 256
                    and type(value["output_tokens"]) is int
                    and 1 <= value["output_tokens"] <= 131072
                    and type(value["stream"]) is bool
                )
            except UnicodeError:
                valid = False
        if not valid:
            raise ForestError(
                "AGENT_MANAGEMENT_PROFILE_INVALID",
                "Agent preset or execution limits are invalid",
                status,
            )
        return value

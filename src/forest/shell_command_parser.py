"""Deterministic parser and ingress projection for the reserved group Shell."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.ingress_unit_of_work import ShellIngressCommand
from forest.storage.payload_authority import PayloadSource
from forest.storage.shell_command_authority import ShellPayloadRef

STATUS_ARGUMENT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.shell.arguments/1"},
        "command_id": {"const": "core.status"},
        "arguments": {"type": "object", "maxProperties": 0},
    },
    "required": ["protocol", "command_id", "arguments"],
    "additionalProperties": False,
}
STATUS_ARGUMENT_SCHEMA_DIGEST = digest_json(STATUS_ARGUMENT_SCHEMA)


def _maintenance_schema(command_id: str) -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.arguments/1"},
            "command_id": {"const": command_id},
            "arguments": {
                "type": "object",
                "properties": {
                    "expected_maintenance_epoch": {"type": "integer", "minimum": 1}
                },
                "required": ["expected_maintenance_epoch"],
                "additionalProperties": False,
            },
        },
        "required": ["protocol", "command_id", "arguments"],
        "additionalProperties": False,
    }


PAUSE_ARGUMENT_SCHEMA_DIGEST = digest_json(_maintenance_schema("core.pause"))
RESUME_ARGUMENT_SCHEMA_DIGEST = digest_json(_maintenance_schema("core.resume"))
_SCHEMA_DIGESTS = {
    "core.status": STATUS_ARGUMENT_SCHEMA_DIGEST,
    "core.pause": PAUSE_ARGUMENT_SCHEMA_DIGEST,
    "core.resume": RESUME_ARGUMENT_SCHEMA_DIGEST,
}


def _stable(kind: str, value: str) -> str:
    return f"shell:{kind}:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _tokenize(value: str) -> tuple[str, ...]:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 4096:
        raise ForestError("SHELL_SYNTAX_INVALID", "Shell command is too large", 422)
    tokens: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is None:
            if character in " \t":
                if current:
                    tokens.append("".join(current))
                    current.clear()
                index += 1
                continue
            if character in {"'", '"'}:
                quote = character
                index += 1
                continue
            current.append(character)
            index += 1
            continue
        if character == quote:
            quote = None
            index += 1
            continue
        if quote == '"' and character == "\\":
            index += 1
            if index >= len(value) or value[index] not in {'"', "\\"}:
                raise ForestError("SHELL_SYNTAX_INVALID", "Invalid Shell escape", 422)
            current.append(value[index])
            index += 1
            continue
        current.append(character)
        index += 1
    if quote is not None:
        raise ForestError("SHELL_SYNTAX_INVALID", "Unclosed Shell quote", 422)
    if current:
        tokens.append("".join(current))
    if len(tokens) > 64 or any(not token or len(token.encode("utf-8")) > 512 for token in tokens):
        raise ForestError("SHELL_SYNTAX_INVALID", "Shell token bounds were exceeded", 422)
    return tuple(tokens)


@dataclass(frozen=True)
class ParsedShellCommand:
    command_id: str
    command_version: int
    document: dict


class ShellCommandParser:
    def __init__(self, payloads) -> None:
        self.payloads = payloads

    @staticmethod
    def parse(command_text: str) -> ParsedShellCommand:
        try:
            tokens = _tokenize(command_text)
        except ForestError as exc:
            return ParsedShellCommand(
                "shell.invalid",
                1,
                {
                    "protocol": "forest.shell.arguments/1",
                    "command_id": "shell.invalid",
                    "arguments": {"error_code": exc.code},
                },
            )
        if tokens == ("core", "status"):
            return ParsedShellCommand(
                "core.status",
                1,
                {
                    "protocol": "forest.shell.arguments/1",
                    "command_id": "core.status",
                    "arguments": {},
                },
            )
        if len(tokens) in {3, 4} and tokens[:2] in {
            ("core", "pause"),
            ("core", "resume"),
        }:
            value = None
            if len(tokens) == 3 and tokens[2].startswith("--expect-maintenance="):
                value = tokens[2].partition("=")[2]
            elif len(tokens) == 4 and tokens[2] == "--expect-maintenance":
                value = tokens[3]
            parsed_epoch = int(value) if value is not None and value.isdecimal() else 0
            if 0 < parsed_epoch <= 9_223_372_036_854_775_807:
                command_id = "core." + tokens[1]
                return ParsedShellCommand(
                    command_id,
                    1,
                    {
                        "protocol": "forest.shell.arguments/1",
                        "command_id": command_id,
                        "arguments": {"expected_maintenance_epoch": parsed_epoch},
                    },
                )
        command_id = "shell.invalid"
        if tokens:
            candidate = ".".join(tokens[:2]).lower()
            if candidate and len(candidate.encode("utf-8")) <= 128:
                command_id = candidate
        return ParsedShellCommand(
            command_id,
            1,
            {
                "protocol": "forest.shell.arguments/1",
                "command_id": command_id,
                "arguments": {"error_code": "SHELL_COMMAND_NOT_FOUND"},
            },
        )

    async def persist(
        self,
        *,
        command_text: str,
        event_ref: str,
    ) -> tuple[ParsedShellCommand, ShellPayloadRef, ShellPayloadRef]:
        parsed = self.parse(command_text)
        owner_id = _stable("arguments", event_ref)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="shell.command",
            owner_kind="shell.arguments",
            owner_id=owner_id,
            media_type="application/json",
            content=_canonical(parsed.document),
            sources=(
                PayloadSource(
                    "shell.command",
                    "onebot.shell-command",
                    event_ref,
                    1,
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        arguments = ShellPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "shell.command",
            "shell.arguments",
            owner_id,
        )
        rejection_owner = _stable("rejection", event_ref)
        rejection_saved = await self.payloads.put_bytes(
            owner_component_id="shell.command",
            owner_kind="shell.result",
            owner_id=rejection_owner,
            media_type="application/json",
            content=_canonical(
                {"text": "Forest 控制命令已拒绝；请检查命令、管理员策略和审计记录。"}
            ),
            sources=(PayloadSource("shell.command", "onebot.shell-command", event_ref, 1),),
            parents=(saved.payload_id,),
            expected_deletion_epoch=epoch,
        )
        rejection = ShellPayloadRef(
            rejection_saved.payload_id,
            rejection_saved.content_digest,
            "shell.command",
            "shell.result",
            rejection_owner,
        )
        return parsed, arguments, rejection

    async def validate_admission(
        self,
        *,
        command_text: str,
        command_id: str,
        command_version: int,
        argument_schema_digest: str,
        arguments: ShellPayloadRef,
    ) -> bool:
        if (
            command_version != 1
            or _SCHEMA_DIGESTS.get(command_id) != argument_schema_digest
        ):
            return False
        parsed = self.parse(command_text)
        if parsed.command_id != command_id:
            return False
        if "error_code" in parsed.document.get("arguments", {}):
            return False
        body = await self.payloads.read_bytes(arguments.payload_id)
        return body == _canonical(parsed.document)


def direct_shell_text(normalized: dict) -> str | None:
    """Return only direct text after an optional leading self mention."""

    segments = normalized.get("segments")
    self_id = normalized.get("self_id")
    if not isinstance(segments, list) or not isinstance(self_id, str):
        return None
    index = 0
    if segments and isinstance(segments[0], dict):
        first = segments[0]
        if first.get("type") == "mention" and first.get("external_user_id") == self_id:
            index = 1
    texts: list[str] = []
    for segment in segments[index:]:
        if not isinstance(segment, dict) or segment.get("type") != "text":
            if texts and "".join(texts).lstrip(" \t").startswith("#forest"):
                return "".join(texts) + " __non_text_segment__"
            return None
        text = segment.get("text")
        if not isinstance(text, str):
            return None
        texts.append(text)
    return "".join(texts) if texts else None


def ingress_command(
    match,
    parsed: ParsedShellCommand,
    arguments: ShellPayloadRef,
    rejection: ShellPayloadRef,
) -> ShellIngressCommand:
    identity = match.proof.event_ref
    return ShellIngressCommand(
        request_id=_stable("request", identity),
        proof=match.proof,
        command_id=parsed.command_id,
        command_version=parsed.command_version,
        arguments=arguments,
        idempotency_key=_stable("idempotency", identity),
        rejection=rejection,
    )

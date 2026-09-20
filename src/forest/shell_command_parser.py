"""Deterministic parser and ingress projection for the reserved group Shell."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.storage.ingress_unit_of_work import ShellIngressCommand
from forest.storage.payload_authority import PayloadSource
from forest.storage.shell_command_authority import ShellPayloadRef


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
    def __init__(self, payloads, commands=()) -> None:
        self.payloads = payloads
        self.commands = tuple(commands)
        self._by_words = {
            tuple(item.command_id.split(".")): item for item in self.commands
        }
        if len(self._by_words) != len(self.commands):
            raise ValueError("Shell command syntax identities repeat")

    @staticmethod
    def _invalid(command_id: str, error_code: str) -> ParsedShellCommand:
        return ParsedShellCommand(
            command_id,
            1,
            {
                "protocol": "forest.shell.arguments/1",
                "command_id": command_id,
                "arguments": {"error_code": error_code},
            },
        )

    @staticmethod
    def _coerce(raw: str | None, schema: dict):
        kind = schema.get("type")
        if kind is None and isinstance(schema.get("enum"), list) and all(
            isinstance(item, str) for item in schema["enum"]
        ):
            kind = "string"
        if kind == "boolean":
            if raw is None:
                return True
            if raw in {"true", "false"}:
                return raw == "true"
            raise ValueError
        if raw is None:
            raise ValueError
        if kind == "integer":
            if not raw.isdecimal():
                raise ValueError
            return int(raw)
        if kind == "string":
            return raw
        raise ValueError

    def parse(self, command_text: str) -> ParsedShellCommand:
        try:
            tokens = _tokenize(command_text)
        except ForestError as exc:
            return self._invalid("shell.invalid", exc.code)
        matches = [
            (words, contribution)
            for words, contribution in self._by_words.items()
            if tokens[: len(words)] == words
        ]
        if not matches:
            candidate = ".".join(tokens[:2]).lower() if tokens else "shell.invalid"
            command_id = candidate if 0 < len(candidate.encode("utf-8")) <= 128 else "shell.invalid"
            return self._invalid(command_id, "SHELL_COMMAND_NOT_FOUND")
        words, contribution = max(matches, key=lambda item: len(item[0]))
        argument_schema = contribution.argument_schema["properties"]["arguments"]
        properties = argument_schema.get("properties", {})
        option_names = {
            name.replace("_", "-"): name for name in properties
        } | dict(contribution.option_aliases)
        arguments = {}
        remaining = list(tokens[len(words) :])
        try:
            while remaining:
                option = remaining.pop(0)
                if not option.startswith("--") or option == "--":
                    raise ValueError
                key, separator, raw = option[2:].partition("=")
                name = option_names.get(key)
                if name not in properties or name in arguments:
                    raise ValueError
                if not separator and properties[name].get("type") != "boolean":
                    if not remaining:
                        raise ValueError
                    raw = remaining.pop(0)
                arguments[name] = self._coerce(raw if separator or raw else None, properties[name])
        except ValueError:
            return self._invalid(contribution.command_id, "SHELL_COMMAND_NOT_FOUND")
        document = {
            "protocol": "forest.shell.arguments/1",
            "command_id": contribution.command_id,
            "arguments": arguments,
        }
        if not Draft202012Validator(contribution.argument_schema).is_valid(document):
            return self._invalid(contribution.command_id, "SHELL_COMMAND_NOT_FOUND")
        return ParsedShellCommand(
            contribution.command_id,
            contribution.command_version,
            document,
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
            or not any(
                item.command_id == command_id
                and item.command_version == command_version
                and item.argument_schema_digest == argument_schema_digest
                for item in self.commands
            )
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

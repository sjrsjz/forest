"""Typed execution boundary between the Shell dispatcher and command modules."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShellCommandExecutionRequest:
    command_id: str
    command_version: int
    arguments: dict
    request_id: str
    principal_ref: str
    conversation_ref: str
    claim_token: str | None = None
    shell_generation_id: str | None = None

    def __post_init__(self) -> None:
        if (
            not self.command_id
            or type(self.command_version) is not int
            or self.command_version < 1
            or not isinstance(self.arguments, dict)
            or not self.request_id
            or not self.principal_ref
            or not self.conversation_ref
            or (self.claim_token is not None and not self.claim_token)
            or (self.shell_generation_id is not None and not self.shell_generation_id)
        ):
            raise ValueError("Shell command execution request is invalid")


@dataclass(frozen=True)
class ShellCommandExecutionResult:
    text: str

    def __post_init__(self) -> None:
        if not isinstance(self.text, str) or not self.text or len(self.text.encode("utf-8")) > 65536:
            raise ValueError("Shell command execution result is invalid")

"""Small process-local gate for the reserved ``#forest`` namespace."""

from __future__ import annotations

import hashlib
import re
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.storage.shell_command_authority import ShellAdmissionProof

_PREFIX = re.compile(r"#forest(?:\s|$)")


@dataclass(frozen=True)
class ShellGateMatch:
    proof: ShellAdmissionProof
    command_text: str


class CoreShellGate:
    """Recognize a bounded direct prefix and issue short-lived process evidence.

    The gate has no administrator list, command registry, database access or
    business authorization semantics.  ``shell.command`` owns those decisions.
    """

    def __init__(
        self,
        bot_id: str,
        *,
        max_bytes: int = 4096,
        proof_ttl_seconds: int = 20,
        proof_capacity: int = 4096,
    ) -> None:
        if not isinstance(bot_id, str) or not bot_id:
            raise ValueError("bot_id")
        if type(max_bytes) is not int or not 64 <= max_bytes <= 65536:
            raise ValueError("max_bytes")
        if type(proof_ttl_seconds) is not int or not 1 <= proof_ttl_seconds <= 60:
            raise ValueError("proof_ttl_seconds")
        if type(proof_capacity) is not int or proof_capacity < 1:
            raise ValueError("proof_capacity")
        self.bot_id = bot_id
        self.max_bytes = max_bytes
        self.proof_ttl_seconds = proof_ttl_seconds
        self.proof_capacity = proof_capacity
        self._proofs: OrderedDict[str, tuple[float, str, str]] = OrderedDict()

    @staticmethod
    def _bounded(value, field: str, limit: int) -> str:
        if (
            not isinstance(value, str)
            or not 1 <= len(value.encode("utf-8")) <= limit
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise ForestError("SHELL_GATE_INPUT_INVALID", f"Invalid {field}", 422)
        return value

    def inspect(
        self,
        *,
        text: str,
        binding_ref: str,
        binding_epoch: int,
        connection_epoch: int,
        client_incarnation: str,
        adapter_generation_id: str,
        executor_fence_digest: str,
        conversation_ref: str,
        event_ref: str,
        principal_ref: str,
        now: datetime | None = None,
    ) -> ShellGateMatch | None:
        if not isinstance(text, str):
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Shell text must be a string", 422)
        direct = text.lstrip(" \t")
        if _PREFIX.match(direct) is None:
            return None
        if len(direct.encode("utf-8")) > self.max_bytes or any(
            character in direct for character in ("\r", "\n", "\x00")
        ):
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Reserved Shell input is invalid", 422)
        for value, field, limit in (
            (binding_ref, "binding_ref", 256),
            (client_incarnation, "client_incarnation", 256),
            (adapter_generation_id, "adapter_generation_id", 256),
            (conversation_ref, "conversation_ref", 256),
            (event_ref, "event_ref", 512),
            (principal_ref, "principal_ref", 256),
        ):
            self._bounded(value, field, limit)
        if type(binding_epoch) is not int or binding_epoch <= 0:
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Invalid Binding epoch", 422)
        if type(connection_epoch) is not int or connection_epoch <= 0:
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Invalid connection epoch", 422)
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", executor_fence_digest):
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Invalid executor fence", 422)
        issued_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        command_text = direct[len("#forest") :].lstrip(" \t")
        command_digest = "sha256:" + hashlib.sha256(direct.encode("utf-8")).hexdigest()
        nonce = secrets.token_urlsafe(24)
        proof = ShellAdmissionProof(
            bot_id=self.bot_id,
            binding_ref=binding_ref,
            binding_epoch=binding_epoch,
            connection_epoch=connection_epoch,
            client_incarnation=client_incarnation,
            adapter_generation_id=adapter_generation_id,
            executor_fence_digest=executor_fence_digest,
            conversation_ref=conversation_ref,
            event_ref=event_ref,
            principal_ref=principal_ref,
            command_line_digest=command_digest,
            nonce=nonce,
            expires_at=issued_at + timedelta(seconds=self.proof_ttl_seconds),
        )
        self._proofs[nonce] = (
            time.monotonic() + self.proof_ttl_seconds,
            proof.proof_digest,
            command_text,
        )
        self._proofs.move_to_end(nonce)
        while len(self._proofs) > self.proof_capacity:
            self._proofs.popitem(last=False)
        return ShellGateMatch(proof, command_text)

    def verify(self, proof: ShellAdmissionProof) -> str:
        if type(proof) is not ShellAdmissionProof or proof.bot_id != self.bot_id:
            raise ForestError("SHELL_PROOF_INVALID", "Shell proof issuer differs", 403)
        saved = self._proofs.get(proof.nonce)
        if saved is None or saved[0] <= time.monotonic() or saved[1] != proof.proof_digest:
            self._proofs.pop(proof.nonce, None)
            raise ForestError("SHELL_PROOF_INVALID", "Shell proof is unavailable", 403)
        self._proofs.move_to_end(proof.nonce)
        return saved[2]

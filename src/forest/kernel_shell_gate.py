"""Stable kernel recognition of the reserved group Shell prefix."""

from __future__ import annotations

import hashlib
import json
import re

from forest.errors import ForestError

_RESCUE_TARGET = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,255}\Z")
_RESCUE_REVISION = re.compile(r"[1-9][0-9]{0,18}\Z")


def _inspect_core_rescue(gate: dict) -> dict:
    if (
        not isinstance(gate, dict)
        or gate.get("protocol") != "forest.shell-gate/1"
        or gate.get("claimed") is not True
        or not isinstance(gate.get("command_text"), str)
        or type(gate.get("text_only")) is not bool
        or len(gate["command_text"].encode("utf-8")) > 4096
    ):
        raise ForestError("CORE_RESCUE_GATE_INVALID", "Core rescue gate is invalid", 422)
    text = gate["command_text"]
    if not text.startswith("core") or (len(text) > 4 and not text[4].isspace()):
        return {"protocol": "forest.core-rescue-inspection/1", "claimed": False}
    result = {
        "protocol": "forest.core-rescue-inspection/1",
        "claimed": True,
        "action": None,
        "target_ref": None,
        "expected_revision": None,
        "error_code": "CORE_RESCUE_SYNTAX_INVALID",
    }
    if not gate["text_only"] or any(char.isspace() and char not in " \t" for char in text):
        return result
    parts = text.split()
    if len(parts) < 2 or parts[0] != "core":
        return result
    action = parts[1]
    if action not in {"status", "cancel", "disable", "reload"}:
        return result
    result["action"] = action
    if action == "status" and len(parts) == 2:
        result["error_code"] = None
        return result
    if action == "cancel":
        if len(parts) != 5 or _RESCUE_TARGET.fullmatch(parts[2]) is None:
            return result
        option, revision = parts[3:]
        if option != "--expect-revision":
            return result
        result["target_ref"] = parts[2]
    elif action in {"disable", "reload"}:
        if len(parts) != 4:
            return result
        option, revision = parts[2:]
        if option != ("--expect-maintenance" if action == "disable" else "--expect-graph"):
            return result
    else:
        return result
    if _RESCUE_REVISION.fullmatch(revision) is None:
        return result
    expected = int(revision)
    if expected > 9223372036854775807:
        return result
    result["expected_revision"] = expected
    result["error_code"] = None
    return result


def core_rescue_inspect_service(_context):
    async def inspect(request):
        if (
            not isinstance(request, dict)
            or set(request) != {"protocol", "gate"}
            or request.get("protocol") != "forest.core-rescue-inspect-request/1"
        ):
            raise ForestError("CORE_RESCUE_INPUT_INVALID", "Core rescue request is invalid", 422)
        return _inspect_core_rescue(request["gate"])

    return inspect


def _inspect(normalized: dict) -> dict:
    segments = normalized.get("segments")
    self_id = normalized.get("self_id")
    if not isinstance(segments, list) or not isinstance(self_id, str):
        raise ForestError("SHELL_GATE_INPUT_INVALID", "Shell gate input is invalid", 422)
    index = 0
    if segments and isinstance(segments[0], dict):
        first = segments[0]
        if first.get("type") == "mention" and first.get("external_user_id") == self_id:
            index = 1
    texts = []
    text_only = True
    for segment in segments[index:]:
        if not isinstance(segment, dict) or segment.get("type") != "text":
            text_only = False
            break
        value = segment.get("text")
        if not isinstance(value, str):
            text_only = False
            break
        texts.append(value)
    value = "".join(texts).lstrip(" 	")
    if value == "#forest":
        command_text = ""
    elif value.startswith("#forest") and len(value) > 7 and value[7].isspace():
        separator = value[7]
        command_text = value[7:].lstrip(" 	") if separator in " 	" else ""
        if separator not in " 	":
            text_only = False
    else:
        return {"protocol": "forest.shell-gate/1", "claimed": False}
    if len(command_text.encode("utf-8")) > 4096:
        command_text = ""
        text_only = False
    return {
        "protocol": "forest.shell-gate/1", "claimed": True,
        "command_text": command_text, "text_only": text_only,
    }


def shell_gate_inspect_service(_context):
    async def inspect(request):
        if (
            not isinstance(request, dict)
            or request.get("protocol") != "forest.shell-gate-request/1"
            or set(request) != {"protocol", "normalized"}
            or not isinstance(request["normalized"], dict)
        ):
            raise ForestError("SHELL_GATE_INPUT_INVALID", "Shell gate request is invalid", 422)
        return _inspect(request["normalized"])
    return inspect

class KernelShellGateUnitOfWorkParticipant:
    """Prove Shell admission from the event staged in this UnitOfWork."""

    component_id = "kernel.runtime"

    def __init__(self, runtime, payloads) -> None:
        self.runtime = runtime
        self.service = runtime.service
        self.payloads = payloads

    @property
    def generation_id(self) -> str:
        identity = self.runtime.generation_identity
        if identity is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "Runtime Generation is not bound", 503,
            )
        return identity.generation_id

    async def invoke_in(self, conn, name: str, envelope: dict, **_invocation) -> dict:
        def invalid() -> None:
            raise ForestError(
                "SHELL_GATE_PROOF_INVALID",
                "Shell ingress does not match its admitted event", 409,
            )

        if name not in {"shell.gate-prove-ingress", "core.rescue.prove-ingress"}:
            invalid()
        if not isinstance(envelope, dict) or envelope.get("protocol") != "forest.uow-step/1":
            invalid()
        caller = envelope.get("caller")
        plan = envelope.get("input")
        prior = envelope.get("prior")
        if (
            not isinstance(caller, dict)
            or caller.get("component_id") != "adapter.onebot"
            or not isinstance(caller.get("generation_id"), str)
            or not isinstance(plan, dict)
            or not isinstance(prior, dict)
            or set(prior) != {"adapter", "conversation"}
        ):
            invalid()
        adapter = plan.get("adapter")
        conversation = plan.get("conversation")
        shell = plan.get(
            "shell" if name == "shell.gate-prove-ingress" else "rescue"
        )
        gate = plan.get("gate")
        if not all(isinstance(item, dict) for item in (
            adapter, conversation, shell, gate,
            prior["adapter"], prior["conversation"],
        )):
            invalid()
        event = adapter.get("event")
        normalized_ref = event.get("normalized") if isinstance(event, dict) else None
        raw_ref = event.get("raw") if isinstance(event, dict) else None
        event_ref = event.get("event_id") if isinstance(event, dict) else None
        if (
            not isinstance(event_ref, str) or not event_ref
            or not isinstance(normalized_ref, dict)
            or not isinstance(raw_ref, dict)
            or normalized_ref != conversation.get("content")
            or prior["adapter"].get("event_id") != event_ref
            or prior["adapter"].get("binding_ref") != adapter.get("binding_ref")
            or prior["adapter"].get("binding_epoch") != adapter.get("binding_epoch")
            or prior["conversation"].get("conversation_id") != conversation.get("conversation_id")
            or prior["conversation"].get("event_id") != conversation.get("event_id")
            or conversation.get("source_event_ref") != event_ref
            or conversation.get("binding_ref") != adapter.get("binding_ref")
            or conversation.get("author_subject_ref") != event.get("author_subject_ref")
            or conversation.get("conversation_id") != event.get("conversation_ref")
            or shell.get("event_ref") != event_ref
            or shell.get("binding_ref") != adapter.get("binding_ref")
            or shell.get("binding_epoch") != adapter.get("binding_epoch")
            or shell.get("client_incarnation") != adapter.get("client_incarnation")
            or type(shell.get("connection_epoch")) is not int
            or shell["connection_epoch"] < 1
            or shell.get("principal_ref") != event.get("author_subject_ref")
            or shell.get("conversation_ref") != event.get("conversation_ref")
            or shell.get("adapter_generation_id") != caller["generation_id"]
            or shell.get("received_at") != event.get("received_at")
            or shell.get("admission_deadline") != event.get("classification_deadline")
            or not isinstance(prior["conversation"].get("ingress_seq"), int)
            or prior["conversation"]["ingress_seq"] < 1
            or not isinstance(gate.get("self_id"), str)
            or not gate["self_id"]
            or _invocation.get("causal_root_kind") != "adapter.platform-frame"
        ):
            invalid()
        await self.payloads.verify_in(
            conn,
            payload_id=raw_ref.get("payload_id"),
            digest=raw_ref.get("digest"),
            owner_component_id="adapter.onebot",
            owner_kind="onebot.raw-event",
            owner_id=event_ref,
        )
        raw_body = await self.payloads.read_bytes_in(conn, raw_ref["payload_id"])
        if len(raw_body) > 262144:
            invalid()
        origin = plan.get("shell_origin")
        if not isinstance(origin, dict) or set(origin) != {"raw_frame"}:
            invalid()
        try:
            raw_frame = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            invalid()
        if not isinstance(raw_frame, dict) or raw_frame != origin["raw_frame"]:
            invalid()
        frame_digest = hashlib.sha256(
            adapter["binding_ref"].encode("utf-8")
            + b"\0"
            + str(shell["connection_epoch"]).encode("ascii")
            + b"\0"
            + raw_body
        ).hexdigest()
        if _invocation.get("causal_root_id") != "onebot-frame:" + frame_digest:
            invalid()
        await self.payloads.verify_in(
            conn,
            payload_id=normalized_ref.get("payload_id"),
            digest=normalized_ref.get("digest"),
            owner_component_id="adapter.onebot",
            owner_kind="onebot.normalized-event",
            owner_id=event_ref,
        )
        body = await self.payloads.read_bytes_in(conn, normalized_ref["payload_id"])
        if len(body) > 262144 or conversation.get("content_byte_length") != len(body):
            invalid()
        try:
            normalized = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            invalid()
        if not isinstance(normalized, dict) or set(normalized) != {"parts"}:
            invalid()
        inspection = _inspect({"self_id": gate["self_id"], "segments": normalized["parts"]})
        if inspection.get("claimed") is not True or inspection != gate.get("inspection"):
            invalid()
        core_inspection = None
        if name == "core.rescue.prove-ingress":
            core_inspection = _inspect_core_rescue(inspection)
            if core_inspection.get("claimed") is not True or core_inspection != shell.get(
                "inspection"
            ):
                invalid()
        result = {
            "protocol": "forest.shell-ingress-proof/1",
            "event_ref": event_ref,
            "binding_ref": adapter["binding_ref"],
            "binding_epoch": adapter["binding_epoch"],
            "conversation_ref": event["conversation_ref"],
            "principal_ref": event["author_subject_ref"],
            "ingress_seq": prior["conversation"]["ingress_seq"],
            "adapter_generation_id": caller["generation_id"],
            "received_at": event["received_at"],
            "admission_deadline": event["classification_deadline"],
            "normalized_payload_digest": normalized_ref["digest"],
            "self_id": gate["self_id"],
        }
        if core_inspection is not None:
            result["core_inspection"] = core_inspection
        return result

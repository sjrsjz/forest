"""Removable Shell arguments, outcomes, and terminal delivery receipts."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from forest.errors import ForestError
from forest.packages import digest_json
from forest.shell_registry_v10 import get_command_spec
from forest.storage.payloads import MAX_PARENTS, PayloadStore

UNAVAILABLE = frozenset({
    "PAYLOAD_ERASED",
    "PAYLOAD_INCOMPLETE",
    "SOURCE_SUPPRESSED",
    "SOURCE_LINK_SCOPE_LIMIT",
    "PAYLOAD_UNAVAILABLE",
})
ROLES = {
    "arguments": ("shell.arguments", "shell-arguments-v1:"),
    "outcome": ("control.result", "shell-outcome-v1:"),
    "delivery": ("control.outbox", "shell-delivery-v1:"),
}
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")


def _clone(value, *, maximum=65536):
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
        if len(encoded) > maximum:
            raise ValueError("too large")
        return json.loads(encoded)
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ForestError("SHELL_PAYLOAD_INVALID", "Shell content must be bounded JSON", 422) from exc


def owner_id(role: str, command_id: str) -> str:
    try:
        _, prefix = ROLES[role]
    except KeyError:
        raise ForestError("SHELL_PAYLOAD_INVALID", "Unknown Shell payload role", 422) from None
    if not isinstance(command_id, str) or not 1 <= len(command_id.encode()) <= 256:
        raise ForestError("SHELL_PAYLOAD_INVALID", "Shell command identity is invalid", 422)
    return prefix + digest_json([command_id])


def arguments_envelope(command_name, gate_version, arguments):
    values = _clone(arguments)
    if not isinstance(values, dict):
        raise ForestError("SHELL_ARGUMENTS_INVALID", "Shell arguments must be an object", 422)
    spec = get_command_spec(command_name, gate_version)
    if spec is None and values:
        raise ForestError(
            "SHELL_ARGUMENTS_INVALID", "An unsupported Shell command cannot retain arguments", 422
        )
    return {
        "gate_version": gate_version,
        "command_name": command_name,
        "schema_digest": spec.schema_digest if spec is not None else None,
        "arguments": values,
    }


def arguments_metadata(command_name, gate_version, arguments):
    envelope = arguments_envelope(command_name, gate_version, arguments)
    return {
        "schema_digest": envelope["schema_digest"],
        "argument_keys": sorted(envelope["arguments"]),
    }


def outcome_metadata(value):
    value = _clone(value)
    if not isinstance(value, dict):
        raise ForestError("INVALID_COMMAND_RESULT", "Control outcome must be an object", 422)
    code = value.get("code")
    if not isinstance(code, str) or not _CODE.fullmatch(code):
        raise ForestError("INVALID_COMMAND_RESULT", "Control outcome code is invalid", 422)
    return {"code": code}


def delivery_metadata(value, *, legacy_untracked=False):
    value = _clone(value)
    if value is not None and not isinstance(value, dict):
        raise ForestError("INVALID_COMMAND_RESULT", "Control delivery receipt is invalid", 422)
    if legacy_untracked:
        return {"legacy_untracked": True}
    required = {
        "intent_id",
        "operation_id",
        "send_lane",
        "conversation_id",
        "response_seq",
        "command_id",
        "control_seq",
    }
    if value is None or set(value) != required:
        raise ForestError("INVALID_COMMAND_RESULT", "Control delivery receipt is incomplete", 422)
    if value["send_lane"] != "CONTROL" or value["response_seq"] is not None:
        raise ForestError("INVALID_COMMAND_RESULT", "Control delivery receipt has the wrong lane", 422)
    for key in ("intent_id", "operation_id", "conversation_id", "command_id"):
        if not isinstance(value[key], str) or not 1 <= len(value[key].encode()) <= 256:
            raise ForestError("INVALID_COMMAND_RESULT", "Control delivery identity is invalid", 422)
    if type(value["control_seq"]) is not int or value["control_seq"] < 1:
        raise ForestError("INVALID_COMMAND_RESULT", "Control delivery sequence is invalid", 422)
    return {key: value[key] for key in required}


class ShellPayloadStore:
    def __init__(self, store):
        self.store, self.payloads = store, PayloadStore(store)

    async def _put_in(self, conn, *, role, command_id, value, parents, epoch):
        parents = tuple(sorted(set(parents)))
        if not parents or len(parents) > MAX_PARENTS:
            raise ForestError("SHELL_PAYLOAD_PARENTS_INVALID", "Shell content needs real parents", 422)
        kind, _ = ROLES[role]
        ref = await self.payloads.put_json_in(
            conn,
            owner_kind=kind,
            owner_id=owner_id(role, command_id),
            value=value,
            parents=parents,
            expected_deletion_epoch=epoch,
        )
        if ref.state != "ACTIVE":
            raise ForestError("CONTROL_CONTENT_REVOKED", "Shell content is unavailable", 409)
        return ref

    async def put_arguments_in(
        self, conn, *, command_id, command_name, gate_version, arguments, parent, epoch
    ):
        return await self._put_in(
            conn,
            role="arguments",
            command_id=command_id,
            value=arguments_envelope(command_name, gate_version, arguments),
            parents=(parent,),
            epoch=epoch,
        )

    async def put_outcome_in(self, conn, *, command_id, outcome, parents, epoch):
        outcome_metadata(outcome)
        return await self._put_in(
            conn,
            role="outcome",
            command_id=command_id,
            value={"command_id": command_id, "outcome": _clone(outcome)},
            parents=parents,
            epoch=epoch,
        )

    async def put_delivery_in(self, conn, *, command_id, delivery, parents, epoch):
        delivery_metadata(delivery)
        return await self._put_in(
            conn,
            role="delivery",
            command_id=command_id,
            value={"command_id": command_id, "delivery": _clone(delivery)},
            parents=parents,
            epoch=epoch,
        )

    @staticmethod
    def _unavailable(required):
        if required:
            raise ForestError("CONTROL_CONTENT_REVOKED", "Shell content is unavailable", 409)
        return None, "UNAVAILABLE"

    async def _load_in(
        self, conn, row, *, role, payload_key, digest_key, required_parent, epoch, required
    ):
        if not isinstance(row, Mapping):
            raise ForestError("SHELL_PAYLOAD_MISSING", "Shell payload identity is absent", 409)
        command_id = row.get("command_id")
        payload_id, expected_digest = row.get(payload_key), row.get(digest_key)
        if payload_id is None or expected_digest is None:
            if required:
                raise ForestError("SHELL_PAYLOAD_MISSING", "Shell payload identity is absent", 409)
            return None, "UNAVAILABLE"
        kind, _ = ROLES[role]
        expected_owner = owner_id(role, command_id)
        if not isinstance(expected_digest, str) or not _DIGEST.fullmatch(expected_digest):
            raise ForestError("SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell payload digest is invalid", 409)
        header = await self.payloads._header_in(conn, payload_id)
        manifest = header["manifest"]
        expected_id = "payload:" + digest_json([self.store.bot_id, kind, expected_owner])
        if (
            header["bot_id"] != self.store.bot_id
            or header["owner_kind"] != kind
            or header["owner_id"] != expected_owner
            or header["encoding"] != "json"
            or header["digest"] != expected_digest
            or payload_id != expected_id
            or not isinstance(manifest, dict)
            or set(manifest) != {"parents", "sources"}
            or not isinstance(manifest["parents"], list)
            or required_parent not in manifest["parents"]
        ):
            raise ForestError(
                "SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell payload identity differs", 409
            )
        actual = await (
            await conn.execute(
                "SELECT parent_payload_id FROM public.bot_payload_derivations "
                "WHERE bot_id=%s AND payload_id=%s ORDER BY parent_payload_id LIMIT %s",
                (self.store.bot_id, payload_id, MAX_PARENTS + 1),
            )
        ).fetchall()
        if [item["parent_payload_id"] for item in actual] != manifest["parents"]:
            raise ForestError(
                "SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell payload parent facts differ", 409
            )
        try:
            body = await self.payloads.read_json_in(
                conn, payload_id, expected_deletion_epoch=epoch
            )
        except ForestError as exc:
            if exc.code in UNAVAILABLE:
                return self._unavailable(required)
            raise
        return body, "ACTIVE"

    async def hydrate_arguments_in(self, conn, row, *, epoch, required=True):
        source = await (
            await conn.execute(
                "SELECT payload_id FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, row["event_id"]),
            )
        ).fetchone()
        if not source or not source["payload_id"]:
            raise ForestError("SHELL_PAYLOAD_MISSING", "Shell source input is absent", 409)
        body, state = await self._load_in(
            conn,
            row,
            role="arguments",
            payload_key="arguments_payload_id",
            digest_key="arguments_payload_digest",
            required_parent=source["payload_id"],
            epoch=epoch,
            required=required,
        )
        arguments = None
        if state == "ACTIVE":
            expected = arguments_envelope(row["command_name"], row["gate_version"], body.get("arguments"))
            if body != expected or arguments_metadata(
                row["command_name"], row["gate_version"], body["arguments"]
            ) != row.get("arguments_metadata"):
                raise ForestError(
                    "SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell arguments differ from metadata", 409
                )
            arguments = body["arguments"]
        return {**row, "arguments": arguments, "arguments_payload_state": state}

    async def hydrate_result_in(self, conn, row, *, epoch, required=False):
        if row.get("status") != "COMPLETED":
            return {
                **row,
                "outcome": None,
                "delivery": None,
                "outcome_payload_state": "UNAVAILABLE",
                "delivery_payload_state": "UNAVAILABLE",
            }
        source = await (
            await conn.execute(
                "SELECT payload_id FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, row["event_id"]),
            )
        ).fetchone()
        if not source or not source["payload_id"]:
            raise ForestError("SHELL_PAYLOAD_MISSING", "Shell source input is absent", 409)
        outcome_body, outcome_state = await self._load_in(
            conn,
            row,
            role="outcome",
            payload_key="outcome_payload_id",
            digest_key="outcome_payload_digest",
            required_parent=source["payload_id"],
            epoch=epoch,
            required=required,
        )
        outcome = None
        if outcome_state == "ACTIVE":
            if not isinstance(outcome_body, dict) or set(outcome_body) != {"command_id", "outcome"}:
                raise ForestError("SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell outcome is invalid", 409)
            outcome = outcome_body["outcome"]
            expected_outcome_metadata = (
                {}
                if outcome is None and row.get("legacy_result_untracked")
                else outcome_metadata(outcome)
            )
            if (
                outcome_body["command_id"] != row["command_id"]
                or expected_outcome_metadata != row.get("outcome_metadata")
            ):
                raise ForestError(
                    "SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell outcome differs from metadata", 409
                )
        required_delivery_parent = row["outcome_payload_id"]
        delivery_body, delivery_state = await self._load_in(
            conn,
            row,
            role="delivery",
            payload_key="delivery_payload_id",
            digest_key="delivery_payload_digest",
            required_parent=required_delivery_parent,
            epoch=epoch,
            required=required,
        )
        delivery = None
        if delivery_state == "ACTIVE":
            if not isinstance(delivery_body, dict) or set(delivery_body) != {"command_id", "delivery"}:
                raise ForestError("SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell delivery is invalid", 409)
            delivery = delivery_body["delivery"]
            if delivery_body["command_id"] != row["command_id"]:
                raise ForestError(
                    "SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell delivery identity differs", 409
                )
            expected = delivery_metadata(
                delivery, legacy_untracked=bool(row.get("legacy_result_untracked"))
            )
            if expected != row.get("delivery_metadata"):
                raise ForestError(
                    "SHELL_PAYLOAD_IDENTITY_CONFLICT", "Shell delivery differs from metadata", 409
                )
        return {
            **row,
            "outcome": outcome,
            "delivery": delivery,
            "outcome_payload_state": outcome_state,
            "delivery_payload_state": delivery_state,
        }

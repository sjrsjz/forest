"""Clean durable authority for the single-Agent Session and Run state machine."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from psycopg import sql

from forest.agent_protocol import (
    CORE_PROMPT_SHA256,
    PARSER_REVISION,
    PROTOCOL_ID,
    PROTOCOL_VERSION,
    ModelTurn,
)
from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_LOCK_DOMAIN = b"forest.agent.runtime/1\0"
_TERMINAL_RUN = frozenset({"COMPLETED", "FAILED", "CANCELLED"})
_RUN_PHASES = frozenset(
    {
        "WAIT_INPUT",
        "PREPARING_MODEL",
        "MODEL_PENDING",
        "PARSING",
        "REPAIR_READY",
        "WAIT_TOOL",
        "WAIT_APPROVAL",
        "BLOCKED_UNKNOWN",
        "SAFE_BOUNDARY",
        "FINAL_READY",
        "RELOAD_WAIT",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
    }
)


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("AGENT_RUNTIME_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str):
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("AGENT_RUNTIME_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _canonical(value) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError(
            "AGENT_RUNTIME_INPUT_INVALID", "Agent runtime identity is not canonical", 422
        ) from exc


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _bytes_digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _timestamp(value, field: str):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ForestError("AGENT_RUNTIME_INPUT_INVALID", f"{field} needs a timezone", 422)
    return value.astimezone(timezone.utc)


def _lock_key(bot_id: str, identity: str) -> int:
    digest = hashlib.sha256(_LOCK_DOMAIN + bot_id.encode() + b"\0" + identity.encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


@dataclass(frozen=True)
class AgentPayloadRef:
    payload_id: str
    digest: str
    owner_component_id: str
    owner_kind: str
    owner_id: str

    def validate(self, field: str) -> None:
        _bounded(self.payload_id, f"{field}.payload_id", 256)
        _validated_digest(self.digest, f"{field}.digest")
        _bounded(self.owner_component_id, f"{field}.owner_component_id", 128)
        _bounded(self.owner_kind, f"{field}.owner_kind", 128)
        _bounded(self.owner_id, f"{field}.owner_id", 512)

    def canonical(self) -> dict:
        return {
            "payload_id": self.payload_id,
            "digest": self.digest,
            "owner_component_id": self.owner_component_id,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
        }


@dataclass(frozen=True, order=True)
class AgentTriggerInput:
    trigger_ref: str
    principal_ref: str
    ingress_seq: int

    def validate(self) -> None:
        _bounded(self.trigger_ref, "trigger_ref", 256)
        _bounded(self.principal_ref, "principal_ref", 256)
        if type(self.ingress_seq) is not int or self.ingress_seq <= 0:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid trigger ingress_seq", 422)


@dataclass(frozen=True)
class AgentInputBatch:
    input_batch_ref: str
    input_batch_digest: str
    selected_input_end_seq: int
    context_revision: int
    context_view_revision: int
    source_manifest_digest: str
    triggers: tuple[AgentTriggerInput, ...]

    def validate(self) -> None:
        _bounded(self.input_batch_ref, "input_batch_ref", 256)
        _validated_digest(self.input_batch_digest, "input_batch_digest")
        _validated_digest(self.source_manifest_digest, "source_manifest_digest")
        if type(self.selected_input_end_seq) is not int or self.selected_input_end_seq < 0:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Invalid selected_input_end_seq", 422
            )
        if type(self.context_revision) is not int or self.context_revision <= 0:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid context_revision", 422)
        if type(self.context_view_revision) is not int or self.context_view_revision < 0:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Invalid context_view_revision", 422
            )
        if not 1 <= len(self.triggers) <= 4096:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Input has no trigger", 422)
        for trigger in self.triggers:
            if type(trigger) is not AgentTriggerInput:
                raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid trigger input", 422)
            trigger.validate()
        identities = [trigger.trigger_ref for trigger in self.triggers]
        if len(identities) != len(set(identities)):
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Trigger identity repeats", 422)
        if tuple(sorted(self.triggers, key=lambda item: (item.ingress_seq, item.trigger_ref))) != (
            self.triggers
        ):
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Triggers are not ordered", 422)
        if max(trigger.ingress_seq for trigger in self.triggers) > self.selected_input_end_seq:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Trigger exceeds selected input cut", 422
            )

    def canonical(self) -> dict:
        return {
            "input_batch_ref": self.input_batch_ref,
            "input_batch_digest": self.input_batch_digest,
            "selected_input_end_seq": self.selected_input_end_seq,
            "context_revision": self.context_revision,
            "context_view_revision": self.context_view_revision,
            "source_manifest_digest": self.source_manifest_digest,
            "triggers": [
                {
                    "trigger_ref": item.trigger_ref,
                    "principal_ref": item.principal_ref,
                    "ingress_seq": item.ingress_seq,
                }
                for item in self.triggers
            ],
        }


@dataclass(frozen=True)
class AgentRunOwner:
    run_id: str
    owner_id: str
    epoch: int
    expires_at: datetime


@dataclass(frozen=True)
class ModelRequestPlan:
    request_id: str
    request_kind: str
    provider_component_id: str
    provider_generation_id: str
    provider_profile_ref: str
    model_ref: str
    instruction_channel: str
    capability_digest: str
    projection: AgentPayloadRef
    projected_content_digest: str
    source_manifest_digest: str
    output_token_budget: int

    def validate(self) -> None:
        _bounded(self.request_id, "request_id", 256)
        if self.request_kind not in {"PRIMARY", "CONTINUATION", "REPAIR"}:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid model request kind", 422)
        _bounded(self.provider_component_id, "provider_component_id", 128)
        _bounded(self.provider_generation_id, "provider_generation_id", 256)
        _bounded(self.provider_profile_ref, "provider_profile_ref", 256)
        _bounded(self.model_ref, "model_ref", 256)
        if self.instruction_channel not in {
            "SYSTEM",
            "DEVELOPER",
            "INSTRUCTIONS",
            "DEGRADED_USER",
        }:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Invalid instruction channel", 422
            )
        _validated_digest(self.capability_digest, "capability_digest")
        self.projection.validate("projection")
        _validated_digest(self.projected_content_digest, "projected_content_digest")
        _validated_digest(self.source_manifest_digest, "source_manifest_digest")
        if type(self.output_token_budget) is not int or self.output_token_budget <= 0:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid output token budget", 422)

    def canonical(self) -> dict:
        return {
            "request_id": self.request_id,
            "request_kind": self.request_kind,
            "provider_component_id": self.provider_component_id,
            "provider_generation_id": self.provider_generation_id,
            "provider_profile_ref": self.provider_profile_ref,
            "model_ref": self.model_ref,
            "instruction_channel": self.instruction_channel,
            "capability_digest": self.capability_digest,
            "projection": self.projection.canonical(),
            "projected_content_digest": self.projected_content_digest,
            "source_manifest_digest": self.source_manifest_digest,
            "output_token_budget": self.output_token_budget,
        }


@dataclass(frozen=True)
class ToolProposal:
    tool_call_id: str
    arguments: AgentPayloadRef
    authority_trigger_ref: str
    tool_contract_digest: str
    effect_operation_ref: str

    def validate(self) -> None:
        _bounded(self.tool_call_id, "tool_call_id", 256)
        self.arguments.validate("tool.arguments")
        _bounded(self.authority_trigger_ref, "authority_trigger_ref", 256)
        _validated_digest(self.tool_contract_digest, "tool_contract_digest")
        _bounded(self.effect_operation_ref, "effect_operation_ref", 256)


class AgentRuntimeRepository:
    """Persist one recoverable Agent execution chain without owning Conversation facts."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier: Callable[..., Awaitable[object]] | None = None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.agent.runtime")
        self.payload_verifier = payload_verifier

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    async def _lock(self, conn, identity: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)", (_lock_key(self.store.bot_id, identity),)
        )

    async def _verify_payload(self, conn, reference: AgentPayloadRef) -> None:
        if self.payload_verifier is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "Agent payload verifier is unavailable", 503
            )
        await self.payload_verifier(
            conn,
            payload_id=reference.payload_id,
            digest=reference.digest,
            owner_component_id=reference.owner_component_id,
            owner_kind=reference.owner_kind,
            owner_id=reference.owner_id,
        )

    async def _run(self, conn, run_id: str, *, update: bool = False):
        row = await (
            await conn.execute(
                sql.SQL("SELECT *,clock_timestamp() AS database_now FROM {} WHERE bot_id=%s "
                        "AND run_id=%s {}").format(
                    self._table("agent_runs"), sql.SQL("FOR UPDATE" if update else "")
                ),
                (self.store.bot_id, run_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("AGENT_RUN_NOT_FOUND", "Agent Run does not exist", 404)
        return row

    async def _expanded_run(self, conn, run_id: str) -> dict:
        run = await self._run(conn, run_id)
        run_values = dict(run)
        run_values.pop("database_now", None)
        session = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND session_id=%s").format(
                    self._table("agent_sessions")
                ),
                (self.store.bot_id, run["session_id"]),
            )
        ).fetchone()
        inputs = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND run_id=%s "
                        "ORDER BY input_revision").format(self._table("run_inputs")),
                (self.store.bot_id, run_id),
            )
        ).fetchall()
        trigger_rows = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND run_id=%s "
                        "ORDER BY input_revision,trigger_index").format(
                    self._table("run_input_triggers")
                ),
                (self.store.bot_id, run_id),
            )
        ).fetchall()
        return {
            **run_values,
            "session": dict(session),
            "inputs": [dict(item) for item in inputs],
            "triggers": [dict(item) for item in trigger_rows],
        }

    async def start_run(
        self,
        *,
        operation_id: str,
        conversation_ref: str,
        session_id: str,
        run_id: str,
        input_batch: AgentInputBatch,
        agent_spec_revision: str,
        config: AgentPayloadRef,
        preset_revision: str,
        preset: AgentPayloadRef,
        absolute_deadline: datetime,
        _connection=None,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (conversation_ref, "conversation_ref"),
            (session_id, "session_id"),
            (run_id, "run_id"),
            (agent_spec_revision, "agent_spec_revision"),
            (preset_revision, "preset_revision"),
        ):
            _bounded(value, field, 256)
        if type(input_batch) is not AgentInputBatch:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid InputBatch", 422)
        input_batch.validate()
        config.validate("config")
        preset.validate("preset")
        absolute_deadline = _timestamp(absolute_deadline, "absolute_deadline")
        input_digest = _digest(
            {
                "conversation_ref": conversation_ref,
                "session_id": session_id,
                "run_id": run_id,
                "input_batch": input_batch.canonical(),
                "agent_spec_revision": agent_spec_revision,
                "config": config.canonical(),
                "preset_revision": preset_revision,
                "preset": preset.canonical(),
                "absolute_deadline": absolute_deadline.isoformat(),
                "core_protocol_id": PROTOCOL_ID,
                "core_protocol_version": PROTOCOL_VERSION,
                "core_prompt_digest": CORE_PROMPT_SHA256,
                "parser_revision": PARSER_REVISION,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND start_operation_id=%s").format(
                        self._table("agent_runs")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "AGENT_START_OPERATION_CONFLICT", "Agent start input differs", 409
                    )
                return await self._expanded_run(conn, replay["run_id"])
            for reference in sorted((config, preset), key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            await self._lock(conn, "conversation:" + conversation_ref)
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            if not now["now"] < absolute_deadline <= now["now"] + timedelta(hours=24):
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Run deadline is outside its bound", 422
                )
            existing_session = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND "
                            "(session_id=%s OR (conversation_ref=%s AND state='ACTIVE')) "
                            "FOR UPDATE").format(self._table("agent_sessions")),
                    (self.store.bot_id, session_id, conversation_ref),
                )
            ).fetchall()
            if existing_session:
                raise ForestError(
                    "AGENT_SESSION_CONFLICT", "Conversation already has an Agent Session", 409
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,session_id,conversation_ref,start_operation_id,"
                    "input_digest,active_run_id) VALUES (%s,%s,%s,%s,%s,%s)"
                ).format(self._table("agent_sessions")),
                (
                    self.store.bot_id,
                    session_id,
                    conversation_ref,
                    operation_id,
                    input_digest,
                    run_id,
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,run_id,session_id,start_operation_id,input_digest,state,"
                    "agent_spec_revision,config_payload_id,config_payload_digest,"
                    "config_owner_component_id,config_owner_kind,config_owner_id,preset_revision,"
                    "preset_payload_id,preset_payload_digest,preset_owner_component_id,"
                    "preset_owner_kind,preset_owner_id,core_protocol_id,core_protocol_version,"
                    "core_prompt_digest,parser_revision,absolute_deadline,input_revision,"
                    "selected_input_end_seq,context_revision,context_view_revision) VALUES "
                    "(%s,%s,%s,%s,%s,'PREPARING_MODEL',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,1,%s,%s,%s)"
                ).format(self._table("agent_runs")),
                (
                    self.store.bot_id,
                    run_id,
                    session_id,
                    operation_id,
                    input_digest,
                    agent_spec_revision,
                    config.payload_id,
                    config.digest,
                    config.owner_component_id,
                    config.owner_kind,
                    config.owner_id,
                    preset_revision,
                    preset.payload_id,
                    preset.digest,
                    preset.owner_component_id,
                    preset.owner_kind,
                    preset.owner_id,
                    PROTOCOL_ID,
                    PROTOCOL_VERSION,
                    CORE_PROMPT_SHA256,
                    PARSER_REVISION,
                    absolute_deadline,
                    input_batch.selected_input_end_seq,
                    input_batch.context_revision,
                    input_batch.context_view_revision,
                ),
            )
            await self._insert_input(conn, run_id, 1, input_batch)
            return await self._expanded_run(conn, run_id)

    async def _insert_input(
        self, conn, run_id: str, input_revision: int, input_batch: AgentInputBatch
    ) -> None:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,run_id,input_revision,input_batch_ref,input_batch_digest,"
                "selected_input_end_seq,context_revision,context_view_revision,"
                "source_manifest_digest,trigger_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            ).format(self._table("run_inputs")),
            (
                self.store.bot_id,
                run_id,
                input_revision,
                input_batch.input_batch_ref,
                input_batch.input_batch_digest,
                input_batch.selected_input_end_seq,
                input_batch.context_revision,
                input_batch.context_view_revision,
                input_batch.source_manifest_digest,
                len(input_batch.triggers),
            ),
        )
        for index, trigger in enumerate(input_batch.triggers):
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,run_id,input_revision,trigger_index,trigger_ref,"
                    "principal_ref,ingress_seq) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                ).format(self._table("run_input_triggers")),
                (
                    self.store.bot_id,
                    run_id,
                    input_revision,
                    index,
                    trigger.trigger_ref,
                    trigger.principal_ref,
                    trigger.ingress_seq,
                ),
            )

    async def start_successor_run(
        self,
        *,
        operation_id: str,
        session_id: str,
        completed_run_id: str,
        run_id: str,
        input_batch: AgentInputBatch,
        absolute_deadline: datetime,
        _connection=None,
    ) -> dict:
        """Start the next serial Run without creating another Agent Session."""

        for value, field in (
            (operation_id, "operation_id"),
            (session_id, "session_id"),
            (completed_run_id, "completed_run_id"),
            (run_id, "run_id"),
        ):
            _bounded(value, field, 256)
        if type(input_batch) is not AgentInputBatch:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid InputBatch", 422)
        input_batch.validate()
        absolute_deadline = _timestamp(absolute_deadline, "absolute_deadline")
        input_digest = _digest(
            {
                "session_id": session_id,
                "completed_run_id": completed_run_id,
                "run_id": run_id,
                "input_batch": input_batch.canonical(),
                "absolute_deadline": absolute_deadline.isoformat(),
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND start_operation_id=%s").format(
                        self._table("agent_runs")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "AGENT_START_OPERATION_CONFLICT", "Agent start input differs", 409
                    )
                return await self._expanded_run(conn, replay["run_id"])
            await self._lock(conn, "session:" + session_id)
            session = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND session_id=%s FOR UPDATE").format(
                        self._table("agent_sessions")
                    ),
                    (self.store.bot_id, session_id),
                )
            ).fetchone()
            previous = await self._run(conn, completed_run_id, update=True)
            now = previous["database_now"]
            if not now < absolute_deadline <= now + timedelta(hours=24):
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Run deadline is outside its bound", 422
                )
            if (
                session is None
                or session["state"] != "ACTIVE"
                or session["active_run_id"] != completed_run_id
                or previous["session_id"] != session_id
                or previous["state"] != "COMPLETED"
            ):
                raise ForestError(
                    "AGENT_SUCCESSOR_CONFLICT",
                    "Session is not ready for the requested successor Run",
                    409,
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,run_id,session_id,start_operation_id,input_digest,state,"
                    "agent_spec_revision,config_payload_id,config_payload_digest,"
                    "config_owner_component_id,config_owner_kind,config_owner_id,preset_revision,"
                    "preset_payload_id,preset_payload_digest,preset_owner_component_id,"
                    "preset_owner_kind,preset_owner_id,core_protocol_id,core_protocol_version,"
                    "core_prompt_digest,parser_revision,absolute_deadline,input_revision,"
                    "selected_input_end_seq,context_revision,context_view_revision) VALUES "
                    "(%s,%s,%s,%s,%s,'PREPARING_MODEL',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,1,%s,%s,%s)"
                ).format(self._table("agent_runs")),
                (
                    self.store.bot_id,
                    run_id,
                    session_id,
                    operation_id,
                    input_digest,
                    previous["agent_spec_revision"],
                    previous["config_payload_id"],
                    previous["config_payload_digest"],
                    previous["config_owner_component_id"],
                    previous["config_owner_kind"],
                    previous["config_owner_id"],
                    previous["preset_revision"],
                    previous["preset_payload_id"],
                    previous["preset_payload_digest"],
                    previous["preset_owner_component_id"],
                    previous["preset_owner_kind"],
                    previous["preset_owner_id"],
                    previous["core_protocol_id"],
                    previous["core_protocol_version"],
                    previous["core_prompt_digest"],
                    previous["parser_revision"],
                    absolute_deadline,
                    input_batch.selected_input_end_seq,
                    input_batch.context_revision,
                    input_batch.context_view_revision,
                ),
            )
            await self._insert_input(conn, run_id, 1, input_batch)
            await conn.execute(
                sql.SQL("UPDATE {} SET active_run_id=%s,session_epoch=session_epoch+1,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND session_id=%s").format(
                    self._table("agent_sessions")
                ),
                (run_id, self.store.bot_id, session_id),
            )
            return await self._expanded_run(conn, run_id)

    async def acquire_run(
        self, run_id: str, owner_id: str, *, lease_seconds: int = 30, _connection=None
    ) -> AgentRunOwner:
        _bounded(run_id, "run_id", 256)
        _bounded(owner_id, "owner_id", 256)
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid Run lease", 422)
        async with self._transaction(_connection) as conn:
            run = await self._run(conn, run_id, update=True)
            if run["state"] in _TERMINAL_RUN:
                raise ForestError("AGENT_RUN_TERMINAL", "Agent Run is terminal", 409)
            alive = run["owner_id"] is not None and run["owner_expires_at"] > run["database_now"]
            if alive and run["owner_id"] != owner_id:
                raise ForestError("AGENT_RUN_BUSY", "Agent Run lease is busy", 409)
            epoch = run["owner_epoch"] if alive else run["owner_epoch"] + 1
            expires = run["database_now"] + timedelta(seconds=lease_seconds)
            await conn.execute(
                sql.SQL("UPDATE {} SET owner_id=%s,owner_epoch=%s,owner_expires_at=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s").format(
                    self._table("agent_runs")
                ),
                (owner_id, epoch, expires, self.store.bot_id, run_id),
            )
            return AgentRunOwner(run_id, owner_id, epoch, expires)

    async def _guard_owner(self, conn, owner: AgentRunOwner):
        if type(owner) is not AgentRunOwner:
            raise ForestError("STALE_AGENT_RUN_OWNER", "Invalid Agent Run owner", 409)
        run = await self._run(conn, owner.run_id, update=True)
        if (
            run["owner_id"] != owner.owner_id
            or run["owner_epoch"] != owner.epoch
            or run["owner_expires_at"] is None
            or run["owner_expires_at"] <= run["database_now"]
        ):
            raise ForestError("STALE_AGENT_RUN_OWNER", "Agent Run owner is stale", 409)
        return run

    async def renew_run(
        self, owner: AgentRunOwner, *, lease_seconds: int = 30, _connection=None
    ) -> AgentRunOwner:
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid Run lease", 422)
        async with self._transaction(_connection) as conn:
            run = await self._guard_owner(conn, owner)
            expires = run["database_now"] + timedelta(seconds=lease_seconds)
            await conn.execute(
                sql.SQL("UPDATE {} SET owner_expires_at=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND run_id=%s").format(self._table("agent_runs")),
                (expires, self.store.bot_id, owner.run_id),
            )
            return AgentRunOwner(owner.run_id, owner.owner_id, owner.epoch, expires)

    async def release_run(self, owner: AgentRunOwner, *, _connection=None) -> None:
        async with self._transaction(_connection) as conn:
            await self._guard_owner(conn, owner)
            await conn.execute(
                sql.SQL("UPDATE {} SET owner_id=NULL,owner_expires_at=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s").format(
                    self._table("agent_runs")
                ),
                (self.store.bot_id, owner.run_id),
            )

    async def append_input(
        self, owner: AgentRunOwner, input_batch: AgentInputBatch, *, _connection=None
    ) -> dict:
        if type(input_batch) is not AgentInputBatch:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid InputBatch", 422)
        input_batch.validate()
        async with self._transaction(_connection) as conn:
            run = await self._guard_owner(conn, owner)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND input_batch_ref=%s").format(
                        self._table("run_inputs")
                    ),
                    (self.store.bot_id, input_batch.input_batch_ref),
                )
            ).fetchone()
            if existing is not None:
                expected = (
                    owner.run_id,
                    input_batch.input_batch_digest,
                    input_batch.selected_input_end_seq,
                    input_batch.context_revision,
                    input_batch.context_view_revision,
                    input_batch.source_manifest_digest,
                    len(input_batch.triggers),
                )
                actual = (
                    existing["run_id"],
                    existing["input_batch_digest"],
                    existing["selected_input_end_seq"],
                    existing["context_revision"],
                    existing["context_view_revision"],
                    existing["source_manifest_digest"],
                    existing["trigger_count"],
                )
                if actual != expected:
                    raise ForestError("AGENT_INPUT_CONFLICT", "InputBatch identity differs", 409)
                return await self._expanded_run(conn, owner.run_id)
            if run["state"] not in {"WAIT_INPUT", "SAFE_BOUNDARY", "FINAL_READY"}:
                raise ForestError(
                    "AGENT_INPUT_BOUNDARY_CLOSED", "Run is not at a model-safe input boundary", 409
                )
            if input_batch.selected_input_end_seq <= run["selected_input_end_seq"]:
                raise ForestError(
                    "AGENT_INPUT_CONFLICT", "Input cut did not advance", 409
                )
            if input_batch.context_revision <= run["context_revision"]:
                raise ForestError(
                    "AGENT_INPUT_CONFLICT", "Context revision did not advance", 409
                )
            revision = run["input_revision"] + 1
            await self._insert_input(conn, owner.run_id, revision, input_batch)
            await conn.execute(
                sql.SQL("UPDATE {} SET state='PREPARING_MODEL',input_revision=%s,"
                        "selected_input_end_seq=%s,context_revision=%s,context_view_revision=%s,"
                        "final_candidate_payload_id=NULL,final_candidate_payload_digest=NULL,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s").format(
                    self._table("agent_runs")
                ),
                (
                    revision,
                    input_batch.selected_input_end_seq,
                    input_batch.context_revision,
                    input_batch.context_view_revision,
                    self.store.bot_id,
                    owner.run_id,
                ),
            )
            return await self._expanded_run(conn, owner.run_id)

    @staticmethod
    def _request_matches(row, plan: ModelRequestPlan) -> bool:
        return (
            row["request_id"] == plan.request_id
            and row["request_kind"] == plan.request_kind
            and row["provider_component_id"] == plan.provider_component_id
            and row["provider_generation_id"] == plan.provider_generation_id
            and row["provider_profile_ref"] == plan.provider_profile_ref
            and row["model_ref"] == plan.model_ref
            and row["instruction_channel"] == plan.instruction_channel
            and row["capability_digest"] == plan.capability_digest
            and row["projection_payload_id"] == plan.projection.payload_id
            and row["projection_payload_digest"] == plan.projection.digest
            and row["projection_owner_component_id"] == plan.projection.owner_component_id
            and row["projection_owner_kind"] == plan.projection.owner_kind
            and row["projection_owner_id"] == plan.projection.owner_id
            and row["projected_content_digest"] == plan.projected_content_digest
            and row["source_manifest_digest"] == plan.source_manifest_digest
            and row["output_token_budget"] == plan.output_token_budget
        )

    async def prepare_model_request(
        self, owner: AgentRunOwner, plan: ModelRequestPlan, *, _connection=None
    ) -> dict:
        if type(plan) is not ModelRequestPlan:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid model request", 422)
        plan.validate()
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "request:" + plan.request_id)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s").format(
                        self._table("model_requests")
                    ),
                    (self.store.bot_id, plan.request_id),
                )
            ).fetchone()
            if existing is not None:
                await self._guard_owner(conn, owner)
                if existing["run_id"] != owner.run_id or not self._request_matches(existing, plan):
                    raise ForestError(
                        "MODEL_REQUEST_ID_CONFLICT", "Model request identity differs", 409
                    )
                return dict(existing)
            await self._verify_payload(conn, plan.projection)
            run = await self._guard_owner(conn, owner)
            if run["state"] != "PREPARING_MODEL":
                raise ForestError(
                    "MODEL_REQUEST_BOUNDARY_CLOSED", "Run is not preparing a model request", 409
                )
            semantic_digest = _digest(
                {
                    "run_id": owner.run_id,
                    "step_seq": run["next_step_seq"],
                    "input_revision": run["input_revision"],
                    "context_revision": run["context_revision"],
                    "context_view_revision": run["context_view_revision"],
                    "core_protocol_version": run["core_protocol_version"],
                    "core_prompt_digest": run["core_prompt_digest"],
                    "preset_revision": run["preset_revision"],
                    "preset_payload_digest": run["preset_payload_digest"],
                    "plan": plan.canonical(),
                }
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,request_id,run_id,step_seq,request_kind,"
                        "semantic_request_digest,input_revision,context_revision,"
                        "context_view_revision,core_protocol_version,core_prompt_digest,"
                        "preset_revision,preset_payload_digest,provider_component_id,"
                        "provider_generation_id,provider_profile_ref,model_ref,instruction_channel,"
                        "capability_digest,projection_payload_id,projection_payload_digest,"
                        "projection_owner_component_id,projection_owner_kind,projection_owner_id,"
                        "projected_content_digest,source_manifest_digest,output_token_budget) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("model_requests")),
                    (
                        self.store.bot_id,
                        plan.request_id,
                        owner.run_id,
                        run["next_step_seq"],
                        plan.request_kind,
                        semantic_digest,
                        run["input_revision"],
                        run["context_revision"],
                        run["context_view_revision"],
                        run["core_protocol_version"],
                        run["core_prompt_digest"],
                        run["preset_revision"],
                        run["preset_payload_digest"],
                        plan.provider_component_id,
                        plan.provider_generation_id,
                        plan.provider_profile_ref,
                        plan.model_ref,
                        plan.instruction_channel,
                        plan.capability_digest,
                        plan.projection.payload_id,
                        plan.projection.digest,
                        plan.projection.owner_component_id,
                        plan.projection.owner_kind,
                        plan.projection.owner_id,
                        plan.projected_content_digest,
                        plan.source_manifest_digest,
                        plan.output_token_budget,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def prepare_model_attempt(
        self,
        owner: AgentRunOwner,
        *,
        request_id: str,
        attempt_id: str,
        effect_operation_ref: str,
        wire: AgentPayloadRef,
        _connection=None,
    ) -> dict:
        for value, field in (
            (request_id, "request_id"),
            (attempt_id, "attempt_id"),
            (effect_operation_ref, "effect_operation_ref"),
        ):
            _bounded(value, field, 256)
        wire.validate("wire")
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "attempt:" + attempt_id)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s").format(
                        self._table("model_transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if existing is not None:
                await self._guard_owner(conn, owner)
                expected = (
                    request_id,
                    effect_operation_ref,
                    wire.payload_id,
                    wire.digest,
                    wire.owner_component_id,
                    wire.owner_kind,
                    wire.owner_id,
                )
                actual = tuple(
                    existing[key]
                    for key in (
                        "request_id",
                        "effect_operation_ref",
                        "wire_payload_id",
                        "wire_payload_digest",
                        "wire_owner_component_id",
                        "wire_owner_kind",
                        "wire_owner_id",
                    )
                )
                if actual != expected:
                    raise ForestError(
                        "MODEL_ATTEMPT_ID_CONFLICT", "Model attempt identity differs", 409
                    )
                return dict(existing)
            await self._verify_payload(conn, wire)
            run = await self._guard_owner(conn, owner)
            request = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s FOR UPDATE").format(
                        self._table("model_requests")
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            if request is None or request["run_id"] != owner.run_id:
                raise ForestError("MODEL_REQUEST_NOT_FOUND", "Model request does not exist", 404)
            if run["state"] != "PREPARING_MODEL" or request["state"] not in {
                "READY",
                "NOT_APPLIED",
            }:
                raise ForestError(
                    "MODEL_ATTEMPT_BOUNDARY_CLOSED", "Model request cannot prepare an attempt", 409
                )
            attempt_no = request["attempt_count"] + 1
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,attempt_id,request_id,attempt_no,"
                        "effect_operation_ref,wire_payload_id,wire_payload_digest,"
                        "wire_owner_component_id,wire_owner_kind,wire_owner_id) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("model_transport_attempts")),
                    (
                        self.store.bot_id,
                        attempt_id,
                        request_id,
                        attempt_no,
                        effect_operation_ref,
                        wire.payload_id,
                        wire.digest,
                        wire.owner_component_id,
                        wire.owner_kind,
                        wire.owner_id,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET state='ATTEMPT_PREPARED',current_attempt_id=%s,"
                        "attempt_count=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND request_id=%s").format(
                    self._table("model_requests")
                ),
                (attempt_id, attempt_no, self.store.bot_id, request_id),
            )
            return dict(row)

    async def mark_model_handoff(
        self, owner: AgentRunOwner, *, request_id: str, attempt_id: str, _connection=None
    ) -> dict:
        _bounded(request_id, "request_id", 256)
        _bounded(attempt_id, "attempt_id", 256)
        async with self._transaction(_connection) as conn:
            run = await self._guard_owner(conn, owner)
            request = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s FOR UPDATE").format(
                        self._table("model_requests")
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                        self._table("model_transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if request is None or attempt is None or request["run_id"] != owner.run_id:
                raise ForestError("MODEL_ATTEMPT_NOT_FOUND", "Model attempt does not exist", 404)
            if request["current_attempt_id"] != attempt_id or attempt["request_id"] != request_id:
                raise ForestError("MODEL_ATTEMPT_CONFLICT", "Current model attempt differs", 409)
            if request["state"] == attempt["state"] == "DISPATCHING":
                return dict(attempt)
            if (
                run["state"] != "PREPARING_MODEL"
                or request["state"] != "ATTEMPT_PREPARED"
                or attempt["state"] != "PREPARED"
            ):
                raise ForestError("MODEL_ATTEMPT_CONFLICT", "Model handoff boundary closed", 409)
            started = run["database_now"]
            await conn.execute(
                sql.SQL("UPDATE {} SET state='DISPATCHING',started_at=%s "
                        "WHERE bot_id=%s AND attempt_id=%s").format(
                    self._table("model_transport_attempts")
                ),
                (started, self.store.bot_id, attempt_id),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='DISPATCHING',updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND request_id=%s").format(self._table("model_requests")),
                (self.store.bot_id, request_id),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='MODEL_PENDING',current_model_request_id=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s").format(
                    self._table("agent_runs")
                ),
                (request_id, self.store.bot_id, owner.run_id),
            )
            return {
                **dict(attempt),
                "state": "DISPATCHING",
                "started_at": started,
            }

    async def observe_model(
        self,
        owner: AgentRunOwner,
        *,
        observation_operation_id: str,
        request_id: str,
        attempt_id: str,
        observed_state: str,
        evidence_kind: str,
        effect_observation_ref: str,
        result: AgentPayloadRef | None = None,
        usage: AgentPayloadRef | None = None,
        _connection=None,
    ) -> dict:
        for value, field in (
            (observation_operation_id, "observation_operation_id"),
            (request_id, "request_id"),
            (attempt_id, "attempt_id"),
            (effect_observation_ref, "effect_observation_ref"),
        ):
            _bounded(value, field, 256)
        if observed_state not in {"SUCCEEDED", "NOT_APPLIED", "UNKNOWN"}:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid model observation", 422)
        if evidence_kind not in {"EXECUTOR", "RECONCILIATION"}:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid evidence kind", 422)
        if observed_state == "SUCCEEDED" and result is None:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Success needs a model result", 422)
        if result is not None:
            result.validate("result")
        if usage is not None:
            usage.validate("usage")
        input_digest = _digest(
            {
                "request_id": request_id,
                "attempt_id": attempt_id,
                "observed_state": observed_state,
                "evidence_kind": evidence_kind,
                "effect_observation_ref": effect_observation_ref,
                "result": result.canonical() if result else None,
                "usage": usage.canonical() if usage else None,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "model-observation:" + observation_operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s "
                            "AND observation_operation_id=%s").format(
                        self._table("model_observations")
                    ),
                    (self.store.bot_id, observation_operation_id),
                )
            ).fetchone()
            if replay is not None:
                await self._guard_owner(conn, owner)
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "MODEL_OBSERVATION_CONFLICT", "Model observation input differs", 409
                    )
                return dict(replay)
            references = [item for item in (result, usage) if item is not None]
            for reference in sorted(references, key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            run = await self._guard_owner(conn, owner)
            request = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s FOR UPDATE").format(
                        self._table("model_requests")
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                        self._table("model_transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if (
                request is None
                or attempt is None
                or request["run_id"] != owner.run_id
                or request["current_attempt_id"] != attempt_id
                or attempt["request_id"] != request_id
            ):
                raise ForestError("MODEL_ATTEMPT_NOT_FOUND", "Model attempt does not exist", 404)
            if result is not None and (
                result.owner_component_id != request["provider_component_id"]
                or result.owner_kind != "model.result"
                or result.owner_id != attempt_id
            ):
                raise ForestError("MODEL_RESULT_IDENTITY_CONFLICT", "Model result owner differs", 409)
            if usage is not None and (
                usage.owner_component_id != request["provider_component_id"]
                or usage.owner_kind != "model.usage"
                or usage.owner_id != attempt_id
            ):
                raise ForestError("MODEL_RESULT_IDENTITY_CONFLICT", "Model usage owner differs", 409)
            previous = attempt["state"]
            if previous not in {"DISPATCHING", "SUCCEEDED", "NOT_APPLIED", "UNKNOWN"}:
                raise ForestError(
                    "MODEL_OBSERVATION_BOUNDARY_CLOSED",
                    "Model attempt has not crossed the dispatch boundary",
                    409,
                )
            accepted = False
            conflict_code = None
            if previous == "DISPATCHING":
                accepted = True
            elif (
                previous == "UNKNOWN"
                and evidence_kind == "RECONCILIATION"
                and observed_state in {"SUCCEEDED", "NOT_APPLIED"}
            ):
                accepted = True
            else:
                conflict_code = "MODEL_RESULT_ALREADY_SETTLED"
            resulting = observed_state if accepted else previous
            observation = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,observation_operation_id,attempt_id,input_digest,"
                        "evidence_kind,observed_state,effect_observation_ref,result_payload_id,"
                        "result_payload_digest,usage_payload_id,usage_payload_digest,accepted,"
                        "previous_state,resulting_state,conflict_code) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("model_observations")),
                    (
                        self.store.bot_id,
                        observation_operation_id,
                        attempt_id,
                        input_digest,
                        evidence_kind,
                        observed_state,
                        effect_observation_ref,
                        result.payload_id if result else None,
                        result.digest if result else None,
                        usage.payload_id if usage else None,
                        usage.digest if usage else None,
                        accepted,
                        previous,
                        resulting,
                        conflict_code,
                    ),
                )
            ).fetchone()
            if not accepted:
                return dict(observation)
            settled = run["database_now"]
            resolved = settled if previous == "UNKNOWN" else None
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,result_payload_id=%s,result_payload_digest=%s,"
                        "usage_payload_id=%s,usage_payload_digest=%s,settled_at=COALESCE(settled_at,%s),"
                        "resolved_at=%s WHERE bot_id=%s AND attempt_id=%s").format(
                    self._table("model_transport_attempts")
                ),
                (
                    observed_state,
                    result.payload_id if result else None,
                    result.digest if result else None,
                    usage.payload_id if usage else None,
                    usage.digest if usage else None,
                    settled,
                    resolved,
                    self.store.bot_id,
                    attempt_id,
                ),
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND request_id=%s").format(self._table("model_requests")),
                (observed_state, self.store.bot_id, request_id),
            )
            run_state = {
                "SUCCEEDED": "PARSING",
                "NOT_APPLIED": "PREPARING_MODEL",
                "UNKNOWN": "BLOCKED_UNKNOWN",
            }[observed_state]
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND run_id=%s").format(self._table("agent_runs")),
                (run_state, self.store.bot_id, owner.run_id),
            )
            return dict(observation)

    async def record_model_turn(
        self,
        owner: AgentRunOwner,
        *,
        turn_id: str,
        request_id: str,
        turn: ModelTurn,
        turn_payload: AgentPayloadRef,
        assistant_payload: AgentPayloadRef | None = None,
        tool: ToolProposal | None = None,
        repair_of_request_id: str | None = None,
        _connection=None,
    ) -> dict:
        _bounded(turn_id, "turn_id", 256)
        _bounded(request_id, "request_id", 256)
        _bounded(repair_of_request_id, "repair_of_request_id", 256, optional=True)
        if type(turn) is not ModelTurn:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid ModelTurn", 422)
        _validated_digest(turn.raw_candidate_digest, "raw_candidate_digest")
        if turn.protocol_version != PROTOCOL_VERSION:
            raise ForestError("AGENT_PROTOCOL_VERSION_CONFLICT", "ModelTurn version differs", 409)
        turn_payload.validate("turn_payload")
        expected_turn_digest = _bytes_digest(_canonical(turn.as_dict()))
        if turn_payload.digest != expected_turn_digest:
            raise ForestError("MODEL_TURN_PAYLOAD_CONFLICT", "ModelTurn payload differs", 409)
        if turn_payload.owner_component_id != "agent.runtime" or (
            turn_payload.owner_kind,
            turn_payload.owner_id,
        ) != ("model.turn", turn_id):
            raise ForestError("MODEL_TURN_PAYLOAD_CONFLICT", "ModelTurn owner differs", 409)
        if turn.assistant_text:
            if assistant_payload is None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Assistant block needs a candidate payload", 422
                )
            assistant_payload.validate("assistant_payload")
            if assistant_payload.digest != _bytes_digest(
                _canonical({"text": turn.assistant_text})
            ) or (
                assistant_payload.owner_component_id,
                assistant_payload.owner_kind,
                assistant_payload.owner_id,
            ) != ("agent.runtime", "assistant.candidate", turn_id):
                raise ForestError(
                    "MODEL_TURN_PAYLOAD_CONFLICT", "Assistant candidate payload differs", 409
                )
        elif assistant_payload is not None:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "ModelTurn has no Assistant block", 422
            )
        function_call = turn.function_call
        if function_call is None:
            if tool is not None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Tool proposal has no FunctionCall", 422
                )
        else:
            if type(tool) is not ToolProposal:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "FunctionCall needs a tool proposal", 422
                )
            tool.validate()
            if tool.arguments.digest != _bytes_digest(_canonical(function_call.arguments)) or (
                tool.arguments.owner_component_id,
                tool.arguments.owner_kind,
                tool.arguments.owner_id,
            ) != ("agent.runtime", "tool.arguments", tool.tool_call_id):
                raise ForestError(
                    "MODEL_TURN_PAYLOAD_CONFLICT", "Tool arguments payload differs", 409
                )
            wrapper = function_call.arguments
            if set(wrapper) != {"authority_trigger_id", "arguments"} or (
                wrapper["authority_trigger_id"] != tool.authority_trigger_ref
            ):
                raise ForestError(
                    "AGENT_TOOL_AUTHORITY_INVALID", "FunctionCall authority trigger differs", 409
                )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "turn:" + turn_id)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s").format(
                        self._table("model_turns")
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            if existing is not None:
                await self._guard_owner(conn, owner)
                expected = (
                    turn_id,
                    turn_payload.payload_id,
                    turn_payload.digest,
                    assistant_payload.payload_id if assistant_payload else None,
                    assistant_payload.digest if assistant_payload else None,
                    tool.arguments.payload_id if tool else None,
                    tool.arguments.digest if tool else None,
                    turn.terminal,
                    turn.thinking_only,
                    turn.truncated_after_function_call,
                    repair_of_request_id,
                )
                actual = tuple(
                    existing[key]
                    for key in (
                        "turn_id",
                        "turn_payload_id",
                        "turn_payload_digest",
                        "assistant_payload_id",
                        "assistant_payload_digest",
                        "function_call_payload_id",
                        "function_call_payload_digest",
                        "terminal",
                        "thinking_only",
                        "truncated_after_function_call",
                        "repair_of_request_id",
                    )
                )
                if actual != expected:
                    raise ForestError("MODEL_TURN_ID_CONFLICT", "ModelTurn identity differs", 409)
                stored_tool = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND turn_id=%s").format(
                            self._table("tool_calls")
                        ),
                        (self.store.bot_id, turn_id),
                    )
                ).fetchone()
                if tool is None and stored_tool is not None:
                    raise ForestError("MODEL_TURN_ID_CONFLICT", "ModelTurn tool differs", 409)
                if tool is not None:
                    tool_expected = (
                        tool.tool_call_id,
                        function_call.name,
                        tool.arguments.payload_id,
                        tool.arguments.digest,
                        tool.authority_trigger_ref,
                        tool.tool_contract_digest,
                        tool.effect_operation_ref,
                    )
                    tool_actual = None if stored_tool is None else tuple(
                        stored_tool[key]
                        for key in (
                            "tool_call_id",
                            "tool_name",
                            "arguments_payload_id",
                            "arguments_payload_digest",
                            "authority_trigger_ref",
                            "tool_contract_digest",
                            "effect_operation_ref",
                        )
                    )
                    if tool_actual != tool_expected:
                        raise ForestError(
                            "MODEL_TURN_ID_CONFLICT", "ModelTurn tool identity differs", 409
                        )
                return dict(existing)
            references = [turn_payload]
            if assistant_payload is not None:
                references.append(assistant_payload)
            if tool is not None:
                references.append(tool.arguments)
            for reference in sorted(references, key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            run = await self._guard_owner(conn, owner)
            request = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s FOR SHARE").format(
                        self._table("model_requests")
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            if (
                request is None
                or request["run_id"] != owner.run_id
                or request["state"] != "SUCCEEDED"
                or request["step_seq"] != run["next_step_seq"]
                or run["state"] != "PARSING"
            ):
                raise ForestError("MODEL_TURN_BOUNDARY_CLOSED", "ModelTurn boundary closed", 409)
            principal_ref = None
            if tool is not None:
                trigger = await (
                    await conn.execute(
                        sql.SQL("SELECT principal_ref FROM {} WHERE bot_id=%s AND run_id=%s "
                                "AND trigger_ref=%s ORDER BY input_revision DESC LIMIT 1").format(
                            self._table("run_input_triggers")
                        ),
                        (self.store.bot_id, owner.run_id, tool.authority_trigger_ref),
                    )
                ).fetchone()
                if trigger is None:
                    raise ForestError(
                        "AGENT_TOOL_AUTHORITY_INVALID", "Tool trigger is not owned by this Run", 403
                    )
                principal_ref = trigger["principal_ref"]
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,turn_id,run_id,request_id,step_seq,protocol_version,"
                        "raw_candidate_digest,turn_payload_id,turn_payload_digest,"
                        "turn_owner_component_id,turn_owner_kind,turn_owner_id,assistant_payload_id,"
                        "assistant_payload_digest,function_call_payload_id,"
                        "function_call_payload_digest,terminal,thinking_only,"
                        "truncated_after_function_call,repair_of_request_id) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "RETURNING *"
                    ).format(self._table("model_turns")),
                    (
                        self.store.bot_id,
                        turn_id,
                        owner.run_id,
                        request_id,
                        request["step_seq"],
                        turn.protocol_version,
                        turn.raw_candidate_digest,
                        turn_payload.payload_id,
                        turn_payload.digest,
                        turn_payload.owner_component_id,
                        turn_payload.owner_kind,
                        turn_payload.owner_id,
                        assistant_payload.payload_id if assistant_payload else None,
                        assistant_payload.digest if assistant_payload else None,
                        tool.arguments.payload_id if tool else None,
                        tool.arguments.digest if tool else None,
                        turn.terminal,
                        turn.thinking_only,
                        turn.truncated_after_function_call,
                        repair_of_request_id,
                    ),
                )
            ).fetchone()
            if tool is not None:
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,tool_call_id,run_id,turn_id,step_seq,tool_name,"
                        "arguments_payload_id,arguments_payload_digest,authority_trigger_ref,"
                        "authority_principal_ref,tool_contract_digest,effect_operation_ref) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("tool_calls")),
                    (
                        self.store.bot_id,
                        tool.tool_call_id,
                        owner.run_id,
                        turn_id,
                        request["step_seq"],
                        function_call.name,
                        tool.arguments.payload_id,
                        tool.arguments.digest,
                        tool.authority_trigger_ref,
                        principal_ref,
                        tool.tool_contract_digest,
                        tool.effect_operation_ref,
                    ),
                )
                next_state = "WAIT_TOOL"
            elif turn.terminal:
                next_state = "SAFE_BOUNDARY"
            else:
                next_state = "PREPARING_MODEL"
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,next_step_seq=next_step_seq+1,"
                        "current_model_request_id=NULL,current_tool_call_id=%s,"
                        "tool_count=tool_count+%s,thinking_count=thinking_count+%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s").format(
                    self._table("agent_runs")
                ),
                (
                    next_state,
                    tool.tool_call_id if tool else None,
                    1 if tool else 0,
                    1 if turn.thinking_only else 0,
                    self.store.bot_id,
                    owner.run_id,
                ),
            )
            return dict(row)

    async def observe_tool(
        self,
        owner: AgentRunOwner,
        *,
        observation_operation_id: str,
        tool_call_id: str,
        observed_state: str,
        evidence_kind: str,
        effect_observation_ref: str | None = None,
        approval_ref: str | None = None,
        result: AgentPayloadRef | None = None,
        error_code: str | None = None,
        _connection=None,
    ) -> dict:
        _bounded(observation_operation_id, "observation_operation_id", 256)
        _bounded(tool_call_id, "tool_call_id", 256)
        _bounded(effect_observation_ref, "effect_observation_ref", 256, optional=True)
        _bounded(approval_ref, "approval_ref", 256, optional=True)
        _bounded(error_code, "error_code", 128, optional=True)
        if observed_state not in {
            "WAIT_APPROVAL",
            "DISPATCHING",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
            "UNKNOWN",
        }:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid tool observation", 422)
        if evidence_kind not in {"EXECUTOR", "RECONCILIATION", "AUTHORIZATION"}:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid tool evidence kind", 422)
        if observed_state == "WAIT_APPROVAL" and approval_ref is None:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Approval wait needs a reference", 422)
        if observed_state in {"DISPATCHING", "SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"}:
            if effect_observation_ref is None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Tool effect transition needs evidence", 422
                )
        if observed_state == "SUCCEEDED":
            if result is None or error_code is not None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Tool success needs only a result", 422
                )
        elif observed_state in {"FAILED", "CANCELLED"}:
            if error_code is None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Tool failure needs an error", 422
                )
        elif result is not None or error_code is not None:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Nonterminal tool evidence has a result", 422
            )
        if result is not None:
            result.validate("tool.result")
        input_digest = _digest(
            {
                "tool_call_id": tool_call_id,
                "observed_state": observed_state,
                "evidence_kind": evidence_kind,
                "effect_observation_ref": effect_observation_ref,
                "approval_ref": approval_ref,
                "result": result.canonical() if result else None,
                "error_code": error_code,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "tool-observation:" + observation_operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s "
                            "AND observation_operation_id=%s").format(
                        self._table("tool_observations")
                    ),
                    (self.store.bot_id, observation_operation_id),
                )
            ).fetchone()
            if replay is not None:
                await self._guard_owner(conn, owner)
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "TOOL_OBSERVATION_CONFLICT", "Tool observation input differs", 409
                    )
                return dict(replay)
            if result is not None:
                await self._verify_payload(conn, result)
            run = await self._guard_owner(conn, owner)
            tool = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND tool_call_id=%s FOR UPDATE").format(
                        self._table("tool_calls")
                    ),
                    (self.store.bot_id, tool_call_id),
                )
            ).fetchone()
            if tool is None or tool["run_id"] != owner.run_id:
                raise ForestError("AGENT_TOOL_NOT_FOUND", "Tool call does not exist", 404)
            if run["current_tool_call_id"] != tool_call_id:
                raise ForestError("AGENT_TOOL_STALE", "Tool call is no longer current", 409)
            previous = tool["state"]
            accepted = False
            conflict_code = None
            allowed = {
                "PROPOSED": {"WAIT_APPROVAL", "DISPATCHING", "FAILED", "CANCELLED"},
                "WAIT_APPROVAL": {"DISPATCHING", "FAILED", "CANCELLED"},
                "DISPATCHING": {"SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"},
            }
            if observed_state in allowed.get(previous, set()):
                accepted = True
            elif (
                previous == "UNKNOWN"
                and evidence_kind == "RECONCILIATION"
                and observed_state in {"SUCCEEDED", "FAILED", "CANCELLED"}
            ):
                accepted = True
            else:
                conflict_code = "TOOL_RESULT_ALREADY_SETTLED"
            resulting = observed_state if accepted else previous
            observation = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,observation_operation_id,tool_call_id,input_digest,"
                        "observed_state,evidence_kind,effect_observation_ref,approval_ref,"
                        "result_payload_id,result_payload_digest,error_code,accepted,previous_state,"
                        "resulting_state,conflict_code) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("tool_observations")),
                    (
                        self.store.bot_id,
                        observation_operation_id,
                        tool_call_id,
                        input_digest,
                        observed_state,
                        evidence_kind,
                        effect_observation_ref,
                        approval_ref,
                        result.payload_id if result else None,
                        result.digest if result else None,
                        error_code,
                        accepted,
                        previous,
                        resulting,
                        conflict_code,
                    ),
                )
            ).fetchone()
            if not accepted:
                return dict(observation)
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,approval_ref=COALESCE(%s,approval_ref),"
                        "result_payload_id=%s,result_payload_digest=%s,error_code=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND tool_call_id=%s").format(
                    self._table("tool_calls")
                ),
                (
                    observed_state,
                    approval_ref,
                    result.payload_id if result else None,
                    result.digest if result else None,
                    error_code,
                    self.store.bot_id,
                    tool_call_id,
                ),
            )
            run_state = {
                "WAIT_APPROVAL": "WAIT_APPROVAL",
                "DISPATCHING": "WAIT_TOOL",
                "UNKNOWN": "BLOCKED_UNKNOWN",
                "SUCCEEDED": "PREPARING_MODEL",
                "FAILED": "PREPARING_MODEL",
                "CANCELLED": "PREPARING_MODEL",
            }[observed_state]
            terminal_tool = observed_state in {"SUCCEEDED", "FAILED", "CANCELLED"}
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,current_tool_call_id=%s,"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND run_id=%s").format(
                    self._table("agent_runs")
                ),
                (
                    run_state,
                    None if terminal_tool else tool_call_id,
                    self.store.bot_id,
                    owner.run_id,
                ),
            )
            return dict(observation)

    async def checkpoint(
        self,
        owner: AgentRunOwner,
        *,
        checkpoint_id: str,
        state: AgentPayloadRef,
        _connection=None,
    ) -> dict:
        _bounded(checkpoint_id, "checkpoint_id", 256)
        state.validate("state")
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "checkpoint:" + checkpoint_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND checkpoint_id=%s").format(
                        self._table("run_checkpoints")
                    ),
                    (self.store.bot_id, checkpoint_id),
                )
            ).fetchone()
            if replay is not None:
                await self._guard_owner(conn, owner)
                expected_digest = _digest(
                    {
                        "run_id": owner.run_id,
                        "phase": replay["phase"],
                        "input_revision": replay["input_revision"],
                        "next_step_seq": replay["next_step_seq"],
                        "state": state.canonical(),
                        "owner_epoch": owner.epoch,
                    }
                )
                if replay["input_digest"] != expected_digest:
                    raise ForestError(
                        "AGENT_CHECKPOINT_CONFLICT", "Checkpoint identity differs", 409
                    )
                return dict(replay)
            await self._verify_payload(conn, state)
            run = await self._guard_owner(conn, owner)
            sequence = run["checkpoint_seq"] + 1
            input_digest = _digest(
                {
                    "run_id": owner.run_id,
                    "phase": run["state"],
                    "input_revision": run["input_revision"],
                    "next_step_seq": run["next_step_seq"],
                    "state": state.canonical(),
                    "owner_epoch": owner.epoch,
                }
            )
            checksum = _digest(
                [
                    owner.run_id,
                    sequence,
                    run["state"],
                    run["input_revision"],
                    run["next_step_seq"],
                    state.digest,
                    owner.epoch,
                ]
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,"
                        "phase,input_revision,next_step_seq,state_payload_id,state_payload_digest,"
                        "state_owner_component_id,state_owner_kind,state_owner_id,"
                        "checkpoint_checksum,owner_epoch) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("run_checkpoints")),
                    (
                        self.store.bot_id,
                        owner.run_id,
                        sequence,
                        checkpoint_id,
                        input_digest,
                        run["state"],
                        run["input_revision"],
                        run["next_step_seq"],
                        state.payload_id,
                        state.digest,
                        state.owner_component_id,
                        state.owner_kind,
                        state.owner_id,
                        checksum,
                        owner.epoch,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET checkpoint_seq=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND run_id=%s").format(self._table("agent_runs")),
                (sequence, self.store.bot_id, owner.run_id),
            )
            return dict(row)

    async def transition(
        self,
        owner: AgentRunOwner,
        *,
        checkpoint_id: str,
        expected_phase: str,
        target_phase: str,
        state: AgentPayloadRef,
        error_code: str | None = None,
        final_candidate: AgentPayloadRef | None = None,
        _connection=None,
    ) -> dict:
        _bounded(checkpoint_id, "checkpoint_id", 256)
        if expected_phase not in _RUN_PHASES or target_phase not in _RUN_PHASES:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid Run phase", 422)
        state.validate("state")
        _bounded(error_code, "error_code", 128, optional=True)
        if target_phase in {"FAILED", "CANCELLED"}:
            if error_code is None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "Failed terminal phase needs an error", 422
                )
        elif error_code is not None:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Non-failure phase cannot set an error", 422
            )
        if target_phase == "FINAL_READY":
            if final_candidate is None:
                raise ForestError(
                    "AGENT_RUNTIME_INPUT_INVALID", "FINAL_READY needs a final candidate", 422
                )
            final_candidate.validate("final_candidate")
        elif final_candidate is not None:
            raise ForestError(
                "AGENT_RUNTIME_INPUT_INVALID", "Final candidate only belongs to FINAL_READY", 422
            )
        input_digest = _digest(
            {
                "run_id": owner.run_id,
                "owner_id": owner.owner_id,
                "owner_epoch": owner.epoch,
                "expected_phase": expected_phase,
                "target_phase": target_phase,
                "state": state.canonical(),
                "error_code": error_code,
                "final_candidate": final_candidate.canonical() if final_candidate else None,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "checkpoint:" + checkpoint_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND checkpoint_id=%s").format(
                        self._table("run_checkpoints")
                    ),
                    (self.store.bot_id, checkpoint_id),
                )
            ).fetchone()
            if replay is not None:
                await self._guard_owner(conn, owner)
                if replay["input_digest"] != input_digest or replay["phase"] != target_phase:
                    raise ForestError(
                        "AGENT_CHECKPOINT_CONFLICT", "Transition checkpoint identity differs", 409
                    )
                return dict(replay)
            references = [state]
            if final_candidate is not None:
                references.append(final_candidate)
            for reference in sorted(references, key=lambda item: item.payload_id):
                await self._verify_payload(conn, reference)
            run = await self._guard_owner(conn, owner)
            if run["state"] != expected_phase:
                raise ForestError("AGENT_PHASE_CONFLICT", "Agent Run phase changed", 409)
            sequence = run["checkpoint_seq"] + 1
            checksum = _digest(
                [
                    owner.run_id,
                    sequence,
                    target_phase,
                    run["input_revision"],
                    run["next_step_seq"],
                    state.digest,
                    owner.epoch,
                ]
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,"
                        "phase,input_revision,next_step_seq,state_payload_id,state_payload_digest,"
                        "state_owner_component_id,state_owner_kind,state_owner_id,"
                        "checkpoint_checksum,owner_epoch) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("run_checkpoints")),
                    (
                        self.store.bot_id,
                        owner.run_id,
                        sequence,
                        checkpoint_id,
                        input_digest,
                        target_phase,
                        run["input_revision"],
                        run["next_step_seq"],
                        state.payload_id,
                        state.digest,
                        state.owner_component_id,
                        state.owner_kind,
                        state.owner_id,
                        checksum,
                        owner.epoch,
                    ),
                )
            ).fetchone()
            terminal_at = run["database_now"] if target_phase in _TERMINAL_RUN else None
            final_payload_id = (
                final_candidate.payload_id
                if final_candidate is not None
                else run["final_candidate_payload_id"]
            )
            final_payload_digest = (
                final_candidate.digest
                if final_candidate is not None
                else run["final_candidate_payload_digest"]
            )
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,checkpoint_seq=%s,error_code=%s,"
                        "final_candidate_payload_id=%s,final_candidate_payload_digest=%s,"
                        "terminal_at=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND run_id=%s").format(self._table("agent_runs")),
                (
                    target_phase,
                    sequence,
                    error_code,
                    final_payload_id,
                    final_payload_digest,
                    terminal_at,
                    self.store.bot_id,
                    owner.run_id,
                ),
            )
            if target_phase in {"FAILED", "CANCELLED"}:
                session_state = {
                    "FAILED": "FAILED",
                    "CANCELLED": "CANCELLED",
                }[target_phase]
                await conn.execute(
                    sql.SQL("UPDATE {} SET state=%s,active_run_id=NULL,terminal_reason_code=%s,"
                            "closed_at=clock_timestamp(),updated_at=clock_timestamp() "
                            "WHERE bot_id=%s AND session_id=%s AND state='ACTIVE' "
                            "AND active_run_id=%s").format(self._table("agent_sessions")),
                    (
                        session_state,
                        error_code,
                        self.store.bot_id,
                        run["session_id"],
                        owner.run_id,
                    ),
                )
            return dict(row)

    async def close_session(
        self,
        *,
        operation_id: str,
        session_id: str,
        completed_run_id: str,
        _connection=None,
    ) -> dict:
        """Close an idle Session only after its final completed Run has no successor."""

        for value, field in (
            (operation_id, "operation_id"),
            (session_id, "session_id"),
            (completed_run_id, "completed_run_id"),
        ):
            _bounded(value, field, 256)
        input_digest = _digest(
            {
                "session_id": session_id,
                "completed_run_id": completed_run_id,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "session-close:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("session_close_operations")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "AGENT_SESSION_CLOSE_CONFLICT", "Session close input differs", 409
                    )
                return dict(replay)
            await self._lock(conn, "session:" + session_id)
            session = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND session_id=%s FOR UPDATE").format(
                        self._table("agent_sessions")
                    ),
                    (self.store.bot_id, session_id),
                )
            ).fetchone()
            run = await self._run(conn, completed_run_id, update=True)
            if (
                session is None
                or session["state"] != "ACTIVE"
                or session["active_run_id"] != completed_run_id
                or run["session_id"] != session_id
                or run["state"] != "COMPLETED"
            ):
                raise ForestError(
                    "AGENT_SESSION_CLOSE_CONFLICT",
                    "Session is not idle at the completed Run",
                    409,
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='CLOSED',active_run_id=NULL,closed_at=clock_timestamp(),"
                        "updated_at=clock_timestamp() WHERE bot_id=%s AND session_id=%s").format(
                    self._table("agent_sessions")
                ),
                (self.store.bot_id, session_id),
            )
            receipt = await (
                await conn.execute(
                    sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,session_id,"
                            "completed_run_id) VALUES (%s,%s,%s,%s,%s) RETURNING *").format(
                        self._table("session_close_operations")
                    ),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        session_id,
                        completed_run_id,
                    ),
                )
            ).fetchone()
            return dict(receipt)

    async def load(self, run_id: str, *, _connection=None) -> dict:
        _bounded(run_id, "run_id", 256)
        async with self._transaction(_connection) as conn:
            return await self._expanded_run(conn, run_id)

    async def recoverable_runs(self, *, limit: int = 64, _connection=None) -> tuple[dict, ...]:
        """List active Session heads that must resume or be explicitly closed.

        A completed Run remains the head until Conversation either installs a
        successor or confirms that the group queue was silently drained.  It is
        therefore intentionally included in recovery discovery.
        """

        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ForestError("AGENT_RUNTIME_INPUT_INVALID", "Invalid recovery limit", 422)
        async with self._transaction(_connection) as conn:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT s.session_id,s.conversation_ref,s.session_epoch,"
                        "s.active_run_id,r.state AS run_state,r.owner_id,r.owner_epoch,"
                        "r.owner_expires_at,r.absolute_deadline,r.checkpoint_seq,r.updated_at "
                        "FROM {} s JOIN {} r ON r.bot_id=s.bot_id AND r.run_id=s.active_run_id "
                        "WHERE s.bot_id=%s AND s.state='ACTIVE' "
                        "ORDER BY r.updated_at,r.run_id LIMIT %s"
                    ).format(
                        self._table("agent_sessions"),
                        self._table("agent_runs"),
                    ),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            return tuple(dict(row) for row in rows)

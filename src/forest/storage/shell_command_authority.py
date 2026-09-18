"""Clean Shell policy, route, admission and execution-claim authority."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")
_LOCK_DOMAIN = b"forest.shell.command/1\0"


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("SHELL_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _validated_digest(value, field: str, *, optional: bool = False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("SHELL_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _timestamp(value, field: str):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ForestError("SHELL_INPUT_INVALID", f"{field} needs a timezone", 422)
    return value.astimezone(timezone.utc)


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
        raise ForestError("SHELL_INPUT_INVALID", "Shell input is not canonical", 422) from exc


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _lock_key(bot_id: str, identity: str) -> int:
    digest = hashlib.sha256(
        _LOCK_DOMAIN + bot_id.encode() + b"\0" + identity.encode()
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def _call(port, *args, **values):
    result = port(*args, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class ShellPayloadRef:
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


@dataclass(frozen=True)
class ShellPolicyGrant:
    scope_kind: str
    principal_ref: str
    allow_p0: bool
    conversation_ref: str | None = None

    def validate(self) -> None:
        if self.scope_kind not in {"BINDING", "CONVERSATION"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell policy scope", 422)
        _bounded(self.principal_ref, "grant.principal_ref", 256)
        if type(self.allow_p0) is not bool:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell P0 grant", 422)
        _bounded(self.conversation_ref, "grant.conversation_ref", 256, optional=True)
        if (self.scope_kind == "BINDING") != (self.conversation_ref is None):
            raise ForestError("SHELL_INPUT_INVALID", "Shell grant scope is incomplete", 422)

    def canonical(self) -> dict:
        return {
            "scope_kind": self.scope_kind,
            "conversation_ref": self.conversation_ref,
            "principal_ref": self.principal_ref,
            "allow_p0": self.allow_p0,
        }


@dataclass(frozen=True)
class ShellCommandRoute:
    command_id: str
    command_version: int
    owner_component_id: str
    owner_generation_id: str
    argument_schema_digest: str
    management_operation_id: str
    target_scope: str
    service_class: str
    required_capabilities_digest: str
    output_policy: str
    timeout_seconds: int

    def validate(self) -> None:
        _bounded(self.command_id, "route.command_id", 128)
        if type(self.command_version) is not int or self.command_version <= 0:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid command version", 422)
        _bounded(self.owner_component_id, "route.owner_component_id", 128)
        _bounded(self.owner_generation_id, "route.owner_generation_id", 256)
        _validated_digest(self.argument_schema_digest, "route.argument_schema_digest")
        _bounded(self.management_operation_id, "route.management_operation_id", 256)
        if self.target_scope not in {"CONVERSATION", "BINDING", "BOT", "DEPLOYMENT"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid command target scope", 422)
        if self.service_class not in {"P0", "P1"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell service class", 422)
        _validated_digest(
            self.required_capabilities_digest, "route.required_capabilities_digest"
        )
        if self.output_policy not in {"SHORT", "ARTIFACT", "SILENT"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid command output policy", 422)
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 900:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid command timeout", 422)

    def canonical(self) -> dict:
        return vars(self)

    @property
    def route_digest(self) -> str:
        return _digest(self.canonical())


@dataclass(frozen=True)
class ShellAdmissionProof:
    bot_id: str
    binding_ref: str
    binding_epoch: int
    connection_epoch: int
    client_incarnation: str
    adapter_generation_id: str
    executor_fence_digest: str
    conversation_ref: str
    event_ref: str
    principal_ref: str
    command_line_digest: str
    nonce: str
    expires_at: datetime

    def validate(self) -> None:
        for value, field, limit in (
            (self.bot_id, "proof.bot_id", 256),
            (self.binding_ref, "proof.binding_ref", 256),
            (self.client_incarnation, "proof.client_incarnation", 256),
            (self.adapter_generation_id, "proof.adapter_generation_id", 256),
            (self.conversation_ref, "proof.conversation_ref", 256),
            (self.event_ref, "proof.event_ref", 512),
            (self.principal_ref, "proof.principal_ref", 256),
            (self.nonce, "proof.nonce", 256),
        ):
            _bounded(value, field, limit)
        if type(self.binding_epoch) is not int or self.binding_epoch <= 0:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid proof Binding epoch", 422)
        if type(self.connection_epoch) is not int or self.connection_epoch <= 0:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid proof connection epoch", 422)
        _validated_digest(self.executor_fence_digest, "proof.executor_fence_digest")
        _validated_digest(self.command_line_digest, "proof.command_line_digest")
        _timestamp(self.expires_at, "proof.expires_at")

    def canonical(self) -> dict:
        return {
            **vars(self),
            "expires_at": self.expires_at.astimezone(timezone.utc).isoformat(),
        }

    def admission_identity(self) -> dict:
        """Stable event identity; nonce and expiry are only live gate evidence."""

        return {
            "bot_id": self.bot_id,
            "binding_ref": self.binding_ref,
            "binding_epoch": self.binding_epoch,
            "connection_epoch": self.connection_epoch,
            "client_incarnation": self.client_incarnation,
            "adapter_generation_id": self.adapter_generation_id,
            "executor_fence_digest": self.executor_fence_digest,
            "conversation_ref": self.conversation_ref,
            "event_ref": self.event_ref,
            "principal_ref": self.principal_ref,
            "command_line_digest": self.command_line_digest,
        }

    @property
    def proof_digest(self) -> str:
        return _digest(self.canonical())


@dataclass(frozen=True)
class ShellClaim:
    bot_id: str
    request_id: str
    worker_id: str
    owner_epoch: int
    owner_expires_at: datetime
    binding_ref: str
    binding_epoch: int
    connection_epoch: int
    client_incarnation: str
    adapter_generation_id: str
    executor_fence_digest: str
    conversation_ref: str
    principal_ref: str
    policy_revision: int
    registry_revision: int
    command_id: str
    command_version: int
    owner_component_id: str
    owner_generation_id: str
    argument_schema_digest: str
    management_operation_id: str
    target_scope: str
    effective_service_class: str
    required_capabilities_digest: str
    route_digest: str
    arguments: ShellPayloadRef


class ShellCommandRepository:
    """Own Shell policy, frozen command requests and execution claims."""

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier=None,
        proof_verifier=None,
        admission_parser=None,
        binding_fence_verifier=None,
        generation_verifier=None,
        authorization_consumer=None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.shell.command")
        self.payload_verifier = payload_verifier
        self.proof_verifier = proof_verifier
        self.admission_parser = admission_parser
        self.binding_fence_verifier = binding_fence_verifier
        self.generation_verifier = generation_verifier
        self.authorization_consumer = authorization_consumer

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _lock(self, conn, identity: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (_lock_key(self.store.bot_id, identity),),
        )

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    async def _verify_payload(self, conn, reference: ShellPayloadRef) -> None:
        if self.payload_verifier is None:
            raise ForestError("PAYLOAD_AUTHORITY_MISSING", "Shell payload port is unavailable", 503)
        await _call(
            self.payload_verifier,
            conn,
            payload_id=reference.payload_id,
            digest=reference.digest,
            owner_component_id=reference.owner_component_id,
            owner_kind=reference.owner_kind,
            owner_id=reference.owner_id,
        )

    async def policy_revision_in(self, conn, binding_ref: str) -> int:
        _bounded(binding_ref, "binding_ref", 256)
        row = await (
            await conn.execute(
                sql.SQL("SELECT revision FROM {} WHERE bot_id=%s AND binding_ref=%s").format(
                    self._table("policy_heads")
                ),
                (self.store.bot_id, binding_ref),
            )
        ).fetchone()
        return 0 if row is None else row["revision"]

    async def registry_revision_in(self, conn) -> int:
        row = await (
            await conn.execute(
                sql.SQL("SELECT revision FROM {} WHERE bot_id=%s").format(
                    self._table("registry_heads")
                ),
                (self.store.bot_id,),
            )
        ).fetchone()
        return 0 if row is None else row["revision"]

    async def publish_policy(
        self,
        *,
        operation_id: str,
        binding_ref: str,
        expected_revision: int,
        actor_principal_ref: str,
        grants,
        _connection=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (binding_ref, "binding_ref", 256),
            (actor_principal_ref, "actor_principal_ref", 256),
        ):
            _bounded(value, field, limit)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid expected policy revision", 422)
        items = tuple(grants)
        if len(items) > 4096 or any(type(item) is not ShellPolicyGrant for item in items):
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell grant set", 422)
        for item in items:
            item.validate()
        canonical = sorted(
            (item.canonical() for item in items),
            key=lambda item: (
                item["scope_kind"],
                item["conversation_ref"] or "",
                item["principal_ref"],
            ),
        )
        if len({
            (item["scope_kind"], item["conversation_ref"], item["principal_ref"])
            for item in canonical
        }) != len(canonical):
            raise ForestError("SHELL_INPUT_INVALID", "Shell grant identity repeats", 422)
        input_digest = _digest(
            {
                "binding_ref": binding_ref,
                "expected_revision": expected_revision,
                "actor_principal_ref": actor_principal_ref,
                "grants": canonical,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "policy-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("policy_revisions")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest or replay["binding_ref"] != binding_ref:
                    raise ForestError("SHELL_POLICY_CONFLICT", "Policy operation input differs", 409)
                return dict(replay)
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,binding_ref) VALUES (%s,%s) "
                        "ON CONFLICT DO NOTHING").format(self._table("policy_heads")),
                (self.store.bot_id, binding_ref),
            )
            head = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND binding_ref=%s FOR UPDATE").format(
                        self._table("policy_heads")
                    ),
                    (self.store.bot_id, binding_ref),
                )
            ).fetchone()
            if head["revision"] != expected_revision:
                raise ForestError("SHELL_POLICY_CONFLICT", "Shell policy revision changed", 409)
            revision = expected_revision + 1
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,binding_ref,revision,previous_revision,operation_id,"
                        "input_digest,actor_principal_ref,grant_count) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("policy_revisions")),
                    (
                        self.store.bot_id,
                        binding_ref,
                        revision,
                        expected_revision,
                        operation_id,
                        input_digest,
                        actor_principal_ref,
                        len(canonical),
                    ),
                )
            ).fetchone()
            for index, item in enumerate(canonical):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,binding_ref,revision,grant_index,scope_kind,"
                        "conversation_ref,principal_ref,allow_p0) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("policy_grants")),
                    (
                        self.store.bot_id,
                        binding_ref,
                        revision,
                        index,
                        item["scope_kind"],
                        item["conversation_ref"],
                        item["principal_ref"],
                        item["allow_p0"],
                    ),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET revision=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND binding_ref=%s").format(
                    self._table("policy_heads")
                ),
                (revision, self.store.bot_id, binding_ref),
            )
            return dict(row)

    async def publish_registry(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        graph_digest: str,
        routes,
        _connection=None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid registry revision", 422)
        _validated_digest(graph_digest, "graph_digest")
        items = tuple(routes)
        if not 1 <= len(items) <= 4096 or any(
            type(item) is not ShellCommandRoute for item in items
        ):
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell command registry", 422)
        for item in items:
            item.validate()
        ordered = sorted(items, key=lambda item: item.command_id)
        if len({item.command_id for item in ordered}) != len(ordered):
            raise ForestError("SHELL_INPUT_INVALID", "Shell command route repeats", 422)
        input_digest = _digest(
            {
                "expected_revision": expected_revision,
                "graph_digest": graph_digest,
                "routes": [item.canonical() for item in ordered],
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "registry-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("registry_revisions")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("SHELL_REGISTRY_CONFLICT", "Registry operation input differs", 409)
                return dict(replay)
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id) VALUES (%s) ON CONFLICT DO NOTHING").format(
                    self._table("registry_heads")
                ),
                (self.store.bot_id,),
            )
            head = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("registry_heads")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if head["revision"] != expected_revision:
                raise ForestError("SHELL_REGISTRY_CONFLICT", "Shell registry revision changed", 409)
            revision = expected_revision + 1
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,revision,previous_revision,operation_id,input_digest,"
                        "graph_digest,route_count) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("registry_revisions")),
                    (
                        self.store.bot_id,
                        revision,
                        expected_revision,
                        operation_id,
                        input_digest,
                        graph_digest,
                        len(ordered),
                    ),
                )
            ).fetchone()
            for route in ordered:
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,registry_revision,command_id,command_version,"
                        "owner_component_id,owner_generation_id,argument_schema_digest,"
                        "management_operation_id,target_scope,service_class,"
                        "required_capabilities_digest,output_policy,timeout_seconds,route_digest) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("command_routes")),
                    (
                        self.store.bot_id,
                        revision,
                        route.command_id,
                        route.command_version,
                        route.owner_component_id,
                        route.owner_generation_id,
                        route.argument_schema_digest,
                        route.management_operation_id,
                        route.target_scope,
                        route.service_class,
                        route.required_capabilities_digest,
                        route.output_policy,
                        route.timeout_seconds,
                        route.route_digest,
                    ),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET revision=%s,graph_digest=%s,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s").format(self._table("registry_heads")),
                (revision, graph_digest, self.store.bot_id),
            )
            return dict(row)

    async def _policy_qualification(self, conn, proof: ShellAdmissionProof) -> tuple[int, bool, bool]:
        head = await (
            await conn.execute(
                sql.SQL("SELECT revision FROM {} WHERE bot_id=%s AND binding_ref=%s FOR SHARE").format(
                    self._table("policy_heads")
                ),
                (self.store.bot_id, proof.binding_ref),
            )
        ).fetchone()
        if head is None or head["revision"] == 0:
            return 0, False, False
        grants = await (
            await conn.execute(
                sql.SQL("SELECT allow_p0 FROM {} WHERE bot_id=%s AND binding_ref=%s "
                        "AND revision=%s AND principal_ref=%s AND "
                        "(scope_kind='BINDING' OR (scope_kind='CONVERSATION' "
                        "AND conversation_ref=%s))").format(self._table("policy_grants")),
                (
                    self.store.bot_id,
                    proof.binding_ref,
                    head["revision"],
                    proof.principal_ref,
                    proof.conversation_ref,
                ),
            )
        ).fetchall()
        return head["revision"], bool(grants), any(row["allow_p0"] for row in grants)

    async def admit_command(
        self,
        *,
        operation_id: str,
        request_id: str,
        proof: ShellAdmissionProof,
        command_id: str,
        command_version: int,
        arguments: ShellPayloadRef,
        idempotency_key: str,
        received_at: datetime,
        admission_deadline: datetime,
        _connection=None,
    ) -> dict:
        for value, field, limit in (
            (operation_id, "operation_id", 256),
            (request_id, "request_id", 256),
            (command_id, "command_id", 128),
            (idempotency_key, "idempotency_key", 256),
        ):
            _bounded(value, field, limit)
        if type(proof) is not ShellAdmissionProof:
            raise ForestError("SHELL_PROOF_INVALID", "Invalid Shell ingress proof", 403)
        proof.validate()
        if proof.bot_id != self.store.bot_id:
            raise ForestError("SHELL_PROOF_INVALID", "Shell proof Bot differs", 403)
        if type(command_version) is not int or command_version <= 0:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid command version", 422)
        arguments.validate("arguments")
        received_at = _timestamp(received_at, "received_at")
        admission_deadline = _timestamp(admission_deadline, "admission_deadline")
        if not received_at < admission_deadline <= received_at + timedelta(minutes=2):
            raise ForestError("SHELL_INPUT_INVALID", "Shell admission deadline is outside its bound", 422)
        input_digest = _digest(
            {
                "request_id": request_id,
                "proof": proof.admission_identity(),
                "command_id": command_id,
                "command_version": command_version,
                "arguments": arguments.canonical(),
                "idempotency_key": idempotency_key,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "admission-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND admission_operation_id=%s").format(
                        self._table("shell_requests")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest or replay["request_id"] != request_id:
                    raise ForestError("SHELL_ADMISSION_CONFLICT", "Admission input differs", 409)
                return dict(replay)
            occupied = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND "
                            "(request_id=%s OR event_ref=%s OR idempotency_key=%s) "
                            "FOR UPDATE").format(self._table("shell_requests")),
                    (
                        self.store.bot_id,
                        request_id,
                        proof.event_ref,
                        idempotency_key,
                    ),
                )
            ).fetchall()
            if occupied:
                if len(occupied) == 1 and occupied[0]["input_digest"] == input_digest:
                    return dict(occupied[0])
                raise ForestError(
                    "SHELL_ADMISSION_CONFLICT",
                    "Shell request, event or idempotency identity differs",
                    409,
                )
            if self.proof_verifier is None:
                raise ForestError("SHELL_GATE_MISSING", "Shell ingress proof port is unavailable", 503)
            command_text = await _call(self.proof_verifier, proof)
            if not isinstance(command_text, str):
                raise ForestError("SHELL_PROOF_INVALID", "Shell proof has no command text", 403)
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            if proof.expires_at <= now["now"] or admission_deadline <= now["now"]:
                raise ForestError("SHELL_PROOF_EXPIRED", "Shell ingress proof expired", 409)
            if self.binding_fence_verifier is None:
                raise ForestError("BINDING_AUTHORITY_MISSING", "Shell Binding port is unavailable", 503)
            await _call(
                self.binding_fence_verifier,
                conn,
                binding_ref=proof.binding_ref,
                binding_epoch=proof.binding_epoch,
                connection_epoch=proof.connection_epoch,
                client_incarnation=proof.client_incarnation,
                adapter_generation_id=proof.adapter_generation_id,
                executor_fence_digest=proof.executor_fence_digest,
            )
            await self._verify_payload(conn, arguments)
            registry = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR SHARE").format(
                        self._table("registry_heads")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            route = None
            if registry is not None and registry["revision"] > 0:
                route = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND registry_revision=%s "
                                "AND command_id=%s").format(self._table("command_routes")),
                        (self.store.bot_id, registry["revision"], command_id),
                    )
                ).fetchone()
                if route is not None and route["command_version"] != command_version:
                    route = None
            arguments_valid = True
            if route is not None:
                if self.admission_parser is None:
                    raise ForestError(
                        "SHELL_PARSER_MISSING", "Shell command parser port is unavailable", 503
                    )
                arguments_valid = bool(
                    await _call(
                        self.admission_parser,
                        command_text=command_text,
                        command_id=command_id,
                        command_version=command_version,
                        argument_schema_digest=route["argument_schema_digest"],
                        arguments=arguments,
                    )
                )
            policy_revision, authorized, allow_p0 = await self._policy_qualification(conn, proof)
            rejection_code = None
            if route is None:
                state = "REJECTED"
                rejection_code = "SHELL_COMMAND_NOT_FOUND"
            elif not arguments_valid:
                state = "REJECTED"
                rejection_code = "SHELL_ARGUMENTS_INVALID"
            elif not authorized:
                state = "REJECTED"
                rejection_code = "SHELL_FORBIDDEN"
            else:
                state = "QUEUED"
            requested = route["service_class"] if route is not None else None
            effective = "P0" if requested == "P0" and allow_p0 else "P1"
            completed_at = now["now"] if state == "REJECTED" else None
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,request_id,admission_operation_id,input_digest,"
                        "event_ref,conversation_ref,binding_ref,binding_epoch,connection_epoch,"
                        "client_incarnation,adapter_generation_id,executor_fence_digest,principal_ref,"
                        "gate_proof_digest,command_line_digest,command_id,command_version,"
                        "policy_revision,registry_revision,graph_digest,owner_component_id,"
                        "owner_generation_id,argument_schema_digest,management_operation_id,"
                        "target_scope,requested_service_class,effective_service_class,"
                        "required_capabilities_digest,output_policy,timeout_seconds,route_digest,"
                        "arguments_payload_id,arguments_payload_digest,arguments_owner_component_id,"
                        "arguments_owner_kind,arguments_owner_id,idempotency_key,state,rejection_code,"
                        "received_at,admission_deadline,completed_at) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("shell_requests")),
                    (
                        self.store.bot_id,
                        request_id,
                        operation_id,
                        input_digest,
                        proof.event_ref,
                        proof.conversation_ref,
                        proof.binding_ref,
                        proof.binding_epoch,
                        proof.connection_epoch,
                        proof.client_incarnation,
                        proof.adapter_generation_id,
                        proof.executor_fence_digest,
                        proof.principal_ref,
                        proof.proof_digest,
                        proof.command_line_digest,
                        command_id,
                        command_version,
                        policy_revision,
                        registry["revision"] if route is not None else 0,
                        registry["graph_digest"] if route is not None else None,
                        route["owner_component_id"] if route is not None else None,
                        route["owner_generation_id"] if route is not None else None,
                        route["argument_schema_digest"] if route is not None else None,
                        route["management_operation_id"] if route is not None else None,
                        route["target_scope"] if route is not None else None,
                        requested,
                        effective,
                        route["required_capabilities_digest"] if route is not None else None,
                        route["output_policy"] if route is not None else None,
                        route["timeout_seconds"] if route is not None else None,
                        route["route_digest"] if route is not None else None,
                        arguments.payload_id,
                        arguments.digest,
                        arguments.owner_component_id,
                        arguments.owner_kind,
                        arguments.owner_id,
                        idempotency_key,
                        state,
                        rejection_code,
                        received_at,
                        admission_deadline,
                        completed_at,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def admit_command_in(self, conn, **values) -> dict:
        return await self.admit_command(_connection=conn, **values)

    @staticmethod
    def _claim_from(row) -> ShellClaim:
        return ShellClaim(
            bot_id=row["bot_id"],
            request_id=row["request_id"],
            worker_id=row["owner_id"],
            owner_epoch=row["owner_epoch"],
            owner_expires_at=row["owner_expires_at"],
            binding_ref=row["binding_ref"],
            binding_epoch=row["binding_epoch"],
            connection_epoch=row["connection_epoch"],
            client_incarnation=row["client_incarnation"],
            adapter_generation_id=row["adapter_generation_id"],
            executor_fence_digest=row["executor_fence_digest"],
            conversation_ref=row["conversation_ref"],
            principal_ref=row["principal_ref"],
            policy_revision=row["policy_revision"],
            registry_revision=row["registry_revision"],
            command_id=row["command_id"],
            command_version=row["command_version"],
            owner_component_id=row["owner_component_id"],
            owner_generation_id=row["owner_generation_id"],
            argument_schema_digest=row["argument_schema_digest"],
            management_operation_id=row["management_operation_id"],
            target_scope=row["target_scope"],
            effective_service_class=row["effective_service_class"],
            required_capabilities_digest=row["required_capabilities_digest"],
            route_digest=row["route_digest"],
            arguments=ShellPayloadRef(
                row["arguments_payload_id"],
                row["arguments_payload_digest"],
                row["arguments_owner_component_id"],
                row["arguments_owner_kind"],
                row["arguments_owner_id"],
            ),
        )

    async def claim(
        self,
        *,
        operation_id: str,
        worker_id: str,
        lease_seconds: int = 30,
        service_classes=("P0", "P1"),
    ) -> ShellClaim | None:
        _bounded(operation_id, "operation_id", 256)
        _bounded(worker_id, "worker_id", 256)
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 120:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell claim lease", 422)
        classes = tuple(service_classes)
        if not classes or set(classes) - {"P0", "P1"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell claim classes", 422)
        input_digest = _digest(
            {
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
                "service_classes": sorted(set(classes)),
            }
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._lock(conn, "claim-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT r.*,c.input_digest AS claim_input_digest,"
                            "c.owner_id AS claim_owner_id,c.owner_epoch AS claim_owner_epoch,"
                            "c.owner_expires_at AS claim_owner_expires_at FROM {} c JOIN {} r "
                            "ON r.bot_id=c.bot_id "
                            "AND r.request_id=c.request_id WHERE c.bot_id=%s "
                            "AND c.operation_id=%s").format(
                        self._table("claim_operations"), self._table("shell_requests")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["claim_input_digest"] != input_digest:
                    raise ForestError("SHELL_CLAIM_CONFLICT", "Claim operation input differs", 409)
                reconstructed = dict(replay)
                reconstructed["owner_id"] = replay["claim_owner_id"]
                reconstructed["owner_epoch"] = replay["claim_owner_epoch"]
                reconstructed["owner_expires_at"] = replay["claim_owner_expires_at"]
                return self._claim_from(reconstructed)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT *,clock_timestamp() AS database_now FROM {} WHERE bot_id=%s "
                        "AND effective_service_class=ANY(%s) AND "
                        "(state='QUEUED' OR (state='CLAIMED' AND owner_expires_at<=clock_timestamp())) "
                        "ORDER BY CASE effective_service_class WHEN 'P0' THEN 0 ELSE 1 END,"
                        "received_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1"
                    ).format(self._table("shell_requests")),
                    (self.store.bot_id, list(set(classes))),
                )
            ).fetchone()
            if row is None:
                return None
            epoch = row["owner_epoch"] + 1
            expires = row["database_now"] + timedelta(seconds=lease_seconds)
            claimed = await (
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='CLAIMED',owner_id=%s,owner_epoch=%s,"
                            "owner_expires_at=%s,updated_at=%s WHERE bot_id=%s AND request_id=%s "
                            "RETURNING *").format(self._table("shell_requests")),
                    (
                        worker_id,
                        epoch,
                        expires,
                        row["database_now"],
                        self.store.bot_id,
                        row["request_id"],
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("INSERT INTO {}(bot_id,operation_id,input_digest,request_id,owner_id,"
                        "owner_epoch,owner_expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s)").format(
                    self._table("claim_operations")
                ),
                (
                    self.store.bot_id,
                    operation_id,
                    input_digest,
                    row["request_id"],
                    worker_id,
                    epoch,
                    expires,
                ),
            )
            return self._claim_from(claimed)

    async def _guard_claim(self, conn, claim: ShellClaim):
        if type(claim) is not ShellClaim or claim.bot_id != self.store.bot_id:
            raise ForestError("STALE_SHELL_CLAIM", "Invalid Shell claim", 409)
        row = await (
            await conn.execute(
                sql.SQL("SELECT *,clock_timestamp() AS database_now FROM {} WHERE bot_id=%s "
                        "AND request_id=%s FOR UPDATE").format(self._table("shell_requests")),
                (self.store.bot_id, claim.request_id),
            )
        ).fetchone()
        if (
            row is None
            or row["state"] not in {"CLAIMED", "EXECUTING", "WAITING_APPROVAL"}
            or row["owner_id"] != claim.worker_id
            or row["owner_epoch"] != claim.owner_epoch
            or row["owner_expires_at"] != claim.owner_expires_at
            or row["owner_expires_at"] <= row["database_now"]
            or row["binding_ref"] != claim.binding_ref
            or row["binding_epoch"] != claim.binding_epoch
            or row["connection_epoch"] != claim.connection_epoch
            or row["client_incarnation"] != claim.client_incarnation
            or row["adapter_generation_id"] != claim.adapter_generation_id
            or row["executor_fence_digest"] != claim.executor_fence_digest
            or row["policy_revision"] != claim.policy_revision
            or row["registry_revision"] != claim.registry_revision
            or row["command_id"] != claim.command_id
            or row["command_version"] != claim.command_version
            or row["owner_component_id"] != claim.owner_component_id
            or row["owner_generation_id"] != claim.owner_generation_id
            or row["argument_schema_digest"] != claim.argument_schema_digest
            or row["management_operation_id"] != claim.management_operation_id
            or row["target_scope"] != claim.target_scope
            or row["effective_service_class"] != claim.effective_service_class
            or row["required_capabilities_digest"] != claim.required_capabilities_digest
            or row["route_digest"] != claim.route_digest
            or row["arguments_payload_id"] != claim.arguments.payload_id
            or row["arguments_payload_digest"] != claim.arguments.digest
        ):
            raise ForestError("STALE_SHELL_CLAIM", "Shell claim changed or expired", 409)
        return row

    @staticmethod
    def invocation_input_digest(claim: ShellClaim) -> str:
        if type(claim) is not ShellClaim:
            raise ForestError("STALE_SHELL_CLAIM", "Invalid Shell claim", 409)
        return _digest(
            {
                "request_id": claim.request_id,
                "principal_ref": claim.principal_ref,
                "command_id": claim.command_id,
                "command_version": claim.command_version,
                "route_digest": claim.route_digest,
                "argument_schema_digest": claim.argument_schema_digest,
                "arguments": claim.arguments.canonical(),
            }
        )

    async def authorize_claim(
        self,
        claim: ShellClaim,
        *,
        ticket_id: str,
        authorization_operation_id: str,
        _connection=None,
    ) -> dict:
        _bounded(ticket_id, "ticket_id", 256)
        _bounded(authorization_operation_id, "authorization_operation_id", 256)
        async with self._transaction(_connection) as conn:
            row = await self._guard_claim(conn, claim)
            if self.binding_fence_verifier is None:
                raise ForestError("BINDING_AUTHORITY_MISSING", "Shell Binding port is unavailable", 503)
            await _call(
                self.binding_fence_verifier,
                conn,
                binding_ref=claim.binding_ref,
                binding_epoch=claim.binding_epoch,
                connection_epoch=claim.connection_epoch,
                client_incarnation=claim.client_incarnation,
                adapter_generation_id=claim.adapter_generation_id,
                executor_fence_digest=claim.executor_fence_digest,
            )
            head = await (
                await conn.execute(
                    sql.SQL("SELECT revision FROM {} WHERE bot_id=%s AND binding_ref=%s FOR SHARE").format(
                        self._table("policy_heads")
                    ),
                    (self.store.bot_id, claim.binding_ref),
                )
            ).fetchone()
            if head is None:
                raise ForestError("SHELL_FORBIDDEN", "Shell policy is unavailable", 403)
            grants = await (
                await conn.execute(
                    sql.SQL("SELECT allow_p0 FROM {} WHERE bot_id=%s AND binding_ref=%s "
                            "AND revision=%s AND principal_ref=%s AND "
                            "(scope_kind='BINDING' OR (scope_kind='CONVERSATION' "
                            "AND conversation_ref=%s))").format(self._table("policy_grants")),
                    (
                        self.store.bot_id,
                        claim.binding_ref,
                        head["revision"],
                        claim.principal_ref,
                        claim.conversation_ref,
                    ),
                )
            ).fetchall()
            if not grants or (
                claim.effective_service_class == "P0"
                and not any(grant["allow_p0"] for grant in grants)
            ):
                raise ForestError("SHELL_FORBIDDEN", "Shell authority was revoked", 403)
            if self.generation_verifier is None:
                raise ForestError(
                    "GENERATION_AUTHORITY_MISSING",
                    "Shell command Generation port is unavailable",
                    503,
                )
            await _call(
                self.generation_verifier,
                conn,
                component_id=claim.owner_component_id,
                generation_id=claim.owner_generation_id,
                graph_digest=row["graph_digest"],
                route_digest=claim.route_digest,
            )
            await self._verify_payload(conn, claim.arguments)
            if self.authorization_consumer is None:
                raise ForestError(
                    "AUTHORIZATION_AUTHORITY_MISSING",
                    "Shell authorization ticket port is unavailable",
                    503,
                )
            authorization = await _call(
                self.authorization_consumer,
                conn,
                ticket_id=ticket_id,
                operation_id=authorization_operation_id,
                action="management.invoke",
                resource_kind="bot",
                resource_id=self.store.bot_id,
                input_digest=self.invocation_input_digest(claim),
                plan_digest=claim.required_capabilities_digest,
                audience_component_id=claim.owner_component_id,
                audience_generation_id=claim.owner_generation_id,
            )
            return {
                "request_id": claim.request_id,
                "command_id": claim.command_id,
                "management_operation_id": claim.management_operation_id,
                "owner_component_id": claim.owner_component_id,
                "owner_generation_id": claim.owner_generation_id,
                "current_policy_revision": head["revision"],
                "arguments": claim.arguments,
                "authorization": authorization,
                "request": dict(row),
            }

    async def authorize_claim_in(self, conn, claim: ShellClaim, **values) -> dict:
        return await self.authorize_claim(claim, _connection=conn, **values)

    async def start(self, claim: ShellClaim) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await self._guard_claim(conn, claim)
            if row["state"] == "EXECUTING":
                return dict(row)
            if row["state"] != "CLAIMED":
                raise ForestError("SHELL_STATE_CONFLICT", "Shell request cannot start", 409)
            return dict(
                await (
                    await conn.execute(
                        sql.SQL("UPDATE {} SET state='EXECUTING',started_at=clock_timestamp(),"
                                "updated_at=clock_timestamp() WHERE bot_id=%s AND request_id=%s "
                                "RETURNING *").format(self._table("shell_requests")),
                        (self.store.bot_id, claim.request_id),
                    )
                ).fetchone()
            )

    async def complete(
        self,
        claim: ShellClaim,
        *,
        operation_id: str,
        resulting_state: str,
        result: ShellPayloadRef,
        authorization_ticket_id: str,
        authorization_operation_id: str,
        error_code: str | None = None,
        _connection=None,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(authorization_ticket_id, "authorization_ticket_id", 256)
        _bounded(authorization_operation_id, "authorization_operation_id", 256)
        if resulting_state not in {"SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell result state", 422)
        _bounded(error_code, "error_code", 128, optional=True)
        if (resulting_state == "SUCCEEDED") != (error_code is None):
            raise ForestError("SHELL_INPUT_INVALID", "Shell result error identity is incomplete", 422)
        result.validate("result")
        input_digest = _digest(
            {
                "request_id": claim.request_id,
                "owner_id": claim.worker_id,
                "owner_epoch": claim.owner_epoch,
                "resulting_state": resulting_state,
                "result": result.canonical(),
                "error_code": error_code,
                "authorization_ticket_id": authorization_ticket_id,
                "authorization_operation_id": authorization_operation_id,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "result-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("request_results")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("SHELL_RESULT_CONFLICT", "Shell result input differs", 409)
                return dict(replay)
            request = await self._guard_claim(conn, claim)
            if request["state"] not in {"CLAIMED", "EXECUTING", "WAITING_APPROVAL"}:
                raise ForestError("SHELL_STATE_CONFLICT", "Shell request cannot complete", 409)
            await self.authorize_claim(
                claim,
                ticket_id=authorization_ticket_id,
                authorization_operation_id=authorization_operation_id,
                _connection=conn,
            )
            await self._verify_payload(conn, result)
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,request_id,resulting_state,"
                        "result_payload_id,result_payload_digest,result_owner_component_id,"
                        "result_owner_kind,result_owner_id,error_code,owner_id,owner_epoch) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("request_results")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        claim.request_id,
                        resulting_state,
                        result.payload_id,
                        result.digest,
                        result.owner_component_id,
                        result.owner_kind,
                        result.owner_id,
                        error_code,
                        claim.worker_id,
                        claim.owner_epoch,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL("UPDATE {} SET state=%s,owner_id=NULL,owner_expires_at=NULL,"
                        "completed_at=%s,updated_at=%s WHERE bot_id=%s AND request_id=%s").format(
                    self._table("shell_requests")
                ),
                (
                    resulting_state,
                    now["now"],
                    now["now"],
                    self.store.bot_id,
                    claim.request_id,
                ),
            )
            return dict(row)

    async def complete_in(self, conn, claim: ShellClaim, **values) -> dict:
        return await self.complete(claim, _connection=conn, **values)

    async def fail_claim(
        self,
        claim: ShellClaim,
        *,
        operation_id: str,
        resulting_state: str,
        result: ShellPayloadRef,
        error_code: str,
        _connection=None,
    ) -> dict:
        """Record a safe terminal failure without granting a management effect."""

        _bounded(operation_id, "operation_id", 256)
        if resulting_state not in {"FAILED", "UNKNOWN"}:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell failure state", 422)
        _bounded(error_code, "error_code", 128)
        result.validate("result")
        input_digest = _digest(
            {
                "request_id": claim.request_id,
                "owner_id": claim.worker_id,
                "owner_epoch": claim.owner_epoch,
                "resulting_state": resulting_state,
                "result": result.canonical(),
                "error_code": error_code,
                "terminal_without_authorization": True,
            }
        )
        async with self._transaction(_connection) as conn:
            await self._lock(conn, "result-operation:" + operation_id)
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("request_results")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("SHELL_RESULT_CONFLICT", "Shell result input differs", 409)
                return dict(replay)
            request = await self._guard_claim(conn, claim)
            if request["state"] not in {"CLAIMED", "EXECUTING", "WAITING_APPROVAL"}:
                raise ForestError("SHELL_STATE_CONFLICT", "Shell request cannot fail", 409)
            await self._verify_payload(conn, result)
            now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,request_id,resulting_state,"
                        "result_payload_id,result_payload_digest,result_owner_component_id,"
                        "result_owner_kind,result_owner_id,error_code,owner_id,owner_epoch) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("request_results")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        claim.request_id,
                        resulting_state,
                        result.payload_id,
                        result.digest,
                        result.owner_component_id,
                        result.owner_kind,
                        result.owner_id,
                        error_code,
                        claim.worker_id,
                        claim.owner_epoch,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,owner_id=NULL,owner_expires_at=NULL,"
                    "completed_at=%s,updated_at=%s WHERE bot_id=%s AND request_id=%s"
                ).format(self._table("shell_requests")),
                (
                    resulting_state,
                    now["now"],
                    now["now"],
                    self.store.bot_id,
                    claim.request_id,
                ),
            )
            return dict(row)

    async def fail_claim_in(self, conn, claim: ShellClaim, **values) -> dict:
        return await self.fail_claim(claim, _connection=conn, **values)

    async def load_request(self, request_id: str) -> dict:
        _bounded(request_id, "request_id", 256)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s").format(
                        self._table("shell_requests")
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("SHELL_REQUEST_NOT_FOUND", "Shell request does not exist", 404)
        return dict(row)

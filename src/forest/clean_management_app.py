"""Compact management gateway for one active clean Bot host."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from pydantic import BaseModel, ConfigDict

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.errors import ForestError
from forest.package_management import PackageManagementContract
from forest.packages import digest_json
from forest.storage.kernel_audit import AuditEnvelope
from forest.storage.management_asset_authority import (
    CHUNK_BYTES,
    ManagementAssetAuthority,
    management_asset_owner_id,
)
from forest.web_operation_policy import authorize_web_operation


class ConfigurationPlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_desired_revision: int
    value: Any


class ConfigurationApplyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_id: str
    expected_desired_revision: int


class CleanManagementGateway:
    def __init__(
        self, host, *, ticket_authority, scope_id: str | None = None
    ) -> None:
        if ticket_authority is None:
            raise ValueError("Management Web authority ticket is required")
        self.host = host
        self.ticket_authority = ticket_authority
        self.scope_id = scope_id or host.settings.bot_id
        self._apply_lock = asyncio.Lock()

    def contract(self, component_id: str) -> CleanModuleConfigurationContract:
        registry = self.registry()
        try:
            module = registry.get(component_id)
        except KeyError:
            raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Runtime module does not exist", 404) from None
        contract = getattr(module, "management_contract", None)
        if not isinstance(contract, CleanModuleConfigurationContract):
            raise ForestError(
                "MODULE_MANAGEMENT_UNAVAILABLE",
                "Runtime module does not publish a configuration contract",
                404,
            )
        return contract

    def registry(self):
        registry = self.host.registry
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot runtime is unavailable", 503)
        return registry

    def operation_id(self, component_id: str, kind: str, key: str | None) -> str:
        if key is None or not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422
            )
        return "web:" + digest_json(
            {
                "protocol": "forest.clean-management-operation/1",
                "bot_id": self.host.settings.bot_id,
                "component_id": component_id,
                "kind": kind,
                "key": key,
            }
        )

    def authorization_target(self, component_id: str) -> tuple[str, int]:
        registry = self.registry()
        graph = registry.graph_identity
        if graph is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Runtime graph is unavailable", 503)
        if component_id == "$runtime":
            return graph.graph_id, graph.graph_revision
        self.contract(component_id)
        try:
            generation_id = graph.generations[component_id]
        except KeyError:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_FOUND", "Runtime module Generation is unavailable", 503
            ) from None
        return generation_id, graph.graph_revision

    def action_plan_digest(self, component_id: str, action_id: str, body: dict) -> str | None:
        resolver = getattr(self.contract(component_id), "authorization_plan_digest", None)
        return resolver(action_id, body) if callable(resolver) else None

    def asset_declaration(self, component_id: str, asset_id: str) -> dict:
        resolver = getattr(self.contract(component_id), "asset", None)
        if not callable(resolver):
            raise ForestError(
                "MODULE_MANAGEMENT_ASSET_NOT_FOUND", "Module asset does not exist", 404
            )
        return resolver(asset_id)

    def asset_operation_id(self, component_id: str, asset_id: str, stage: str, key: str) -> str:
        return self.operation_id(
            component_id, "asset." + asset_id + "." + stage,
            hashlib.sha256(key.encode("utf-8")).hexdigest(),
        )

    def _asset_authority(self):
        return ManagementAssetAuthority(self.host._kernel.payload)

    async def _asset_write(
        self, *, component_id: str, asset_id: str, stage: str, body: dict,
        authorization, operation_id: str, mutate,
    ):
        action_key = "asset." + asset_id + "." + stage
        input_digest = digest_json(body)
        await self._authorize(
            authorization, component_id=component_id, operation_id=operation_id,
            action_key=action_key, input_digest=input_digest, required_role="ADMIN",
        )
        payloads = self.host._kernel.payload
        async with payloads.store.pool.connection() as conn, conn.transaction():
            await self._authorize_mutation_in(
                conn, ticket=authorization, component_id=component_id,
                operation_id=operation_id, action_key=action_key,
                input_digest=input_digest, plan_digest=None,
            )
            return await mutate(conn)

    async def asset_begin(
        self, component_id: str, asset_id: str, body: dict, *,
        authorization=None, operation_id=None,
    ) -> dict:
        declaration = self.asset_declaration(component_id, asset_id)
        authority = self._asset_authority()
        if not isinstance(body, dict) or set(body) != {
            "upload_id", "target_ref", "part_id", "media_type", "byte_length",
            "digest", "chunk_count",
        }:
            raise ForestError("MANAGEMENT_ASSET_INVALID", "Upload input is invalid", 422)
        upload_id = body["upload_id"]
        if not isinstance(upload_id, str) or not 1 <= len(upload_id) <= 128:
            raise ForestError("MANAGEMENT_ASSET_INVALID", "Upload ID is invalid", 422)
        operation_id = operation_id or self.asset_operation_id(
            component_id, asset_id, "begin", upload_id
        )
        async def mutate(conn):
            return await authority.begin_in(
                conn, upload_id=upload_id, component_id=component_id,
                asset_id=asset_id, actor_subject_ref=authorization.web_subject_ref,
                generation_id=authorization.target_generation_id,
                target_ref=body["target_ref"], part_id=body["part_id"],
                owner_kind=declaration["owner_kind"],
                owner_id=management_asset_owner_id(
                    declaration["owner_prefix"], body["target_ref"], body["part_id"]
                ),
                media_type=body["media_type"], byte_length=body["byte_length"],
                content_digest=body["digest"], chunk_count=body["chunk_count"],
                allowed_media_types=tuple(declaration["media_types"]),
                max_bytes=declaration["max_bytes"],
            )
        return await self._asset_write(
            component_id=component_id, asset_id=asset_id, stage="begin",
            body=body, authorization=authorization, operation_id=operation_id,
            mutate=mutate,
        )

    async def asset_chunk(
        self, component_id: str, asset_id: str, upload_id: str,
        chunk_index: int, content: bytes, *, authorization=None, operation_id=None,
    ) -> dict:
        self.asset_declaration(component_id, asset_id)
        authority = self._asset_authority()
        if type(content) is not bytes or not 1 <= len(content) <= CHUNK_BYTES:
            raise ForestError("MANAGEMENT_ASSET_CHUNK_INVALID", "Chunk exceeds 256 KiB", 413)
        body = {
            "upload_id": upload_id, "chunk_index": chunk_index,
            "byte_length": len(content),
            "digest": "sha256:" + hashlib.sha256(content).hexdigest(),
        }
        operation_id = operation_id or self.asset_operation_id(
            component_id, asset_id, "chunk", f"{upload_id}:{chunk_index}"
        )
        async def mutate(conn):
            return await authority.put_chunk_in(
                conn, upload_id=upload_id, component_id=component_id,
                asset_id=asset_id, actor_subject_ref=authorization.web_subject_ref,
                generation_id=authorization.target_generation_id,
                chunk_index=chunk_index, content=content,
                content_digest=body["digest"],
            )
        return await self._asset_write(
            component_id=component_id, asset_id=asset_id, stage="chunk",
            body=body, authorization=authorization, operation_id=operation_id,
            mutate=mutate,
        )

    async def asset_finalize(
        self, component_id: str, asset_id: str, upload_id: str, *,
        authorization=None, operation_id=None,
    ) -> dict:
        self.asset_declaration(component_id, asset_id)
        authority = self._asset_authority()
        body = {"upload_id": upload_id}
        operation_id = operation_id or self.asset_operation_id(
            component_id, asset_id, "finalize", upload_id
        )
        async def mutate(conn):
            return await authority.finalize_in(
                conn, upload_id=upload_id, component_id=component_id,
                asset_id=asset_id, actor_subject_ref=authorization.web_subject_ref,
                generation_id=authorization.target_generation_id,
            )
        return await self._asset_write(
            component_id=component_id, asset_id=asset_id, stage="finalize",
            body=body, authorization=authorization, operation_id=operation_id,
            mutate=mutate,
        )

    async def _authorize(
        self,
        ticket,
        *,
        component_id: str,
        operation_id: str | None,
        action_key: str,
        input_digest: str,
        required_role: str,
        plan_digest: str | None = None,
    ) -> None:
        if ticket is None or operation_id is None:
            raise ForestError(
                "WEB_AUTHORITY_TICKET_REQUIRED", "Web authority ticket is required", 403
            )
        generation_id, graph_revision = self.authorization_target(component_id)
        await self.ticket_authority.verify(
            ticket,
            target_scope_kind="bot",
            target_scope_id=self.scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=action_key,
            input_digest=input_digest,
            plan_digest=plan_digest,
            required_role=required_role,
        )
        await self._authorize_bot_policy(
            ticket,
            component_id=component_id,
            action_key=action_key,
        )

    async def _authorize_bot_policy(
        self,
        ticket,
        *,
        component_id: str,
        action_key: str,
        conn=None,
    ) -> int:
        try:
            configuration = self.host._kernel.configurations["kernel.authorization"]
        except (AttributeError, KeyError):
            raise ForestError(
                "WEB_OPERATION_POLICY_UNAVAILABLE",
                "Bot Web operation policy is unavailable",
                503,
            ) from None
        effective = (
            await configuration.effective(include_value=True)
            if conn is None
            else await configuration.effective_in(conn, include_value=True)
        )
        authorize_web_operation(
            effective.value,
            web_subject_ref=ticket.web_subject_ref,
            web_role=ticket.web_role,
            target_component_id=component_id,
            action_key=action_key,
        )
        return effective.revision

    async def _authorize_mutation_in(
        self,
        conn,
        *,
        ticket,
        component_id: str,
        operation_id: str,
        action_key: str,
        input_digest: str,
        plan_digest: str | None,
    ) -> dict:
        await self._verify_management_target_in(
            conn,
            ticket=ticket,
            component_id=component_id,
            operation_id=operation_id,
            action_key=action_key,
            input_digest=input_digest,
            plan_digest=plan_digest,
        )
        receipt = await self.host._kernel.authorization.accept_web_authority_ticket_in(
            conn, ticket=ticket
        )
        audit = getattr(self.host._kernel, "audit", None)
        if audit is None:
            raise ForestError(
                "WEB_AUTHORITY_TRANSACTION_REQUIRED",
                "Bot Web audit authority is unavailable",
                503,
            )
        audit_id = "audit:web:" + hashlib.sha256(
            (self.host.settings.bot_id + "\0" + operation_id).encode()
        ).hexdigest()
        await audit.append_in(
            conn,
            AuditEnvelope(
                audit_id=audit_id,
                action=action_key,
                severity="SECURITY",
                producer_component_id=component_id,
                producer_generation_id=ticket.target_generation_id,
                origin_kind="web.management",
                actor_kind="web.subject",
                actor_id=ticket.web_subject_ref,
                resource_kind="bot.component",
                resource_id=component_id,
                operation_kind="web.action",
                operation_id=operation_id,
                call_id=receipt["ticket_id"],
            ),
        )
        return receipt

    async def _verify_management_target_in(
        self,
        conn,
        *,
        ticket,
        component_id: str,
        operation_id: str,
        action_key: str,
        input_digest: str,
        plan_digest: str | None,
    ) -> None:
        await self.ticket_authority.verify(
            ticket,
            target_scope_kind="bot",
            target_scope_id=self.scope_id,
            target_component_id=component_id,
            target_generation_id=ticket.target_generation_id,
            target_graph_revision=ticket.target_graph_revision,
            target_operation_ref=operation_id,
            action_key=action_key,
            input_digest=input_digest,
            plan_digest=plan_digest,
            required_role="ADMIN",
        )
        route = await self.host._kernel.runtime.verify_active_generation_in(
            conn,
            component_id=component_id,
            generation_id=ticket.target_generation_id,
            admission_lane="CONTROL",
        )
        if route.graph_revision != ticket.target_graph_revision:
            raise ForestError(
                "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                "Web authority ticket names another RuntimeGraph revision",
                403,
            )
        await self._authorize_bot_policy(
            ticket,
            component_id=component_id,
            action_key=action_key,
            conn=conn,
        )

    async def runtime(self, *, authorization=None, operation_id=None) -> dict:
        await self._authorize(
            authorization,
            component_id="$runtime",
            operation_id=operation_id,
            action_key="runtime.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        registry = self.registry()
        return {
            "protocol": "forest.clean-management-runtime/1",
            "runtime": await self.host.generation_authority.status(),
            "modules": registry.describe(),
        }

    async def read_configuration(
        self, component_id: str, *, authorization=None, operation_id=None
    ) -> dict:
        await self._authorize(
            authorization,
            component_id=component_id,
            operation_id=operation_id,
            action_key="configuration.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        return await self.contract(component_id).read()

    async def read_management_ui(
        self,
        component_id: str,
        artifact_sha256: str,
        *,
        authorization=None,
        operation_id=None,
    ) -> bytes:
        await self._authorize(
            authorization,
            component_id=component_id,
            operation_id=operation_id,
            action_key="configuration.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        try:
            module = self.registry().get(component_id)
        except KeyError:
            raise ForestError(
                "RUNTIME_MODULE_NOT_FOUND", "Runtime module does not exist", 404
            ) from None
        read_artifact = getattr(module, "management_ui_artifact", None)
        if not callable(read_artifact):
            raise ForestError(
                "PACKAGE_MANAGEMENT_UI_NOT_FOUND",
                "Runtime module does not publish a package management UI",
                404,
            )
        _metadata, content = read_artifact(artifact_sha256)
        return content

    async def plan_configuration(
        self,
        component_id: str,
        *,
        body: ConfigurationPlanInput,
        idempotency_key: str | None,
        authorization=None,
        operation_id=None,
    ) -> dict:
        operation_id = operation_id or self.operation_id(
            component_id, "configuration.plan", idempotency_key
        )
        await self._authorize(
            authorization,
            component_id=component_id,
            operation_id=operation_id,
            action_key="configuration.plan",
            input_digest=digest_json(body.model_dump(mode="json")),
            required_role="ADMIN",
        )
        return await self.contract(component_id).plan(
            operation_id=operation_id,
            expected_desired_revision=body.expected_desired_revision,
            value=body.value,
            authorization=authorization,
            authorization_guard=self._authorize_mutation_in,
        )

    async def apply_configuration(
        self,
        component_id: str,
        *,
        body: ConfigurationApplyInput,
        idempotency_key: str | None,
        authorization=None,
        operation_id=None,
    ) -> dict:
        operation_id = operation_id or self.operation_id(
            component_id, "configuration.apply", idempotency_key
        )
        await self._authorize(
            authorization,
            component_id=component_id,
            operation_id=operation_id,
            action_key="configuration.apply",
            input_digest=digest_json(body.model_dump(mode="json")),
            plan_digest=digest_json({"plan_id": body.plan_id}),
            required_role="ADMIN",
        )
        contract = self.contract(component_id)
        if isinstance(contract, PackageManagementContract):
            return await contract.apply(
                operation_id=operation_id,
                plan_id=body.plan_id,
                expected_desired_revision=body.expected_desired_revision,
                authorization=authorization,
                authorization_guard=self._authorize_mutation_in,
                authorization_replay_guard=self._verify_management_target_in,
            )
        async with self._apply_lock:
            saved = await contract.apply(
                operation_id=operation_id,
                plan_id=body.plan_id,
                expected_desired_revision=body.expected_desired_revision,
                authorization=authorization,
                authorization_guard=self._authorize_mutation_in,
            )
            effective = await contract.configuration.effective()
            replaced = effective.revision != saved["revision"]
            registry = (
                await self.host.replace_runtime_graph() if replaced else self.registry()
            )
            graph = registry.graph_identity
            return {
                "configuration": saved,
                "replaced": replaced,
                "graph_id": graph.graph_id,
                "graph_revision": graph.graph_revision,
            }

    async def module_query(
        self,
        component_id: str,
        query_id: str,
        body: dict,
        *,
        authorization=None,
        operation_id=None,
    ):
        contract = self.contract(component_id)
        query = getattr(contract, "query", None)
        if not callable(query):
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Module query does not exist", 404
            )
        await self._authorize(
            authorization,
            component_id=component_id,
            operation_id=operation_id,
            action_key=query_id,
            input_digest=digest_json(body),
            required_role=contract.query_required_role(query_id),
        )
        return await contract.query_authorized(
            query_id, body, authorization=authorization
        )

    async def module_action(
        self,
        component_id: str,
        action_id: str,
        *,
        body: dict,
        idempotency_key: str | None,
        authorization=None,
        operation_id=None,
    ) -> dict:
        operation_id = operation_id or self.operation_id(component_id, action_id, idempotency_key)
        plan_digest = self.action_plan_digest(component_id, action_id, body)
        await self._authorize(
            authorization,
            component_id=component_id,
            operation_id=operation_id,
            action_key=action_id,
            input_digest=digest_json(body),
            plan_digest=plan_digest,
            required_role="ADMIN",
        )
        async with self._apply_lock:
            contract = self.contract(component_id)
            action = getattr(contract, "action", None)
            if not callable(action):
                raise ForestError(
                    "MODULE_MANAGEMENT_OPERATION_NOT_FOUND",
                    "Module action does not exist",
                    404,
                )
            result = await action(
                action_id,
                operation_id=operation_id,
                payload=body,
                authorization=authorization,
                authorization_guard=self._authorize_mutation_in,
            )
            if not isinstance(result, dict):
                raise ForestError(
                    "MODULE_MANAGEMENT_CONTRACT_INVALID",
                    "Module action returned an invalid result",
                    500,
                )
            graph = self.registry().graph_identity
            return {
                **result,
                "graph_id": graph.graph_id,
                "graph_revision": graph.graph_revision,
            }

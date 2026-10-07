"""Registered management scope providers for the deployment Web gateway."""

from __future__ import annotations

import hashlib
import inspect
import re
import secrets
from contextlib import asynccontextmanager
from typing import Callable

from forest.control_action_authorization import ControlActionAuthorization
from forest.errors import ForestError
from forest.management_gateway import ManagementGateway
from forest.packages import digest_json

_SCOPE_KIND = re.compile(r"[a-z][a-z0-9.-]{0,63}\Z")
_SCOPE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")


class ControlManagementGateway:
    def __init__(
        self,
        *,
        scope_kind: str,
        control_id: str,
        ticket_authority=None,
        web_receipts=None,
        audit=None,
        runtime_registry_provider: Callable | None = None,
        graph_change_authority=None,
        graph_change_previewer: Callable | None = None,
    ) -> None:
        if scope_kind not in {"deployment", "host"}:
            raise ValueError("Control management scope is invalid")
        self.scope_kind = scope_kind
        self.control_id = control_id
        self.authorization_scope_id = (
            "deployment" if scope_kind == "deployment" else control_id
        )
        self.ticket_authority = ticket_authority
        self.web_receipts = web_receipts
        self.audit = audit
        self._runtime_registry_provider = runtime_registry_provider
        self.graph_change_authority = graph_change_authority
        self.graph_change_previewer = graph_change_previewer

    @property
    def runtime_registry(self):
        return (
            self._runtime_registry_provider()
            if self._runtime_registry_provider is not None
            else None
        )

    def _package_binding(self, component_id: str):
        registry = self.runtime_registry
        if registry is None or component_id not in registry.states:
            return None
        graph = registry.graph_identity
        if graph is None or registry.states[component_id] != "ACTIVE":
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Control module is unavailable", 409
            )
        return graph.generations[component_id], graph.graph_revision, registry.get(component_id)

    def _package_module(self, component_id: str):
        binding = self._package_binding(component_id)
        return binding[2] if binding is not None else None

    def authorization_target(self, component_id: str) -> tuple[str, int]:
        registry = self.runtime_registry
        graph = registry.graph_identity if registry is not None else None
        if component_id == "$runtime":
            return (
                (graph.graph_id, graph.graph_revision)
                if graph is not None
                else (f"{self.scope_kind}/control-plane/1", 1)
            )
        if registry is not None and component_id in registry.states:
            if graph is None or registry.states[component_id] != "ACTIVE":
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_ACTIVE", "Control module is unavailable", 409
                )
            return graph.generations[component_id], graph.graph_revision
        raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Component does not exist", 404)

    async def _authorize(self, ticket, **expected) -> None:
        if self.ticket_authority is None:
            return
        if ticket is None:
            raise ForestError(
                "WEB_AUTHORITY_TICKET_REQUIRED", "Web authority ticket is required", 403
            )
        await self.ticket_authority.verify(ticket, **expected)
        component_id = expected["target_component_id"]
        if self.authorization_target(component_id) != (
            expected["target_generation_id"],
            expected["target_graph_revision"],
        ):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                "Control RuntimeGraph changed during authorization",
                403,
            )

    async def runtime(self, *, authorization=None, operation_id=None) -> dict:
        generation_id, graph_revision = self.authorization_target("$runtime")
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id="$runtime",
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="runtime.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        registry = self.runtime_registry
        graph = registry.graph_identity if registry is not None else None
        return {
            "protocol": f"forest.{self.scope_kind}-management-runtime/1",
            "runtime": (
                {
                    "scope_kind": self.scope_kind,
                    "scope_id": self.control_id,
                    "graph_state": "ACTIVE",
                    "graph_id": graph.graph_id,
                    "graph_revision": graph.graph_revision,
                }
                if graph is not None
                else {
                    "scope_kind": self.scope_kind,
                    "scope_id": self.control_id,
                    "graph_state": "CONTROL_PLANE",
                    "graph_id": generation_id,
                }
            ),
            "modules": registry.describe() if registry is not None else [],
        }

    async def read_configuration(self, component_id: str, *, authorization=None, operation_id=None):
        generation_id, graph_revision, module = self._require_package_binding(component_id)
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        return await module.management_contract.read()

    async def read_management_ui(
        self,
        component_id: str,
        artifact_sha256: str,
        *,
        authorization=None,
        operation_id=None,
    ) -> bytes:
        binding = self._package_binding(component_id)
        if binding is None:
            raise ForestError("PACKAGE_MANAGEMENT_UI_NOT_FOUND", "Package UI is unavailable", 404)
        generation_id, graph_revision, module = binding
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        _metadata, content = module.management_ui_artifact(artifact_sha256)
        return content

    async def artifact_download(
        self, component_id: str, artifact_id: str, body: dict,
        *, authorization=None, operation_id=None,
    ):
        binding = self._package_binding(component_id)
        if binding is None:
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND",
                "Package artifact operation is unavailable", 404,
            )
        generation_id, graph_revision, module = binding
        contract = module.management_contract
        role = contract.artifact_required_role(artifact_id)
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind, target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=f"artifact.{artifact_id}.download",
            input_digest=digest_json(body), required_role=role,
        )
        return await contract.download_artifact(artifact_id, body)

    @asynccontextmanager
    async def artifact_upload(
        self, component_id: str, artifact_id: str, payload: dict,
        *, content_length: int, content_sha256: str,
        authorization=None, operation_id=None,
    ):
        binding = self._package_binding(component_id)
        if binding is None:
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND",
                "Package artifact operation is unavailable", 404,
            )
        generation_id, graph_revision, module = binding
        contract = module.management_contract
        role = contract.artifact_required_role(artifact_id)
        body = {
            "payload": payload, "content_length": content_length,
            "content_sha256": content_sha256,
        }
        async with contract.open_artifact_upload(artifact_id, payload) as (max_bytes, publish):
            if not 1 <= content_length <= max_bytes:
                raise ForestError(
                    "PACKAGE_TOO_LARGE", "Artifact upload exceeds its declared limit", 413,
                )
            await self._authorize(
                authorization,
                target_scope_kind=self.scope_kind, target_scope_id=self.authorization_scope_id,
                target_component_id=component_id,
                target_generation_id=generation_id,
                target_graph_revision=graph_revision,
                target_operation_ref=operation_id,
                action_key=f"artifact.{artifact_id}.upload",
                input_digest=digest_json(body), required_role=role,
            )
            if self.web_receipts is None or (
                self.scope_kind == "deployment" and self.audit is None
            ):
                raise ForestError(
                    "WEB_AUTHORITY_TRANSACTION_REQUIRED",
                    "Artifact upload requires control-scope Web receipt storage", 503,
                )
            async with self.web_receipts.store.pool.connection() as conn, conn.transaction():
                await self._authorize_mutation_in(
                    conn, ticket=authorization, component_id=component_id,
                    operation_id=operation_id, action_key=f"artifact.{artifact_id}.upload",
                    input_digest=digest_json(body), plan_digest=None,
                )
                reservation = await contract.artifact_command_in(
                    conn, artifact_id, "reserve", {
                        "operation_id": operation_id,
                        "actor_ref": authorization.web_subject_ref,
                        "generation_id": generation_id,
                        "content_length": content_length,
                        "archive_sha256": content_sha256,
                    }, operation_id=operation_id,
                )
            if reservation is not None and reservation["status"] in {
                "REJECTED", "INTERRUPTED",
            }:
                raise ForestError(
                    "PACKAGE_IMPORT_ALREADY_TERMINAL",
                    "This import operation has a terminal result", 409,
                )

            publish_started = False

            async def commit(content: bytes):
                nonlocal publish_started
                publish_started = True
                result = await publish(
                    content, operation_id=operation_id,
                    actor_ref=authorization.web_subject_ref,
                    content_sha256=content_sha256,
                )
                async with self.web_receipts.store.pool.connection() as conn, conn.transaction():
                    await self._authorize_mutation_in(
                        conn, ticket=authorization, component_id=component_id,
                        operation_id=operation_id, action_key=f"artifact.{artifact_id}.upload",
                        input_digest=digest_json(body), plan_digest=None,
                    )
                    await contract.artifact_command_in(
                        conn, artifact_id, "settle", {
                            "operation_id": operation_id,
                            "actor_ref": authorization.web_subject_ref,
                            "archive_sha256": content_sha256,
                            "status": "SUCCEEDED",
                            "package_digest": result["package_digest"],
                        }, operation_id=operation_id,
                    )
                return result

            try:
                yield max_bytes, commit
            except ForestError as error:
                if reservation is not None and reservation["status"] == "RESERVED" and (
                    not publish_started and error.code in {
                    "PACKAGE_TOO_LARGE", "PACKAGE_ARCHIVE_DIGEST_MISMATCH",
                    }
                ):
                    receipt = await contract.artifact_close(
                        artifact_id, {
                            "operation_id": operation_id,
                            "actor_ref": authorization.web_subject_ref,
                            "archive_sha256": content_sha256,
                        },
                    )
                    if receipt is None:
                        raise ForestError(
                            "PACKAGE_ARTIFACT_CLOSE_UNAVAILABLE",
                            "Artifact rejection requires a durable physical close fence", 503,
                        )
                    settlement = {
                        "operation_id": operation_id,
                        "actor_ref": authorization.web_subject_ref,
                        "archive_sha256": content_sha256,
                        "status": "SUCCEEDED" if receipt["found"] else "REJECTED",
                    }
                    if receipt["found"]:
                        settlement["package_digest"] = receipt["package_digest"]
                    else:
                        settlement["error_code"] = error.code
                    async with self.web_receipts.store.pool.connection() as conn, conn.transaction():
                        await self._authorize_mutation_in(
                            conn, ticket=authorization, component_id=component_id,
                            operation_id=operation_id,
                            action_key=f"artifact.{artifact_id}.upload",
                            input_digest=digest_json(body), plan_digest=None,
                        )
                        await contract.artifact_command_in(
                            conn, artifact_id, "settle", settlement,
                            operation_id=operation_id,
                        )
                raise

    async def plan_configuration(
        self, component_id: str, *, authorization=None, operation_id=None, **_values
    ):
        body = _values["body"]
        generation_id, graph_revision, module = self._require_package_binding(component_id)
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.plan",
            input_digest=digest_json(body.model_dump(mode="json")),
            required_role="ADMIN",
        )
        return await module.management_contract.plan(
            operation_id=operation_id,
            expected_desired_revision=body.expected_desired_revision,
            value=body.value,
            authorization=authorization,
            authorization_guard=self._authorize_mutation_in,
        )

    async def apply_configuration(
        self, component_id: str, *, authorization=None, operation_id=None, **_values
    ):
        body = _values["body"]
        generation_id, graph_revision, module = self._require_package_binding(component_id)
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.apply",
            input_digest=digest_json(body.model_dump(mode="json")),
            plan_digest=digest_json({"plan_id": body.plan_id}),
            required_role="ADMIN",
        )
        return await module.management_contract.apply(
            operation_id=operation_id,
            plan_id=body.plan_id,
            expected_desired_revision=body.expected_desired_revision,
            authorization=authorization,
            authorization_guard=self._authorize_mutation_in,
        )

    async def module_query(
        self, component_id: str, query_id: str, body: dict, *, authorization=None, operation_id=None
    ):
        if component_id == "$runtime":
            if query_id == "module.change.status":
                valid = isinstance(body, dict) and set(body) == {"operation_id"}
            elif query_id == "module.change.preview":
                valid = isinstance(body, dict) and set(body) == {
                    "operation_kind", "component_id", "package_digest",
                    "expected_graph_revision",
                }
            else:
                valid = False
            if not valid:
                raise ForestError(
                    "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Query does not exist", 404
                )
            generation_id, graph_revision = self.authorization_target("$runtime")
            await self._authorize(
                authorization,
                target_scope_kind=self.scope_kind,
                target_scope_id=self.authorization_scope_id,
                target_component_id="$runtime",
                target_generation_id=generation_id,
                target_graph_revision=graph_revision,
                target_operation_ref=operation_id,
                action_key=query_id,
                input_digest=digest_json(body),
                required_role="ADMIN" if query_id == "module.change.preview" else "AUDITOR",
            )
            if query_id == "module.change.preview":
                if self.graph_change_previewer is None:
                    raise ForestError(
                        f"{self.scope_kind.upper()}_RUNTIME_UNAVAILABLE",
                        "Graph preview is unavailable", 503,
                    )
                return await self.graph_change_previewer(**body)
            if self.graph_change_authority is None:
                raise ForestError(
                    f"{self.scope_kind.upper()}_RUNTIME_UNAVAILABLE",
                    "Replacement authority is unavailable", 503,
                )
            return await self.graph_change_authority.get(body["operation_id"])
        generation_id, graph_revision, module = self._require_package_binding(component_id)
        contract = module.management_contract
        required_role_for_query = getattr(contract, "query_required_role", None)
        required_role = (
            required_role_for_query(query_id)
            if callable(required_role_for_query) else "AUDITOR"
        )
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=query_id,
            input_digest=digest_json(body),
            required_role=required_role,
        )
        return await contract.query_authorized(query_id, body, authorization=authorization)

    async def module_action(
        self,
        component_id: str,
        action_id: str,
        *,
        body: dict,
        idempotency_key: str | None,
        authorization=None,
        operation_id=None,
    ):
        operation_id = operation_id or self.operation_id(component_id, action_id, idempotency_key)
        if component_id == "$runtime":
            if action_id not in {"module.add", "module.replace"}:
                raise ForestError(
                    "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Action does not exist", 404
                )
            if self.graph_change_authority is None:
                raise ForestError(
                    f"{self.scope_kind.upper()}_RUNTIME_UNAVAILABLE",
                    "Replacement authority is unavailable", 503,
                )
            registry = self.runtime_registry
            if registry is None:
                raise ForestError(
                    f"{self.scope_kind.upper()}_RUNTIME_UNAVAILABLE",
                    "Control Registry is unavailable", 503,
                )
            generation_id, graph_revision = self.authorization_target("$runtime")
            if not isinstance(body, dict) or set(body) != {
                "component_id", "package_digest", "expected_graph_revision", "plan_digest"
            }:
                raise ForestError(
                    f"{self.scope_kind.upper()}_GRAPH_CHANGE_INPUT_INVALID",
                    "Graph change fields differ", 422,
                )
            input_digest = digest_json(body)
            await self._authorize(
                authorization,
                target_scope_kind=self.scope_kind,
                target_scope_id=self.authorization_scope_id,
                target_component_id="$runtime",
                target_generation_id=generation_id,
                target_graph_revision=graph_revision,
                target_operation_ref=operation_id,
                action_key=action_id,
                input_digest=input_digest,
                plan_digest=body["plan_digest"],
                required_role="ADMIN",
            )
            if await self.graph_change_authority.find(operation_id) is None:
                if self.graph_change_previewer is None:
                    raise ForestError(
                        f"{self.scope_kind.upper()}_RUNTIME_UNAVAILABLE",
                        "Graph preview is unavailable", 503,
                    )
                try:
                    preview = await self.graph_change_previewer(
                        operation_kind="ADD" if action_id == "module.add" else "REPLACE",
                        component_id=body["component_id"],
                        package_digest=body["package_digest"],
                        expected_graph_revision=body["expected_graph_revision"],
                    )
                except ForestError:
                    if await self.graph_change_authority.find(operation_id) is None:
                        raise
                else:
                    if body["plan_digest"] != preview["plan_digest"]:
                        raise ForestError(
                            f"{self.scope_kind.upper()}_GRAPH_CHANGE_PLAN_CONFLICT",
                            "Submitted Graph plan differs from the current candidate", 409,
                        )
            guard = ControlActionAuthorization(
                authorization,
                self._authorize_mutation_in,
                "$runtime",
                action_id,
                input_digest,
                operation_id,
                body["plan_digest"],
            )
            return await self.graph_change_authority.enqueue_web(
                lease=registry.runtime.authority_lease(),
                ticket=authorization,
                body=body,
                authorize_in=guard.accept_in,
            )
        generation_id, graph_revision = self.authorization_target(component_id)
        input_digest = digest_json(body)
        await self._authorize(
            authorization,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=action_id,
            input_digest=input_digest,
            required_role="ADMIN",
        )
        package = self._require_package_binding(component_id)[2]
        return await package.management_contract.action(
            action_id,
            operation_id=operation_id,
            payload=body,
            authorization=authorization,
            authorization_guard=self._authorize_mutation_in,
        )

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
        if self.ticket_authority is None or self.web_receipts is None or (
            self.scope_kind == "deployment" and self.audit is None
        ):
            raise ForestError(
                "WEB_AUTHORITY_TRANSACTION_REQUIRED",
                "Control Web authority receipt storage is unavailable",
                503,
            )
        generation_id, graph_revision = self.authorization_target(component_id)
        await self.ticket_authority.verify(
            ticket,
            target_scope_kind=self.scope_kind,
            target_scope_id=self.authorization_scope_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=action_key,
            input_digest=input_digest,
            plan_digest=plan_digest,
            required_role="ADMIN",
        )
        receipt = await self.web_receipts.accept_in(conn, ticket=ticket)
        if self.audit is not None:
            await self.audit.append_web_action_in(conn, ticket=ticket, receipt=receipt)
        return receipt

    def _require_package_binding(self, component_id: str):
        binding = self._package_binding(component_id)
        if binding is None:
            raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Component does not exist", 404)
        return binding

    def action_plan_digest(self, component_id: str, action_id: str, body: dict) -> str | None:
        if (
            component_id == "$runtime"
            and action_id in {"module.add", "module.replace"}
            and isinstance(body, dict)
        ):
            return body.get("plan_digest")
        return None

    def operation_id(self, component_id: str, action_id: str, key: str | None) -> str:
        if key is None or not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422
            )
        return "web:" + digest_json(
            [
                f"forest.{self.scope_kind}-management-operation/1",
                self.control_id,
                component_id,
                action_id,
                key,
            ]
        )


class DeploymentManagementGateway(ControlManagementGateway):
    def __init__(self, *, deployment_id: str, **kwargs) -> None:
        super().__init__(
            scope_kind="deployment", control_id=deployment_id, **kwargs,
        )


class HostManagementGateway(ControlManagementGateway):
    def __init__(self, *, host_id: str, **kwargs) -> None:
        super().__init__(scope_kind="host", control_id=host_id, **kwargs)


class DeploymentManagementScopeProvider:
    scope_kind = "deployment"
    scope_id = "deployment"

    def __init__(self, *, deployment_id: str, gateway: DeploymentManagementGateway) -> None:
        self.deployment_id = deployment_id
        self._gateway = gateway

    async def list_scopes(self) -> tuple[dict, ...]:
        return (
            {
                "scope_id": self.scope_id,
                "scope_kind": self.scope_kind,
                "source_id": self.deployment_id,
                "label": self.deployment_id,
                "available": True,
                "desired_state": None,
                "observed_state": "CONTROL_PLANE",
            },
        )

    @asynccontextmanager
    async def gateway(self, scope_id: str):
        if scope_id != self.scope_id:
            raise ForestError("MANAGEMENT_SCOPE_NOT_FOUND", "Management scope does not exist", 404)
        yield self._gateway


class HostManagementScopeProvider:
    scope_kind = "host"

    def __init__(self, *, host_id: str, gateway: HostManagementGateway) -> None:
        if not re.fullmatch(r"host/[a-f0-9]{32}", host_id):
            raise ValueError("Host management identity is invalid")
        self.host_id = host_id
        self.scope_id = "host-" + host_id.removeprefix("host/")
        self._gateway = gateway

    async def list_scopes(self) -> tuple[dict, ...]:
        return (await self.resolve(self.host_id),)

    async def resolve(self, source_id: str) -> dict:
        if source_id != self.host_id:
            raise ForestError("MANAGEMENT_SCOPE_NOT_FOUND", "Host scope does not exist", 404)
        return {
            "scope_id": self.scope_id,
            "scope_kind": self.scope_kind,
            "source_id": self.host_id,
            "label": self.host_id,
            "available": True,
            "desired_state": None,
            "observed_state": "CONTROL_PLANE",
        }

    @asynccontextmanager
    async def gateway(self, scope_id: str):
        if scope_id != self.scope_id:
            raise ForestError("MANAGEMENT_SCOPE_NOT_FOUND", "Host scope does not exist", 404)
        yield self._gateway


class BotManagementScopeProvider:
    scope_kind = "bot"

    def __init__(self, *, supervisor, ticket_authority) -> None:
        if ticket_authority is None:
            raise ValueError("Bot management requires a Web authority ticket signer")
        self.supervisor = supervisor
        self.ticket_authority = ticket_authority

    async def list_scopes(self) -> tuple[dict, ...]:
        return ()

    async def resolve(self, bot_id: str) -> dict:
        return await self.supervisor.resolve_management_scope(bot_id)

    @asynccontextmanager
    async def gateway(self, scope_id: str):
        async with self.supervisor.management_host(scope_id) as host:
            yield ManagementGateway(
                host,
                ticket_authority=self.ticket_authority,
                scope_id=scope_id,
            )


class ManagementScopeRegistry:
    def __init__(self, providers: tuple, *, ticket_authority=None) -> None:
        self.ticket_authority = ticket_authority
        self.providers = {}
        for provider in providers:
            kind = getattr(provider, "scope_kind", None)
            if not isinstance(kind, str) or _SCOPE_KIND.fullmatch(kind) is None:
                raise ValueError("Management scope provider kind is invalid")
            if kind in self.providers:
                raise ValueError("Management scope provider kind is duplicated")
            self.providers[kind] = provider

    async def list_scopes(self) -> list[dict]:
        result = []
        seen = set()
        for provider in self.providers.values():
            rows = await provider.list_scopes()
            for row in rows:
                if (
                    row.get("scope_kind") != provider.scope_kind
                    or not isinstance(row.get("scope_id"), str)
                    or _SCOPE_ID.fullmatch(row["scope_id"]) is None
                ):
                    raise ForestError(
                        "MANAGEMENT_SCOPE_PROVIDER_INVALID",
                        "Management scope provider returned an invalid scope",
                        500,
                    )
                identity = (row["scope_kind"], row["scope_id"])
                if identity in seen:
                    raise ForestError(
                        "MANAGEMENT_SCOPE_PROVIDER_INVALID",
                        "Management scope provider returned a duplicate scope",
                        500,
                    )
                seen.add(identity)
                result.append(row)
        return result

    async def resolve(self, scope_kind: str, source_id: str, session) -> dict:
        principal = getattr(session, "subject", session)
        if getattr(principal, "role", None) not in {"ADMIN", "AUDITOR"}:
            raise ForestError("MANAGEMENT_AUTHORIZATION_DENIED", "Role is invalid", 403)
        provider = self.providers.get(scope_kind)
        if provider is None or not hasattr(provider, "resolve"):
            raise ForestError("MANAGEMENT_SCOPE_NOT_FOUND", "Management scope does not exist", 404)
        row = await provider.resolve(source_id)
        if (row.get("scope_kind") != scope_kind
            or row.get("source_id") != source_id
            or not isinstance(row.get("scope_id"), str)
            or _SCOPE_ID.fullmatch(row["scope_id"]) is None
            or row.get("available") is not True):
            raise ForestError("MANAGEMENT_SCOPE_PROVIDER_INVALID", "Invalid resolved scope", 500)
        return row

    @asynccontextmanager
    async def gateway(self, scope_kind: str, scope_id: str, session):
        if (
            not isinstance(scope_kind, str)
            or _SCOPE_KIND.fullmatch(scope_kind) is None
            or not isinstance(scope_id, str)
            or _SCOPE_ID.fullmatch(scope_id) is None
        ):
            raise ForestError("MANAGEMENT_SCOPE_NOT_FOUND", "Management scope does not exist", 404)
        provider = self.providers.get(scope_kind)
        if provider is None:
            raise ForestError("MANAGEMENT_SCOPE_NOT_FOUND", "Management scope does not exist", 404)
        async with provider.gateway(scope_id) as gateway:
            yield _AuthorizedGateway(
                gateway,
                session,
                ticket_authority=self.ticket_authority,
                scope_kind=scope_kind,
                scope_id=scope_id,
            )


class _AuthorizedGateway:
    """Repeat authorization at the target gateway boundary."""

    def __init__(
        self, gateway, session, *, ticket_authority, scope_kind: str, scope_id: str
    ) -> None:
        principal = getattr(session, "subject", session)
        if getattr(principal, "role", None) not in {"ADMIN", "AUDITOR"}:
            raise ForestError("MANAGEMENT_AUTHORIZATION_DENIED", "Role is invalid", 403)
        self.gateway = gateway
        self.principal = principal
        self.session = session
        self.ticket_authority = ticket_authority
        self.scope_kind = scope_kind
        self.scope_id = scope_id

    def _write(self) -> None:
        if self.principal.role != "ADMIN":
            raise ForestError(
                "MANAGEMENT_AUTHORIZATION_DENIED",
                "Administrator role is required",
                403,
            )

    async def _ticket(
        self,
        component_id: str,
        action_key: str,
        body: dict,
        *,
        operation_id: str | None = None,
        plan_digest: str | None = None,
    ):
        if self.ticket_authority is None:
            return None, operation_id
        generation_id, graph_revision = self.gateway.authorization_target(component_id)
        operation_id = operation_id or "web-read:" + digest_json(
            [
                self.session.session_ref,
                self.scope_kind,
                self.scope_id,
                component_id,
                action_key,
                body,
                secrets.token_urlsafe(16),
            ]
        )
        ticket = await self.ticket_authority.issue(
            session=self.session,
            target_scope_kind=self.scope_kind,
            target_scope_id=getattr(
                self.gateway, "authorization_scope_id", self.scope_id,
            ),
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=action_key,
            input_digest=digest_json(body),
            plan_digest=plan_digest,
        )
        return ticket, operation_id

    async def runtime(self):
        ticket, operation_id = await self._ticket("$runtime", "runtime.read", {})
        return await self.gateway.runtime(authorization=ticket, operation_id=operation_id)

    async def read_configuration(self, component_id: str):
        ticket, operation_id = await self._ticket(component_id, "configuration.read", {})
        return await self.gateway.read_configuration(
            component_id, authorization=ticket, operation_id=operation_id
        )

    async def read_management_ui(self, component_id: str, artifact_sha256: str):
        ticket, operation_id = await self._ticket(component_id, "configuration.read", {})
        return await self.gateway.read_management_ui(
            component_id,
            artifact_sha256,
            authorization=ticket,
            operation_id=operation_id,
        )

    async def artifact_download(self, component_id: str, artifact_id: str, body: dict):
        download = getattr(self.gateway, "artifact_download", None)
        if not callable(download):
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND",
                "Package artifact operation is unavailable", 404,
            )
        ticket, operation_id = await self._ticket(
            component_id, f"artifact.{artifact_id}.download", body
        )
        return await download(
            component_id, artifact_id, body,
            authorization=ticket, operation_id=operation_id,
        )

    @asynccontextmanager
    async def artifact_upload(
        self, component_id: str, artifact_id: str, payload: dict,
        *, content_length: int, content_sha256: str, idempotency_key: str | None,
    ):
        self._write()
        upload = getattr(self.gateway, "artifact_upload", None)
        if not callable(upload):
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND",
                "Package artifact upload is unavailable", 404,
            )
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 96:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422,
            )
        generation_id, _revision = self.gateway.authorization_target(component_id)
        operation_id = self.gateway.operation_id(
            component_id, f"artifact.{artifact_id}.upload",
            digest_json([generation_id, idempotency_key]),
        )
        body = {
            "payload": payload, "content_length": content_length,
            "content_sha256": content_sha256,
        }
        ticket, operation_id = await self._ticket(
            component_id, f"artifact.{artifact_id}.upload", body,
            operation_id=operation_id,
        )
        async with upload(
            component_id, artifact_id, payload,
            content_length=content_length, content_sha256=content_sha256,
            authorization=ticket, operation_id=operation_id,
        ) as permit:
            yield permit

    async def plan_configuration(self, component_id: str, **values):
        self._write()
        body = values["body"]
        operation_id = self.gateway.operation_id(
            component_id, "configuration.plan", values.get("idempotency_key")
        )
        ticket, operation_id = await self._ticket(
            component_id,
            "configuration.plan",
            body.model_dump(mode="json"),
            operation_id=operation_id,
        )
        return await self.gateway.plan_configuration(
            component_id, **values, authorization=ticket, operation_id=operation_id
        )

    async def apply_configuration(self, component_id: str, **values):
        self._write()
        body = values["body"]
        operation_id = self.gateway.operation_id(
            component_id, "configuration.apply", values.get("idempotency_key")
        )
        ticket, operation_id = await self._ticket(
            component_id,
            "configuration.apply",
            body.model_dump(mode="json"),
            operation_id=operation_id,
            plan_digest=digest_json({"plan_id": body.plan_id}),
        )
        return await self.gateway.apply_configuration(
            component_id, **values, authorization=ticket, operation_id=operation_id
        )

    async def module_query(self, component_id: str, query_id: str, body: dict):
        ticket, operation_id = await self._ticket(component_id, query_id, body)
        result = await self.gateway.module_query(
            component_id,
            query_id,
            body,
            authorization=ticket,
            operation_id=operation_id,
        )
        validator = getattr(self.ticket_authority, "session_validator", None)
        if callable(validator):
            checked = validator(self.session)
            if inspect.isawaitable(checked):
                await checked
        return result

    async def module_action(self, component_id: str, action_id: str, **values):
        self._write()
        plan_resolver = getattr(self.gateway, "action_plan_digest", None)
        plan_digest = (
            plan_resolver(component_id, action_id, values["body"])
            if callable(plan_resolver)
            else None
        )
        operation_id = self.gateway.operation_id(
            component_id, action_id, values.get("idempotency_key")
        )
        ticket, operation_id = await self._ticket(
            component_id,
            action_id,
            values["body"],
            operation_id=operation_id,
            plan_digest=plan_digest,
        )
        return await self.gateway.module_action(
            component_id,
            action_id,
            **values,
            authorization=ticket,
            operation_id=operation_id,
        )

    async def asset_begin(self, component_id: str, asset_id: str, body: dict):
        self._write()
        operation_id = self.gateway.asset_operation_id(
            component_id, asset_id, "begin", body.get("upload_id", "")
        )
        ticket, operation_id = await self._ticket(
            component_id,
            "asset." + asset_id + ".begin",
            body,
            operation_id=operation_id,
        )
        return await self.gateway.asset_begin(
            component_id,
            asset_id,
            body,
            authorization=ticket,
            operation_id=operation_id,
        )

    async def asset_chunk(
        self,
        component_id: str,
        asset_id: str,
        upload_id: str,
        chunk_index: int,
        content: bytes,
    ):
        self._write()
        body = {
            "upload_id": upload_id,
            "chunk_index": chunk_index,
            "byte_length": len(content),
            "digest": "sha256:" + hashlib.sha256(content).hexdigest(),
        }
        operation_id = self.gateway.asset_operation_id(
            component_id, asset_id, "chunk", f"{upload_id}:{chunk_index}"
        )
        ticket, operation_id = await self._ticket(
            component_id,
            "asset." + asset_id + ".chunk",
            body,
            operation_id=operation_id,
        )
        return await self.gateway.asset_chunk(
            component_id,
            asset_id,
            upload_id,
            chunk_index,
            content,
            authorization=ticket,
            operation_id=operation_id,
        )

    async def asset_finalize(self, component_id: str, asset_id: str, upload_id: str):
        self._write()
        body = {"upload_id": upload_id}
        operation_id = self.gateway.asset_operation_id(
            component_id, asset_id, "finalize", upload_id
        )
        ticket, operation_id = await self._ticket(
            component_id,
            "asset." + asset_id + ".finalize",
            body,
            operation_id=operation_id,
        )
        return await self.gateway.asset_finalize(
            component_id,
            asset_id,
            upload_id,
            authorization=ticket,
            operation_id=operation_id,
        )

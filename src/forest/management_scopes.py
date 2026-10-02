"""Registered management scope providers for the deployment Web gateway."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import re
import secrets
from contextlib import asynccontextmanager
from typing import Callable

from forest.deployment_action_authorization import DeploymentActionAuthorization
from forest.errors import ForestError
from forest.management_gateway import ManagementGateway
from forest.packages import digest_json

_SCOPE_KIND = re.compile(r"[a-z][a-z0-9.-]{0,63}\Z")
_SCOPE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")


class DeploymentManagementGateway:
    def __init__(
        self,
        *,
        deployment_id: str,
        components: tuple,
        ticket_authority=None,
        web_receipts=None,
        audit=None,
        runtime_registry_provider: Callable | None = None,
        graph_change_authority=None,
    ) -> None:
        self.deployment_id = deployment_id
        self.components = components
        self.registry = {component.component_id: component for component in components}
        self.ticket_authority = ticket_authority
        self.web_receipts = web_receipts
        self.audit = audit
        self._runtime_registry_provider = runtime_registry_provider
        self.graph_change_authority = graph_change_authority
        self._operation_lock = asyncio.Lock()

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
                "RUNTIME_GENERATION_NOT_ACTIVE", "Deployment module is unavailable", 409
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
                else ("deployment/control-plane/1", 1)
            )
        if registry is not None and component_id in registry.states:
            if graph is None or registry.states[component_id] != "ACTIVE":
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_ACTIVE", "Deployment module is unavailable", 409
                )
            return graph.generations[component_id], graph.graph_revision
        if component_id not in self.registry:
            raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Component does not exist", 404)
        return "deployment/" + component_id + "/1", 1

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
                "Deployment RuntimeGraph changed during authorization",
                403,
            )

    async def runtime(self, *, authorization=None, operation_id=None) -> dict:
        generation_id, graph_revision = self.authorization_target("$runtime")
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
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
            "protocol": "forest.deployment-management-runtime/1",
            "runtime": (
                {
                    "scope_kind": "deployment",
                    "scope_id": self.deployment_id,
                    "graph_state": "ACTIVE",
                    "graph_id": graph.graph_id,
                    "graph_revision": graph.graph_revision,
                }
                if graph is not None
                else {
                    "scope_kind": "deployment",
                    "scope_id": self.deployment_id,
                    "graph_state": "CONTROL_PLANE",
                    "graph_id": generation_id,
                }
            ),
            "modules": (registry.describe() if registry is not None else [])
            + [component.describe() for component in self.components],
        }

    async def read_configuration(self, component_id: str, *, authorization=None, operation_id=None):
        binding = self._package_binding(component_id)
        generation_id, graph_revision = (
            binding[:2] if binding is not None else self.authorization_target(component_id)
        )
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        if binding is not None:
            return await binding[2].management_contract.read()
        self._unsupported_configuration(component_id)

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
            target_scope_kind="deployment",
            target_scope_id="deployment",
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

    async def plan_configuration(
        self, component_id: str, *, authorization=None, operation_id=None, **_values
    ):
        generation_id, graph_revision = self.authorization_target(component_id)
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.plan",
            input_digest=digest_json(_values["body"].model_dump(mode="json")),
            required_role="ADMIN",
        )
        self._unsupported_configuration(component_id)

    async def apply_configuration(
        self, component_id: str, *, authorization=None, operation_id=None, **_values
    ):
        body = _values["body"]
        generation_id, graph_revision = self.authorization_target(component_id)
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key="configuration.apply",
            input_digest=digest_json(body.model_dump(mode="json")),
            plan_digest=digest_json({"plan_id": body.plan_id}),
            required_role="ADMIN",
        )
        self._unsupported_configuration(component_id)

    async def module_query(
        self, component_id: str, query_id: str, body: dict, *, authorization=None, operation_id=None
    ):
        if component_id == "$runtime":
            if (
                query_id != "module.change.status"
                or not isinstance(body, dict)
                or (set(body) != {"operation_id"})
            ):
                raise ForestError(
                    "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Query does not exist", 404
                )
            generation_id, graph_revision = self.authorization_target("$runtime")
            await self._authorize(
                authorization,
                target_scope_kind="deployment",
                target_scope_id="deployment",
                target_component_id="$runtime",
                target_generation_id=generation_id,
                target_graph_revision=graph_revision,
                target_operation_ref=operation_id,
                action_key=query_id,
                input_digest=digest_json(body),
                required_role="AUDITOR",
            )
            if self.graph_change_authority is None:
                raise ForestError(
                    "DEPLOYMENT_RUNTIME_UNAVAILABLE", "Replacement authority is unavailable", 503
                )
            return await self.graph_change_authority.get(body["operation_id"])
        binding = self._package_binding(component_id)
        generation_id, graph_revision = (
            binding[:2] if binding is not None else self.authorization_target(component_id)
        )
        contract = (
            binding[2].management_contract if binding is not None else self._contract(component_id)
        )
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=query_id,
            input_digest=digest_json(body),
            required_role="AUDITOR",
        )
        return await contract.query(query_id, body)

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
                    "DEPLOYMENT_RUNTIME_UNAVAILABLE", "Replacement authority is unavailable", 503
                )
            registry = self.runtime_registry
            if registry is None:
                raise ForestError(
                    "DEPLOYMENT_RUNTIME_UNAVAILABLE", "Deployment Registry is unavailable", 503
                )
            generation_id, graph_revision = self.authorization_target("$runtime")
            input_digest = digest_json(body)
            await self._authorize(
                authorization,
                target_scope_kind="deployment",
                target_scope_id="deployment",
                target_component_id="$runtime",
                target_generation_id=generation_id,
                target_graph_revision=graph_revision,
                target_operation_ref=operation_id,
                action_key=action_id,
                input_digest=input_digest,
                required_role="ADMIN",
            )
            guard = DeploymentActionAuthorization(
                authorization,
                self._authorize_mutation_in,
                "$runtime",
                action_id,
                input_digest,
                operation_id,
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
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph_revision,
            target_operation_ref=operation_id,
            action_key=action_id,
            input_digest=input_digest,
            required_role="ADMIN",
        )
        async with self._operation_lock:
            return await self._contract(component_id).action(
                action_id,
                operation_id=operation_id,
                payload=body,
                authorization=(
                    DeploymentActionAuthorization(
                        authorization,
                        self._authorize_mutation_in,
                        component_id,
                        action_id,
                        input_digest,
                        operation_id,
                    )
                    if self.ticket_authority is not None
                    else None
                ),
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
        if self.ticket_authority is None or self.web_receipts is None or self.audit is None:
            raise ForestError(
                "WEB_AUTHORITY_TRANSACTION_REQUIRED",
                "Deployment Web authority receipt storage is unavailable",
                503,
            )
        generation_id, graph_revision = self.authorization_target(component_id)
        await self.ticket_authority.verify(
            ticket,
            target_scope_kind="deployment",
            target_scope_id="deployment",
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
        await self.audit.append_web_action_in(conn, ticket=ticket, receipt=receipt)
        return receipt

    def _contract(self, component_id: str):
        module = self._package_module(component_id)
        if module is not None:
            return module.management_contract
        component = self.registry.get(component_id)
        if component is None:
            raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Component does not exist", 404)
        return component.contract

    def _unsupported_configuration(self, component_id: str) -> None:
        self._contract(component_id)
        raise ForestError(
            "MODULE_MANAGEMENT_UNAVAILABLE",
            "Component does not publish a configuration contract",
            404,
        )

    def operation_id(self, component_id: str, action_id: str, key: str | None) -> str:
        if key is None or not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422
            )
        return "web:" + digest_json(
            [
                "forest.deployment-management-operation/1",
                self.deployment_id,
                component_id,
                action_id,
                key,
            ]
        )


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
                "bot_id": self.deployment_id,
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


class BotManagementScopeProvider:
    scope_kind = "bot"

    def __init__(self, *, supervisor, ticket_authority) -> None:
        if ticket_authority is None:
            raise ValueError("Bot management requires a Web authority ticket signer")
        self.supervisor = supervisor
        self.ticket_authority = ticket_authority
        self._gateways: dict[str, tuple[object, ManagementGateway]] = {}

    async def list_scopes(self) -> tuple[dict, ...]:
        return await self.supervisor.management_scopes()

    @asynccontextmanager
    async def gateway(self, scope_id: str):
        async with self.supervisor.management_host(scope_id) as host:
            cached = self._gateways.get(scope_id)
            if cached is None or cached[0] is not host:
                cached = (
                    host,
                    ManagementGateway(
                        host,
                        ticket_authority=self.ticket_authority,
                        scope_id=scope_id,
                    ),
                )
                self._gateways[scope_id] = cached
            yield cached[1]


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
            target_scope_id=self.scope_id,
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

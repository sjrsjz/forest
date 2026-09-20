"""Registered management scope providers for the deployment Web gateway."""

from __future__ import annotations

import asyncio
import re
import secrets
from contextlib import asynccontextmanager

from forest.clean_management_app import CleanManagementGateway
from forest.errors import ForestError
from forest.packages import digest_json

_SCOPE_KIND = re.compile(r"[a-z][a-z0-9.-]{0,63}\Z")
_SCOPE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")


class DeploymentManagementGateway:
    def __init__(self, *, deployment_id: str, components: tuple, ticket_authority=None) -> None:
        self.deployment_id = deployment_id
        self.components = components
        self.registry = {component.component_id: component for component in components}
        self.ticket_authority = ticket_authority
        self._operation_lock = asyncio.Lock()

    def authorization_target(self, component_id: str) -> tuple[str, int]:
        if component_id != "$runtime":
            self._contract(component_id)
        return "deployment/control-plane/1", 1

    async def _authorize(self, ticket, **expected) -> None:
        if self.ticket_authority is None:
            return
        if ticket is None:
            raise ForestError(
                "WEB_AUTHORITY_TICKET_REQUIRED", "Web authority ticket is required", 403
            )
        await self.ticket_authority.verify(ticket, **expected)

    async def runtime(self, *, authorization=None, operation_id=None) -> dict:
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id="$runtime",
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
            target_operation_ref=operation_id,
            action_key="runtime.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        return {
            "protocol": "forest.deployment-management-runtime/1",
            "runtime": {
                "bot_id": self.deployment_id,
                "graph_state": "CONTROL_PLANE",
                "graph_id": "deployment/control-plane/1",
            },
            "modules": [component.describe() for component in self.components],
        }

    async def read_configuration(self, component_id: str, *, authorization=None, operation_id=None):
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
            target_operation_ref=operation_id,
            action_key="configuration.read",
            input_digest=digest_json({}),
            required_role="AUDITOR",
        )
        self._unsupported_configuration(component_id)

    async def plan_configuration(
        self, component_id: str, *, authorization=None, operation_id=None, **_values
    ):
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
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
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
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
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
            target_operation_ref=operation_id,
            action_key=query_id,
            input_digest=digest_json(body),
            required_role="AUDITOR",
        )
        return await self._contract(component_id).query(query_id, body)

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
        await self._authorize(
            authorization,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id=component_id,
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
            target_operation_ref=operation_id,
            action_key=action_id,
            input_digest=digest_json(body),
            required_role="ADMIN",
        )
        async with self._operation_lock:
            return await self._contract(component_id).action(
                action_id,
                operation_id=operation_id,
                payload=body,
            )

    def _contract(self, component_id: str):
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

    def operation_id(
        self, component_id: str, action_id: str, key: str | None
    ) -> str:
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
            raise ForestError(
                "MANAGEMENT_SCOPE_NOT_FOUND", "Management scope does not exist", 404
            )
        yield self._gateway


class BotManagementScopeProvider:
    scope_kind = "bot"

    def __init__(self, *, supervisor, ticket_authority=None) -> None:
        self.supervisor = supervisor
        self.ticket_authority = ticket_authority
        self._internal_token = secrets.token_urlsafe(32)
        self._gateways: dict[str, tuple[object, CleanManagementGateway]] = {}

    async def list_scopes(self) -> tuple[dict, ...]:
        return await self.supervisor.management_scopes()

    @asynccontextmanager
    async def gateway(self, scope_id: str):
        async with self.supervisor.management_host(scope_id) as host:
            cached = self._gateways.get(scope_id)
            if cached is None or cached[0] is not host:
                cached = (
                    host,
                    CleanManagementGateway(
                        host,
                        self._internal_token,
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
            [self.session.session_ref, self.scope_kind, self.scope_id, component_id,
             action_key, body, secrets.token_urlsafe(16)]
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
        return await self.gateway.module_query(
            component_id,
            query_id,
            body,
            authorization=ticket,
            operation_id=operation_id,
        )

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

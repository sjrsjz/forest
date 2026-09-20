"""Registered management scope providers for the deployment Web gateway."""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager

from forest.clean_management_app import CleanManagementGateway
from forest.errors import ForestError
from forest.packages import digest_json

_SCOPE_KIND = re.compile(r"[a-z][a-z0-9.-]{0,63}\Z")
_SCOPE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")


class DeploymentManagementGateway:
    def __init__(self, *, deployment_id: str, components: tuple) -> None:
        self.deployment_id = deployment_id
        self.components = components
        self.registry = {component.component_id: component for component in components}
        self._operation_lock = asyncio.Lock()

    async def runtime(self) -> dict:
        return {
            "protocol": "forest.deployment-management-runtime/1",
            "runtime": {
                "bot_id": self.deployment_id,
                "graph_state": "CONTROL_PLANE",
                "graph_id": "deployment/control-plane/1",
            },
            "modules": [component.describe() for component in self.components],
        }

    async def read_configuration(self, component_id: str):
        self._unsupported_configuration(component_id)

    async def plan_configuration(self, component_id: str, **_values):
        self._unsupported_configuration(component_id)

    async def apply_configuration(self, component_id: str, **_values):
        self._unsupported_configuration(component_id)

    async def module_query(self, component_id: str, query_id: str, body: dict):
        return await self._contract(component_id).query(query_id, body)

    async def module_action(
        self,
        component_id: str,
        action_id: str,
        *,
        body: dict,
        idempotency_key: str | None,
    ):
        async with self._operation_lock:
            return await self._contract(component_id).action(
                action_id,
                operation_id=self._operation_id(component_id, action_id, idempotency_key),
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

    def _operation_id(
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

    def __init__(self, *, supervisor, access_token: str) -> None:
        self.supervisor = supervisor
        self.access_token = access_token
        self._gateways: dict[str, tuple[object, CleanManagementGateway]] = {}

    async def list_scopes(self) -> tuple[dict, ...]:
        return await self.supervisor.management_scopes()

    @asynccontextmanager
    async def gateway(self, scope_id: str):
        async with self.supervisor.management_host(scope_id) as host:
            cached = self._gateways.get(scope_id)
            if cached is None or cached[0] is not host:
                cached = (host, CleanManagementGateway(host, self.access_token))
                self._gateways[scope_id] = cached
            yield cached[1]


class ManagementScopeRegistry:
    def __init__(self, providers: tuple) -> None:
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
    async def gateway(self, scope_kind: str, scope_id: str):
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
            yield gateway

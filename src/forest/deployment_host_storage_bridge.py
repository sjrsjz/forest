"""Restricted process bridge from frozen deployment intent to Host storage."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation


class DeploymentHostStorageBridge:
    """Advance one frozen request through Host-owned storage services."""

    def __init__(
        self, *, requests, deployment_supervisor, host_supervisor,
        host_controller_provider,
    ) -> None:
        self.requests = requests
        self.deployment_supervisor = deployment_supervisor
        self.host_supervisor = host_supervisor
        self.host_controller_provider = host_controller_provider

    async def reserve(self, request_id: str):
        request = await self.requests.read(request_id)
        self._require_host(request)
        existing = await self.requests.resource_binding(request_id)
        if existing is not None:
            return existing
        receipt = await self._invoke_host(
            request, "host.storage.reserve", "reserve",
            {
                "deployment_id": request.deployment_id,
                "bot_id": request.bot_id,
                "storage_generation": request.storage_generation,
            },
        )
        return await self.requests.bind_resource(
            self.deployment_supervisor.authority_lease(),
            request_id=request.request_id, receipt=receipt,
        )

    async def enqueue_materialization(self, request_id: str) -> dict:
        request = await self.requests.read(request_id)
        binding = await self.reserve(request_id)
        return await self._invoke_host(
            request, "host.storage.materialization.enqueue", "materialize",
            {"resource_id": binding.resource_id,
             "credential_ref": request.credential_ref},
        )

    async def materialization_status(self, request_id: str) -> dict:
        request = await self.requests.read(request_id)
        self._require_host(request)
        binding = await self.requests.resource_binding(request_id)
        if binding is None:
            raise ForestError(
                "STORAGE_RESOURCE_NOT_BOUND", "Host resource is not bound", 409,
            )
        return await self._invoke_host(
            request, "host.storage.materialization.status", "status",
            {
                "resource_id": binding.resource_id,
                "deployment_id": request.deployment_id,
                "bot_id": request.bot_id,
                "storage_generation": request.storage_generation,
            },
        )

    def _require_host(self, request) -> None:
        host_lease = self.host_supervisor.authority_lease()
        if host_lease.host_id != request.host_id:
            raise ForestError(
                "STORAGE_REQUEST_HOST_UNAVAILABLE",
                "The frozen storage request targets another Host", 409,
            )

    async def _invoke_host(
        self, request, service_id: str, operation_suffix: str, payload: dict,
    ) -> dict:
        self._require_host(request)
        if (service_id, operation_suffix) not in {
            ("host.storage.reserve", "reserve"),
            ("host.storage.materialization.enqueue", "materialize"),
            ("host.storage.materialization.status", "status"),
        }:
            raise ValueError("Host storage bridge service is not declared")
        registry = self.host_controller_provider().registry
        graph = registry.graph_identity
        if (
            graph is None
            or "host.storage" not in graph.generations
            or getattr(registry.states.get("host.storage"), "value", None) != "ACTIVE"
        ):
            raise ForestError(
                "HOST_STORAGE_GENERATION_UNAVAILABLE",
                "Host storage Generation is unavailable", 503,
            )
        member = registry.get("host.storage")
        if member.scope_id != request.host_id:
            raise ForestError(
                "HOST_STORAGE_SCOPE_MISMATCH",
                "Host storage Generation belongs to another Host", 409,
            )
        route = FrozenServiceRoute(
            service_id=service_id, version=1,
            caller_component_id="deployment.storage-bridge",
            provider_component_id="host.storage",
            provider_generation_id=graph.generations["host.storage"],
            graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        endpoint = member.runtime_service_handlers()[(service_id, 1)]
        return await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                request.request_id + "/" + operation_suffix, 15,
                "deployment.storage-request", request.request_id,
            ),
            (payload,), {},
        )

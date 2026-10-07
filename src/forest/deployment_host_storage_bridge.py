"""Restricted process bridge from frozen deployment intent to Host storage."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation


class DeploymentHostStorageReservationBridge:
    """Reserve one Host resource without inventing identity on recovery."""

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
        host_lease = self.host_supervisor.authority_lease()
        if host_lease.host_id != request.host_id:
            raise ForestError(
                "STORAGE_REQUEST_HOST_UNAVAILABLE",
                "The frozen storage request targets another Host", 409,
            )
        existing = await self.requests.resource_binding(request_id)
        if existing is not None:
            return existing
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
            service_id="host.storage.reserve", version=1,
            caller_component_id="deployment.storage-bridge",
            provider_component_id="host.storage",
            provider_generation_id=graph.generations["host.storage"],
            graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        endpoint = member.runtime_service_handlers()[("host.storage.reserve", 1)]
        receipt = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                request.request_id + "/reserve", 15,
                "deployment.storage-request", request.request_id,
            ),
            ({
                "deployment_id": request.deployment_id,
                "bot_id": request.bot_id,
                "storage_generation": request.storage_generation,
            },), {},
        )
        return await self.requests.bind_resource(
            self.deployment_supervisor.authority_lease(),
            request_id=request.request_id, receipt=receipt,
        )

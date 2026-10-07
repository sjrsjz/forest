"""Restricted process bridge from frozen deployment intent to Host storage."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation


class DeploymentHostStorageBridge:
    """Advance one frozen request through Host-owned storage services."""

    def __init__(
        self, *, requests, profiles, deployment_supervisor, host_supervisor,
        host_controller_provider,
    ) -> None:
        self.requests = requests
        self.profiles = profiles
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
            self._resource_lookup(request, binding.resource_id),
        )

    async def pin_reference(self, request_id: str):
        request = await self.requests.read(request_id)
        self._require_host(request)
        existing = await self.requests.pinned_resource(request_id)
        if existing is not None:
            return existing
        binding = await self.requests.resource_binding(request_id)
        if binding is None:
            raise ForestError(
                "STORAGE_RESOURCE_NOT_BOUND", "Host resource is not bound", 409,
            )
        lookup = self._resource_lookup(request, binding.resource_id)
        pin = await self._invoke_host(
            request, "host.storage.reference.pin", "pin",
            {**lookup, "profile_ref": request.profile_ref},
        )
        resource = await self._invoke_host(
            request, "host.storage.resource", "resource", lookup,
        )
        return await self.requests.bind_pin(
            self.deployment_supervisor.authority_lease(),
            request_id=request_id, pin_receipt=pin, resource=resource,
        )

    async def release_reference(self, request_id: str) -> dict:
        """Forward only the currently rechecked deployment revocation."""
        request = await self.requests.read(request_id)
        self._require_host(request)
        pinned = await self.requests.pinned_resource(request_id)
        if pinned is None:
            raise ForestError(
                "STORAGE_REQUEST_NOT_PINNED", "Host resource has no deployment pin", 409,
            )
        revoked = await self.profiles.revocation_for_release(
            self.deployment_supervisor.authority_lease(), request_id,
        )
        if (
            revoked["profile_ref"] != request.profile_ref
            or revoked["bot_id"] != request.bot_id
            or revoked["storage_generation"] != request.storage_generation
            or revoked["host_id"] != request.host_id
            or revoked["resource_id"] != pinned.resource_id
            or pinned.host_id != request.host_id
        ):
            raise ForestError(
                "STORAGE_PROFILE_RETIREMENT_MISMATCH",
                "Revocation differs from the pinned Host resource", 409,
            )
        return await self._invoke_host(
            request, "host.storage.reference.release", "release",
            {
                **self._resource_lookup(request, pinned.resource_id),
                "profile_ref": request.profile_ref,
                "pinned_resource_revision": pinned.resource_revision,
                "retirement_operation_id": revoked["retirement_operation_id"],
                "revocation_operation_id": revoked["operation_id"],
                "bot_revision": revoked["bot_revision"],
                "intent_revision": revoked["intent_revision"],
                "observation_revision": revoked["observation_revision"],
            },
        )

    async def enqueue_reclamation(self, request_id: str) -> dict:
        """Admit Host-owned physical work only from a current deployment fence."""
        request = await self.requests.read(request_id)
        released = await self.release_reference(request_id)
        return await self._invoke_host(
            request, "host.storage.reclaim.enqueue", "reclaim", released,
        )

    async def reclamation_status(self, request_id: str) -> dict:
        request = await self.requests.read(request_id)
        pinned = await self.requests.pinned_resource(request_id)
        if pinned is None:
            raise ForestError(
                "STORAGE_REQUEST_NOT_PINNED", "Host resource has no deployment pin", 409,
            )
        return await self._invoke_host(
            request, "host.storage.reclaim.status", "reclaim-status",
            self._resource_lookup(request, pinned.resource_id),
        )

    @staticmethod
    def _resource_lookup(request, resource_id: str) -> dict:
        return {
            "resource_id": resource_id,
            "deployment_id": request.deployment_id,
            "bot_id": request.bot_id,
            "storage_generation": request.storage_generation,
        }

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
            ("host.storage.reference.pin", "pin"),
            ("host.storage.reference.release", "release"),
            ("host.storage.reclaim.enqueue", "reclaim"),
            ("host.storage.reclaim.status", "reclaim-status"),
            ("host.storage.resource", "resource"),
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

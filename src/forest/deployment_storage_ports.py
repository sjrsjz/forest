"""Generation-bound native ports for frozen Host storage requests."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.deployment_storage_dispatch import StorageDispatchClaim

_PORTS = {
    "deployment.storage-orchestrator.inspect": "read",
    "deployment.storage-orchestrator.enqueue": "idempotent",
    "deployment.storage-orchestrator.status": "read",
    "deployment.storage-orchestrator.pin": "idempotent",
    "deployment.storage-orchestrator.finalize": "idempotent",
}


class DeploymentStoragePortEffectDriver:
    """Expose one constrained native operation; the Worker owns sequencing."""

    def __init__(
        self, configuration, configuration_revision, identity, declaration, *,
        runtime, dispatch, requests, profiles, secrets, bridge, settlement,
    ) -> None:
        name = declaration.get("name")
        if (
            identity.component_id != "deployment.storage-orchestrator"
            or name not in _PORTS
            or declaration.get("kind") != "deployment.host-storage-port"
            or declaration.get("semantics") != _PORTS[name]
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Host storage port received another binding", 500,
            )
        self.name = name
        self.identity = identity
        self.runtime = runtime
        self.dispatch = dispatch
        self.requests = requests
        self.profiles = profiles
        self.secrets = secrets
        self.bridge = bridge
        self.settlement = settlement
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False

    async def invoke(self, payload, *, scope, invocation) -> dict:
        if (
            not self.accepting
            or scope.scope_kind != "deployment"
            or scope.scope_id != self.runtime.scope_id
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Storage port is outside its Generation", 409,
            )
        lease = self.runtime.authority_lease()
        claim = StorageDispatchClaim(
            payload["request_id"], payload["claim_token"],
            lease.owner_epoch, payload["attempt_no"],
        )
        async with self.runtime.store.transaction() as conn:
            await self.runtime.generation_authority.verify_owned_work_in(
                conn, lease,
                component_id=scope.component_id,
                generation_id=scope.generation_id,
                claim_new=False,
            )
            await self.dispatch.held_claim(lease, claim, conn=conn)

        if self.name == "deployment.storage-orchestrator.inspect":
            request = await self.requests.read(claim.request_id)
            try:
                existing = await self.profiles.metadata(request.profile_ref)
            except ForestError as exc:
                if exc.code != "STORAGE_PROFILE_NOT_FOUND":
                    raise
                state = "PENDING"
            else:
                if (
                    existing.bot_id != request.bot_id
                    or existing.storage_generation != request.storage_generation
                    or existing.dsn_secret_ref != request.dsn_secret_ref
                ):
                    raise ForestError(
                        "STORAGE_SETTLEMENT_CONFLICT",
                        "Existing storage profile differs from frozen request", 409,
                    )
                state = "SETTLED"
            return {"request_id": claim.request_id, "state": state}

        if self.name == "deployment.storage-orchestrator.enqueue":
            await self.bridge.enqueue_materialization(claim.request_id)
            return {"request_id": claim.request_id, "state": "ENQUEUED"}

        if self.name == "deployment.storage-orchestrator.status":
            status = await self.bridge.materialization_status(claim.request_id)
            result = {"request_id": claim.request_id, "state": status["state"]}
            if status.get("last_error_code") is not None:
                result["last_error_code"] = status["last_error_code"]
            return result

        if self.name == "deployment.storage-orchestrator.pin":
            await self.bridge.pin_reference(claim.request_id)
            return {"request_id": claim.request_id, "state": "PINNED"}

        request = await self.requests.read(claim.request_id)
        password = await self.secrets.resolve_text_for_purpose(
            request.credential_ref, "storage.postgresql.password",
        )
        await self.settlement.finalize(
            lease, request_id=claim.request_id, password=password,
        )
        return {"request_id": claim.request_id, "state": "SETTLED"}

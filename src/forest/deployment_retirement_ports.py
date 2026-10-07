"""Narrow Generation-bound effects used by the retirement package Worker."""

from __future__ import annotations

import hashlib

from forest.errors import ForestError


class DeploymentRetirementPort:
    _PORTS = {
        "deployment.retirement.quiescence": "read",
        "deployment.retirement.revoke": "idempotent",
        "deployment.retirement.release": "idempotent",
        "deployment.retirement.reclaim": "idempotent",
        "deployment.retirement.status": "read",
    }

    def __init__(
        self, configuration, configuration_revision, identity, declaration,
        *, runtime, jobs, catalog, profiles, bridge,
    ) -> None:
        name = declaration.get("name")
        if (
            identity.component_id != "deployment.retirement"
            or declaration.get("kind") != "deployment.retirement-port"
            or self._PORTS.get(name) != declaration.get("semantics")
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Retirement port received another Generation binding", 500,
            )
        if jobs.store is not runtime.store or catalog.store is not runtime.store:
            raise ValueError("Retirement port authorities differ from deployment")
        self.identity = identity
        self.name = name
        self.runtime = runtime
        self.jobs = jobs
        self.catalog = catalog
        self.profiles = profiles
        self.bridge = bridge
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
                "Retirement port is outside its Generation", 409,
            )
        lease = self.runtime.authority_lease()
        async with self.runtime.store.transaction() as conn:
            await self.runtime.generation_authority.verify_owned_work_in(
                conn, lease, component_id=scope.component_id,
                generation_id=scope.generation_id, claim_new=False,
            )
            claim = await self.jobs.held_claim(
                lease, operation_id=payload["operation_id"],
                claim_token=payload["claim_token"],
                attempt_no=payload["attempt_no"], conn=conn,
            )
            if claim.bot_id != payload["bot_id"]:
                raise ForestError(
                    "RETIREMENT_INPUT_TAMPERED", "Port input differs from claim", 409,
                )
            revocation = None
            if self.name not in {
                "deployment.retirement.quiescence", "deployment.retirement.revoke",
            }:
                row = await (
                    await conn.execute(
                        "SELECT operation_id,request_id,resource_id FROM "
                        "forest_deployment.storage_profile_revocations "
                        "WHERE retirement_operation_id=%s AND bot_id=%s FOR SHARE",
                        (claim.operation_id, claim.bot_id),
                    )
                ).fetchone()
                if row is None:
                    raise ForestError(
                        "STORAGE_PROFILE_NOT_REVOKED",
                        "Retirement has no committed profile revocation", 409,
                    )
                revocation = dict(row)
        if self.name == "deployment.retirement.quiescence":
            witness = await self.catalog.retirement_quiescence(claim.bot_id)
            if witness["retirement_operation_id"] != claim.operation_id:
                raise ForestError(
                    "RETIREMENT_FENCE_MISMATCH", "Bot retirement differs from claim", 409,
                )
            return {"state": "QUIESCENT"}
        if self.name == "deployment.retirement.revoke":
            witness = await self.catalog.retirement_quiescence(claim.bot_id)
            if witness["retirement_operation_id"] != claim.operation_id:
                raise ForestError(
                    "RETIREMENT_FENCE_MISMATCH", "Bot retirement differs from claim", 409,
                )
            digest = hashlib.sha256(claim.operation_id.encode("utf-8")).hexdigest()
            revoked = await self.profiles.revoke(
                lease, operation_id="retire-revoke/" + digest,
                profile_ref=witness["storage_profile_ref"],
                bot_id=claim.bot_id,
                expected_bot_revision=witness["bot_revision"],
            )
            if revoked["retirement_operation_id"] != claim.operation_id:
                raise ForestError(
                    "RETIREMENT_FENCE_MISMATCH", "Profile revocation differs", 409,
                )
            return self._resource(revoked)
        if self.name == "deployment.retirement.release":
            released = await self.bridge.release_reference(revocation["request_id"])
            if (
                released["resource_id"] != revocation["resource_id"]
                or released["revocation_operation_id"] != revocation["operation_id"]
                or released["state"] != "RETIRED"
            ):
                raise ForestError(
                    "RETIREMENT_HOST_PROOF_MISMATCH", "Host release differs", 409,
                )
            return self._resource(revocation)
        if self.name == "deployment.retirement.reclaim":
            work = await self.bridge.enqueue_reclamation(revocation["request_id"])
            if work["resource_id"] != revocation["resource_id"]:
                raise ForestError(
                    "RETIREMENT_HOST_PROOF_MISMATCH", "Host work differs", 409,
                )
            return self._resource(revocation)
        status = await self.bridge.reclamation_status(revocation["request_id"])
        if status["resource_id"] != revocation["resource_id"]:
            raise ForestError(
                "RETIREMENT_HOST_PROOF_MISMATCH", "Host status differs", 409,
            )
        return {
            **self._resource(revocation),
            "state": status["state"],
            "last_error_code": status["last_error_code"],
        }

    @staticmethod
    def _resource(revocation) -> dict:
        return {
            "request_id": revocation["request_id"],
            "resource_id": revocation["resource_id"],
        }

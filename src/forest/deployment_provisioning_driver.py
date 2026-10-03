"""Generation-bound native materialization port for a claimed Bot supply Job."""

from __future__ import annotations

from forest.errors import ForestError


class DeploymentProvisioningEffectDriver:
    def __init__(
        self, configuration, configuration_revision, identity, declaration,
        *, runtime, jobs, storage_profiles, bootstrap_profiles, provisioner,
    ) -> None:
        if (
            identity.component_id != "deployment.provisioning"
            or declaration.get("name") != "deployment.provisioning.materialize"
            or declaration.get("kind") != "deployment.provisioning-materializer"
            or declaration.get("semantics") != "idempotent"
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Provisioning materializer received another port", 500,
            )
        self.identity = identity
        self.runtime = runtime
        self.jobs = jobs
        self.storage_profiles = storage_profiles
        self.bootstrap_profiles = bootstrap_profiles
        self.provisioner = provisioner
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
                "Provisioning materializer is outside its Generation", 409,
            )
        lease = self.runtime.authority_lease()
        async with self.runtime.store.transaction() as conn:
            await self.runtime.generation_authority.verify_owned_work_in(
                conn, lease, component_id=scope.component_id,
                generation_id=scope.generation_id, claim_new=False,
            )
            claim = await self.jobs.held_claim(
                lease, job_id=payload["job_id"],
                claim_token=payload["claim_token"],
                attempt_no=payload["attempt_no"], conn=conn,
            )
            if any(getattr(claim, name) != payload[name] for name in (
                "bot_id", "storage_profile_ref", "storage_generation",
                "bootstrap_profile_ref", "bootstrap_manifest_digest",
            )):
                raise ForestError(
                    "PROVISIONING_INPUT_TAMPERED",
                    "Materialization inputs differ from the claimed Job", 409,
                )
        storage = await self.storage_profiles.resolve(
            profile_ref=claim.storage_profile_ref,
            expected_bot_id=claim.bot_id,
            expected_storage_generation=claim.storage_generation,
        )
        profile = await self.bootstrap_profiles.load(claim.bootstrap_profile_ref)
        if (
            profile.bot_id != claim.bot_id
            or profile.storage_profile_ref != claim.storage_profile_ref
            or profile.storage_generation != claim.storage_generation
            or profile.manifest_digest != claim.bootstrap_manifest_digest
        ):
            raise ForestError(
                "PROVISIONING_INPUT_TAMPERED",
                "Bootstrap profile differs from the claimed Job", 409,
            )
        await self.provisioner(claim=claim, storage=storage, profile=profile)
        return {"bot_id": claim.bot_id, "materialized": True}

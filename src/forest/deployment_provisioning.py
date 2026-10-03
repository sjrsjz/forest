"""Execution boundary for deployment-scoped Bot provisioning Jobs."""

from __future__ import annotations

import asyncio
import inspect
import re

from forest.errors import ForestError

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class DeploymentProvisioningExecutor:
    """Resolve immutable inputs, invoke an idempotent provisioner, then publish READY."""

    def __init__(
        self,
        *,
        jobs,
        catalog,
        storage_profiles,
        bootstrap_profiles,
        provisioner,
        worker_id: str,
        lease_seconds: int = 120,
    ) -> None:
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 900:
            raise ValueError("lease_seconds is outside its bound")
        self.jobs = jobs
        self.catalog = catalog
        self.storage_profiles = storage_profiles
        self.bootstrap_profiles = bootstrap_profiles
        self.provisioner = provisioner
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds

    async def run_once(self, supervisor_lease):
        claim = await self.jobs.claim(
            supervisor_lease,
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
        )
        if claim is None:
            return None
        try:
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
                    "Provisioning input differs from the claimed immutable facts",
                    409,
                )
            task = asyncio.create_task(
                self._invoke(claim=claim, storage=storage, profile=profile),
                name="deployment-provision/" + claim.job_id,
            )
            try:
                while not task.done():
                    done, _ = await asyncio.wait(
                        {task}, timeout=max(3, self.lease_seconds // 3)
                    )
                    if done:
                        break
                    claim = await self.jobs.renew(
                        supervisor_lease,
                        claim,
                        lease_seconds=self.lease_seconds,
                    )
                await task
            except BaseException:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise
            return await self.jobs.complete_ready(
                supervisor_lease, claim, catalog=self.catalog,
            )
        except Exception as exc:
            error_code = exc.code if isinstance(exc, ForestError) else "BOT_PROVISIONING_FAILED"
            if not isinstance(error_code, str) or _ERROR.fullmatch(error_code) is None:
                error_code = "BOT_PROVISIONING_FAILED"
            try:
                return await self.jobs.fail(
                    supervisor_lease,
                    claim,
                    error_code=error_code,
                )
            except ForestError as settlement_error:
                if settlement_error.code in {
                    "STALE_DEPLOYMENT_SUPERVISOR",
                    "STALE_PROVISIONING_CLAIM",
                }:
                    raise settlement_error from exc
                raise

    async def _invoke(self, *, claim, storage, profile) -> None:
        result = self.provisioner(claim=claim, storage=storage, profile=profile)
        if inspect.isawaitable(result):
            await result

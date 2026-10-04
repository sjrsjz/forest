"""Bind one package Generation to the kernel's sole provisioning Job ledger."""

from __future__ import annotations

import asyncio
import hashlib

from forest.errors import ForestError


class DeploymentProvisioningOwnedWorkAuthority:
    def __init__(self, runtime, jobs, catalog) -> None:
        if jobs.store is not runtime.store or catalog.store is not runtime.store:
            raise ValueError("Provisioning authorities belong to different deployments")
        self.runtime = runtime
        self.jobs = jobs
        self.catalog = catalog

    async def invoke_owned_work(
        self, phase, scope, declaration, payload, *, timeout_seconds
    ):
        if (
            scope.scope_kind != "deployment"
            or scope.scope_id != self.runtime.scope_id
            or scope.component_id != "deployment.provisioning"
            or declaration.get("authority") != "kernel"
            or declaration.get("name") != "deployment.provisioning.execute"
            or phase not in {"claim", "renew", "settle"}
        ):
            raise ForestError(
                "PROVISIONING_WORK_BINDING_INVALID",
                "Provisioning work differs from its official Generation binding", 403,
            )
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self.runtime.store.transaction() as conn:
                    lease = self.runtime.authority_lease()
                    await self.runtime.generation_authority.verify_owned_work_in(
                        conn, lease, component_id=scope.component_id,
                        generation_id=scope.generation_id,
                        claim_new=phase == "claim",
                    )
                    if phase == "claim":
                        if payload.get("protocol") != "forest.owned-work-claim-request/1":
                            raise ForestError(
                                "PROVISIONING_WORK_INPUT_INVALID", "Claim input is invalid", 422
                            )
                        claimed = await self.jobs.claim(
                            lease, worker_id=payload["claim_token"],
                            lease_seconds=payload["lease_seconds"], conn=conn,
                        )
                        if claimed is None:
                            return None
                        operation = hashlib.sha256(
                            (claimed.deployment_id + "/" + claimed.job_id + "/"
                             + str(claimed.attempt_no)).encode("utf-8")
                        ).hexdigest()
                        return {
                            "protocol": "forest.owned-work-claim/1",
                            "work_id": claimed.job_id,
                            "claim_token": claimed.lease_owner,
                            "attempt": claimed.attempt_no,
                            "operation_id": "provision:" + operation,
                            "causal_root_kind": "deployment.provisioning",
                            "causal_root_id": claimed.job_id,
                            "payload": {
                                "job_id": claimed.job_id,
                                "bot_id": claimed.bot_id,
                                "storage_profile_ref": claimed.storage_profile_ref,
                                "storage_generation": claimed.storage_generation,
                                "bootstrap_profile_ref": claimed.bootstrap_profile_ref,
                                "bootstrap_manifest_digest": claimed.bootstrap_manifest_digest,
                                "claim_token": claimed.lease_owner,
                                "attempt_no": claimed.attempt_no,
                            },
                        }
                    if payload.get("work_id") is None or payload.get("claim_token") is None:
                        raise ForestError(
                            "PROVISIONING_WORK_INPUT_INVALID", "Work identity is missing", 422
                        )
                    claimed = await self.jobs.held_claim(
                        lease, job_id=payload["work_id"],
                        claim_token=payload["claim_token"],
                        attempt_no=payload["attempt"], conn=conn,
                    )
                    if phase == "renew":
                        if payload.get("protocol") != "forest.owned-work-renew-request/1":
                            raise ForestError(
                                "PROVISIONING_WORK_INPUT_INVALID", "Renew input is invalid", 422
                            )
                        await self.jobs.renew(
                            lease, claimed, lease_seconds=payload["lease_seconds"], conn=conn
                        )
                        return {
                            "protocol": "forest.owned-work-renewed/1",
                            "work_id": claimed.job_id,
                        }
                    if payload.get("protocol") != "forest.owned-work-settle-request/1":
                        raise ForestError(
                            "PROVISIONING_WORK_INPUT_INVALID", "Settle input is invalid", 422
                        )
                    outcome = payload["outcome"]
                    if outcome["status"] == "SUCCEEDED":
                        if outcome["result"] != {
                            "bot_id": claimed.bot_id, "materialized": True
                        }:
                            raise ForestError(
                                "PROVISIONING_WORK_RESULT_INVALID",
                                "Materialization result differs from the claimed Bot", 409,
                            )
                        await self.jobs.complete_ready(
                            lease, claimed, catalog=self.catalog, conn=conn
                        )
                    elif outcome["status"] == "FAILED":
                        error = outcome["error"]
                        if error["unknown"]:
                            await self.jobs.block_unknown(
                                lease, claimed, error_code=error["code"], conn=conn
                            )
                        else:
                            await self.jobs.fail(
                                lease, claimed, error_code=error["code"],
                                retryable=error["retryable"], conn=conn,
                            )
                    else:
                        raise ForestError(
                            "PROVISIONING_WORK_RESULT_INVALID", "Work outcome is invalid", 422
                        )
                    return {
                        "protocol": "forest.owned-work-settled/1",
                        "work_id": claimed.job_id,
                    }
        except TimeoutError as exc:
            raise ForestError(
                "PROVISIONING_AUTHORITY_TIMEOUT",
                "Provisioning Job authority timed out", 504,
            ) from exc

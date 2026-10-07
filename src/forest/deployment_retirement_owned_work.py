"""Bind the retirement package Generation to the kernel's Bot retirement Job."""

from __future__ import annotations

import asyncio
import hashlib

from forest.errors import ForestError


class DeploymentRetirementOwnedWorkAuthority:
    def __init__(self, runtime, jobs, catalog, bridge) -> None:
        if jobs.store is not runtime.store or catalog.store is not runtime.store:
            raise ValueError("Retirement authorities differ from deployment")
        self.runtime = runtime
        self.jobs = jobs
        self.catalog = catalog
        self.bridge = bridge

    async def invoke_owned_work(
        self, phase, scope, declaration, payload, *, timeout_seconds,
    ):
        if (
            scope.scope_kind != "deployment"
            or scope.scope_id != self.runtime.scope_id
            or scope.component_id != "deployment.retirement"
            or declaration.get("authority") != "kernel"
            or declaration.get("name") != "deployment.retirement.execute"
            or phase not in {"claim", "renew", "settle"}
        ):
            raise ForestError(
                "RETIREMENT_WORK_BINDING_INVALID",
                "Retirement work differs from its Generation binding", 403,
            )
        try:
            async with asyncio.timeout(timeout_seconds):
                if phase == "claim":
                    return await self._claim(scope, payload)
                if phase == "renew":
                    return await self._renew(scope, payload)
                return await self._settle(scope, payload)
        except TimeoutError as exc:
            raise ForestError(
                "RETIREMENT_AUTHORITY_TIMEOUT", "Retirement authority timed out", 504,
            ) from exc

    async def _claim(self, scope, payload):
        if payload.get("protocol") != "forest.owned-work-claim-request/1":
            raise ForestError("RETIREMENT_WORK_INPUT_INVALID", "Claim input is invalid", 422)
        async with self.runtime.store.transaction() as conn:
            lease = self.runtime.authority_lease()
            await self._verify_generation(conn, lease, scope, claim_new=True)
            claim = await self.jobs.claim(
                lease, worker_id=payload["claim_token"],
                lease_seconds=payload["lease_seconds"], conn=conn,
            )
        if claim is None:
            return None
        digest = hashlib.sha256(
            (claim.deployment_id + "/" + claim.operation_id + "/"
             + str(claim.attempt_no)).encode("utf-8")
        ).hexdigest()
        return {
            "protocol": "forest.owned-work-claim/1",
            "work_id": claim.operation_id,
            "claim_token": claim.lease_owner,
            "attempt": claim.attempt_no,
            "operation_id": "retire:" + digest,
            "causal_root_kind": "deployment.retirement",
            "causal_root_id": claim.operation_id,
            "payload": {
                "operation_id": claim.operation_id,
                "bot_id": claim.bot_id,
                "claim_token": claim.lease_owner,
                "attempt_no": claim.attempt_no,
            },
        }

    async def _renew(self, scope, payload):
        if payload.get("protocol") != "forest.owned-work-renew-request/1":
            raise ForestError("RETIREMENT_WORK_INPUT_INVALID", "Renew input is invalid", 422)
        async with self.runtime.store.transaction() as conn:
            lease = self.runtime.authority_lease()
            await self._verify_generation(conn, lease, scope, claim_new=False)
            claim = await self.jobs.held_claim(
                lease, operation_id=payload["work_id"],
                claim_token=payload["claim_token"],
                attempt_no=payload["attempt"], conn=conn,
            )
            await self.jobs.renew(
                lease, claim, lease_seconds=payload["lease_seconds"], conn=conn,
            )
        return {
            "protocol": "forest.owned-work-renewed/1",
            "work_id": claim.operation_id,
        }

    async def _settle(self, scope, payload):
        if payload.get("protocol") != "forest.owned-work-settle-request/1":
            raise ForestError("RETIREMENT_WORK_INPUT_INVALID", "Settle input is invalid", 422)
        outcome = payload["outcome"]
        async with self.runtime.store.transaction() as conn:
            lease = self.runtime.authority_lease()
            await self._verify_generation(conn, lease, scope, claim_new=False)
            claim = await self.jobs.held_claim(
                lease, operation_id=payload["work_id"],
                claim_token=payload["claim_token"],
                attempt_no=payload["attempt"], conn=conn,
            )
        result = outcome.get("result") if outcome.get("status") == "SUCCEEDED" else None
        if result is not None:
            if result.get("bot_id") != claim.bot_id:
                raise ForestError(
                    "RETIREMENT_WORK_RESULT_INVALID", "Result differs from Bot claim", 409,
                )
            if result.get("state") == "READY_TO_PUBLISH":
                request_id = result.get("request_id")
                resource_id = result.get("resource_id")
                if not isinstance(request_id, str) or not isinstance(resource_id, str):
                    raise ForestError(
                        "RETIREMENT_WORK_RESULT_INVALID", "Host identity is missing", 409,
                    )
                await self._host_completion(request_id, resource_id)
        async with self.runtime.store.transaction() as conn:
            lease = self.runtime.authority_lease()
            await self._verify_generation(conn, lease, scope, claim_new=False)
            claim = await self.jobs.held_claim(
                lease, operation_id=payload["work_id"],
                claim_token=payload["claim_token"],
                attempt_no=payload["attempt"], conn=conn,
            )
            if result is not None:
                if result["state"] == "READY_TO_PUBLISH":
                    await self.jobs.complete(
                        lease, claim, catalog=self.catalog,
                        request_id=request_id, resource_id=resource_id, conn=conn,
                    )
                elif result["state"] == "WAITING":
                    await self.jobs.retry(
                        lease, claim, error_code=result.get("error_code", "RETIREMENT_WAITING"),
                        retry_seconds=10, conn=conn,
                    )
                elif result["state"] == "BLOCKED":
                    await self.jobs.block_unknown(
                        lease, claim, error_code=result["error_code"], conn=conn,
                    )
                else:
                    raise ForestError(
                        "RETIREMENT_WORK_RESULT_INVALID", "Result state is invalid", 422,
                    )
            elif outcome.get("status") == "FAILED":
                error = outcome["error"]
                code = error["code"]
                if error["unknown"] or error["retryable"]:
                    await self.jobs.retry(
                        lease, claim, error_code=code, retry_seconds=10, conn=conn,
                    )
                else:
                    await self.jobs.block_unknown(
                        lease, claim, error_code=code, conn=conn,
                    )
            else:
                raise ForestError(
                    "RETIREMENT_WORK_RESULT_INVALID", "Work outcome is invalid", 422,
                )
        return {
            "protocol": "forest.owned-work-settled/1",
            "work_id": claim.operation_id,
        }

    async def _host_completion(self, request_id: str, resource_id: str) -> None:
        released = await self.bridge.release_reference(request_id)
        work = await self.bridge.reclamation_status(request_id)
        resource = await self.bridge.resource_status(request_id)
        if (
            released["resource_id"] != resource_id
            or released["state"] != "RETIRED"
            or work["resource_id"] != resource_id
            or work["state"] != "SUCCEEDED"
            or resource["resource_id"] != resource_id
            or resource["state"] != "DELETED"
            or resource["revision"] != released["resource_revision"] + 1
        ):
            raise ForestError(
                "RETIREMENT_HOST_PROOF_UNAVAILABLE",
                "Host has not completed this exact reclamation", 409,
            )

    async def _verify_generation(self, conn, lease, scope, *, claim_new: bool):
        await self.runtime.generation_authority.verify_owned_work_in(
            conn, lease, component_id=scope.component_id,
            generation_id=scope.generation_id, claim_new=claim_new,
        )

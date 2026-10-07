"""Bind the storage orchestrator Generation to deployment-owned delivery leases."""

from __future__ import annotations

import asyncio
import hashlib

from forest.errors import ForestError
from forest.storage.deployment_storage_dispatch import StorageDispatchClaim


class DeploymentStorageOwnedWorkAuthority:
    def __init__(self, runtime, dispatch) -> None:
        if dispatch.store is not runtime.store:
            raise ValueError("Storage dispatch belongs to another deployment")
        self.runtime = runtime
        self.dispatch = dispatch

    async def invoke_owned_work(self, phase, scope, declaration, payload, *, timeout_seconds):
        if (
            scope.scope_kind != "deployment"
            or scope.scope_id != self.runtime.scope_id
            or scope.component_id != "deployment.storage-orchestrator"
            or declaration.get("authority") != "kernel"
            or declaration.get("name") != "deployment.storage-orchestrator.execute"
            or phase not in {"claim", "renew", "settle"}
        ):
            raise ForestError(
                "STORAGE_WORK_BINDING_INVALID",
                "Storage work differs from its Generation binding",
                403,
            )
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self.runtime.store.transaction() as conn:
                    lease = self.runtime.authority_lease()
                    await self.runtime.generation_authority.verify_owned_work_in(
                        conn,
                        lease,
                        component_id=scope.component_id,
                        generation_id=scope.generation_id,
                        claim_new=phase == "claim",
                    )
                    if phase == "claim":
                        if payload.get("protocol") != "forest.owned-work-claim-request/1":
                            raise ForestError(
                                "STORAGE_WORK_INPUT_INVALID",
                                "Claim input is invalid",
                                422,
                            )
                        claim = await self.dispatch.claim(
                            lease,
                            lease_seconds=payload["lease_seconds"],
                            conn=conn,
                        )
                        if claim is None:
                            return None
                        digest = hashlib.sha256(
                            (
                                self.runtime.scope_id
                                + "/"
                                + claim.request_id
                                + "/"
                                + str(claim.attempt_no)
                            ).encode("utf-8")
                        ).hexdigest()
                        return {
                            "protocol": "forest.owned-work-claim/1",
                            "work_id": claim.request_id,
                            "claim_token": claim.claim_token,
                            "attempt": claim.attempt_no,
                            "operation_id": "storage-advance:" + digest,
                            "causal_root_kind": "deployment.storage-request",
                            "causal_root_id": claim.request_id,
                            "payload": {
                                "request_id": claim.request_id,
                                "claim_token": claim.claim_token,
                                "attempt_no": claim.attempt_no,
                            },
                        }
                    if (
                        payload.get("work_id") is None
                        or payload.get("claim_token") is None
                        or type(payload.get("attempt")) is not int
                    ):
                        raise ForestError(
                            "STORAGE_WORK_INPUT_INVALID",
                            "Work identity is missing",
                            422,
                        )
                    claim = StorageDispatchClaim(
                        payload["work_id"],
                        payload["claim_token"],
                        lease.owner_epoch,
                        payload["attempt"],
                    )
                    if phase == "renew":
                        if payload.get("protocol") != "forest.owned-work-renew-request/1":
                            raise ForestError(
                                "STORAGE_WORK_INPUT_INVALID",
                                "Renew input is invalid",
                                422,
                            )
                        await self.dispatch.renew(
                            lease,
                            claim,
                            lease_seconds=payload["lease_seconds"],
                            conn=conn,
                        )
                        return {
                            "protocol": "forest.owned-work-renewed/1",
                            "work_id": claim.request_id,
                        }
                    if payload.get("protocol") != "forest.owned-work-settle-request/1":
                        raise ForestError(
                            "STORAGE_WORK_INPUT_INVALID",
                            "Settle input is invalid",
                            422,
                        )
                    outcome = payload["outcome"]
                    if outcome["status"] == "SUCCEEDED":
                        result = outcome["result"]
                        if (
                            result["request_id"] != claim.request_id
                            or (result["state"] == "BLOCKED" and "error_code" not in result)
                            or (result["state"] == "SETTLED" and "error_code" in result)
                        ):
                            raise ForestError(
                                "STORAGE_WORK_RESULT_INVALID",
                                "Storage result differs from the claimed request",
                                409,
                            )
                        await self.dispatch.settle(
                            lease,
                            claim,
                            outcome=result["state"],
                            error_code=result.get("error_code"),
                            conn=conn,
                        )
                    elif outcome["status"] == "FAILED":
                        error = outcome["error"]
                        await self.dispatch.settle(
                            lease,
                            claim,
                            outcome=(
                                "WAITING" if error["unknown"] or error["retryable"] else "BLOCKED"
                            ),
                            error_code=error["code"],
                            conn=conn,
                        )
                    else:
                        raise ForestError(
                            "STORAGE_WORK_RESULT_INVALID",
                            "Work outcome is invalid",
                            422,
                        )
                    return {
                        "protocol": "forest.owned-work-settled/1",
                        "work_id": claim.request_id,
                    }
        except TimeoutError as exc:
            raise ForestError(
                "STORAGE_WORK_AUTHORITY_TIMEOUT",
                "Storage work authority timed out",
                504,
            ) from exc

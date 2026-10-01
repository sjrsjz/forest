"""Bot-wide admission of declared Python Worker capacity owned by kernel.runtime.

These are sums of Worker IPC and concurrency claims, not operating-system CPU or
resident-memory limits. The same policy is read from kernel.runtime's versioned
configuration for preview, commit, and actual Worker preparation.
"""

from __future__ import annotations

from dataclasses import dataclass

from forest.errors import ForestError
from forest.runtime_modules import RuntimeWorkerCapacity

_FIELDS = (
    "max_workers",
    "max_frame_bytes",
    "max_inflight",
    "max_queue_items",
    "max_state_inflight",
    "max_owned_tasks",
    "max_owned_resources",
)

KERNEL_RESOURCE_POLICY_DEFAULT = {
    "protocol": "forest.kernel-resource-policy/1",
    "worker_budget": {
        "max_workers": 64,
        "max_frame_bytes": 16 * 1024 * 1024,
        "max_inflight": 1024,
        "max_queue_items": 8192,
        "max_state_inflight": 512,
        "max_owned_tasks": 1024,
        "max_owned_resources": 512,
    },
}

KERNEL_RESOURCE_POLICY_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.kernel-resource-policy/1"},
        "worker_budget": {
            "type": "object",
            "properties": {
                field: {"type": "integer", "minimum": 1, "maximum": 1000000000}
                for field in _FIELDS
            },
            "required": list(_FIELDS),
            "additionalProperties": False,
        },
    },
    "required": ["protocol", "worker_budget"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class WorkerCapacityAdmission:
    policy_revision: int
    policy_payload_digest: str
    budget: dict[str, int]
    peak: dict[str, int]

    def as_fact(self) -> dict:
        return {
            "protocol": "forest.worker-capacity-admission/1",
            "policy_revision": self.policy_revision,
            "policy_payload_digest": self.policy_payload_digest,
            "budget": self.budget,
            "peak": self.peak,
        }


def validate_kernel_resource_policy(value: object) -> dict:
    if (
        type(value) is not dict
        or set(value) != {"protocol", "worker_budget"}
        or value["protocol"] != "forest.kernel-resource-policy/1"
        or type(value["worker_budget"]) is not dict
        or set(value["worker_budget"]) != set(_FIELDS)
        or any(
            type(value["worker_budget"][field]) is not int
            or not 1 <= value["worker_budget"][field] <= 1000000000
            for field in _FIELDS
        )
    ):
        raise ValueError("Kernel Worker capacity policy has an invalid shape")
    return value


def assess_worker_capacity(
    capacity: RuntimeWorkerCapacity,
    *,
    policy_revision: int,
    policy_payload_digest: str,
    policy: object,
) -> WorkerCapacityAdmission:
    budget = validate_kernel_resource_policy(policy)["worker_budget"]
    peak = {field: 0 for field in _FIELDS}
    for _, limits in capacity.peak:
        peak["max_workers"] += 1
        for field in _FIELDS[1:]:
            peak[field] += getattr(limits, field)
    exceeded = tuple(field for field in _FIELDS if peak[field] > budget[field])
    if exceeded:
        raise ForestError(
            "KERNEL_WORKER_CAPACITY_EXCEEDED",
            "Candidate Worker capacity exceeds the Bot policy",
            409,
            details={
                "policy_revision": policy_revision,
                "exceeded": list(exceeded),
                "peak": peak,
                "budget": budget,
            },
        )
    return WorkerCapacityAdmission(
        policy_revision, policy_payload_digest, dict(budget), peak
    )


async def assess_worker_capacity_in(kernel, conn, capacity) -> WorkerCapacityAdmission:
    desired = await kernel.configurations["kernel.runtime"].desired_checked_in(conn)
    return assess_worker_capacity(
        capacity,
        policy_revision=desired.revision,
        policy_payload_digest=desired.payload_digest,
        policy=desired.value,
    )

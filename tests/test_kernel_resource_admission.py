from __future__ import annotations

import pytest

from forest.errors import ForestError
from forest.kernel_resource_admission import (
    KERNEL_RESOURCE_POLICY_DEFAULT,
    assess_worker_capacity,
    validate_kernel_resource_policy,
)
from forest.runtime_modules import RuntimeWorkerCapacity
from forest.worker_limits import WorkerLimits


def test_replacement_capacity_counts_old_and_new_workers_before_drain():
    old = WorkerLimits(max_inflight=5, max_queue_items=2)
    new = WorkerLimits(max_inflight=8, max_queue_items=3)
    capacity = RuntimeWorkerCapacity(
        active=(("agent.runtime", old),),
        preparing=(("agent.runtime", new),),
    )
    policy = {
        "protocol": "forest.kernel-resource-policy/1",
        "worker_budget": {
            **KERNEL_RESOURCE_POLICY_DEFAULT["worker_budget"],
            "max_workers": 2,
            "max_inflight": 13,
        },
    }
    admitted = assess_worker_capacity(
        capacity, policy_revision=3, policy_payload_digest="sha256:" + "a" * 64,
        policy=policy,
    )
    assert admitted.peak["max_workers"] == 2
    assert admitted.peak["max_inflight"] == 13
    assert admitted.policy_revision == 3

    policy["worker_budget"]["max_inflight"] = 12
    with pytest.raises(ForestError) as rejected:
        assess_worker_capacity(
            capacity, policy_revision=4, policy_payload_digest="sha256:" + "b" * 64,
            policy=policy,
        )
    assert rejected.value.code == "KERNEL_WORKER_CAPACITY_EXCEEDED"
    assert rejected.value.details["exceeded"] == ["max_inflight"]


def test_resource_policy_rejects_boolean_budget():
    policy = {
        "protocol": "forest.kernel-resource-policy/1",
        "worker_budget": {
            **KERNEL_RESOURCE_POLICY_DEFAULT["worker_budget"],
            "max_workers": True,
        },
    }
    with pytest.raises(ValueError):
        validate_kernel_resource_policy(policy)

"""Host Shell pre-admission is bounded and keeps P0 capacity independent."""

import asyncio
from dataclasses import replace

import pytest

from forest.errors import ForestError
from forest.shell_ingress import IngressPlan, ShellIngressGuard


def binding(*, epoch=3, revision=7, admins=("admin",)):
    return {
        "binding_id": "binding-a",
        "binding_epoch": epoch,
        "effective_revision": revision,
        "config_payload_digest": f"digest-{revision}",
        "config_payload_state": "ACTIVE",
        "config": {"admin_user_ids": list(admins)},
    }


def message(text, *, user="admin", principal=None, anonymous=None, group="group-a"):
    author = {
        "external_user_id": user,
        "display_name": "user",
        "is_self": False,
        "identity_origin": "adapter_anonymous" if anonymous is not None else "adapter_verified",
        "principal_ref": principal or f"principal:{user}",
    }
    if anonymous is not None:
        author["anonymous"] = anonymous
    return {
        "platform": "onebot-v11",
        "self_id": "1000",
        "channel_kind": "group",
        "channel_id": group,
        "thread_id": "",
        "author": author,
        "segments": [{"type": "text", "text": text}],
    }


def test_shell_preclassification_uses_three_lanes_and_revision_scoped_cache():
    guard = ShellIngressGuard("bot-a")
    ordinary = guard.classify(binding(), message("hello"))
    p0 = guard.classify(binding(), message("#forest core status"))
    denied = guard.classify(binding(), message("#forest core status", user="stranger"))
    p1 = guard.classify(binding(), message("#forest help"))
    assert ordinary == IngressPlan("ORDINARY")
    assert p0.lane == "PRIVILEGED_CONTROL" and p0.proof.priority == 0
    assert denied.lane == "REJECTION" and denied.proof.priority == 1
    assert p1.lane == "ORDINARY" and p1.proof.requested_priority == 1

    guard.classify(binding(), message("#forest core status"))
    guard.classify(binding(epoch=4), message("#forest core status"))
    guard.classify(binding(revision=8), message("#forest core status"))
    stats = guard.snapshot()["qualification_cache"]
    assert stats == {"hits": 1, "misses": 5, "entries": 5}


def test_anonymous_rejections_share_a_group_bucket_across_event_principals():
    guard = ShellIngressGuard("bot-a")
    first = guard.classify(
        binding(),
        message(
            "#forest core status",
            user="anonymous",
            principal="anonymous:event-1",
            anonymous={"name": "same"},
        ),
    )
    second = guard.classify(
        binding(),
        message(
            "#forest core status",
            user="anonymous",
            principal="anonymous:event-2",
            anonymous={"name": "same"},
        ),
    )
    assert first.rejection_bucket == second.rejection_bucket
    assert first.proof.principal_ref != second.proof.principal_ref
    assert guard.snapshot()["qualification_cache"] == {
        "hits": 1,
        "misses": 1,
        "entries": 1,
    }


async def test_full_ordinary_lane_does_not_delay_privileged_control():
    guard = ShellIngressGuard(
        "bot-a", ordinary_concurrency=1, ordinary_wait=0.03, privileged_wait=0.03
    )
    ordinary = IngressPlan("ORDINARY")
    privileged = guard.classify(binding(), message("#forest core status"))
    started = asyncio.Event()

    async def blocked_ordinary():
        started.set()
        with pytest.raises(ForestError) as caught:
            async with guard.enter(ordinary):
                raise AssertionError("ordinary capacity unexpectedly borrowed")
        assert caught.value.code == "INGRESS_OVERLOADED"

    async with guard.enter(ordinary):
        task = asyncio.create_task(blocked_ordinary())
        await started.wait()
        async with guard.enter(privileged) as proof:
            assert proof is privileged.proof
        await task
    stats = guard.snapshot()["lanes"]
    assert stats["ORDINARY"] == {"active": 0, "accepted": 1, "overloaded": 1}
    assert stats["PRIVILEGED_CONTROL"] == {
        "active": 0,
        "accepted": 1,
        "overloaded": 0,
    }


async def test_rejection_bucket_fails_before_work_and_releases_capacity():
    guard = ShellIngressGuard("bot-a", rejection_burst=1, rejection_refill_seconds=60)
    plan = guard.classify(binding(), message("#forest help", user="stranger"))
    async with guard.enter(plan) as proof:
        assert proof is plan.proof
    with pytest.raises(ForestError) as caught:
        async with guard.enter(replace(plan)):
            raise AssertionError("rate-limited work was admitted")
    assert caught.value.code == "SHELL_REJECTION_OVERLOADED"
    assert caught.value.status == 429
    stats = guard.snapshot()
    assert stats["lanes"]["REJECTION"] == {
        "active": 0,
        "accepted": 1,
        "overloaded": 1,
    }
    assert stats["rejection_budget"] == {"rate_limited": 1, "buckets": 1}

"""Bounded, host-only admission lanes for signed platform input."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from forest.errors import ForestError
from forest.shell_authority import P0_PRIORITY_POLICY, is_shell_admin, qualify_priority
from forest.shell_commands import ShellCommand, parse_command

IngressLane = Literal["ORDINARY", "PRIVILEGED_CONTROL", "REJECTION"]


@dataclass(frozen=True)
class ShellPriorityProof:
    """Process-local evidence used only to persist an admission-time priority."""

    bot_id: str
    binding_id: str
    binding_epoch: int
    config_revision: int
    config_payload_digest: str | None
    principal_ref: str
    command_name: str
    gate_version: str
    requested_priority: int
    priority: int
    priority_eligible: bool
    priority_policy: str = P0_PRIORITY_POLICY

    def persistence(self) -> dict[str, Any]:
        return {
            "requested_priority": self.requested_priority,
            "priority": self.priority,
            "priority_eligible": self.priority_eligible,
            "priority_policy": self.priority_policy,
            "priority_basis_binding_epoch": self.binding_epoch,
            "priority_basis_config_revision": self.config_revision,
        }


@dataclass(frozen=True)
class IngressPlan:
    lane: IngressLane
    proof: ShellPriorityProof | None = None
    rejection_bucket: tuple[Any, ...] | None = None


@dataclass
class _Bucket:
    tokens: float
    updated_at: float


class ShellIngressGuard:
    """Keep P0 control capacity separate before any BotStore connection is used.

    The cache is only an availability hint. MessageStore binds its proof to the
    current Binding row, and ShellDispatcher authorizes the command again when
    execution starts.
    """

    def __init__(
        self,
        bot_id: str,
        *,
        ordinary_concurrency: int = 4,
        privileged_concurrency: int = 2,
        rejection_concurrency: int = 1,
        ordinary_wait: float = 10,
        privileged_wait: float = 2,
        rejection_wait: float = 0.05,
        cache_ttl: float = 5,
        cache_size: int = 512,
        rejection_burst: int = 4,
        rejection_refill_seconds: float = 5,
        bucket_size: int = 2048,
    ) -> None:
        limits = (ordinary_concurrency, privileged_concurrency, rejection_concurrency)
        if any(type(value) is not int or value < 1 for value in limits):
            raise ValueError("Ingress concurrency must be positive")
        if any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0
            for value in (ordinary_wait, privileged_wait, rejection_wait, cache_ttl,
                          rejection_refill_seconds)
        ):
            raise ValueError("Ingress timing limits must be positive")
        if any(type(value) is not int or value < 1
               for value in (cache_size, rejection_burst, bucket_size)):
            raise ValueError("Ingress collection limits must be positive")
        self.bot_id = bot_id
        self._semaphores = {
            "ORDINARY": asyncio.Semaphore(ordinary_concurrency),
            "PRIVILEGED_CONTROL": asyncio.Semaphore(privileged_concurrency),
            "REJECTION": asyncio.Semaphore(rejection_concurrency),
        }
        self._wait = {
            "ORDINARY": float(ordinary_wait),
            "PRIVILEGED_CONTROL": float(privileged_wait),
            "REJECTION": float(rejection_wait),
        }
        self._cache_ttl = float(cache_ttl)
        self._cache_size = cache_size
        self._cache: OrderedDict[tuple[Any, ...], tuple[float, dict[str, Any]]] = OrderedDict()
        self._rejection_burst = rejection_burst
        self._rejection_refill_seconds = float(rejection_refill_seconds)
        self._bucket_size = bucket_size
        self._buckets: OrderedDict[tuple[Any, ...], _Bucket] = OrderedDict()
        self._active = {lane: 0 for lane in self._semaphores}
        self._accepted = {lane: 0 for lane in self._semaphores}
        self._overloaded = {lane: 0 for lane in self._semaphores}
        self._cache_hits = 0
        self._cache_misses = 0
        self._rejection_rate_limited = 0

    @staticmethod
    def _principal_bucket(binding_id: str, event: dict) -> tuple[Any, ...]:
        author = event["author"]
        if author.get("anonymous") is not None:
            return (
                "anonymous",
                binding_id,
                event["platform"],
                event["channel_kind"],
                event["channel_id"],
            )
        return ("verified", author["principal_ref"])

    def _qualification(
        self,
        binding: dict,
        event: dict,
        command: ShellCommand,
        bucket: tuple[Any, ...],
    ) -> dict[str, Any]:
        key = (
            binding["binding_id"],
            binding["binding_epoch"],
            binding["effective_revision"],
            bucket,
            command.name,
            command.priority,
        )
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached is not None and cached[0] > now:
            self._cache.move_to_end(key)
            self._cache_hits += 1
            return dict(cached[1])
        if cached is not None:
            del self._cache[key]
        self._cache_misses += 1
        config = binding.get("config") if binding.get("config_payload_state") == "ACTIVE" else None
        value = {
            **qualify_priority(command.priority, event["author"], config),
            "is_current_admin": is_shell_admin(event["author"], config),
        }
        self._cache[key] = (now + self._cache_ttl, value)
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return dict(value)

    def classify(self, binding: dict, event: dict | None) -> IngressPlan:
        if event is None:
            return IngressPlan("ORDINARY")
        command = parse_command(event)
        if command is None:
            return IngressPlan("ORDINARY")
        bucket = self._principal_bucket(binding["binding_id"], event)
        qualified = self._qualification(binding, event, command, bucket)
        proof = ShellPriorityProof(
            bot_id=self.bot_id,
            binding_id=binding["binding_id"],
            binding_epoch=binding["binding_epoch"],
            config_revision=binding["effective_revision"],
            config_payload_digest=binding.get("config_payload_digest"),
            principal_ref=event["author"]["principal_ref"],
            command_name=command.name,
            gate_version=command.gate_version,
            requested_priority=command.priority,
            priority=qualified["priority"],
            priority_eligible=qualified["priority_eligible"],
        )
        if not qualified["is_current_admin"]:
            return IngressPlan("REJECTION", proof, bucket)
        if command.priority == 0:
            return IngressPlan("PRIVILEGED_CONTROL", proof)
        return IngressPlan("ORDINARY", proof)

    def _consume_rejection(self, key: tuple[Any, ...]) -> bool:
        now = time.monotonic()
        bucket = self._buckets.pop(key, None)
        if bucket is None:
            bucket = _Bucket(float(self._rejection_burst), now)
        else:
            bucket.tokens = min(
                float(self._rejection_burst),
                bucket.tokens + (now - bucket.updated_at) / self._rejection_refill_seconds,
            )
            bucket.updated_at = now
        allowed = bucket.tokens >= 1
        if allowed:
            bucket.tokens -= 1
        self._buckets[key] = bucket
        while len(self._buckets) > self._bucket_size:
            self._buckets.popitem(last=False)
        return allowed

    @asynccontextmanager
    async def enter(self, plan: IngressPlan):
        lane = plan.lane
        semaphore = self._semaphores[lane]
        acquired = False
        try:
            try:
                async with asyncio.timeout(self._wait[lane]):
                    await semaphore.acquire()
                acquired = True
            except TimeoutError:
                self._overloaded[lane] += 1
                code = {
                    "ORDINARY": "INGRESS_OVERLOADED",
                    "PRIVILEGED_CONTROL": "CONTROL_INGRESS_OVERLOADED",
                    "REJECTION": "SHELL_REJECTION_OVERLOADED",
                }[lane]
                raise ForestError(code, "Input admission capacity is full", 429) from None
            self._active[lane] += 1
            if lane == "REJECTION":
                if plan.rejection_bucket is None or not self._consume_rejection(
                    plan.rejection_bucket
                ):
                    self._overloaded[lane] += 1
                    self._rejection_rate_limited += 1
                    raise ForestError(
                        "SHELL_REJECTION_OVERLOADED",
                        "Rejected Shell input exceeded its admission budget",
                        429,
                    )
            self._accepted[lane] += 1
            yield plan.proof
        finally:
            if acquired:
                self._active[lane] -= 1
                semaphore.release()

    def snapshot(self) -> dict[str, Any]:
        return {
            "lanes": {
                lane: {
                    "active": self._active[lane],
                    "accepted": self._accepted[lane],
                    "overloaded": self._overloaded[lane],
                }
                for lane in self._semaphores
            },
            "qualification_cache": {
                "hits": self._cache_hits,
                "misses": self._cache_misses,
                "entries": len(self._cache),
            },
            "rejection_budget": {
                "rate_limited": self._rejection_rate_limited,
                "buckets": len(self._buckets),
            },
        }

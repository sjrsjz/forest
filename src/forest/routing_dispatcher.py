"""Owned-work dispatcher for durable component Event and ComponentJob queues."""

from __future__ import annotations

import asyncio
import hashlib
import json

from forest.errors import ForestError
from forest.routing_protocol import DurableComponentJobRequest, DurableEventDeliveryRequest


class RoutingDispatcher:
    def __init__(
        self,
        *,
        event_commands,
        job_commands,
        payloads,
        event_subscriptions,
        component_jobs,
        subscription_routes,
        component_job_routes,
    ) -> None:
        self.event_commands = event_commands
        self.job_commands = job_commands
        self.payloads = payloads
        self._event_subscriptions = event_subscriptions
        self._component_jobs = component_jobs
        self._subscription_routes = subscription_routes
        self._component_job_routes = component_job_routes
        self.generation_identity = None
        self.failure: str | None = None
        self._closing = False
        self._claims_open = False
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._dispatch_idle = asyncio.Event()
        self._dispatch_idle.set()

    async def start(self, generation_identity) -> None:
        if self.generation_identity is not None:
            raise ForestError(
                "RUNTIME_MODULE_LIFECYCLE_INVALID",
                "Routing dispatcher is already started",
                409,
            )
        self.generation_identity = generation_identity
        self._task = asyncio.create_task(
            self._run(),
            name="routing-dispatcher/" + generation_identity.generation_id,
        )

    def notify(self) -> None:
        self._wake.set()

    def open_admission(self) -> None:
        self._claims_open = True
        self._wake.set()

    def freeze_admission(self) -> None:
        self._claims_open = False
        self._wake.set()

    async def drain(self) -> None:
        await self._dispatch_idle.wait()

    async def close(self) -> None:
        self._claims_open = False
        self._closing = True
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def owned_tasks(self) -> tuple:
        return (self._task,) if self._task is not None else ()

    async def _wait(self) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=0.25)
        except TimeoutError:
            pass
        self._wake.clear()

    async def _run(self) -> None:
        try:
            while not self._closing:
                if not self._claims_open:
                    await self._wait()
                    continue
                identity = self.generation_identity
                if identity is None:
                    raise ForestError(
                        "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                        "Routing dispatcher has no Generation",
                        503,
                    )
                self._dispatch_idle.clear()
                claimed = False
                try:
                    event = await self.event_commands.claim_event_delivery(
                        routing_generation_id=identity.generation_id,
                        routes=self._subscription_routes(),
                    )
                    if event is not None:
                        claimed = True
                        await self._dispatch_event(event)
                    elif self._claims_open:
                        job = await self.job_commands.claim_component_job(
                            routing_generation_id=identity.generation_id,
                            routes=self._component_job_routes(),
                        )
                        if job is not None:
                            claimed = True
                            await self._dispatch_job(job)
                finally:
                    self._dispatch_idle.set()
                if not claimed:
                    await self._wait()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            raise
        except Exception:
            self.failure = "ROUTING_DISPATCHER_FAILED"
            raise

    async def _payload(self, payload_id: str, expected_digest: str):
        body = await self.payloads.read_bytes(payload_id)
        if "sha256:" + hashlib.sha256(body).hexdigest() != expected_digest:
            raise ForestError(
                "ROUTING_PAYLOAD_DIGEST_MISMATCH",
                "Managed routing payload digest differs",
                500,
            )
        try:
            return json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "ROUTING_PAYLOAD_INVALID", "Managed routing payload is not JSON", 500
            ) from exc

    async def _dispatch_event(self, claim) -> None:
        route = self._event_subscriptions.get(
            (claim.subscriber_id, int(claim.contract.split(".", 1)[0]))
        )
        if route is None:
            await self._settle_event_error(
                claim, "EVENT_ROUTE_UNAVAILABLE", retryable=False
            )
            return
        owner, contribution, service = route
        frozen = service.route
        if (
            owner != frozen.provider_component_id
            or frozen.provider_generation_id != claim.target_generation_id
            or frozen.service_id != claim.handler_service_id
            or frozen.version != claim.handler_service_version
            or contribution.topic != claim.topic
            or contribution.contract != claim.contract
        ):
            await self._settle_event_error(
                claim, "EVENT_ROUTE_STALE", retryable=False
            )
            return
        try:
            payload = await self._payload(claim.payload_id, claim.payload_digest)
            result = await service.invoke(
                DurableEventDeliveryRequest(
                    claim.event_id,
                    claim.delivery_id,
                    claim.subscriber_id,
                    claim.topic,
                    claim.contract,
                    claim.publisher_component_id,
                    claim.publisher_generation_id,
                    claim.attempt_no,
                    payload,
                ),
                operation_id=claim.attempt_id,
                causal_root_kind=claim.causal_root_kind,
                causal_root_id=claim.causal_root_id,
                timeout_seconds=30,
            )
            if result != {"acknowledged": True}:
                raise ForestError(
                    "EVENT_ACK_INVALID", "Event handler returned no acknowledgement", 502
                )
            await self.event_commands.succeed_event_delivery(claim)
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            if exc.code == "STALE_ROUTING_CLAIM":
                return
            await self._settle_event_error(
                claim,
                exc.code,
                retryable=exc.status >= 500,
                unknown=exc.code == "SERVICE_OPERATION_UNKNOWN",
            )
        except Exception:
            await self._settle_event_error(
                claim, "EVENT_DISPATCH_UNKNOWN", retryable=False, unknown=True
            )

    async def _settle_event_error(
        self, claim, error_code: str, *, retryable: bool, unknown: bool = False
    ) -> None:
        try:
            await self.event_commands.fail_event_delivery(
                claim,
                error_code=error_code,
                retryable=retryable,
                unknown=unknown,
            )
        except ForestError as exc:
            if exc.code != "STALE_ROUTING_CLAIM":
                raise

    async def _dispatch_job(self, claim) -> None:
        route = self._component_jobs.get(
            (claim.owner_component_id, claim.handler_id)
        )
        if route is None:
            await self._settle_job_error(
                claim, "COMPONENT_JOB_ROUTE_UNAVAILABLE", retryable=False
            )
            return
        contribution, service = route
        frozen = service.route
        if (
            frozen.provider_component_id != claim.owner_component_id
            or frozen.provider_generation_id != claim.target_generation_id
            or frozen.service_id != claim.handler_service_id
            or frozen.version != claim.handler_service_version
            or contribution.handler_id != claim.handler_id
        ):
            await self._settle_job_error(
                claim, "COMPONENT_JOB_ROUTE_STALE", retryable=False
            )
            return
        try:
            payload = await self._payload(
                claim.input_payload_id, claim.input_payload_digest
            )
            if not isinstance(payload, dict):
                raise ForestError(
                    "COMPONENT_JOB_INPUT_INVALID",
                    "ComponentJob input payload is not an object",
                    500,
                )
            result = await service.invoke(
                DurableComponentJobRequest(
                    claim.job_id,
                    claim.handler_id,
                    claim.owner_component_id,
                    claim.scheduled_by_generation_id,
                    claim.attempt_no,
                    payload,
                ),
                operation_id=claim.attempt_id,
                causal_root_kind=claim.causal_root_kind,
                causal_root_id=claim.causal_root_id,
                timeout_seconds=claim.timeout_seconds,
            )
            if not isinstance(result, dict):
                raise ForestError(
                    "COMPONENT_JOB_RESULT_INVALID",
                    "ComponentJob handler returned no object",
                    502,
                )
            await self.job_commands.succeed_component_job(claim, result=result)
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            if exc.code == "STALE_ROUTING_CLAIM":
                return
            await self._settle_job_error(
                claim,
                exc.code,
                retryable=exc.status >= 500,
                unknown=exc.code == "SERVICE_OPERATION_UNKNOWN",
            )
        except Exception:
            await self._settle_job_error(
                claim, "COMPONENT_JOB_DISPATCH_UNKNOWN", retryable=False, unknown=True
            )

    async def _settle_job_error(
        self, claim, error_code: str, *, retryable: bool, unknown: bool = False
    ) -> None:
        try:
            await self.job_commands.fail_component_job(
                claim,
                error_code=error_code,
                retryable=retryable,
                unknown=unknown,
            )
        except ForestError as exc:
            if exc.code != "STALE_ROUTING_CLAIM":
                raise

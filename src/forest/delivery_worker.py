"""Lifecycle-owned Delivery worker for clean Adapter and Effect ports."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json

from forest.errors import ForestError
from forest.storage.delivery_authority import DeliveryPayloadRef


def _operation(kind: str, *parts: object) -> str:
    wire = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"delivery-worker:{kind}:{hashlib.sha256(wire).hexdigest()}"


class CleanDeliveryWorker:
    def __init__(
        self,
        *,
        repository,
        authorization,
        adapter,
        payloads,
        owner_id: str,
        poll_interval: float = 0.25,
    ) -> None:
        if not owner_id or len(owner_id.encode("utf-8")) > 256:
            raise ValueError("Delivery worker owner is invalid")
        if poll_interval <= 0:
            raise ValueError("Delivery worker poll interval must be positive")
        self.repository = repository
        self.authorization = authorization
        self.adapter = adapter
        self.payloads = payloads
        self.owner_id = owner_id
        self.poll_interval = poll_interval
        self._admission = asyncio.Event()
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._closing = False
        self._task: asyncio.Task | None = None
        self._sequence = 0
        self.failure: str | None = None

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Delivery worker already started")
        self._task = asyncio.create_task(self._run(), name=f"{self.owner_id}:delivery")

    def activate(self) -> None:
        self._admission.set()
        self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    async def quiesce(self) -> None:
        self._admission.clear()
        self._wake.set()
        await self._idle.wait()

    async def close(self) -> None:
        self._closing = True
        self._admission.set()
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return (self._task,) if self._task is not None else ()

    async def _wait(self) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
        except TimeoutError:
            pass
        self._wake.clear()

    @staticmethod
    def _parts(content: bytes) -> list[dict]:
        try:
            value = json.loads(content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError("DELIVERY_PAYLOAD_INVALID", "Delivery payload is invalid", 422) from exc
        if isinstance(value, dict) and set(value) == {"text"} and isinstance(value["text"], str):
            return [{"type": "text", "text": value["text"]}]
        if isinstance(value, dict) and set(value) == {"parts"} and isinstance(value["parts"], list):
            return value["parts"]
        raise ForestError("DELIVERY_PAYLOAD_INVALID", "Delivery payload has no supported parts", 422)

    async def _evidence(self, operation_id: str, value: dict) -> DeliveryPayloadRef:
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind="effect.evidence",
            owner_id=operation_id,
            media_type="application/json",
            content=json.dumps(value, sort_keys=True, separators=(",", ":")).encode(),
            expected_deletion_epoch=epoch,
        )
        return DeliveryPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "kernel.effect",
            "effect.evidence",
            operation_id,
        )

    async def _recover(self) -> None:
        for attempt in await self.repository.recoverable_attempts():
            if attempt["state"] == "PREPARED":
                await self.repository.abandon_prepared_attempt(
                    attempt_id=attempt["attempt_id"],
                    operation_id=_operation("recover-abandon", attempt["attempt_id"]),
                    reason_code="worker_restarted_before_handoff",
                )
            elif attempt["state"] == "DISPATCHING":
                effect_observation = _operation("recover-unknown-effect", attempt["attempt_id"])
                await self.repository.observe(
                    observation_operation_id=_operation(
                        "recover-unknown-delivery", attempt["attempt_id"]
                    ),
                    effect_observation_operation_id=effect_observation,
                    attempt_id=attempt["attempt_id"],
                    observed_status="UNKNOWN",
                    evidence_kind="RECONCILIATION",
                    error_code="DELIVERY_RESULT_UNKNOWN_AFTER_RESTART",
                )

    async def _run(self) -> None:
        try:
            while not self._closing:
                await self._admission.wait()
                if self._closing:
                    return
                self._idle.clear()
                try:
                    await self._recover()
                    self._sequence += 1
                    claim = await self.repository.claim(
                        operation_id=_operation("claim", self.owner_id, self._sequence),
                        worker_id=self.owner_id,
                    )
                    if claim is not None:
                        await self._dispatch(claim)
                finally:
                    self._idle.set()
                if claim is None:
                    await self._wait()
                    continue
                if not self._admission.is_set():
                    await self._wait()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            raise
        except Exception:
            self.failure = "DELIVERY_WORKER_FAILED"
            raise

    async def _dispatch(self, claim) -> None:
        attempt_id = _operation(
            "attempt", claim.intent_id, claim.part_index, claim.owner_epoch
        )
        permit_operation_id = _operation("permit", attempt_id)
        prepared_attempt = False
        handed_off = False

        async def abort() -> None:
            if handed_off:
                await self.repository.observe(
                    observation_operation_id=_operation("unknown", attempt_id),
                    effect_observation_operation_id=_operation("unknown-effect", attempt_id),
                    attempt_id=attempt_id,
                    observed_status="UNKNOWN",
                    evidence_kind="EXECUTOR",
                    error_code="DELIVERY_RESULT_UNKNOWN",
                )
            elif prepared_attempt:
                await self.repository.abandon_prepared_attempt(
                    attempt_id=attempt_id,
                    operation_id=_operation("abandon", attempt_id),
                    reason_code="dispatch_not_handed_off",
                )
            else:
                await self.repository.release_claim(
                    claim,
                    operation_id=_operation("release", attempt_id),
                    reason_code="dispatch_not_prepared",
                )

        try:
            content = await self.payloads.read_bytes(claim.payload.payload_id)
            adapter_send = self.adapter.prepare_delivery(
                binding_id=claim.binding_ref,
                channel_kind=claim.channel_kind,
                channel_ref=claim.channel_ref,
                parts=self._parts(content),
            )
            if inspect.isawaitable(adapter_send):
                adapter_send = await adapter_send
            epoch = await self.payloads.epoch()
            wire = await self.payloads.put_bytes(
                owner_component_id="kernel.effect",
                owner_kind="effect.wire",
                owner_id=attempt_id,
                media_type="application/json",
                content=adapter_send.prepared.wire_body,
                expected_deletion_epoch=epoch,
            )
            receipt = await self.authorization.authorize(
                claim,
                permit_operation_id=permit_operation_id,
                adapter_generation_id=adapter_send.adapter_generation_id,
                binding_epoch=adapter_send.binding_epoch,
                connection_epoch=adapter_send.connection_epoch,
                client_incarnation=adapter_send.client_incarnation,
                executor_fence_digest=adapter_send.executor_fence_digest,
            )
            await self.repository.prepare_attempt(
                claim,
                operation_id=permit_operation_id,
                attempt_id=attempt_id,
                transport_action=adapter_send.prepared.action,
                wire=DeliveryPayloadRef(
                    wire.payload_id,
                    wire.content_digest,
                    "kernel.effect",
                    "effect.wire",
                    attempt_id,
                ),
                binding_epoch=adapter_send.binding_epoch,
                connection_epoch=adapter_send.connection_epoch,
                client_incarnation=adapter_send.client_incarnation,
                adapter_generation_id=adapter_send.adapter_generation_id,
                executor_fence_digest=adapter_send.executor_fence_digest,
                authorization_ticket_ref=receipt.ticket_id,
                authorization_plan_digest=receipt.plan_digest,
                deadline_at=receipt.expires_at,
            )
            prepared_attempt = True
            await self.repository.mark_handoff(
                claim,
                attempt_id=attempt_id,
                handoff_operation_id=_operation("handoff", attempt_id),
            )
            handed_off = True
            result = await self.adapter.dispatch(
                binding_id=adapter_send.binding_id,
                client_incarnation=adapter_send.client_incarnation,
                binding_epoch=adapter_send.binding_epoch,
                connection_epoch=adapter_send.connection_epoch,
                adapter_generation_id=adapter_send.adapter_generation_id,
                executor_fence_digest=adapter_send.executor_fence_digest,
                prepared=adapter_send.prepared,
            )
        except asyncio.CancelledError:
            await asyncio.shield(abort())
            raise
        except Exception:
            await abort()
            return
        effect_observation = _operation("observe-effect", attempt_id)
        if result.status == "SENT":
            evidence = await self._evidence(effect_observation, result.as_dict())
            await self.repository.observe(
                observation_operation_id=_operation("observe-delivery", attempt_id),
                effect_observation_operation_id=effect_observation,
                attempt_id=attempt_id,
                observed_status="SENT",
                evidence_kind="EXECUTOR",
                receipt=evidence,
            )
        elif result.status == "NOT_SENT":
            await self.repository.observe(
                observation_operation_id=_operation("observe-delivery", attempt_id),
                effect_observation_operation_id=effect_observation,
                attempt_id=attempt_id,
                observed_status="RETRYABLE_NOT_APPLIED",
                evidence_kind="EXECUTOR",
                error_code=result.error_code or "DELIVERY_NOT_SENT",
            )
        else:
            await self.repository.observe(
                observation_operation_id=_operation("observe-delivery", attempt_id),
                effect_observation_operation_id=effect_observation,
                attempt_id=attempt_id,
                observed_status="UNKNOWN",
                evidence_kind="EXECUTOR",
                error_code=result.error_code or "DELIVERY_RESULT_UNKNOWN",
            )

"""The small, Shell-independent Core rescue executor.

Every request is read, reauthorized, applied and settled inside one control
transaction.  Package-owned actions are invoked only through their current
Generation's typed UnitOfWork participant.
"""

from __future__ import annotations

import re

from forest.errors import ForestError
from forest.runtime_contracts import RuntimeUnitOfWorkParticipantHandle
from forest.storage.kernel_audit import AuditEnvelope
from forest.storage.payload_authority import PayloadSource

_ACTION_FAILURES = frozenset({
    "RUNTIME_MAINTENANCE_CONFLICT",
    "RUNTIME_GRAPH_REVISION_CONFLICT",
    "RUNTIME_REPLACEMENT_CANCEL_CONFLICT",
    "RUNTIME_REPLACEMENT_NOT_FOUND",
    "AGENT_CANCEL_BOUNDARY_STALE",
    "AGENT_RUN_NOT_FOUND",
    "AGENT_CANCEL_ALREADY_REQUESTED",
})
_RESCUE_EFFECT = re.compile(r"core-intent:([0-9a-f]{64})/effect\Z")


class KernelCoreRescueExecutor:
    def __init__(self, runtime_host, runtime, policy, requests, payloads, audit) -> None:
        self.runtime_host = runtime_host
        self.runtime = runtime
        self.policy = policy
        self.requests = requests
        self.payloads = payloads
        self.audit = audit

    def _participant(self, component_id: str):
        registry = getattr(self.runtime_host, "registry", None)
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "RuntimeGraph is unavailable", 503)
        return RuntimeUnitOfWorkParticipantHandle(
            registry.contract_router, component_id
        ).resolve()

    async def run_once(self, runtime_module) -> bool:
        """Commit one queued request, or return False when no request is due."""

        lock = getattr(self.runtime_host, "_replacement_lock", None)
        if lock is None or getattr(self.runtime_host, "registry", None) is None:
            return False
        async with lock:
            generation = runtime_module.generation_identity
            if not runtime_module.generation_active or generation is None:
                return False
            lease = self.runtime_host.authority_lease()
            async with self.runtime.store.pool.connection() as conn, conn.transaction():
                row = await self.requests.next_queued_in(conn, lease)
                if row is None:
                    return False
                try:
                    await self.runtime.verify_active_generation_in(
                        conn, component_id="kernel.runtime",
                        generation_id=generation.generation_id,
                        admission_lane="CONTROL",
                    )
                    async with conn.transaction():
                        await self._prove_binding_in(conn, row, generation.generation_id)
                except ForestError as error:
                    if error.code == "ONEBOT_RESCUE_BINDING_STALE":
                        await self._complete_in(
                            conn, lease, row, generation.generation_id,
                            state="DENIED", value={}, error_code=error.code,
                        )
                    else:
                        await self.requests.defer_transient_in(
                            conn, lease, request_id=row["request_id"],
                            error_code=error.code,
                        )
                    return True
                policy_revision, actions = await self.policy.scope_actions_in(
                    conn,
                    binding_ref=row["binding_ref"],
                    group_ref=row["group_ref"],
                    principal_ref=row["principal_ref"],
                )
                if policy_revision != row["policy_revision"] or row["action"] not in actions:
                    await self._complete_in(
                        conn, lease, row, generation.generation_id,
                        state="DENIED", value={},
                        error_code="CORE_RESCUE_AUTHORITY_CHANGED",
                    )
                    return True
                try:
                    async with conn.transaction():
                        value = await self._effect_in(
                            conn, lease, row, generation.generation_id
                        )
                except ForestError as error:
                    if error.code not in _ACTION_FAILURES:
                        await self.requests.defer_transient_in(
                            conn, lease, request_id=row["request_id"],
                            error_code=error.code,
                        )
                        return True
                    await self._complete_in(
                        conn, lease, row, generation.generation_id,
                        state="FAILED", value={}, error_code=error.code,
                    )
                    return True
                await self._complete_in(
                    conn, lease, row, generation.generation_id,
                    state="SUCCEEDED", value=value, error_code=None,
                )
                return True

    async def run_delivery_once(self, runtime_module) -> bool:
        """Join a settled result to Delivery without losing either side on crash."""

        lock = getattr(self.runtime_host, "_replacement_lock", None)
        if lock is None or getattr(self.runtime_host, "registry", None) is None:
            return False
        async with lock:
            generation = runtime_module.generation_identity
            if not runtime_module.generation_active or generation is None:
                return False
            lease = self.runtime_host.authority_lease()
            async with self.runtime.store.pool.connection() as conn, conn.transaction():
                row = await self.requests.next_pending_delivery_in(conn, lease)
                if row is None:
                    return False
                await self.runtime.verify_active_generation_in(
                    conn, component_id="kernel.runtime",
                    generation_id=generation.generation_id,
                    admission_lane="CONTROL",
                )
                try:
                    async with conn.transaction():
                        await self._prove_binding_in(conn, row, generation.generation_id)
                except ForestError as error:
                    if error.code == "ONEBOT_RESCUE_BINDING_STALE":
                        await self.requests.mark_delivery_in(
                            conn, lease, request_id=row["request_id"], intent_id=None
                        )
                        return True
                    raise
                revision, actions = await self.policy.scope_actions_in(
                    conn, binding_ref=row["binding_ref"],
                    group_ref=row["group_ref"], principal_ref=row["principal_ref"],
                )
                if revision != row["policy_revision"] or row["action"] not in actions:
                    await self.requests.mark_delivery_in(
                        conn, lease, request_id=row["request_id"], intent_id=None
                    )
                    return True
                delivery = self._participant("delivery")
                await self.runtime.verify_active_generation_in(
                    conn, component_id="delivery",
                    generation_id=delivery.generation_id, admission_lane="CONTROL",
                )
                request_id = row["request_id"]
                intent_id = "core-intent:" + request_id.removeprefix("core:")
                payload = {
                    "payload_id": row["result_payload_id"],
                    "digest": row["result_payload_digest"],
                    "owner_component_id": "kernel.runtime",
                    "owner_kind": "core.rescue.result",
                    "owner_id": request_id,
                }
                await self.payloads.verify_in(
                    conn, payload_id=payload["payload_id"], digest=payload["digest"],
                    owner_component_id="kernel.runtime", owner_kind="core.rescue.result",
                    owner_id=request_id,
                )
                await delivery.invoke_in(
                    conn, "delivery.create-core-rescue-intent-uow",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "kernel.runtime",
                            "generation_id": generation.generation_id,
                        },
                        "input": {
                            "protocol": "forest.delivery-intent/1",
                            "intent_id": intent_id,
                            "binding_ref": row["binding_ref"],
                            "lane_kind": "CONTROL",
                            "ordering_key": "onebot:group:" + row["group_ref"],
                            "conversation_ref": None,
                            "channel_kind": "GROUP",
                            "channel_ref": row["group_ref"],
                            "cause_component_id": "kernel.runtime",
                            "cause_kind": "core.rescue.request",
                            "cause_id": request_id,
                            "delivery_generation_id": delivery.generation_id,
                            "reply_ref": None,
                            "parts": [{
                                "part_operation_id": intent_id + "/part",
                                "effect_operation_ref": intent_id + "/effect",
                                "payload": payload,
                            }],
                        },
                        "prior": {},
                    },
                    operation_id=intent_id + "/create",
                    causal_root_kind="core.rescue.request",
                    causal_root_id=request_id,
                    parent_call_id="call:" + request_id + "/delivery",
                    timeout_seconds=10,
                )
                await self.requests.mark_delivery_in(
                    conn, lease, request_id=request_id, intent_id=intent_id
                )
            self.runtime_host.notify()
            return True

    async def permit_delivery(self, request: dict) -> dict:
        """Authorize the exact queued result immediately before network dispatch."""

        if (
            not isinstance(request, dict)
            or set(request) != {
                "protocol", "request_id", "intent_id", "binding_ref",
                "channel_ref", "payload",
            }
            or request.get("protocol") != "forest.core-rescue-delivery-permit/1"
            or not isinstance(request.get("request_id"), str)
            or not isinstance(request.get("intent_id"), str)
            or not isinstance(request.get("binding_ref"), str)
            or not isinstance(request.get("channel_ref"), str)
            or not isinstance(request.get("payload"), dict)
        ):
            raise ForestError("CORE_RESCUE_DELIVERY_INVALID", "Invalid rescue delivery", 422)
        registry = getattr(self.runtime_host, "registry", None)
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "RuntimeGraph is unavailable", 503)
        generation = registry.instances["kernel.runtime"].generation_identity
        if generation is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Runtime Generation is unavailable", 503)
        lease = self.runtime_host.authority_lease()
        async with self.runtime.store.pool.connection() as conn, conn.transaction():
            await self._authorize_delivery_in(conn, lease, generation.generation_id, request)
        return {"protocol": "forest.core-rescue-delivery-permit/1", "allowed": True}

    async def dispatch_with_policy_guard(
        self, *, effect_operation_id: str, binding_ref: str,
        channel_kind: str, channel_ref: str, send,
    ):
        """Serialize a Core result's network send with rescue-policy mutation."""

        if not effect_operation_id.startswith("core-intent:"):
            return await send()
        match = _RESCUE_EFFECT.fullmatch(effect_operation_id)
        if match is None or channel_kind != "group":
            raise ForestError("CORE_RESCUE_DELIVERY_INVALID", "Invalid rescue effect", 422)
        request_id = "core:" + match.group(1)
        registry = getattr(self.runtime_host, "registry", None)
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "RuntimeGraph is unavailable", 503)
        generation = registry.instances["kernel.runtime"].generation_identity
        if generation is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Runtime Generation is unavailable", 503)
        lease = self.runtime_host.authority_lease()
        request = {
            "request_id": request_id,
            "intent_id": "core-intent:" + match.group(1),
            "binding_ref": binding_ref,
            "channel_ref": channel_ref,
        }
        async with self.runtime.store.pool.connection() as conn, conn.transaction():
            await self._authorize_delivery_in(conn, lease, generation.generation_id, request)
            # scope_actions_in holds a shared policy row lock until this send
            # returns. A concurrent grant change cannot commit in the gap.
            return await send()

    async def _authorize_delivery_in(self, conn, lease, generation_id: str, request: dict):
        row = await self.requests.delivery_in(conn, lease, request["request_id"])
        if (
            row is None or row["delivery_state"] != "QUEUED"
            or row["delivery_intent_id"] != request["intent_id"]
            or row["binding_ref"] != request["binding_ref"]
            or row["group_ref"] != request["channel_ref"]
            or ("payload" in request and request["payload"] != {
                "payload_id": row["result_payload_id"],
                "digest": row["result_payload_digest"],
                "owner_component_id": "kernel.runtime",
                "owner_kind": "core.rescue.result",
                "owner_id": row["request_id"],
            })
        ):
            raise ForestError(
                "CORE_RESCUE_DELIVERY_REVOKED", "Rescue result is not sendable", 409
            )
        await self.runtime.verify_active_generation_in(
            conn, component_id="kernel.runtime",
            generation_id=generation_id, admission_lane="CONTROL",
        )
        await self._prove_binding_in(conn, row, generation_id)
        revision, actions = await self.policy.scope_actions_in(
            conn, binding_ref=row["binding_ref"],
            group_ref=row["group_ref"], principal_ref=row["principal_ref"],
        )
        if revision != row["policy_revision"] or row["action"] not in actions:
            raise ForestError(
                "CORE_RESCUE_DELIVERY_REVOKED", "Rescue authority changed", 409
            )

    async def _prove_binding_in(self, conn, row: dict, kernel_generation_id: str):
        adapter = self._participant("adapter.onebot")
        await self.runtime.verify_active_generation_in(
            conn, component_id="adapter.onebot",
            generation_id=adapter.generation_id, admission_lane="CONTROL",
        )
        return await adapter.invoke_in(
            conn,
            "adapter.prove-rescue-binding",
            {
                "protocol": "forest.uow-step/1",
                "caller": {
                    "component_id": "kernel.runtime",
                    "generation_id": kernel_generation_id,
                },
                "input": {
                    "protocol": "forest.core-rescue-binding-check/1",
                    "event_ref": row["event_ref"],
                    "binding_ref": row["binding_ref"],
                    "binding_epoch": row["binding_epoch"],
                    "self_id": row["self_id"],
                    "adapter_generation_id": row["adapter_generation_id"],
                    "conversation_ref": row["conversation_ref"],
                    "group_ref": row["group_ref"],
                    "principal_ref": row["principal_ref"],
                    "client_incarnation": row["client_incarnation"],
                },
                "prior": {},
            },
            operation_id=row["request_id"] + "/binding-proof",
            causal_root_kind="core.rescue.request",
            causal_root_id=row["request_id"],
            parent_call_id="call:" + row["request_id"] + "/execute",
            timeout_seconds=10,
            read_only=True,
        )

    async def _effect_in(self, conn, lease, row: dict, kernel_generation_id: str) -> dict:
        action = row["action"]
        if action == "status":
            snapshot = await self.runtime.status_in(conn)
            return {
                "runtime_status": snapshot["status"],
                "graph_revision": snapshot["graph_revision"],
                "graph_state": snapshot["graph_state"],
                "maintenance_epoch": snapshot["maintenance_epoch"],
                "data_admission": snapshot["data_admission"],
                "control_admission": snapshot["control_admission"],
                "module_count": len(snapshot["members"]),
            }
        if action == "disable":
            saved = await self.runtime.set_data_admission_in(
                conn, lease,
                expected_maintenance_epoch=row["expected_revision"],
                data_admission=False,
            )
            return {
                "maintenance_epoch": saved["maintenance_epoch"],
                "data_admission": saved["data_admission"],
            }
        if action == "reload":
            operation_id = row["request_id"] + "/reload"
            saved = await self.runtime.request_graph_replacement_in(
                conn, lease, operation_id=operation_id,
                expected_graph_revision=row["expected_revision"],
                requested_by=row["principal_ref"],
            )
            return {"operation_id": operation_id, "state": saved["state"]}
        if action == "cancel" and row["target_ref"].startswith("run:"):
            agent = self._participant("agent.runtime")
            await self.runtime.verify_active_generation_in(
                conn, component_id="agent.runtime",
                generation_id=agent.generation_id, admission_lane="CONTROL",
            )
            return await agent.invoke_in(
                conn, "agent.request-core-cancel",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "kernel.runtime",
                        "generation_id": kernel_generation_id,
                    },
                    "input": {
                        "run_id": row["target_ref"],
                        "expected_checkpoint_seq": row["expected_revision"],
                        "requested_by": row["principal_ref"],
                        "reason_code": "CORE_RESCUE_CANCEL",
                    },
                    "prior": {},
                },
                operation_id=row["request_id"] + "/cancel",
                causal_root_kind="core.rescue.request",
                causal_root_id=row["request_id"],
                parent_call_id="call:" + row["request_id"] + "/execute",
                timeout_seconds=10,
            )
        if action == "cancel":
            saved = await self.runtime.cancel_graph_replacement_in(
                conn, lease, operation_id=row["target_ref"],
                expected_graph_revision=row["expected_revision"],
                cancel_operation_id=row["request_id"] + "/cancel",
                cancelled_by=row["principal_ref"],
            )
            return {"operation_id": saved["operation_id"], "state": saved["state"]}
        raise ForestError("CORE_RESCUE_ACTION_INVALID", "Invalid Core action", 422)

    async def _complete_in(
        self, conn, lease, row: dict, generation_id: str, *,
        state: str, value: dict, error_code: str | None,
    ) -> dict:
        result = {
            "protocol": "forest.core-rescue-result/1",
            "request_id": row["request_id"],
            "action": row["action"],
            "state": state,
            "value": value,
            "error_code": error_code,
        }
        payload_ref = None
        if state != "DENIED":
            summary = self._summary(row["action"], state, value, error_code)
            payload = await self.payloads.put_json_in(
                conn,
                owner_component_id="kernel.runtime",
                owner_kind="core.rescue.result",
                owner_id=row["request_id"],
                value={"text": summary},
                sources=(PayloadSource(
                    source_component_id="kernel.runtime",
                    source_kind="core.rescue.request",
                    source_id=row["request_id"],
                    source_revision=1,
                    observation_conversation_id=row["conversation_ref"],
                    observation_binding_id=row["binding_ref"],
                    author_subject_id=row["principal_ref"],
                    occurred_at=row["received_at"],
                ),),
                expected_deletion_epoch=await self.payloads.epoch_in(conn),
            )
            payload_ref = {
                "payload_id": payload.payload_id,
                "digest": payload.content_digest,
            }
        saved = await self.requests.complete_in(
            conn, lease, request_id=row["request_id"], state=state,
            result_value=result, result_code=error_code,
            payload_ref=payload_ref,
        )
        await self.audit.append_in(
            conn,
            AuditEnvelope(
                audit_id="audit:core-result:" + row["request_id"].removeprefix("core:"),
                action="core.rescue." + state.lower(),
                severity="SECURITY" if state == "DENIED" else (
                    "ERROR" if state == "FAILED" else "INFO"
                ),
                producer_component_id="kernel.runtime",
                producer_generation_id=generation_id,
                origin_kind="onebot.group",
                actor_kind="qq.principal",
                actor_id=row["principal_ref"],
                resource_kind="core.rescue.request",
                resource_id=row["request_id"],
                operation_kind="core.rescue.execution",
                operation_id=row["request_id"],
            ),
        )
        return saved

    @staticmethod
    def _summary(action: str, state: str, value: dict, error_code: str | None) -> str:
        if state == "FAILED":
            return f"Core {action} 失败：{error_code}"
        if action == "status":
            return (
                f"Core 状态：{value['runtime_status']}；图修订 "
                f"{value['graph_revision']}；DATA {'开启' if value['data_admission'] else '关闭'}；"
                f"maintenance epoch {value['maintenance_epoch']}；模块 {value['module_count']} 个。"
            )
        if action == "disable":
            return f"Core 已关闭 DATA；maintenance epoch {value['maintenance_epoch']}。"
        if action == "reload":
            return f"Core 已排队整图替换：{value['operation_id']}。"
        return f"Core 取消已记录：{value.get('operation_id') or value.get('run_id')}。"

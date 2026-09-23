import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta

from forest.errors import ForestError
from forest.sdk import Plugin, job


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class Delivery(Plugin):
    """Own the durable Delivery state machine and its isolated dispatch worker."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.delivery/1"}:
            raise ForestError(
                "DELIVERY_CONFIGURATION_INVALID",
                "Delivery configuration protocol is unsupported",
                422,
            )

    @staticmethod
    def _parts(value):
        if isinstance(value, dict) and set(value) == {"text"} and isinstance(value["text"], str):
            return [{"type": "text", "text": value["text"]}]
        if isinstance(value, dict) and set(value) == {"parts"} and isinstance(value["parts"], list):
            return value["parts"]
        raise ForestError("DELIVERY_PAYLOAD_INVALID", "Delivery payload has no supported parts", 422)

    @job("delivery.dispatch")
    async def dispatch(self, task, ctx):
        work = _plain(task["payload"])
        if work["mode"] == "RECOVER_PREPARED":
            await self._abandon(work, ctx, "worker_restarted_before_handoff")
            return {"intent_id": work["intent_id"], "state": "QUEUED"}
        if work["mode"] == "RECOVER_DISPATCHING":
            await self._observe(work, ctx, "UNKNOWN", None, "DELIVERY_RESULT_UNKNOWN_AFTER_RESTART")
            return {"intent_id": work["intent_id"], "state": "UNKNOWN"}

        prepared_recorded = False
        handed_off = False
        try:
            source = await ctx.services.call(
                "kernel.payload.json-read",
                {"payload": work["payload"]},
                operation_id=ctx.operations.stable("payload-read"),
            )
            adapter = await ctx.services.call(
                "adapter.onebot.delivery.prepare",
                {
                    "binding_ref": work["binding_ref"],
                    "channel_kind": work["channel_kind"],
                    "channel_ref": work["channel_ref"],
                    "parts": self._parts(source["value"]),
                    "operation_id": ctx.operations.stable("wire-prepare"),
                },
                operation_id=ctx.operations.stable("adapter-prepare"),
            )
            wire_value = json.loads(base64.b64decode(adapter["prepared"]["wire_body_b64"]))
            wire = await ctx.services.call(
                "kernel.effect.json-materialize",
                {
                    "protocol": "forest.effect-json-materialize/1",
                    "owner_kind": "effect.wire",
                    "owner_id": work["attempt_id"],
                    "value": wire_value,
                },
                operation_id=ctx.operations.stable("wire-materialize"),
            )
            if wire["payload"]["digest"] != "sha256:" + adapter["prepared"]["wire_digest"]:
                raise ForestError("DELIVERY_WIRE_DIGEST_MISMATCH", "Prepared wire bytes changed", 500)
            plan_digest = _digest(
                {
                    "semantic_digest": work["semantic_digest"],
                    "wire_digest": wire["payload"]["digest"],
                    "binding_ref": work["binding_ref"],
                    "connection_epoch": adapter["connection_epoch"],
                    "adapter_generation_id": adapter["adapter_generation_id"],
                }
            )
            ticket_id = ctx.operations.stable("authorization-ticket")
            permit_operation = ctx.operations.stable("effect-permit")
            deadline_at = (datetime.now(UTC) + timedelta(seconds=25)).isoformat()
            authorization = {
                "protocol": "forest.authorization-issue/1",
                "ticket_id": ticket_id,
                "issuance_operation_id": ctx.operations.stable("authorization-issue"),
                "operation_id": permit_operation,
                "action": "delivery.send",
                "resource_kind": "bot",
                "resource_id": ctx.identity["bot_id"],
                "input_digest": work["semantic_digest"],
                "plan_digest": plan_digest,
                "expires_at": deadline_at,
                "audience_component_id": "adapter.onebot",
                "audience_generation_id": adapter["adapter_generation_id"],
            }
            effect = {
                "protocol": "forest.effect-prepare/1",
                "effect_operation_id": work["effect_operation_ref"],
                "input_digest": work["semantic_digest"],
                "effect_kind": "onebot.send",
                "target_kind": work["channel_kind"].lower(),
                "target_id": work["channel_ref"],
                "input_payload": None,
                "attempt_id": work["attempt_id"],
                "permit_operation_id": permit_operation,
                "wire_payload": {
                    "payload_id": wire["payload"]["payload_id"],
                    "digest": wire["payload"]["digest"],
                },
                "authorization": {
                    "ticket_id": ticket_id,
                    "action": "delivery.send",
                    "resource_kind": "bot",
                    "resource_id": ctx.identity["bot_id"],
                    "plan_digest": plan_digest,
                },
                "executor": {
                    "component_id": "adapter.onebot",
                    "generation_id": adapter["adapter_generation_id"],
                    "fence_kind": "adapter.binding",
                    "fence_id": adapter["client_incarnation"],
                    "fence_epoch": adapter["connection_epoch"],
                },
                "deadline_at": deadline_at,
            }
            attempt = await ctx.uow.call(
                "delivery.prepare",
                {
                    "work": work,
                    "transport": {
                        "protocol": "forest.onebot-transport-fence/1",
                        "binding_ref": work["binding_ref"],
                        "binding_epoch": adapter["binding_epoch"],
                        "connection_epoch": adapter["connection_epoch"],
                        "client_incarnation": adapter["client_incarnation"],
                        "adapter_generation_id": adapter["adapter_generation_id"],
                        "executor_fence_digest": adapter["executor_fence_digest"],
                    },
                    "authorization": authorization,
                    "effect": effect,
                    "adapter": adapter,
                    "wire": wire["payload"],
                    "plan_digest": plan_digest,
                },
                operation_id=ctx.operations.stable("prepare-transaction"),
            )
            prepared_recorded = True
            await ctx.uow.call(
                "delivery.handoff",
                {
                    "work": work,
                    "attempt": attempt,
                    "effect": {
                        "protocol": "forest.effect-handoff/1",
                        "permit": attempt["permit"],
                        "handoff_operation_id": ctx.operations.stable("effect-handoff"),
                    },
                },
                operation_id=ctx.operations.stable("handoff-transaction"),
            )
            handed_off = True
            result = await ctx.services.call(
                "adapter.onebot.delivery.dispatch",
                {
                    "binding_ref": adapter["binding_ref"],
                    "client_incarnation": adapter["client_incarnation"],
                    "binding_epoch": adapter["binding_epoch"],
                    "connection_epoch": adapter["connection_epoch"],
                    "adapter_generation_id": adapter["adapter_generation_id"],
                    "executor_fence_digest": adapter["executor_fence_digest"],
                    "prepared": adapter["prepared"],
                    "timeout_seconds": 20,
                },
                operation_id=ctx.operations.stable("adapter-dispatch"),
                timeout=25,
            )
        except ForestError:
            if handed_off:
                await self._observe(work, ctx, "UNKNOWN", None, "DELIVERY_RESULT_UNKNOWN")
            elif prepared_recorded:
                await self._abandon(work, ctx, "dispatch_not_handed_off")
            else:
                await ctx.uow.call(
                    "delivery.release",
                    {"work": work, "reason_code": "dispatch_not_prepared"},
                    operation_id=ctx.operations.stable("release-transaction"),
                )
            raise

        if result["status"] == "SENT":
            evidence = await ctx.services.call(
                "kernel.effect.json-materialize",
                {
                    "protocol": "forest.effect-json-materialize/1",
                    "owner_kind": "effect.evidence",
                    "owner_id": ctx.operations.stable("effect-observation"),
                    "value": result,
                },
                operation_id=ctx.operations.stable("evidence-materialize"),
            )
            await self._observe(work, ctx, "SENT", evidence["payload"], None)
            return {"intent_id": work["intent_id"], "state": "SENT"}
        status = "RETRYABLE_NOT_APPLIED" if result["status"] == "NOT_SENT" else "UNKNOWN"
        await self._observe(
            work, ctx, status, None, result.get("error_code") or "DELIVERY_RESULT_UNKNOWN"
        )
        return {"intent_id": work["intent_id"], "state": "QUEUED" if status != "UNKNOWN" else "UNKNOWN"}

    async def _observe(self, work, ctx, status, evidence, error_code):
        effect_status = {
            "SENT": "SUCCEEDED",
            "RETRYABLE_NOT_APPLIED": "NOT_APPLIED",
            "UNKNOWN": "UNKNOWN",
        }[status]
        observation_id = ctx.operations.stable("effect-observation")
        await ctx.uow.call(
            "delivery.observe",
            {
                "work": work,
                "observed_status": status,
                "error_code": error_code,
                "receipt": evidence,
                "effect": {
                    "protocol": "forest.effect-observe/1",
                    "effect_operation_id": work["effect_operation_ref"],
                    "attempt_id": work["attempt_id"],
                    "observation_operation_id": observation_id,
                    "status": effect_status,
                    "evidence_kind": "executor",
                    "evidence_payload": None if evidence is None else {
                        "payload_id": evidence["payload_id"], "digest": evidence["digest"]
                    },
                },
            },
            operation_id=ctx.operations.stable("observe-transaction:" + status),
        )

    async def _abandon(self, work, ctx, reason):
        await ctx.uow.call(
            "delivery.abandon",
            {
                "work": work,
                "reason_code": reason,
                "effect": {
                    "protocol": "forest.effect-abandon/1",
                    "effect_operation_id": work["effect_operation_ref"],
                    "attempt_id": work["attempt_id"],
                    "abandonment_operation_id": ctx.operations.stable("effect-abandon"),
                    "reason_code": reason,
                    "cancel_effect": False,
                },
            },
            operation_id=ctx.operations.stable("abandon-transaction"),
        )

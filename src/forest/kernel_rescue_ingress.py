"""Independent Core rescue admission inside an Adapter-owned UnitOfWork."""

from __future__ import annotations

import hashlib
from datetime import datetime

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.kernel_audit import AuditEnvelope


class KernelRuntimeIngressParticipant:
    """Dispatch two kernel-owned ingress ports without delegating Core to Shell."""

    component_id = "kernel.runtime"

    def __init__(self, gate, rescue, executor=None) -> None:
        self.gate = gate
        self.rescue = rescue
        self.executor = executor
        self.service = gate.service

    @property
    def generation_id(self) -> str:
        return self.gate.generation_id

    async def invoke_in(self, conn, name: str, envelope: dict, **invocation) -> dict:
        if name in {"shell.gate-prove-ingress", "core.rescue.prove-ingress"}:
            return await self.gate.invoke_in(conn, name, envelope, **invocation)
        if name == "core.rescue.admit":
            return await self.rescue.invoke_in(conn, envelope, **invocation)
        raise ForestError("RUNTIME_UOW_PARTICIPANT_INVALID", "Unknown runtime ingress port", 404)

    async def permit_delivery(self, request: dict) -> dict:
        if self.executor is None:
            raise ForestError("CORE_RESCUE_UNAVAILABLE", "Core rescue is unavailable", 503)
        return await self.executor.permit_delivery(request)


def core_rescue_delivery_permit_service(context):
    participant = context.participant("kernel.runtime")

    async def permit(request):
        return await participant.permit_delivery(request)

    return permit


class KernelCoreRescueAdmission:
    """Freeze the two ingress proofs, exact grant and request in one transaction."""

    def __init__(self, runtime, policy, audit) -> None:
        self.runtime = runtime
        self.policy = policy
        self.audit = audit

    def _table(self):
        return sql.Identifier(self.policy.namespace, "rescue_requests")

    async def invoke_in(self, conn, envelope: dict, **_invocation) -> dict:
        def invalid() -> None:
            raise ForestError("CORE_RESCUE_INGRESS_INVALID", "Core rescue ingress is invalid", 409)

        if not isinstance(envelope, dict) or envelope.get("protocol") != "forest.uow-step/1":
            invalid()
        caller, body, prior = (
            envelope.get("caller"), envelope.get("input"), envelope.get("prior")
        )
        required = {
            "protocol", "binding_ref", "binding_epoch", "connection_epoch",
            "client_incarnation", "adapter_generation_id", "executor_fence_digest",
            "conversation_ref", "event_ref", "principal_ref", "group_ref",
            "received_at", "admission_deadline", "inspection",
        }
        if (
            not isinstance(caller, dict)
            or caller.get("component_id") != "adapter.onebot"
            or not isinstance(body, dict)
            or set(body) != required
            or body.get("protocol") != "forest.core-rescue-admit/1"
            or not isinstance(prior, dict)
        ):
            invalid()
        adapter = prior.get("adapter")
        conversation = prior.get("conversation")
        gate = prior.get("gate")
        origin = prior.get("origin")
        inspection = body["inspection"]
        if not all(isinstance(item, dict) for item in (
            adapter, conversation, gate, origin, inspection
        )):
            invalid()
        group_ref = body["group_ref"]
        self_id = origin.get("self_id")
        if (
            gate.get("protocol") != "forest.shell-ingress-proof/1"
            or origin.get("protocol") != "forest.onebot-shell-origin-proof/1"
            or inspection.get("protocol") != "forest.core-rescue-inspection/1"
            or inspection.get("claimed") is not True
            or gate.get("core_inspection") != inspection
            or not isinstance(group_ref, str)
            or not group_ref.isascii()
            or not group_ref.isdecimal()
            or not 1 <= len(group_ref) <= 20
            or group_ref[0] == "0"
            or not isinstance(self_id, str)
            or not self_id.isascii()
            or not self_id.isdecimal()
            or not 1 <= len(self_id) <= 19
            or self_id[0] == "0"
            or self_id != gate.get("self_id")
            or body.get("conversation_ref") != "onebot:group:" + group_ref
            or body.get("conversation_ref") != conversation.get("conversation_id")
            or body.get("conversation_ref") != gate.get("conversation_ref")
            or body.get("conversation_ref") != origin.get("conversation_ref")
            or body.get("event_ref") != adapter.get("event_id")
            or body.get("event_ref") != gate.get("event_ref")
            or body.get("event_ref") != origin.get("event_ref")
            or body.get("binding_ref") != adapter.get("binding_ref")
            or body.get("binding_ref") != gate.get("binding_ref")
            or body.get("binding_ref") != origin.get("binding_ref")
            or body.get("binding_epoch") != adapter.get("binding_epoch")
            or body.get("binding_epoch") != gate.get("binding_epoch")
            or body.get("binding_epoch") != origin.get("binding_epoch")
            or type(body.get("connection_epoch")) is not int
            or body["connection_epoch"] < 1
            or not isinstance(body.get("client_incarnation"), str)
            or not isinstance(body.get("executor_fence_digest"), str)
            or body.get("adapter_generation_id") != caller.get("generation_id")
            or body.get("adapter_generation_id") != gate.get("adapter_generation_id")
            or body.get("adapter_generation_id") != origin.get("adapter_generation_id")
            or body.get("principal_ref") != gate.get("principal_ref")
            or body.get("principal_ref") != origin.get("principal_ref")
            or body.get("received_at") != gate.get("received_at")
            or body.get("admission_deadline") != gate.get("admission_deadline")
            or type(conversation.get("ingress_seq")) is not int
            or conversation["ingress_seq"] != gate.get("ingress_seq")
        ):
            invalid()
        try:
            received_at = datetime.fromisoformat(body["received_at"])
            deadline = datetime.fromisoformat(body["admission_deadline"])
        except (TypeError, ValueError):
            invalid()
        if (
            received_at.tzinfo is None or deadline.tzinfo is None
            or deadline <= received_at
        ):
            invalid()
        event_ref = body["event_ref"]
        request_id = "core:" + hashlib.sha256(
            (self.policy.store.bot_id + "\0" + event_ref).encode("utf-8")
        ).hexdigest()
        input_digest = digest_json(body)
        # Serialize admission for this Bot before replay and capacity checks.
        # A shared policy row lock permits parallel readers, so it cannot by
        # itself enforce the bounded queue or one-result replay invariant.
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("forest.core.rescue.admission/" + self.policy.store.bot_id,),
        )
        prior_request = await (
            await conn.execute(
                sql.SQL(
                    "SELECT request_id,input_digest,admission_state,policy_revision,"
                    "kernel_generation_id,self_id,"
                    "rejection_code FROM {} WHERE bot_id=%s AND event_ref=%s FOR SHARE"
                ).format(self._table()),
                (self.policy.store.bot_id, event_ref),
            )
        ).fetchone()
        if prior_request is not None:
            if (
                prior_request["request_id"] != request_id
                or prior_request["input_digest"] != input_digest
                or prior_request["self_id"] != self_id
            ):
                raise ForestError("CORE_RESCUE_REQUEST_CONFLICT", "Core rescue replay differs", 409)
            return self._receipt(prior_request, duplicate=True)
        now = (await (await conn.execute("SELECT clock_timestamp() AS value")).fetchone())["value"]
        if now >= deadline:
            raise ForestError("CORE_RESCUE_ADMISSION_EXPIRED", "Core rescue ingress expired", 409)
        policy_revision, granted = await self.policy.scope_actions_in(
            conn,
            binding_ref=body["binding_ref"], group_ref=group_ref,
            principal_ref=body["principal_ref"],
        )
        action = inspection.get("action")
        if action not in {"status", "cancel", "disable", "reload", None}:
            invalid()
        authorized = action in granted if action is not None else bool(granted)
        rejection_code = None
        if not authorized:
            rejection_code = "CORE_RESCUE_PRINCIPAL_DENIED"
        elif inspection.get("error_code") is not None:
            if inspection["error_code"] != "CORE_RESCUE_SYNTAX_INVALID":
                invalid()
            rejection_code = "CORE_RESCUE_SYNTAX_INVALID"
        elif action is None:
            invalid()
        service_class = "P1" if action == "reload" else "P0" if action else None
        if rejection_code is None:
            pending = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS count FROM {} WHERE bot_id=%s "
                        "AND state='QUEUED' AND service_class=%s"
                    ).format(self._table()),
                    (self.policy.store.bot_id, service_class),
                )
            ).fetchone()
            if pending["count"] >= (8 if service_class == "P1" else 16):
                rejection_code = "CORE_RESCUE_QUEUE_FULL"
        admission_state = "REJECTED" if rejection_code else "QUEUED"
        generation = self.runtime.generation_identity
        if generation is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Runtime Generation is unavailable", 503)
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,request_id,input_digest,event_ref,"
                    "conversation_ref,binding_ref,binding_epoch,self_id,connection_epoch,"
                    "client_incarnation,adapter_generation_id,executor_fence_digest,"
                    "group_ref,principal_ref,ingress_seq,policy_revision,"
                    "kernel_generation_id,action,target_ref,"
                    "expected_revision,service_class,admission_state,state,rejection_code,"
                    "received_at,admission_deadline) VALUES ("
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "RETURNING request_id,admission_state,policy_revision,"
                    "kernel_generation_id,rejection_code"
                ).format(self._table()),
                (
                    self.policy.store.bot_id, request_id, input_digest, event_ref,
                    body["conversation_ref"], body["binding_ref"], body["binding_epoch"],
                    self_id,
                    body["connection_epoch"], body["client_incarnation"],
                    body["adapter_generation_id"], body["executor_fence_digest"],
                    group_ref, body["principal_ref"],
                    conversation["ingress_seq"], policy_revision,
                    generation.generation_id, action,
                    inspection.get("target_ref"), inspection.get("expected_revision"),
                    service_class, admission_state, admission_state, rejection_code,
                    received_at, deadline,
                ),
            )
        ).fetchone()
        await self.audit.append_in(
            conn,
            AuditEnvelope(
                audit_id="audit:core:" + request_id.removeprefix("core:"),
                action="core.rescue.admitted" if admission_state == "QUEUED" else "core.rescue.rejected",
                severity="SECURITY",
                producer_component_id="kernel.runtime",
                producer_generation_id=generation.generation_id,
                origin_kind="onebot.group",
                actor_kind="qq.principal",
                actor_id=body["principal_ref"],
                resource_kind="core.rescue.request",
                resource_id=request_id,
                operation_kind="core.rescue.admission",
                operation_id=request_id,
            ),
        )
        return self._receipt(row, duplicate=False)

    def _receipt(self, row, *, duplicate: bool) -> dict:
        return {
            "protocol": "forest.core-rescue-admission/1",
            "request_id": row["request_id"],
            "state": row["admission_state"],
            "kernel_generation_id": row["kernel_generation_id"],
            "policy_revision": row["policy_revision"],
            "rejection_code": row["rejection_code"],
            "duplicate": duplicate,
        }

"""Immutable ordinary-message route snapshots and Agent fallback barriers."""

from __future__ import annotations

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.message_handler_limits import MessageHandlerLimitStore
from forest.storage.message_handler_policies import MessageHandlerPolicyStore


class MessageHandlerChainStore:
    def __init__(self, store, bindings):
        self.store = store
        self.bindings = bindings
        self.policies = MessageHandlerPolicyStore(store)
        self.limits = MessageHandlerLimitStore(store)

    @staticmethod
    def _fallback(decision, rule_revision, reason):
        if decision == "PENDING":
            return {
                "fallback_decision": "PENDING",
                "fallback_rule_revision": None,
                "fallback_reason": None,
                "fallback_digest": None,
            }
        if decision not in {"TRIGGER", "NO_TRIGGER"}:
            raise ForestError(
                "MESSAGE_HANDLER_FALLBACK_INVALID",
                "Ordinary handler fallback must be a conversation trigger decision",
                422,
            )
        return {
            "fallback_decision": decision,
            "fallback_rule_revision": rule_revision,
            "fallback_reason": reason,
            "fallback_digest": digest_json([decision, rule_revision, reason]),
        }

    async def snapshot_in(self, conn, binding):
        await conn.execute(
            "INSERT INTO public.bot_message_handler_clocks(bot_id) VALUES (%s) "
            "ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        clock = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_clocks "
                "WHERE bot_id=%s FOR SHARE",
                (self.store.bot_id,),
            )
        ).fetchone()
        heads = await (
            await conn.execute(
                "SELECT h.*,g.package_digest FROM public.bot_message_handler_heads h "
                "JOIN public.bot_message_handler_generations g ON "
                "(g.bot_id,g.generation_id)=(h.bot_id,h.generation_id) "
                "WHERE h.bot_id=%s AND h.active=true AND h.retired=false "
                "ORDER BY h.priority DESC,h.component_id,h.handler_id FOR SHARE OF h,g",
                (self.store.bot_id,),
            )
        ).fetchall()
        candidates = []
        for head in heads:
            policy_head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_policy_heads WHERE bot_id=%s "
                    "AND binding_id=%s AND component_id=%s AND handler_id=%s FOR SHARE",
                    (
                        self.store.bot_id,
                        binding["binding_id"],
                        head["component_id"],
                        head["handler_id"],
                    ),
                )
            ).fetchone()
            if policy_head is None or policy_head["revision"] == 0:
                continue
            policy = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_policy_revisions "
                    "WHERE bot_id=%s AND binding_id=%s AND component_id=%s "
                    "AND handler_id=%s AND revision=%s",
                    (
                        self.store.bot_id,
                        binding["binding_id"],
                        head["component_id"],
                        head["handler_id"],
                        policy_head["revision"],
                    ),
                )
            ).fetchone()
            if policy is None or policy["handler_spec_digest"] != head["spec_digest"]:
                continue
            candidates.append((head, policy))
        return clock["revision"], candidates

    async def _routes_in(self, conn, binding, author, snapshot=None):
        registry_revision, candidates = (
            snapshot if snapshot is not None else await self.snapshot_in(conn, binding)
        )
        if author.get("is_self") or author.get("identity_origin") != "adapter_verified":
            return registry_revision, []
        routes = []
        binding_config = None
        for head, policy in candidates:
            saved = await self.policies._hydrate_in(
                conn, policy, kind="REVISION", required=False
            )
            if saved["values_payload_state"] != "ACTIVE":
                continue
            values = saved["values"]
            allowed = values["mode"] == "EVERYONE"
            binding_config_revision = None
            if values["mode"] == "USER_SET":
                allowed = author["external_user_id"] in values["user_ids"]
            elif values["mode"] == "ADMINS":
                if binding_config is None:
                    binding_config = await self.bindings.current_config_in(
                        conn,
                        self.store.bot_id,
                        binding["binding_id"],
                        expected_binding_epoch=binding["binding_epoch"],
                    )
                binding_config_revision = binding_config["effective_revision"]
                allowed = author["external_user_id"] in binding_config["config"][
                    "admin_user_ids"
                ]
            if not allowed:
                continue
            routes.append(
                {
                    "route_index": len(routes),
                    "component_id": head["component_id"],
                    "handler_id": head["handler_id"],
                    "handler": head["handler"],
                    "priority": head["priority"],
                    "generation_id": head["generation_id"],
                    "package_digest": head["package_digest"],
                    "route_epoch": head["route_epoch"],
                    "route_registry_revision": head["registry_revision"],
                    "spec_digest": head["spec_digest"],
                    "policy_revision": policy["revision"],
                    "policy_mode": policy["policy_mode"],
                    "policy_payload_id": policy["values_payload_id"],
                    "policy_payload_digest": policy["values_payload_digest"],
                    "binding_config_revision": binding_config_revision,
                }
            )
        return registry_revision, routes

    async def freeze_in(
        self,
        conn,
        *,
        binding,
        inbox,
        author,
        fallback_decision,
        fallback_rule_revision,
        fallback_reason,
        event_bytes,
        snapshot=None,
        limit_snapshot=None,
        admission_exempt=False,
    ):
        existing = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, inbox["event_id"]),
            )
        ).fetchone()
        if existing is not None:
            return existing
        registry_revision, routes = await self._routes_in(
            conn, binding, author, snapshot=snapshot
        )
        route_set_digest = (
            await (
                await conn.execute(
                    "SELECT 'sha256:' || encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') "
                    "AS digest",
                    (Jsonb(routes),),
                )
            ).fetchone()
        )["digest"]
        fallback = self._fallback(
            fallback_decision, fallback_rule_revision, fallback_reason
        )
        admission_exempt = bool(routes) and admission_exempt
        limit_revision = None
        deadline_seconds = 30
        admission_error = None
        if routes and not admission_exempt:
            if limit_snapshot is None:
                # Internal callers must establish the limit-head lock before
                # removable input/policy payloads are read.
                raise ForestError(
                    "MESSAGE_HANDLER_LIMIT_SNAPSHOT_REQUIRED",
                    "Handler admission has no locked limit snapshot",
                    409,
                )
            limit_revision, deadline_seconds, admission_error = (
                await self.limits.admit_locked_in(
                    conn,
                    snapshot=limit_snapshot,
                    binding_id=binding["binding_id"],
                    principal_ref=author["principal_ref"],
                    event_bytes=event_bytes,
                    route_count=len(routes),
                )
            )
        chain_id = "message-handler-chain:" + digest_json(inbox["event_id"])
        status = "FAILED" if admission_error else "PENDING" if routes else "PASSED"
        chain = await (
            await conn.execute(
                "INSERT INTO public.bot_message_handler_chains("
                "bot_id,chain_id,event_id,conversation_id,binding_id,ingress_seq,"
                "principal_ref,external_user_id,input_payload_id,registry_revision,"
                "route_count,route_set_digest,fallback_decision,fallback_rule_revision,"
                "fallback_reason,fallback_digest,status,deadline_at,completed_at,"
                "admission_limit_revision,event_bytes,admission_exempt,terminal_reason) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                "clock_timestamp()+(%s * interval '1 second'),"
                "CASE WHEN %s IN ('PASSED','FAILED') THEN clock_timestamp() END,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    self.store.bot_id,
                    chain_id,
                    inbox["event_id"],
                    inbox["conversation_id"],
                    binding["binding_id"],
                    inbox["ingress_seq"],
                    author["principal_ref"],
                    author["external_user_id"],
                    inbox["payload_id"],
                    registry_revision,
                    len(routes),
                    route_set_digest,
                    fallback["fallback_decision"],
                    fallback["fallback_rule_revision"],
                    fallback["fallback_reason"],
                    fallback["fallback_digest"],
                    status,
                    deadline_seconds,
                    status,
                    limit_revision,
                    event_bytes,
                    admission_exempt,
                    admission_error,
                ),
            )
        ).fetchone()
        for route in routes:
            await conn.execute(
                "INSERT INTO public.bot_message_handler_chain_items("
                "bot_id,chain_id,route_index,binding_id,component_id,handler_id,handler,"
                "priority,generation_id,package_digest,route_epoch,route_registry_revision,"
                "spec_digest,policy_revision,policy_mode,policy_payload_id,"
                "policy_payload_digest,binding_config_revision) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    chain_id,
                    route["route_index"],
                    binding["binding_id"],
                    route["component_id"],
                    route["handler_id"],
                    route["handler"],
                    route["priority"],
                    route["generation_id"],
                    route["package_digest"],
                    route["route_epoch"],
                    route["route_registry_revision"],
                    route["spec_digest"],
                    route["policy_revision"],
                    route["policy_mode"],
                    route["policy_payload_id"],
                    route["policy_payload_digest"],
                    route["binding_config_revision"],
                ),
            )
        await self.store._event(
            conn,
            "message_handler.chain_frozen",
            chain_id,
            {
                "conversation_id": inbox["conversation_id"],
                "event_id": inbox["event_id"],
                "ingress_seq": inbox["ingress_seq"],
                "registry_revision": registry_revision,
                "route_count": len(routes),
                "fallback_decision": fallback["fallback_decision"],
                "admission_limit_revision": limit_revision,
                "event_bytes": event_bytes,
                "admission_error_code": admission_error,
                "admission_exempt": admission_exempt,
            },
        )
        return chain

    async def admit_deferred_in(self, conn, chain, limit_snapshot):
        """Resolve a classification barrier before it becomes Actor work."""
        if not chain["admission_exempt"] or chain["status"] != "PENDING":
            return chain
        limit_revision, deadline_seconds, admission_error = (
            await self.limits.admit_locked_in(
                conn,
                snapshot=limit_snapshot,
                binding_id=chain["binding_id"],
                principal_ref=chain["principal_ref"],
                event_bytes=chain["event_bytes"],
                route_count=chain["route_count"],
            )
        )
        status = "FAILED" if admission_error else "PENDING"
        return await (
            await conn.execute(
                "UPDATE public.bot_message_handler_chains SET admission_exempt=false,"
                "admission_limit_revision=%s,deadline_at=clock_timestamp()+(%s * interval '1 second'),"
                "status=%s,terminal_reason=%s,"
                "completed_at=CASE WHEN %s='FAILED' THEN clock_timestamp() END,"
                "revision=revision+1 WHERE bot_id=%s AND chain_id=%s "
                "AND admission_exempt=true AND status='PENDING' RETURNING *",
                (
                    limit_revision,
                    deadline_seconds,
                    status,
                    admission_error,
                    status,
                    self.store.bot_id,
                    chain["chain_id"],
                ),
            )
        ).fetchone()

    async def resolve_fallback_in(self, conn, chain, decision, rule_revision, reason):
        fallback = self._fallback(decision, rule_revision, reason)
        current = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                "AND chain_id=%s FOR UPDATE",
                (self.store.bot_id, chain["chain_id"]),
            )
        ).fetchone()
        if current["fallback_decision"] != "PENDING":
            if current["fallback_digest"] != fallback["fallback_digest"]:
                raise ForestError(
                    "MESSAGE_HANDLER_FALLBACK_CONFLICT",
                    "The Agent fallback decision is already frozen",
                    409,
                )
            return current
        return await (
            await conn.execute(
                "UPDATE public.bot_message_handler_chains SET fallback_decision=%s,"
                "fallback_rule_revision=%s,fallback_reason=%s,fallback_digest=%s,"
                "revision=revision+1 WHERE bot_id=%s AND chain_id=%s RETURNING *",
                (
                    fallback["fallback_decision"],
                    fallback["fallback_rule_revision"],
                    fallback["fallback_reason"],
                    fallback["fallback_digest"],
                    self.store.bot_id,
                    current["chain_id"],
                ),
            )
        ).fetchone()

    async def cancel_in(self, conn, event_id, reason):
        if reason not in {
            "BUSINESS_COMMAND_OWNED",
            "BUSINESS_CLASSIFICATION_FAILED",
            "POLICY_REVOKED",
        }:
            raise ValueError("unsupported message handler chain cancellation reason")
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                "AND event_id=%s FOR UPDATE",
                (self.store.bot_id, event_id),
            )
        ).fetchone()
        if chain is None or chain["status"] == "CANCELLED":
            return chain
        if chain["status"] not in {"PENDING", "PASSED"}:
            raise ForestError(
                "MESSAGE_HANDLER_CHAIN_OWNERSHIP_CONFLICT",
                "Business command ownership arrived after handler execution",
                409,
            )
        terminal_reason = reason
        clear_claim = chain["claim_actor_id"] is not None
        if clear_claim:
            if reason != "POLICY_REVOKED" or chain["status"] != "PENDING":
                raise ForestError(
                    "MESSAGE_HANDLER_CHAIN_OWNERSHIP_CONFLICT",
                    "A claimed handler chain can only be suppressed after policy revocation",
                    409,
                )
            attempt = await (
                await conn.execute(
                    "SELECT a.call_operation_id,c.status AS call_status "
                    "FROM public.bot_message_handler_attempts a LEFT JOIN public.bot_calls c "
                    "ON c.bot_id=a.bot_id AND c.operation_id=a.call_operation_id "
                    "WHERE (a.bot_id,a.chain_id,a.attempt_no)=(%s,%s,%s) FOR SHARE OF a",
                    (self.store.bot_id, chain["chain_id"], chain["attempt_count"]),
                )
            ).fetchone()
            if attempt is None:
                raise ForestError(
                    "MESSAGE_HANDLER_ATTEMPT_INVALID",
                    "Claimed handler has no immutable attempt proof",
                    409,
                )
            if attempt["call_status"] == "ADMITTED":
                raise ForestError(
                    "MESSAGE_HANDLER_RECOVERY_REQUIRED",
                    "An admitted handler call must settle before its revoked result is suppressed",
                    503,
                )
            terminal_reason = (
                "POLICY_REVOKED_AFTER_CALL"
                if attempt["call_status"] == "COMPLETED"
                else "POLICY_REVOKED_AFTER_CLAIM"
            )
        return await (
            await conn.execute(
                "UPDATE public.bot_message_handler_chains SET status='CANCELLED',"
                "terminal_reason=%s,completed_at=clock_timestamp(),"
                "claim_actor_id=CASE WHEN %s THEN NULL ELSE claim_actor_id END,"
                "claim_actor_epoch=CASE WHEN %s THEN NULL ELSE claim_actor_epoch END,"
                "claim_host_incarnation=CASE WHEN %s THEN NULL ELSE claim_host_incarnation END,"
                "revision=revision+1 WHERE bot_id=%s AND chain_id=%s RETURNING *",
                (
                    terminal_reason,
                    clear_claim,
                    clear_claim,
                    clear_claim,
                    self.store.bot_id,
                    chain["chain_id"],
                ),
            )
        ).fetchone()

    async def cancel_for_business_command_in(self, conn, event_id):
        return await self.cancel_in(conn, event_id, "BUSINESS_COMMAND_OWNED")

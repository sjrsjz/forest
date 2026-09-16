"""Read-only dispatch traces projected from durable execution authorities."""

from forest.storage.control import BotControl


def _iso(value):
    return value.isoformat() if value is not None else None


class DispatchTraceStore:
    """Read-only projection over the durable authorities that handled each input."""

    def __init__(self, store, *, control=None):
        self.store = store
        self.control = control or BotControl(store)

    @staticmethod
    def _handler_status(chain, item, result, failure):
        if result is not None:
            return result["disposition"], None
        if failure is not None:
            return ("RETRY_PENDING" if failure["retryable"] else "FAILED"), failure["error_code"]
        if chain["status"] in {"FAILED", "CANCELLED", "CONSUMED", "REPLIED"}:
            if item["route_index"] >= chain["current_index"]:
                return "SKIPPED", chain["terminal_reason"] or chain["status"]
        if item["route_index"] < chain["current_index"]:
            return "PASSED", None
        return "PENDING", None

    async def read(self, ticket, operation_id: str, payload: dict) -> dict:
        limit = payload["limit"]
        conversation_id = payload.get("conversation_id")
        binding_id = payload.get("binding_id")
        channel_id = payload.get("channel_id")
        event_id = payload.get("event_id")
        component_id = payload.get("component_id")
        generation_id = payload.get("generation_id")
        status = payload.get("status")
        received_after = payload.get("received_after")
        received_before = payload.get("received_before")
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "module.read", operation_id, payload)
            roots = await (
                await conn.execute(
                    "SELECT i.event_id,i.conversation_id,c.binding_id,c.channel_id,i.ingress_seq,"
                    "i.graph_digest,i.trigger_decision,i.rule_revision,i.classification_reason,"
                    "i.received_at,COALESCE(s.status,b.status,h.status,t.status,i.trigger_decision) AS status "
                    "FROM public.bot_message_inbox i JOIN public.bot_conversations c "
                    "ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id "
                    "LEFT JOIN public.bot_shell_requests s ON s.bot_id=i.bot_id AND s.event_id=i.event_id "
                    "LEFT JOIN public.bot_business_command_requests b ON b.bot_id=i.bot_id AND b.event_id=i.event_id "
                    "LEFT JOIN public.bot_message_handler_chains h ON h.bot_id=i.bot_id AND h.event_id=i.event_id "
                    "LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=i.bot_id AND t.event_id=i.event_id "
                    "WHERE i.bot_id=%s AND (%s::text IS NULL OR i.conversation_id=%s) "
                    "AND (%s::text IS NULL OR c.binding_id=%s) "
                    "AND (%s::text IS NULL OR c.channel_id=%s) "
                    "AND (%s::text IS NULL OR i.event_id=%s) "
                    "AND (%s::timestamptz IS NULL OR i.received_at>=%s::timestamptz) "
                    "AND (%s::timestamptz IS NULL OR i.received_at<%s::timestamptz) "
                    "AND (%s::text IS NULL OR b.component_id=%s OR EXISTS(SELECT 1 FROM "
                    "public.bot_message_handler_chain_items x WHERE x.bot_id=i.bot_id "
                    "AND x.chain_id=h.chain_id AND x.component_id=%s)) "
                    "AND (%s::text IS NULL OR b.generation_id=%s OR EXISTS(SELECT 1 FROM "
                    "public.bot_message_handler_chain_items x WHERE x.bot_id=i.bot_id "
                    "AND x.chain_id=h.chain_id AND x.generation_id=%s)) "
                    "AND (%s::text IS NULL OR COALESCE(s.status,b.status,h.status,t.status,"
                    "i.trigger_decision)=%s) ORDER BY i.received_at DESC,i.event_id DESC LIMIT %s",
                    (
                        self.store.bot_id,
                        conversation_id, conversation_id,
                        binding_id, binding_id,
                        channel_id, channel_id,
                        event_id, event_id,
                        received_after, received_after,
                        received_before, received_before,
                        component_id, component_id, component_id,
                        generation_id, generation_id, generation_id,
                        status, status,
                        limit,
                    ),
                )
            ).fetchall()
            event_ids = [row["event_id"] for row in roots]
            if not event_ids:
                await self.control._time(conn, claims)
                return {"filter": payload, "traces": []}

            shells = await (
                await conn.execute(
                    "SELECT command_id,event_id,command_name,status,operation_id,result_digest,"
                    "created_at,completed_at FROM public.bot_shell_requests "
                    "WHERE bot_id=%s AND event_id=ANY(%s)",
                    (self.store.bot_id, event_ids),
                )
            ).fetchall()
            businesses = await (
                await conn.execute(
                    "SELECT b.request_id,b.event_id,b.command_id,b.command_name,b.component_id,"
                    "b.generation_id,b.status,b.terminal_reason,b.created_at,b.completed_at,"
                    "r.result_id,r.operation_id,r.call_operation_id,r.disposition,r.result_digest,"
                    "r.response_seq,r.created_at AS result_at FROM public.bot_business_command_requests b "
                    "LEFT JOIN public.bot_business_command_results r ON r.bot_id=b.bot_id "
                    "AND r.request_id=b.request_id WHERE b.bot_id=%s AND b.event_id=ANY(%s)",
                    (self.store.bot_id, event_ids),
                )
            ).fetchall()
            chains = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                    "AND event_id=ANY(%s)",
                    (self.store.bot_id, event_ids),
                )
            ).fetchall()
            chain_ids = [row["chain_id"] for row in chains]
            items = []
            if chain_ids:
                items = await (
                    await conn.execute(
                        "SELECT i.*,a.attempt_no,a.operation_id,a.call_operation_id,"
                        "a.created_at AS attempt_at,r.result_id,r.disposition,r.result_digest,"
                        "r.response_seq,r.created_at AS result_at,f.failure_id,f.error_code,"
                        "f.retryable,f.retry_at,f.created_at AS failure_at FROM "
                        "public.bot_message_handler_chain_items i "
                        "LEFT JOIN LATERAL (SELECT z.* FROM public.bot_message_handler_attempts z "
                        "WHERE z.bot_id=i.bot_id AND z.chain_id=i.chain_id "
                        "AND z.route_index=i.route_index ORDER BY z.attempt_no DESC LIMIT 1) a ON true "
                        "LEFT JOIN public.bot_message_handler_results r ON r.bot_id=a.bot_id "
                        "AND r.chain_id=a.chain_id AND r.attempt_no=a.attempt_no "
                        "LEFT JOIN public.bot_message_handler_failures f ON f.bot_id=a.bot_id "
                        "AND f.chain_id=a.chain_id AND f.attempt_no=a.attempt_no "
                        "WHERE i.bot_id=%s AND i.chain_id=ANY(%s) "
                        "ORDER BY i.chain_id,i.route_index",
                        (self.store.bot_id, chain_ids),
                    )
                ).fetchall()
            agents = await (
                await conn.execute(
                    "SELECT t.event_id,t.trigger_id,t.status AS trigger_status,t.claimed_run_id,"
                    "t.terminal_reason,a.phase AS agent_phase,a.final_reply_id,a.error_code,"
                    "a.created_at,a.deadline_at FROM public.bot_conversation_triggers t "
                    "LEFT JOIN public.bot_agent_runs a ON a.bot_id=t.bot_id AND a.run_id=t.claimed_run_id "
                    "WHERE t.bot_id=%s AND t.event_id=ANY(%s)",
                    (self.store.bot_id, event_ids),
                )
            ).fetchall()
            await self.control._time(conn, claims)

        shell_by_event = {row["event_id"]: row for row in shells}
        business_by_event = {row["event_id"]: row for row in businesses}
        chain_by_event = {row["event_id"]: row for row in chains}
        items_by_chain = {}
        for row in items:
            items_by_chain.setdefault(row["chain_id"], []).append(row)
        agent_by_event = {row["event_id"]: row for row in agents}
        traces = []
        for root in roots:
            entries = [{
                "kind": "ingress",
                "node_id": "runtime:adapter.onebot",
                "component_id": "adapter.onebot",
                "generation_id": None,
                "status": root["trigger_decision"],
                "reason": root["classification_reason"],
                "started_at": _iso(root["received_at"]),
                "completed_at": _iso(root["received_at"]) if root["trigger_decision"] != "PENDING" else None,
                "references": {},
            }]
            shell = shell_by_event.get(root["event_id"])
            if shell:
                entries.append({
                    "kind": "shell",
                    "node_id": "runtime:shell.core",
                    "component_id": "shell.core",
                    "generation_id": None,
                    "status": shell["status"],
                    "reason": None,
                    "started_at": _iso(shell["created_at"]),
                    "completed_at": _iso(shell["completed_at"]),
                    "references": {
                        "command_id": shell["command_id"],
                        "operation_id": shell["operation_id"],
                        "result_digest": shell["result_digest"],
                    },
                })
            business = business_by_event.get(root["event_id"])
            if business:
                entries.append({
                    "kind": "business_command",
                    "node_id": "command:" + business["command_id"],
                    "component_id": business["component_id"],
                    "generation_id": business["generation_id"],
                    "status": business["disposition"] or business["status"],
                    "reason": business["terminal_reason"],
                    "started_at": _iso(business["created_at"]),
                    "completed_at": _iso(business["result_at"] or business["completed_at"]),
                    "references": {
                        key: business[key] for key in (
                            "request_id", "result_id", "operation_id", "call_operation_id",
                            "result_digest", "response_seq",
                        ) if business[key] is not None
                    },
                })
            chain = chain_by_event.get(root["event_id"])
            if chain:
                for item in items_by_chain.get(chain["chain_id"], []):
                    result = item if item["result_id"] is not None else None
                    failure = item if item["failure_id"] is not None else None
                    item_status, reason = self._handler_status(chain, item, result, failure)
                    references = {"chain_id": chain["chain_id"], "route_index": item["route_index"]}
                    for key in (
                        "attempt_no", "operation_id", "call_operation_id", "result_id",
                        "failure_id", "result_digest", "response_seq",
                    ):
                        if item[key] is not None:
                            references[key] = item[key]
                    entries.append({
                        "kind": "message_handler",
                        "node_id": f'message:{item["component_id"]}:{item["handler_id"]}',
                        "component_id": item["component_id"],
                        "generation_id": item["generation_id"],
                        "status": item_status,
                        "reason": reason,
                        "started_at": _iso(item["attempt_at"]),
                        "completed_at": _iso(item["result_at"] or item["failure_at"]),
                        "references": references,
                    })
            agent = agent_by_event.get(root["event_id"])
            if agent:
                entries.append({
                    "kind": "agent",
                    "node_id": "runtime:agent.core",
                    "component_id": "agent.core",
                    "generation_id": None,
                    "status": agent["agent_phase"] or agent["trigger_status"],
                    "reason": agent["error_code"] or agent["terminal_reason"],
                    "started_at": _iso(agent["created_at"]),
                    "completed_at": None,
                    "references": {
                        key: agent[key] for key in ("trigger_id", "claimed_run_id", "final_reply_id")
                        if agent[key] is not None
                    },
                })
            traces.append({
                **root,
                "received_at": _iso(root["received_at"]),
                "entries": entries,
            })
        return {"filter": payload, "traces": traces}



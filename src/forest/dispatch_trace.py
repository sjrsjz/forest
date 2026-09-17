"""Read-only dispatch traces projected from durable execution authorities."""

import base64
import binascii
import json
from datetime import datetime

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.control import BotControl

_MAX_CAUSAL_ENTRIES = 256
_MAX_CAUSAL_HOPS = 8
_TRACE_FILTER_FIELDS = (
    "conversation_id", "binding_id", "channel_id", "event_id", "component_id",
    "generation_id", "status", "received_after", "received_before",
)


def _iso(value):
    return value.isoformat() if value is not None else None


def _trace_filters(payload):
    return {key: payload[key] for key in _TRACE_FILTER_FIELDS if payload.get(key) is not None}


def _trace_cursor(filters, row):
    value = {
        "v": 1,
        "filters": digest_json(filters),
        "received_at": row["received_at"].isoformat(),
        "event_id": row["event_id"],
    }
    raw = json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _read_trace_cursor(value, filters):
    if value is None:
        return None, None
    try:
        if (
            not isinstance(value, str)
            or not 1 <= len(value) <= 512
            or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for char in value)
        ):
            raise ValueError
        padded = value + "=" * (-len(value) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded).decode())
        if (
            set(decoded) != {"v", "filters", "received_at", "event_id"}
            or decoded["v"] != 1
            or decoded["filters"] != digest_json(filters)
            or not isinstance(decoded["event_id"], str)
            or not 1 <= len(decoded["event_id"]) <= 256
        ):
            raise ValueError
        received_at = datetime.fromisoformat(decoded["received_at"])
        if received_at.tzinfo is None:
            raise ValueError
    except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError, binascii.Error):
        raise ForestError(
            "DISPATCH_TRACE_CURSOR_INVALID",
            "Dispatch trace cursor is invalid or belongs to different filters",
            422,
        ) from None
    return received_at, decoded["event_id"]


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

    async def _causal_entries(self, conn, seed_operations, causes):
        """Follow only durable causal identities; never infer lineage from payload text."""
        calls, events, event_deliveries, jobs = {}, {}, {}, {}
        cause_by_operation = dict(seed_operations)
        frontier = set(seed_operations)
        truncated = False
        causal_kinds = {
            "KERNEL_OPERATION", "BUSINESS_COMMAND_REQUEST", "MESSAGE_HANDLER_CHAIN",
            "CONVERSATION_RUN",
        }
        cause_ids_by_kind = {}
        for item in causes:
            if item["kind"] in causal_kinds:
                cause_ids_by_kind.setdefault(item["kind"], set()).add(item["id"])
        for cause_kind, cause_ids in sorted(cause_ids_by_kind.items()):
            rows = await (
                await conn.execute(
                    "SELECT operation_id,causal_root_id FROM public.bot_calls WHERE bot_id=%s "
                    "AND causal_root_kind=%s AND causal_root_id=ANY(%s) "
                    "ORDER BY created_at,call_id LIMIT %s",
                    (
                        self.store.bot_id, cause_kind, sorted(cause_ids),
                        _MAX_CAUSAL_ENTRIES + 1,
                    ),
                )
            ).fetchall()
            if len(rows) > _MAX_CAUSAL_ENTRIES:
                truncated = True
                rows = rows[:_MAX_CAUSAL_ENTRIES]
            for row in rows:
                frontier.add(row["operation_id"])
                cause_by_operation[row["operation_id"]] = {
                    "kind": cause_kind.lower(),
                    "id": row["causal_root_id"],
                }
        for _ in range(_MAX_CAUSAL_HOPS):
            frontier -= {row["operation_id"] for row in calls.values()}
            if not frontier:
                break
            call_rows = await (
                await conn.execute(
                    "WITH RECURSIVE tree AS ("
                    "SELECT c.* FROM public.bot_calls c WHERE c.bot_id=%s AND c.operation_id=ANY(%s) "
                    "UNION ALL SELECT child.* FROM public.bot_calls child JOIN tree parent "
                    "ON child.bot_id=parent.bot_id AND child.parent_call_id=parent.call_id "
                    "WHERE child.call_depth<=8) SELECT DISTINCT ON (call_id) * FROM tree "
                    "ORDER BY call_id,call_depth LIMIT %s",
                    (self.store.bot_id, list(frontier), _MAX_CAUSAL_ENTRIES + 1),
                )
            ).fetchall()
            if len(call_rows) > _MAX_CAUSAL_ENTRIES:
                truncated = True
                call_rows = call_rows[:_MAX_CAUSAL_ENTRIES]
            new_calls = []
            for row in call_rows:
                if row["call_id"] not in calls:
                    calls[row["call_id"]] = row
                    new_calls.append(row)
            if not new_calls:
                break
            call_ids = [row["call_id"] for row in new_calls]
            event_rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_events WHERE bot_id=%s "
                    "AND publisher_call_id=ANY(%s) ORDER BY event_seq,event_id LIMIT %s",
                    (self.store.bot_id, call_ids, _MAX_CAUSAL_ENTRIES + 1),
                )
            ).fetchall()
            if len(event_rows) > _MAX_CAUSAL_ENTRIES:
                truncated = True
                event_rows = event_rows[:_MAX_CAUSAL_ENTRIES]
            for row in event_rows:
                events[row["event_id"]] = row
            if event_rows:
                delivery_rows = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_event_deliveries WHERE bot_id=%s "
                        "AND event_id=ANY(%s) ORDER BY event_seq,delivery_id LIMIT %s",
                        (
                            self.store.bot_id,
                            [row["event_id"] for row in event_rows],
                            _MAX_CAUSAL_ENTRIES + 1,
                        ),
                    )
                ).fetchall()
                if len(delivery_rows) > _MAX_CAUSAL_ENTRIES:
                    truncated = True
                    delivery_rows = delivery_rows[:_MAX_CAUSAL_ENTRIES]
                for row in delivery_rows:
                    event_deliveries[row["delivery_id"]] = row
                    if row["call_operation_id"]:
                        cause_by_operation[row["call_operation_id"]] = {
                            "kind": "component_event_delivery",
                            "id": row["delivery_id"],
                        }
                        frontier.add(row["call_operation_id"])
            job_rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_component_jobs WHERE bot_id=%s "
                    "AND source_call_id=ANY(%s) ORDER BY job_seq,job_id LIMIT %s",
                    (self.store.bot_id, call_ids, _MAX_CAUSAL_ENTRIES + 1),
                )
            ).fetchall()
            if len(job_rows) > _MAX_CAUSAL_ENTRIES:
                truncated = True
                job_rows = job_rows[:_MAX_CAUSAL_ENTRIES]
            for row in job_rows:
                jobs[row["job_id"]] = row
                if row["call_operation_id"]:
                    cause_by_operation[row["call_operation_id"]] = {
                        "kind": "component_job",
                        "id": row["job_id"],
                    }
                    frontier.add(row["call_operation_id"])
            if len(calls) + len(events) + len(event_deliveries) + len(jobs) > _MAX_CAUSAL_ENTRIES:
                truncated = True
                break
        else:
            truncated = bool(frontier - {row["operation_id"] for row in calls.values()})

        call_by_operation = {row["operation_id"]: row for row in calls.values()}
        entries = []
        for row in calls.values():
            if row["parent_call_id"]:
                cause = {"kind": "kernel_call", "id": row["parent_call_id"]}
            else:
                cause = cause_by_operation.get(row["operation_id"])
            references = {
                "call_id": row["call_id"],
                "operation_id": row["operation_id"],
                "root_call_id": row["root_call_id"],
                "call_depth": row["call_depth"],
                "input_digest": row["input_digest"],
            }
            if cause:
                references["caused_by"] = cause
            if row["parent_call_id"]:
                references["parent_call_id"] = row["parent_call_id"]
            if row.get("result_digest"):
                references["result_digest"] = row["result_digest"]
            entries.append({
                "kind": "kernel_call",
                "node_id": "call:" + row["call_id"],
                "component_id": row["component_id"],
                "generation_id": row["generation_id"],
                "status": row["status"],
                "reason": row["error_code"],
                "started_at": _iso(row["created_at"]),
                "completed_at": None,
                "references": references,
            })
        for row in events.values():
            entries.append({
                "kind": "component_event",
                "node_id": "event:" + row["event_id"],
                "component_id": row["publisher_component_id"],
                "generation_id": row["publisher_generation_id"],
                "status": "PUBLISHED",
                "reason": None,
                "started_at": _iso(row["created_at"]),
                "completed_at": _iso(row["created_at"]),
                "references": {
                    "event_id": row["event_id"], "event_seq": row["event_seq"],
                    "topic": row["topic"], "payload_digest": row["payload_digest"],
                    "caused_by": {"kind": "kernel_call", "id": row["publisher_call_id"]},
                },
            })
        for row in event_deliveries.values():
            call = call_by_operation.get(row["call_operation_id"])
            entries.append({
                "kind": "component_event_delivery",
                "node_id": "event-delivery:" + row["delivery_id"],
                "component_id": row["component_id"],
                "generation_id": call["generation_id"] if call else None,
                "status": row["status"],
                "reason": row["last_error_code"],
                "started_at": _iso(row["created_at"]),
                "completed_at": _iso(row["acked_at"]),
                "references": {
                    "delivery_id": row["delivery_id"], "event_id": row["event_id"],
                    "subscriber_id": row["subscriber_id"],
                    "call_operation_id": row["call_operation_id"],
                    "caused_by": {"kind": "component_event", "id": row["event_id"]},
                },
            })
        for row in jobs.values():
            call = call_by_operation.get(row["call_operation_id"])
            entries.append({
                "kind": "component_job",
                "node_id": "job:" + row["job_id"],
                "component_id": row["component_id"],
                "generation_id": call["generation_id"] if call else None,
                "status": row["status"],
                "reason": row["last_error_code"],
                "started_at": _iso(row["created_at"]),
                "completed_at": _iso(row["acked_at"]),
                "references": {
                    "job_id": row["job_id"], "job_seq": row["job_seq"],
                    "handler_id": row["handler_id"],
                    "call_operation_id": row["call_operation_id"],
                    "caused_by": {"kind": "kernel_call", "id": row["source_call_id"]},
                },
            })

        cause_ids_by_kind = {}
        for item in causes:
            cause_ids_by_kind.setdefault(item["kind"], set()).add(item["id"])
        delivery_rows = []
        for cause_kind, cause_ids in sorted(cause_ids_by_kind.items()):
            rows = await (
                await conn.execute(
                    "SELECT i.*,p.part_index,p.part_operation_id,p.payload_digest,p.state,p.revision,"
                    "p.error_code,p.attempt_id,p.attempt_count,p.updated_at,a.state AS attempt_state,"
                    "a.wire_digest,a.executor_ref,a.permitted_at,a.started_at,a.observed_at,"
                    "r.operation_id AS resolution_operation_id,r.decision AS resolution_decision "
                    "FROM public.bot_send_intents i JOIN public.bot_send_parts p USING(bot_id,intent_id) "
                    "LEFT JOIN public.bot_send_attempts a ON a.bot_id=p.bot_id AND a.attempt_id=p.attempt_id "
                    "LEFT JOIN LATERAL (SELECT x.operation_id,x.decision FROM public.bot_delivery_resolutions x "
                    "WHERE (x.bot_id,x.intent_id,x.part_index)=(p.bot_id,p.intent_id,p.part_index) "
                    "ORDER BY x.created_at DESC LIMIT 1) r ON true "
                    "WHERE i.bot_id=%s AND i.cause_kind=%s AND i.cause_id=ANY(%s) "
                    "ORDER BY i.created_at,i.intent_id,p.part_index "
                    "LIMIT %s",
                    (
                        self.store.bot_id, cause_kind, sorted(cause_ids),
                        _MAX_CAUSAL_ENTRIES + 1,
                    ),
                )
            ).fetchall()
            delivery_rows.extend(rows)
        if delivery_rows:
            delivery_rows.sort(
                key=lambda row: (row["created_at"], row["intent_id"], row["part_index"])
            )
            if len(delivery_rows) > _MAX_CAUSAL_ENTRIES:
                truncated = True
                delivery_rows = delivery_rows[:_MAX_CAUSAL_ENTRIES]
            for row in delivery_rows:
                executor = row["executor_ref"] or {}
                references = {
                    "intent_id": row["intent_id"], "part_index": row["part_index"],
                    "part_operation_id": row["part_operation_id"],
                    "root_operation_id": row["root_operation_id"],
                    "payload_digest": row["payload_digest"], "revision": row["revision"],
                    "attempt_count": row["attempt_count"],
                    "caused_by": {"kind": row["cause_kind"].lower(), "id": row["cause_id"]},
                }
                for key in ("attempt_id", "attempt_state", "wire_digest", "resolution_operation_id", "resolution_decision"):
                    if row[key] is not None:
                        references[key] = row[key]
                entries.append({
                    "kind": "delivery_part",
                    "node_id": f'delivery:{row["intent_id"]}:{row["part_index"]}',
                    "component_id": executor.get("component_id", "adapter.onebot"),
                    "generation_id": executor.get("generation_id"),
                    "status": row["state"],
                    "reason": row["error_code"],
                    "started_at": _iso(row["started_at"] or row["permitted_at"] or row["created_at"]),
                    "completed_at": _iso(row["observed_at"]),
                    "references": references,
                })
        entries.sort(key=lambda item: (item["started_at"] or "", item["kind"], item["node_id"]))
        if len(entries) > _MAX_CAUSAL_ENTRIES:
            truncated = True
            entries = entries[:_MAX_CAUSAL_ENTRIES]
        return entries, truncated

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
        before = payload.get("before")
        filters = _trace_filters(payload)
        call_root = lambda alias: (  # noqa: E731
            f"(({alias}.causal_root_kind='BUSINESS_COMMAND_REQUEST' AND {alias}.causal_root_id=b.request_id) OR "
            f"({alias}.causal_root_kind='MESSAGE_HANDLER_CHAIN' AND {alias}.causal_root_id=h.chain_id) OR "
            f"({alias}.causal_root_kind='CONVERSATION_RUN' AND {alias}.causal_root_id=t.claimed_run_id))"
        )
        delivery_root = lambda alias: (  # noqa: E731
            f"(({alias}.cause_kind='SHELL_COMMAND' AND {alias}.cause_id=s.command_id) OR "
            f"({alias}.cause_kind='BUSINESS_COMMAND_REQUEST' AND {alias}.cause_id=b.request_id) OR "
            f"({alias}.cause_kind='MESSAGE_HANDLER_CHAIN' AND {alias}.cause_id=h.chain_id) OR "
            f"({alias}.cause_kind='CONVERSATION_RUN' AND {alias}.cause_id=t.claimed_run_id))"
        )

        def entry_filter(component_expression, generation_expression):
            parts, values = [], []
            if component_id is not None:
                parts.append(component_expression + "=%s")
                values.append(component_id)
            if generation_id is not None:
                parts.append(generation_expression + "=%s")
                values.append(generation_id)
            return "(" + " AND ".join(parts) + ")", values

        root_filters, owner_filters, root_filter_values = [], [], []
        if component_id is not None or generation_id is not None:
            direct, values = entry_filter("b.component_id", "b.generation_id")
            owner_filters.append("(b.request_id IS NOT NULL AND " + direct + ")")
            root_filter_values.extend(values)
            handler, values = entry_filter("x.component_id", "x.generation_id")
            owner_filters.append(
                "EXISTS(SELECT 1 FROM public.bot_message_handler_chain_items x "
                "WHERE x.bot_id=i.bot_id AND x.chain_id=h.chain_id AND " + handler + ")"
            )
            root_filter_values.extend(values)
            runtime, values = entry_filter("n->>'component_id'", "n->>'generation_id'")
            owner_filters.append(
                "EXISTS(SELECT 1 FROM public.bot_route_graph_snapshots gs CROSS JOIN LATERAL "
                "jsonb_array_elements(gs.snapshot->'nodes') n WHERE gs.bot_id=i.bot_id "
                "AND gs.graph_digest=i.graph_digest AND n->>'kind'='runtime_module' AND "
                + runtime + ")"
            )
            root_filter_values.extend(values)
            call, values = entry_filter("kc.component_id", "kc.generation_id")
            owner_filters.append(
                "EXISTS(SELECT 1 FROM public.bot_calls kc WHERE kc.bot_id=i.bot_id AND "
                + call_root("kc") + " AND " + call + ")"
            )
            root_filter_values.extend(values)
            event, values = entry_filter("ed.component_id", "dc.generation_id")
            owner_filters.append(
                "EXISTS(SELECT 1 FROM public.bot_component_events ce "
                "JOIN public.bot_calls pc ON pc.bot_id=ce.bot_id AND pc.call_id=ce.publisher_call_id "
                "JOIN public.bot_event_deliveries ed ON ed.bot_id=ce.bot_id AND ed.event_id=ce.event_id "
                "LEFT JOIN public.bot_calls dc ON dc.bot_id=ed.bot_id "
                "AND dc.operation_id=ed.call_operation_id WHERE ce.bot_id=i.bot_id AND "
                + call_root("pc") + " AND " + event + ")"
            )
            root_filter_values.extend(values)
            job, values = entry_filter("j.component_id", "jc.generation_id")
            owner_filters.append(
                "EXISTS(SELECT 1 FROM public.bot_component_jobs j "
                "JOIN public.bot_calls pc ON pc.bot_id=j.bot_id AND pc.call_id=j.source_call_id "
                "LEFT JOIN public.bot_calls jc ON jc.bot_id=j.bot_id "
                "AND jc.operation_id=j.call_operation_id WHERE j.bot_id=i.bot_id AND "
                + call_root("pc") + " AND " + job + ")"
            )
            root_filter_values.extend(values)
            effect, values = entry_filter(
                "COALESCE(sa.executor_ref->>'component_id','adapter.onebot')",
                "sa.executor_ref->>'generation_id'",
            )
            owner_filters.append(
                "EXISTS(SELECT 1 FROM public.bot_send_intents si "
                "JOIN public.bot_send_parts sp USING(bot_id,intent_id) "
                "LEFT JOIN public.bot_send_attempts sa ON sa.bot_id=sp.bot_id "
                "AND sa.attempt_id=sp.attempt_id WHERE si.bot_id=i.bot_id AND "
                + delivery_root("si") + " AND " + effect + ")"
            )
            root_filter_values.extend(values)
            root_filters.append("(" + " OR ".join(owner_filters) + ")")
        if status is not None:
            root_filters.append(
                "(COALESCE(s.status,b.status,h.status,t.status,i.trigger_decision)=%s OR "
                "EXISTS(SELECT 1 FROM public.bot_calls kc WHERE kc.bot_id=i.bot_id AND "
                + call_root("kc") + " AND kc.status=%s) OR "
                "EXISTS(SELECT 1 FROM public.bot_component_events ce "
                "JOIN public.bot_calls pc ON pc.bot_id=ce.bot_id AND pc.call_id=ce.publisher_call_id "
                "JOIN public.bot_event_deliveries ed ON ed.bot_id=ce.bot_id AND ed.event_id=ce.event_id "
                "WHERE ce.bot_id=i.bot_id AND " + call_root("pc") + " AND ed.status=%s) OR "
                "EXISTS(SELECT 1 FROM public.bot_component_jobs j "
                "JOIN public.bot_calls pc ON pc.bot_id=j.bot_id AND pc.call_id=j.source_call_id "
                "WHERE j.bot_id=i.bot_id AND " + call_root("pc") + " AND j.status=%s) OR "
                "EXISTS(SELECT 1 FROM public.bot_send_intents si "
                "JOIN public.bot_send_parts sp USING(bot_id,intent_id) WHERE si.bot_id=i.bot_id "
                "AND " + delivery_root("si") + " AND sp.state=%s))"
            )
            root_filter_values.extend([status] * 5)
        extra_where = ""
        if root_filters:
            extra_where = "AND " + " AND ".join(root_filters) + " "
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, "module.read", operation_id, payload)
            before_at, before_event_id = _read_trace_cursor(before, filters)
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
                    "AND (%s::timestamptz IS NULL OR "
                    "(i.received_at,i.event_id)<(%s::timestamptz,%s)) "
                    + extra_where +
                    "ORDER BY i.received_at DESC,i.event_id DESC LIMIT %s",
                    (
                        self.store.bot_id,
                        conversation_id, conversation_id,
                        binding_id, binding_id,
                        channel_id, channel_id,
                        event_id, event_id,
                        received_after, received_after,
                        received_before, received_before,
                        before_at, before_at, before_event_id,
                        *root_filter_values,
                        limit + 1,
                    ),
                )
            ).fetchall()
            has_more = len(roots) > limit
            roots = roots[:limit]
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
            run_ids = sorted({row["claimed_run_id"] for row in agents if row["claimed_run_id"]})
            agent_tools = []
            if run_ids:
                agent_tools = await (
                    await conn.execute(
                        "SELECT run_id,tool_call_id,operation_id FROM public.bot_agent_tools "
                        "WHERE bot_id=%s AND run_id=ANY(%s) ORDER BY run_id,call_index",
                        (self.store.bot_id, run_ids),
                    )
                ).fetchall()
            graph_rows = await (
                await conn.execute(
                    "SELECT graph_digest,snapshot FROM public.bot_route_graph_snapshots "
                    "WHERE bot_id=%s AND graph_digest=ANY(%s)",
                    (self.store.bot_id, list({row["graph_digest"] for row in roots})),
                )
            ).fetchall()
            shell_by_event = {row["event_id"]: row for row in shells}
            business_by_event = {row["event_id"]: row for row in businesses}
            chain_by_event = {row["event_id"]: row for row in chains}
            items_by_chain = {}
            for row in items:
                items_by_chain.setdefault(row["chain_id"], []).append(row)
            agent_by_event = {row["event_id"]: row for row in agents}
            tools_by_run = {}
            for row in agent_tools:
                tools_by_run.setdefault(row["run_id"], []).append(row)
            causal_by_event = {}
            for root in roots:
                seeds = {}
                cause_roots = []
                shell = shell_by_event.get(root["event_id"])
                if shell:
                    cause_roots.append({"kind": "SHELL_COMMAND", "id": shell["command_id"]})
                business = business_by_event.get(root["event_id"])
                if business:
                    cause = {"kind": "business_command_request", "id": business["request_id"]}
                    cause_roots.append({"kind": "BUSINESS_COMMAND_REQUEST", "id": business["request_id"]})
                    if business["call_operation_id"]:
                        seeds[business["call_operation_id"]] = cause
                chain = chain_by_event.get(root["event_id"])
                if chain:
                    cause_roots.append({"kind": "MESSAGE_HANDLER_CHAIN", "id": chain["chain_id"]})
                    for item in items_by_chain.get(chain["chain_id"], ()):
                        if item["call_operation_id"]:
                            seeds[item["call_operation_id"]] = {
                                "kind": "message_handler_attempt",
                                "id": f'{chain["chain_id"]}:{item["route_index"]}:{item["attempt_no"]}',
                            }
                agent = agent_by_event.get(root["event_id"])
                if agent and agent["claimed_run_id"]:
                    run_id = agent["claimed_run_id"]
                    cause_roots.append({"kind": "CONVERSATION_RUN", "id": run_id})
                    for tool in tools_by_run.get(run_id, ()):
                        seeds[tool["operation_id"]] = {
                            "kind": "agent_tool", "id": f'{run_id}:{tool["tool_call_id"]}'
                        }
                causal_by_event[root["event_id"]] = await self._causal_entries(
                    conn, seeds, cause_roots
                )
            await self.control._time(conn, claims)

        graph_by_digest = {row["graph_digest"]: row["snapshot"] for row in graph_rows}

        def runtime_identity(graph_digest, component):
            graph = graph_by_digest.get(graph_digest, {})
            for node in graph.get("nodes", ()):
                if node.get("kind") == "runtime_module" and node.get("component_id") == component:
                    return node["node_id"], node["generation_id"]
            return "runtime:" + component, None

        traces = []
        for root in roots:
            adapter_node, adapter_generation = runtime_identity(
                root["graph_digest"], "adapter.onebot"
            )
            entries = [{
                "kind": "ingress",
                "node_id": adapter_node,
                "component_id": "adapter.onebot",
                "generation_id": adapter_generation,
                "status": root["trigger_decision"],
                "reason": root["classification_reason"],
                "started_at": _iso(root["received_at"]),
                "completed_at": _iso(root["received_at"]) if root["trigger_decision"] != "PENDING" else None,
                "references": {},
            }]
            shell = shell_by_event.get(root["event_id"])
            if shell:
                shell_node, shell_generation = runtime_identity(
                    root["graph_digest"], "shell.command"
                )
                entries.append({
                    "kind": "shell",
                    "node_id": shell_node,
                    "component_id": "shell.command",
                    "generation_id": shell_generation,
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
                agent_node, agent_generation = runtime_identity(
                    root["graph_digest"], "agent.runtime"
                )
                entries.append({
                    "kind": "agent",
                    "node_id": agent_node,
                    "component_id": "agent.runtime",
                    "generation_id": agent_generation,
                    "status": agent["agent_phase"] or agent["trigger_status"],
                    "reason": agent["error_code"] or agent["terminal_reason"],
                    "started_at": _iso(agent["created_at"]),
                    "completed_at": None,
                    "references": {
                        key: agent[key] for key in ("trigger_id", "claimed_run_id", "final_reply_id")
                        if agent[key] is not None
                    },
                })
            causal_entries, causal_truncated = causal_by_event[root["event_id"]]
            entries.extend(causal_entries)
            traces.append({
                **root,
                "received_at": _iso(root["received_at"]),
                "entries": entries,
                "causal_truncated": causal_truncated,
            })
        if component_id is not None or generation_id is not None or status is not None:
            def matches(trace):
                entries = trace["entries"]
                if component_id is not None and generation_id is not None:
                    if not any(
                        entry["component_id"] == component_id
                        and entry["generation_id"] == generation_id for entry in entries
                    ):
                        return False
                elif component_id is not None and not any(
                    entry["component_id"] == component_id for entry in entries
                ):
                    return False
                elif generation_id is not None and not any(
                    entry["generation_id"] == generation_id for entry in entries
                ):
                    return False
                return status is None or trace["status"] == status or any(
                    entry["status"] == status for entry in entries
                )
            traces = [trace for trace in traces if matches(trace)][:limit]
        result = {"filter": payload, "traces": traces}
        if has_more and roots:
            result["next_before"] = _trace_cursor(filters, roots[-1])
        return result
